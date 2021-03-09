import logging
import math
import time
from enum import Enum, auto
from typing import Callable, Any, Dict, cast, Union, Type, Optional
import redis
from redis import Redis
from frocket.common.config import config
from frocket.common.serializable import Envelope, SerializableDataClass
from frocket.common.tasks.base import TaskStatus, BaseTaskResult, TaskAttemptId, TaskStatusUpdate, \
    BaseTaskRequest, BlobId
from frocket.common.dataset import DatasetInfo, DatasetPartsInfo, DatasetPartId, DatasetShortSchema, DatasetSchema
from frocket.common.helpers.utils import timestamped_uuid
from frocket.datastore.datastore import Datastore, WorkerSelectedPart, DEFAULT_QUEUE, DEFAULT_DEQUEUE_WAIT_TIME
from frocket.datastore.blobstore import Blobstore, BLOB_DEFAULT_TTL, BLOB_MAX_TTL

logger = logging.getLogger(__name__)

FROCKET_KEY_PREFIX = config.get('datastore.redis.prefix')
DATASETS_HASH_KEY = f"{FROCKET_KEY_PREFIX}datasets"
DS_PARTS_HASH_KEY = f"{FROCKET_KEY_PREFIX}dataset_parts"
DS_LAST_USED_HASH_KEY = f"{FROCKET_KEY_PREFIX}dataset_lastused"
SCHEMAS_HASH_KEY = f"{FROCKET_KEY_PREFIX}schemas"
SHORT_SCHEMAS_HASH_KEY = f"{FROCKET_KEY_PREFIX}short_schemas"
QUEUES_PREFIX = f"{FROCKET_KEY_PREFIX}queues"
BLOB_KEY_PREFIX = f"{FROCKET_KEY_PREFIX}blob"


class RedisStore(Datastore, Blobstore):
    """
    Implementation of both Datastore and Blobstore interfaces with Redis.

    Note that each of these interfaces can be configured to use its own host/port/logical DB, or they could use the
    same one (see configuration guide).

    The underlying Redis object is thread-safe, and since this class has no mutable state,
    it is also considered thread-safe. However, running in a multi-threaded model has not been seriously tested yet.
    """
    ConverterFunc = Callable[[str], Any]

    class ScopedKey(Enum):
        TASK_ATTEMPT_STATUS = auto()
        TASK_ATTEMPT_RESULT = auto()
        TASK_ATTEMPT_NO = auto()
        JOB_PUBLISHED_PARTS = auto()

        # noinspection PyUnusedLocal
        def __init__(self, *args):
            self._name_for_key = self.name.lower().replace('_', ':')

        def build(self, reqid: str, suffix: Any = None) -> str:
            res = f"{FROCKET_KEY_PREFIX}req-{reqid}:{self._name_for_key}"
            if suffix:
                res += f":{suffix}"
            return res

    _redis: redis.Redis
    _host: str

    def __init__(self, role=Union["blobstore", "datastore"]):
        self._role = role
        self._host = config.get_with_fallbacks(f"{role}.redis.host", "redis.host")
        self._port = int(config.get_with_fallbacks(f"{role}.redis.port", "redis.port"))
        self._db = int(config.get_with_fallbacks(f"{role}.redis.db", "redis.db"))

        self._redis = redis.Redis(host=self._host, port=self._port, db=self._db)
        self._redis.ping()  # Just make sure it works

    def write_dataset_info(self, dataset, parts, schema):
        # When re-registering a dataset (with a newer timestamp), it overrides the previous one -
        # That's why the key in datasets hash is by name only.
        # Related objects are written in the same transaction to prevent corruption
        with self._redis.pipeline() as pipe:
            dskey = dataset.id.name
            pipe.hset(DATASETS_HASH_KEY, dskey, dataset.to_json())
            pipe.hset(DS_PARTS_HASH_KEY, dskey, parts.to_json())
            pipe.hset(SCHEMAS_HASH_KEY, dskey, schema.to_json())  # TODO backlog make more compact for storage
            pipe.hset(SHORT_SCHEMAS_HASH_KEY, dskey, schema.short().to_json())
            pipe.hdel(DS_LAST_USED_HASH_KEY, dskey)
            pipe.execute()

    def remove_dataset_info(self, name):
        with self._redis.pipeline() as pipe:
            dskey = name
            deleted_count = pipe.hdel(DATASETS_HASH_KEY, dskey)
            pipe.hdel(DS_PARTS_HASH_KEY, dskey)
            pipe.hdel(SCHEMAS_HASH_KEY, dskey)
            pipe.hdel(SHORT_SCHEMAS_HASH_KEY, dskey)
            pipe.hdel(DS_LAST_USED_HASH_KEY, dskey)
            pipe.execute()
        return deleted_count != 0

    def dataset_info(self, name):
        return self._read_dataset_data(name, DATASETS_HASH_KEY, DatasetInfo)

    def dataset_parts_info(self, ds):
        return self._read_dataset_data(ds.id.name, DS_PARTS_HASH_KEY, DatasetPartsInfo)

    def schema(self, ds):
        return self._read_dataset_data(ds.id.name, SCHEMAS_HASH_KEY, DatasetSchema)

    def short_schema(self, ds):
        return self._read_dataset_data(ds.id.name, SHORT_SCHEMAS_HASH_KEY, DatasetShortSchema)

    def _read_dataset_data(self, dataset_name: str, key: str,
                           cls: Type[SerializableDataClass]) -> Optional[SerializableDataClass]:
        res = self._redis.hget(key, dataset_name)
        return cls.from_json(res) if res else None

    # TODO backlog monitor and limit queue size (if no workers are active to consume it, or they fail to start)
    def enqueue(self, requests, queue=DEFAULT_QUEUE):
        queue_key = f"{QUEUES_PREFIX}:{queue}"

        with self._redis.pipeline() as pipe:
            for req in requests:
                pipe.lpush(queue_key, Envelope.seal_to_json(req))  # See Envelope class - enables polymorphism
            pipe.execute()

    def dequeue(self, queue=DEFAULT_QUEUE, timeout=DEFAULT_DEQUEUE_WAIT_TIME):
        queue_key = f"{QUEUES_PREFIX}:{queue}"

        json_string = None
        if timeout > 0:
            res = self._redis.brpop(queue_key, timeout)
            if res:
                json_string = res[1].decode()
        else:
            res = self._redis.rpop(queue_key)
            if res:
                json_string = res.decode()

        if json_string:
            obj = Envelope.open_from_json(json_string, expected_superclass=BaseTaskRequest)
            return obj
        else:
            return None

    def update_task_status(self, reqid, tasks, status):
        if type(tasks) is TaskAttemptId:
            self._write_task_status(reqid, tasks, status)
        elif type(tasks) is list:
            with self._redis.pipeline() as pipe:
                for taskid in tasks:
                    self._write_task_status(reqid, taskid, status, conn=pipe)
                pipe.execute()
        else:
            raise Exception(f"Unknown type: {type(tasks)}")

    def _write_task_status(self, reqid: str, taskid: TaskAttemptId, status: TaskStatus, conn: Redis = None):
        conn = conn or self._redis
        conn.hset(self.ScopedKey.TASK_ATTEMPT_STATUS.build(reqid),
                  taskid.to_json(),
                  TaskStatusUpdate.now(status).to_json())

    def tasks_status(self, reqid):
        return self._read_hash_for_request(self.ScopedKey.TASK_ATTEMPT_STATUS, reqid,
                                           lambda k: TaskAttemptId.from_json(k),
                                           lambda v: TaskStatusUpdate.from_json(v))

    def write_task_result(self, reqid, taskid, result):
        with self._redis.pipeline() as pipe:
            pipe.hset(self.ScopedKey.TASK_ATTEMPT_RESULT.build(reqid),
                      taskid.to_json(),
                      Envelope.seal_to_json(result))
            self._write_task_status(reqid, taskid, result.status, conn=pipe)
            pipe.execute()

    def task_results(self, reqid):
        return self._read_hash_for_request(self.ScopedKey.TASK_ATTEMPT_RESULT, reqid,
                                           lambda k: TaskAttemptId.from_json(k),
                                           lambda v: Envelope.open_from_json(v, expected_superclass=BaseTaskResult))

    def datasets(self):
        items = self._read_hash(DATASETS_HASH_KEY,
                                lambda k: k,  # Un-needed
                                lambda v: DatasetInfo.from_json(v))
        return list(items.values())

    def last_used(self, ds: DatasetInfo) -> int:
        res = self._redis.hget(DS_LAST_USED_HASH_KEY, ds.id.name)
        return int(res) if res else None

    def mark_used(self, ds: DatasetInfo):
        self._redis.hset(DS_LAST_USED_HASH_KEY, ds.id.name, math.floor(time.time()))
        pass

    def _read_hash_for_request(self, scoped_key: ScopedKey, reqid: str,
                               key_reader: ConverterFunc, value_reader: ConverterFunc) -> Dict:
        return self._read_hash(scoped_key.build(reqid), key_reader, value_reader)

    def _read_hash(self, key: str, key_reader: ConverterFunc, value_reader: ConverterFunc) -> Dict:
        items_dict = self._redis.hgetall(key)
        if items_dict:
            return {key_reader(k.decode('utf-8')): value_reader(v.decode('utf-8'))
                    for k, v in items_dict.items()}
        else:
            return {}

    def publish_for_worker_selection(self, reqid, attempt_round, parts):
        set_key = self.ScopedKey.JOB_PUBLISHED_PARTS.build(reqid, suffix=attempt_round)
        string_members = [part.to_json() for part in parts]
        self._redis.sadd(set_key, *string_members)

    def self_select_part(self, reqid, attempt_round, candidates=None):
        set_key = self.ScopedKey.JOB_PUBLISHED_PARTS.build(reqid, attempt_round)
        # Try selecting one of this worker's candidates, if any
        if candidates:
            for candidate in candidates:
                cand_string = candidate.to_json()
                removed_count = self._redis.srem(set_key, cand_string)
                if removed_count == 1:
                    return WorkerSelectedPart(part_id=candidate, random=False, task_attempt_no=attempt_round)

        res = self._redis.spop(set_key)  # Fallback to taking a random task
        if res:
            part_id = cast(DatasetPartId, DatasetPartId.from_json(res.decode()))
            return WorkerSelectedPart(part_id=part_id, random=True, task_attempt_no=attempt_round)
        else:
            return None

    def increment_attempt(self, reqid, partidx):
        new_attempt_no = self._redis.hincrby(self.ScopedKey.TASK_ATTEMPT_NO.build(reqid), str(partidx))
        if not new_attempt_no:
            raise Exception(f"Failed incrementing task attempt no. for request {reqid} part {partidx}")

        return int(new_attempt_no)

    def cleanup_request_data(self, reqid: str) -> None:
        # Cleanup all scoped keys. Not all necessarily exist.
        # (also with suffixes for JOB_PUBLISHED_PARTS: see method self.publish_for_worker_selection)
        keys = [sk.build(reqid) for sk in self.ScopedKey] + \
               [self.ScopedKey.JOB_PUBLISHED_PARTS.build(reqid, suffix=i) for i in range(3)]
        self._redis.delete(*keys)

    #
    # Blobstore methods
    #

    def write_blob(self, data, ttl=None, tag=None):
        ttl = ttl or BLOB_DEFAULT_TTL
        if BLOB_MAX_TTL != 0 and (ttl == 0 or ttl > BLOB_MAX_TTL):
            raise Exception(f"Requested TTL {ttl} is longer than maximum: {BLOB_MAX_TTL}")

        prefix = f"{BLOB_KEY_PREFIX}:{tag or ''}-"
        new_id = timestamped_uuid(prefix)
        self._redis.setex(new_id, ttl, data)
        return new_id

    @staticmethod
    def _validate_blob_id(blobid: BlobId):
        if not blobid.startswith(BLOB_KEY_PREFIX):
            raise Exception(f"Invalid blob ID: {blobid}")

    def read_blob(self, blobid):
        self._validate_blob_id(blobid)
        return self._redis.get(blobid)

    def delete_blob(self, blobid):
        self._validate_blob_id(blobid)
        deleted_count = self._redis.delete(blobid)
        return deleted_count == 1

    def __str__(self):
        return f"RedisStore(role {self._role}, host {self._host}:{self._port}, db {self._db})"
