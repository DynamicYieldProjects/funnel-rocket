import os
import random
import shutil
import tempfile
import time
from typing import Type, cast, List, NamedTuple
from contextlib import contextmanager
import pytest
import numpy as np
from pandas import Series, RangeIndex, DataFrame
from frocket.common.config import config
from frocket.common.dataset import DatasetPartsInfo, DatasetPartId, PartNamingMethod, DatasetId
from frocket.common.helpers.utils import timestamped_uuid
from frocket.common.metrics import MetricsBag, ComponentLabel
from frocket.common.tasks.base import BaseTaskResult, BaseTaskRequest, TaskStatus, TaskAttemptsInfo, JobStatus, \
    BaseJobResult
from frocket.common.tasks.registration import DatasetValidationMode, REGISTER_DEFAULT_FILENAME_PATTERN, RegisterArgs, \
    RegistrationTaskRequest, RegistrationTaskResult, RegistrationJobResult
from frocket.datastore.registered_datastores import get_datastore, get_blobstore
from frocket.invoker.jobs.job_builder import JobBuilder
from frocket.invoker.jobs.registration_job_builder import RegistrationJobBuilder
from frocket.worker.impl.generic_env_metrics import GenericEnvMetricsProvider
from frocket.worker.runners.base_task_runner import TaskRunnerContext
from frocket.worker.runners.registered_runners import REGISTERED_RUNNERS

DEFAULT_ROW_COUNT = 1000
DEFAULT_GROUP_COUNT = 200
BASE_TIME = 1609459200000  # Start of 2021
TIME_SHIFT = 10000
STR_OR_NONE_VALUES = ["1", "2", "3", None]
CAT_SHORT_TOP = [0.9, 0.07, 0.02, 0.01]
CAT_LONG_TOP = [0.5, 0.2] + [0.01] * 30
# noinspection PyProtectedMember
TEMP_DIR = tempfile._get_default_tempdir()


def weighted_list(size: int, weights: list) -> list:
    res = []
    for idx, w in enumerate(weights):
        v = str(idx)
        vlen = size * w
        res += [v] * int(vlen)
    assert len(res) == size
    return res


# noinspection PyProtectedMember
def temp_filename(suffix='', with_dir: bool = True):
    fname = next(tempfile._get_candidate_names()) + suffix
    return f"{TEMP_DIR}/{fname}" if with_dir else fname


@pytest.fixture(scope="session", autouse=True)
def init_redis():
    config['redis.db'] = '15'
    config['datastore.redis.prefix'] = "frocket:tests:"
    print(get_datastore(), get_blobstore())  # Fail on no connection, print connection details


@pytest.fixture(scope="module")
def datafile() -> str:
    return create_datafile()


def create_datafile(part: int = 0, size: int = DEFAULT_ROW_COUNT, filename: str = None) -> str:
    idx = RangeIndex(size)
    min_ts = BASE_TIME + (TIME_SHIFT * part)
    max_ts = BASE_TIME + (TIME_SHIFT * (part + 1))
    int_timestamps = [min_ts, max_ts] + [random.randint(min_ts, max_ts) for _ in range(size - 2)]
    # print(f"For part: {part}, min-ts: {min(int_timestamps)}, max-ts: {max(int_timestamps)}")
    float_timestamps = [ts + random.random() for ts in int_timestamps]
    dts = [ts * 1000000 for ts in float_timestamps]
    initial_user_id = 100000000 * part
    int64_user_ids = list(range(DEFAULT_GROUP_COUNT)) + \
        [random.randrange(DEFAULT_GROUP_COUNT) for _ in range(size - DEFAULT_GROUP_COUNT)]
    int64_user_ids = [initial_user_id + uid for uid in int64_user_ids]
    str_user_ids = [str(uid) for uid in int64_user_ids]
    str_or_none_ids = random.choices(STR_OR_NONE_VALUES, k=size)
    lists = [[1, 2, 3]] * size

    columns = {'bool': Series(data=random.choices([True, False], k=size), index=idx, dtype='bool'),
               'none_float': Series(data=None, index=idx, dtype='float64'),
               'none_object': Series(data=None, index=idx, dtype='object'),
               'none_str': Series(data=None, index=idx, dtype='str'),
               'int64_userid': Series(data=int64_user_ids, index=idx),
               'str_userid': Series(data=str_user_ids, index=idx),
               'str_none_userid': Series(data=str_or_none_ids, index=idx),
               'int64_ts': Series(data=int_timestamps, index=idx),
               'float64_ts': Series(data=float_timestamps, index=idx),
               'uint32': Series(data=random.choices(range(100), k=size),
                                index=idx, dtype='uint32'),
               'float32': Series(data=[np.nan, *[random.random() for _ in range(size - 2)], np.nan],
                                 index=idx, dtype='float32'),
               'cat_userid_str': Series(data=str_user_ids, index=idx, dtype='category'),
               'cat_short': Series(data=weighted_list(size, CAT_SHORT_TOP), index=idx),
               'cat_long': Series(data=weighted_list(size, CAT_LONG_TOP), index=idx, dtype='category'),
               'cat_float': Series(data=float_timestamps, index=idx, dtype='category'),
               'dt': Series(data=dts, index=idx, dtype='datetime64[us]'),
               'lists': Series(data=lists, index=idx)
               }

    df = DataFrame(columns)
    if not filename:
        filename = temp_filename('.testpq')
    df.to_parquet(filename)
    # Enable if needed
    # print(df.dtypes)
    # print(df)
    # print(f"Written DataFrame to {filename}")
    return filename


def simple_task_runner_ctx() -> TaskRunnerContext:
    return TaskRunnerContext(metrics=MetricsBag(component=ComponentLabel.WORKER,
                                                env_metrics_provider=GenericEnvMetricsProvider()))


def simple_run_task(req: BaseTaskRequest, expected_resultcls: Type[BaseTaskResult]) -> BaseTaskResult:
    ctx = simple_task_runner_ctx()
    task_runner = REGISTERED_RUNNERS[type(req)](req, ctx)
    result = task_runner.run()
    assert type(result) == expected_resultcls
    return result


class InprocessInvokerAndWorker:
    def __init__(self, job: JobBuilder,
                 taskreq_cls: Type[BaseTaskRequest],
                 taskres_cls: Type[BaseTaskResult],
                 jobres_cls: Type[BaseJobResult]):
        self._job = job
        self._taskreq_cls = taskreq_cls
        self._taskres_cls = taskres_cls
        self._jobres_cls = jobres_cls
        self._queue_name = timestamped_uuid('queue-')
        self.num_tasks = None
        self._job_status = None
        self._final_job_status = None
        self._task_results = None

    def prerun(self):
        self._job.prerun()

    def enqueue_tasks(self) -> List[BaseTaskRequest]:
        requests = cast(List[self._taskreq_cls], self._job.build_tasks())
        self.num_tasks = len(requests)
        get_datastore().enqueue(requests, queue=self._queue_name)
        return requests

    def run_tasks(self):
        for i in range(self.num_tasks):
            req = get_datastore().dequeue(queue=self._queue_name, timeout=1)
            assert req
            result = simple_run_task(req, self._taskres_cls)
            assert result.status == TaskStatus.ENDED_SUCCESS
        assert get_datastore().dequeue(queue=self._queue_name, timeout=0) is None  # Queue depleted

        all_attempts = get_datastore().task_results(self._job.request_id)
        assert len(all_attempts) == self.num_tasks
        self._task_results = cast(List[self._taskres_cls], list(all_attempts.values()))
        assert all([res.status == TaskStatus.ENDED_SUCCESS for res in self._task_results])

        tasks_status = get_datastore().tasks_status(self._job.request_id)
        attempts_status = [TaskAttemptsInfo(i) for i in range(self.num_tasks)]
        for at, update in tasks_status.items():
            attempts_status[at.task_index].add(at, update)
        assert all([tat.latest_update.status == TaskStatus.ENDED_SUCCESS for tat in attempts_status])
        self._job_status = JobStatus(success=True, error_message=None, attempts_status=attempts_status)
        return self._task_results

    def complete(self, assert_success: bool = True) -> JobStatus:
        self._final_job_status = self._job.complete(self._job_status, latest_task_results=self._task_results)
        if assert_success:
            assert self._final_job_status.success and not self._final_job_status.error_message
        return self._final_job_status

    def build_result(self, assert_success: bool = True) -> BaseJobResult:
        base_attributes = BaseJobResult(
            request_id=self._job.request_id,
            success=self._final_job_status.success,
            error_message=self._final_job_status.error_message,
            task_counters={}, metrics=[], cost=None). \
            shallowdict(include_none=True)

        job_result = self._job.build_result(base_attributes, self._final_job_status, self._task_results)
        assert type(job_result) is self._jobres_cls
        if assert_success:
            assert job_result.success
        return job_result

    def cleanup(self):
        get_datastore().cleanup_request_data(self._job.request_id)


@contextmanager
def registration_job_invoker(job: RegistrationJobBuilder) -> InprocessInvokerAndWorker:
    invoker_runner = InprocessInvokerAndWorker(
        job, RegistrationTaskRequest, RegistrationTaskResult, RegistrationJobResult)
    try:
        yield invoker_runner
    finally:
        invoker_runner.cleanup()


def build_registration_job(basepath: str,
                           mode: DatasetValidationMode,
                           group_id_column: str = 'int64_userid',
                           pattern: str = REGISTER_DEFAULT_FILENAME_PATTERN,
                           uniques: bool = True) -> RegistrationJobBuilder:
    args = RegisterArgs(
        name=f"test-{basepath}-{time.time()}",
        basepath=basepath,
        group_id_column=group_id_column,
        timestamp_column="int64_ts",
        pattern=pattern,
        validation_mode=mode,
        validate_uniques=uniques)
    job = RegistrationJobBuilder(args=args)
    job.request_id = timestamped_uuid('test-')
    return job


class TestDatasetInfo(NamedTuple):
    basepath: str
    basename_files: List[str]
    fullpath_files: List[str]
    expected_parts: DatasetPartsInfo
    registration_jobs: List[RegistrationJobBuilder] = []

    def expected_part_ids(self, dsid: DatasetId, parts: List[int] = None) -> List[DatasetPartId]:
        parts = parts or range(len(self.fullpath_files))
        return [DatasetPartId(dataset_id=dsid, path=self.fullpath_files[i], part_idx=i)
                for i in parts]

    def registration_job(self,
                         mode: DatasetValidationMode,
                         group_id_column: str = 'int64_userid',
                         pattern: str = REGISTER_DEFAULT_FILENAME_PATTERN,
                         uniques: bool = True) -> RegistrationJobBuilder:
        job = build_registration_job(self.basepath, mode, group_id_column, pattern, uniques)
        self.registration_jobs.append(job)
        return job

    def cleanup(self):
        assert self.basepath.startswith(TEMP_DIR)
        shutil.rmtree(self.basepath)
        for rj in self.registration_jobs:
            if rj.dataset:
                get_datastore().remove_dataset_info(rj.dataset.id.name)


def _build_test_dataset(parts: int, prefix: str = '', suffix: str = '.parquet') -> TestDatasetInfo:
    basepath = temp_filename(suffix="-dataset")
    os.makedirs(basepath)
    basename_files = [f"{prefix}{temp_filename(suffix=('-p' + str(i) + suffix), with_dir=False)}"
                      for i in range(parts)]
    fullpath_files = [f"{basepath}/{fname}" for fname in basename_files]
    [create_datafile(part=i, filename=fname) for i, fname in enumerate(fullpath_files)]

    expected_parts = DatasetPartsInfo(naming_method=PartNamingMethod.LIST,
                                      total_parts=parts,
                                      total_size=sum([os.stat(fname).st_size for fname in fullpath_files]),
                                      running_number_pattern=None,
                                      filenames=basename_files)
    # PyCharm issue?
    # noinspection PyArgumentList
    return TestDatasetInfo(basepath=basepath, basename_files=basename_files,
                           fullpath_files=fullpath_files, expected_parts=expected_parts)


@contextmanager
def new_dataset(parts: int, prefix: str = '', suffix: str = '.parquet') -> TestDatasetInfo:
    test_ds = _build_test_dataset(parts=parts, prefix=prefix, suffix=suffix)
    try:
        yield test_ds
    finally:
        test_ds.cleanup()
