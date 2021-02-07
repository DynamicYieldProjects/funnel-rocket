import logging
import time
from abc import abstractmethod
from typing import Optional
from frocket.common.config import config
from frocket.common.metrics import MetricName, MetricsBag
from frocket.common.tasks.base import TaskStatus, BaseTaskRequest, BaseTaskResult, TaskAttemptId
from frocket.datastore.datastore import Datastore
from frocket.datastore.blobstore import Blobstore
from frocket.datastore.registered_datastores import get_datastore, get_blobstore
from frocket.worker.runners.part_loader import PartLoader, shared_part_loader

logger = logging.getLogger(__name__)
REQUEST_MAX_AGE = int(config.get("worker.reject.age"))
DEFAULT_PREFLIGHT_DURATION_MS = config.int("part.selection.preflight.ms")


class TaskRunnerContext:
    def __init__(self,
                 metrics: MetricsBag,
                 private_part_loader: PartLoader = None,
                 preflight_duration_ms: int = None):
        self._metrics = metrics
        self._part_loader = private_part_loader or shared_part_loader()
        if preflight_duration_ms is None:
            preflight_duration_ms = DEFAULT_PREFLIGHT_DURATION_MS
        self._preflight_duration_seconds = preflight_duration_ms / 1000

    @property
    def metrics(self) -> MetricsBag:
        return self._metrics

    # The underlying get_datastore and get_blobstore are memoized - initialized on demand
    @property
    def datastore(self) -> Datastore:
        return get_datastore()

    @property
    def blobstore(self) -> Blobstore:
        return get_blobstore()

    @property
    def part_loader(self) -> PartLoader:
        return self._part_loader

    @property
    def preflight_duration_seconds(self) -> float:
        return self._preflight_duration_seconds


class BaseTaskRunner:
    # Returns (should_run, reject_reason)
    @classmethod
    def should_run(cls, req: BaseTaskRequest) -> (bool, str):
        if cls.time_since_invocation(req) > REQUEST_MAX_AGE:
            return False, f"request is more than {REQUEST_MAX_AGE} seconds old"
        else:
            return True, None

    @staticmethod
    def time_since_invocation(req: BaseTaskRequest):
        return time.time() - req.invoke_time

    def __init__(self, req: BaseTaskRequest,
                 ctx: TaskRunnerContext):
        self._req = req
        self._ctx = ctx
        # TODO important this should be initialized by default (unless not known - make it nicer)
        self._task_attempt_id: Optional[TaskAttemptId] = None

    def run(self) -> BaseTaskResult:
        error_message, engine_result = None, None

        with self._ctx.metrics.measure(MetricName.TASK_TOTAL_RUN_SECONDS):
            try:
                self._ctx.metrics.set_metric(MetricName.INVOKE_TO_RUN_SECONDS,
                                             self.time_since_invocation(self._req))

                self._do_run()
                final_status = TaskStatus.ENDED_SUCCESS
            except Exception as e:
                final_status = TaskStatus.ENDED_FAILED
                error_message = str(e)
                logger.exception('Task FAILED!')

        final_metrics = self._ctx.metrics.finalize(success=(final_status == TaskStatus.ENDED_SUCCESS))
        base_attributes = BaseTaskResult(
            task_index=self._task_attempt_id.task_index,
            status=final_status,
            error_message=error_message,
            metrics=final_metrics).shallowdict(include_none=True)
        result = self._build_result(base_attributes)

        # TODO Later have an optional secondary channel to report failures,
        #  when there is no attempt ID / no datastore connected
        if self._task_attempt_id:
            self._ctx.datastore.write_task_result(self._req.request_id, self._task_attempt_id, result)
        else:
            logger.error("Can't report result: no part was selected for loading")

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(result)
        return result

    def _update_status(self, status: TaskStatus):
        self._ctx.datastore.update_task_status(self._req.request_id, self._task_attempt_id, status)

    @abstractmethod
    def _do_run(self):
        pass

    # TODO doc: this is expected to still work even if _do_run failed
    @abstractmethod
    def _build_result(self, base_attributes: dict):
        pass
