import time
from contextlib import contextmanager
from typing import Type, List, cast
from frocket.common.helpers.utils import timestamped_uuid
from frocket.common.metrics import MetricsBag, ComponentLabel
from frocket.common.tasks.base import BaseTaskRequest, BaseTaskResult, BaseJobResult, TaskStatus, TaskAttemptsInfo, \
    JobStatus
from frocket.common.tasks.registration import RegistrationTaskRequest, RegistrationTaskResult, RegistrationJobResult, \
    DatasetValidationMode, REGISTER_DEFAULT_FILENAME_PATTERN, RegisterArgs
from frocket.datastore.registered_datastores import get_datastore
from frocket.invoker.jobs.job_builder import JobBuilder
from frocket.invoker.jobs.registration_job_builder import RegistrationJobBuilder
from frocket.worker.impl.generic_env_metrics import GenericEnvMetricsProvider
from frocket.worker.runners.base_task_runner import TaskRunnerContext
from frocket.worker.runners.registered_runners import REGISTERED_RUNNERS
# noinspection PyUnresolvedReferences
from tests.redis_fixture import init_redis


def _simple_task_runner_ctx() -> TaskRunnerContext:
    return TaskRunnerContext(metrics=MetricsBag(component=ComponentLabel.WORKER,
                                                env_metrics_provider=GenericEnvMetricsProvider()))


def simple_run_task(req: BaseTaskRequest, expected_resultcls: Type[BaseTaskResult]) -> BaseTaskResult:
    ctx = _simple_task_runner_ctx()
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

    def run_all_stages(self) -> BaseJobResult:
        self.prerun()
        self.enqueue_tasks()
        self.run_tasks()
        self.complete()
        return self.build_result()

    def cleanup(self):
        get_datastore().cleanup_request_data(self._job.request_id)


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


@contextmanager
def registration_job_invoker(job: RegistrationJobBuilder) -> InprocessInvokerAndWorker:
    invoker_runner = InprocessInvokerAndWorker(
        job, RegistrationTaskRequest, RegistrationTaskResult, RegistrationJobResult)
    try:
        yield invoker_runner
    finally:
        invoker_runner.cleanup()
