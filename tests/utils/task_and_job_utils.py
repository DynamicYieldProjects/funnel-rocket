import time
from contextlib import contextmanager
from typing import Type, List, cast, Optional
from frocket.common.dataset import DatasetInfo, DatasetPartsInfo, DatasetShortSchema
from frocket.common.helpers.utils import timestamped_uuid
from frocket.common.metrics import MetricsBag, ComponentLabel
from frocket.common.tasks.base import BaseTaskRequest, BaseTaskResult, BaseJobResult, TaskStatus, TaskAttemptsInfo, \
    JobStatus, TaskAttemptId, JobStats
from frocket.common.tasks.query import QueryTaskResult, QueryTaskRequest, QueryJobResult
from frocket.common.tasks.registration import RegistrationTaskRequest, RegistrationTaskResult, RegistrationJobResult, \
    DatasetValidationMode, REGISTER_DEFAULT_FILENAME_PATTERN, RegisterArgs
from frocket.common.validation.query_validator import QueryValidator
from frocket.datastore.registered_datastores import get_datastore
from frocket.invoker.base_invoker import BaseInvoker
from frocket.invoker.jobs.job import Job
from frocket.invoker.jobs.query_job import QueryJob
from frocket.invoker.jobs.registration_job import RegistrationJob
from frocket.worker.impl.generic_env_metrics import GenericEnvMetricsProvider
from frocket.worker.runners.base_task_runner import TaskRunnerContext
from frocket.worker.runners.part_loader import PartLoader
from frocket.worker.runners.registered_runners import REGISTERED_RUNNERS
from tests.utils.dataset_utils import DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN, TestDatasetInfo
# noinspection PyUnresolvedReferences
from tests.utils.redis_fixture import init_test_redis_settings


def simple_run_task(req: BaseTaskRequest,
                    expected_resultcls: Type[BaseTaskResult],
                    private_part_loader: PartLoader = None,
                    preflight_duration_ms: int = None) -> BaseTaskResult:
    ctx = TaskRunnerContext(metrics=MetricsBag(component=ComponentLabel.WORKER,
                                               env_metrics_provider=GenericEnvMetricsProvider()),
                            private_part_loader=private_part_loader,
                            preflight_duration_ms=preflight_duration_ms)
    task_runner = REGISTERED_RUNNERS[type(req)](req, ctx)
    result = task_runner.run()
    assert type(result) == expected_resultcls
    return result


class InprocessInvokerAndWorker:
    def __init__(self, job: Job,
                 taskreq_cls: Type[BaseTaskRequest],
                 taskres_cls: Type[BaseTaskResult],
                 jobres_cls: Type[BaseJobResult]):
        self._job = job
        self._taskreq_cls = taskreq_cls
        self._taskres_cls = taskres_cls
        self._jobres_cls = jobres_cls
        self._queue_name = timestamped_uuid('queue-')
        self.num_tasks: Optional[int] = None
        self._job_status: Optional[JobStatus] = None
        self._all_task_results: Optional[List[BaseTaskResult]] = None
        self._latest_task_results: Optional[List[BaseTaskResult]] = None
        self._final_job_status: Optional[JobStatus] = None

    def prerun(self):
        self._job.prerun()

    def enqueue_tasks(self) -> List[BaseTaskRequest]:
        requests = cast(List[self._taskreq_cls], self._job.build_tasks())
        self.num_tasks = len(requests)
        parts_to_publish = self._job.dataset_parts_to_publish()
        if parts_to_publish:
            get_datastore().publish_for_worker_selection(reqid=self._job.request_id,
                                                         attempt_round=0,
                                                         parts=parts_to_publish)
        get_datastore().enqueue(requests, queue=self._queue_name)
        return requests

    def run_tasks(self):
        return self._run_tasks(self.num_tasks)

    def _run_tasks(self, expected_task_count: int):
        for i in range(expected_task_count):
            req = get_datastore().dequeue(queue=self._queue_name, timeout=1)
            assert req
            result = simple_run_task(req, self._taskres_cls)
            assert result.status == TaskStatus.ENDED_SUCCESS
        assert get_datastore().dequeue(queue=self._queue_name, timeout=0) is None  # Queue depleted

        all_attempts = get_datastore().task_results(self._job.request_id)
        assert len(all_attempts) >= self.num_tasks
        self._all_task_results = cast(List[self._taskres_cls], list(all_attempts.values()))
        assert all([res.status == TaskStatus.ENDED_SUCCESS for res in self._all_task_results])

        tasks_status = get_datastore().tasks_status(self._job.request_id)
        attempts_status = [TaskAttemptsInfo(i) for i in range(self.num_tasks)]
        for at, update in tasks_status.items():
            attempts_status[at.task_index].add(at, update)
        assert all([tat.latest_update.status == TaskStatus.ENDED_SUCCESS for tat in attempts_status])

        self._job_status = JobStatus(success=True, error_message=None, attempts_status=attempts_status)
        self._latest_task_results = BaseInvoker.latest_results_only(self._job_status, all_attempts)
        return self._all_task_results

    def retry(self, task_index) -> List[BaseTaskResult]:
        attempt_no = get_datastore().increment_attempt(self._job.request_id, task_index)
        attempt_id = TaskAttemptId(task_index, attempt_no)

        req = self._job.build_retry_task(attempt_no, task_index)
        get_datastore().enqueue([req], queue=self._queue_name)
        results = self._run_tasks(expected_task_count=1)
        return results

    def complete(self, assert_success: bool = True) -> JobStatus:
        self._final_job_status = self._job.complete(self._job_status, latest_task_results=self._latest_task_results)
        if assert_success:
            assert self._final_job_status.success and not self._final_job_status.error_message
        return self._final_job_status

    def build_result(self, assert_success: bool = True) -> BaseJobResult:
        base_attributes = BaseJobResult(
            request_id=self._job.request_id,
            success=self._final_job_status.success,
            error_message=self._final_job_status.error_message,
            stats=JobStats()). \
            shallowdict(include_none=True)

        job_result = self._job.build_result(base_attributes, self._final_job_status, self._latest_task_results)
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
                           group_id_column: str = DEFAULT_GROUP_COLUMN,
                           pattern: str = REGISTER_DEFAULT_FILENAME_PATTERN,
                           uniques: bool = True) -> RegistrationJob:
    args = RegisterArgs(
        name=f"test-{basepath}-{time.time()}",
        basepath=basepath,
        group_id_column=group_id_column,
        timestamp_column=DEFAULT_TIMESTAMP_COLUMN,
        pattern=pattern,
        validation_mode=mode,
        validate_uniques=uniques)
    job = RegistrationJob(args=args)
    job.request_id = timestamped_uuid('test-reg-')
    return job


@contextmanager
def registration_job(test_ds: TestDatasetInfo,
                     mode: DatasetValidationMode,
                     group_id_column: str = DEFAULT_GROUP_COLUMN,
                     pattern: str = REGISTER_DEFAULT_FILENAME_PATTERN,
                     uniques: bool = True) -> RegistrationJob:
    job = build_registration_job(test_ds.basepath, mode, group_id_column, pattern, uniques)
    try:
        yield job
    finally:
        if job.dataset:
            get_datastore().remove_dataset_info(job.dataset.id.name)


def build_query_job(query: dict,
                    ds: DatasetInfo,
                    parts: DatasetPartsInfo,
                    short_schema: DatasetShortSchema,
                    worker_can_select_part: bool = None) -> QueryJob:
    validation_result = QueryValidator(query, ds, short_schema).expand_and_validate()
    job = QueryJob(ds, parts, short_schema,
                   query=validation_result.expanded_query,
                   used_columns=validation_result.used_columns,
                   worker_can_select_part=worker_can_select_part)
    job.request_id = timestamped_uuid('test-query-')
    return job


@contextmanager
def registration_job_invoker(job: RegistrationJob) -> InprocessInvokerAndWorker:
    invoker_runner = InprocessInvokerAndWorker(
        job, RegistrationTaskRequest, RegistrationTaskResult, RegistrationJobResult)
    try:
        yield invoker_runner
    finally:
        invoker_runner.cleanup()


@contextmanager
def query_job_invoker(job: QueryJob) -> InprocessInvokerAndWorker:
    invoker_runner = InprocessInvokerAndWorker(
        job, QueryTaskRequest, QueryTaskResult, QueryJobResult)
    try:
        yield invoker_runner
    finally:
        invoker_runner.cleanup()
