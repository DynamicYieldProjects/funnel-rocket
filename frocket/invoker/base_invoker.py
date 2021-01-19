import logging
from abc import abstractmethod
from typing import Dict, Counter, List, NamedTuple
from frocket.common.metrics import MetricsBag, ComponentLabel, MetricName, MetricData, SourceAndMetricTuple, LabelsDict
from frocket.common.tasks.base import TaskAttemptId, TaskStatus, BaseTaskResult, \
    BaseTaskRequest, BaseJobResult, JobStatus
from frocket.common.tasks.async_tracker import AsyncJobStatusUpdater, AsyncJobStage
from frocket.common.helpers.utils import timestamped_uuid
from frocket.datastore.registered_datastores import get_datastore
from frocket.invoker.jobs.job_builder import JobBuilder
from frocket.invoker.metrics_frame import MetricsFrame
from frocket.worker.impl.generic_env_metrics import GenericEnvMetricsProvider

logger = logging.getLogger(__name__)


class BaseInvoker:
    def __init__(self, job_builder: JobBuilder):
        self._job_builder = job_builder
        self._request_id = self.new_request_id()
        self._job_builder.request_id = self._request_id
        self._datastore = get_datastore()
        self._metrics = MetricsBag(component=ComponentLabel.INVOKER,
                                   env_metrics_provider=GenericEnvMetricsProvider())

    # noinspection PyBroadException
    def run(self, async_status_updater: AsyncJobStatusUpdater = None) -> BaseJobResult:
        try:
            with self._metrics.measure(MetricName.INVOKER_TOTAL_SECONDS):
                final_job_status, all_task_results, latest_task_results = \
                    self._run_to_completion(async_status_updater)

            job_labels = self._job_builder.metric_labels
            final_metrics = self._metrics.finalize(success=final_job_status.success,
                                                   added_labels=job_labels)
            # Note that metrics are collected for all task attempts, successful and failed
            metrics_frame = self.__build_metrics(job_labels=job_labels,
                                                 invoker_metrics=final_metrics,
                                                 all_task_results=all_task_results)
            run_result = self.__build_run_result(final_job_status=final_job_status,
                                                 final_metrics=final_metrics,
                                                 latest_task_results=latest_task_results,
                                                 cost=metrics_frame.total_cost())
            metrics_frame.export()
        except Exception:
            try:
                final_job_status = JobStatus(success=False, error_message='Unexpected error', attempts_status=[])
                run_result = self.__build_run_result(final_job_status=final_job_status,
                                                     final_metrics=[], latest_task_results=[])
                logger.exception('Run failed')
            except Exception:
                logger.exception('Failed to a "success=False" result via the job builder')
                run_result = BaseJobResult(success=False, error_message='Unexpected error',
                                           request_id=self._request_id, task_counters={}, metrics=[], cost=None)

        # In case some lost tasks are still running, they may still "resurrect" some keys with their own data -
        # though the job is done as far as the invoker is concerned.
        # TODO look for any "old junk" to periodically prune, based on its timestamped request IDs
        self._datastore.cleanup_request_data(self._request_id)

        if async_status_updater:
            async_status_updater.done(run_result)
        return run_result

    class RunCompletionResult(NamedTuple):
        final_job_status: JobStatus
        all_task_results: Dict[TaskAttemptId, BaseTaskResult] = {}
        latest_task_results: List[BaseTaskResult] = []

    def _run_to_completion(self, async_status_updater) -> RunCompletionResult:
        # First step: call the job builder to run any preparation code which needs to run invoker-side,
        # and might take a bit of time or fail
        try:
            error_message = self._job_builder.prerun(async_status_updater)
        except Exception as e:
            logger.exception(f"Prerun failed: {e}")
            error_message = str(e)

        if error_message:
            final_job_status = JobStatus(success=False, error_message=error_message, attempts_status=[])
            return self.RunCompletionResult(final_job_status)

        # Second step: build tasks, run them till done successfully or retries exhausted, and gather results

        task_requests = self.__build_initial_tasks()
        if async_status_updater:
            async_status_updater.update(stage=AsyncJobStage.RUNNING)
        job_status = self._do_run(task_requests, async_status_updater)
        all_task_results = self._datastore.task_results(self._request_id)

        # Third step: finishing up: sanity check, and call job builder again now that results are in
        # The job builder can now process and/or validate task results taken together, and may still fail the run.

        if async_status_updater:
            async_status_updater.update(stage=AsyncJobStage.FINISHING)

        latest_task_results = self._latest_results_only(job_status, all_task_results)
        if job_status.success:
            successful_tasks = [tr for tr in latest_task_results if tr.status == TaskStatus.ENDED_SUCCESS]
            assert (len(successful_tasks) == self._job_builder.total_tasks())

        final_job_status = self._job_builder.complete(job_status, latest_task_results, async_status_updater)
        return self.RunCompletionResult(final_job_status, all_task_results, latest_task_results)

    @abstractmethod
    def _do_run(self,
                task_requests: Dict[TaskAttemptId, BaseTaskRequest],
                async_status_updater: AsyncJobStatusUpdater = None) -> JobStatus:
        pass

    def __build_initial_tasks(self) -> Dict[TaskAttemptId, BaseTaskRequest]:
        requests = self._job_builder.build_tasks()
        request_attempts = {TaskAttemptId(task_index=req.task_index): req for req in requests}

        self._datastore.update_task_status(self._request_id, list(request_attempts.keys()), TaskStatus.QUEUED)

        parts_to_publish = self._job_builder.dataset_parts_to_publish()
        if parts_to_publish:
            self._datastore.publish_for_worker_selection(self._request_id,
                                                         parts=parts_to_publish,
                                                         attempt_round=0)

        return request_attempts

    def _build_retry_task(self, task_index) -> (TaskAttemptId, BaseTaskRequest):
        attempt_no = self._datastore.increment_attempt(self._request_id, task_index)
        attempt_id = TaskAttemptId(task_index, attempt_no)

        req = self._job_builder.build_retry_task(attempt_no, task_index)
        self._datastore.update_task_status(self._request_id, attempt_id, TaskStatus.QUEUED)
        return req

    def __build_run_result(self,
                           final_job_status: JobStatus,
                           final_metrics: List[MetricData],
                           latest_task_results: List[BaseTaskResult],
                           cost: float = None) -> BaseJobResult:

        # Collect errors from invoker and tasks (only from latest attempts, not from recovered attempts)
        full_error_message = final_job_status.error_message
        task_errors: List[str] = [res.error_message for res in latest_task_results if res.error_message]
        if len(task_errors) > 0:
            full_error_message = f"{full_error_message or 'No errors in invoker'}. Task errors: {task_errors}"

        # Count final known status of all task attempts
        task_counters = Counter[str]()
        for at in final_job_status.attempts_status:
            status_names = [task_update.status.name for task_update in at.attempts.values()]
            task_counters.update(status_names)

        base_attributes = BaseJobResult(
            request_id=self._request_id,
            success=final_job_status.success,
            error_message=full_error_message,
            task_counters=task_counters,
            metrics=final_metrics,
            cost=cost). \
            shallowdict(include_none=True)

        run_result = self._job_builder.build_result(
            base_attributes, final_job_status, latest_task_results)
        return run_result

    @staticmethod
    def __build_metrics(job_labels: LabelsDict,
                        invoker_metrics: List[MetricData],
                        all_task_results: Dict[TaskAttemptId, BaseTaskResult]) -> MetricsFrame:
        all_metrics: List[SourceAndMetricTuple] = \
            [SourceAndMetricTuple(source='invoker', metric=m) for m in invoker_metrics]

        for attempt_id, task_result in all_task_results.items():
            source_name = f"task-{attempt_id.task_index}-{attempt_id.attempt_no}"
            all_metrics += [SourceAndMetricTuple(source=source_name, metric=m.with_added_labels(job_labels))
                            for m in task_result.metrics]

        return MetricsFrame(all_metrics)

    # Get results only for the single latest attempt per each task
    @classmethod
    def _latest_results_only(cls, job_status: JobStatus,
                             all_task_results: Dict[TaskAttemptId, BaseTaskResult]) -> List[BaseTaskResult]:

        latest_attempts = [ts.latest_attempt for ts in job_status.attempts_status]
        results = [result for attempt_id, result in all_task_results.items()
                   if attempt_id in latest_attempts]
        # Prevent possible erratic behavior down the line by making order predictable
        return sorted(results, key=lambda result: result.task_index)

    @classmethod
    def new_request_id(cls) -> str:
        return timestamped_uuid()

    @classmethod
    def simple_status_string(cls, status_counts: Counter[TaskStatus]) -> str:
        return ', '.join([f"{status.name}: {count}"
                          for status, count in status_counts.items()])
