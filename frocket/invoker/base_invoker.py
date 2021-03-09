import logging
from abc import abstractmethod, ABCMeta
from typing import Dict, Counter, List, NamedTuple, Optional
from frocket.common.metrics import MetricsBag, ComponentLabel, MetricName, MetricData, SourceAndMetricTuple, LabelsDict
from frocket.common.tasks.base import TaskAttemptId, TaskStatus, BaseTaskResult, \
    BaseTaskRequest, BaseJobResult, JobStatus, JobStats
from frocket.common.tasks.async_tracker import AsyncJobStatusUpdater, AsyncJobStage
from frocket.common.helpers.utils import timestamped_uuid
from frocket.datastore.registered_datastores import get_datastore
from frocket.invoker.jobs.job import Job
from frocket.invoker.metrics_frame import MetricsFrame
from frocket.invoker.stats_builder import build_stats
from frocket.worker.impl.generic_env_metrics import GenericEnvMetricsProvider

logger = logging.getLogger(__name__)


class BaseInvoker(metaclass=ABCMeta):
    """
    BaseInvoker defined the basic logic of invoking jobs using the concrete Job subclass it's initialized with.

    However, this class does not contain the logic for how to actually 'get workers to do stuff' and collect results,
    which can possibly be done in various ways. That part is handled by AsyncInvoker and its own subclasses
    (WorkQueueInvoker, AwsLambdaInvoker, and future ones). It is ofc possible to also implement a SyncInvoker subclass
    (and as its subclasses have either regular Lambda invocation, calls to HTTP APIs, etc.)

    See also the Job class documentation for the workflow from the job's perspective.
    """
    def __init__(self, job: Job):
        self._job = job
        # Generate a new unique request ID and also hand it to the job instance
        self._request_id = self.new_request_id()
        self._job.request_id = self._request_id
        self._datastore = get_datastore()
        self._metrics = MetricsBag(component=ComponentLabel.INVOKER,
                                   env_metrics_provider=GenericEnvMetricsProvider())

    # noinspection PyBroadException
    def run(self, async_status_updater: AsyncJobStatusUpdater = None) -> BaseJobResult:
        try:
            # The run itself
            with self._metrics.measure(MetricName.INVOKER_TOTAL_SECONDS):
                final_job_status, all_task_results, latest_task_results = \
                    self._run_to_completion(async_status_updater)

            # Post-run: collect metrics, build job result
            job_labels = self._job.metric_labels
            final_metrics = self._metrics.finalize(success=final_job_status.success,
                                                   added_labels=job_labels)
            # Note that metrics are collected for all task attempts, successful and failed
            metrics_frame = self.__build_metrics(job_labels=job_labels,
                                                 invoker_metrics=final_metrics,
                                                 all_task_results=all_task_results)
            run_result = self.__build_run_result(final_job_status=final_job_status,
                                                 latest_task_results=latest_task_results,
                                                 metrics_frame=metrics_frame)
            metrics_frame.export()
        except Exception:
            try:
                final_job_status = JobStatus(success=False, error_message='Unexpected error', attempts_status=[])
                run_result = self.__build_run_result(final_job_status=final_job_status,
                                                     latest_task_results=[],
                                                     metrics_frame=None)
                logger.exception('Run failed')
            except Exception:
                logger.exception('Failed to set a "success=False" result via the job builder')
                run_result = BaseJobResult(success=False, error_message='Unexpected error',
                                           request_id=self._request_id, stats=JobStats())

        # In case some lost tasks are still running, they may still "resurrect" some keys with their own data -
        # though the job is done as far as the invoker is concerned.
        # TODO backlog periodic "old junk" cleaner (can look for outdated request IDs, which are timestamped)
        self._datastore.cleanup_request_data(self._request_id)

        if async_status_updater:
            async_status_updater.done(run_result)
        return run_result

    class RunCompletionResult(NamedTuple):
        final_job_status: JobStatus
        all_task_results: Dict[TaskAttemptId, BaseTaskResult] = {}
        latest_task_results: List[BaseTaskResult] = []

    def _run_to_completion(self, async_status_updater) -> RunCompletionResult:
        # First step: call the job to run any preparation code which needs to run invoker-side,
        # might take a bit of time and might also fail (e.g. dataset files discovery)
        try:
            error_message = self._job.prerun(async_status_updater)
        except Exception as e:
            logger.exception(f"Prerun failed: {e}")
            error_message = str(e)

        if error_message:  # Failed already at pre-run, before any tasks were invoked
            final_job_status = JobStatus(success=False, error_message=error_message, attempts_status=[])
            return self.RunCompletionResult(final_job_status)

        # Second step: build tasks, run them till done successfully or retries exhausted, and gather results

        task_requests = self.__build_initial_tasks()
        if async_status_updater:
            async_status_updater.update(stage=AsyncJobStage.RUNNING)
        job_status = self._do_run(task_requests, async_status_updater)  # Calls subclass!
        all_task_results = self._datastore.task_results(self._request_id)  # Read results

        # Third step: finishing up: sanity check, and call job again now that results are in.
        # The job can now process and/or validate task results taken together, and may still fail the run.

        if async_status_updater:
            async_status_updater.update(stage=AsyncJobStage.FINISHING)

        latest_task_results = self.latest_results_only(job_status, all_task_results)
        if job_status.success:
            successful_tasks = [tr for tr in latest_task_results if tr.status == TaskStatus.ENDED_SUCCESS]
            assert (len(successful_tasks) == self._job.total_tasks())

        final_job_status = self._job.complete(job_status, latest_task_results, async_status_updater)
        return self.RunCompletionResult(final_job_status=final_job_status,
                                        all_task_results=all_task_results,
                                        latest_task_results=latest_task_results)

    @abstractmethod
    def _do_run(self,
                task_requests: List[BaseTaskRequest],
                async_status_updater: AsyncJobStatusUpdater = None) -> JobStatus:
        """Actually invoke the tasks and return with a status when they are complete."""
        pass

    def __build_initial_tasks(self) -> List[BaseTaskRequest]:
        requests = self._job.build_tasks()
        # If relevant, publish dataset parts for self-selection by workers
        parts_to_publish = self._job.dataset_parts_to_publish()
        if parts_to_publish:
            assert all([req.invoker_set_task_index is None for req in requests])

        # All tasks' status is always set to QUEUED before actually invoking them
        attempt_ids = [TaskAttemptId(task_index=req.invoker_set_task_index or i) for i, req in enumerate(requests)]
        self._datastore.update_task_status(self._request_id, attempt_ids, TaskStatus.QUEUED)

        if parts_to_publish:
            self._datastore.publish_for_worker_selection(self._request_id,
                                                         parts=parts_to_publish,
                                                         attempt_round=0)

        return requests

    def _build_retry_task(self, task_index) -> (TaskAttemptId, BaseTaskRequest):
        attempt_no = self._datastore.increment_attempt(self._request_id, task_index)
        attempt_id = TaskAttemptId(task_index, attempt_no)

        req = self._job.build_retry_task(attempt_no, task_index)
        self._datastore.update_task_status(self._request_id, attempt_id, TaskStatus.QUEUED)
        return req

    def __build_run_result(self,
                           final_job_status: JobStatus,
                           latest_task_results: List[BaseTaskResult],
                           metrics_frame: Optional[MetricsFrame]) -> BaseJobResult:

        # Collect errors from invoker and tasks (only from latest attempts)
        full_error_message = final_job_status.error_message
        task_errors: List[str] = [res.error_message for res in latest_task_results if res.error_message]
        if len(task_errors) > 0:
            full_error_message = f"{full_error_message or 'No errors in invoker'}. Task errors: {task_errors}"
        stats = build_stats(metrics_frame, self._job.parts_info()) if metrics_frame else None

        # Collect the common 'base' attributes and pass them to the job, to use as **args in init'ing the result object
        base_attributes = BaseJobResult(
            request_id=self._request_id,
            success=final_job_status.success,
            error_message=full_error_message,
            stats=stats).\
            shallowdict(include_none=True)

        run_result = self._job.build_result(
            base_attributes, final_job_status, latest_task_results)
        return run_result

    @staticmethod
    def __build_metrics(job_labels: LabelsDict,
                        invoker_metrics: List[MetricData],
                        all_task_results: Dict[TaskAttemptId, BaseTaskResult]) -> MetricsFrame:
        """Build a unified list of all reported metrics from the invoker and all tasks."""
        all_metrics: List[SourceAndMetricTuple] = \
            [SourceAndMetricTuple(source='invoker', metric=m) for m in invoker_metrics]

        for attempt_id, task_result in all_task_results.items():
            source_name = f"task-{attempt_id.task_index}-{attempt_id.attempt_no}"
            all_metrics += [SourceAndMetricTuple(source=source_name, metric=m.with_added_labels(job_labels))
                            for m in task_result.metrics]

        return MetricsFrame(all_metrics)

    @classmethod
    def latest_results_only(cls, job_status: JobStatus,
                            all_task_results: Dict[TaskAttemptId, BaseTaskResult]) -> List[BaseTaskResult]:
        """Get tasks results only for the single latest attempt per each task."""
        latest_attempts = [ts.latest_attempt for ts in job_status.attempts_status]
        results = [result for attempt_id, result in all_task_results.items()
                   if attempt_id in latest_attempts]
        # Prevent possible erratic behavior down the line by making order predictable (by task index)
        return sorted(results, key=lambda result: result.task_index)

    @classmethod
    def new_request_id(cls) -> str:
        return timestamped_uuid()

    @classmethod
    def simple_status_string(cls, status_counts: Counter[TaskStatus]) -> str:
        return ', '.join([f"{status.name}: {count}"
                          for status, count in status_counts.items()])
