import logging
import time
from time import sleep
from typing import List
from abc import abstractmethod
from collections import Counter
from frocket.common.config import config
from frocket.common.metrics import MetricName
from frocket.common.tasks.base import BaseTaskRequest, TaskStatus, TaskAttemptsInfo
from frocket.common.tasks.async_tracker import AsyncJobStatusUpdater
from frocket.invoker.base_invoker import BaseInvoker, JobStatus
from frocket.invoker.jobs.job import Job

logger = logging.getLogger(__name__)


class AsyncInvoker(BaseInvoker):
    """
    Subclassing BaseInvoker to implement an async. invocation:
    first, enqueue tasks; then, poll waiting for successful completion. Handle retries according to configuration, and
    fail on timeout if necessary.
    NOTE: this class is also abstract, as it doesn't handle how tasks are actually enqueued. See concrete subclasses
    WorkQueueInvoker and AwsLambdaInvoker.
    """
    def __init__(self, job: Job):
        super().__init__(job)
        self._run_timeout_seconds = config.int("invoker.run.timeout")
        self._poll_interval_seconds = config.int("invoker.async.poll.interval.ms") / 1000
        self._log_interval_seconds = config.int("invoker.async.log.interval.ms") / 1000
        self._max_attempts = config.int("invoker.retry.max.attempts")
        self._retry_failed_interval = config.float("invoker.retry.failed.interval")
        self._retry_lost_interval = config.int("invoker.retry.lost.interval")

    def _do_run(self, task_requests: List[BaseTaskRequest],
                async_status_updater: AsyncJobStatusUpdater = None):
        """Implementing abstract BaseInvoker._do_run() as a dual-stage process: enqueue, then poll."""
        try:
            with self._metrics.measure(MetricName.ASYNC_ENQUEUE_SECONDS):
                if async_status_updater:
                    async_status_updater.update(message="Enqueuing tasks")
                self._enqueue(task_requests)
                logger.info(f"Enqueued {len(task_requests)} tasks")

            with self._metrics.measure(MetricName.ASYNC_POLL_SECONDS):
                if async_status_updater:
                    async_status_updater.update(message="Polling results")
                raw_result = self._poll(async_status_updater)
        except Exception as e:
            logger.error(str(e))
            raw_result = JobStatus(success=False, error_message=str(e), attempts_status=[])
        return raw_result

    @abstractmethod
    def _enqueue(self, requests: List[BaseTaskRequest]) -> None:
        """Actual invocation method left to subclasses"""
        pass

    def _poll(self, async_status_updater: AsyncJobStatusUpdater = None) -> JobStatus:
        """Once tasks were enqueued, the polling mechanism is shared to all subclasses: status is read from the
        datastore and analyzed."""
        total_tasks = self._job.total_tasks()
        poll_start_time = time.time()
        last_log_time = poll_start_time
        error_message = None

        while True:
            # TODO backlog to prevent the small-ish drift, sleep only the remaining time to the next 'tick'
            sleep(self._poll_interval_seconds)
            poll_running_time = time.time() - poll_start_time

            # Fetch current state - for each task index, take it latest attempt only (if more than one)
            attempts_status = self._get_attempts_status()
            latest_status_values = [at.latest_update.status for at in attempts_status]
            status_counts = Counter(latest_status_values)  # Counts instances by TaskStatus member
            parts_ended_ok = status_counts[TaskStatus.ENDED_SUCCESS]

            if async_status_updater:
                async_status_updater.update(task_counters=status_counts)

            # End polling if all done, or timed out
            if parts_ended_ok == total_tasks:
                break  # All done!
            elif poll_running_time > self._run_timeout_seconds:
                error_message = f"Query timed out after {poll_running_time:.2f} seconds"
                break

            # Periodically log progress
            should_log_progress = time.time() - last_log_time >= self._log_interval_seconds
            if should_log_progress:
                status_string = self.simple_status_string(status_counts)
                percent_done = parts_ended_ok / total_tasks * 100
                logger.info(f"{percent_done:.1f}% done in {poll_running_time:.2f}s... {status_string}")
                last_log_time = time.time()

            # Handling retries for failed and seemingly-lost tasks
            # If any failed task reached its max attempts, fail fast without trying to recover other tasks
            no_retry_failures = [at for at in attempts_status
                                 if at.latest_update.status == TaskStatus.ENDED_FAILED
                                 and at.attempt_count >= self._max_attempts]

            if len(no_retry_failures) > 0:
                error_message = f"No more retries for task(s): {[at.task_index for at in no_retry_failures]}, failing"
                break

            retries = self._build_retries(attempts_status, should_log_progress)
            if len(retries) > 0:
                self._enqueue(retries)
                logger.debug(f"Enqueued retry attempts: {retries}")

        # Poll loop has exited, return final state
        parts_failed = total_tasks - parts_ended_ok
        success = (error_message is None and parts_failed == 0)
        return JobStatus(success, error_message, attempts_status)

    def _build_retries(self,
                       attempts_status: List[TaskAttemptsInfo],
                       should_log_progress: bool) -> List[BaseTaskRequest]:
        retries = []
        failures = [at for at in attempts_status if at.latest_update.status == TaskStatus.ENDED_FAILED]

        if len(failures) > 0:
            for at in failures:
                if at.time_since_update >= self._retry_failed_interval:
                    # This already initializes a QUEUED status for the new attempt, so visible to next poll
                    retries.append(self._build_retry_task(at.task_index))
                    logger.info(f"Retrying task {at.task_index}, last failure {at.time_since_update}s ago")
                else:
                    if should_log_progress:
                        logger.info(f"Failed task {at.task_index} pending retry, failed {at.time_since_update}s ago")

        lost_attempts = [at for at in attempts_status
                         if (not at.latest_update.status.ended)
                         and at.time_since_update >= self._retry_lost_interval]

        if len(lost_attempts) > 0:
            for at in lost_attempts:
                if at.attempt_count < self._max_attempts:
                    # This already initializes a QUEUED status for the new attempt, so visible to next poll
                    retries.append(self._build_retry_task(at.task_index))
                    logger.info(f"Retrying task {at.task_index} considered lost/stuck. Last status was "
                                f"{at.latest_update.status} at {at.time_since_update} seconds ago")
                else:
                    if should_log_progress:
                        logger.info(f"No more retries for lost task {at.task_index}. May it recover before timeout!")

        return retries

    def _get_attempts_status(self) -> List[TaskAttemptsInfo]:
        # Read status of ALL attempts, and init a list of per-task attempt summaries
        tasks_status = self._datastore.tasks_status(self._request_id)

        attempts_status = [TaskAttemptsInfo(task_index) for task_index in range(self._job.total_tasks())]
        for attempt_id, status_update in tasks_status.items():
            attempts_status[attempt_id.task_index].add(attempt_id, status_update)

        # All task should have been attempted at least once, with QUEUED status or above
        count_zero_attempts = sum(1 for ts in attempts_status if ts.attempt_count == 0)
        if count_zero_attempts > 0:
            raise Exception("Some tasks have no attempt at all!")

        return attempts_status
