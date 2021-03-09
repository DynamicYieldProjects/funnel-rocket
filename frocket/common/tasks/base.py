"""
Base types for all concrete task and job classes
"""
import logging
import time
from enum import auto
from typing import Optional, Dict, List, NamedTuple
from dataclasses import dataclass, field
from frocket.common.metrics import MetricData
from frocket.common.serializable import SerializableDataClass, AutoNamedEnum

logger = logging.getLogger(__name__)

ErrorMessage = str
BlobId = str


@dataclass(frozen=True)
class BaseTaskRequest(SerializableDataClass):
    """
    All task requests (enqueud by the invoker, processed by a worker) inherit from this class.

    There are two main modes of request:
    1. Either the invoker explicitly specifies a task_index and all the details of the task to run
       (as attributes of the concrete task request class), OR:
    2. (For jobs which support it, where it makes sense to do)
       The invoker publishes a set of tasks to do in the datastore, and then launches the appropriate amount of task
       requests - but it's then the worker role to choose the task to perform. This gives workers a chance
       to choose work over files they already have cached, or fallback to any remaining task.
       See: query_task_runner.py
    """
    request_id: str  # Unique ID for storing all request/response related payloads in the datastore
    invoke_time: float
    attempt_no: int
    invoker_set_task_index: Optional[int]


class TaskStatus(AutoNamedEnum):
    def __init__(self, *args):
        self.ended = args[0].startswith('ENDED_')

    QUEUED = auto()
    LOADING_DATA = auto()
    RUNNING_QUERY = auto()
    RUNNING = auto()  # Generic status for non-query tasks
    ENDED_SUCCESS = auto()
    ENDED_FAILED = auto()


@dataclass(frozen=True)
class BaseTaskResult(SerializableDataClass):
    task_index: int
    status: TaskStatus
    error_message: Optional[str]  # Only (possibly) available if status is ENDED_FAILED
    metrics: List[MetricData]


@dataclass(frozen=True)
class TaskAttemptId(SerializableDataClass):
    """Represents a single attempt at running a specific task_index."""
    task_index: int
    attempt_no: int = 0


@dataclass(frozen=True)
class TaskStatusUpdate(SerializableDataClass):
    """Couple a status change with when it was set."""
    status: TaskStatus
    time: float

    @classmethod
    def now(cls, status):
        return TaskStatusUpdate(status, time.time())


class TaskAttemptsInfo:
    """
    A clumsily-named class used by the invoker to determine the latest status a given task_index.

    The invoker reads all attempt statuses from the datastore, and holds an instance of this object
    for each task_index, calling add() for all attempts for it (hopefully, just one).
    """
    class AttemptAndUpdate(NamedTuple):
        attempt_id: TaskAttemptId
        update: TaskStatusUpdate

    def __init__(self, task_index: int):
        self._task_index = task_index
        self._attempts: Dict[TaskAttemptId, TaskAttemptsInfo.AttemptAndUpdate] = {}
        self._latest: Optional[TaskAttemptsInfo.AttemptAndUpdate] = None

    def add(self, attempt_id: TaskAttemptId, update: TaskStatusUpdate) -> None:
        assert (attempt_id.task_index == self._task_index)
        self._attempts[attempt_id] = TaskAttemptsInfo.AttemptAndUpdate(attempt_id, update)
        self._latest = None  # Invalidate

    def _update_latest_attempt(self):
        """
        Determine latest attempt:

        1. if there is an attempt with ENDED_SUCCESS status - take the earliest attempt with that status.
           Note it IS possible to have more than one, in case an attempt was judged "lost" and another attempt launched.
           If then the first attempt manages to complete - we may have >1 attempts with ENDED_SUCCESS status by the
           point where all tasks have ended. This is fine AS LONG AS ONLY THE RESULT OF A SINGLE ONE IS USED to compose
           the aggregated result, or you will have double+ counts for such tasks...
        2. If there is no attempt with ENDED_SUCCESS status (for now), take the status of the most recent attempt.
        """
        if not self._latest and len(self._attempts) > 0:
            attempt_tuples = self._attempts.values()
            ordered_attempts = sorted(attempt_tuples, key=lambda t: t.attempt_id.attempt_no)

            ended_attempts = [t for t in ordered_attempts if t.update.status == TaskStatus.ENDED_SUCCESS]
            if ended_attempts:
                self._latest = ended_attempts[0]
            else:
                self._latest = ordered_attempts[-1]

    @property
    def task_index(self) -> int:
        return self._task_index

    @property
    def latest_attempt(self) -> Optional[TaskAttemptId]:
        self._update_latest_attempt()
        return self._latest.attempt_id if self._latest else None

    @property
    def latest_update(self) -> Optional[TaskStatusUpdate]:
        self._update_latest_attempt()
        return self._latest.update if self._latest else None

    @property
    def attempt_count(self) -> int:
        return len(self._attempts)

    @property
    def time_since_update(self) -> Optional[float]:
        self._update_latest_attempt()
        return time.time() - self._latest.update.time if self._latest else None

    @property
    def attempts(self) -> Dict[TaskAttemptId, TaskStatusUpdate]:
        return {attempt_id: t.update for attempt_id, t in self._attempts.items()}


@dataclass(frozen=True)
class BaseApiResult(SerializableDataClass):
    """ The minimal result from invoker_api operations which return a status, but not necessarily
    run a job (invokes tasks via workers)."""
    success: bool
    error_message: Optional[str]


@dataclass(frozen=True)
class JobDatasetStats(SerializableDataClass):
    total_size: int
    parts: int


@dataclass(frozen=True)
class JobInvokerStats(SerializableDataClass):
    enqueue_time: float
    poll_time: float
    total_tasks: int
    failed_tasks: int
    task_success_over_time: Dict[float, int]  # Non-cumulative
    # TODO backlog lost_task_retries: int - this should be reported by the invoker as task status doesn't signal it


TimingStats = Dict[str, float]


@dataclass(frozen=True)
class JobWorkerStats(SerializableDataClass):
    cold_tasks: int
    warm_tasks: int
    invoke_latency: TimingStats
    load_time: TimingStats
    total_time: TimingStats
    scanned_rows: int
    scanned_groups: int
    cache: Dict[str, int]
    # TODO backlog loaded_column_types: Dict[str, int]  - type: count of all column types actually loaded,
    #  e.g. int, float, string (and string:categorical!) for optimizing query times.


@dataclass(frozen=True)
class JobStats(SerializableDataClass):
    total_time: Optional[float] = field(default=None)
    invoker: Optional[JobInvokerStats] = field(default=None)
    worker: Optional[JobWorkerStats] = field(default=None)
    dataset: Optional[JobDatasetStats] = field(default=None)
    cost: Optional[float] = field(default=None)


@dataclass(frozen=True)
class BaseJobResult(BaseApiResult):
    """Concrete jobs' result classes inherit from this class."""
    request_id: str
    stats: JobStats


@dataclass(frozen=True)
class JobStatus:
    """Final status of a job run & all its task attempts."""
    success: bool
    error_message: Optional[str]
    attempts_status: List[TaskAttemptsInfo]

    def with_error(self, message: str) -> object:
        """Copy an existing object, but with an error - create a new "failed" version of this immutable object."""
        if self.error_message:
            message = f"{self.error_message}; {message}"
        return JobStatus(success=False,
                         error_message=message,
                         attempts_status=self.attempts_status)
