import logging
import time
from enum import auto
from typing import Optional, Dict, List, NamedTuple
from dataclasses import dataclass, field
from frocket.common.metrics import MetricData
from frocket.common.serializable import SerializableDataClass, API_PUBLIC_METADATA, AutoNamedEnum

logger = logging.getLogger(__name__)

#
# Base types for all task types
#

# TODO Consider using types to avoid mixup between TaskIndex and PartIndex!
ErrorMessage = str
BlobId = str


@dataclass(frozen=True)
class BaseTaskRequest(SerializableDataClass):
    request_id: str
    invoke_time: float
    attempt_no: int
    task_index: int


class TaskStatus(AutoNamedEnum):
    # TODO sane way to use auto() (used by base class) *and* pass extra arguments, instead of naming convention
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
    error_message: Optional[str]
    metrics: List[MetricData]


@dataclass(frozen=True)
class TaskAttemptId(SerializableDataClass):
    task_index: int
    attempt_no: int = 0


@dataclass(frozen=True)
class TaskStatusUpdate(SerializableDataClass):
    status: TaskStatus
    time: float

    @classmethod
    def now(cls, status):
        return TaskStatusUpdate(status, time.time())


class TaskAttemptsInfo:
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
    success: bool = field(metadata=API_PUBLIC_METADATA)
    error_message: Optional[str] = field(metadata=API_PUBLIC_METADATA)


@dataclass(frozen=True)
class BaseJobResult(BaseApiResult):
    request_id: str = field(metadata=API_PUBLIC_METADATA)
    task_counters: Dict[str, int]
    metrics: List[MetricData]
    cost: Optional[float]


# Final status of a job run & all its task attempts
@dataclass(frozen=True)
class JobStatus:
    success: bool
    error_message: Optional[str]
    attempts_status: List[TaskAttemptsInfo]

    # Create a copy but with an error
    def with_error(self, message: str) -> object:
        if self.error_message:
            message = f"{self.error_message}; {message}"
        return JobStatus(success=False,
                         error_message=message,
                         attempts_status=self.attempts_status)
