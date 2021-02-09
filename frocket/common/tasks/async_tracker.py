import logging
import time
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from enum import auto
from queue import Queue, Empty
from typing import Optional, Dict, Generator
from frocket.common.serializable import AutoNamedEnum
from frocket.common.tasks.base import BaseJobResult, TaskStatus

logger = logging.getLogger(__name__)


class AsyncJobStage(AutoNamedEnum):
    STARTING = auto()
    RUNNING = auto()
    FINISHING = auto()
    DONE = auto()


@dataclass(frozen=True)
class AsyncJobStatus:
    stage: AsyncJobStage
    message: Optional[str] = None
    result: Optional[BaseJobResult] = None
    task_counters: Optional[Dict[TaskStatus, int]] = None


class JobTimeoutError(Exception):
    pass


class AsyncJobTracker(metaclass=ABCMeta):
    @property
    @abstractmethod
    def status(self) -> AsyncJobStatus:
        pass

    @property
    @abstractmethod
    def elapsed_time(self) -> float:
        pass

    @property
    @abstractmethod
    def wait_time_remaining(self) -> Optional[float]:
        pass

    @abstractmethod
    def wait(self, timeout: float = None) -> bool:
        pass

    def generator(self) -> Generator[AsyncJobStatus, None, None]:
        while True:
            update_available = self.wait()
            if not self.wait_time_remaining:
                raise JobTimeoutError()

            status_snapshot = self.status
            if status_snapshot.result:
                break

            if update_available:
                yield status_snapshot

        yield status_snapshot


class AsyncJobStatusUpdater(AsyncJobTracker):
    def __init__(self, max_wait: float = None):
        self._status: AsyncJobStatus = AsyncJobStatus(stage=AsyncJobStage.STARTING)
        self._update_queue = Queue()
        self._max_wait = max_wait
        self._start_time = time.time()

    @property
    def elapsed_time(self) -> float:
        return time.time() - self._start_time

    @property
    def wait_time_remaining(self) -> Optional[float]:
        assert self._max_wait
        remaining = self._max_wait - self.elapsed_time
        return remaining if remaining > 0 else 0

    @property
    def status(self) -> AsyncJobStatus:
        return self._status

    def _update_status(self, new_status: AsyncJobStatus) -> None:
        modified = self._status != new_status
        self._status = new_status
        if modified:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Updated async status from\n:{self._status} to:\n{new_status}")
            self._signal_update()
        pass

    def update(self, stage: AsyncJobStage = None, message: str = None, task_counters: Dict[TaskStatus, int] = None):
        assert stage != AsyncJobStage.DONE
        assert self._status.stage != AsyncJobStage.DONE
        stage = stage or self._status.stage
        task_counters = task_counters or self._status.task_counters
        # Cleanup message when moving in stages
        message = message or (self._status.message if (stage == self._status.stage) else None)

        self._update_status(AsyncJobStatus(stage=stage, message=message,
                                           task_counters=task_counters))

    def done(self, result: BaseJobResult):
        self._update_status(AsyncJobStatus(stage=AsyncJobStage.DONE, result=result,
                                           task_counters=self._status.task_counters))

    def _signal_update(self):
        if self._update_queue.empty():
            # In case of more than single updater thread, there might momentarily be more than a single item
            # TODO consider adding pattern of "lock within condition, then re-test condition"
            self._update_queue.put(object())

    def wait(self, timeout=None):
        assert timeout is None or timeout > 0
        try:
            should_block = True
            if self._max_wait:
                remaining = self.wait_time_remaining
                if remaining == 0:
                    should_block = False
                    timeout = None
                elif timeout:
                    timeout = min(timeout, remaining)
                else:
                    timeout = remaining

            self._update_queue.get(block=should_block, timeout=timeout)
            return True
        except Empty:
            return False
