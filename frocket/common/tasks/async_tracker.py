"""
AsyncJobTracker object is handed by invoker_api to clients that launch a job in a non-blocking fashion.
It enables either periodic polling or blocking on updates. Updates are guaranteed to be atomic - that is,
there may be further updates, but the status you have in hand is consistent.
"""
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
    message: Optional[str] = None  # The job may set descriptive text for what it's doing
    result: Optional[BaseJobResult] = None  # Only available on stage=AsyncJobStage.DONE
    task_counters: Optional[Dict[TaskStatus, int]] = None


class JobTimeoutError(Exception):
    pass


class AsyncJobTracker(metaclass=ABCMeta):
    """The interface as known to clients"""

    @property
    @abstractmethod
    def status(self) -> AsyncJobStatus:
        """Get the latest status - as a consistent object which will not be mutated while using it"""
        pass

    @property
    @abstractmethod
    def elapsed_time(self) -> float:
        pass

    @property
    @abstractmethod
    def wait_time_remaining(self) -> Optional[float]:
        """
        If a tracker object was initialized with a timeout value by its creator (the invoker_api,
        based on configuration), then time remaining till timeout is known and can be returned.
        """
        pass

    @abstractmethod
    def wait(self, timeout: float = None) -> bool:
        """
        Blocking wait for updates with the given timeout, in seconds - but always capped to max wait time if set.
        By default, timeout is None - meaning wait up till max wait time (or indefinitely, in case it wasn't set).
        Assuming wait time is set, this is a good choice since no busy loop or semi-busy loop is needed.
        """
        pass

    def generator(self) -> Generator[AsyncJobStatus, None, None]:
        """
        Returns updates as they come for easier consumption, if blocking behavior is ok.
        This generator does not rely on any private attributes.
        """
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
    """
    Implementation of AsyncJobTracker, which is only created within invoker_api and updated by invoker/job code.

    The one curiousity here is the blocking wait() mechanism which is based on a Queue instance.
    How it works: the client's wait() call blocks on waiting for a queue item. If there's already one,
    it's immediately returned. Once consumed, the queue is empty again and a subsequent wait() will repeat
    the process. Typically, the queue should have either zero or a only single item - see _signal_update() below.
    """
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
        """Only signal an update if there was actually any change."""
        modified = self._status != new_status
        self._status = new_status
        if modified:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Updated async status from\n:{self._status} to:\n{new_status}")
            self._signal_update()
        pass

    def update(self, stage: AsyncJobStage = None, message: str = None, task_counters: Dict[TaskStatus, int] = None):
        # Asserts are used here as the invoker/job classes are internal to the invoker_api, and are expected to conform
        # to this class' requirements. If not, it's probably a bug.
        assert stage != AsyncJobStage.DONE  # To move to DONE stage, done() should be explicitly called
        assert self._status.stage != AsyncJobStage.DONE  # No more updates after DONE was called once
        stage = stage or self._status.stage
        task_counters = task_counters or self._status.task_counters
        # Automatically cleanup message when moving in stages
        message = message or (self._status.message if (stage == self._status.stage) else None)

        self._update_status(AsyncJobStatus(stage=stage, message=message,
                                           task_counters=task_counters))

    def done(self, result: BaseJobResult):
        self._update_status(AsyncJobStatus(stage=AsyncJobStage.DONE, result=result,
                                           task_counters=self._status.task_counters))

    def _signal_update(self):
        if self._update_queue.empty():
            # If the client *already* has an update waiting for it, no need to do anything - it will read the latest
            # state anyway when it gets to consume it (the queue item itself doesn't hold any information).
            # In case of more than single updater thread, there might momentarily be more than a single item.
            # However, this is not currently used in this way, and it seems that having multiple items would not
            # have any detrimental effect (i.e. break correctness) if it actually occurs in other edge cases.
            # TODO backlog to ensure a single item always, consider a lock here and re-test empty() within that lock.
            self._update_queue.put(object())

    def wait(self, timeout=None):
        assert timeout is None or timeout > 0
        try:
            should_block = True
            if self._max_wait:
                remaining = self.wait_time_remaining
                if remaining == 0:
                    # No more blocking wait - immediately return what's in the queue (or None)
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
