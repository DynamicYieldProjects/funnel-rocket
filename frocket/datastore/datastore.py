from abc import abstractmethod, ABCMeta
from dataclasses import dataclass
from typing import List, Dict, Set, Optional, Union
from frocket.common.tasks.base import TaskStatus, BaseTaskResult, TaskAttemptId, TaskStatusUpdate, BaseTaskRequest
from frocket.common.dataset import DatasetInfo, DatasetPartsInfo, DatasetPartId, DatasetShortSchema, DatasetSchema
from frocket.common.serializable import SerializableDataClass

DEFAULT_QUEUE = 'default'
DEFAULT_DEQUEUE_WAIT_TIME = 60


# Used in PartSelectionMode.SELECTED_BY_WORKER
@dataclass(frozen=True)
class WorkerSelectedPart(SerializableDataClass):
    part_id: DatasetPartId
    random: bool
    task_attempt_no: int


class Datastore(metaclass=ABCMeta):
    """
    Interface to the data store, which holds:

    * The list, metadata and schema of all registered datasets
    * For running jobs:
      * Task statuses and results
      * Atomic attempt counter for retried tasks
      * For jobs running in mode PartSelectionMode.SELECTED_BY_WORKER, the manifest of available tasks to select from.
      * When the system is configured to use the 'work_queue' invoker (rather than 'aws_lambda'), the datastore also
        provides the queue through which tasks are enqueued by the invoker and picked up by the workers, like a very
        simplistic queue management system.

    The datastore is not for storing the actual dataset or other persistent large data.
    """
    @abstractmethod
    def write_dataset_info(self, dataset: DatasetInfo, parts: DatasetPartsInfo, schema: DatasetSchema) -> None:
        pass

    @abstractmethod
    def remove_dataset_info(self, name: str) -> bool:
        pass

    @abstractmethod
    def dataset_info(self, name: str) -> DatasetInfo:
        pass

    @abstractmethod
    def dataset_parts_info(self, ds: DatasetInfo) -> DatasetPartsInfo:
        pass

    @abstractmethod
    def schema(self, ds: DatasetInfo) -> DatasetSchema:
        pass

    @abstractmethod
    def short_schema(self, ds: DatasetInfo) -> DatasetShortSchema:
        pass

    @abstractmethod
    def last_used(self, ds: DatasetInfo) -> int:
        pass

    @abstractmethod
    def mark_used(self, ds: DatasetInfo):
        pass

    @abstractmethod
    def datasets(self) -> List[DatasetInfo]:
        pass

    @abstractmethod
    def enqueue(self, requests: List[BaseTaskRequest], queue: str = DEFAULT_QUEUE) -> None:
        pass

    @abstractmethod
    def dequeue(self, queue: str = DEFAULT_QUEUE, timeout: int = DEFAULT_DEQUEUE_WAIT_TIME) -> BaseTaskRequest:
        pass

    @abstractmethod
    def update_task_status(self, reqid: str,
                           tasks: Union[TaskAttemptId, List[TaskAttemptId]], status: TaskStatus) -> None:
        pass

    @abstractmethod
    def tasks_status(self, reqid: str) -> Dict[TaskAttemptId, TaskStatusUpdate]:
        pass

    @abstractmethod
    def write_task_result(self, reqid: str, taskid: TaskAttemptId, result: BaseTaskResult) -> None:
        pass

    @abstractmethod
    def task_results(self, reqid: str) -> Dict[TaskAttemptId, BaseTaskResult]:
        pass

    @abstractmethod
    def increment_attempt(self, reqid: str, part_idx: int) -> int:
        pass

    @abstractmethod
    def publish_for_worker_selection(self, reqid: str, attempt_round: int, parts: Set[DatasetPartId]) -> None:
        pass

    @abstractmethod
    def self_select_part(self, reqid: str, attempt_round: int,
                         candidates: Set[DatasetPartId] = None) -> Optional[WorkerSelectedPart]:
        pass

    @abstractmethod
    def cleanup_request_data(self, reqid: str) -> None:
        pass
