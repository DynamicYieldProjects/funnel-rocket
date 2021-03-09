from abc import abstractmethod, ABCMeta
from typing import List, Optional, Set
from frocket.common.dataset import DatasetPartId, DatasetPartsInfo
from frocket.common.metrics import LabelsDict
from frocket.common.tasks.base import BaseTaskRequest, BaseTaskResult, BaseJobResult, JobStatus, ErrorMessage
from frocket.common.tasks.async_tracker import AsyncJobStatusUpdater


class Job(metaclass=ABCMeta):
    """
    For each job type (registratioThen, qeury, and future ones) there is a concrete subclass.
    That concrete class is handed to the invoker object, which is agnostic to the job details but calls the
    job's method in a set order.

    The flow, at high level:
    1. On prerun(), the job validates its arguments (and can fail by returnning an error message) and can prepare data
       for building tasks.

    2. When build_tasks() is called by the invoker - return a list of concrete task request objects,
       all with attempt no. 0.

    3. If the job supports task self-selection by workers, it should override dataset_parts_to_publish() and
       return a list of parts to be consumed by workers (workers would try to select parts they have cached locally).
       This list is published by the data store before tasks are invoked.

    4.  In case the invoker decides to retry a task, it calls build_retry_task() to create a specific retry task

    5. After all tasks have completed, either successfully or not, complete() is called to run any validations on the
       final results of all tasks, and perform any needed aggregations. The job may fail at this stage if the results of
       tasks, taken together, are invalid.

    6. Lastly, build_result() is called to construct the final job result.
       At this stage, the final success status of the job should not change.
    """
    _request_id = None
    _labels = {}

    @property
    def request_id(self) -> Optional[str]:
        return self._request_id

    @request_id.setter
    def request_id(self, request_id: str):
        self._request_id = request_id

    def prerun(self, async_updater: AsyncJobStatusUpdater = None) -> Optional[ErrorMessage]:
        pass

    @abstractmethod
    def build_tasks(self) -> List[BaseTaskRequest]:
        pass

    def dataset_parts_to_publish(self) -> Optional[Set[DatasetPartId]]:
        return None

    @abstractmethod
    def total_tasks(self) -> int:
        pass

    @abstractmethod
    def build_retry_task(self, attempt_no: int, task_index: int) -> BaseTaskRequest:
        pass

    def complete(self,
                 tasks_final_status: JobStatus,
                 latest_task_results: List[BaseTaskResult],
                 async_updater: AsyncJobStatusUpdater = None) -> JobStatus:
        return tasks_final_status

    @abstractmethod
    def build_result(self,
                     base_attributes: dict,
                     final_status: JobStatus,
                     latest_task_results: List[BaseTaskResult]) -> BaseJobResult:
        pass

    @property
    def metric_labels(self) -> LabelsDict:
        return self._labels

    @abstractmethod
    def parts_info(self) -> Optional[DatasetPartsInfo]:
        pass
