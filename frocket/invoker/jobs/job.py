from abc import abstractmethod
from typing import List, Optional, Set
from frocket.common.dataset import DatasetPartId, DatasetPartsInfo
from frocket.common.metrics import LabelsDict
from frocket.common.tasks.base import BaseTaskRequest, BaseTaskResult, BaseJobResult, JobStatus, ErrorMessage
from frocket.common.tasks.async_tracker import AsyncJobStatusUpdater


# TODO doc nicely


class Job:
    _request_id = None
    _labels = {}

    @property
    def request_id(self) -> Optional[str]:
        return self._request_id

    @request_id.setter
    def request_id(self, request_id: str):
        self._request_id = request_id

    @property
    def metric_labels(self) -> LabelsDict:
        return self._labels

    @abstractmethod
    def parts_info(self) -> Optional[DatasetPartsInfo]:
        pass

    @abstractmethod
    def total_tasks(self) -> int:
        pass

    @abstractmethod
    def build_tasks(self) -> List[BaseTaskRequest]:
        pass

    @abstractmethod
    def build_retry_task(self, attempt_no: int, task_index: int) -> BaseTaskRequest:
        pass

    def dataset_parts_to_publish(self) -> Optional[Set[DatasetPartId]]:
        return None

    def prerun(self, async_updater: AsyncJobStatusUpdater = None) -> Optional[ErrorMessage]:
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
