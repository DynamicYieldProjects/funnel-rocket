import time
from typing import List, Optional, Set, cast
from frocket.common.config import config
from frocket.common.dataset import DatasetInfo, DatasetPartId, DatasetPartsInfo, DatasetShortSchema
from frocket.common.metrics import JobTypeLabel, DATASET_LABEL
from frocket.common.tasks.base import BaseTaskRequest, JobStatus, BaseTaskResult, BaseJobResult
from frocket.common.tasks.query import PartSelectionMode, QueryTaskRequest, QueryTaskResult, QueryJobResult, \
    QueryResult
from frocket.invoker.jobs.job_builder import JobBuilder


class QueryJobBuilder(JobBuilder):
    def __init__(self, dataset: DatasetInfo, parts: DatasetPartsInfo,
                 short_schema: DatasetShortSchema, query: dict, used_columns: List[str]):
        self._dataset = dataset
        self._query = query
        self._used_columns = used_columns
        self._paths = parts.fullpaths(parent=dataset)
        self._worker_can_select_part = config.bool('worker.self.select.enabled')
        if config.bool('dataset.categorical.potential.use'):
            self._load_as_categoricals = short_schema.potential_categoricals
        else:
            self._load_as_categoricals = None
        self._labels = {
            JobTypeLabel.QUERY.label_name: JobTypeLabel.QUERY.label_value,
            DATASET_LABEL: self._dataset.id.name
        }

    def total_tasks(self) -> int:
        return len(self._paths)

    def build_tasks(self) -> List[BaseTaskRequest]:
        if self._worker_can_select_part:
            mode = PartSelectionMode.SELECTED_BY_WORKER
        else:
            mode = PartSelectionMode.SET_BY_INVOKER

        requests = [self._build_task(mode, i) for i in range(self.total_tasks())]
        return requests

    def dataset_parts_to_publish(self) -> Optional[Set[DatasetPartId]]:
        if self._worker_can_select_part:
            parts_to_publish = {DatasetPartId(self._dataset.id, path, part_index)
                                for part_index, path in enumerate(self._paths)}
            return parts_to_publish
        else:
            return None

    def build_retry_task(self, attempt_no: int, task_index: int) -> BaseTaskRequest:
        return self._build_task(PartSelectionMode.SET_BY_INVOKER,
                                part_index=task_index,
                                attempt_no=attempt_no)

    def _build_task(self, mode: PartSelectionMode, part_index: int, attempt_no: int = 0) -> QueryTaskRequest:
        if mode == PartSelectionMode.SET_BY_INVOKER:
            invoker_set_part = DatasetPartId(dataset_id=self._dataset.id,
                                             path=self._paths[part_index],
                                             part_idx=part_index)
        elif mode == PartSelectionMode.SELECTED_BY_WORKER:
            assert attempt_no == 0
            invoker_set_part = None
        else:
            raise Exception("Unknown mode {mode}")

        request = QueryTaskRequest(
            request_id=self._request_id,
            invoke_time=time.time(),
            dataset=self._dataset,
            load_as_categoricals=self._load_as_categoricals,
            query=self._query,
            task_index=part_index,
            attempt_no=attempt_no,
            mode=mode,
            invoker_set_part=invoker_set_part,
            used_columns=self._used_columns)
        return request

    def build_result(self, base_attributes: dict,
                     final_status: JobStatus,
                     latest_task_results: List[BaseTaskResult]) -> BaseJobResult:
        aggregated_query_result = None
        # Only if query was successful, aggregate query results (for each task - from a single successful attempt)
        if final_status.success:
            latest_task_results = cast(List[QueryTaskResult], latest_task_results)
            query_results = [task_result.query_result for task_result in latest_task_results]
            aggregated_query_result = cast(QueryResult,
                                           QueryResult.reduce(query_results))

        result = QueryJobResult(
            **base_attributes,
            query=aggregated_query_result.query if aggregated_query_result else None,
            funnel=aggregated_query_result.funnel if aggregated_query_result else None
        )
        return result
