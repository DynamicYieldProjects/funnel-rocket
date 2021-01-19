import logging
import time
from typing import List, cast, Optional
from pandas import DataFrame
from frocket.common.config import config
from frocket.common.dataset import DatasetPartId
from frocket.common.metrics import MetricName, PartSelectMethodLabel
from frocket.common.tasks.base import TaskStatus, TaskAttemptId, BaseTaskRequest
from frocket.common.tasks.query import PartSelectionMode, QueryTaskRequest, QueryResult, QueryTaskResult
from frocket.engine.query_engine import QueryEngine
from frocket.worker.runners.base_task_runner import BaseTaskRunner, TaskRunnerContext
from frocket.worker.runners.part_loader import load_dataframe, get_cached_candidates, FilterPredicate

logger = logging.getLogger(__name__)

preflight_duration = int(config.get("part.selection.preflight.ms"))
if preflight_duration < 0 or preflight_duration > 2000:
    raise Exception("Preflight duration should be between 0 (disabled) and 2000 milliseconds"
                    f", but configured: {preflight_duration}")
PREFLIGHT_DURATION_SECONDS = preflight_duration / 1000


class QueryTaskRunner(BaseTaskRunner):
    def __init__(self, req: BaseTaskRequest, ctx: TaskRunnerContext):
        super().__init__(req, ctx)
        self._req = cast(QueryTaskRequest, req)  # Avoid type warnings
        self._dataset_part_id: Optional[DatasetPartId] = None
        self._query_result: Optional[QueryResult] = None

    def _do_run(self):
        self._set_part_to_load()
        self._update_status(TaskStatus.LOADING_DATA)
        with self._ctx.metrics.measure(MetricName.TASK_TOTAL_LOAD_SECONDS):
            df = self._load(needed_columns=self._req.used_columns,
                            load_as_categoricals=self._req.load_as_categoricals)

        self._update_status(TaskStatus.RUNNING_QUERY)
        with self._ctx.metrics.measure(MetricName.TASK_RUN_QUERY_SECONDS):
            engine = QueryEngine(self._req.dataset.group_id_column, self._req.dataset.timestamp_column)
            engine_result = engine.run(df, self._req.query)
        self._query_result = engine_result

    def _set_part_to_load(self) -> None:
        task_attempt_no = self._req.attempt_no
        if self._req.mode == PartSelectionMode.SET_BY_INVOKER:
            part_id = self._req.invoker_set_part
            actual_select_method = PartSelectMethodLabel.SET_BY_INVOKER
        elif self._req.mode == PartSelectionMode.SELECTED_BY_WORKER:
            actual_select_method, part_id = self._select_part_myself()
            logger.info(f"Worker selected part: method: {actual_select_method}, file ID: {part_id}, "
                        f"task attempt no.: {task_attempt_no}")
        else:
            raise Exception(f"Don't know how to handle request mode: {self._req.mode}")

        if not part_id:
            raise Exception("No part to load")

        self._ctx.metrics.set_label_enum(actual_select_method)
        self._dataset_part_id = part_id
        self._task_attempt_id = TaskAttemptId(part_id.part_idx, task_attempt_no)

    def _select_part_myself(self):
        time_left_in_preflight = PREFLIGHT_DURATION_SECONDS - BaseTaskRunner.time_since_invocation(self._req)
        candidates = get_cached_candidates(self._req.dataset.id)
        if not candidates:
            if time_left_in_preflight > 0:
                logger.info("Got no candidates but we're still during preflight"
                            f", so sleeping for {time_left_in_preflight} seconds")
                time.sleep(time_left_in_preflight)

        # If a worker got some candidates, we still gonna try to grab them even if preflight time has ended
        selected_part = self._ctx.datastore.self_select_part(self._req.request_id, self._req.attempt_no, candidates)
        if not selected_part.part_id:
            # TODO not supposed to happen, unless there's a retry mechanism gone awry
            raise Exception("Got no part for me!")

        if candidates:
            if not selected_part.random:
                actual_select_method = PartSelectMethodLabel.SPECIFIC_CANDIDATE
            else:
                actual_select_method = PartSelectMethodLabel.RANDOM_CANDIDATES_TAKEN
        else:
            actual_select_method = PartSelectMethodLabel.RANDOM_NO_CANDIDATES

        return actual_select_method, selected_part.part_id

    def _load(self, needed_columns: List[str] = None, load_as_categoricals: List[str] = None) -> DataFrame:
        filters = self._predicate_pushdown_filters()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Filters used when loading: {filters}")
            logger.debug(f"Columns to explicitly load as categorical: {load_as_categoricals}")

        df = load_dataframe(file_id=self._dataset_part_id, metrics=self._ctx.metrics,
                            needed_columns=needed_columns, filters=filters, load_as_categoricals=load_as_categoricals)
        self._ctx.metrics.set_metric(MetricName.SCANNED_ROWS, len(df))
        self._ctx.metrics.set_metric(MetricName.SCANNED_GROUPS, df[self._req.dataset.group_id_column].nunique())
        return df

    # A reminder here that the query timeframe affects not just conditions/sequences, but *also aggregations* -
    # hence it can be used to filter rows (and this quite probably whole users) from being loaded at all.
    def _predicate_pushdown_filters(self):
        filters = []
        timeframe = self._req.query.get('timeframe', None)
        if timeframe:
            fromtime = timeframe.get('from', None)
            if fromtime is not None:
                filters.append(FilterPredicate(column=self._req.dataset.timestamp_column, op='>=', value=str(fromtime)))
            totime = timeframe.get('to', None)
            if totime is not None:
                filters.append(FilterPredicate(column=self._req.dataset.timestamp_column, op='<', value=str(totime)))

        return filters if len(filters) > 0 else None

    def _build_result(self, base_attributes):
        return QueryTaskResult(
            **base_attributes,
            query_result=self._query_result)
