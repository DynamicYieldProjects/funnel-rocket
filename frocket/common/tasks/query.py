"""
Query job's task classes
"""
from dataclasses import dataclass
from enum import auto
from typing import Optional, List, Dict, Union, cast
import inflection
from frocket.common.dataset import DatasetInfo, DatasetPartId
from frocket.common.serializable import AutoNamedEnum, enveloped, SerializableDataClass, reducable
from frocket.common.tasks.base import BaseTaskRequest, BaseTaskResult, BaseJobResult


class PartSelectionMode(AutoNamedEnum):
    """Whether the invoker sets the task_index or the worker selects it from available tasks in the datastore."""
    SET_BY_INVOKER = auto()
    SELECTED_BY_WORKER = auto()


@enveloped
@dataclass(frozen=True)
class QueryTaskRequest(BaseTaskRequest):
    dataset: DatasetInfo
    # String columns to load as Pandas categoricals, as performance optimization. These columns are detected during
    # dataset registration. Not needed for columns already of categorical type in files saved by Pandas.
    load_as_categoricals: Optional[List[str]]
    mode: PartSelectionMode
    # If (and only if) mode=SET_BY_INVOKER, the invoker also sets the dataset part index to query
    # Note that task_index not necessarily equals part ID
    invoker_set_part: Optional[DatasetPartId]
    used_columns: List[str]  # Which columns to actually load (as optimization), as analyzed by QueryValidator.
    query: dict


class AggregationType(AutoNamedEnum):
    # noinspection PyUnusedLocal
    def __init__(self, *args):
        if not hasattr(self.__class__, '_camels'):
            self.__class__._camels = {}

        self.camelized = inflection.camelize(self.name.lower(), uppercase_first_letter=False)
        self.__class__._camels[self.camelized] = self
        self.value_is_dict = self.name.endswith("_PER_VALUE")

    COUNT = auto()
    COUNT_PER_VALUE = auto()
    GROUPS_PER_VALUE = auto()
    SUM_PER_VALUE = auto()
    MEAN_PER_VALUE = auto()

    @classmethod
    def from_camelcase(cls, camelcase_name: str) -> AutoNamedEnum:
        return cls._camels[camelcase_name]


AggrValue = Union[int, float]
AggrValueMap = Dict[str, AggrValue]


@reducable
@dataclass(frozen=True)
class AggregationResult(SerializableDataClass):
    column: str
    type: str
    # For some aggregation types ('count') the value is a single number. In others (the '<X>perValue' ones), value is
    # a dict of column value->aggregated number
    value: Optional[Union[AggrValue, AggrValueMap]]
    top: Optional[int]  # Relevant for values of type dict
    name: Optional[str]  # Only set if the user has set a custom name for this aggregation

    @classmethod
    def _reduce_fields(cls, serializables):
        """See: SerializableDataClass."""
        all_values = [e.value for e in cast(List[AggregationResult], serializables)]
        # Reduce either a primitive values or a dicts of counters
        if isinstance(all_values[0], dict):
            reduced_value = cls.reduce_counter_dicts(all_values, top_count=cast(cls, serializables[0]).top)
        else:
            reduced_value = sum(all_values)
        return {'value': reduced_value}


@reducable
@dataclass(frozen=True)
class QueryConditionsResult(SerializableDataClass):
    matching_groups: int  # e.g. user ID
    matching_group_rows: int  # All rows of the matching groups, whether that row matches a condition or not
    aggregations: Optional[List[AggregationResult]]

    @classmethod
    def _reduce_fields(cls, serializables):
        results = cast(List[cls], serializables)
        return {'matching_groups': sum([e.matching_groups for e in results]),
                'matching_group_rows': sum([e.matching_group_rows for e in results]),
                'aggregations': cls.reduce_lists([e.aggregations for e in results])}


@reducable
@dataclass(frozen=True)
class FunnelResult(SerializableDataClass):
    sequence: List[QueryConditionsResult]
    end_aggregations: Optional[List[AggregationResult]]

    @classmethod
    def _reduce_fields(cls, serializables):
        funnel_results = cast(List[cls], serializables)
        return {'sequence': cls.reduce_lists([e.sequence for e in funnel_results]),
                'end_aggregations': cls.reduce_lists([e.end_aggregations for e in funnel_results])}


@reducable
@dataclass(frozen=True)
class QueryResult(SerializableDataClass):
    query: QueryConditionsResult
    funnel: Optional[FunnelResult]

    @classmethod
    def _reduce_fields(cls, serializables):
        query_results = cast(List[cls], serializables)
        return {'query': QueryConditionsResult.reduce([e.query for e in query_results]),
                'funnel': FunnelResult.reduce([e.funnel for e in query_results])}


@enveloped
@dataclass(frozen=True)
class QueryTaskResult(BaseTaskResult):
    query_result: Optional[QueryResult]  # Not set if query failed (when success=False)


@dataclass(frozen=True)
class QueryJobResult(BaseJobResult):
    query: Optional[QueryConditionsResult]
    funnel: Optional[FunnelResult]
