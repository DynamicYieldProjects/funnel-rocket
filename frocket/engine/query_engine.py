"""
Implementation of queries (conditions, aggregations, funnel) with Pandas, over a single DataFrame.

The QueryEngine is agnostic to the wider system in which it runs - it is handed a DataFrame object and an already
validated query (with any shorthand notations and ommitted values expanded to the elaborate version), and does its work.
It should not import any code from the invoker/worker layers, and should remain testable as a stand-alone component.

TODO backlog get rid of any conditions/aggregations that are using composable strings (df.query()),
 although the query schema is validated in various layers rather than any client input naively taken as is.
"""

import json
import logging
import math
from dataclasses import dataclass
from typing import Dict, List, Callable, Any, Union, Optional, cast
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Index
from frocket.common.config import config
from frocket.common.helpers.pandas import filter_by_isin, add_column_by_value_map
from frocket.common.tasks.query import AggregationResult, QueryConditionsResult, FunnelResult, \
    QueryResult, AggregationType
from frocket.common.validation.consts import CONDITION_COLUMN_PREFIX, TARGET_TYPES_WITH_INCLUDE_ZERO, NUMERIC_OPERATORS
from frocket.common.validation.path_visitor import PathVisitor
from frocket.common.validation.relation_parser import RelationParser
from frocket.engine.relation_to_pandas import relation_to_pandas_query

logger = logging.getLogger(__name__)
INCLUDE_ZERO = 'includeZero'
SUPPORTED_CONDITION_TYPES = {'filter', 'filters', 'sequence'}
AGGREGATION_TOP_DEFAULT_COUNT = config.int('aggregations.top.default.count')
AGGREGATION_TOP_GRACE_FACTOR = config.float('aggregations.top.grace.factor')


@dataclass(frozen=True)
class ColumnAggregatorContext:
    column: str
    other_column: Optional[str]
    group_by_column: str
    timestamp_column: str


ColumnAggregatorFunc = Callable[[DataFrame, ColumnAggregatorContext],
                                Union[int, float, Series]]

AGGREGATION_FUNCTIONS: Dict[AggregationType, ColumnAggregatorFunc] = {
    AggregationType.COUNT: lambda df, ctx: df[ctx.column].notnull().sum().item(),
    AggregationType.COUNT_PER_VALUE: lambda df, ctx: df[ctx.column].value_counts(),
    AggregationType.GROUPS_PER_VALUE: lambda df, ctx: df.groupby(ctx.column, sort=False)[ctx.group_by_column].nunique(),
    AggregationType.SUM_PER_VALUE: lambda df, ctx: df.groupby(ctx.column, sort=False)[ctx.other_column].sum(),
    AggregationType.MEAN_PER_VALUE: lambda df, ctx: df.groupby(ctx.column, sort=False)[ctx.other_column].mean()
}

FilterFunc = Callable[[DataFrame], Series]


class QueryEngine:
    def __init__(self, group_by_column: str, timestamp_column: str):
        self._group_by_column = group_by_column
        self._timestamp_column = timestamp_column

    def run(self, df: DataFrame, full_query: Dict) -> QueryResult:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Got query: {json.dumps(full_query, indent=2)}")

        # First stage: run query conditions, if any
        conditions = PathVisitor(full_query, 'query.conditions').list()
        if conditions:
            relation_query = self._build_relation_query(full_query)
            matching_groups = self._run_query_conditions(df, conditions, relation_query)
            rows_of_matching_groups = filter_by_isin(df=df, column=self._group_by_column, values=matching_groups)
        else:
            # No query entry at all / empty or no conditions array means: all group IDs match, and thus all rows
            matching_groups = df[self._group_by_column].unique()
            rows_of_matching_groups = df

        # Second stage: run any aggregations defined over the query (regardless of whether there's also a funnel)
        query_aggregations = PathVisitor(full_query, 'query.aggregations').list()
        aggregation_results = \
            self._run_aggregations(rows_of_matching_groups, query_aggregations) if query_aggregations else None

        # Wrapping up the query part
        query_result = QueryConditionsResult(matching_groups=len(matching_groups),
                                             matching_group_rows=len(rows_of_matching_groups),
                                             aggregations=aggregation_results)

        # Third stage: run funnel, if defined - over rows of all groups matching the query conditions
        funnel = full_query.get('funnel', None)
        funnel_result = self._run_funnel(rows_of_matching_groups, funnel) if funnel else None

        return QueryResult(query_result, funnel_result)

    @staticmethod
    def _build_relation_query(full_query: dict) -> str:
        parsed_relation_elements = RelationParser(full_query).parse()
        return relation_to_pandas_query(parsed_relation_elements, CONDITION_COLUMN_PREFIX)

    def _run_query_conditions(self, df: DataFrame, conditions: List[dict], relation_query: str) -> np.ndarray:
        condition_results: List[Series] = []
        for i, condition in enumerate(conditions):
            filter_func = self._get_filter_function(condition)
            result_series = filter_func(df)
            condition_results.append(result_series.rename(CONDITION_COLUMN_PREFIX + str(i)))

        all_conditions_df = pd.concat(condition_results, axis=1, copy=False)
        all_conditions_df.fillna(False, inplace=True)

        groups_matching_relation = all_conditions_df.query(relation_query)
        return groups_matching_relation.index.values

    def _get_filter_function(self, condition: dict) -> FilterFunc:
        ftype_to_handler: Dict[str, FilterFunc] = {
            'filter': lambda df: self._run_condition(df, condition),
            'filters': lambda df: self._run_condition(df, condition),
            'sequence': lambda df: self._run_sequence_condition(df, condition)
        }

        # Ensure there's exxactly one of the supported filter types in the given condition
        found_filter_type = [k for k in condition.keys() if k in SUPPORTED_CONDITION_TYPES]
        assert len(found_filter_type) == 1
        return ftype_to_handler[found_filter_type[0]]

    # noinspection PyUnusedLocal
    def _run_condition(self, df: DataFrame, condition: Dict) -> Series:
        # First stage: run the filter query and group - only groups with one or more matching rows will remain
        filter_query_string = QueryEngine._build_query_string(condition)
        logger.debug(f"Running filter query: '{filter_query_string}' and grouping by '{self._group_by_column}'")
        matching_groups_df = df.query(filter_query_string).groupby(self._group_by_column, sort=False)

        # Second stage: run target
        target = condition['target']
        assert target['op'] in NUMERIC_OPERATORS
        assert type(target['value']) in [int, float]
        target_type = target['type']
        target_includes_zero = condition.get(INCLUDE_ZERO, False)
        if target_includes_zero:
            assert target_type in TARGET_TYPES_WITH_INCLUDE_ZERO

        if target_type == 'sum':
            group_sums = matching_groups_df[target['column']].sum()
            target_expression = f"group_sums {target['op']} {target['value']}"
            logger.debug(f"Evaluating target: '{target_expression}'. Group sums are for column '{target['column']}'")
            result_series = eval(target_expression)  # TODO backlog move to numexpr engine?
            result_series = result_series[result_series]  # Filter the series to rows whose value is True
            logger.debug(f"Group with sum matching target: {len(result_series)}")

        elif target_type == 'count':
            group_sizes = matching_groups_df.size()
            target_expression = f"group_sizes {target['op']} {target['value']}"
            logger.debug(f"Evaluating target: {target_expression}")
            result_series = eval(target_expression)
            result_series = result_series[result_series]  # Filter the series to rows whose value is True
            logger.debug(f"Group with size matching target: {len(result_series)} (no zero-sized groups)")

            if target_includes_zero:
                matching_ids = list(matching_groups_df.indices.keys())
                all_group_ids = df[self._group_by_column].unique()
                ids_wo_match_array = all_group_ids[~np.isin(all_group_ids, matching_ids)]
                ids_without_match = Series(data=True, index=ids_wo_match_array)
                logger.debug(f"Group IDs with zero metching rows: {len(ids_without_match)}")

                result_series = result_series.append(ids_without_match, verify_integrity=False)
                logger.debug(f"Group IDs either matching target or zero matches: {len(result_series)}")
        else:
            raise Exception(f"Uknown target type: {target_type}")

        return result_series

    @staticmethod
    def _build_query_string(condition: dict) -> str:
        if 'filter' in condition:
            return QueryEngine._single_filter_query_string(condition['filter'])
        elif 'filters' in condition:
            return QueryEngine._multi_filter_query_string(condition['filters'])
        else:
            raise Exception(f"Don't know how to handle condition: {condition}")

    @staticmethod
    def _single_filter_query_string(f: dict) -> str:
        # Back-ticks around column name allows names which aren't valid Python identifiers
        assert '`' not in f['column']  # Sanity/safety
        safe_column_name = f"`{f['column']}`"

        # Add quotes around string values
        value = f['value']
        if type(value) is str:
            value = value.replace("'", "\\'")
            value = "'" + value + "'"

        if f['op'] in ('contains', 'not contains'):
            return f"{'~' if f['op'].startswith('not') else ''}{safe_column_name}.str.contains({value}, \
                    case=True, na=False, regex=False)"
        else:
            assert f['op'] in NUMERIC_OPERATORS
            return f"{safe_column_name} {f['op']} {value}"

    @staticmethod
    def _multi_filter_query_string(filters: List[dict]) -> str:
        single_queries = [QueryEngine._single_filter_query_string(f) for f in filters]
        return ' & '.join(single_queries)  # Relation between elements in 'filters' is currently only AND

    def _run_sequence_condition(self, df: DataFrame, condition: dict) -> Series:
        last_step_group_ids = self._run_sequence(df, condition['sequence'], return_all_steps=False)[0]
        return pd.Series(data=True, index=last_step_group_ids)

    def _run_sequence(self, df: DataFrame, sequence: List[dict], return_all_steps: bool = True) -> List[Index]:
        def column_for_min_timestamp(step_idx: int) -> str:
            return f"ts_of_event_{step_idx}"

        step_queries = [QueryEngine._build_query_string(f) for f in sequence]
        df_with_step_columns = df.copy(deep=False)
        users_who_passed_each_step = []

        for i, query_string in enumerate(step_queries):
            if i == 0:
                query_to_run = query_string
            else:
                query_to_run = f"{self._timestamp_column} > {column_for_min_timestamp(i - 1)} & {query_string}"

            rows_matching_step = df_with_step_columns.query(query_to_run)
            min_ts_per_matching_group = \
                rows_matching_step.groupby(self._group_by_column, sort=False)[self._timestamp_column].min()

            is_last_step = (i == len(step_queries) - 1)
            if return_all_steps or is_last_step:
                users_who_passed_each_step.append(min_ts_per_matching_group.index)

            if not is_last_step:
                add_column_by_value_map(df=df_with_step_columns,
                                        keys_column=self._group_by_column,
                                        values_map_series=min_ts_per_matching_group,
                                        new_column=column_for_min_timestamp(i))

        return users_who_passed_each_step

    def _run_aggregations(self,
                          df: DataFrame,
                          aggregations: Optional[List[dict]]) -> Optional[List[AggregationResult]]:
        if aggregations is None:
            return None
        elif not aggregations:
            return []

        result = []
        for agg in aggregations:
            agg_type = cast(AggregationType, AggregationType.from_camelcase(agg['type']))
            if agg_type.value_is_dict:
                top_count = agg.get('top', AGGREGATION_TOP_DEFAULT_COUNT)
                actual_top_count = math.ceil(top_count * AGGREGATION_TOP_GRACE_FACTOR)
            else:
                top_count = None
                actual_top_count = None

            if len(df) == 0:
                value = {} if agg_type.value_is_dict else 0
            else:
                ctx = ColumnAggregatorContext(column=agg['column'],
                                              other_column=agg.get('otherColumn', None),
                                              group_by_column=self._group_by_column,
                                              timestamp_column=self._timestamp_column)
                aggr_func = AGGREGATION_FUNCTIONS[agg_type]
                aggr_result = aggr_func(df, ctx)
                if agg_type.value_is_dict:
                    assert type(aggr_result) is Series
                    top_dict = aggr_result.nlargest(actual_top_count).to_dict()
                    value = {str(k): v for k, v in top_dict.items()}
                else:
                    assert type(aggr_result) in [int, float]
                    value = aggr_result

            result.append(AggregationResult(column=agg['column'],
                                            type=agg['type'],
                                            name=agg.get('name', None),
                                            value=value,
                                            top=top_count))

        return result

    def _run_funnel(self, df: DataFrame, funnel: Dict[str, Any]) -> FunnelResult:
        # First stage: calculate the group IDs and all-their-rows for each step in the funnel
        groups_per_step = self._run_sequence(df, funnel['sequence'])
        all_rows_per_step = [filter_by_isin(df, self._group_by_column, values=groups)
                             for groups in groups_per_step]

        # Second stage: if there are aggregations to run after each step, run 'em now for all steps
        step_aggregations = PathVisitor(funnel, 'stepAggregations').list()
        if step_aggregations:
            step_agg_results = [self._run_aggregations(step_df, step_aggregations) for
                                step_df in all_rows_per_step]
        else:
            step_agg_results = None

        # Third stage: if the are aggregations to run after the full funnel is completed, run
        end_aggregations = PathVisitor(funnel, 'endAggregations').list()
        if end_aggregations:
            end_agg_results = self._run_aggregations(all_rows_per_step[-1], end_aggregations)
        else:
            end_agg_results = None

        per_step_results = [
            QueryConditionsResult(matching_groups=len(groups_per_step[i]),
                                  matching_group_rows=len(all_rows_per_step[i]),
                                  aggregations=step_agg_results[i] if step_agg_results else None)
            for i in range(len(all_rows_per_step))
        ]
        return FunnelResult(sequence=per_step_results, end_aggregations=end_agg_results)
