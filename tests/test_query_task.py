import time
from typing import cast, List, Union, Tuple

import pytest

from frocket.common.dataset import DatasetPartId
from frocket.common.helpers.utils import timestamped_uuid
from frocket.common.metrics import MetricName, LoadFromLabel, PartSelectMethodLabel
from frocket.common.tasks.base import TaskStatus
from frocket.common.tasks.query import QueryTaskRequest, PartSelectionMode, QueryTaskResult, AggregationResult
from frocket.common.validation.query_validator import QueryValidator
from frocket.datastore.registered_datastores import get_datastore
from tests.base_test_utils import assert_metric_value, assert_label_value_exists
from tests.dataset_utils import new_test_dataset, str_and_none_column_values, TestDatasetInfo
from tests.base_test_schema import TestColumn, DEFAULT_ROW_COUNT, DEFAULT_GROUP_COUNT, DEFAULT_GROUP_COLUMN, \
    DEFAULT_TIMESTAMP_COLUMN, datafile_schema, BASE_TIME
from tests.mock_s3_utils import SKIP_MOCK_S3_TESTS
from tests.task_and_job_utils import simple_run_task


# Note: not trying to test the query engine itself here, but that the query task "wraps it" correctly.
# Specifically, that part self-selection works (in PartSelectionMode.SELECTED_BY_WORKER)


def build_task_requests(test_ds: TestDatasetInfo,
                        query: dict,
                        part_selection_mode: PartSelectionMode,
                        parts: List[int],
                        copy_to_s3: bool = False,
                        attempt_no: int = 0) -> Tuple[List[QueryTaskRequest], List[DatasetPartId]]:
    if part_selection_mode == PartSelectionMode.SELECTED_BY_WORKER:
        assert attempt_no == 0  # Prevent errors in the test code (forcing an unsupported feature)
    validation_result = QueryValidator(query,
                                       dataset=test_ds.default_dataset_info,
                                       short_schema=datafile_schema()).expand_and_validate()
    assert validation_result.success
    if copy_to_s3:
        s3path = test_ds.copy_to_s3()
    else:
        s3path = None

    requests = []
    part_ids = []
    request_id = timestamped_uuid()
    for i, part_idx in enumerate(parts):
        part_id = test_ds.make_part(part_idx, s3path)
        if part_selection_mode == PartSelectionMode.SET_BY_INVOKER:
            invoker_set_part = part_id
        else:
            invoker_set_part = None

        req = QueryTaskRequest(
            dataset=test_ds.default_dataset_info,
            query=validation_result.expanded_query,
            mode=part_selection_mode,
            invoker_set_part=invoker_set_part,
            used_columns=[DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN, TestColumn.str_and_none],
            load_as_categoricals=None,
            attempt_no=attempt_no, invoke_time=time.time(), request_id=request_id, task_index=i)
        requests.append(req)
        part_ids.append(part_id)
    return requests, part_ids


def run_query_task(test_ds: TestDatasetInfo,
                   query: dict,
                   part_selection_mode: PartSelectionMode,
                   parts: Union[int, List[int]] = 0,
                   copy_to_s3: bool = False,
                   attempt_no: int = 0) -> Union[List[QueryTaskResult], QueryTaskResult]:
    single_mode = type(parts) is not list
    if single_mode:
        parts = [parts]
    task_requests, part_ids = build_task_requests(test_ds, query, part_selection_mode, parts, copy_to_s3)
    reqid = task_requests[0].request_id
    if part_selection_mode == PartSelectionMode.SELECTED_BY_WORKER:
        get_datastore().publish_for_worker_selection(reqid, attempt_no, set(part_ids))

    results = []
    for req in task_requests:
        result = cast(QueryTaskResult, simple_run_task(req, QueryTaskResult))
        assert result.status == TaskStatus.ENDED_SUCCESS
        results.append(result)

    if part_selection_mode == PartSelectionMode.SELECTED_BY_WORKER:
        assert get_datastore().self_select_part(reqid, attempt_no) is None  # No more parts to select from
    get_datastore().cleanup_request_data(reqid)
    return results[0] if single_mode else results


def test_empty_query():
    with new_test_dataset(1) as test_ds:
        task_result = run_query_task(test_ds, query={}, parts=0,
                                     part_selection_mode=PartSelectionMode.SET_BY_INVOKER)
        assert task_result.query_result.query.matching_groups == DEFAULT_GROUP_COUNT
        assert task_result.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT
        assert_metric_value(task_result.metrics, MetricName.SCANNED_GROUPS, DEFAULT_GROUP_COUNT)
        assert_metric_value(task_result.metrics, MetricName.SCANNED_ROWS, DEFAULT_ROW_COUNT)
        assert_label_value_exists(task_result.metrics, LoadFromLabel.SOURCE)
        assert_label_value_exists(task_result.metrics, PartSelectMethodLabel.SET_BY_INVOKER)
        assert task_result.query_result.funnel is None
        assert task_result.query_result.query.aggregations is None


def test_local_invoker_set_part():
    num_parts = 3
    with new_test_dataset(num_parts) as test_ds:
        query = {
            'query': {
                'conditions': [
                    {
                        "filter": [
                            DEFAULT_TIMESTAMP_COLUMN,
                            ">=",
                            BASE_TIME
                        ]
                    }
                ],
                'aggregations': [
                    {'column': TestColumn.str_and_none, 'name': 'cond_aggr', 'type': 'countPerValue'}
                ]
            },
            'funnel': {
                "sequence": [
                    {
                        "filter": [
                            DEFAULT_TIMESTAMP_COLUMN,
                            ">=",
                            BASE_TIME
                        ]
                    }
                ],
                "stepAggregations": [
                    {'column': TestColumn.str_and_none, 'name': 'funnel_step_aggr', 'type': 'countPerValue'}
                ],
                "endAggregations": [
                    {'column': TestColumn.str_and_none, 'name': 'funnel_end_aggr', 'type': 'countPerValue'}
                ]
            }
        }
        task_result = run_query_task(test_ds, query,
                                     parts=num_parts - 1,
                                     part_selection_mode=PartSelectionMode.SET_BY_INVOKER)
        assert task_result.query_result.query.matching_groups == DEFAULT_GROUP_COUNT
        assert task_result.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT
        assert len(task_result.query_result.funnel.sequence) == 1
        assert task_result.query_result.funnel.sequence[0].matching_groups == DEFAULT_GROUP_COUNT
        assert task_result.query_result.funnel.sequence[0].matching_group_rows == DEFAULT_ROW_COUNT
        assert_label_value_exists(task_result.metrics, LoadFromLabel.SOURCE)
        assert_label_value_exists(task_result.metrics, PartSelectMethodLabel.SET_BY_INVOKER)

        def validate_aggr(aggr_results: List[AggregationResult], expected_name: str):
            assert len(aggr_results) == 1
            aggr = aggr_results[0]
            assert aggr.name == expected_name
            # By checking the keys in 'str_and_none', test that the right keys for the given part idx exist
            assert aggr.column == TestColumn.str_and_none and aggr.type == "countPerValue"
            expected_aggr_keys = sorted(str_and_none_column_values(part=num_parts - 1, with_none=False))
            assert sorted(aggr.value.keys()) == expected_aggr_keys

        validate_aggr(task_result.query_result.query.aggregations, expected_name='cond_aggr')
        validate_aggr(task_result.query_result.funnel.sequence[0].aggregations, expected_name='funnel_step_aggr')
        validate_aggr(task_result.query_result.funnel.end_aggregations, expected_name='funnel_end_aggr')

        # Re-run: check that results are ok and that part was again loaded from source (as it is local)
        task_result = run_query_task(test_ds, query,
                                     parts=num_parts - 1,
                                     part_selection_mode=PartSelectionMode.SET_BY_INVOKER)
        assert task_result.query_result.query.matching_groups == DEFAULT_GROUP_COUNT
        assert task_result.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT
        assert_label_value_exists(task_result.metrics, LoadFromLabel.SOURCE)


@pytest.mark.skipif(SKIP_MOCK_S3_TESTS, reason="Skipping mock S3 tests")
def test_s3_invoker_set_part():
    with new_test_dataset(2) as test_ds:
        def validate_counts_and_source(part: int, source: LoadFromLabel):
            task_result = run_query_task(test_ds, query={}, parts=part,
                                         part_selection_mode=PartSelectionMode.SET_BY_INVOKER,
                                         copy_to_s3=True)
            assert task_result.query_result.query.matching_groups == DEFAULT_GROUP_COUNT
            assert task_result.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT
            assert_label_value_exists(task_result.metrics, source)
            assert_label_value_exists(task_result.metrics, PartSelectMethodLabel.SET_BY_INVOKER)

        validate_counts_and_source(0, LoadFromLabel.SOURCE)
        validate_counts_and_source(0, LoadFromLabel.DISK_CACHE)
        validate_counts_and_source(1, LoadFromLabel.SOURCE)
        validate_counts_and_source(1, LoadFromLabel.DISK_CACHE)


def test_worker_set_part_local():
    num_parts = 3
    with new_test_dataset(num_parts) as test_ds:
        def validate():
            task_results = run_query_task(test_ds, query={}, parts=list(range(num_parts)),
                                          part_selection_mode=PartSelectionMode.SELECTED_BY_WORKER,
                                          copy_to_s3=False)
            for res in task_results:
                assert res.query_result.query.matching_groups == DEFAULT_GROUP_COUNT
                assert res.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT
                assert_label_value_exists(res.metrics, LoadFromLabel.SOURCE)
                assert_label_value_exists(res.metrics, PartSelectMethodLabel.RANDOM_NO_CANDIDATES)

        validate()
        # And again - with no caching
        # And again - with no caching
        validate()


"""
Tests:
    Worker set part - S3, single part
        With clean cache, dataset of 3 parts
            first, random no candidate
            longer preflight?
                -> second, specific during preflight
                wait till end of preflight
            then, specific after preflight

    Worker set part - simulate multiple runners
        multiple threads with *separate caches*?
        four runners, four tasks
            3 of them have
            one has parts A,B       - should get its specific candidate
            another has parts A,B   - should get its specific candidate
            another has A,B       - should get random-candidates-taken
            another has none      - should get random-no-candidates

    From/to - filtered (no conditions query)

    * Worker set part - zero preflight

    needed columns
"""
