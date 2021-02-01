import time
from typing import cast

from frocket.common.helpers.utils import timestamped_uuid
from frocket.common.tasks.base import TaskStatus
from frocket.common.tasks.query import QueryTaskRequest, PartSelectionMode, QueryTaskResult
from frocket.common.validation.query_validator import QueryValidator
from frocket.datastore.registered_datastores import get_datastore
from tests.dataset_utils import new_test_dataset, str_and_none_column_values, TestDatasetInfo
from tests.base_test_schema import TestColumn, DEFAULT_ROW_COUNT, DEFAULT_GROUP_COUNT, DEFAULT_GROUP_COLUMN, \
    DEFAULT_TIMESTAMP_COLUMN, datafile_schema
from tests.task_and_job_utils import simple_run_task
from tests.test_part_loader import clean_loader_cache


def run_query_task(test_ds: TestDatasetInfo,
                   query: dict,
                   part_selection_mode: PartSelectionMode,
                   part_idx: int = 0) -> QueryTaskResult:
    validation_result = QueryValidator(query,
                                       dataset=test_ds.default_dataset_info,
                                       short_schema=datafile_schema()).expand_and_validate()
    assert validation_result.success
    invoker_set_part = test_ds.make_part(part_idx) if part_selection_mode == PartSelectionMode.SET_BY_INVOKER else None
    req = QueryTaskRequest(
        dataset=test_ds.default_dataset_info,
        query=query,
        mode=part_selection_mode,
        invoker_set_part=invoker_set_part,
        used_columns=[DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN, TestColumn.str_and_none],
        load_as_categoricals=None,
        attempt_no=0, invoke_time=time.time(), request_id=timestamped_uuid(), task_index=0)

    result = cast(QueryTaskResult, simple_run_task(req, QueryTaskResult))
    get_datastore().cleanup_request_data(req.request_id)
    assert result.status == TaskStatus.ENDED_SUCCESS
    return result


def test_empty_query():
    with new_test_dataset(1) as test_ds:
        task_result = run_query_task(test_ds, query={}, part_idx=0,
                                     part_selection_mode=PartSelectionMode.SET_BY_INVOKER)
        assert task_result.query_result.query.matching_groups == DEFAULT_GROUP_COUNT
        assert task_result.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT
        assert task_result.query_result.funnel is None
        assert task_result.query_result.query.aggregations is None


def test_with_invoker_set_part():
    # Ensure that (a) a no-condition query brings back all expected groups and rows,
    # and (b) a basic aggregation also returns - and has a value in it that it specific to the part idx requested
    # (ensuring that the correct part was loaded)
    num_parts = 3
    with new_test_dataset(num_parts) as test_ds:
        query = {
            'query': {
                'aggregations': [
                    {'column': TestColumn.str_and_none, 'type': 'countPerValue'}
                ]}}
        task_result = run_query_task(test_ds, query,
                                     part_idx=num_parts-1,
                                     part_selection_mode=PartSelectionMode.SET_BY_INVOKER)
        assert task_result.query_result.query.matching_groups == DEFAULT_GROUP_COUNT
        assert task_result.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT
        assert task_result.query_result.funnel is None
        aggrs = task_result.query_result.query.aggregations
        assert len(aggrs) == 1
        # By checking the keys in 'str_and_none', test that the right keys for the given part idx exist
        assert aggrs[0].column == TestColumn.str_and_none and aggrs[0].type == "countPerValue"
        expected_aggr_keys = sorted(str_and_none_column_values(part=num_parts - 1, with_none=False))
        assert sorted(aggrs[0].value.keys()) == expected_aggr_keys


"""
{
  "matching_groups": 200,
  "matching_group_rows": 1000,
  "aggregations": [
    {
      "column": "str_and_none",
      "type": "countPerValue",
      "value": {
        "3": 206,
        "part-2": 203,
        "2": 194,
        "1": 193
      },
      "top": 10,
      "name": null
    }
  ]
}

Tests:

    Invoker set part
        with clean cache:
            minimal no-conditions query
            check all groups and rows
                check in metrics also
            check select method enum label
            local - correct part is loaded
            s3 - correct part, from source
            from source, then from cache

            task runner again from S3
                from source

            task runner again from S3
                from cache

    Worker set part - local
        With clean cache:
            always RANDOM_NO_CANDIDATES

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
