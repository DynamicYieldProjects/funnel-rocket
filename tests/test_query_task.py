import time
from typing import cast, List, Union, Tuple
import pytest
from frocket.common.dataset import DatasetPartId
from frocket.common.helpers.utils import timestamped_uuid
from frocket.common.metrics import MetricName, LoadFromLabel, PartSelectMethodLabel, MetricsBag, ComponentLabel
from frocket.common.tasks.base import TaskStatus
from frocket.common.tasks.query import QueryTaskRequest, PartSelectionMode, QueryTaskResult, AggregationResult
from frocket.common.validation.query_validator import QueryValidator
from frocket.datastore.registered_datastores import get_datastore
from frocket.worker.runners.base_task_runner import DEFAULT_PREFLIGHT_DURATION_MS
from frocket.worker.runners.part_loader import PartLoader
from tests.utils.base_test_utils import assert_metric_value, assert_label_value_exists, find_first_label_value, \
    get_metric_value
from tests.utils.dataset_utils import new_test_dataset, str_and_none_column_values, TestDatasetInfo, TestColumn, \
    DEFAULT_ROW_COUNT, DEFAULT_GROUP_COUNT, DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN, BASE_TIME, TIME_SHIFT, \
    datafile_schema
from tests.utils.mock_s3_utils import SKIP_S3_TESTS
from tests.utils.task_and_job_utils import simple_run_task

# Note: not trying to test the query engine itself here, but that the query task "wraps it" correctly.
# Specifically, that part self-selection works (in PartSelectionMode.SELECTED_BY_WORKER)

TEST_QUERY_WITH_AGGRS = {
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
            {'column': TestColumn.str_and_none, 'name': 'cond_aggr1', 'type': 'countPerValue'},
            {'column': TestColumn.str_and_none, 'name': 'cond_aggr2', 'type': 'count'}
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
            {'column': TestColumn.str_and_none, 'name': 'funnel_step_aggr1', 'type': 'countPerValue'},
            {'column': TestColumn.str_and_none, 'name': 'funnel_step_aggr2', 'type': 'count'}

        ],
        "endAggregations": [
            {'column': TestColumn.str_and_none, 'name': 'funnel_end_aggr1', 'type': 'countPerValue'},
            {'column': TestColumn.str_and_none, 'name': 'funnel_end_aggr2', 'type': 'count'}

        ]
    }
}


def build_task_requests(test_ds: TestDatasetInfo,
                        query: dict,
                        part_selection_mode: PartSelectionMode,
                        parts: List[int],
                        copy_to_s3: bool = False,
                        existing_s3path: str = None,
                        attempt_no: int = 0) -> Tuple[List[QueryTaskRequest], List[DatasetPartId]]:
    if part_selection_mode == PartSelectionMode.SELECTED_BY_WORKER:
        assert attempt_no == 0  # Prevent errors in the test code (forcing an unsupported feature)
    validation_result = QueryValidator(query,
                                       dataset=test_ds.default_dataset_info,
                                       short_schema=datafile_schema()).expand_and_validate()
    assert validation_result.success
    if existing_s3path:
        s3path = existing_s3path
    elif copy_to_s3:
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
            task_index = i
        else:
            invoker_set_part = None
            task_index = None

        req = QueryTaskRequest(
            dataset=test_ds.default_dataset_info,
            query=validation_result.expanded_query,
            mode=part_selection_mode,
            invoker_set_part=invoker_set_part,
            used_columns=[DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN, TestColumn.str_and_none.value],
            load_as_categoricals=None,
            attempt_no=attempt_no, invoke_time=time.time(), request_id=request_id, invoker_set_task_index=task_index)
        requests.append(req)
        part_ids.append(part_id)
    return requests, part_ids


def run_query_task(test_ds: TestDatasetInfo,
                   query: dict,
                   part_selection_mode: PartSelectionMode = PartSelectionMode.SET_BY_INVOKER,
                   parts: Union[int, List[int]] = 0,
                   private_part_loaders: List[PartLoader] = None,
                   copy_to_s3: bool = False,
                   existing_s3path: str = None,
                   preflight_duration_ms: int = None,
                   attempt_no: int = 0) -> Union[List[QueryTaskResult], QueryTaskResult]:
    single_mode = type(parts) is not list
    if single_mode:
        parts = [parts]
    task_requests, part_ids = build_task_requests(test_ds, query, part_selection_mode, parts,
                                                  copy_to_s3, existing_s3path)
    reqid = task_requests[0].request_id
    if part_selection_mode == PartSelectionMode.SELECTED_BY_WORKER:
        get_datastore().publish_for_worker_selection(reqid, attempt_no, set(part_ids))

    results = []
    for i, req in enumerate(task_requests):
        loader = private_part_loaders[i] if private_part_loaders else None
        result = cast(QueryTaskResult, simple_run_task(req, QueryTaskResult, loader, preflight_duration_ms))
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
        task_result = run_query_task(test_ds, TEST_QUERY_WITH_AGGRS,
                                     parts=num_parts - 1,
                                     part_selection_mode=PartSelectionMode.SET_BY_INVOKER)
        assert task_result.query_result.query.matching_groups == DEFAULT_GROUP_COUNT
        assert task_result.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT
        assert len(task_result.query_result.funnel.sequence) == 1
        assert task_result.query_result.funnel.sequence[0].matching_groups == DEFAULT_GROUP_COUNT
        assert task_result.query_result.funnel.sequence[0].matching_group_rows == DEFAULT_ROW_COUNT
        assert_label_value_exists(task_result.metrics, LoadFromLabel.SOURCE)
        assert_label_value_exists(task_result.metrics, PartSelectMethodLabel.SET_BY_INVOKER)

        # Totally hard-coded :-|
        def validate_aggr(aggr_results: List[AggregationResult], expected_name: str):
            assert len(aggr_results) == 2
            aggr1 = aggr_results[0]
            assert aggr1.name == expected_name + '1'
            # By checking the keys in 'str_and_none', test that the right keys for the given part idx exist
            assert aggr1.column == TestColumn.str_and_none and aggr1.type == "countPerValue"
            expected_aggr_keys = sorted(str_and_none_column_values(part=num_parts - 1, with_none=False))
            assert sorted(aggr1.value.keys()) == expected_aggr_keys

            aggr2 = aggr_results[1]
            assert aggr2.name == expected_name + '2'
            assert aggr2.column == TestColumn.str_and_none and aggr2.type == "count"
            assert aggr2.value == sum(aggr1.value.values())

        validate_aggr(task_result.query_result.query.aggregations, expected_name='cond_aggr')
        validate_aggr(task_result.query_result.funnel.sequence[0].aggregations, expected_name='funnel_step_aggr')
        validate_aggr(task_result.query_result.funnel.end_aggregations, expected_name='funnel_end_aggr')

        # Re-run: check that results are ok and that part was again loaded from source (as it is local)
        task_result = run_query_task(test_ds, TEST_QUERY_WITH_AGGRS,
                                     parts=num_parts - 1,
                                     part_selection_mode=PartSelectionMode.SET_BY_INVOKER)
        assert task_result.query_result.query.matching_groups == DEFAULT_GROUP_COUNT
        assert task_result.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT
        assert_label_value_exists(task_result.metrics, LoadFromLabel.SOURCE)


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
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
        validate()


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_worker_set_part_s3():
    with new_test_dataset(1) as test_ds:
        def validate(s3path: str, source: LoadFromLabel, select_method: PartSelectMethodLabel):
            res = run_query_task(test_ds, query={}, parts=0,
                                 part_selection_mode=PartSelectionMode.SELECTED_BY_WORKER,
                                 existing_s3path=s3path)
            print(res.to_json(indent=2))
            assert res.query_result.query.matching_groups == DEFAULT_GROUP_COUNT
            assert res.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT
            assert_label_value_exists(res.metrics, source)
            assert_label_value_exists(res.metrics, select_method)

        s3path = test_ds.copy_to_s3()
        validate(s3path, LoadFromLabel.SOURCE, PartSelectMethodLabel.RANDOM_NO_CANDIDATES)
        validate(s3path, LoadFromLabel.DISK_CACHE, PartSelectMethodLabel.SPECIFIC_CANDIDATE)


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_self_select_multi_loaders():
    """In worker self-select part mode, validate the expected actual select method:
    Given:
    * 4-part datasets
    * 4 "independent" task runners, each with a separate part loader (meaning, a separate disk cache)
    * The first 3 runners have parts [0, 1] in their cache already.
    Expected: giving each of the runners a chance to run, sequentially, we expect the first two to succesfully select
    eithr part 0 or 1. The third one should report "I have cached candidates, but they're taken already so selected
    a random one (2 or 3), and the last one should say: "I got no candidates, so selected at random".

    In a second run over the same parts, it's expected that the first 3 runner would all successfully select a candidate
    (the first two would take either 0 or 1 again, and the third would take either 2 or 3 - whatever it loaded before),
    and that the 4th and last one would *not* report "I have no candidates" (though it may or may not successfully
    select a candidate rather than pick a random part - as we're dealing with *sets* both at Redis and Python layers)

    We also test that the expected preflight time matches the actual select method: if a task has candidates, it will
    not sleep. If it doesn't, it will sleep till end of preflight. We test this with default and zero preflight values.
    """
    with new_test_dataset(4) as test_ds:
        s3path = test_ds.copy_to_s3()

        def test_for_preflight_duration(preflight_duration_ms: int = None):
            loaders = [PartLoader() for _ in range(4)]
            for loader in loaders[:-1]:
                for part_idx in [0, 1]:
                    metrics = MetricsBag(component=ComponentLabel.WORKER)
                    loader.load_dataframe(test_ds.make_part(part_idx, s3path), metrics)
                    assert_label_value_exists(metrics.finalize(success=True), LoadFromLabel.SOURCE)

            def run():
                task_results = run_query_task(
                    test_ds, query={},
                    part_selection_mode=PartSelectionMode.SELECTED_BY_WORKER,
                    parts=list(range(4)),
                    private_part_loaders=loaders,
                    existing_s3path=s3path,
                    preflight_duration_ms=preflight_duration_ms)

                methods = [find_first_label_value(task_result.metrics, PartSelectMethodLabel)
                           for task_result in task_results]
                preflight_sleeps = [get_metric_value(task_result.metrics, MetricName.TASK_PREFLIGHT_SLEEP_SECONDS)
                                    for task_result in task_results]
                return methods, preflight_sleeps

            actual_methods, sleeps = run()
            expected_methods = [e.label_value for e in [PartSelectMethodLabel.SPECIFIC_CANDIDATE,
                                                        PartSelectMethodLabel.SPECIFIC_CANDIDATE,
                                                        PartSelectMethodLabel.RANDOM_CANDIDATES_TAKEN,
                                                        PartSelectMethodLabel.RANDOM_NO_CANDIDATES]]
            assert sorted(actual_methods) == sorted(expected_methods)
            # Since first 3 had a candidate in mind, they don't spend time in preflight - unlike the last one
            # (unless preflight is disabled - i.e. set to zero)s
            assert sleeps[:-1] == [0, 0, 0]
            if preflight_duration_ms == 0:
                assert sleeps[-1] == 0
            else:
                assert 0 < sleeps[-1] < DEFAULT_PREFLIGHT_DURATION_MS

            # Run again with same loaders. This time all need to have some candidates, even if none
            actual_methods, sleeps = run()
            expected_methods = [PartSelectMethodLabel.SPECIFIC_CANDIDATE.label_value] * 3

            assert sorted(actual_methods[:-1]) == expected_methods
            assert actual_methods[-1] != PartSelectMethodLabel.RANDOM_NO_CANDIDATES.label_value
            assert sleeps == [0] * 4

        test_for_preflight_duration()  # Default
        test_for_preflight_duration(preflight_duration_ms=0)


def test_time_filters():
    with new_test_dataset(1) as test_ds:
        # All rows start at the 'from' value
        q = {"timeframe": {"from": BASE_TIME}}
        res = run_query_task(test_ds, q)
        assert res.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT

        # Only one row should match (have this value)
        q = {"timeframe": {"from": BASE_TIME + TIME_SHIFT}}
        res = run_query_task(test_ds, q)
        assert res.query_result.query.matching_group_rows == 1

        q = {"timeframe": {"from": BASE_TIME, "to": BASE_TIME + 1}}
        res = run_query_task(test_ds, q)
        assert res.query_result.query.matching_group_rows == 1

        q = {"timeframe": {"from": BASE_TIME, "to": BASE_TIME + TIME_SHIFT}}
        res = run_query_task(test_ds, q)
        assert res.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT - 1

        q = {"timeframe": {"from": BASE_TIME, "to": BASE_TIME + TIME_SHIFT + 1}}
        res = run_query_task(test_ds, q)
        assert res.query_result.query.matching_group_rows == DEFAULT_ROW_COUNT

        q = {"timeframe": {"to": BASE_TIME}}
        res = run_query_task(test_ds, q)
        assert res.query_result.query.matching_group_rows == 0

        q = {"timeframe": {"to": BASE_TIME + 1}}
        res = run_query_task(test_ds, q)
        assert res.query_result.query.matching_group_rows == 1
