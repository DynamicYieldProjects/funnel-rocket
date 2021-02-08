from collections import Counter
from dataclasses import dataclass
from typing import cast, List
import pytest

from frocket.common.config import config
from frocket.common.dataset import DatasetInfo, DatasetPartsInfo, DatasetShortSchema
from frocket.common.tasks.base import BaseTaskResult
from frocket.common.tasks.query import QueryJobResult, PartSelectionMode, AggregationResult, FunnelResult, \
    QueryConditionsResult
from frocket.common.tasks.registration import DatasetValidationMode, RegistrationJobResult
from frocket.datastore.registered_datastores import get_datastore
from frocket.invoker import invoker_api
from frocket.invoker.jobs.query_job import QueryJob
from tests.utils.dataset_utils import TestDatasetInfo, str_and_none_column_values, TestColumn, DEFAULT_ROW_COUNT, \
    DEFAULT_GROUP_COUNT
from tests.utils.task_and_job_utils import build_registration_job, query_job_invoker, registration_job_invoker, \
    build_query_job
from tests.test_query_task import run_query_task, TEST_QUERY_WITH_AGGRS

QUERY_DATASET_NUM_PARTS = 4


@dataclass
class QueryDatasetInfo:
    test_ds: TestDatasetInfo
    local_dataset: DatasetInfo
    remote_dataset: DatasetInfo
    parts_info: DatasetPartsInfo
    short_schema: DatasetShortSchema

    def cleanup(self):
        invoker_api.unregister_dataset(self.local_dataset.id.name)
        invoker_api.unregister_dataset(self.remote_dataset.id.name)
        self.test_ds.cleanup()

    @classmethod
    def shared_instance(cls):
        if not hasattr(cls, '_shared_instance'):
            print("Creating QueryDatasetInfo shared instance (should happen once)")
            test_ds = TestDatasetInfo.build(parts=QUERY_DATASET_NUM_PARTS)
            job = build_registration_job(test_ds.basepath, mode=DatasetValidationMode.SINGLE)
            with registration_job_invoker(job) as invoker:
                job_result = cast(RegistrationJobResult, invoker.run_all_stages())  # Asserts success by default
            local_ds = job_result.dataset

            s3path = test_ds.copy_to_s3()
            job = build_registration_job(s3path, mode=DatasetValidationMode.SINGLE)
            with registration_job_invoker(job) as invoker:
                job_result = cast(RegistrationJobResult, invoker.run_all_stages())
            remote_ds = job_result.dataset

            info = QueryDatasetInfo(test_ds=test_ds,
                                    local_dataset=local_ds,
                                    remote_dataset=remote_ds,
                                    parts_info=get_datastore().dataset_parts_info(local_ds),
                                    short_schema=get_datastore().short_schema(local_ds))
            cls._shared_instance = info
        return cls._shared_instance


@pytest.fixture(scope='module')
def query_dataset_info():
    info = cast(QueryDatasetInfo, QueryDatasetInfo.shared_instance())
    yield
    info.cleanup()


def query_job(query: dict,
              remote_dataset: bool = False,
              worker_can_select_part: bool = None) -> QueryJob:
    query_ds_info = QueryDatasetInfo.shared_instance()
    ds = query_ds_info.remote_dataset if remote_dataset else query_ds_info.local_dataset
    return build_query_job(query, ds, query_ds_info.parts_info, query_ds_info.short_schema, worker_can_select_part)


def run_query(query: dict,
              remote_dataset: bool,
              worker_can_select_part: bool,
              iterations: int = 1,
              expected_groups: int = DEFAULT_GROUP_COUNT * QUERY_DATASET_NUM_PARTS,
              expected_rows: int = DEFAULT_ROW_COUNT * QUERY_DATASET_NUM_PARTS,
              expected_aggregations: List[AggregationResult] = None,
              expected_funnel: FunnelResult = None):

    def sorted_aggregations(aggrs: List[AggregationResult]) -> List[AggregationResult]:
        return sorted(aggrs, key=lambda ag: f"{ag.column}-{ag.type}-{ag.name}")

    for _ in range(iterations):
        job = query_job(query=query, worker_can_select_part=worker_can_select_part, remote_dataset=remote_dataset)
        with query_job_invoker(job) as invoker:
            job_result = cast(QueryJobResult, invoker.run_all_stages())

        if worker_can_select_part:
            assert len(job.dataset_parts_to_publish()) == QUERY_DATASET_NUM_PARTS
        else:
            assert job.dataset_parts_to_publish() is None

        assert job_result.query.matching_groups == expected_groups
        assert job_result.query.matching_group_rows == expected_rows

        if not expected_aggregations:
            assert not job_result.query.aggregations
        else:
            # noinspection PyTypeChecker
            print(expected_aggregations)
            print(job_result.query.aggregations)
            assert sorted_aggregations(expected_aggregations) == sorted_aggregations(job_result.query.aggregations)

        if not expected_funnel:
            assert not job_result.funnel
        else:
            assert job_result.funnel == expected_funnel


def test_empty_query_local_invoker_set():
    # Running a few times just to see that nothing breaks
    run_query(query={}, remote_dataset=False, worker_can_select_part=False, iterations=2)


def test_empty_query_local_worker_set():
    run_query(query={}, remote_dataset=False, worker_can_select_part=True, iterations=2)


def test_empty_query_remote_invoker_set():
    # Running a few times just to see that nothing breaks
    run_query(query={}, remote_dataset=True, worker_can_select_part=False, iterations=2)


def test_empty_query_remote_worker_set():
    run_query(query={}, remote_dataset=True, worker_can_select_part=True, iterations=2)


def test_query_with_aggrs():
    # Get individual task results
    num_parts = QueryDatasetInfo.shared_instance().parts_info.total_parts
    task_results = run_query_task(query=TEST_QUERY_WITH_AGGRS,
                                  test_ds=QueryDatasetInfo.shared_instance().test_ds,
                                  part_selection_mode=PartSelectionMode.SET_BY_INVOKER,
                                  parts=list(range(num_parts)))

    expected_aggr_keys = set()
    for i in range(num_parts):
        expected_aggr_keys.update(str_and_none_column_values(part=i, with_none=False))
    expected_aggr_map = Counter()
    for res in task_results:
        expected_aggr_map.update(res.query_result.query.aggregations[0].value)
    assert sorted(expected_aggr_keys) == sorted(expected_aggr_map.keys())
    expected_count_total = sum(expected_aggr_map.values())

    default_top_count = config.int('aggregations.top.default.count')

    def expected_aggr_results(aggrs_list: List[dict]) -> List[AggregationResult]:
        assert len(aggrs_list) == 2  # Matches test query
        return [
            AggregationResult(**aggrs_list[0],
                              top=default_top_count, value=expected_aggr_map),
            AggregationResult(**aggrs_list[1],
                              top=None, value=expected_count_total),
        ]

    expected_funnel = FunnelResult(
        sequence=[
            QueryConditionsResult(
                matching_groups=DEFAULT_GROUP_COUNT * QUERY_DATASET_NUM_PARTS,
                matching_group_rows=DEFAULT_ROW_COUNT * QUERY_DATASET_NUM_PARTS,
                aggregations=expected_aggr_results(TEST_QUERY_WITH_AGGRS['funnel']['stepAggregations'])
            )
        ],
        end_aggregations=expected_aggr_results(TEST_QUERY_WITH_AGGRS['funnel']['endAggregations'])
    )

    run_query(query=TEST_QUERY_WITH_AGGRS, remote_dataset=False, worker_can_select_part=False,
              expected_aggregations=expected_aggr_results(TEST_QUERY_WITH_AGGRS['query']['aggregations']),
              expected_funnel=expected_funnel)


def test_reduce_ok():
    elem1 = FunnelResult(
        sequence=[
            QueryConditionsResult(matching_groups=100, matching_group_rows=1000, aggregations=[
                AggregationResult(column="c1", type="count", name=None, value=10000, top=None),
                AggregationResult(column="c2", type="countPerValue", name="pv", value={
                    "funny": 20, "few": 4, "not funny": 10, "few2": 3, "a1": 1, "medium1": 5}, top=3)
            ]),
            QueryConditionsResult(matching_groups=50, matching_group_rows=500, aggregations=[
                AggregationResult(column="c1", type="count", name=None, value=20000, top=None),
                AggregationResult(column="c2", type="countPerValue", name="pv", value={
                    "funny": 40, "few": 8, "not funny": 20, "few2": 6, "medium": 2, "another": 1}, top=6)
            ]),
        ],
        end_aggregations=[
            AggregationResult(column="ce2", type="countPerValue", name="pv", top=2,
                              value={f"value{i}": i*10 for i in range(1, 101)})
        ]
    )

    elem2 = FunnelResult(
        sequence=[
            QueryConditionsResult(matching_groups=1000, matching_group_rows=10000, aggregations=[
                AggregationResult(column="c1", type="count", name=None, value=100000, top=None),
                AggregationResult(column="c2", type="countPerValue", name="pv", value={
                    "funny": 200, "few": 40, "not funny": 100, "few2": 30, "a2": 10, "medium2": 499}, top=3)
            ]),
            QueryConditionsResult(matching_groups=500, matching_group_rows=5000, aggregations=[
                AggregationResult(column="c1", type="count", name=None, value=200000, top=None),
                AggregationResult(column="c2", type="countPerValue", name="pv", value={
                    "super-funny": 4001, "few2": 1}, top=6)
            ]),
        ],
        end_aggregations=[
            AggregationResult(column="ce2", type="countPerValue", name="pv", top=2,
                              value={f"value{i}": i * 3 for i in range(1, 201)})
        ]
    )

    elems = [elem1]*100 + [elem2]

    expected_result = FunnelResult(
            sequence=[
                QueryConditionsResult(matching_groups=11000, matching_group_rows=110000, aggregations=[
                    AggregationResult(column="c1", type="count", name=None, value=1100000, top=None),
                    AggregationResult(column="c2", type="countPerValue", name="pv", value={
                        "funny": 2200, "not funny": 1100, "medium1": 500}, top=3)
                ]),
                QueryConditionsResult(matching_groups=5500, matching_group_rows=55000, aggregations=[
                    AggregationResult(column="c1", type="count", name=None, value=2200000, top=None),
                    AggregationResult(column="c2", type="countPerValue", name="pv", value={
                        "super-funny": 4001, "funny": 4000, "not funny": 2000, "few": 800, "few2": 601, "medium": 200},
                                      top=6)
                ]),
            ],
            end_aggregations=[
                AggregationResult(column="ce2", type="countPerValue", name="pv", top=2,
                                  value={"value100": 100300, "value99": 99297})
            ]
        )

    reduced_result = cast(FunnelResult, FunnelResult.reduce(elems))
    assert reduced_result == expected_result


def test_reduce_with_none_empty():
    elems = [
        QueryConditionsResult(matching_groups=0, matching_group_rows=0, aggregations=None),
        QueryConditionsResult(matching_groups=1, matching_group_rows=1, aggregations=None),
        QueryConditionsResult(matching_groups=2, matching_group_rows=2, aggregations=None)
    ]
    res = cast(QueryConditionsResult, QueryConditionsResult.reduce(elems))
    assert res.matching_groups == 3 and res.matching_group_rows == 3
    assert res.aggregations is None

    elems = [
        QueryConditionsResult(matching_groups=0, matching_group_rows=0, aggregations=[]),
        QueryConditionsResult(matching_groups=1, matching_group_rows=1, aggregations=[]),
        QueryConditionsResult(matching_groups=2, matching_group_rows=2, aggregations=[])
    ]
    res = cast(QueryConditionsResult, QueryConditionsResult.reduce(elems))
    assert res.matching_groups == 3 and res.matching_group_rows == 3
    assert res.aggregations == []


@pytest.mark.xfail(strict=True)
def test_reduce_partially_none_xfail():
    elems = [
        QueryConditionsResult(matching_groups=0, matching_group_rows=0, aggregations=None),
        QueryConditionsResult(matching_groups=1, matching_group_rows=1, aggregations=None),
        QueryConditionsResult(matching_groups=2, matching_group_rows=2, aggregations=[])
    ]
    QueryConditionsResult.reduce(elems)


@pytest.mark.xfail(strict=True)
def test_reduce_wrong_class_xfail():
    elems = [
        QueryConditionsResult(matching_groups=0, matching_group_rows=0, aggregations=None),
        QueryConditionsResult(matching_groups=1, matching_group_rows=1, aggregations=None),
    ]
    FunnelResult.reduce(elems)


@pytest.mark.xfail(strict=True)
def test_reduce_aggrs_len_xfail():
    elems = [
        QueryConditionsResult(matching_groups=0, matching_group_rows=0, aggregations=[
            AggregationResult(column="c", type="count", name="n", value=1, top=None)
        ]),
        QueryConditionsResult(matching_groups=1, matching_group_rows=1, aggregations=[
            AggregationResult(column="c", type="count", name="n", value=1, top=None),
            AggregationResult(column="c2", type="count", name="n2", value=1, top=None)
        ]),
    ]
    QueryConditionsResult.reduce(elems)


@pytest.mark.xfail(strict=True)
def test_reduce_different_aggrs_xfail():
    elems = [
        QueryConditionsResult(matching_groups=0, matching_group_rows=0, aggregations=[
            AggregationResult(column="c1", type="count", name="n", value=1, top=None)
        ]),
        QueryConditionsResult(matching_groups=1, matching_group_rows=1, aggregations=[
            AggregationResult(column="c2", type="count", name="n", value=1, top=None),
        ]),
    ]
    QueryConditionsResult.reduce(elems)


@pytest.mark.xfail(strict=True)
def test_reduce_different_value_types_xfail():
    elems = [
        QueryConditionsResult(matching_groups=0, matching_group_rows=0, aggregations=[
            AggregationResult(column="c1", type="count", name="n", value=1, top=None)
        ]),
        QueryConditionsResult(matching_groups=1, matching_group_rows=1, aggregations=[
            AggregationResult(column="c1", type="count", name="n", value={"a": 1}, top=None),
        ]),
    ]
    QueryConditionsResult.reduce(elems)


# TODO explain this mess...
def test_build_retry_task():
    num_parts = QueryDatasetInfo.shared_instance().parts_info.total_parts
    job = query_job(
        query={'query': {'aggregations': [{'column': TestColumn.str_userid.value, 'type': 'count'}]}},
        worker_can_select_part=True)
    iterations = 5
    for _ in range(iterations):
        with query_job_invoker(job) as invoker:
            invoker.prerun()
            invoker.enqueue_tasks()
            results = invoker.run_tasks()
            assert len(results) == num_parts

            per_task_retries = 2
            for _ in range(per_task_retries):
                results = invoker.retry(task_index=0)
                results = invoker.retry(task_index=num_parts - 1)

            assert len(results) == num_parts + (per_task_retries * 2)
            results = sorted(results, key=lambda res: res.task_index)

            def ensure_results_equal(results: List[BaseTaskResult]):
                dicts = []
                for res in results:
                    d = res.to_dict()
                    d.pop('metrics')
                    dicts.append(d)
                assert all([d == dicts[0] for d in dicts[1:]])

            ensure_results_equal(results[:(per_task_retries+1)])
            ensure_results_equal(results[-(per_task_retries+1):])

            # Now ensure that only one result per task index (== part idx in this case) is used for aggregation,
            # regardless of no. of retries
            invoker.complete()
            job_result = cast(QueryJobResult, invoker.build_result())
            assert job_result.query.matching_groups == DEFAULT_GROUP_COUNT * QUERY_DATASET_NUM_PARTS
            assert job_result.query.matching_group_rows == DEFAULT_ROW_COUNT * QUERY_DATASET_NUM_PARTS
            assert job_result.query.aggregations[0].value == DEFAULT_ROW_COUNT * QUERY_DATASET_NUM_PARTS
