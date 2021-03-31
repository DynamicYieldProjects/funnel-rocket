import math
import time
from dataclasses import dataclass, field
from typing import cast, List
import pytest
from frocket.common.config import config
from frocket.common.dataset import DatasetInfo
from frocket.common.helpers.utils import timestamped_uuid
from frocket.common.tasks.async_tracker import AsyncJobTracker, AsyncJobStage
from frocket.common.tasks.base import BaseJobResult, TaskStatus, JobStats
from frocket.common.tasks.query import QueryJobResult
from frocket.common.tasks.registration import RegisterArgs, DatasetValidationMode, REGISTER_DEFAULT_VALIDATION_MODE, \
    REGISTER_DEFAULT_VALIDATE_UNIQUES, RegistrationJobResult
from frocket.datastore.registered_datastores import get_datastore
from frocket.invoker import invoker_api
from frocket.invoker.stats_builder import TASK_COMPLETION_GRANULARITY_SECONDS
from tests.utils.base_test_utils import SKIP_LAMBDA_TESTS
from tests.utils.dataset_utils import TestDatasetInfo, DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN, TestColumn, \
    BASE_TIME, DEFAULT_GROUP_COUNT, DEFAULT_ROW_COUNT, new_test_dataset
# noinspection PyUnresolvedReferences
from tests.utils.redis_fixture import init_test_redis_settings

# TODO skip this whole module if no docker env/processes running - using a class?
# TODO test query/register timeout (sync/async)
# TODO Test S3 permission denied - how to simulate with mock? (probably should be test_registration_job)

ORIGINAL_INVOKER_TYPE = config['invoker']
INVOKER_TYPES = ['work_queue']
if not SKIP_LAMBDA_TESTS:
    INVOKER_TYPES.append('aws_lambda')
API_DATASET_NUM_PARTS = 3


@pytest.fixture(scope="session", autouse=True)
def set_invoker_intervals():
    config['invoker.retry.failed.interval'] = '0.05'


@pytest.fixture(params=INVOKER_TYPES)
def curr_invoker_type(request):
    config['invoker'] = request.param
    print(f"Running test with invoker type set to: {request.param}")
    yield request.param
    config['invoker'] = ORIGINAL_INVOKER_TYPE


@dataclass
class ApiDatasetInfo:
    test_ds: TestDatasetInfo
    remote_basepath: str
    datasets_to_clean: List[str] = field(default_factory=list)
    default_dataset: DatasetInfo = field(default=None)

    def build_register_args(self,
                            name_prefix: str = None,
                            override_basepath: str = None,
                            group_id_column: str = DEFAULT_GROUP_COLUMN,
                            validation_mode: DatasetValidationMode = REGISTER_DEFAULT_VALIDATION_MODE,
                            validate_uniques: bool = REGISTER_DEFAULT_VALIDATE_UNIQUES) -> RegisterArgs:
        return RegisterArgs(name=timestamped_uuid(name_prefix),
                            basepath=override_basepath or self.remote_basepath,
                            group_id_column=group_id_column,
                            timestamp_column=DEFAULT_TIMESTAMP_COLUMN,
                            validation_mode=validation_mode,
                            validate_uniques=validate_uniques)

    def _async_run(self, tracker: AsyncJobTracker) -> BaseJobResult:
        statuses = list(tracker.generator())
        for st in statuses:
            print(st)

        assert statuses[0].stage in [AsyncJobStage.STARTING, AsyncJobStage.RUNNING]
        assert statuses[-2].stage in [AsyncJobStage.FINISHING, AsyncJobStage.RUNNING]
        assert statuses[-1].stage == AsyncJobStage.DONE

        # Ensure that all "mid" stages are RUNNING, and that the no. of ENDED_SUCCESS can only increase
        first_running_idx = 0 if statuses[0].stage == AsyncJobStage.RUNNING else 1
        last_running_idx = -2 if statuses[-2].stage == AsyncJobStage.RUNNING else -3

        run_stages = statuses[first_running_idx:(last_running_idx + 1)]
        run_stages_count = len(run_stages)
        assert run_stages_count >= 1
        for i, status in enumerate(run_stages):
            assert status.stage == AsyncJobStage.RUNNING
            prev_status = run_stages[i - 1] if i > 0 else None
            if status.task_counters:
                num_tasks = sum(status.task_counters.values())
                assert 1 <= num_tasks <= API_DATASET_NUM_PARTS  # Exact no. of parts not exposed by invoker_api
                if prev_status and prev_status.task_counters:
                    assert status.task_counters.get(TaskStatus.ENDED_SUCCESS, 0) >= \
                           prev_status.task_counters.get(TaskStatus.ENDED_SUCCESS, 0)
        return statuses[-1].result

    def register(self, args: RegisterArgs = None,
                 run_async: bool = False,
                 expected_success_value: bool = True,
                 expected_basepath: str = None,
                 expected_size: int = None) -> RegistrationJobResult:
        args = args or self.build_register_args()
        self.datasets_to_clean.append(args.name)
        start_time = time.time()
        if run_async:
            tracker = invoker_api.register_dataset_async(args, set_max_wait=True)
            result = self._async_run(tracker)
        else:
            result = invoker_api.register_dataset(args)
        total_time = time.time() - start_time
        assert type(result) == RegistrationJobResult
        result = cast(RegistrationJobResult, result)
        if expected_success_value:
            assert result.success
            assert result.error_message is None
            assert result.dataset.total_parts == API_DATASET_NUM_PARTS

            expected_size = expected_size or self.test_ds.expected_parts.total_size
            expected_total_tasks = 1 if args.validation_mode == DatasetValidationMode.SINGLE else 2
            self.check_stats(result.stats, expected_total_tasks, expected_size, total_time)

            if expected_basepath:
                assert result.dataset.basepath == expected_basepath
        else:
            assert not result.success
            assert not result.dataset
        return result

    def get_default_dataset(self) -> DatasetInfo:
        if not self.default_dataset:
            self.default_dataset = self.register().dataset
        return self.default_dataset

    def query(self,
              query: dict,
              override_dataset: DatasetInfo = None,
              expected_size: int = None,
              run_async: bool = False,
              expected_success_value: bool = True) -> QueryJobResult:
        ds = override_dataset or self.get_default_dataset()
        start_time = time.time()
        if run_async:
            tracker = invoker_api.run_query_async(dataset=ds, query=query, set_max_wait=True)
            result = self._async_run(tracker)
        else:
            result = invoker_api.run_query(ds, query)
        total_time = time.time() - start_time
        assert type(result) == QueryJobResult
        result = cast(QueryJobResult, result)

        if expected_success_value:
            assert result.success
            assert result.error_message is None
            assert result.query
            expected_parts = override_dataset.total_parts if override_dataset else self.default_dataset.total_parts
            expected_size = expected_size or self.test_ds.expected_parts.total_size
            self.check_stats(result.stats,
                             expected_total_tasks=expected_parts,
                             expected_size=expected_size,
                             total_time=total_time)
        else:
            assert not result.success
            assert not result.query
            assert not result.funnel
        return result

    def check_stats(self, stats: JobStats, expected_total_tasks: int, expected_size: int, total_time: float):
        # Part count & size
        assert stats.dataset.parts == API_DATASET_NUM_PARTS
        assert stats.dataset.total_size == expected_size
        # Task count
        assert stats.invoker.total_tasks == expected_total_tasks and stats.invoker.failed_tasks == 0
        assert sum(stats.invoker.task_success_over_time.values()) == expected_total_tasks
        assert stats.worker.cold_tasks + stats.worker.warm_tasks == expected_total_tasks
        assert sum(stats.worker.cache.values()) == expected_total_tasks
        # Timing (0.5 / 0.25 of total were set low numbers for sanity, but CI tests are still flaky)
        assert total_time >= stats.total_time > 0  # TODO backlog fix flakiness in > (total_time * 0.5)
        assert total_time >= (stats.invoker.enqueue_time +
                              stats.invoker.poll_time) > 0  # TODO backlog fix flakiness in > (total_time * 0.25)
        max_key_in_completion_dict = \
            math.ceil(total_time / TASK_COMPLETION_GRANULARITY_SECONDS) * TASK_COMPLETION_GRANULARITY_SECONDS
        assert all([0 < ts <= max_key_in_completion_dict
                    for ts in stats.invoker.task_success_over_time.keys()])
        # Just a most basic sanity for the TimingStats
        for timing in [stats.worker.invoke_latency, stats.worker.load_time, stats.worker.total_time]:
            assert timing is not None
            assert all(0 < t < total_time for t in timing.values())
            assert min(timing.values()) == timing['min'] and max(timing.values()) == timing['max']
            assert timing['max'] >= timing['50%'] >= timing['min']

    def cleanup(self):
        self.test_ds.cleanup()
        for ds_name in self.datasets_to_clean:
            invoker_api.unregister_dataset(ds_name, force=True)

    @classmethod
    def instance(cls):
        if not hasattr(cls, '_shared_instance'):
            print("Creating ApiDatasetInfo shared instance (should happen once)")
            test_ds = TestDatasetInfo.build(parts=API_DATASET_NUM_PARTS)
            s3path = test_ds.copy_to_s3()
            info = ApiDatasetInfo(test_ds=test_ds,
                                  remote_basepath=s3path)
            cls._shared_instance = info
        return cls._shared_instance


@pytest.fixture(scope='module')
def api_dataset():
    info = cast(ApiDatasetInfo, ApiDatasetInfo.instance())
    yield info
    info.cleanup()


def test_register_remote(api_dataset: ApiDatasetInfo, curr_invoker_type):
    common_args = api_dataset.build_register_args()
    # Register local dataset
    result = api_dataset.register(common_args, expected_basepath=api_dataset.remote_basepath)
    first_id = result.dataset.id

    # Now re-register with same args (== same name), check that it works and registered_at is newer
    result = api_dataset.register(common_args, expected_basepath=api_dataset.remote_basepath)
    second_id = result.dataset.id

    assert first_id != second_id and \
           first_id.name == second_id.name and \
           first_id.registered_at < second_id.registered_at


def test_register_invalid_path(api_dataset: ApiDatasetInfo):
    args = api_dataset.build_register_args(override_basepath='/thisisnotadirectory')
    result = api_dataset.register(args, expected_success_value=False)

    args = api_dataset.build_register_args(override_basepath='/root')
    result = api_dataset.register(args, expected_success_value=False)

    args = api_dataset.build_register_args(override_basepath='s3://thisisnotabucket/')
    result = api_dataset.register(args, expected_success_value=False)


def test_register_invalid_column(api_dataset: ApiDatasetInfo, curr_invoker_type):
    args = api_dataset.build_register_args(group_id_column='notacolumn',
                                           validation_mode=DatasetValidationMode.SINGLE, validate_uniques=False)
    result = api_dataset.register(args, expected_success_value=False)


def test_register_async(api_dataset: ApiDatasetInfo, curr_invoker_type):
    result = api_dataset.register(run_async=True)


def test_register_async_fail(api_dataset: ApiDatasetInfo, curr_invoker_type):
    args = api_dataset.build_register_args(group_id_column='notacolumn',
                                           validation_mode=DatasetValidationMode.SINGLE, validate_uniques=False)
    result = api_dataset.register(args, run_async=True, expected_success_value=False)


def test_get_dataset_missing_none():
    result = invoker_api.get_dataset(name="notreallyman", throw_if_missing=False)
    assert result is None


@pytest.mark.xfail(strict=True)
def test_get_dataset_missing_xfail():
    _ = invoker_api.get_dataset(name="notreallyman", throw_if_missing=True)


def test_get_dataset(api_dataset: ApiDatasetInfo):
    job_result = api_dataset.register(api_dataset.build_register_args(name_prefix="getme-"))
    assert job_result.dataset.id.name.startswith("getme-")
    ds = invoker_api.get_dataset(job_result.dataset.id.name, throw_if_missing=True)
    assert ds == job_result.dataset


def test_get_dataset_detailed(api_dataset: ApiDatasetInfo):
    job_result = api_dataset.register()
    ds = invoker_api.get_dataset(job_result.dataset.id.name)
    short_schema = invoker_api.get_dataset_schema(ds, full=False)
    full_schema = invoker_api.get_dataset_schema(ds, full=True)
    parts_info = invoker_api.get_dataset_parts(ds)
    # The details of these objects are tested elsewhere, here we're mainly testing that invoker_api fetches them...
    assert short_schema == get_datastore().short_schema(ds)
    assert full_schema == get_datastore().schema(ds)
    assert parts_info == api_dataset.test_ds.expected_parts and parts_info == get_datastore().dataset_parts_info(ds)


def test_unregister_no_usage(api_dataset: ApiDatasetInfo):
    def unreg(force: bool):
        ds_name = api_dataset.register().dataset.id.name
        unreg_result = invoker_api.unregister_dataset(ds_name, force=force)
        assert unreg_result.success and unreg_result.error_message is None
        assert unreg_result.dataset_found and unreg_result.dataset_last_used is None

        ds = invoker_api.get_dataset(ds_name, throw_if_missing=False)
        assert not ds

        unreg_result = invoker_api.unregister_dataset(ds_name, force=force)
        assert unreg_result.success and unreg_result.error_message is None
        assert not unreg_result.dataset_found and unreg_result.dataset_last_used is None

    unreg(force=False)
    unreg(force=True)


def test_unregister_after_use_force(api_dataset: ApiDatasetInfo):
    # fail, force, wait, set to zero...
    ds = api_dataset.register().dataset
    api_dataset.query(query={}, override_dataset=ds, )

    unreg_result = invoker_api.unregister_dataset(ds.id.name)
    assert not unreg_result.success
    assert invoker_api.get_dataset(ds.id.name, throw_if_missing=True)

    unreg_result = invoker_api.unregister_dataset(ds.id.name, force=True)
    assert unreg_result.success and unreg_result.dataset_found and unreg_result.dataset_last_used
    assert invoker_api.get_dataset(ds.id.name, throw_if_missing=False) is None


def test_unregister_after_use_wait(api_dataset: ApiDatasetInfo):
    orig_interval = config['unregister.last.used.interval']
    try:
        config['unregister.last.used.interval'] = '100'
        ds = api_dataset.register().dataset
        start = time.time()
        api_dataset.query(query={}, override_dataset=ds)
        unreg_result = invoker_api.unregister_dataset(ds.id.name)
        assert not unreg_result.success

        time_passed = time.time() - start
        next_second = int(math.ceil(time_passed))
        config['unregister.last.used.interval'] = str(next_second)
        time.sleep(next_second - time_passed + 0.2)
        unreg_result = invoker_api.unregister_dataset(ds.id.name)
        assert unreg_result.success
    finally:
        config['unregister.last.used.interval'] = orig_interval


def test_list_datasets(api_dataset: ApiDatasetInfo):
    original_list = invoker_api.list_datasets()

    ds1_args = api_dataset.build_register_args()
    ds1 = api_dataset.register(ds1_args).dataset
    ds2 = api_dataset.register().dataset
    updated_list = invoker_api.list_datasets()
    assert len(updated_list) == len(original_list) + 2
    for e in [*original_list, ds1, ds2]:
        assert e in updated_list

    # Re-register dataset with same name
    ds1new = api_dataset.register(ds1_args).dataset
    updated_list = invoker_api.list_datasets()
    assert len(updated_list) == len(original_list) + 2
    for e in [*original_list, ds1new, ds2]:
        assert e in updated_list

    assert invoker_api.unregister_dataset(ds1new.id.name).success
    assert invoker_api.unregister_dataset(ds2.id.name).success
    updated_list = invoker_api.list_datasets()
    assert len(updated_list) == len(original_list)
    assert sorted(updated_list, key=lambda d: d.id.name) == sorted(original_list, key=lambda d: d.id.name)


def run_and_validate_query(api_dataset: ApiDatasetInfo, run_async: bool):
    query = {
        'query': {
            'conditions': [
                {'filter': [TestColumn.int_64_ts.value, '>=', BASE_TIME]}
            ]
        },
        'funnel': {
            'sequence': [
                {'filter': [TestColumn.int_64_ts.value, '==', BASE_TIME]}
            ],
            'endAggregations': [
                {'column': TestColumn.str_userid, 'type': 'count'}
            ]
        }
    }

    query_result = api_dataset.query(query, run_async=run_async)
    print(query_result.to_json(indent=2))
    assert query_result.query.matching_groups == DEFAULT_GROUP_COUNT * API_DATASET_NUM_PARTS
    assert query_result.query.matching_group_rows == DEFAULT_ROW_COUNT * API_DATASET_NUM_PARTS
    assert query_result.query.aggregations is None
    assert query_result.stats.worker.scanned_groups == DEFAULT_GROUP_COUNT * API_DATASET_NUM_PARTS
    assert query_result.stats.worker.scanned_rows == DEFAULT_ROW_COUNT * API_DATASET_NUM_PARTS

    assert len(query_result.funnel.sequence) == 1
    assert query_result.funnel.sequence[0].matching_groups == 1
    funnel_row_count = query_result.funnel.sequence[0].matching_group_rows
    assert 1 <= funnel_row_count < DEFAULT_ROW_COUNT
    assert query_result.funnel.sequence[0].aggregations is None

    assert len(query_result.funnel.end_aggregations) == 1
    assert query_result.funnel.end_aggregations[0].value == funnel_row_count


def test_query(api_dataset: ApiDatasetInfo, curr_invoker_type):
    run_and_validate_query(api_dataset, run_async=False)


def test_query_async(api_dataset: ApiDatasetInfo, curr_invoker_type):
    iterations = 5  # A bit more certainty on our asserts around async job progress
    for _ in range(iterations):
        run_and_validate_query(api_dataset, run_async=True)


@pytest.mark.xfail(strict=True)
def test_query_invalid_xfail(api_dataset: ApiDatasetInfo):
    query = {'query': {'conditions': [{'filter': [TestColumn.int_64_ts.value, 'contains', BASE_TIME]}]}}
    api_dataset.query(query)


def run_query_missing_files(api_dataset: ApiDatasetInfo, run_async: bool):
    with new_test_dataset(API_DATASET_NUM_PARTS) as test_ds:
        s3path = test_ds.copy_to_s3()
        query = {}

        register_args = RegisterArgs(
            name=timestamped_uuid(),
            basepath=s3path,
            group_id_column=DEFAULT_GROUP_COLUMN,
            timestamp_column=DEFAULT_TIMESTAMP_COLUMN)

        ds = api_dataset.register(register_args,
                                  expected_basepath=s3path,
                                  expected_size=test_ds.expected_parts.total_size).dataset
        api_dataset.query(query,
                          override_dataset=ds,
                          expected_size=test_ds.expected_parts.total_size,
                          expected_success_value=True)

        # Re-register the dataset, so workers won't use disk cache but rather fetch the (missing) files from source
        ds = api_dataset.register(register_args,
                                  expected_basepath=s3path,
                                  expected_size=test_ds.expected_parts.total_size).dataset
        for key in test_ds.bucket.objects.all():
            key.delete()
        api_dataset.query(query, override_dataset=ds, expected_success_value=False)


def test_query_missing_files_failure(api_dataset: ApiDatasetInfo, curr_invoker_type):
    run_query_missing_files(api_dataset, run_async=False)


def test_query_async_fail(api_dataset: ApiDatasetInfo, curr_invoker_type):
    run_query_missing_files(api_dataset, run_async=True)
