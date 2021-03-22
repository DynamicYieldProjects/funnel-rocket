import datetime
import os
from typing import Dict, List
import pandas
import pytest
from frocket.common.dataset import DatasetPartId, DatasetId
from frocket.common.helpers.utils import timestamped_uuid
from frocket.common.metrics import MetricsBag, ComponentLabel, LoadFromLabel
from tests.utils.dataset_utils import new_test_dataset, are_test_dfs_equal, clean_loader_cache, TestColumn, BASE_TIME, \
    TIME_SHIFT
from frocket.worker.runners.part_loader import shared_part_loader, FilterPredicate
from tests.utils.mock_s3_utils import SKIP_S3_TESTS


def test_local_non_cached():
    with clean_loader_cache():
        with new_test_dataset(4) as test_ds:
            dataset_id = DatasetId.now(timestamped_uuid())
            original_df = pandas.read_parquet(test_ds.fullpath_files[0])
            file_id = DatasetPartId(dataset_id, path=test_ds.fullpath_files[0], part_idx=0)

            metrics = MetricsBag(component=ComponentLabel.WORKER)
            loaded_df = shared_part_loader().load_dataframe(file_id, metrics=metrics)
            assert are_test_dfs_equal(original_df, loaded_df)
            assert metrics.label_value(LoadFromLabel) == LoadFromLabel.SOURCE.label_value

            metrics = MetricsBag(component=ComponentLabel.WORKER)
            loaded_df = shared_part_loader().load_dataframe(file_id, metrics=metrics)
            assert are_test_dfs_equal(original_df, loaded_df)
            assert metrics.label_value(LoadFromLabel) == LoadFromLabel.SOURCE.label_value


def test_needed_columns():
    with clean_loader_cache():
        with new_test_dataset(1) as test_ds:
            dataset_id = DatasetId.now(timestamped_uuid())
            original_df = pandas.read_parquet(test_ds.fullpath_files[0])
            file_id = DatasetPartId(dataset_id, path=test_ds.fullpath_files[0], part_idx=0)
            cols = [TestColumn.int_64_userid, TestColumn.str_userid, TestColumn.str_all_none,
                    TestColumn.float_32, TestColumn.str_category_many]

            metrics = MetricsBag(component=ComponentLabel.WORKER)
            loaded_df = shared_part_loader().load_dataframe(file_id, metrics=metrics, needed_columns=cols)
            assert are_test_dfs_equal(original_df[cols], loaded_df)
            assert metrics.label_value(LoadFromLabel) == LoadFromLabel.SOURCE.label_value

            metrics = MetricsBag(component=ComponentLabel.WORKER)
            loaded_df = shared_part_loader().load_dataframe(file_id, metrics=metrics, needed_columns=cols)
            assert are_test_dfs_equal(original_df[cols], loaded_df)
            assert metrics.label_value(LoadFromLabel) == LoadFromLabel.SOURCE.label_value

            metrics = MetricsBag(component=ComponentLabel.WORKER)
            loaded_df = shared_part_loader().load_dataframe(file_id, metrics=metrics, needed_columns=cols,
                                                            load_as_categoricals=['str_userid'])
            assert loaded_df['str_userid'].dtype == 'category'
            assert are_test_dfs_equal(original_df[cols], loaded_df)


def test_filters():
    # Note that we're not testing Arrow's filter functionality, but rather that we're using it as intended
    with clean_loader_cache():
        with new_test_dataset(1) as test_ds:
            dataset_id = DatasetId.now(timestamped_uuid())
            original_df = pandas.read_parquet(test_ds.fullpath_files[0])
            file_id = DatasetPartId(dataset_id, path=test_ds.fullpath_files[0], part_idx=0)

            # All rows should return
            metrics = MetricsBag(component=ComponentLabel.WORKER)
            filters = [FilterPredicate(TestColumn.int_64_ts.value, '>=', str(BASE_TIME)),
                       FilterPredicate(TestColumn.int_64_ts.value, '<=', str(BASE_TIME + TIME_SHIFT))]
            loaded_df = shared_part_loader().load_dataframe(file_id, metrics=metrics, filters=filters)
            assert are_test_dfs_equal(original_df, loaded_df)

            # Only rows with exact match should return
            metrics = MetricsBag(component=ComponentLabel.WORKER)
            filters = [FilterPredicate(TestColumn.int_64_ts.value, '==', str(BASE_TIME))]
            loaded_df = shared_part_loader().load_dataframe(file_id, metrics=metrics, filters=filters)
            assert are_test_dfs_equal(original_df.loc[original_df[TestColumn.int_64_ts] == BASE_TIME], loaded_df)

            # Now test a string column filter + specific needed columns
            metrics = MetricsBag(component=ComponentLabel.WORKER)
            filters = [FilterPredicate(TestColumn.str_userid.value, '==', '0')]
            loaded_df = shared_part_loader().load_dataframe(file_id, metrics=metrics, filters=filters,
                                                            needed_columns=[TestColumn.bool, TestColumn.str_userid])
            # In the loaded DF, matching rows have 0..n index (regardless of their "original" position)
            expected_df = original_df[[TestColumn.bool, TestColumn.str_userid]]. \
                loc[original_df[TestColumn.str_userid] == '0']. \
                reset_index(drop=True)
            assert are_test_dfs_equal(expected_df, loaded_df)

            # No rows match
            metrics = MetricsBag(component=ComponentLabel.WORKER)
            filters = [FilterPredicate(TestColumn.int_64_ts.value, '<', str(BASE_TIME))]
            loaded_df = shared_part_loader().load_dataframe(file_id, metrics=metrics, filters=filters)
            assert len(loaded_df) == 0


def validate_load(file_id: DatasetPartId, original_df: pandas.DataFrame, expected_load_from: LoadFromLabel):
    metrics = MetricsBag(component=ComponentLabel.WORKER)
    loaded_df = shared_part_loader().load_dataframe(file_id, metrics=metrics)
    assert are_test_dfs_equal(original_df, loaded_df)
    assert metrics.label_value(LoadFromLabel) == expected_load_from.label_value


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_remote_caching():
    with clean_loader_cache():
        with new_test_dataset(4) as test_ds:
            s3path = test_ds.copy_to_s3(timestamped_uuid())

            ds_name = timestamped_uuid()
            dataset_id = DatasetId.now(ds_name)
            original_df1 = pandas.read_parquet(test_ds.fullpath_files[0])
            original_df2 = pandas.read_parquet(test_ds.fullpath_files[1])
            partid1 = DatasetPartId(dataset_id, path=f"{s3path}/{test_ds.basename_files[0]}", part_idx=0)
            partid2 = DatasetPartId(dataset_id, path=f"{s3path}/{test_ds.basename_files[1]}", part_idx=1)

            validate_load(partid1, original_df1, LoadFromLabel.SOURCE)
            for i in range(3):
                validate_load(partid1, original_df1, LoadFromLabel.DISK_CACHE)

            validate_load(partid2, original_df2, LoadFromLabel.SOURCE)
            for i in range(3):
                validate_load(partid2, original_df2, LoadFromLabel.DISK_CACHE)
                validate_load(partid1, original_df1, LoadFromLabel.DISK_CACHE)

            new_dataset_id = DatasetId.now(ds_name)
            new_fileid1 = DatasetPartId(new_dataset_id, path=partid1.path, part_idx=partid1.part_idx)
            validate_load(new_fileid1, original_df1, LoadFromLabel.SOURCE)
            validate_load(new_fileid1, original_df1, LoadFromLabel.DISK_CACHE)
            # NOTE: For now, cache doesn't auto-clear when newer dataset revision is found
            validate_load(partid1, original_df1, LoadFromLabel.DISK_CACHE)

            assert shared_part_loader().cache_len == 3


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_cache_size():
    num_parts = 4
    with new_test_dataset(num_parts) as test_ds:
        name = timestamped_uuid()
        s3path = test_ds.copy_to_s3(name)
        dataset_id = DatasetId.now(name)
        original_dfs = [pandas.read_parquet(test_ds.fullpath_files[i])
                        for i in range(num_parts)]
        part_ids = [DatasetPartId(dataset_id, path=f"{s3path}/{test_ds.basename_files[i]}", part_idx=i)
                    for i in range(num_parts)]

        with clean_loader_cache(size_mb=0):
            for i in range(num_parts):
                validate_load(part_ids[i], original_dfs[i], LoadFromLabel.SOURCE)
                validate_load(part_ids[i], original_dfs[i], LoadFromLabel.SOURCE)

            for i in range(num_parts):
                validate_load(part_ids[i], original_dfs[i], LoadFromLabel.SOURCE)

            # With no cache, the most recent file loaded was still downloaded locally
            # TODO reconsider this behavior
            assert shared_part_loader().cache_len == 1
            filesize_mb = os.path.getsize(test_ds.fullpath_files[-1]) / (1024 ** 2)
            assert shared_part_loader().cache_current_size_mb == filesize_mb

        # Constrain cache size to contain about half the dataset files, +/-1
        # This assumes all test files are about the same size.
        assert shared_part_loader().cache_len == 0
        half_ds_size_bytes = sum([os.path.getsize(test_ds.fullpath_files[i]) for i in range(num_parts - 2)])
        half_ds_size_mb = half_ds_size_bytes / (1024 ** 2)

        with clean_loader_cache(size_mb=half_ds_size_mb):
            for i in range(num_parts):
                validate_load(part_ids[i], original_dfs[i], LoadFromLabel.SOURCE)
            # Most recently used should be cached
            validate_load(part_ids[-1], original_dfs[-1], LoadFromLabel.DISK_CACHE)

            # Going again from the least-recently used, they should all be evicted by now
            # while parts re-loaded replace the cache content
            for i in range(num_parts):
                validate_load(part_ids[i], original_dfs[i], LoadFromLabel.SOURCE)

            # Cache should hold up to about half the files
            assert 0 < shared_part_loader().cache_len <= (num_parts / 2 + 1)

            # All parts now in cache should be "valid candidates" from the same dataset.
            # Basically, using the get_cached_candidates() to extract cache contents
            parts_in_cache = shared_part_loader().get_cached_candidates(dataset_id)
            assert len(parts_in_cache) == shared_part_loader().cache_len
            # Validate that it's in fact the most-recently used parts that are in that list
            parts_in_cache = sorted(parts_in_cache, key=lambda part: part.part_idx)
            assert parts_in_cache == part_ids[-len(parts_in_cache):]


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_get_candidates():
    # Creating 3 different DatasetIds (two of them with same name but different reg. data),
    # and ensure that parts are cached as different files (although source path in S3 is similar),
    # and that candidates are returned for the relevant ds ID only
    num_parts = 2
    with new_test_dataset(num_parts) as test_ds:
        s3path = test_ds.copy_to_s3()
        original_dfs = [pandas.read_parquet(test_ds.fullpath_files[i])
                        for i in range(num_parts)]

        dsid_1a = DatasetId.now("ds1")
        dsid_1b = DatasetId(name=dsid_1a.name,
                            registered_at=dsid_1a.registered_at + datetime.timedelta(seconds=1))
        dsid_2 = DatasetId.now("ds2")
        dsid_to_parts: Dict[DatasetId, List[DatasetPartId]] = {}

        def add_parts(dsid: DatasetId):
            dsid_to_parts[dsid] = [DatasetPartId(dsid, path=f"{s3path}{test_ds.basename_files[i]}", part_idx=i)
                                   for i in range(num_parts)]
        add_parts(dsid_1a)
        add_parts(dsid_1b)
        add_parts(dsid_2)

        with clean_loader_cache():
            for parts in dsid_to_parts.values():
                for part in parts:
                    validate_load(part, original_dfs[part.part_idx], LoadFromLabel.SOURCE)

            for dsid, parts in dsid_to_parts.items():
                dsid_copy = DatasetId(name=dsid.name, registered_at=dsid.registered_at)
                candidates = shared_part_loader().get_cached_candidates(dsid_copy)
                candidates = sorted(candidates, key=lambda k: k.part_idx)
                assert candidates == parts
