import math
import os
import shutil
from typing import List, cast
import pytest
import pandas as pd
from tests.utils.mock_s3_utils import SKIP_S3_TESTS
from tests.utils.task_and_job_utils import registration_job_invoker, build_registration_job, registration_job
from tests.utils.dataset_utils import new_test_dataset, STR_CAT_MANY_WEIGHTS, TestColumn, DEFAULT_GROUP_COUNT
from tests.utils.base_test_utils import temp_filename, SKIP_SLOW_TESTS
from frocket.common.helpers.utils import bytes_to_ndarray
from frocket.common.tasks.registration import DatasetValidationMode,  \
    RegistrationTaskResult, RegistrationJobResult
from frocket.datastore.registered_datastores import get_datastore, get_blobstore
from frocket.invoker.jobs.registration_job import VALIDATION_MAX_SAMPLES, \
    VALIDATION_SAMPLE_RATIO, CATEGORICAL_TOP_COUNT
# noinspection PyUnresolvedReferences
from tests.utils.redis_fixture import init_test_redis_settings


# TODO create dataset and tear it down nicer
def test_local_single_file_discovery():
    with new_test_dataset(1) as test_ds:
        with registration_job(test_ds, DatasetValidationMode.SINGLE) as job:
            job.prerun()

            assert job.parts == test_ds.expected_parts
            assert job.sampled_parts == test_ds.expected_part_ids(job.dataset.id)

        with registration_job(test_ds, DatasetValidationMode.SAMPLE, uniques=True) as job:
            job.prerun()
            assert job.parts == test_ds.expected_parts
            assert job.sampled_parts == test_ds.expected_part_ids(job.dataset.id)

        with registration_job(test_ds, DatasetValidationMode.FIRST_LAST, uniques=True, pattern="*t") as job:
            job.prerun()
            assert job.parts == test_ds.expected_parts
            assert job.sampled_parts == test_ds.expected_part_ids(job.dataset.id)

        with open(f"{test_ds.basepath}/touched", "w") as f:
            f.write("hello")

        with registration_job(test_ds, DatasetValidationMode.FIRST_LAST) as job:
            job.prerun()
            assert job.parts == test_ds.expected_parts
            assert job.sampled_parts == test_ds.expected_part_ids(job.dataset.id)


def do_test_multiple_local_files(parts: int):
    with new_test_dataset(parts) as test_ds:
        with registration_job(test_ds, DatasetValidationMode.SINGLE) as job:
            job.prerun()
            assert sorted(job.parts.filenames) == sorted(test_ds.expected_parts.filenames)
            assert job.parts.total_size == test_ds.expected_parts.total_size
            assert len(job.sampled_parts) == 1
            assert job.sampled_parts[0].path in test_ds.fullpath_files

        with registration_job(test_ds, DatasetValidationMode.FIRST_LAST) as job:
            job.prerun()
            assert sorted(job.parts.filenames) == sorted(test_ds.expected_parts.filenames)
            assert job.parts.total_size == test_ds.expected_parts.total_size
            assert len(job.sampled_parts) == 2
            sampled_fnames = sorted([sp.path for sp in job.sampled_parts])
            assert sampled_fnames[0] == sorted(test_ds.fullpath_files)[0]
            assert sampled_fnames[-1] == sorted(test_ds.fullpath_files)[-1]

        with registration_job(test_ds, DatasetValidationMode.SAMPLE) as job:
            job.prerun()
            assert sorted(job.parts.filenames) == sorted(test_ds.expected_parts.filenames)
            assert job.parts.total_size == test_ds.expected_parts.total_size
            assert 2 <= len(job.sampled_parts) <= len(test_ds.fullpath_files)
            assert len(job.sampled_parts) <= VALIDATION_MAX_SAMPLES
            sampled_fnames = sorted([sp.path for sp in job.sampled_parts])
            assert sampled_fnames[0] == sorted(test_ds.fullpath_files)[0]
            assert sampled_fnames[-1] == sorted(test_ds.fullpath_files)[-1]

            if len(test_ds.fullpath_files) > 2:
                expected_sample_count = math.floor(len(test_ds.fullpath_files) * VALIDATION_SAMPLE_RATIO)
                if expected_sample_count <= VALIDATION_MAX_SAMPLES:
                    assert len(sampled_fnames) == expected_sample_count
                else:
                    assert len(sampled_fnames) == VALIDATION_MAX_SAMPLES
                sampled_fnames = [sp.path for sp in job.sampled_parts]
                assert all([fname in test_ds.fullpath_files for fname in sampled_fnames])


def test_local_two_file_discovery():
    do_test_multiple_local_files(2)


# TODO what's the right marker
@pytest.mark.skipif(SKIP_SLOW_TESTS, reason="Skipping slow tests")
def test_local_many_file_discovery():
    do_test_multiple_local_files(20)
    do_test_multiple_local_files(120)


@pytest.mark.xfail(strict=True, raises=FileNotFoundError)
def test_local_missing_path():
    job = build_registration_job(basepath="blahblahblah", mode=DatasetValidationMode.SINGLE)
    job.prerun()


def test_nofiles_no_samples():
    basepath = temp_filename()
    os.mkdir(basepath)
    job = build_registration_job(basepath, mode=DatasetValidationMode.SINGLE)
    job.prerun()
    assert job.sampled_parts is None


@pytest.mark.xfail(strict=True)
def test_nofiles_failure():
    basepath = temp_filename()
    os.mkdir(basepath)
    job = build_registration_job(basepath, DatasetValidationMode.SINGLE)
    with registration_job_invoker(job) as invoker:
        invoker.prerun()
        invoker.enqueue_tasks()


def test_local_nondefault_pattern():
    num_parts = 4
    with new_test_dataset(num_parts, "blah-", ".123") as test_ds:
        with open(f"{test_ds.basepath}/single", "w") as f:
            f.write("herehere")

        mode = DatasetValidationMode.SINGLE
        with registration_job(test_ds, mode=mode) as job:
            job.prerun()
            assert job.sampled_parts is None

        with registration_job(test_ds, mode=mode, pattern="blahx*") as job:
            job.prerun()
            assert job.sampled_parts is None

        with registration_job(test_ds, mode=mode, pattern="bl*.123") as job:
            job.prerun()
            assert job.parts.total_parts == num_parts

        with registration_job(test_ds, mode=mode, pattern="single") as job:
            job.prerun()
            assert job.parts.total_parts == 1


def test_merged_schema():
    num_parts = 20
    with new_test_dataset(num_parts) as test_ds:
        with registration_job(test_ds, mode=DatasetValidationMode.SAMPLE, uniques=True) as job:
            with registration_job_invoker(job) as invoker:
                invoker.prerun()
                invoker.enqueue_tasks()
                assert invoker.num_tasks >= 2 and invoker.num_tasks == len(job.sampled_parts)

                task_results = cast(List[RegistrationTaskResult], invoker.run_tasks())
                paths = [tr.part_id.path for tr in task_results]
                assert sorted(paths) == sorted([sp.path for sp in job.sampled_parts])

                all_group_ids = []
                for res in task_results:
                    blob_content = get_blobstore().read_blob(res.group_ids_blob_id)
                    assert blob_content
                    uniques = bytes_to_ndarray(blob_content)
                    all_group_ids += list(uniques)
                assert len(all_group_ids) == DEFAULT_GROUP_COUNT * len(job.sampled_parts)
                assert len(set(all_group_ids)) == len(all_group_ids)

                final_job_status = invoker.complete()
                job_result = cast(RegistrationJobResult, invoker.build_result())

                result_ds = job_result.dataset
                assert get_datastore().dataset_info(result_ds.id.name) == result_ds

                result_schema = get_datastore().schema(result_ds)
                result_short_schema = get_datastore().short_schema(result_ds)

    assert task_results[0].dataset_schema.group_id_column == result_schema.group_id_column
    assert task_results[0].dataset_schema.timestamp_column == result_schema.timestamp_column
    assert task_results[0].dataset_schema.short().columns == result_short_schema.columns
    assert task_results[0].dataset_schema.short().source_categoricals == result_short_schema.source_categoricals
    assert task_results[0].dataset_schema.short().potential_categoricals == \
           result_short_schema.potential_categoricals

    min_ts = min([res.dataset_schema.columns[result_schema.timestamp_column].colattrs.numeric_min
                  for res in task_results])
    max_ts = max([res.dataset_schema.columns[result_schema.timestamp_column].colattrs.numeric_max
                  for res in task_results])

    assert result_short_schema.min_timestamp == min_ts
    assert result_short_schema.max_timestamp == max_ts

    attrs = result_schema.columns[TestColumn.str_category_many].colattrs
    assert attrs.categorical and len(attrs.cat_top_values) == CATEGORICAL_TOP_COUNT
    assert attrs.cat_top_values['0'] == STR_CAT_MANY_WEIGHTS[0]
    assert attrs.cat_top_values['1'] == STR_CAT_MANY_WEIGHTS[1]


def test_non_unique_ids():
    with new_test_dataset(2) as test_ds:
        # Copy first file over the second one - so they now have the same group IDs
        shutil.copyfile(test_ds.fullpath_files[0], test_ds.fullpath_files[-1])
        with registration_job(test_ds, mode=DatasetValidationMode.FIRST_LAST, uniques=True) as job:
            with registration_job_invoker(job) as invoker:
                invoker.prerun()
                assert len(job.sampled_parts) == 2
                invoker.enqueue_tasks()
                invoker.run_tasks()  # This checks that all *task* results have finished ok
                # The job fails (at the complete() phase) but does not crash
                final_job_status = invoker.complete(assert_success=False)
                assert not final_job_status.success
                result = invoker.build_result(assert_success=False)
                assert not result.success


def test_schemas_differ():
    with new_test_dataset(2) as test_ds:
        fname = test_ds.fullpath_files[-1]
        df = pd.read_parquet(fname)
        df['added_column'] = df[TestColumn.int_64_userid]
        df.to_parquet(fname)

        with registration_job(test_ds, mode=DatasetValidationMode.FIRST_LAST, uniques=True) as job:
            with registration_job_invoker(job) as invoker:
                invoker.prerun()
                assert len(job.sampled_parts) == 2
                invoker.enqueue_tasks()
                invoker.run_tasks()  # This checks that all *task* results have finished ok
                # The job fails (at the complete() phase) but does not crash
                final_job_status = invoker.complete(assert_success=False)
                assert not final_job_status.success
                result = invoker.build_result(assert_success=False)
                assert not result.success


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_s3_paths():
    ignore_this_file = temp_filename()
    with open(ignore_this_file, 'w') as f:
        f.write("Not really")

    # Simple case - no sub dir, default filename pattern
    with new_test_dataset(2) as test_ds:
        s3path = test_ds.copy_to_s3()
        job = build_registration_job(s3path, mode=DatasetValidationMode.SAMPLE)
        job.prerun()
        assert job.parts == test_ds.expected_parts
        assert sorted([sp.path for sp in job.sampled_parts]) == \
               sorted([f"{s3path}{fname}" for fname in test_ds.basename_files])

        # Add a file which shouldn't be picked up
        test_ds.bucket.upload_file(ignore_this_file, 'ignore.pq')
        job = build_registration_job(s3path, mode=DatasetValidationMode.SAMPLE)
        job.prerun()
        assert job.parts == test_ds.expected_parts

    # Now with non-default patterns
    with new_test_dataset(8, prefix="blah-", suffix=".pq") as test_ds:
        s3path = test_ds.copy_to_s3()
        # Add files to be ignored
        test_ds.bucket.upload_file(ignore_this_file, 'ignore.parquet')
        test_ds.bucket.upload_file(ignore_this_file, 'sub1/ignore.pq')
        test_ds.bucket.upload_file(ignore_this_file, 'sub2/ignore.parquet')
        test_ds.bucket.upload_file(ignore_this_file, 'sub1/sub11/ignore.pq')

        # Lookup in bucket root
        job = build_registration_job(s3path, pattern="blah*pq", mode=DatasetValidationMode.SAMPLE)
        job.prerun()
        assert job.parts == test_ds.expected_parts

        # Lookup in nested keys
        test_ds.copy_to_s3('sub1')
        test_ds.copy_to_s3('sub1/sub11')
        job = build_registration_job(s3path + 'sub1', pattern="b*.pq", mode=DatasetValidationMode.SAMPLE)
        job.prerun()
        assert job.parts == test_ds.expected_parts

        job = build_registration_job(s3path + 'sub1/sub11/', pattern="b*.pq", mode=DatasetValidationMode.SAMPLE)
        job.prerun()
        assert job.parts == test_ds.expected_parts

        # Ensure registration can actually run on the found paths
        with registration_job_invoker(job) as invoker:
            job_result = invoker.run_all_stages()  # Asserts success by default

        # Key (dir) exists but no matching files
        job = build_registration_job(s3path + 'sub2/', pattern="b*.pq", mode=DatasetValidationMode.SAMPLE)
        job.prerun()
        assert not job.parts


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
@pytest.mark.xfail(strict=True)
def test_s3_missing_bucket_failure():
    job = build_registration_job('s3://whatsthat/', mode=DatasetValidationMode.SAMPLE)
    job.prerun()


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
@pytest.mark.xfail(strict=True)
def test_s3_missing_key_failure():
    with new_test_dataset(2) as test_ds:
        s3path = test_ds.copy_to_s3('exists')
        job = build_registration_job(s3path + '/notreally', mode=DatasetValidationMode.SAMPLE)
        job.prerun()
