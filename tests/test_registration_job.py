import math
import os
import shutil
from typing import List, cast
import pytest
import pandas as pd
from fixtures_n_helpers import DEFAULT_GROUP_COUNT, CAT_LONG_TOP, new_dataset, \
    temp_filename, build_registration_job, registration_job_invoker
from frocket.common.helpers.utils import bytes_to_ndarray
from frocket.common.tasks.registration import DatasetValidationMode,  \
    RegistrationTaskResult, RegistrationJobResult
from frocket.datastore.registered_datastores import get_datastore, get_blobstore
from frocket.invoker.jobs.registration_job_builder import VALIDATION_MAX_SAMPLES, \
    VALIDATION_SAMPLE_RATIO, CATEGORICAL_TOP_COUNT


# TODO create dataset and tear it down nicer
def test_local_single_file_discovery():
    with new_dataset(1) as test_ds:
        job = test_ds.registration_job(DatasetValidationMode.SINGLE)
        job.prerun()

        assert job.parts == test_ds.expected_parts
        assert job.sampled_parts == test_ds.expected_part_ids(job.dataset.id)

        job = test_ds.registration_job(DatasetValidationMode.SAMPLE, uniques=True)
        job.prerun()
        assert job.parts == test_ds.expected_parts
        assert job.sampled_parts == test_ds.expected_part_ids(job.dataset.id)

        job = test_ds.registration_job(DatasetValidationMode.FIRST_LAST, uniques=True, pattern="*t")
        job.prerun()
        assert job.parts == test_ds.expected_parts
        assert job.sampled_parts == test_ds.expected_part_ids(job.dataset.id)

        with open(f"{test_ds.basepath}/touched", "w") as f:
            f.write("hello")

        job = test_ds.registration_job(DatasetValidationMode.FIRST_LAST)
        job.prerun()
        assert job.parts == test_ds.expected_parts
        assert job.sampled_parts == test_ds.expected_part_ids(job.dataset.id)


def do_test_multiple_local_files(parts: int):
    with new_dataset(parts) as test_ds:
        job = test_ds.registration_job(DatasetValidationMode.SINGLE)
        job.prerun()
        assert sorted(job.parts.filenames) == sorted(test_ds.expected_parts.filenames)
        assert job.parts.total_size == test_ds.expected_parts.total_size
        assert len(job.sampled_parts) == 1
        assert job.sampled_parts[0].path in test_ds.fullpath_files

        job = test_ds.registration_job(DatasetValidationMode.FIRST_LAST)
        job.prerun()
        assert sorted(job.parts.filenames) == sorted(test_ds.expected_parts.filenames)
        assert job.parts.total_size == test_ds.expected_parts.total_size
        assert len(job.sampled_parts) == 2
        sampled_fnames = sorted([sp.path for sp in job.sampled_parts])
        assert sampled_fnames[0] == sorted(test_ds.fullpath_files)[0]
        assert sampled_fnames[-1] == sorted(test_ds.fullpath_files)[-1]

        job = test_ds.registration_job(DatasetValidationMode.SAMPLE)
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
@pytest.mark.skip()
def test_local_many_file_discovery():
    do_test_multiple_local_files(20)
    do_test_multiple_local_files(120)


@pytest.mark.xfail(raises=FileNotFoundError)
def test_local_missing_path():
    job = build_registration_job(basepath="blahblahblah", mode=DatasetValidationMode.SINGLE)
    job.prerun()


def test_nofiles_no_samples():
    basepath = temp_filename()
    os.mkdir(basepath)
    job = build_registration_job(basepath, mode=DatasetValidationMode.SINGLE)
    job.prerun()
    assert job.sampled_parts is None


@pytest.mark.xfail
def test_nofiles_failure():
    basepath = temp_filename()
    os.mkdir(basepath)
    job = build_registration_job(basepath, DatasetValidationMode.SINGLE)
    with registration_job_invoker(job) as invoker:
        invoker.prerun()
        invoker.enqueue_tasks()


def test_local_nondefault_pattern():
    num_parts = 4
    with new_dataset(num_parts, "blah-", ".123") as test_ds:
        with open(f"{test_ds.basepath}/single", "w") as f:
            f.write("herehere")

        mode = DatasetValidationMode.SINGLE
        job = test_ds.registration_job(mode=mode)
        job.prerun()
        assert job.sampled_parts is None

        job = test_ds.registration_job(mode=mode, pattern="blahx*")
        job.prerun()
        assert job.sampled_parts is None

        job = test_ds.registration_job(mode=mode, pattern="bl*.123")
        job.prerun()
        assert job.parts.total_parts == num_parts

        job = test_ds.registration_job(mode=mode, pattern="single")
        job.prerun()
        assert job.parts.total_parts == 1


def test_merged_schema():
    num_parts = 20
    with new_dataset(num_parts) as test_ds:
        job = test_ds.registration_job(mode=DatasetValidationMode.SAMPLE, uniques=True)
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

            cat_long_attrs = result_schema.columns['cat_long'].colattrs
            assert cat_long_attrs.categorical and len(cat_long_attrs.cat_top_values) == CATEGORICAL_TOP_COUNT
            assert cat_long_attrs.cat_top_values['0'] == CAT_LONG_TOP[0]
            assert cat_long_attrs.cat_top_values['1'] == CAT_LONG_TOP[1]


def test_non_unique_ids():
    with new_dataset(2) as test_ds:
        # Copy first file over the second one - so they now have the same group IDs
        shutil.copyfile(test_ds.fullpath_files[0], test_ds.fullpath_files[-1])
        job = test_ds.registration_job(mode=DatasetValidationMode.FIRST_LAST, uniques=True)
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
    with new_dataset(2) as test_ds:
        fname = test_ds.fullpath_files[-1]
        df = pd.read_parquet(fname)
        df['added_column'] = df['int64_userid']
        df.to_parquet(fname)

        job = test_ds.registration_job(mode=DatasetValidationMode.FIRST_LAST, uniques=True)
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


"""
Tests left to do:
    discovery
        S3: ok + bad paths
        S3: file count and sizes
        S3: non default pattern

# TODO add context manager for invoker_runner
# TODO Later: categorical merging...
"""
