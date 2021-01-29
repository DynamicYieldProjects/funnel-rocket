import os
import random
import shutil
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import List
import numpy as np
import pytest
from pandas import RangeIndex, Series, DataFrame
from frocket.common.dataset import DatasetPartsInfo, DatasetId, DatasetPartId, PartNamingMethod
from frocket.common.tasks.registration import DatasetValidationMode, REGISTER_DEFAULT_FILENAME_PATTERN
from frocket.datastore.registered_datastores import get_datastore
from frocket.invoker.jobs.registration_job_builder import RegistrationJobBuilder
from tests.base_test_utils import temp_filename, TEMP_DIR
from tests.task_and_job_utils import build_registration_job
from tests.mock_s3_utils import SKIP_MOCK_S3_TESTS, new_mock_s3_bucket

DEFAULT_ROW_COUNT = 1000
DEFAULT_GROUP_COUNT = 200
BASE_TIME = 1609459200000  # Start of 2021, UTC
TIME_SHIFT = 10000
STR_OR_NONE_VALUES = ["1", "2", "3", None]
CAT_SHORT_TOP = [0.9, 0.07, 0.02, 0.01]
CAT_LONG_TOP = [0.5, 0.2] + [0.01] * 30


def weighted_list(size: int, weights: list) -> list:
    res = []
    for idx, w in enumerate(weights):
        v = str(idx)
        vlen = size * w
        res += [v] * int(vlen)
    assert len(res) == size
    return res


@pytest.fixture(scope="module")
def datafile() -> str:
    return create_datafile()


def create_datafile(part: int = 0, size: int = DEFAULT_ROW_COUNT, filename: str = None) -> str:
    idx = RangeIndex(size)
    min_ts = BASE_TIME + (TIME_SHIFT * part)
    max_ts = BASE_TIME + (TIME_SHIFT * (part + 1))
    int_timestamps = [min_ts, max_ts] + [random.randint(min_ts, max_ts) for _ in range(size - 2)]
    # print(f"For part: {part}, min-ts: {min(int_timestamps)}, max-ts: {max(int_timestamps)}")
    float_timestamps = [ts + random.random() for ts in int_timestamps]
    dts = [ts * 1000000 for ts in float_timestamps]
    initial_user_id = 100000000 * part
    int64_user_ids = list(range(DEFAULT_GROUP_COUNT)) + \
        [random.randrange(DEFAULT_GROUP_COUNT) for _ in range(size - DEFAULT_GROUP_COUNT)]
    int64_user_ids = [initial_user_id + uid for uid in int64_user_ids]
    str_user_ids = [str(uid) for uid in int64_user_ids]
    str_or_none_ids = random.choices(STR_OR_NONE_VALUES, k=size)
    lists = [[1, 2, 3]] * size

    columns = {'bool': Series(data=random.choices([True, False], k=size), index=idx, dtype='bool'),
               'none_float': Series(data=None, index=idx, dtype='float64'),
               'none_object': Series(data=None, index=idx, dtype='object'),
               'none_str': Series(data=None, index=idx, dtype='str'),
               'int64_userid': Series(data=int64_user_ids, index=idx),
               'str_userid': Series(data=str_user_ids, index=idx),
               'str_none_userid': Series(data=str_or_none_ids, index=idx),
               'int64_ts': Series(data=int_timestamps, index=idx),
               'float64_ts': Series(data=float_timestamps, index=idx),
               'uint32': Series(data=random.choices(range(100), k=size),
                                index=idx, dtype='uint32'),
               'float32': Series(data=[np.nan, *[random.random() for _ in range(size - 2)], np.nan],
                                 index=idx, dtype='float32'),
               'cat_userid_str': Series(data=str_user_ids, index=idx, dtype='category'),
               'cat_short': Series(data=weighted_list(size, CAT_SHORT_TOP), index=idx),
               'cat_long': Series(data=weighted_list(size, CAT_LONG_TOP), index=idx, dtype='category'),
               'cat_float': Series(data=float_timestamps, index=idx, dtype='category'),
               'dt': Series(data=dts, index=idx, dtype='datetime64[us]'),
               'lists': Series(data=lists, index=idx)
               }

    df = DataFrame(columns)
    if not filename:
        filename = temp_filename('.testpq')
    df.to_parquet(filename)
    # Enable if needed
    # print(f"Written DataFrame to {filename}, columns:\n{df.dtypes}\nSample:\n{df}")
    return filename


@dataclass
class TestDatasetInfo:
    basepath: str
    basename_files: List[str]
    fullpath_files: List[str]
    expected_parts: DatasetPartsInfo
    registration_jobs: List[RegistrationJobBuilder] = field(default_factory=list)
    bucket: object = None

    def expected_part_ids(self, dsid: DatasetId, parts: List[int] = None) -> List[DatasetPartId]:
        parts = parts or range(len(self.fullpath_files))
        return [DatasetPartId(dataset_id=dsid, path=self.fullpath_files[i], part_idx=i)
                for i in parts]

    def registration_job(self,
                         mode: DatasetValidationMode,
                         group_id_column: str = 'int64_userid',
                         pattern: str = REGISTER_DEFAULT_FILENAME_PATTERN,
                         uniques: bool = True) -> RegistrationJobBuilder:
        job = build_registration_job(self.basepath, mode, group_id_column, pattern, uniques)
        self.registration_jobs.append(job)
        return job

    # noinspection PyUnresolvedReferences
    def copy_to_s3(self, path_in_bucket: str = '') -> str:
        assert not SKIP_MOCK_S3_TESTS
        if not self.bucket:
            self.bucket = new_mock_s3_bucket()

        for i, file_fullpath in enumerate(self.fullpath_files):
            file_basename = self.basename_files[i]
            if path_in_bucket:
                file_basename = f"{path_in_bucket}/{file_basename}"
            self.bucket.upload_file(file_fullpath, file_basename)

        s3path = f"s3://{self.bucket.name}/{path_in_bucket}"
        return s3path

    # noinspection PyUnresolvedReferences
    def cleanup(self):
        assert self.basepath.startswith(TEMP_DIR)
        shutil.rmtree(self.basepath)
        for rj in self.registration_jobs:
            if rj.dataset:
                get_datastore().remove_dataset_info(rj.dataset.id.name)
        if self.bucket:
            for key in self.bucket.objects.all():
                key.delete()
            self.bucket.delete()


def _build_test_dataset(parts: int, prefix: str = '', suffix: str = '.parquet') -> TestDatasetInfo:
    basepath = temp_filename(suffix="-dataset")
    os.makedirs(basepath)
    basename_files = [f"{prefix}{temp_filename(suffix=('-p' + str(i) + suffix), with_dir=False)}"
                      for i in range(parts)]
    fullpath_files = [f"{basepath}/{fname}" for fname in basename_files]
    [create_datafile(part=i, filename=fname) for i, fname in enumerate(fullpath_files)]

    expected_parts = DatasetPartsInfo(naming_method=PartNamingMethod.LIST,
                                      total_parts=parts,
                                      total_size=sum([os.stat(fname).st_size for fname in fullpath_files]),
                                      running_number_pattern=None,
                                      filenames=sorted(basename_files))
    # PyCharm issue?
    # noinspection PyArgumentList
    return TestDatasetInfo(basepath=basepath, basename_files=basename_files,
                           fullpath_files=fullpath_files, expected_parts=expected_parts)


@contextmanager
def new_dataset(parts: int, prefix: str = '', suffix: str = '.parquet') -> TestDatasetInfo:
    test_ds = _build_test_dataset(parts=parts, prefix=prefix, suffix=suffix)
    try:
        yield test_ds
    finally:
        test_ds.cleanup()
