import os
import random
import shutil
from contextlib import contextmanager
from dataclasses import dataclass
from enum import auto
from typing import List
import numpy as np
import pytest
from pandas import RangeIndex, Series, DataFrame
from frocket.common.dataset import DatasetPartsInfo, DatasetId, DatasetPartId, PartNamingMethod, DatasetInfo, \
    DatasetColumnType, DatasetShortSchema
from frocket.common.serializable import AutoNamedEnum
from frocket.worker.runners.part_loader import shared_part_loader
from tests.utils.base_test_utils import temp_filename, TEMP_DIR, DisablePyTestCollectionMixin
from tests.utils.mock_s3_utils import SKIP_S3_TESTS, new_mock_s3_bucket


class TestColumn(DisablePyTestCollectionMixin, str, AutoNamedEnum):
    int_64_userid = auto()
    int_64_ts = auto()
    int_u32 = auto()
    float_64_ts = auto()
    float_all_none = auto()
    float_32 = auto()
    float_category = auto()
    str_userid = auto()
    str_and_none = auto()
    str_all_none = auto()
    str_object_all_none = auto()
    str_category_userid = auto()
    str_category_few = auto()
    str_category_many = auto()
    bool = auto()
    unsupported_datetimes = auto()
    unsupported_lists = auto()


DEFAULT_GROUP_COUNT = 200
DEFAULT_ROW_COUNT = 1000
DEFAULT_GROUP_COLUMN = TestColumn.int_64_userid.value
DEFAULT_TIMESTAMP_COLUMN = TestColumn.int_64_ts.value
BASE_TIME = 1609459200000  # Start of 2021, UTC
BASE_USER_ID = 100000
TIME_SHIFT = 10000
UNSUPPORTED_COLUMN_DTYPES = {TestColumn.unsupported_datetimes: 'datetime64[ns]',
                             TestColumn.unsupported_lists: 'object'}
STR_AND_NONE_VALUES = ["1", "2", "3"]
STR_CAT_FEW_WEIGHTS = [0.9, 0.07, 0.02, 0.01]
STR_CAT_MANY_WEIGHTS = [0.5, 0.2] + [0.01] * 30


def test_colname_to_coltype(name: str) -> DatasetColumnType:
    prefix_to_type = {
        'int': DatasetColumnType.INT,
        'float': DatasetColumnType.FLOAT,
        'str': DatasetColumnType.STRING,
        'bool': DatasetColumnType.BOOL,
        'unsupported': None
    }
    coltype = prefix_to_type[name.split('_')[0]]
    return coltype


def datafile_schema(part: int = 0) -> DatasetShortSchema:
    # noinspection PyUnresolvedReferences
    result = DatasetShortSchema(
        min_timestamp=float(BASE_TIME),
        max_timestamp=float(BASE_TIME + TIME_SHIFT),
        source_categoricals=[TestColumn.str_category_userid, TestColumn.str_category_many],
        potential_categoricals=[TestColumn.str_and_none, TestColumn.str_category_few],
        columns={col.value: test_colname_to_coltype(col)
                 for col in TestColumn
                 if test_colname_to_coltype(col)})

    # print(f"Test dataset short schema is:\n{result.to_json(indent=2)}")
    return result


def weighted_list(size: int, weights: list) -> list:
    res = []
    for idx, w in enumerate(weights):
        v = str(idx)
        vlen = size * w
        res += [v] * int(vlen)
    assert len(res) == size
    return res


def str_and_none_column_values(part: int = 0, with_none: bool = True) -> List[str]:
    result = [*STR_AND_NONE_VALUES, f"part-{part}"]
    if with_none:
        result.append(None)
    return result


def create_datafile(part: int = 0, size: int = DEFAULT_ROW_COUNT, filename: str = None) -> str:
    # First, prepare data for columns

    # Each part has a separate set of user (a.k.a. group) IDs
    initial_user_id = BASE_USER_ID * part
    min_user_id = initial_user_id
    max_user_id = initial_user_id + DEFAULT_GROUP_COUNT - 1
    # To each tests, ensure that each user ID appears in the file at least once, by including the whole range,
    # then add random IDs in the range
    int64_user_ids = \
        list(range(min_user_id, max_user_id + 1)) + \
        random.choices(range(min_user_id, max_user_id + 1), k=size - DEFAULT_GROUP_COUNT)
    # And also represent as strings in another column
    str_user_ids = [str(uid) for uid in int64_user_ids]

    # Timestamp: each part has a range of values of size TIME_SHIFT
    min_ts = BASE_TIME + (TIME_SHIFT * part)
    max_ts = BASE_TIME + (TIME_SHIFT * (part + 1))
    # Ensure that min & max timestamps appear exactly once, and fill the rest randomly in the range
    int_timestamps = \
        [min_ts, max_ts] + \
        random.choices(range(min_ts + 1, max_ts), k=size-2)
    # Now as floats and as (incorrect!) datetimes (datetimes currently unsupported)
    float_timestamps = [ts + random.random() for ts in int_timestamps]

    # More test columns
    int_u32_values = random.choices(range(100), k=size)
    float_32_values = [np.nan, *[random.random() for _ in range(size - 2)], np.nan]
    str_and_none_values = random.choices(str_and_none_column_values(part), k=size)
    bool_values = random.choices([True, False], k=size)

    # For yet-unsupported columns below
    lists_values = [[1, 2, 3]] * size
    datetimes = [ts * 1000000 for ts in float_timestamps]

    # Now create all series
    idx = RangeIndex(size)
    columns = {
        TestColumn.int_64_userid: Series(data=int64_user_ids),
        TestColumn.int_64_ts: Series(data=int_timestamps),
        TestColumn.int_u32: Series(data=int_u32_values, dtype='uint32'),
        TestColumn.float_64_ts: Series(data=float_timestamps),
        TestColumn.float_all_none: Series(data=None, index=idx, dtype='float64'),
        TestColumn.float_32: Series(data=float_32_values, dtype='float32'),
        TestColumn.float_category: Series(data=float_timestamps, index=idx, dtype='category'),
        TestColumn.str_userid: Series(data=str_user_ids),
        TestColumn.str_and_none: Series(data=str_and_none_values),
        TestColumn.str_all_none: Series(data=None, index=idx, dtype='str'),
        TestColumn.str_object_all_none: Series(data=None, index=idx, dtype='object'),
        TestColumn.str_category_userid: Series(data=str_user_ids, dtype='category'),
        TestColumn.str_category_few: Series(data=weighted_list(size, STR_CAT_FEW_WEIGHTS)),
        TestColumn.str_category_many: Series(data=weighted_list(size, STR_CAT_MANY_WEIGHTS), dtype='category'),
        TestColumn.bool: Series(data=bool_values, dtype='bool'),
        TestColumn.unsupported_datetimes: Series(data=datetimes, dtype='datetime64[us]'),
        TestColumn.unsupported_lists: Series(data=lists_values)
    }
    assert sorted([e for e in columns.keys()]) == sorted([col for col in TestColumn])  # All enum members covered!

    df = DataFrame({col.value: values for col, values in columns.items()})
    if not filename:
        filename = temp_filename('.testpq')
    df.to_parquet(filename)
    # print(f"Written DataFrame to {filename}, columns:\n{df.dtypes}\nSample:\n{df}")
    return filename


create_datafile()


@pytest.fixture(scope="module")
def datafile() -> str:
    return create_datafile()


@dataclass
class TestDatasetInfo(DisablePyTestCollectionMixin):
    basepath: str
    basename_files: List[str]
    fullpath_files: List[str]
    expected_parts: DatasetPartsInfo
    default_dataset_id: DatasetId
    default_dataset_info: DatasetInfo
    bucket: object = None

    def expected_part_ids(self, dsid: DatasetId, parts: List[int] = None) -> List[DatasetPartId]:
        parts = parts or range(len(self.fullpath_files))
        return [DatasetPartId(dataset_id=dsid, path=self.fullpath_files[i], part_idx=i)
                for i in parts]

    # noinspection PyUnresolvedReferences
    def copy_to_s3(self, path_in_bucket: str = '') -> str:
        assert not SKIP_S3_TESTS
        if not self.bucket:
            self.bucket = new_mock_s3_bucket()

        for i, file_fullpath in enumerate(self.fullpath_files):
            file_basename = self.basename_files[i]
            if path_in_bucket:
                file_basename = f"{path_in_bucket}/{file_basename}"
            self.bucket.upload_file(file_fullpath, file_basename)

        s3path = f"s3://{self.bucket.name}/{path_in_bucket}"
        return s3path

    def make_part(self, part_idx: int, s3path: str = None) -> DatasetPartId:
        if not s3path:
            path = self.fullpath_files[part_idx]
        else:
            if not s3path.endswith('/'):
                s3path += '/'
            path = f"{s3path}{self.basename_files[part_idx]}"
        return DatasetPartId(dataset_id=self.default_dataset_id, path=path, part_idx=part_idx)

    # noinspection PyUnresolvedReferences
    def cleanup(self):
        assert self.basepath.startswith(TEMP_DIR)
        shutil.rmtree(self.basepath)
        if self.bucket:
            for key in self.bucket.objects.all():
                key.delete()
            self.bucket.delete()

    @staticmethod
    def build(parts: int, prefix: str = '', suffix: str = '.parquet'):
        basepath = temp_filename(suffix="-dataset")
        os.makedirs(basepath)
        basename_files = sorted([f"{prefix}{temp_filename(suffix=('-p' + str(i) + suffix), with_dir=False)}"
                                 for i in range(parts)])
        fullpath_files = [f"{basepath}/{fname}" for fname in basename_files]
        [create_datafile(part=i, filename=fname) for i, fname in enumerate(fullpath_files)]

        expected_parts = DatasetPartsInfo(naming_method=PartNamingMethod.LIST,
                                          total_parts=parts,
                                          total_size=sum([os.stat(fname).st_size for fname in fullpath_files]),
                                          running_number_pattern=None,
                                          filenames=basename_files)

        dsid = DatasetId.now(name='dsid:' + basepath)
        # noinspection PyTypeChecker
        ds = DatasetInfo(id=dsid,
                         basepath=basepath,
                         total_parts=parts,
                         group_id_column=DEFAULT_GROUP_COLUMN,
                         timestamp_column=DEFAULT_TIMESTAMP_COLUMN)
        return TestDatasetInfo(basepath=basepath, basename_files=basename_files,
                               fullpath_files=fullpath_files, expected_parts=expected_parts,
                               default_dataset_id=dsid, default_dataset_info=ds)


@contextmanager
def new_test_dataset(parts: int, prefix: str = '', suffix: str = '.parquet') -> TestDatasetInfo:
    test_ds = TestDatasetInfo.build(parts=parts, prefix=prefix, suffix=suffix)
    try:
        yield test_ds
    finally:
        test_ds.cleanup()


def are_test_dfs_equal(df1: DataFrame, df2: DataFrame) -> bool:
    # Pandas doesn't know how to compare object-type columns holding more than a single object,
    # so ignore that column
    cols_to_ignore = list(UNSUPPORTED_COLUMN_DTYPES.keys())
    diff_df = df1.drop(columns=cols_to_ignore, errors='ignore').compare(
        df2.drop(columns=cols_to_ignore, errors='ignore'))
    return len(diff_df) == 0


# noinspection PyProtectedMember
@contextmanager
def clean_loader_cache(size_mb: float = None) -> None:
    loader = shared_part_loader()
    try:
        loader._setup(size_mb)
        yield None
    finally:
        loader._setup()  # Cleanup and make ready for next use
