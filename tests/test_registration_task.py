import math
import time
from typing import cast
import numpy as np
import pytest
from pandas import Series, RangeIndex, DataFrame
import random
import tempfile
from frocket.common.config import config
from frocket.common.dataset import DatasetInfo, DatasetId, DatasetPartId, DatasetColumnType, DatasetShortSchema
from frocket.common.helpers.utils import timestamped_uuid, bytes_to_ndarray
from frocket.common.metrics import MetricsBag, ComponentLabel
from frocket.common.tasks.base import TaskStatus
from frocket.common.tasks.registration import RegistrationTaskRequest, RegistrationTaskResult
from frocket.datastore.registered_datastores import get_datastore, get_blobstore
from frocket.worker.impl.generic_env_metrics import GenericEnvMetricsProvider
from frocket.worker.runners.base_task_runner import TaskRunnerContext
from frocket.worker.runners.registration_runner import RegistrationTaskRunner

# noinspection PyProtectedMember
TEMP_DIR = tempfile._get_default_tempdir()  # + '/' + next(tempfile._get_candidate_names())
DEFAULT_ROW_COUNT = 1000
DEFAULT_GROUP_COUNT = 200
STR_OR_NONE_VALUES = ["1", "2", "3", None]
BASE_TIME = 1609459200000  # Start of 2021
TIME_SHIFT = 10000


# noinspection PyProtectedMember
def temp_filename(suffix=''):
    return TEMP_DIR + '/' + next(tempfile._get_candidate_names()) + suffix


@pytest.fixture(scope="session", autouse=True)
def init_redis():
    config['redis.db'] = '15'
    config['datastore.redis.prefix'] = "frocket:tests:"
    print(get_datastore(), get_blobstore())


@pytest.fixture(scope="module")
def datafile(part: int = 0, size: int = DEFAULT_ROW_COUNT, filename: str = None) -> str:
    idx = RangeIndex(size)
    base_time = 1609459200000  # Start of 2021
    int_timestamps = [-TIME_SHIFT * (part + 1), TIME_SHIFT * (part + 1)] + \
                     [random.randint(-TIME_SHIFT, TIME_SHIFT) for _ in range(size - 2)]
    int_timestamps = [BASE_TIME + ts for ts in int_timestamps]
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
               'cat_float': Series(data=float_timestamps, index=idx, dtype='category'),
               'dt': Series(data=dts, index=idx, dtype='datetime64[us]'),
               'lists': Series(data=lists, index=idx)
               }

    df = DataFrame(columns)
    print(df.dtypes)
    # print(df)  # Enable if needed
    if not filename:
        filename = temp_filename('.parquet')
    df.to_parquet(filename)
    print(f"Written DataFrame to {filename}")
    return filename


def run_task(datafile: str,
             group_id_column: str = 'int64_userid',
             timestamp_column: str = 'int64_ts',
             return_uniques: bool = False) -> RegistrationTaskResult:
    dsid = DatasetId.now(name='test')
    ds = DatasetInfo(id=dsid,
                     basepath=TEMP_DIR,
                     total_parts=1,
                     group_id_column=group_id_column,
                     timestamp_column=timestamp_column)
    req = RegistrationTaskRequest(dataset=ds,
                                  attempt_no=0,
                                  invoke_time=time.time(),
                                  part_id=DatasetPartId(dataset_id=dsid, part_idx=0, path=datafile),
                                  request_id=timestamped_uuid(),
                                  return_group_ids=return_uniques,
                                  task_index=0)
    ctx = TaskRunnerContext(metrics=MetricsBag(component=ComponentLabel.WORKER,
                                               env_metrics_provider=GenericEnvMetricsProvider()))
    task_runner = RegistrationTaskRunner(req, ctx)
    result = task_runner.run()
    assert type(result) is RegistrationTaskResult
    return cast(RegistrationTaskResult, result)


def test_col_types(datafile):
    result = run_task(datafile)
    assert result.status == TaskStatus.ENDED_SUCCESS
    assert result.error_message is None
    sch = result.dataset_schema
    assert sch.group_id_column == 'int64_userid'
    assert sch.timestamp_column == 'int64_ts'
    assert sch.unsupported_columns == {'dt': 'datetime64[ns]', 'lists': 'object'}
    assert result.group_ids_blob_id is None

    col = sch.columns['bool']
    assert col.to_dict() == {
        "name": "bool", "dtype_name": "bool", "coltype": DatasetColumnType.BOOL,
        "colattrs": {
            "numeric_min": None, "numeric_max": None, "categorical": False,
            "cat_top_values": None, "cat_unique_ratio": None}}

    col = sch.columns['none_float']
    assert col.name == 'none_float' and col.dtype_name == "float64" and \
           col.coltype == DatasetColumnType.FLOAT and np.isnan(col.colattrs.numeric_min) and \
           np.isnan(col.colattrs.numeric_max) and not col.colattrs.categorical

    col = sch.columns['none_object']
    assert col.coltype == DatasetColumnType.STRING and not col.colattrs.numeric_min and \
           not col.colattrs.numeric_max and not col.colattrs.categorical

    col = sch.columns['none_str']
    assert col.coltype == DatasetColumnType.STRING and not col.colattrs.numeric_min and \
           not col.colattrs.numeric_max and not col.colattrs.categorical

    col = sch.columns['int64_userid']
    assert col.coltype == DatasetColumnType.INT and \
           col.colattrs.numeric_min == 0 and col.colattrs.numeric_max == DEFAULT_GROUP_COUNT - 1 and \
           not col.colattrs.categorical

    col = sch.columns['str_userid']
    assert col.coltype == DatasetColumnType.STRING and not col.colattrs.categorical

    col = sch.columns['str_none_userid']
    assert col.coltype == DatasetColumnType.STRING and col.colattrs.categorical
    str_values = [v for v in STR_OR_NONE_VALUES if v]
    assert col.colattrs.cat_unique_ratio == len(str_values) / DEFAULT_ROW_COUNT
    assert sorted(col.colattrs.cat_top_values.keys()) == sorted(str_values)
    assert np.isclose(sum(col.colattrs.cat_top_values.values()), 1.0)

    col = sch.columns['int64_ts']
    assert col.coltype == DatasetColumnType.INT and \
           col.colattrs.numeric_min == BASE_TIME - TIME_SHIFT \
           and col.colattrs.numeric_max == BASE_TIME + TIME_SHIFT

    col = sch.columns['float64_ts']
    assert col.coltype == DatasetColumnType.FLOAT and \
           math.floor(col.colattrs.numeric_min) == BASE_TIME - TIME_SHIFT and \
           math.floor(col.colattrs.numeric_max) == BASE_TIME + TIME_SHIFT

    col = sch.columns['uint32']
    assert col.coltype == DatasetColumnType.INT

    col = sch.columns['float32']
    assert col.coltype == DatasetColumnType.FLOAT

    col = sch.columns['cat_userid_str']
    assert col.coltype == DatasetColumnType.STRING and col.colattrs.categorical

    col = sch.columns['cat_float']
    assert col.coltype == DatasetColumnType.FLOAT and not col.colattrs.categorical and \
           math.floor(col.colattrs.numeric_min) == BASE_TIME - TIME_SHIFT and \
           math.floor(col.colattrs.numeric_max) == BASE_TIME + TIME_SHIFT

    expected_short_schema = DatasetShortSchema(min_timestamp=float(BASE_TIME - TIME_SHIFT),
                                               max_timestamp=float(BASE_TIME + TIME_SHIFT),
                                               source_categoricals=['cat_userid_str'],
                                               potential_categoricals=['str_none_userid'],
                                               columns={'bool': DatasetColumnType.BOOL,
                                                        'none_float': DatasetColumnType.FLOAT,
                                                        'none_object': DatasetColumnType.STRING,
                                                        'none_str': DatasetColumnType.STRING,
                                                        'int64_userid': DatasetColumnType.INT,
                                                        'str_userid': DatasetColumnType.STRING,
                                                        'str_none_userid': DatasetColumnType.STRING,
                                                        'int64_ts': DatasetColumnType.INT,
                                                        'float64_ts': DatasetColumnType.FLOAT,
                                                        'uint32': DatasetColumnType.INT,
                                                        'float32': DatasetColumnType.FLOAT,
                                                        'cat_userid_str': DatasetColumnType.STRING,
                                                        'cat_float': DatasetColumnType.FLOAT}).to_dict()
    short_schema_dict = sch.short().to_dict()
    assert short_schema_dict == expected_short_schema


def test_missing_group_column(datafile):
    result = run_task(datafile, group_id_column="mosh")
    assert result.status == TaskStatus.ENDED_FAILED
    result = run_task(datafile, group_id_column="lists")  # Name of an unsupported column
    assert result.status == TaskStatus.ENDED_FAILED


def test_str_group_column_ok(datafile):
    result = run_task(datafile, group_id_column="str_userid")
    assert result.status == TaskStatus.ENDED_SUCCESS


def test_group_column_with_none_notok(datafile):
    result = run_task(datafile, group_id_column="str_none_userid")
    assert result.status == TaskStatus.ENDED_FAILED


def test_float_group_column_notok(datafile):
    result = run_task(datafile, group_id_column="float64_ts")
    assert result.status == TaskStatus.ENDED_FAILED


def test_missing_timestamp_column(datafile):
    result = run_task(datafile, timestamp_column="mosh")
    assert result.status == TaskStatus.ENDED_FAILED


def test_float_timestamp_column_ok(datafile):
    result = run_task(datafile, timestamp_column="float64_ts")
    assert result.status == TaskStatus.ENDED_SUCCESS


def test_ts_column_with_none_notok(datafile):
    result = run_task(datafile, timestamp_column="float32")
    assert result.status == TaskStatus.ENDED_FAILED


def test_missing_file():
    result = run_task("sdlasdghjasghdjahsdk")
    assert result.status == TaskStatus.ENDED_FAILED


def test_invalid_file():
    tmpname = temp_filename()
    with open(tmpname, 'w') as f:
        f.write('abc'*10000)
    result = run_task(tmpname)
    assert result.status == TaskStatus.ENDED_FAILED


def test_uniques_blob(datafile):
    result = run_task(datafile, return_uniques=True)
    assert result.status == TaskStatus.ENDED_SUCCESS
    assert result.group_ids_blob_id
    blob_content = get_blobstore().read_blob(result.group_ids_blob_id)
    assert blob_content
    uniques = bytes_to_ndarray(blob_content)
    assert len(uniques) == DEFAULT_GROUP_COUNT
    assert np.isin(uniques, range(DEFAULT_GROUP_COUNT)).all()

    assert get_blobstore().delete_blob(result.group_ids_blob_id)
    assert not get_blobstore().read_blob(result.group_ids_blob_id)
    assert not get_blobstore().delete_blob(result.group_ids_blob_id)



"""
# Tests

use Redis with db X (and other prefix?)

## registration_runner
    getting uniques back, if requested, through blob
    invalid file
***        get top values over threshold + factor (must be known distribution)

"""
