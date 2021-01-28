import math
import time
from typing import cast
import numpy as np
from frocket.common.config import config
from frocket.common.dataset import DatasetInfo, DatasetId, DatasetPartId, DatasetColumnType, DatasetShortSchema
from frocket.common.helpers.utils import timestamped_uuid, bytes_to_ndarray
from frocket.common.tasks.base import TaskStatus
from frocket.common.tasks.registration import RegistrationTaskRequest, RegistrationTaskResult
from frocket.datastore.registered_datastores import get_blobstore, get_datastore
from frocket.worker.runners.registration_runner import TOP_GRACE_FACTOR
from tests.fixtures_n_helpers import TEMP_DIR, BASE_TIME, TIME_SHIFT, STR_OR_NONE_VALUES, DEFAULT_ROW_COUNT, \
    DEFAULT_GROUP_COUNT, temp_filename, CAT_LONG_TOP, CAT_SHORT_TOP, simple_run_task
# Importing fixtures generates a warning, as they are injected "automagically" to functions by pytest
# noinspection PyUnresolvedReferences
from tests.fixtures_n_helpers import datafile


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

    result = simple_run_task(req, RegistrationTaskResult)
    get_datastore().cleanup_request_data(req.request_id)
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
           col.colattrs.numeric_min == BASE_TIME \
           and col.colattrs.numeric_max == BASE_TIME + TIME_SHIFT

    col = sch.columns['float64_ts']
    assert col.coltype == DatasetColumnType.FLOAT and \
           math.floor(col.colattrs.numeric_min) == BASE_TIME and \
           math.floor(col.colattrs.numeric_max) == BASE_TIME + TIME_SHIFT

    col = sch.columns['uint32']
    assert col.coltype == DatasetColumnType.INT

    col = sch.columns['float32']
    assert col.coltype == DatasetColumnType.FLOAT

    col = sch.columns['cat_userid_str']
    assert col.coltype == DatasetColumnType.STRING and col.colattrs.categorical

    col = sch.columns['cat_short']
    assert col.coltype == DatasetColumnType.STRING and col.colattrs.categorical

    col = sch.columns['cat_long']
    assert col.coltype == DatasetColumnType.STRING and col.colattrs.categorical

    col = sch.columns['cat_float']
    assert col.coltype == DatasetColumnType.FLOAT and not col.colattrs.categorical and \
           math.floor(col.colattrs.numeric_min) == BASE_TIME and \
           math.floor(col.colattrs.numeric_max) == BASE_TIME + TIME_SHIFT

    expected_short_schema = DatasetShortSchema(min_timestamp=float(BASE_TIME),
                                               max_timestamp=float(BASE_TIME + TIME_SHIFT),
                                               source_categoricals=['cat_userid_str', 'cat_long'],
                                               potential_categoricals=['str_none_userid', 'cat_short'],
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
                                                        'cat_short': DatasetColumnType.STRING,
                                                        'cat_long': DatasetColumnType.STRING,
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
        f.write('abc' * 10000)
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


def test_top_list(datafile):
    result = run_task(datafile)
    assert result.status == TaskStatus.ENDED_SUCCESS
    assert result.dataset_schema.columns['cat_short'].colattrs.cat_top_values == \
           {str(i): w for i, w in enumerate(CAT_SHORT_TOP)}

    long_top_values = result.dataset_schema.columns['cat_long'].colattrs.cat_top_values
    assert long_top_values['0'] == CAT_LONG_TOP[0]
    assert long_top_values['1'] == CAT_LONG_TOP[1]
    assert len(long_top_values) == math.floor(config.int('dataset.categorical.top.count') * TOP_GRACE_FACTOR)
