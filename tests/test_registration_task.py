import math
import time
from typing import cast
import numpy as np
from frocket.common.config import config
from frocket.common.dataset import DatasetInfo, DatasetId, DatasetPartId, DatasetColumnType
from frocket.common.helpers.utils import timestamped_uuid, bytes_to_ndarray
from frocket.common.tasks.base import TaskStatus
from frocket.common.tasks.registration import RegistrationTaskRequest, RegistrationTaskResult
from frocket.datastore.registered_datastores import get_blobstore, get_datastore
from frocket.worker.runners.registration_task_runner import TOP_GRACE_FACTOR
from tests.utils.task_and_job_utils import simple_run_task
from tests.utils.dataset_utils import str_and_none_column_values, STR_CAT_FEW_WEIGHTS, STR_CAT_MANY_WEIGHTS, \
    TestColumn, DEFAULT_ROW_COUNT, DEFAULT_GROUP_COUNT, DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN, BASE_TIME, \
    TIME_SHIFT, UNSUPPORTED_COLUMN_DTYPES, datafile_schema
from tests.utils.base_test_utils import TEMP_DIR, temp_filename
# noinspection PyUnresolvedReferences
from tests.utils.dataset_utils import datafile
# noinspection PyUnresolvedReferences
from tests.utils.redis_fixture import init_test_redis_settings


def run_task(datafile: str,
             group_id_column: str = DEFAULT_GROUP_COLUMN,
             timestamp_column: str = DEFAULT_TIMESTAMP_COLUMN,
             return_uniques: bool = False) -> RegistrationTaskResult:
    dsid = DatasetId.now(name=timestamped_uuid('ds-'))
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
                                  invoker_set_task_index=0)

    result = simple_run_task(req, RegistrationTaskResult)
    get_datastore().cleanup_request_data(req.request_id)
    return cast(RegistrationTaskResult, result)


def test_col_types(datafile):
    result = run_task(datafile)
    assert result.status == TaskStatus.ENDED_SUCCESS
    assert result.error_message is None
    sch = result.dataset_schema
    assert sch.group_id_column == DEFAULT_GROUP_COLUMN
    assert sch.timestamp_column == DEFAULT_TIMESTAMP_COLUMN
    assert sch.unsupported_columns == UNSUPPORTED_COLUMN_DTYPES
    assert result.group_ids_blob_id is None

    col = sch.columns[TestColumn.bool]
    assert col.to_dict() == {
        "name": TestColumn.bool, "dtypeName": "bool", "coltype": DatasetColumnType.BOOL,
        "colattrs": {
            "numericMin": None, "numericMax": None, "categorical": False,
            "catTopValues": None, "catUniqueRatio": None}}

    col = sch.columns[TestColumn.float_all_none]
    assert col.name == TestColumn.float_all_none and col.dtype_name == "float64" and \
           col.coltype == DatasetColumnType.FLOAT and np.isnan(col.colattrs.numeric_min) and \
           np.isnan(col.colattrs.numeric_max) and not col.colattrs.categorical

    col = sch.columns[TestColumn.str_object_all_none]
    assert col.coltype == DatasetColumnType.STRING and not col.colattrs.numeric_min and \
           not col.colattrs.numeric_max and not col.colattrs.categorical

    col = sch.columns[TestColumn.str_all_none]
    assert col.coltype == DatasetColumnType.STRING and not col.colattrs.numeric_min and \
           not col.colattrs.numeric_max and not col.colattrs.categorical

    col = sch.columns[TestColumn.int_64_userid]
    assert col.coltype == DatasetColumnType.INT and \
           col.colattrs.numeric_min == 0 and col.colattrs.numeric_max == DEFAULT_GROUP_COUNT - 1 and \
           not col.colattrs.categorical

    col = sch.columns[TestColumn.str_userid]
    assert col.coltype == DatasetColumnType.STRING and not col.colattrs.categorical

    col = sch.columns[TestColumn.str_and_none]
    assert col.coltype == DatasetColumnType.STRING and col.colattrs.categorical
    expected_values = str_and_none_column_values(part=0, with_none=False)  # See datafile creation
    assert col.colattrs.cat_unique_ratio == len(expected_values) / DEFAULT_ROW_COUNT
    assert sorted(col.colattrs.cat_top_values.keys()) == sorted(expected_values)
    assert np.isclose(sum(col.colattrs.cat_top_values.values()), 1.0)

    col = sch.columns[TestColumn.int_64_ts]
    assert col.coltype == DatasetColumnType.INT and \
           col.colattrs.numeric_min == BASE_TIME \
           and col.colattrs.numeric_max == BASE_TIME + TIME_SHIFT

    col = sch.columns[TestColumn.float_64_ts]
    assert col.coltype == DatasetColumnType.FLOAT and \
           math.floor(col.colattrs.numeric_min) == BASE_TIME and \
           math.floor(col.colattrs.numeric_max) == BASE_TIME + TIME_SHIFT

    col = sch.columns[TestColumn.int_u32]
    assert col.coltype == DatasetColumnType.INT

    col = sch.columns[TestColumn.float_32]
    assert col.coltype == DatasetColumnType.FLOAT

    col = sch.columns[TestColumn.str_category_userid]
    assert col.coltype == DatasetColumnType.STRING and col.colattrs.categorical

    col = sch.columns[TestColumn.str_category_few]
    assert col.coltype == DatasetColumnType.STRING and col.colattrs.categorical

    col = sch.columns[TestColumn.str_category_many]
    assert col.coltype == DatasetColumnType.STRING and col.colattrs.categorical

    col = sch.columns[TestColumn.float_category]
    assert col.coltype == DatasetColumnType.FLOAT and not col.colattrs.categorical and \
           math.floor(col.colattrs.numeric_min) == BASE_TIME and \
           math.floor(col.colattrs.numeric_max) == BASE_TIME + TIME_SHIFT

    short_schema_dict = sch.short().to_dict()
    print("Found\n", sch.short().to_json(indent=2))
    print("Expected\n", datafile_schema().to_json(indent=2))
    assert short_schema_dict == datafile_schema().to_dict()


def test_missing_group_column(datafile):
    result = run_task(datafile, group_id_column="mosh")
    assert result.status == TaskStatus.ENDED_FAILED
    result = run_task(datafile, group_id_column="lists")  # Name of an unsupported column
    assert result.status == TaskStatus.ENDED_FAILED


def test_str_group_column_ok(datafile):
    result = run_task(datafile, group_id_column=TestColumn.str_userid)
    assert result.status == TaskStatus.ENDED_SUCCESS


def test_group_column_with_none_notok(datafile):
    result = run_task(datafile, group_id_column=TestColumn.str_and_none)
    assert result.status == TaskStatus.ENDED_FAILED


def test_float_group_column_notok(datafile):
    result = run_task(datafile, group_id_column=TestColumn.float_64_ts)
    assert result.status == TaskStatus.ENDED_FAILED


def test_missing_timestamp_column(datafile):
    result = run_task(datafile, timestamp_column="mosh")
    assert result.status == TaskStatus.ENDED_FAILED


def test_float_timestamp_column_ok(datafile):
    result = run_task(datafile, timestamp_column=TestColumn.float_64_ts)
    assert result.status == TaskStatus.ENDED_SUCCESS


def test_ts_column_with_none_notok(datafile):
    result = run_task(datafile, timestamp_column=TestColumn.float_32)
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
    assert result.dataset_schema.columns[TestColumn.str_category_few].colattrs.cat_top_values == \
           {str(i): w for i, w in enumerate(STR_CAT_FEW_WEIGHTS)}

    long_top_values = result.dataset_schema.columns[TestColumn.str_category_many].colattrs.cat_top_values
    assert long_top_values['0'] == STR_CAT_MANY_WEIGHTS[0]
    assert long_top_values['1'] == STR_CAT_MANY_WEIGHTS[1]
    assert len(long_top_values) == math.floor(config.int('dataset.categorical.top.count') * TOP_GRACE_FACTOR)
