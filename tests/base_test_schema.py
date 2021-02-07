from enum import auto
from frocket.common.dataset import DatasetColumnType, DatasetShortSchema
from frocket.common.serializable import AutoNamedEnum
from tests.base_test_utils import DisablePyTestCollectionMixin


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


DEFAULT_ROW_COUNT = 1000
DEFAULT_GROUP_COUNT = 200
DEFAULT_GROUP_COLUMN = TestColumn.int_64_userid.value
DEFAULT_TIMESTAMP_COLUMN = TestColumn.int_64_ts.value
BASE_TIME = 1609459200000  # Start of 2021, UTC
TIME_SHIFT = 10000
UNSUPPORTED_COLUMN_DTYPES = {TestColumn.unsupported_datetimes: 'datetime64[ns]',
                             TestColumn.unsupported_lists: 'object'}


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
