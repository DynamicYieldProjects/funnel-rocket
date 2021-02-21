import logging
from enum import auto
from datetime import datetime, timezone
from typing import Optional, List, Dict
from dataclasses import dataclass, field
from frocket.common.serializable import SerializableDataClass, AutoNamedEnum, api_public_field

logger = logging.getLogger(__name__)


class PartNamingMethod(AutoNamedEnum):
    RUNNING_NUMBER = auto()
    LIST = auto()


# TODO doc (this is used for versioning -> cache invalidation)
@dataclass(frozen=True)
class DatasetId(SerializableDataClass):
    name: str = api_public_field()
    registered_at: datetime = api_public_field()

    @classmethod
    def now(cls, name: str):
        return DatasetId(name, registered_at=datetime.now(tz=timezone.utc))


@dataclass(frozen=True)
class DatasetPartId(SerializableDataClass):
    dataset_id: DatasetId
    path: str
    part_idx: int


# This class should be kept pretty small, as it's passed in task requests.
# More information can be found in the short and full schema which are stored separately
@dataclass(frozen=True)
class DatasetInfo(SerializableDataClass):
    basepath: str
    total_parts: int
    id: DatasetId = api_public_field()
    group_id_column: str = api_public_field()
    timestamp_column: str = api_public_field()


@dataclass(frozen=True)
class DatasetPartsInfo(SerializableDataClass):
    naming_method: PartNamingMethod
    total_parts: int
    total_size: int
    running_number_pattern: Optional[str] = field(default=None)
    filenames: Optional[List[str]] = field(default=None)

    def fullpaths(self, parent: DatasetInfo) -> List[str]:
        parentpath = parent.basepath if parent.basepath.endswith('/') else parent.basepath + '/'

        if self.naming_method == PartNamingMethod.LIST:
            assert (self.filenames and len(self.filenames) == parent.total_parts)
            return [parentpath + filename for filename in self.filenames]
        else:
            assert self.running_number_pattern
            return [parentpath + self.running_number_pattern.format(idx)
                    for idx in range(parent.total_parts)]


class DatasetColumnType(AutoNamedEnum):
    INT = auto()
    FLOAT = auto()
    BOOL = auto()
    STRING = auto()


# TODO convert to numeric/string sub-classes - on deserialization need to read the right type
@dataclass(frozen=True)
class DatasetColumnAttributes(SerializableDataClass):
    numeric_min: Optional[float] = None
    numeric_max: Optional[float] = None
    categorical: bool = False
    cat_top_values: Optional[Dict[str, float]] = None
    cat_unique_ratio: Optional[float] = None


@dataclass(frozen=True)
class DatasetColumn(SerializableDataClass):
    name: str
    dtype_name: str
    coltype: DatasetColumnType
    colattrs: DatasetColumnAttributes


@dataclass(frozen=True)
class DatasetShortSchema(SerializableDataClass):
    columns: Dict[str, DatasetColumnType] = api_public_field()
    min_timestamp: float = api_public_field()
    max_timestamp: float = api_public_field()
    source_categoricals: List[str] = api_public_field(default=None)
    potential_categoricals: List[str] = api_public_field(default=None)


@dataclass(frozen=True)
class DatasetSchema(SerializableDataClass):
    group_id_column: str = api_public_field()
    timestamp_column: str = api_public_field()
    columns: Dict[str, DatasetColumn] = api_public_field()
    unsupported_columns: Dict[str, str] = api_public_field()

    def short(self) -> DatasetShortSchema:
        cols = {name: col.coltype for name, col in self.columns.items()}
        source_categoricals = []
        potential_categoricals = []
        for name, col in self.columns.items():
            if col.colattrs.categorical:
                if col.dtype_name == 'category':
                    source_categoricals.append(name)
                else:
                    potential_categoricals.append(name)
        ts_attrs = self.columns[self.timestamp_column].colattrs
        min_ts = ts_attrs.numeric_min
        max_ts = ts_attrs.numeric_max

        return DatasetShortSchema(columns=cols,
                                  source_categoricals=source_categoricals,
                                  potential_categoricals=potential_categoricals,
                                  min_timestamp=min_ts, max_timestamp=max_ts)
