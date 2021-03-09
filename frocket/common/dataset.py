"""Base classes for registered datasets and their metadata."""
import logging
from enum import auto
from datetime import datetime, timezone
from typing import Optional, List, Dict
from dataclasses import dataclass, field
from frocket.common.serializable import SerializableDataClass, AutoNamedEnum

logger = logging.getLogger(__name__)


class PartNamingMethod(AutoNamedEnum):
    """
    For future use: currently the full list of dataset filenames is stored as metadata, however if a consistent
    numbering pattern should be identified, it may be useful both for more compact metadata and for a more predictable
    part index -> filename mapping.
    """
    RUNNING_NUMBER = auto()
    LIST = auto()


@dataclass(frozen=True)
class DatasetId(SerializableDataClass):
    """
    The main reason why this class exists: datasets can be re-registered multiple times with the same name, but any
    caching behavior should be sensitive to the registered date and become invalid on re-registration.
    In concrete terms, caching should be based on DatasetId keys (which are immutable) rather than a dataset name.

    Re-registering a dataset is useful, in cases such as:
    1. When you don't need to manage revisions yourself (via specifying a new dataset name and un-registering old ones).
    2. As an alias to the current version (datasets are only metadata, you can register the same physical files N times)
    3. If the datafiles were found to be incomplete/invalid, and after fixing the issue you want to invalidate caching.
    """
    name: str
    registered_at: datetime

    @classmethod
    def now(cls, name: str):
        return DatasetId(name, registered_at=datetime.now(tz=timezone.utc))


@dataclass(frozen=True)
class DatasetPartId(SerializableDataClass):
    """Specifies a single part (file) in a dataset version (see documetation for DatasetId above!)."""
    dataset_id: DatasetId
    path: str
    part_idx: int


@dataclass(frozen=True)
class DatasetInfo(SerializableDataClass):
    """
    Basic metadata for a dataset.

    This class should be kept pretty small, as it's passed along in task requests.
    More detailed metadata is found in the data schema object, which is stored separately and read when needed
    (and also exists in both short and full versions)
    """
    basepath: str
    total_parts: int
    id: DatasetId
    group_id_column: str  # The column by which the dataset is partitioned, and grouping is done.
    timestamp_column: str  # The column by which timeframe conditions and funnels are run.


@dataclass(frozen=True)
class DatasetPartsInfo(SerializableDataClass):
    """Holds the list of files in the dataset. Separate from DatasetInfo only due to size (this data is usually not
    needed to be sent in network calls)."""
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
    # Categorical columns are not a separate type to the query engine. That designation exists and is used separately.
    STRING = auto()


@dataclass(frozen=True)
class DatasetColumnAttributes(SerializableDataClass):
    """
    The 'full' information on each column. TODO backlog use polymorphism? (needs support in de-serialization)

    For columns which were either saved by Pandas as categoricals, or are identified during registration to be such,
    store a mapping of top N values (configurable) to their their normalized share in the dataset. Since registration
    does not read all files but only a sample, that ratio cannot be an absolute number or the exact ratio - but still
    useful for clients.

    cat_unique_ratio is the ratio of unique value count to all values (or: series.nunique()/len(series)), and may be
    a useful rough indicator to how much RAM is saved (and str.match() operations sped-up!) by the categorical
    representation. Columns are determined to be loaded as categorical if this value is lower than configured.
    Loading of columns as categoricals is also usually much faster, but that greatly depends on whether a dictionary
    was saved for that column in the Parquet file or not - so it depends on the tool used to create these files.
    """
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
    """Schema, the short version - typically all you may need."""
    columns: Dict[str, DatasetColumnType]
    min_timestamp: float
    max_timestamp: float
    # In files created by Pandas with its metadata intact in the Parquet file, columns marked as categoricals.
    source_categoricals: List[str] = field(default=None)
    # Columns detected during registration to be good candidates for explicitly loading as categoricals (by PyArrow).
    potential_categoricals: List[str] = field(default=None)


@dataclass(frozen=True)
class DatasetSchema(SerializableDataClass):
    group_id_column: str
    timestamp_column: str
    columns: Dict[str, DatasetColumn]
    # Just the names->dtypes of all columns not (currently) supported.
    unsupported_columns: Dict[str, str]

    def short(self) -> DatasetShortSchema:
        """Make short from full."""
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
