"""
Load and cache parts (data files).
"""
import logging
import time
import os
from pathlib import Path
from typing import List, Dict, Optional, Set, NamedTuple
from pandas import DataFrame
import pyarrow.parquet
from frocket.common.config import config
from frocket.common.helpers.storage import storage_handler_for
from frocket.common.helpers.utils import memoize
from frocket.common.metrics import MetricName, LoadFromLabel, MetricsBag
from frocket.common.dataset import DatasetPartId, DatasetId

logger = logging.getLogger(__name__)


# Just a little typed nicety over tuples which PyArrow accepts as predicate pushdown filters
class FilterPredicate(NamedTuple):
    column: str
    op: str
    value: str


class CacheEntry:
    local_path: str
    size_mb: float
    last_used: float


class PartLoader:
    _cache: Dict[DatasetPartId, CacheEntry] = None  # DatasetPartId is a dataclass with proper hash & equality
    _disk_cache_max_size: float = None

    def __init__(self):
        self._setup()

    # Support re-initialization and overriding the configured size, for testing
    def _setup(self, disk_cache_max_size: float = None):
        if self._cache:
            for entry in self._cache.values():
                os.remove(entry.local_path)
        self._cache = {}
        self._disk_cache_max_size = disk_cache_max_size if disk_cache_max_size is not None \
            else config.float('worker.disk.cache.size.mb')

    @property
    def cache_current_size_mb(self) -> float:
        return sum(entry.size_mb for entry in self._cache.values())

    @property
    def cache_len(self) -> int:
        return len(self._cache)

    def _prune_cache(self) -> None:
        curr_size_mb = self.cache_current_size_mb
        while curr_size_mb > 0 and curr_size_mb > self._disk_cache_max_size:
            logger.info(f"Current cache size is {curr_size_mb}mb, more than the configured "
                        f"{self._disk_cache_max_size}mb")
            lru_key = min(self._cache, key=lambda k: self._cache[k].last_used)
            lru_entry = self._cache[lru_key]
            logger.info(f"Deleting LRU entry of dataset: {lru_key.dataset_id.name} "
                        f"source path: {lru_key.path}, "
                        f"last used {time.time() - lru_entry.last_used:.1f} seconds ago")
            try:
                os.remove(lru_entry.local_path)
            except OSError:
                logger.exception('Failed to delete file!')  # TODO backlog consider disabling any further caching
            del self._cache[lru_key]
            curr_size_mb = self.cache_current_size_mb

    def load_dataframe(self,
                       file_id: DatasetPartId,
                       metrics: MetricsBag,
                       needed_columns: List[str] = None,
                       filters: List[FilterPredicate] = None,
                       load_as_categoricals: List[str] = None) -> DataFrame:
        self._prune_cache()
        loaded_from: Optional[LoadFromLabel] = LoadFromLabel.SOURCE
        handler = storage_handler_for(file_id.path)
        is_source_remote = handler.remote

        local_path = None
        if not is_source_remote:
            local_path = file_id.path  # No caching for local files
        else:
            if file_id in self._cache:
                local_path = self._cache[file_id].local_path
                loaded_from = LoadFromLabel.DISK_CACHE
                self._cache[file_id].last_used = time.time()
                logger.info("File is locally cached, yay")

        if not local_path:
            with metrics.measure(MetricName.TASK_DOWNLOAD_SECONDS):
                local_path = str(handler.get_local_path(file_id.path))  # Download to a local temp file

            entry = CacheEntry()
            entry.local_path = local_path
            entry.size_mb = Path(local_path).stat().st_size / 1024 ** 2
            entry.last_used = time.time()
            self._cache[file_id] = entry

        with metrics.measure(MetricName.TASK_LOAD_FILE_SECONDS):
            # Using PyArrow directly (rather than wrapped through Pandas) allows specifying column names to explicitly
            # load as 'dictionary' type, which then translates to categoricals in Pandas.
            # If the file was created with Pandas, categorical columns are loaded back as such - but we go beyond
            # that to detect 'potential categorical' string columns and load them as such.
            # Except for the memory usage saving, there is a performance gain here if the Parquet file already has a
            # dictionary for the column. Otherwise, PyArrow will create one - but without a performance gain.
            df = pyarrow.parquet.read_table(local_path,
                                            columns=needed_columns,
                                            filters=filters,
                                            read_dictionary=load_as_categoricals).to_pandas()

        metrics.set_label_enum(loaded_from)
        return df

    def get_cached_candidates(self, dataset_id: DatasetId) -> Optional[Set[DatasetPartId]]:
        """Do we have cached parts for this DatasetId, that can be used to self-select parts?"""
        logger.debug(f"Looking for cached candidates matching: {dataset_id}")
        candidates = None
        if self._cache:
            candidates = {part_id for part_id in self._cache.keys() if part_id.dataset_id == dataset_id}

        logger.debug(f"Found candidates: {candidates}")
        return candidates if (candidates and len(candidates) > 0) else None


@memoize
def shared_part_loader() -> PartLoader:
    """This is used by default, but can be overriden in tests."""
    return PartLoader()
