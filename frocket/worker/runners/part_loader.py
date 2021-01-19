import logging
import time
import os
from pathlib import Path
from typing import List, Dict, Optional, Set, NamedTuple
from pandas import DataFrame
import pyarrow.parquet
from frocket.common.config import config
from frocket.common.helpers.storage import is_remote_path, get_local_path
from frocket.common.metrics import MetricName, LoadFromLabel, MetricsBag
from frocket.common.dataset import DatasetPartId, DatasetId

logger = logging.getLogger(__name__)
DISK_CACHE_SIZE = config.int('worker.disk.cache.size.mb')


# Just a little typed nicety over tuples which PyArrow accepts as predicate pushdown filters
class FilterPredicate(NamedTuple):
    column: str
    op: str
    value: str


class CacheEntry:
    local_path: str
    size_mb: float
    last_used: float


_cache: Dict[DatasetPartId, CacheEntry] = dict()  # FileId is a NamedTuple with proper hash & equality


def _cache_current_size() -> float:
    return sum(entry.size_mb for entry in _cache.values())


def _prune_cache() -> None:
    global DISK_CACHE_SIZE
    curr_size_mb = _cache_current_size()

    while curr_size_mb > 0 and curr_size_mb > DISK_CACHE_SIZE:
        logger.info(f"Current cache size is {curr_size_mb}mb, more than the configured {DISK_CACHE_SIZE}mb")
        lru_key = min(_cache, key=lambda k: _cache[k].last_used)
        lru_entry = _cache[lru_key]
        logger.info(f"Deleting LRU entry of dataset: {lru_key.dataset_id.name} "
                    f"source path: {lru_key.path}, "
                    f"last used {time.time() - lru_entry.last_used:.1f} seconds ago")
        try:
            os.remove(lru_entry.local_path)
        except OSError:
            logger.exception('Failed to delete file!')  # TODO consider: disable any further caching
        del _cache[lru_key]
        curr_size_mb = _cache_current_size()


def load_dataframe(file_id: DatasetPartId,
                   metrics: MetricsBag,
                   needed_columns: List[str] = None,
                   filters: List[FilterPredicate] = None,
                   load_as_categoricals: List[str] = None) -> DataFrame:
    _prune_cache()

    loaded_from: Optional[LoadFromLabel] = LoadFromLabel.SOURCE
    is_source_remote = is_remote_path(file_id.path)

    local_path = None
    if not is_source_remote:
        local_path = file_id.path
    else:
        if file_id in _cache:
            local_path = _cache[file_id].local_path
            loaded_from = LoadFromLabel.DISK_CACHE
            _cache[file_id].last_used = time.time()
            logger.info("File is locally cached, yay")

    if not local_path:
        with metrics.measure(MetricName.TASK_DOWNLOAD_SECONDS):
            local_path = str(get_local_path(file_id.path))

        entry = CacheEntry()
        entry.local_path = local_path
        entry.size_mb = Path(local_path).stat().st_size / 1024 ** 2
        entry.last_used = time.time()
        _cache[file_id] = entry

    with metrics.measure(MetricName.TASK_LOAD_FILE_SECONDS):
        # Using PyArrow directly (rather than wrapped through Pandas with engine='pyarrow') allows passing column names
        # to explicitly load as 'dictionary' type, which then automatically translates to categorical columns in Pandas.
        # If the Parquet file was created with Pandas, categorical columns are loaded back as such, but we go beyond
        # that to detect 'potential categorical' string columns and load them as such here.
        # Except for the memory usage saving, there is a performance gain here if the Parquet file already has a
        # dictionary for the column. Otherwise, PyArrow will create one - but without a performance gain.
        df = pyarrow.parquet.read_table(local_path,
                                        columns=needed_columns,
                                        filters=filters,
                                        read_dictionary=load_as_categoricals).to_pandas()

    metrics.set_label_enum(loaded_from)
    return df


def get_cached_candidates(dataset_id: DatasetId) -> Optional[Set[DatasetPartId]]:
    logger.debug(f"Looking for cached candidates matching: {dataset_id}")
    candidates = None
    if _cache:
        candidates = {part_id for part_id in _cache.keys() if part_id.dataset_id == dataset_id}

    logger.debug(f"Found candidates: {candidates}")
    return candidates if (candidates and len(candidates) > 0) else None
