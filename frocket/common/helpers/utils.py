"""For everything but the kitchen sink."""
import functools
import math
import random
import uuid
import time
from io import BytesIO
from typing import Optional, List
import pandas as pd
import pyarrow.feather as feather
import numpy as np


def terminal_red(message: str) -> str:
    return f"\033[31m{message}\033[0m"


def terminal_green(message: str) -> str:
    return f"\033[32m{message}\033[0m"


def memoize(obj):
    """Standard issue memoization decorator for caching function results (which don't need invalidation)."""
    cache = obj._cache = {}

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]

    return memoizer


def sample_from_range(range_max: int,
                      sample_ratio: float,
                      max_samples: int,
                      preselected: Optional[List[int]]) -> List[int]:
    """
    Given a range of numbers in 0..range_max, return random samples.
    Count of samples is set by sample_ratio, up to max_samples.
    If preselected is passed, include these indexes first.
    """
    available_indexes = list(range(range_max))
    sample_count = min(math.floor(range_max * sample_ratio), max_samples)

    if preselected:
        chosen = list(preselected)
        for i in preselected:
            available_indexes.remove(i)
        sample_count = max(sample_count - len(preselected), 0)
    else:
        chosen = []

    if sample_count > 0:
        chosen += random.choices(available_indexes, k=sample_count)
    return chosen


def timestamped_uuid(prefix: str = None) -> str:
    return f"{prefix or ''}{math.floor(time.time())}-{str(uuid.uuid4())[:8]}"


def ndarray_to_bytes(arr: np.ndarray) -> bytes:
    """Use PyArrow's feather format as a compute- and space-efficient format for serializing NumPy arrays."""
    df = pd.DataFrame(data={'arr': arr})
    buf = BytesIO()
    # noinspection PyTypeChecker
    feather.write_feather(df, buf)
    buf.seek(0)
    return buf.read()


def bytes_to_ndarray(data: bytes) -> np.ndarray:
    df = feather.read_feather(BytesIO(data))
    return df['arr']
