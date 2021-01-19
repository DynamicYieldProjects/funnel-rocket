from typing import Iterable
import pandas as pd
import numpy as np


def filter_by_isin(df: pd.DataFrame, column: str, values: Iterable) -> pd.DataFrame:
    """
    For the given DataFrame, return only rows where df[column] is in the given values.
    This is a surprisingly faster alternative to built-in Pandas/NumPy functions: df[np.isin(df[column], values)]
    A value can appear in multiple rows (e.g. the same user ID appearing multiple rows)

    TODO Merge a [Numba-based isin()](https://stackoverflow.com/questions/53046473/numpy-isin-performance-improvement)
     function, compiled AOT for relevant array dtypes. This would be arch-dependent and optional (with fallback)
    """
    # First, create a "map" series from all possible values in the column => whether they should pass the filter
    all_ids = df[column].unique()
    is_id_relevant = pd.Series(np.zeros(len(all_ids)), index=all_ids).astype('bool')  # Default false
    is_id_relevant.loc[values] = True

    # Create a boolean mask for column, based on the mapping above. Grab the raw array.
    mask = is_id_relevant[df[column]].values
    # Apply mask
    return df[mask]


def add_column_by_value_map(df: pd.DataFrame, keys_column: str, values_map_series: pd.Series, new_column: str) -> None:
    """
    Add a new column to the given df. For each row, df[new_column] will be set to an appropriate value from
    values_map_series: the value whose index is df[keys_column] in that row.

    e.g. given a DF of user activities having a userId column (with potentially multiple rows per user), and a
    values_map_series whose unique index is a User ID, and its values are the age of that user, the function will add
    a new column to the given DF with the age of that row's user ID

    If a value in keys_column does not have a matching index in values_map_series, the cell value would be NaN.
    This function is optimized for performance.

    The given DF is modified inplace.
    """
    # Create a new mapping between ALL unique values of IDs of df[keys_column] and their matching value (or NaN)
    unique_keys = df[keys_column].unique()
    key_to_value = pd.Series(data=np.nan, index=unique_keys)
    key_to_value.loc[values_map_series.index] = values_map_series

    # Now we can create the new column, using the mapping
    df[new_column] = key_to_value[df[keys_column]].values
