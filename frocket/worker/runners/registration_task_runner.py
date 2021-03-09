"""
Run the dataset registration task for a single part.

The task's job is to validate *a single file only*, return its validity & schame, and optionally return the list of
unique group IDs in the files so that the job object (on the invoker side) can then validate schema & uniques ACROSS
the sampled files.
"""
import logging
import math
from typing import cast, Optional
import numpy as np
from pandas import DataFrame, Series, CategoricalDtype
from pandas.core.dtypes.common import is_numeric_dtype, is_bool_dtype, is_integer_dtype, \
    is_float_dtype, is_string_dtype, is_categorical_dtype
from frocket.common.config import config
from frocket.common.dataset import DatasetSchema, DatasetColumn, DatasetColumnType, DatasetColumnAttributes
from frocket.common.metrics import MetricName
from frocket.common.tasks.base import TaskStatus, BaseTaskRequest, TaskAttemptId, BlobId
from frocket.common.tasks.registration import RegistrationTaskResult, RegistrationTaskRequest
from frocket.common.helpers.utils import ndarray_to_bytes
from frocket.common.validation.consts import CONDITION_COLUMN_PREFIX
from frocket.worker.runners.base_task_runner import BaseTaskRunner, TaskRunnerContext

logger = logging.getLogger(__name__)
# The top values for categoricals as recorded in the schema are approximate, as we're not reading all dataset files
# to get exact counts, nor return a complete list of value->counts per file. Instead, there's a simple factor over the
# amount of configured top values length which each task returns.
TOP_GRACE_FACTOR = 1.5


class RegistrationTaskRunner(BaseTaskRunner):
    def __init__(self, req: BaseTaskRequest, ctx: TaskRunnerContext):

        super().__init__(req, ctx)
        self._req = cast(RegistrationTaskRequest, req)
        self._part_id = self._req.part_id
        self._task_attempt_id = TaskAttemptId(req.invoker_set_task_index, req.attempt_no)  # TODO backlog handle nicer
        self._schema = None
        self._uniques_blob_id = None
        self._warnings = []
        # See configuration guide for these values
        # TODO backlog pass these configuration attrs from the invoker, so they can be controlled there
        self._categorical_ratio = config.float('dataset.categorical.ratio')
        self._categorical_top_count = math.floor(config.int('dataset.categorical.top.count') * TOP_GRACE_FACTOR)

    def _do_run(self):
        self._update_status(TaskStatus.LOADING_DATA)
        with self._ctx.metrics.measure(MetricName.TASK_TOTAL_LOAD_SECONDS):
            df = self._ctx.part_loader.load_dataframe(self._part_id, self._ctx.metrics)

        self._update_status(TaskStatus.RUNNING)
        self._validate_basic_cols(df)
        self._schema = self._build_schema(df)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Schema: {self._schema.to_json(indent=2)}")

        if self._req.dataset.group_id_column not in self._schema.columns or \
           self._req.dataset.timestamp_column not in self._schema.columns:
            raise Exception(f"Group column or timestamp column not included in result schema: {self._schema.to_json()}")

        if self._req.return_group_ids:
            self._uniques_blob_id = self.write_uniques_as_blob(df)
            logger.info(f"Wrote unique group IDs to blob ID: {self._uniques_blob_id}")

    def _validate_basic_cols(self, df: DataFrame):
        # Validate presence & content of the group ID & timestamp columns, which are mandatory in any dataset
        col_names = df.dtypes.index.tolist()
        group_id_colname = self._req.dataset.group_id_column
        ts_colname = self._req.dataset.timestamp_column

        if group_id_colname not in col_names:
            raise Exception(f"Column {group_id_colname} not found in file")
        if not (is_integer_dtype(df[group_id_colname]) or is_string_dtype(df[group_id_colname])):
            raise Exception(f"Group column {group_id_colname} type must be either integer or string, "
                            f"but found: {df[group_id_colname].dtype}")
        if df[group_id_colname].isnull().any():
            raise Exception(f"Some values in column {group_id_colname} are null")
        logger.info(f"Column {group_id_colname} was found, and without null values")

        if ts_colname not in col_names:
            raise Exception(f"Column {ts_colname} not found in file")
        ts_column = df[ts_colname]
        if not is_numeric_dtype(ts_column.dtype):
            raise Exception(f"Column {ts_colname} is not numeric")
        if np.isnan(ts_column).any():
            raise Exception(f"Column {ts_colname} has NaN values")
        logger.info(f"Column {ts_colname} was found, is numeric and without NaN values")

    def _build_schema(self, df: DataFrame) -> DatasetSchema:
        columns = {}
        unsupported_columns = {}
        all_columns = {name: df[name] for name in df.columns.tolist()}
        for name, series in all_columns.items():
            # noinspection PyBroadException
            try:
                self._validate_column_name(name)
                col_info = self._build_column(series)
                if col_info:
                    columns[name] = col_info
                else:
                    unsupported_columns[name] = str(series.dtype)
            except Exception as e:
                self._warnings.append(f"Skipping column {name} due to error: {e}")
                unsupported_columns[name] = str(series.dtype)

        if len(self._warnings) > 0:
            logger.warning(f"Encountered the following warnings: {self._warnings}")

        return DatasetSchema(columns=columns, unsupported_columns=unsupported_columns,
                             group_id_column=self._req.dataset.group_id_column,
                             timestamp_column=self._req.dataset.timestamp_column)

    # noinspection PyMethodMayBeStatic
    def _validate_column_name(self, name: str) -> None:
        if name.startswith(CONDITION_COLUMN_PREFIX):
            raise Exception(f"Column name starting with {CONDITION_COLUMN_PREFIX} may be overriden by query engine")

    def _build_column(self, series: Series) -> Optional[DatasetColumn]:
        coltype = None
        colattrs = None
        if is_categorical_dtype(series.dtype):
            actual_dtype = series.cat.categories.dtype  # e.g. str for a categorical string column
        else:
            actual_dtype = series.dtype

        if is_bool_dtype(actual_dtype):
            coltype = DatasetColumnType.BOOL
            colattrs = DatasetColumnAttributes()
        elif is_numeric_dtype(actual_dtype):
            if is_integer_dtype(actual_dtype) or is_float_dtype(actual_dtype):
                coltype = DatasetColumnType.INT if is_integer_dtype(actual_dtype) else DatasetColumnType.FLOAT
                stats = series.describe()
                colattrs = DatasetColumnAttributes(numeric_min=stats['min'], numeric_max=stats['max'])
        elif self._is_string_series(series):
            coltype = DatasetColumnType.STRING
            colattrs = self._build_string_attributes(series)

        if coltype:
            return DatasetColumn(name=str(series.name), dtype_name=str(series.dtype),
                                 coltype=coltype, colattrs=colattrs)
        else:
            return None

    @staticmethod
    def _is_string_series(series: Series):
        def string_or_none(v):
            return v is None or type(v) is str

        # If the validated file was created with Pandas, it might already have categorical columns -
        # in which case the .apply() call below also returns a categorical type that will need conversion to bool
        if is_string_dtype(series.dtype) or \
                (is_categorical_dtype(series.dtype) and is_string_dtype(series.cat.categories.dtype)):
            # If the series has a mix of strings and other types, Pandas may think it's a string column -
            # so ensure no non-strings are present.
            # See: https://youtrack.jetbrains.com/issue/PY-43841 for type warning issue below
            # noinspection PyTypeChecker
            are_all_strings = series.apply(string_or_none).astype('bool').all()
            if are_all_strings:
                return True
        return False

    def _build_string_attributes(self, series: Series) -> DatasetColumnAttributes:
        dtype = series.dtype
        is_categorical = is_categorical_dtype(dtype)
        unique_values_ratio = None

        if is_categorical:
            categories = cast(CategoricalDtype, dtype).categories
            # If field was loaded as categorical, respect that. We don't validate the ratio, but do store it.
            unique_values_ratio = len(categories) / len(series)
        else:
            categories = cast(CategoricalDtype, series.astype('category').dtype).categories
            if len(categories) > 0:
                # Only treat string columns as categorical if the ratio of unique values to total values is below X
                unique_values_ratio = len(categories) / len(series)
                if unique_values_ratio <= self._categorical_ratio:
                    is_categorical = True

        if is_categorical:
            if self._categorical_top_count == 0:  # Don't return top values
                top_values = None
            else:
                top_values = series.value_counts(normalize=True)[0:self._categorical_top_count].to_dict()

            return DatasetColumnAttributes(categorical=True,
                                           cat_unique_ratio=unique_values_ratio,
                                           cat_top_values=top_values)
        else:
            return DatasetColumnAttributes()

    def write_uniques_as_blob(self, df) -> BlobId:
        uniques = df[self._req.dataset.group_id_column].unique()
        array_bytes = ndarray_to_bytes(uniques)
        logger.info(f"Uniques array len: {len(uniques)}, serialized bytes: {len(array_bytes)}")

        # Tag is optional, just for debugging/understanding keys' meaning in the blobstore
        blob_tag = f"uniques-{self._part_id.dataset_id.name}-{self._part_id.part_idx}"
        return self._ctx.blobstore.write_blob(array_bytes, tag=blob_tag)

    def _build_result(self, base_attributes: dict):
        return RegistrationTaskResult(**base_attributes,
                                      dataset_schema=self._schema,
                                      part_id=self._part_id,
                                      group_ids_blob_id=self._uniques_blob_id)
