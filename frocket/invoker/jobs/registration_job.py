import logging
import time
from collections import Counter
import numpy as np
from typing import List, Optional, cast, Dict
from frocket.common.config import config
from frocket.common.dataset import DatasetInfo, DatasetPartId, DatasetPartsInfo, \
    DatasetSchema, DatasetColumnType, DatasetColumnAttributes, DatasetColumn, DatasetId
from frocket.common.helpers.storage import storage_handler_for
from frocket.common.metrics import JobTypeLabel, DATASET_LABEL
from frocket.common.tasks.base import ErrorMessage
from frocket.common.tasks.registration import RegistrationTaskRequest, RegistrationJobResult, \
    RegistrationTaskResult, DatasetValidationMode, RegisterArgs
from frocket.common.helpers.utils import sample_from_range, bytes_to_ndarray
from frocket.datastore.registered_datastores import get_blobstore, get_datastore
from frocket.invoker.jobs.job import Job

logger = logging.getLogger(__name__)

VALIDATION_SAMPLE_RATIO = config.float('validation.sample.ratio')  # Ratio of sample size to dataset total parts
VALIDATION_MAX_SAMPLES = config.int('validation.sample.max')  # Hard limit on on. of samples
# How many 'top N' values to store in schema for categoricl fields
CATEGORICAL_TOP_COUNT = config.int('dataset.categorical.top.count')
# To be included in Top N list, a value should have at least this ratio of occurences to total row count
CATEGORICAL_TOP_MIN_PCT = config.float('dataset.categorical.top.minpct')


class RegistrationJob(Job):
    def __init__(self, args: RegisterArgs):
        self._args = args
        self._dataset = None
        self._parts_info = None
        self._validate_uniques = None
        self._sampled_parts = None
        self._final_schema = None

    def prerun(self, async_updater=None):
        # Before validation tasks are run, finc out which parts (files) there are in the given path
        if async_updater:
            async_updater.update(message=f"Discovering files in {self._args.basepath}")

        # TODO backlog run file discovery in a worker task, not directly by the invoker, for better separation of
        #  concerns and *permissions* (invoker won't need list permission to remote storage)
        parts_info = self._discover_parts(self._args.basepath, self._args.pattern)
        if parts_info.total_parts == 0:
            return f"No files found at {self._args.basepath}"

        # If needed, adjust validation mode and whether to validate uniques by actual no. of parts
        validate_uniques = self._args.validate_uniques
        if parts_info.total_parts == 1:
            mode = DatasetValidationMode.SINGLE
            validate_uniques = False
        elif parts_info.total_parts == 2 and self._args.validation_mode == DatasetValidationMode.SAMPLE:
            mode = DatasetValidationMode.FIRST_LAST
        else:
            mode = self._args.validation_mode

        # The dataset will be persisted to datastore once registration completes successfully
        dataset = DatasetInfo(id=DatasetId.now(self._args.name),
                              basepath=self._args.basepath,
                              total_parts=parts_info.total_parts,
                              group_id_column=self._args.group_id_column,
                              timestamp_column=self._args.timestamp_column)

        self._sampled_parts = self._choose_sampled_parts(dataset, parts_info, mode)
        self._dataset = dataset
        self._parts_info = parts_info
        self._validate_uniques = validate_uniques
        self._labels = {
            JobTypeLabel.REGISTER.label_name: JobTypeLabel.REGISTER.label_value,
            DATASET_LABEL: dataset.id.name
        }

    @staticmethod
    def _discover_parts(basepath: str, pattern: str):
        handler = storage_handler_for(basepath)  # Returns the appropriate storage handler based on the path format
        result = handler.discover_files(pattern)
        if result.total_parts > 0:
            logger.info(f"Number of part files: {result.total_parts}, "
                        f"total size {result.total_size / (1024 ** 2):.2f}mb")
            if result.filenames:
                logger.debug(f"List of files: {result.filenames}")
        return result

    @staticmethod
    def _choose_sampled_parts(dataset: DatasetInfo,
                              parts: DatasetPartsInfo,
                              mode: DatasetValidationMode) -> List[DatasetPartId]:
        """Determine the list of parts to sample, and return as DatasetPartIds"""
        if mode == DatasetValidationMode.SINGLE:
            chosen_indexes = [0]
        elif mode == DatasetValidationMode.FIRST_LAST:
            chosen_indexes = [0, parts.total_parts - 1]
        else:
            # In SAMPLE mode, always include first & last part, plus zero..VALIDATION_MAX_SAMPLES extra parts
            preselected = [0, parts.total_parts - 1]
            chosen_indexes = sample_from_range(range_max=parts.total_parts,
                                               sample_ratio=VALIDATION_SAMPLE_RATIO,
                                               max_samples=VALIDATION_MAX_SAMPLES,
                                               preselected=preselected)

        all_paths = parts.fullpaths(dataset)
        result = [DatasetPartId(dataset_id=dataset.id, path=all_paths[part_index], part_idx=part_index)
                  for part_index in chosen_indexes]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Parts chosen to be sampled: {result}")
        return result

    def parts_info(self):
        return self._parts_info

    def total_tasks(self):
        return len(self._sampled_parts)

    def build_tasks(self):
        requests = [self._build_task(task_index=i) for i in range(self.total_tasks())]
        return requests

    def build_retry_task(self, attempt_no, task_index):
        return self._build_task(task_index, attempt_no=attempt_no)

    def _build_task(self, task_index: int, attempt_no: int = 0) -> RegistrationTaskRequest:
        request = RegistrationTaskRequest(
            request_id=self._request_id,
            invoke_time=time.time(),
            invoker_set_task_index=task_index,  # Task index & part are always set by invoker in this job
            attempt_no=attempt_no,
            dataset=self._dataset,
            part_id=self._sampled_parts[task_index],
            return_group_ids=self._validate_uniques)
        return request

    def complete(self, job_status, latest_task_results, async_updater=None):
        """Now that all individual tasks have completed, check for consistency between task results, and build the
        aggregated schema."""
        if not job_status.success:
            return job_status

        try:
            # Validation across task results
            task_results = cast(List[RegistrationTaskResult], latest_task_results)
            if self._validate_uniques:
                message = self._check_uniques(task_results)
                if message:
                    return job_status.with_error(message=message)

            message = self._compare_columns(task_results)
            if message:
                return job_status.with_error(message=message)

            # All valid, now aggregate the final schema
            self._final_schema = self._aggregate_schema(task_results)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Short schema:" + self._final_schema.short().to_json(indent=2))

            # Register to datastore
            datastore = get_datastore()
            datastore.write_dataset_info(self._dataset, self._parts_info, self._final_schema)
            logger.info(f"Dataset '{self._dataset.id.name}' successfully registered in {datastore}")
            return job_status
        except Exception as e:
            logger.exception("Validation of sampled parts failed unexpectedly")
            return job_status.with_error(message=str(e))

    def _aggregate_schema(self, task_results: List[RegistrationTaskResult]) -> DatasetSchema:
        full_schemas = [res.dataset_schema for res in task_results]
        new_columns: Dict[str, DatasetColumn] = {}
        for name, col in full_schemas[0].columns.items():
            all_colattrs = [s.columns[name].colattrs for s in full_schemas]

            if col.coltype == DatasetColumnType.BOOL:
                new_attrs = col.colattrs  # No change

            elif col.coltype in [DatasetColumnType.INT, DatasetColumnType.FLOAT]:
                new_attrs = DatasetColumnAttributes(
                    numeric_min=min([at.numeric_min for at in all_colattrs]),
                    numeric_max=max([at.numeric_max for at in all_colattrs]))

            elif col.coltype == DatasetColumnType.STRING:
                is_categorical = col.colattrs.categorical
                if is_categorical and False in [at.categorical for at in all_colattrs]:
                    logger.info(f"Not all part schemas have column {name} as categorical")
                    is_categorical = False

                if is_categorical:
                    approx_uniques_ratio = sum([at.cat_unique_ratio for at in all_colattrs]) / len(all_colattrs)
                    approx_top_values = self._categorical_top_values(all_colattrs)
                else:
                    approx_uniques_ratio = None
                    approx_top_values = None

                new_attrs = DatasetColumnAttributes(categorical=is_categorical,
                                                    cat_unique_ratio=approx_uniques_ratio,
                                                    cat_top_values=approx_top_values)

            else:
                raise Exception(f"Column {name} has unkonwn type {col.coltype}")
            assert new_attrs
            new_columns[name] = DatasetColumn(name=name, dtype_name=col.dtype_name,
                                              coltype=col.coltype, colattrs=new_attrs)

        # TODO backlog consider merging list of unsupported columns from all sampled parts
        return DatasetSchema(columns=new_columns,
                             unsupported_columns=full_schemas[0].unsupported_columns,
                             group_id_column=full_schemas[0].group_id_column,
                             timestamp_column=full_schemas[0].timestamp_column)

    @staticmethod
    def _categorical_top_values(all_colattrs):
        all_tops = [at.cat_top_values for at in all_colattrs if at.cat_top_values]
        if len(all_tops) == 0:
            return None
        aggr_tops = Counter()
        for top in all_tops:
            aggr_tops.update(top)
        highest_aggr = aggr_tops.most_common(CATEGORICAL_TOP_COUNT)
        highest_normalized = [(name, value / len(all_colattrs))
                              for (name, value) in highest_aggr]
        approx_top_values = {name: value for name, value in highest_normalized
                             if value >= CATEGORICAL_TOP_MIN_PCT}
        return approx_top_values

    def _compare_columns(self, task_results: List[RegistrationTaskResult]) -> Optional[ErrorMessage]:
        short_schemas = [res.dataset_schema.short() for res in task_results]
        first_schema = short_schemas[0]

        for i, curr_schema in enumerate(short_schemas):
            if i > 0 and first_schema.columns != curr_schema.columns:
                message = f"Schemas differ!\n" \
                          f"In {self._sampled_parts[task_results[0].task_index].path} " \
                          f"({len(first_schema.columns)} columns):\n{first_schema} vs.\n" \
                          f"In {self._sampled_parts[task_results[i].task_index].path} " \
                          f"({len(curr_schema.columns)} columns):\n{curr_schema}"
                return message
        return None

    @staticmethod
    def _check_uniques(task_results: List[RegistrationTaskResult]) -> Optional[ErrorMessage]:
        # This part can take a bit of memory,
        # so at least we don't use list comprehension to read all blobs before processing any.
        id_arrays = []
        for res in task_results:
            blob = get_blobstore().read_blob(res.group_ids_blob_id)
            if not blob:
                raise Exception(f"Blob not found, ID: {res.group_ids_blob_id}")
            id_array = bytes_to_ndarray(blob)
            id_arrays.append(id_array)
            get_blobstore().delete_blob(res.group_ids_blob_id)

        all_ids = np.concatenate(id_arrays)
        all_unique = np.unique(all_ids)
        if len(all_ids) == len(all_unique):
            return None
        else:
            message = f"Group IDs are not unique across the {len(id_arrays)} sampled files! " \
                      f"Count of unique IDs merged from all sampled files ({len(all_unique)}) " \
                      f"should equal the simple sum of the unique IDs counts from each file ({len(all_ids)})"
            return message

    def build_result(self, base_attributes, final_status, latest_task_results):
        return RegistrationJobResult(
            **base_attributes,
            dataset=self._dataset if final_status.success else None)

    @property
    def sampled_parts(self) -> Optional[List[DatasetPartId]]:
        return self._sampled_parts

    @property
    def dataset(self) -> Optional[DatasetInfo]:
        return self._dataset

    @property
    def parts(self) -> Optional[DatasetPartsInfo]:
        return self._parts_info
