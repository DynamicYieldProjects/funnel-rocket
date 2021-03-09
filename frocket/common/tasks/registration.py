"""
Task request/response classes for the registration job (discovering, validating and storing metadata for a dataset)
"""
from dataclasses import dataclass
from enum import auto
from typing import Optional
from frocket.common.dataset import DatasetInfo, DatasetPartId, DatasetSchema
from frocket.common.serializable import SerializableDataClass, AutoNamedEnum, enveloped
from frocket.common.tasks.base import BaseTaskRequest, BaseTaskResult, BlobId, BaseJobResult, BaseApiResult


class DatasetValidationMode(AutoNamedEnum):
    SINGLE = auto()  # Only validate a single file in the dataset (meaning no cross-file consistency checks are done!)
    FIRST_LAST = auto()  # Validate only first and last files (by lexicographic sorting) and cross-check them
    SAMPLE = auto()  # Takes a sample of files, proportional to the no.o of files and up to a configured maximum.


REGISTER_DEFAULT_FILENAME_PATTERN = '*.parquet'  # Ignore files such as '_SUCCESS' and the like in discovery
REGISTER_DEFAULT_VALIDATION_MODE = DatasetValidationMode.SAMPLE
REGISTER_DEFAULT_VALIDATE_UNIQUES = True


@dataclass(frozen=True)
class RegisterArgs(SerializableDataClass):
    """Parameters collected by the CLI / API server for the registration job"""
    name: str
    basepath: str
    group_id_column: str
    timestamp_column: str
    pattern: str = REGISTER_DEFAULT_FILENAME_PATTERN
    validation_mode: DatasetValidationMode = REGISTER_DEFAULT_VALIDATION_MODE
    validate_uniques: bool = REGISTER_DEFAULT_VALIDATE_UNIQUES


@enveloped
@dataclass(frozen=True)
class RegistrationTaskRequest(BaseTaskRequest):
    dataset: DatasetInfo
    part_id: DatasetPartId
    # If RegisterArgs.validate_uniques=true, task should return all group IDs in file
    return_group_ids: bool


@enveloped
@dataclass(frozen=True)
class RegistrationTaskResult(BaseTaskResult):
    dataset_schema: Optional[DatasetSchema]  # None on failures
    part_id: DatasetPartId
    # If RegistrationTaskRequest.return_group_ids=true, a reference to the blob with the group IDs
    group_ids_blob_id: Optional[BlobId]


@dataclass(frozen=True)
class RegistrationJobResult(BaseJobResult):
    dataset: DatasetInfo


@dataclass(frozen=True)
class UnregisterApiResult(BaseApiResult):
    dataset_found: bool
    dataset_last_used: Optional[float]
