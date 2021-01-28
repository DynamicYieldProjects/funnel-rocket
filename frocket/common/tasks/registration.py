from dataclasses import dataclass
from enum import auto
from typing import Optional
from frocket.common.dataset import DatasetInfo, DatasetPartId, DatasetSchema
from frocket.common.serializable import SerializableDataClass, AutoNamedEnum, enveloped
from frocket.common.tasks.base import BaseTaskRequest, BaseTaskResult, BlobId, BaseJobResult, BaseApiResult

#
# Registration task
#


class DatasetValidationMode(AutoNamedEnum):
    SINGLE = auto()
    FIRST_LAST = auto()
    SAMPLE = auto()


REGISTER_DEFAULT_FILENAME_PATTERN = '*.parquet'
REGISTER_DEFAULT_VALIDATION_MODE = DatasetValidationMode.SAMPLE
REGISTER_DEFAULT_VALIDATE_UNIQUES = True


@dataclass(frozen=True)
class RegisterArgs(SerializableDataClass):
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
    return_group_ids: bool


@enveloped
@dataclass(frozen=True)
class RegistrationTaskResult(BaseTaskResult):
    dataset_schema: Optional[DatasetSchema]
    part_id: DatasetPartId
    group_ids_blob_id: Optional[BlobId]


@dataclass(frozen=True)
class RegistrationJobResult(BaseJobResult):
    dataset: DatasetInfo


@dataclass(frozen=True)
class UnregisterApiResult(BaseApiResult):
    dataset_found: bool
    dataset_last_used: Optional[float]
