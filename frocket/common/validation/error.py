from enum import auto
from frocket.common.serializable import AutoNamedEnum


class ValidationErrorKind(AutoNamedEnum):
    INVALID_ARGUMENTS = auto()
    SCHEMA = auto()
    TYPE_MISMATCH = auto()
    DATASET_MISMATCH = auto()
    RELATION = auto()
    UNEXPECTED = auto()


class QueryValidationError(Exception):
    def __init__(self, message: str, kind: ValidationErrorKind = None):
        self.message = message
        self.kind = kind or ValidationErrorKind.UNEXPECTED

    @staticmethod
    def wrap(e: Exception, kind: ValidationErrorKind = None):
        return QueryValidationError(str(e), kind)

    def __str__(self):
        return f"ValidationError({self.kind.value}: {self.message})"
