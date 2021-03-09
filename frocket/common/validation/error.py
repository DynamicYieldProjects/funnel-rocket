from enum import auto
from frocket.common.serializable import AutoNamedEnum


class ValidationErrorKind(AutoNamedEnum):
    """Distinguish between types of validation issues in query"""
    INVALID_ARGUMENTS = auto()  # Validator given wrong arguments
    SCHEMA = auto()  # Failure at JSON Schema level
    TYPE_MISMATCH = auto()  # Operator or value type don't match each other, or the context
    DATASET_MISMATCH = auto()  # Column names, types, etc. do not match the schema of the given dataset
    RELATION = auto()  # query.relation expression found invalid by relation_parser.py
    # Note for unexpected errors: unlike other kinds, the message associated with this kind may leak sensitive data
    # if it was returned to the caller - so it is not returned by the API server in PUBLIC mode.
    UNEXPECTED = auto()


class QueryValidationError(Exception):
    def __init__(self, message: str, kind: ValidationErrorKind = None):
        self.message = message
        self.kind = kind or ValidationErrorKind.UNEXPECTED  # Default, but should be rare.

    @staticmethod
    def wrap(e: Exception, kind: ValidationErrorKind = None):
        return QueryValidationError(str(e), kind)

    def __str__(self):
        return f"ValidationError({self.kind.value}: {self.message})"
