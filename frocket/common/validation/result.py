from dataclasses import dataclass
from typing import Optional, List, cast, Dict
from frocket.common.serializable import SerializableDataClass
from frocket.common.validation.error import ValidationErrorKind, QueryValidationError
from frocket.common.validation.relation_parser import RBaseElement


@dataclass(frozen=True)
class QueryValidationResult(SerializableDataClass):
    success: bool
    source_query: dict
    error_message: Optional[str] = None
    error_kind: Optional[ValidationErrorKind] = None
    expanded_query: Optional[dict] = None
    # TODO backlog support non-critical warning/hints to user (e.g. conditions unused by relation expression)
    warnings: Optional[List[str]] = None
    used_columns: Optional[List[str]] = None
    used_conditions: Optional[List[str]] = None
    named_conditions: Optional[Dict[str, int]] = None
    relation_elements: Optional[List[RBaseElement]] = None

    @staticmethod
    def from_exception(e: Exception, source_query: dict):
        if type(e) is QueryValidationError:
            error_kind = cast(QueryValidationError, e).kind
        else:
            error_kind = ValidationErrorKind.UNEXPECTED
        return QueryValidationResult(success=False, error_message=str(e), error_kind=error_kind,
                                     source_query=source_query)
