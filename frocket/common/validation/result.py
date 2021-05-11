#  Copyright 2021 The Funnel Rocket Maintainers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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
