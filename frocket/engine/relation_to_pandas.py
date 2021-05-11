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

from typing import Dict, Type, Callable, List, cast
from frocket.common.validation.relation_parser import RBaseElement, RTextElement, RConditionBaseElement, ROperator


def relation_to_pandas_query(elements: List[RBaseElement], column_prefix: str) -> str:
    """Convert the generic pasred representation of query.relation expression (as returned by QueryValidator or its
    helper class RelationParser) into a Pandas query string."""

    # Mapping of generic element type to a lambda function constructing the Pandas equivalent. Note below that not
    # every concreate element type needs an entry here, as the code would also look for its superclass
    etype_to_handler: Dict[Type[RBaseElement], Callable[[RBaseElement], str]] = {
        RTextElement: lambda v: v.text,
        RConditionBaseElement: lambda v: f"{column_prefix}{v.condition_id}",
        ROperator: lambda v: " & " if v.text in ["and", "&&"] else " | "
    }

    transformed = []
    for e in elements:
        func = None
        # Either there's a handler above for this element type, or go up the superclass chain to find one.
        class_and_supers = cast(List[Type[RBaseElement]], type(e).mro())
        for cls in class_and_supers:
            func = etype_to_handler.get(cls, None)
            if func:
                break
        if not func:
            raise Exception(f"{e} has no handler for any of its superclasses: {class_and_supers}")
        transformed.append(func(e))
    return "".join(transformed)
