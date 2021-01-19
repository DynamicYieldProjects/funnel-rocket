from typing import Dict, Type, Callable, List, cast
from frocket.common.validation.relation_parser import RBaseElement, RTextElement, RConditionBaseElement, ROperator


def relation_to_pandas_query(elements: List[RBaseElement], column_prefix: str) -> str:
    etype_to_handler: Dict[Type[RBaseElement], Callable[[RBaseElement], str]] = {
        RTextElement: lambda v: v.text,
        RConditionBaseElement: lambda v: f"{column_prefix}{v.condition_id}",
        ROperator: lambda v: " & " if v.text in ["and", "&&"] else " | "
    }

    transformed = []
    for e in elements:
        func = None
        class_and_supers = cast(List[Type[RBaseElement]], type(e).mro())
        for cls in class_and_supers:
            func = etype_to_handler.get(cls, None)
            if func:
                break
        if not func:
            raise Exception(f"{e} has no handler for any of its superclasses: {class_and_supers}")
        transformed.append(func(e))
    return "".join(transformed)
