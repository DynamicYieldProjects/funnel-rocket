"""
Base classes for object serialization to JSON/de-serialization from JSON (which is needed both for communication
between invoker and workers, and for request/response handling in the API server), and a few data-related helpers.

I've chosen the package dataclasses-json as a basis, as it's playing nice with Python dataclasses and type hints,
instead of requiring special model classes. On top of that, a few extra capabilities and safeguards are introduced.
"""
import dataclasses
from collections import Counter
from dataclasses import dataclass, fields
from enum import Enum
from typing import List, Type, Dict, Any, Union, Optional
from datetime import datetime
import dataclasses_json
import marshmallow
from dataclasses_json import dataclass_json, LetterCase

# As suggested in dataclasses_json's docs, store datetime objects in ISO format to ensure timezone consistency (UTC)
dataclasses_json.global_config.encoders[datetime] = datetime.isoformat
dataclasses_json.global_config.decoders[datetime] = datetime.fromisoformat
dataclasses_json.global_config.mm_fields[datetime] = marshmallow.fields.DateTime(format='iso')


# For some reason, dataclasses-json package doesn't strictly validate the type of primitive fields,
# so here we are. The 'fallback_cls' below allows e.g. accepting an int where a float is expected.
def ensure_vtype(v, cls: Type, fallback_cls: Type = None):
    if not isinstance(v, cls) and \
            (not fallback_cls or not isinstance(v, fallback_cls)):
        raise ValueError(f"{v} is not a {cls.__name__}")
    return v


dataclasses_json.global_config.decoders[str] = lambda v: ensure_vtype(v, str)
dataclasses_json.global_config.decoders[int] = lambda v: ensure_vtype(v, int)
dataclasses_json.global_config.decoders[float] = lambda v: ensure_vtype(v, float, fallback_cls=int)
dataclasses_json.global_config.decoders[bool] = lambda v: ensure_vtype(v, bool)


def reducable(cls):
    """
    Decorator which marks a serializable class as 'reduceable' -  meaning a list of such objects can be aggregated
    to one by the reduce() class method.

    Reduceable classes must implement the *class method* _reduce_fields(), aceepting a list of similar-typed objects -
    see below. That method returns a dict with only the fields that were aggregated and their value, without any fields
    whose value should be common to all objects.
    """
    if not issubclass(cls, SerializableDataClass):
        raise Exception(f"{cls} is not serializable")
    cls.__reducable = True
    return cls


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class SerializableDataClass(dataclasses_json.DataClassJsonMixin):
    """
    Base class for all serializable classes. Since it's a dataclass, then construction, equality and hashing come out-
    of-the-box. Always immutable (frozen=True) to prevent coding errors and allow Python to safely generate __hash__().
    """

    def to_api_response_dict(self) -> dict:
        """
        Before the API server jsonifies are response, do some cleanups: remove None values and empty dicts,
        stringify enums (so a later json.dumps doesn't fail).
        TODO backlog support public/non-public mode? (including in nested objects)
        """
        def prepare_dict(src: dict, allowed_fields: List[str] = None) -> dict:
            res = {}
            for k, v in src.items():
                if v is None:
                    continue
                if isinstance(v, dict):
                    if len(v) == 0:
                        continue
                    res[k] = prepare_dict(v, allowed_fields=None)
                elif isinstance(v, Enum):
                    res[k] = str(v.value)
                else:
                    res[k] = v
            return res

        d = prepare_dict(self.to_dict())
        return d

    def shallowdict(self, include_none: bool = True):
        """dataclasses.asdict() would recurse into sub-dataclasses and make them dicts, this will keep them classes."""
        return {f.name: self.__getattribute__(f.name) for f in fields(self)
                if (include_none or self.__getattribute__(f.name) is not None)}

    @classmethod
    def reduce(cls, serializables: list) -> object:
        """
        Reduce a list of this class' type to one. The class must implement _reduce_fields() to perform the actual
        aggregation of relevant field, assisted by helper methods below. Fields expected to have a common non-reduced
        value are filled in by this method, and validated to be the same for all objects).
        """
        if not cls.is_reducable():
            raise Exception(f"{cls} is not marked as @reducable (and please implement _reduce_fields() for this class)")

        # Either all objects are None (and the result is thus None), or all must be of this class type
        if all([e is None for e in serializables]):
            return None
        else:
            assert all([type(e) is cls for e in serializables])

        base_fields = dataclasses.asdict(serializables[0])
        reduced_fields = cls._reduce_fields(serializables)  # Call the concrete class to handle all reducable fields
        # Ensure the non-reduced fields indeed have the same value for all objects in list
        common_fields = {k: v for k, v in base_fields.items() if k not in reduced_fields}
        for k, v in common_fields.items():
            assert all(getattr(e, k) == v for e in serializables[1:])

        all_fields = {**common_fields, **reduced_fields}
        # noinspection PyArgumentList
        return cls(**all_fields)

    @classmethod
    def is_reducable(cls) -> bool:
        return getattr(cls, '__reducable', False) is True

    @classmethod
    def _reduce_fields(cls, serializables: list) -> Dict[str, Any]:
        raise Exception(f"{cls.__name__} does not have its own _reduce_fields() implementation")

    @staticmethod
    def reduce_lists(lists: Union[List[list], List[None]]) -> Optional[list]:
        """A helper for subclasses implementing @reducable, for reducing a list field of other reducables."""
        if all(lst is None for lst in lists):
            return None
        else:
            list_size = len(lists[0])
            assert all([len(lst) == list_size for lst in lists])
            if list_size == 0:
                return []
            elemtype = type(lists[0][0])
            assert elemtype.is_reducable()
            assert all([type(lst[0]) == elemtype for lst in lists])

            reduced = [elemtype.reduce([e[i] for e in lists])
                       for i in range(list_size)]
            return reduced

    @staticmethod
    def reduce_counter_dicts(dicts: List[dict], top_count: int = None) -> dict:
        """A helper for reducing a list of dict-type fields, which are used as counters - and trimming the resutl len"""
        assert all(isinstance(d, dict) for d in dicts)
        reduced = Counter()
        for d in dicts:
            reduced.update(d)
        if top_count:
            reduced = dict(reduced.most_common(top_count))  # Sorts in any case
        return reduced


# TODO backlog decide on which of the two enum variants to settle - we want both serialization by name (so setting value
#  seems like the most robust way) and allowing extra attributes to be set per member. Python enums are tricksy!!!

class AutoNamedEnum(Enum):
    """
    Adapted from the Python enum documentation: when using auto(), set the enum value to the member name instead of
    the default auto-incrementing ordinal, to make its serialized form more human-friendly and play well with addition
    /re-ordering of enum members (but not renaming). However, since auto() is used, setting per-member attributes
    doesn't work.
    """

    # noinspection PyMethodParameters
    def _generate_next_value_(name, start, count, last_values):
        return name


class EnumSerializableByName(Enum):
    """Another go at it: this one doesn't set the member value to its name (__new__ doesn't know that name), and
    rather relies on setting a custom encoder/decoder below. Since it doesn't need auto(), other attributes can be set.
    This is all pretty frustrating."""
    def __new__(cls, *args):
        obj = object.__new__(cls)
        obj._value_ = len(cls.__members__) + 1
        return obj

    # noinspection PyUnusedLocal
    def __init__(self, *args):
        cls = self.__class__
        if cls not in dataclasses_json.global_config.encoders:
            dataclasses_json.global_config.encoders[cls] = lambda e: e.name
            dataclasses_json.global_config.decoders[cls] = lambda n: self.__class__[n]


"""
Envelope - holds a SerializableDataClass and its type for later de-serializing the correct concrete type, 
similar to https://awslabs.github.io/aws-lambda-powertools-python/utilities/parser/#envelopes

Currently, only classes decorated with @enveloped (see below) are allowed, to prevent misuse.
"""

enveloped_classes = []


def enveloped(cls):
    if not issubclass(cls, SerializableDataClass):
        raise Exception(f"{cls} is not serializable")
    enveloped_classes.append(cls)
    return cls


@dataclass(frozen=True)
class Envelope(SerializableDataClass):
    content_cls: str
    content_dict: dict

    @classmethod
    def seal(cls, obj: SerializableDataClass) -> SerializableDataClass:
        if type(obj) not in enveloped_classes:
            raise Exception(f"Type {type(obj)} is not marked as @enveloped")
        return cls(content_cls=type(obj).__name__, content_dict=obj.to_dict())

    @classmethod
    def seal_to_json(cls, obj: SerializableDataClass) -> str:
        return cls.seal(obj).to_json()

    def open(self, expected_superclass: Type[SerializableDataClass] = None) -> SerializableDataClass:
        content_class = next((cls for cls in enveloped_classes if cls.__name__ == self.content_cls), None)
        if not content_class:
            raise Exception(f"Type {self.content_cls} is not marked as @enveloped")
        elif expected_superclass and not issubclass(content_class, expected_superclass):
            raise Exception(f"Type {content_class} is not a subclass of {expected_superclass}")

        return content_class.from_dict(self.content_dict)

    @classmethod
    def open_from_json(cls, json_string: str,
                       expected_superclass: Type[SerializableDataClass] = None) -> SerializableDataClass:
        envelope = Envelope.from_json(json_string)
        return envelope.open(expected_superclass)
