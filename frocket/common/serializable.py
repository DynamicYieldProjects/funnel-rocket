import dataclasses
from collections import Counter
from dataclasses import dataclass, fields, field, Field
from enum import Enum
from typing import List, Type, Dict, Any, Union, Optional
from datetime import datetime
import dataclasses_json
import marshmallow
from dataclasses_json import dataclass_json, LetterCase

# TODO Organize & doc

# As suggested in dataclasses_json's docs, store datetime in ISO format to ensure timezone consistency (UTC)
dataclasses_json.global_config.encoders[datetime] = datetime.isoformat
dataclasses_json.global_config.decoders[datetime] = datetime.fromisoformat
dataclasses_json.global_config.mm_fields[datetime] = marshmallow.fields.DateTime(format='iso')


# For some reason, dataclasses-json package doesn't strictly validate primitive types by default,
# so here we are. Note below - a float value can also accept an int.
def ensure_vtype(v, cls: Type, fallback_cls: Type = None):
    if not isinstance(v, cls) and \
            (not fallback_cls or not isinstance(v, fallback_cls)):
        raise ValueError(f"{v} is not a {cls.__name__}")
    return v


dataclasses_json.global_config.decoders[str] = lambda v: ensure_vtype(v, str)
dataclasses_json.global_config.decoders[int] = lambda v: ensure_vtype(v, int)
dataclasses_json.global_config.decoders[float] = lambda v: ensure_vtype(v, float, fallback_cls=int)
dataclasses_json.global_config.decoders[bool] = lambda v: ensure_vtype(v, bool)

API_PUBLIC_FIELD = 'api_public'
API_PUBLIC_METADATA = {API_PUBLIC_FIELD: True}


def api_public_field(**kwargs) -> Field:
    assert 'metadata' not in kwargs  # Not supporting merging metadata for now
    kwargs['metadata'] = API_PUBLIC_METADATA
    return field(**kwargs)


def reducable(cls):
    if not issubclass(cls, SerializableDataClass):
        raise Exception(f"{cls} is not serializable")
    cls.__reducable = True
    return cls


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class SerializableDataClass(dataclasses_json.DataClassJsonMixin):
    @classmethod
    def api_public_fields(cls) -> List[str]:
        result = getattr(cls, '_api_public_fields', None)
        if not result:
            result = [f.name for f in fields(cls) if f.metadata.get(API_PUBLIC_FIELD, False)]
            cls._api_public_fields = result
        return result

    def to_api_response_dict(self, public_fields_only: bool) -> dict:
        # remove none values and empty dicts, stringify enums (so a later json.dumps doesn't fail)
        def prepare_dict(src: dict, allowed_fields: List[str] = None) -> dict:
            res = {}
            for k, v in src.items():
                if v is None:
                    continue
                # TODO support removing non-public fields (including non top-level fields?),
                #  note that k is now already camelCased
                # elif allowed_fields and (k not in allowed_fields):
                #    continue
                if isinstance(v, dict):
                    if len(v) == 0:
                        continue
                    res[k] = prepare_dict(v, allowed_fields=None)
                elif isinstance(v, Enum):
                    res[k] = str(v.value)
                else:
                    res[k] = v
            return res

        # allowed_fields = self.api_public_fields() if public_fields_only else None
        d = prepare_dict(self.to_dict())
        return d

    def shallowdict(self, include_none: bool = True):
        # dataclasses.asdict() would recurse into sub-dataclasses and make them dicts, this will keep them classes
        return {f.name: self.__getattribute__(f.name) for f in fields(self)
                if (include_none or self.__getattribute__(f.name) is not None)}

    @classmethod
    def reduce(cls, serializables: list) -> object:
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
        assert all(isinstance(d, dict) for d in dicts)
        reduced = Counter()
        for d in dicts:
            reduced.update(d)
        if top_count:
            reduced = dict(reduced.most_common(top_count))  # Sorts in any case
        return reduced


# Adapted from the Python enum documentation: set the enum value to the member name when using auto().
# This makes for human-friendly JSON serialization (though de-serialization still requires a library).
class AutoNamedEnum(Enum):
    # noinspection PyMethodParameters
    def _generate_next_value_(name, start, count, last_values):
        return name


# TODO doc / decide
class EnumSerializableByName(Enum):
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


#
# Envelope - enables writing various object types, and serializing automatically back to the right type
#

enveloped_classes = []


# TODO use metaclass instead? support inheritance automatically
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
