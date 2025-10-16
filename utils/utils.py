import inspect

import warnings
import re
from typing import TypeVar, Iterable, Any
from xsdata.models.datatype import XmlDuration, XmlTime

from domain.netex.model import (
    VersionOfObjectRefStructure,
    EntityStructure,
    VersionFrameDefaultsStructure,
)

T = TypeVar("T")
Tid = TypeVar("Tid", bound=EntityStructure)
Tref = TypeVar("Tref", bound=VersionOfObjectRefStructure)


def get_object_name(clazz: type[T]) -> str:
    return getattr(getattr(clazz, "Meta", None), "name", str(clazz.__name__))




def project(obj: Tid, clazz: type[Tid], **kwargs: Any) -> Tid:
    assert clazz.__dataclass_fields__ is not None, "Class must have __dataclass_fields__"

    # if issubclass(obj.__class__, clazz_intermediate):
    attributes: dict[str, Any] = {
        x: getattr(obj, x, None)
        for x in clazz.__dataclass_fields__.keys()
        if (hasattr(clazz.__dataclass_fields__[x], "init") and clazz.__dataclass_fields__[x].init is not False)
    }
    if "id" in attributes:
        attributes["id"] = attributes["id"].replace(f":{get_object_name(obj.__class__)}:", f":{get_object_name(clazz)}:")

    return clazz(**{**attributes, **kwargs})


def projectRef(obj: T, clazz: type[Tref]) -> Tref:
    assert clazz.__dataclass_fields__ is not None, "Class must have __dataclass_fields__"

    attributes = {
        x: getattr(obj, x, None) for x, field in clazz.__dataclass_fields__.items() if getattr(field, "init", False)  # Ensure field is included in __init__
    }

    if "name_of_ref_class" not in attributes or attributes["name_of_ref_class"] is None:
        attributes["name_of_ref_class"] = re.sub(r"Ref(Structure)?", "", obj.__class__.__name__)

    # Ensure types match expected argument types of clazz
    return clazz(**{k: v for k, v in attributes.items() if v is not None})


def to_seconds(xml_duration: XmlDuration) -> int:
    if xml_duration.months is not None and xml_duration.months > 0:
        warnings.warn("Duration is bigger than a month!")
    return int((((xml_duration.days or 0) * 24 + (xml_duration.hours or 0)) * 3600) + ((xml_duration.minutes or 0) * 60) + (xml_duration.seconds or 0))


def to_seconds_xmltime(xml_time: XmlTime, offset: int = None) -> int:
    return int((((offset or 0) * 24 + (xml_time.hour)) * 3600) + ((xml_time.minute or 0) * 60) + (xml_time.second or 0))


def dontsetifnone(clazz: type[T], attr: str, value: Any) -> T | None:
    if value is None:
        return None

    try:
        first = value.__next__()
    except StopIteration:
        return None
    else:
        return clazz(**{attr: chain([first], value)})


def chain(*iterables: Any) -> Iterable[Any]:
    for it in iterables:
        for element in it:
            yield element


class GeneratorTester:
    def __init__(self, value: Any):
        self._has_value: bool | None = None
        self.value: Any = value

    def has_value(self) -> bool:
        if self._has_value is not None:
            return self._has_value

        try:
            self.first = self.value.__next__()
            self._has_value = True
        except StopIteration:
            self._has_value = False
            pass

        return self._has_value

    def generator(self) -> Iterable[Any]:
        if self._has_value is None:
            yield self.value

        elif self._has_value:
            yield from chain([self.first], self.value)


def get_boring_classes() -> list[Any]:
    # Get all classes from the generated NeTEx Python Dataclasses
    clsmembers = inspect.getmembers(domain.netex.schema, inspect.isclass)

    # The interesting class members certainly will have a "Meta class" with a namespace
    interesting_members = [x[1] for x in clsmembers if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace")] + [VersionFrameDefaultsStructure]

    return interesting_members


