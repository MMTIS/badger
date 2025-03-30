import inspect

import netex
import warnings
import re
from typing import TypeVar, Iterable, Any
from xsdata.models.datatype import XmlDuration
from netex import (
    VersionFrameDefaultsStructure,
    EntityStructure,
    VersionOfObjectRefStructure,
)

T = TypeVar("T")
Tid = TypeVar("Tid", bound=EntityStructure)
Tref = TypeVar("Tref", bound=VersionOfObjectRefStructure)


def get_object_name(clazz: type[T]) -> str:
    return getattr(getattr(clazz, "Meta", None), "name", str(clazz.__name__))


def get_element_name_with_ns(clazz: type[Tid]) -> str:
    name = get_object_name(clazz)
    meta = getattr(clazz, "Meta", None)

    return "{" + (meta.namespace if meta is not None else "") + "}" + name


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
    clsmembers = inspect.getmembers(netex, inspect.isclass)

    # The interesting class members certainly will have a "Meta class" with a namespace
    interesting_members = [x[1] for x in clsmembers if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace")] + [VersionFrameDefaultsStructure]

    return interesting_members


def get_interesting_classes(
    my_filter: set[T] | None = None,
) -> tuple[list[str], list[str], list[Any]]:
    # Get all classes from the generated NeTEx Python Dataclasses
    clsmembers: list[tuple[str, type[Any]]] = inspect.getmembers(netex, inspect.isclass)

    # The interesting class members certainly will have a "Meta class" with a namespace
    interesting_members: list[tuple[str, type[Any]]] = [x for x in clsmembers if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace")]

    # Specifically we are interested in classes that are derived from "EntityInVersion", to find them, we exclude embedded child objects called "VersionedChild"
    entitiesinversion: list[tuple[str, type[Any]]] = [
        x for x in interesting_members if netex.VersionedChildStructure in x[1].__mro__ or netex.EntityInVersionStructure in x[1].__mro__
    ]

    # Obviously we want to have the VersionedChild too
    # versionedchild = [x for x in interesting_members if netex.VersionedChildStructure in x[1].__mro__]

    # There is one particular container in NeTEx that should reflect almost the same our collection EntityInVersion namely the "GeneralFrame"
    # general_frame_members = netex.GeneralFrameMembersRelStructure.__dataclass_fields__['choice'].metadata['choices']

    # The interesting part here is where the difference between the two lie.
    # geme = [x['type'].Meta.getattr('name', x['type'].__name__) for x in general_frame_members]
    # envi = [x[0] for x in entitiesinversion]
    # set(geme) - set(envi)

    if my_filter is not None:
        clean_element_names = [x[0] for x in entitiesinversion if x[1] in my_filter]
        interesting_element_names = [get_element_name_with_ns(x[1]) for x in entitiesinversion if x[1] in my_filter]
        interesting_clazzes = [x[1] for x in entitiesinversion if x[1] in my_filter]
    else:
        clean_element_names = [x[0] for x in entitiesinversion if not x[0].endswith("Frame")]
        interesting_element_names = [get_element_name_with_ns(x[1]) for x in entitiesinversion if not x[0].endswith("Frame")]
        interesting_clazzes = [x[1] for x in entitiesinversion if not x[0].endswith("Frame")]

    return clean_element_names, interesting_element_names, interesting_clazzes
