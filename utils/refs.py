from operator import attrgetter
from itertools import groupby
from typing import Optional, List, TypeVar, Any, cast

from netex import (
    MultilingualString,
    EntityInVersionStructure,
    EntityStructure,
    VersionOfObjectRefStructure,
    Codespace,
    Version,
)

# TODO: This is required for globals to work, lets fix that later.
from netex import *  # noqa: F403

from utils.utils import get_object_name

import datetime
import re

T = TypeVar("T")
Tid = TypeVar("Tid", bound=EntityStructure)
Tidversion = TypeVar("Tidversion", bound=EntityInVersionStructure)
Tref = TypeVar("Tref", bound=VersionOfObjectRefStructure)


def getRef(obj: Tid, klass: type[VersionOfObjectRefStructure] | None = None) -> VersionOfObjectRefStructure:
    assert obj is not None, "A reference must be made from an existing object."

    if klass is None:
        asobj = type(obj).__name__ + "Ref"  # Was: RefStructure
        klass = cast(type[VersionOfObjectRefStructure], globals()[asobj])  # TODO: review

    assert klass is not None, "Class is not none"

    if hasattr(obj, "id"):
        assert obj.id is not None, "Object does not have an id"
        instance = klass(ref=obj.id)
    elif hasattr(obj, "ref"):
        assert obj.ref is not None, "Object does not have a ref"
        instance = klass(ref=obj.ref)
    else:
        assert False, "Object does not have an id or ref"

    if hasattr(instance, "order") and hasattr(obj, "order"):
        instance.order = obj.order

    name = type(obj).__name__
    if hasattr(obj, "Meta") and hasattr(obj.Meta, "name"):
        name = obj.Meta.name
    elif name.endswith("RefStructure"):
        name = name.replace("RefStructure", "Ref")

    instance.version = getattr(obj, "version", None)

    kname = klass.__name__
    meta_kname = klass.__name__
    meta = getattr(klass, "Meta", None)
    if meta and hasattr(meta, "name"):
        meta_kname = meta.name

    if not (kname.startswith(name) or meta_kname.startswith(name)):
        instance.name_of_ref_class = name
    return instance


def getFakeRefByClass(id: str, klass: type[Tref], version: str | None = None) -> Tref:
    asobj = type(klass).__name__ + "Ref"  # Was: RefStructure
    klass = globals()[asobj]
    instance = klass(ref=id)
    if version is not None:
        instance.version = version
    return instance


def getClassFromRefClass(ref: Tref) -> Any:
    if ref.name_of_ref_class is not None:
        klass = ref.name_of_ref_class
    else:
        klass = re.sub(r"LineRef(Structure)?", "Line", type(ref).__name__)  # TODO: review

    return globals()[klass]


def getFakeRef(id: str, klass: type[Tref], version: str | None, version_ref: str | None = None) -> Tref:
    assert id is not None, "A reference must start with a valid id"
    return (
        klass(
            ref=id,
            version=version if version_ref is None else None,
            version_ref=version_ref,
        )
    )


def getIdByRef(obj: object, codespace: Codespace, ref: str) -> str:
    name = getattr(getattr(type(obj), "Meta", None), "name", type(obj).__name__)
    return "{}:{}:{}".format(codespace.xmlns, name, str(ref).replace(":", "-"))


def getIndex(objects: List[Tid], attr: str | None = None) -> dict[object, Tid]:
    if not attr:
        return {x.id: x for x in objects}

    f = attrgetter(attr)  # TODO: change with our own attrgetter that understands lists
    return {f(x): x for x in objects}


def getIndexByGroup(objects: List[T], attr: str) -> dict[object, list[T]]:
    f = attrgetter(attr)  # TODO: change with our own attrgetter that understands lists
    return {i: list(j) for i, j in groupby(objects, lambda x: f(x))}


def setIdVersion(obj: Tidversion, codespace: Codespace, id: str, version: Optional[Version]) -> None:
    name = getattr(getattr(type(obj), "Meta", None), "name", type(obj).__name__)
    obj.id = "{}:{}:{}".format(codespace.xmlns, name, str(id).replace(":", "-"))
    if version:
        obj.version = version.version
    else:
        obj.version = "any"


def getId(clazz: type[Tid], codespace: Codespace, id: str) -> str:
    name = get_object_name(clazz)
    return "{}:{}:{}".format(codespace.xmlns, name, str(id).replace(":", "-"))


def getVersionOfObjectRef(obj: Tid) -> VersionOfObjectRefStructure:
    assert obj.id is not None, "Object without id"
    return VersionOfObjectRefStructure(name_of_ref_class=type(obj).__name__, ref=obj.id)


def getBitString2(
    available: list[datetime.datetime],
    f_orig: datetime.datetime | None = None,
    t_orig: datetime.datetime | None = None,
) -> str:
    dates_sorted: list[datetime.datetime] = sorted(available)
    if f_orig is None:
        f_orig = dates_sorted[0]
    if t_orig is None:
        t_orig = dates_sorted[-1]

    f = f_orig

    out = ""
    while f <= t_orig:
        out += str(int(f in dates_sorted))
        f += datetime.timedelta(days=1)

    return out


def getOptionalString(name: str | None) -> MultilingualString | None:
    return MultilingualString(value=name) if name else None
