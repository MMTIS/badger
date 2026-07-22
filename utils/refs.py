from operator import attrgetter
from itertools import groupby
from typing import Optional, TypeVar, Any, Iterable

# TODO: This is required for globals to work, lets fix that later.
from domain.netex.model import *  # noqa: F403

import datetime
import re

T = TypeVar("T")
Tid = TypeVar("Tid", bound=EntityStructure)
Tidversion = TypeVar("Tidversion", bound=EntityInVersionStructure)
Tref = TypeVar("Tref", bound=VersionOfObjectRefStructure)



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



def getIdByRef(obj: object, codespace: Codespace, ref: str) -> str:
    name = getattr(getattr(type(obj), "Meta", None), "name", type(obj).__name__)
    return "{}:{}:{}".format(codespace.xmlns, name, str(ref).replace(":", "-"))





def getIndexByGroup(objects: Iterable[T], attr: str) -> dict[object, list[T]]:
    f = attrgetter(attr)  # TODO: change with our own attrgetter that understands lists
    return {i: list(j) for i, j in groupby(objects, lambda x: f(x))}


def setIdVersion(obj: Tidversion, codespace: Codespace, id: str, version: Optional[Version]) -> None:
    name = getattr(getattr(type(obj), "Meta", None), "name", type(obj).__name__)
    obj.id = "{}:{}:{}".format(codespace.xmlns, name, str(id).replace(":", "-"))
    if version:
        obj.version = version.version
    else:
        obj.version = "any"



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



