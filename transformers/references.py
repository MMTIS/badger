from typing import TypeVar

from netex import VersionOfObjectRefStructure
from netexio.attributes import update_attr, resolve_attr
from utils.refs import getRef

T = TypeVar("T")
Tref = TypeVar("Tref", bound=VersionOfObjectRefStructure)


def split_path(path: str) -> list[str | int]:
    split: list[str | int] = []
    for p in path.split("."):
        if p.isnumeric():
            split.append(int(p))
        else:
            split.append(p)
    return split


def replace_with_reference_inplace(
    obj: T, path: str, klass: type[Tref] | None = None
) -> None:
    split = split_path(path)

    attribute = resolve_attr(obj, split)

    # This does the assumption that the caller knows references would be allowed as type
    update_attr(obj, split, getRef(attribute, klass=klass))
