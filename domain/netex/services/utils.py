import inspect
from typing import Any
import dataclasses

import domain.netex.model as netex
from domain.utils import get_not_netex


def get_boring_classes() -> set[type]:
    # Get all classes from the generated NeTEx Python Dataclasses
    clsmembers = inspect.getmembers(netex, inspect.isclass)

    netex_classes = {
        clazz
        for _, clazz in clsmembers
        if dataclasses.is_dataclass(clazz) and not get_not_netex(clazz)
    }

    result = set(netex_classes)

    def collect_subclasses(clazz: type) -> None:
        for member in vars(clazz).values():
            if (
                inspect.isclass(member)
                and dataclasses.is_dataclass(member)
                # and issubclass(member, tuple(netex_classes))
                and member not in result
            ):
                result.add(member)
                collect_subclasses(member)

    for clazz in netex_classes:
        collect_subclasses(clazz)

    return result


from typing import get_type_hints, get_args, get_origin, Union
import types


def get_enum(clazz: type, attribute: str):
    hints = get_type_hints(clazz)

    field_type = hints[attribute]

    # Unwrap Optional / Union
    if get_origin(field_type) in (Union, types.UnionType):
        field_type = next(
            arg for arg in get_args(field_type)
            if arg is not type(None)
        )

    return field_type