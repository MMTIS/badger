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
