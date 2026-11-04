import inspect
from typing import Any
import dataclasses

import domain.netex.model as netex
from domain.utils import get_not_netex


def get_boring_classes() -> set[Any]:
    # Get all classes from the generated NeTEx Python Dataclasses
    clsmembers = inspect.getmembers(netex, inspect.isclass)

    # The interesting class members certainly will have a "Meta class" with a namespace
    # interesting_members = [x[1] for x in clsmembers if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace")] + [netex.VersionFrameDefaultsStructure]

    interesting_members = [x[1] for x in clsmembers if dataclasses.is_dataclass(x[1]) and not get_not_netex(x[1])]

    return set(interesting_members)
