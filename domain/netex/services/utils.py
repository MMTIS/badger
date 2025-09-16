import inspect
from typing import Any

import domain.netex.model as netex


def get_boring_classes() -> list[Any]:
    # Get all classes from the generated NeTEx Python Dataclasses
    clsmembers = inspect.getmembers(netex, inspect.isclass)

    # The interesting class members certainly will have a "Meta class" with a namespace
    interesting_members = [x[1] for x in clsmembers if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace")] + [netex.VersionFrameDefaultsStructure]

    return interesting_members
