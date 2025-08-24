import inspect

import domain.netex.model as netex
from typing import Any

netex.set_all = frozenset(netex.__all__)  # type: ignore # This is the true performance step


def _all_subclasses(cls: type[Any]) -> set[type[Any]]:
    seen = set()
    stack = [cls]
    while stack:
        c = stack.pop()
        for s in c.__subclasses__():
            if s not in seen:
                seen.add(s)
                stack.append(s)
    return seen


def get_boring_classes() -> list[Any]:
    # Get all classes from the generated NeTEx Python Dataclasses
    clsmembers = inspect.getmembers(netex, inspect.isclass)

    # The interesting class members certainly will have a "Meta class" with a namespace
    interesting_members = [x[1] for x in clsmembers if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace")] + [netex.VersionFrameDefaultsStructure]

    return interesting_members


netex.set_ref_types = frozenset(  # type: ignore
    {netex.VersionOfObjectRef, netex.VersionOfObjectRefStructure}
    | _all_subclasses(netex.VersionOfObjectRef)
    | _all_subclasses(netex.VersionOfObjectRefStructure)
)

netex.interesting_members = get_boring_classes()  # type: ignore
