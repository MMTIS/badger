# Alternative implementation for attrgetter, handles list indices
# from operator import attrgetter
from typing import Any


def resolve_attr(obj: Any, attr: list[str | int]) -> Any:
    for name in attr:
        if isinstance(name, int):
            obj = obj[name]
        else:
            obj = getattr(obj, name)
    return obj


def update_attr(obj: Any, attr: list[str | int], value: Any) -> Any:
    for name in attr[0:-1]:
        if isinstance(name, int):
            obj = obj[name]
        else:
            obj = getattr(obj, name)

    name = attr[-1]
    if isinstance(name, int):
        obj[name] = value
    else:
        setattr(obj, name, value)

    return obj
