# Alternative implementation for attrgetter, handles list indices
# from operator import attrgetter
from typing import Any

from transformers.defaults import set_default


def resolve_attr(obj: Any, attr: list[str | int]) -> Any:
    for name in attr:
        if isinstance(name, int):
            obj = obj[name]
        else:
            obj = getattr(obj, name)
    return obj


def update_attr(obj: Any, attr: list[str | int], value: Any | None) -> Any:
    parent = None
    parent_name = None
    for name in attr[0:-1]:
        if isinstance(name, int):
            obj = obj[name]
        else:
            parent = obj
            parent_name = name
            obj = getattr(obj, name)

    name = attr[-1]
    if isinstance(name, int):
        if value is None:
            # For a list, remove the parent
            if parent_name is not None:
                set_default(parent, parent_name)
        else:
            obj[name] = value
    else:
        if value is None:
            set_default(obj, name)
        else:
            setattr(obj, name, value)

    return obj
