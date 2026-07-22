from typing import TypeVar

T = TypeVar("T")


def get_object_name(clazz: type[T]) -> str:
    return getattr(getattr(clazz, "Meta", None), "name", str(clazz.__name__))
