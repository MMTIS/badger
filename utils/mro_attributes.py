import dataclasses
import decimal
import functools
from dataclasses import Field
from enum import Enum
from typing import Any

from xsdata.models.datatype import XmlDateTime, XmlDuration, XmlTime, XmlDate, XmlPeriod

import domain.netex.model as netex
import inspect
import typing

T = typing.TypeVar("T")

# TODO: This class is currently only used by projection, it seems that it is correctly working. It would be good to do some cleanups and tests on it.


def hasdefault(field: dataclasses.Field[typing.Any]) -> typing.Any:
    if isinstance(field.default, dataclasses._MISSING_TYPE):
        return None

    return field.default


@functools.lru_cache(maxsize=None)
def resolved_field_types(clazz: typing.Hashable) -> dict[str, typing.Any]:
    """Field name -> evaluated annotation for a dataclass.

    dataclasses store the literal annotation in Field.type: a typing object on
    older Pythons, a plain string under deferred annotations (Python >= 3.14).
    get_type_hints evaluates both to actual types, walking the MRO.
    """
    try:
        return typing.get_type_hints(clazz)
    except Exception:
        return {field.name: field.type for field in dataclasses.fields(clazz) if not isinstance(field.type, str)}  # type: ignore[arg-type]


def unembed(
    clazz: typing.Any,
) -> typing.Iterable[tuple[str, dataclasses.Field[typing.Any], typing.Any]]:
    hints = resolved_field_types(clazz)
    all_references = []
    all_classes = []

    for name, field in clazz.__dataclass_fields__.items():
        field_type = hints.get(name, field.type)
        if isinstance(field_type, str):
            continue

        resolved_class = resolve_class(field_type)
        if not resolved_class or not isinstance(resolved_class, type):
            continue

        if hasattr(resolved_class, "__forward_arg__"):
            continue

        resolved_class_name = resolved_class.__mro__[0].__name__

        if resolved_class_name.endswith("Ref"):
            all_references.append(resolved_class_name[:-3])

        all_classes.append((name, field, field_type, resolved_class_name))

    for name, field, field_type, resolved_class_name in all_classes:
        if resolved_class_name not in all_references:
            yield name, field, field_type


def resolve_class(clazz: typing.Type[typing.Any]) -> typing.Type[typing.Any] | None:
    # get_origin/get_args understand typing.Optional/Union/List as well as the
    # PEP 604 `None | X` unions the generated model uses.
    origin = typing.get_origin(clazz)
    if origin is None:
        return clazz

    if origin is list:
        return None  # TODO: handle list elements

    args: list[typing.Type[typing.Any]] = [x for x in typing.get_args(clazz) if x is not type(None)]
    if not args:
        return None

    return args[0]


IGNORE_ATTRIBUTES = ["name_of_class_attribute"]


def list_attributes(
    clazz: T, parent_name: str | None = None
) -> typing.Iterable[tuple[str, tuple[type[Any], bool] | None, Any, Field[Any]]]:
    if hasattr(clazz, "__dataclass_fields__"):
        for name, field, field_type in unembed(clazz):
            if name in IGNORE_ATTRIBUTES:
                continue

            if parent_name:
                full_name = parent_name + "." + name
                yield full_name, get_type(field_type, full_name), hasdefault(
                    field
                ), field
            else:
                yield name, get_type(field_type, name), hasdefault(field), field


def likely_type(obj: Field[typing.Any]) -> typing.Any:
    if hasattr(obj.type, "__args__"):
        if hasattr(obj.type.__args__[0], "__name__"):
            return obj.type.__args__[0].__name__

        elif obj.type.__args__[0].__class__ == typing.ForwardRef:
            return obj.type.__args__[0].__forward_arg__

        else:
            return obj.type.__class__.__name__
    elif isinstance(
        obj.type, type
    ):  # TODO: verify is the same obj.type.__class__.__name__ == 'type':
        return obj.type.__name__
    else:
        return obj.type.__class__.__name__


def get_type(
    clazz: type[typing.Any], parent_name: str
) -> tuple[typing.Type[typing.Any], bool] | None:
    optional = False
    clazz_resolved = clazz

    if clazz == typing.List[object]:
        # We don't handle these yet
        return None

    if hasattr(clazz, "_name"):
        if clazz._name == "Optional":
            optional = True
            clazz_resolved = [x for x in clazz.__args__ if x is not None.__class__][0]
        elif clazz._name == "List":
            return None  # TODO: handle list elements
        else:
            clazz_resolved = [x for x in clazz.__args__ if x is not None.__class__][0]

    if isinstance(clazz_resolved, object):  # TODO: check this
        # TODO: We don't handle these yet
        return None

    if hasattr(clazz_resolved, "_name") and clazz_resolved._name == "List":
        # TODO: We don't handle these yet
        return None

    if hasattr(clazz_resolved, "__forward_arg__"):
        # TODO: We don't handle these yet
        return None

    if not hasattr(clazz_resolved, "__mro__"):
        print(parent_name, clazz_resolved)
        print()

    if (
        len([x for x in clazz_resolved.__mro__ if x.__name__.endswith("RelStructure")])
        > 0
    ):
        return None

    if len([x for x in clazz_resolved.__mro__ if x.__name__ == "Enum"]) > 0:
        return str, optional  # (get_enum(clazz_resolved), optional)

    # This is a hack because upstream did not use VersionOfObjectRef as substitution group consistently
    if (
        len([x for x in clazz_resolved.__mro__ if x.__name__.endswith("RefStructure")])
        > 0
    ):
        # Inline RefClasses
        # return (list_attributes(clazz_resolved, parent_name), optional)
        return netex.VersionOfObjectRef, optional

    if clazz_resolved not in (
        int,
        str,
        bool,
        float,
        bytes,
        XmlPeriod,
        XmlTime,
        XmlDate,
        XmlDateTime,
        XmlDuration,
        decimal.Decimal,
        netex.MultilingualString,
    ):
        listed_attributes = list_attributes(clazz_resolved, parent_name)
        if listed_attributes:
            return listed_attributes, optional
        return None

    return clazz_resolved, optional


def get_enum(clazz: type[Enum]) -> list[str]:
    return [x[1].value for x in inspect.getmembers(clazz) if isinstance(x[1], clazz)]
