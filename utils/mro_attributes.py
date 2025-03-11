import dataclasses
import decimal
from dataclasses import Field

from xsdata.models.datatype import XmlDateTime, XmlDuration, XmlTime, XmlDate, XmlPeriod

import netex
import inspect
import typing


def hasdefault(default):
    if isinstance(default, dataclasses._MISSING_TYPE):
        return None

    return default


def unembed(fields):
    all_references = []
    all_classes = []

    for name, field in fields.items():
        resolved_class = resolve_class(field.type)
        if not resolved_class:
            continue

        if hasattr(resolved_class, "__forward_arg__"):
            continue

        resolved_class_name = resolved_class.__mro__[0].__name__

        if resolved_class_name.endswith('Ref'):
            all_references.append(resolved_class_name[:-3])

        all_classes.append((name, field, resolved_class_name))

    result = []
    for name, field, resolved_class_name in all_classes:
        if resolved_class_name not in all_references:
            result.append((name, field))

    return result


def resolve_class(clazz):
    clazz_resolved = clazz

    if hasattr(clazz, '_name'):
        if clazz._name == 'Optional':
            optional = True
            clazz_resolved = [x for x in clazz.__args__ if x is not None.__class__][0]
        elif clazz._name == 'List':
            return None  # TODO: handle list elements
        else:
            clazz_resolved = [x for x in clazz.__args__ if x is not None.__class__][0]

    return clazz_resolved


IGNORE_ATTRIBUTES = ['name_of_class_attribute']


def list_attributes(clazz, parent_name=None):
    buffer = []
    if hasattr(clazz, '__dataclass_fields__'):
        for name, field in unembed(clazz.__dataclass_fields__):
            if name in IGNORE_ATTRIBUTES:
                continue

            if parent_name:
                full_name = parent_name + '.' + name
                buffer.append((full_name, get_type(field.type, full_name), hasdefault(field.default), field))
            else:
                buffer.append((name, get_type(field.type, name), hasdefault(field.default), field))
                # return (name, get_type(field.type, name))

    return buffer


def likely_type(obj: Field):
    if hasattr(obj.type, '__args__'):
        if hasattr(obj.type.__args__[0], '__name__'):
            return obj.type.__args__[0].__name__

        elif obj.type.__args__[0].__class__ == typing.ForwardRef:
            return obj.type.__args__[0].__forward_arg__

        else:
            return obj.type.__class__.__name__
    elif obj.type.__class__.__name__ == 'type':
        return obj.type.__name__
    else:
        return obj.type.__class__.__name__


def get_type(clazz, parent_name):
    optional = False
    clazz_resolved = clazz

    if clazz == typing.List[object]:
        # We don't handle these yet
        return None

    if hasattr(clazz, '_name'):
        if clazz._name == 'Optional':
            optional = True
            clazz_resolved = [x for x in clazz.__args__ if x is not None.__class__][0]
        elif clazz._name == 'List':
            return None  # TODO: handle list elements
        else:
            clazz_resolved = [x for x in clazz.__args__ if x is not None.__class__][0]

    if clazz_resolved == object:
        # TODO: We don't handle these yet
        return None

    if hasattr(clazz_resolved, "_name") and clazz_resolved._name == 'List':
        # TODO: We don't handle these yet
        return None

    if hasattr(clazz_resolved, "__forward_arg__"):
        # TODO: We don't handle these yet
        return None

    if not hasattr(clazz_resolved, "__mro__"):
        print(parent_name, clazz_resolved)
        print()

    if len([x for x in clazz_resolved.__mro__ if x.__name__.endswith('RelStructure')]) > 0:
        return None

    if len([x for x in clazz_resolved.__mro__ if x.__name__ == 'Enum']) > 0:
        return str, optional  # (get_enum(clazz_resolved), optional)

    # This is a hack because upstream did not use VersionOfObjectRef as substitution group consistently
    if len([x for x in clazz_resolved.__mro__ if x.__name__.endswith('RefStructure')]) > 0:
        # Inline RefClasses
        # return (list_attributes(clazz_resolved, parent_name), optional)
        return netex.VersionOfObjectRef, optional

    if clazz_resolved not in (
            int, str, bool, float, bytes, XmlPeriod, XmlTime, XmlDate, XmlDateTime, XmlDuration, decimal.Decimal,
            netex.MultilingualString):
        listed_attributes = list_attributes(clazz_resolved, parent_name)
        if listed_attributes:
            return listed_attributes, optional
        return None

    return clazz_resolved, optional


def get_enum(clazz):
    return [x[1].value for x in inspect.getmembers(clazz) if isinstance(x[1], clazz)]
