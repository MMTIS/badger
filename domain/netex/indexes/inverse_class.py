from __future__ import annotations
from dataclasses import fields, is_dataclass
from typing import Any, get_args, get_origin, Type, Dict, Set, Iterable, Union, List
import inspect

from domain.netex.model import GeneralFrameMembersRelStructure

def collect_classes_index(
    classes: Iterable[Type[Any]],
    ignore_classes: set[Type[Any]] | None = None,
    scope_classes: set[Type[Any]] | None = None
) -> Dict[Type[Any], Set[Type[Any]]]:
    """
    Bouw een inverse index: voor ieder type T (binnen scope_classes), in welke dataclasses (ook beperkt tot scope_classes) komt T (direct of indirect) voor als attribuut?
    :param classes: de dataclasses die onderzocht worden
    :param ignore_classes: types die genegeerd worden (komen niet in index voor, en worden niet uitgeplozen)
    :param scope_classes: types die als key en value mogen voorkomen in de index
    :return: dict met key = type, value = set van types
    """
    ignore_classes = ignore_classes or set()
    scope_classes = scope_classes or set(classes)

    index: Dict[Type[Any], Set[Type[Any]]] = {}

    for clazz in classes:
        if not _is_valid_dataclass(clazz, ignore_classes):
            continue

        contained = _collect_contained_types(clazz, ignore_classes)

        # Filter de contained types op scope_classes
        contained_in_scope = {c for c in contained if c in scope_classes}

        for candidate in contained_in_scope:
            if candidate in scope_classes:
                index.setdefault(candidate, set()).add(clazz)

    # Verwijder eventueel None uit values
    # for k, v in index.items():
    #    v.discard(None)

    return index



def _is_valid_dataclass(tp: Any, ignore_classes: Set[Type[Any]]) -> bool:
    """Check of dit een bruikbare dataclass is."""
    return (
        isinstance(tp, type)
        # and tp in classes
        and tp not in ignore_classes
        and is_dataclass(tp)
        and not inspect.isabstract(tp)
    )


def _collect_contained_types(
    clazz: Type[Any], ignore_classes: Set[Type[Any]], seen: Set[Type[Any]] | None = None
) -> Set[Type[Any]]:
    """
    Verzamelt transitief alle dataclass-types die in clazz voorkomen.
    """
    if seen is None:
        seen = set()

    result: Set[Type[Any]] = set()
    for f in fields(clazz):
        candidates = _extract_types(f.type)
        for c in candidates:
            if not _is_valid_dataclass(c, ignore_classes):
                continue
            if c in seen:
                continue
            seen.add(c)
            result.add(c)
            # recursief verdiepen
            result |= _collect_contained_types(c, ignore_classes, seen)
    return result


def _extract_types(tp: Any) -> Set[Type[Any]]:
    """
    Haal alle relevante dataclass types uit een typehint.
    Bijvoorbeeld: list[X] -> {X}, Optional[Y] -> {Y}
    """
    result: Set[Type[Any]] = set()

    origin = get_origin(tp)
    args = get_args(tp)

    if origin is None:
        if isinstance(tp, type):
            result.add(tp)
    else:
        for arg in args:
            result |= _extract_types(arg)

    return result

def extract_concrete_types(tp: Any) -> Set[Type[Any]]:
    """
    Haal alle concrete types uit een typehint.
    Ondersteunt Union, Optional, List enz.
    """
    result: Set[Type[Any]] = set()

    origin = get_origin(tp)
    args = get_args(tp)

    if origin is Union:
        for arg in args:
            result |= extract_concrete_types(arg)
    elif origin in (list, List):
        for arg in args:
            result |= extract_concrete_types(arg)
    else:
        if isinstance(tp, type):
            result.add(tp)

    return result


if __name__ == "__main__":
    choice_field = next(f for f in fields(GeneralFrameMembersRelStructure) if f.name == "choice")
    all_types = { x for x in extract_concrete_types(choice_field.type) if not x.__name__.endswith('Frame') and x.__name__ != 'EntityEntity' }
    index = collect_classes_index(all_types, scope_classes=set(all_types))
    for k, v in index.items():
        print(k.__name__, "→", {c.__name__ for c in v})