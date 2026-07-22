from functools import lru_cache
from typing import Any, Generator, Hashable, Optional

from domain.netex import model as netex
from domain.netex.model import LocationStructure2, SimplePointVersionStructure, LineString, Polygon, MultiSurface, EntityStructure, DataManagedObject
from domain.netex.services.model_typing import Tid, Tref
from domain.netex.services.utils import get_boring_classes
from storage.interface import Serializer

import inspect

from utils.mro_attributes import resolve_class, unembed
from utils import netex_monkeypatching  # noqa: F401

from dataclasses import fields, MISSING


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


netex.set_ref_types = frozenset(  # type: ignore
    {netex.VersionOfObjectRef, netex.VersionOfObjectRefStructure}
    | _all_subclasses(netex.VersionOfObjectRef)
    | _all_subclasses(netex.VersionOfObjectRefStructure)
)

# netex.set_all = frozenset(netex.__all__)  # type: ignore # This is the true performance step

# TODO: dit gaat fout omdat we nu geen netex meer heten, maar domain.netex.model
netex.set_all = frozenset(
    {name: cls for name, cls in inspect.getmembers(netex, inspect.isclass)}
)  # if cls.__module__ == domain.netex.model.__name__})  # type: ignore[attr-defined]

GEO_CLASSES = {LocationStructure2, SimplePointVersionStructure, LineString, Polygon, MultiSurface}


def get_all_geo_elements() -> Generator[Any, None, None]:
    for clazz_parent in get_boring_classes():
        for _name, _field, field_type in unembed(clazz_parent):
            if resolve_class(field_type) in GEO_CLASSES:
                yield clazz_parent
                break


netex.set_geo_types = frozenset(get_all_geo_elements())  # type: ignore[attr-defined]


@lru_cache(maxsize=None)
def _dc_field_names(cls: Hashable) -> tuple[str, ...]:
    return tuple(cls.__dataclass_fields__.keys())  # type: ignore
    # return tuple(f.name for f in fields(cls))


def recursive_attributes(obj: Tid, depth: list[int]) -> Generator[tuple[Any, tuple[int, ...]], None, None]:
    # We skip data_source_ref_attribute and  responsibility_set_ref_attribute later in the pipeline
    # data_source_ref_attribute = getattr(obj, "data_source_ref_attribute", None)
    # if data_source_ref_attribute:
    #     yield DataSourceRefStructure(ref=data_source_ref_attribute), depth + ["data_source_ref_attribute"]

    # responsibility_set_ref_attribute = getattr(obj, "responsibility_set_ref_attribute", None)
    # if responsibility_set_ref_attribute:
    #     yield ResponsibilitySetRef(ref=responsibility_set_ref_attribute), depth + ["responsibility_set_ref_attribute"]

    mydepth = depth
    mydepth.append(0)
    field_names = _dc_field_names(obj.__class__)  # type: ignore
    for col_idx, field_name in enumerate(field_names):
        mydepth[-1] = col_idx
        v = getattr(obj, field_name, None)
        if v is not None:
            if v.__class__ in netex.set_ref_types:  # type: ignore
                yield v, tuple(mydepth)

            elif v.__class__ in GEO_CLASSES:
                yield v, tuple(mydepth)

            else:
                if v.__class__ in (str, int):
                    continue
                if hasattr(
                    v, "__dataclass_fields__"
                ):  # and v.__class__.__name__ in netex.set_all or isinstance(v, StrictContainmentAggregationStructure):  # type: ignore
                    # if hasattr(v, "id"):
                    #    yield v, tuple(mydepth)
                    yield from recursive_attributes(v, mydepth)
                elif v.__class__ in (list, tuple):
                    mydepth.append(0)
                    for j, x in enumerate(v):
                        mydepth[-1] = j
                        if x is not None:
                            if x.__class__ in netex.set_ref_types:  # type: ignore
                                yield x, tuple(mydepth)  # TODO: mydepth result is incorrect when list() but not as iterator
                            elif hasattr(x, "__dataclass_fields__"):  # and x.__class__.__name__ in netex.set_all:  # type: ignore
                                if hasattr(x, "id"):
                                    yield x, tuple(mydepth)
                                yield from recursive_attributes(x, mydepth)
                    mydepth.pop()
    mydepth.pop()


def only_references(deserialized: Tid, serializer: Serializer) -> Generator[tuple[type[EntityStructure], str, str], None, None]:
    assert deserialized.id is not None, "deserialised.id must not be none"
    already_done: set[tuple[type[EntityStructure], str, str | None]] = set()
    # TODO: Hier deduplicatie implementeren, dat zou veel dubbele objecten schelen

    for obj, path in recursive_attributes(deserialized, []):
        if hasattr(obj, "ref"):
            assert obj.ref is not None, "Object ref must not be none"
            # if obj.version_ref is not None and obj.version is None:
            # Don't include external references
            #    continue

            # if obj.name_of_ref_class is None:
            #    # Hack, because NeTEx does not define the default name of ref class yet
            #    if obj.__class__.__name__.endswith("RefStructure"):
            #        obj.name_of_ref_class = obj.__class__.__name__[0:-12]
            #    elif obj.__class__.__name__.endswith("Ref"):
            #        obj.name_of_ref_class = obj.__class__.__name__[0:-3]

            ref_class = None
            if hasattr(obj.name_of_ref_class, 'value'):
                if obj.name_of_ref_class.value not in serializer.name_object.keys():
                    # log_once(logging.WARN, "unknown name_of_ref_class", "Reference Class cannot be found in serializer")
                    # obj.name_of_ref_class = obj.__class__(ref=None).name_of_ref_class
                    # TODO: Maybe precompute this?
                    f = next(f for f in fields(obj.__class__) if f.name == 'name_of_ref_class')
                    if f.default is not MISSING and f.default is not None:
                        obj.name_of_ref_class = f.default
                        ref_class = serializer.name_object[obj.name_of_ref_class.value]
                    else:
                        # TODO: We should handle the case were we really have no clue, no default, not set
                        obj.name_of_ref_class = 'DataManagedObject'
                        ref_class = DataManagedObject
                else:
                    ref_class = serializer.name_object[obj.name_of_ref_class.value]

            else:
                if obj.name_of_ref_class not in serializer.name_object.keys():
                    # log_once(logging.WARN, "unknown name_of_ref_class", "Reference Class cannot be found in serializer")
                    # obj.name_of_ref_class = obj.__class__(ref=None).name_of_ref_class
                    # TODO: Maybe precompute this?
                    f = next(f for f in fields(obj.__class__) if f.name == 'name_of_ref_class')
                    if f.default is not MISSING and f.default is not None:
                        obj.name_of_ref_class = f.default
                        ref_class = serializer.name_object[obj.name_of_ref_class.value]  # because this one has a value

                    else:
                        # TODO: We should handle the case were we really have no clue, no default, not set
                        obj.name_of_ref_class = 'DataManagedObject'
                        ref_class = DataManagedObject

                else:
                    ref_class = serializer.name_object[obj.name_of_ref_class]

            if ref_class:
                result = (
                    ref_class,  # The object that the reference is towards
                    obj.ref,
                    getattr(obj, "version", getattr(obj, "versionRef", "any")),
                )

                if result not in already_done:
                    already_done.add(result)
                    yield result


def only_reference_objects(deserialized: Tid) -> Generator[Tref, None, None]:
    assert deserialized.id is not None, "deserialised.id must not be none"

    for obj, path in recursive_attributes(deserialized, []):
        if hasattr(obj, "ref"):
            assert obj.ref is not None, "Object ref must not be none"
            # if obj.version_ref is not None and obj.version is None:
            # Don't include external references
            # continue

            if obj.name_of_ref_class is None:
                obj.name_of_ref_class = 'DataManagedObject'
                # Hack, because NeTEx does not define the default name of ref class yet
                # if obj.__class__.__name__.endswith("RefStructure"):
                #     obj.name_of_ref_class = obj.__class__.__name__[0:-12]
                # elif obj.__class__.__name__.endswith("Ref"):
                #    obj.name_of_ref_class = obj.__class__.__name__[0:-3]

            yield obj


def embedding_obj_iter(
    serializer: Serializer, deserialized: Tid, interesting_classes: Optional[set[type[Tid]]], ignore: Optional[set[type[Tid]]]
) -> Generator[tuple[Optional[bytes], Tid, list[int]], None, None]:
    assert deserialized.id is not None, "deserialised.id must not be none"

    if not interesting_classes:
        interesting_classes = serializer.class_idx.keys()

    for obj, path in recursive_attributes(deserialized, []):
        if obj.__class__.__name__ in serializer.name_object:  # TODO: The object should not even enter here
            if hasattr(obj, "id") and obj.id is not None:
                if (ignore is None or obj.__class__ not in ignore) and obj.__class__ in interesting_classes:
                    yield serializer.encode_key(obj.id, obj.version if hasattr(obj, "version") else None, obj.__class__), obj, path


def only_embedding(
    serializer: Serializer, deserialized: Tid, interesting_classes: Optional[set[type[Tid]]], ignore: Optional[set[type[Tid]]] = None
) -> Generator[bytes, None, None]:
    assert deserialized.id is not None, "deserialised.id must not be none"

    if not interesting_classes:
        interesting_classes = serializer.class_idx.keys()

    for obj, path in recursive_attributes(deserialized, []):
        if hasattr(obj, "id") and obj.id is not None:
            if (ignore is None or obj.__class__ not in ignore) and obj.__class__ in interesting_classes:
                yield serializer.encode_key(obj.id, obj.version if hasattr(obj, "version") else None, obj.__class__), obj
