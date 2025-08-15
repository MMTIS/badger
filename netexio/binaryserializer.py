from __future__ import annotations
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from netexio.database import Tid
    from netexio.serializer import Serializer

import struct
import netex
from typing import Tuple, get_type_hints, Any, Generator

# struct formaten:
# H = uint16, B = uint8
HEADER_FORMAT = ">H"  # class_id
LENGTH_FORMAT = ">H"  # uint16 length
PATH_LEN_FORMAT = ">B"  # uint8
PATH_INDEX_FORMAT = ">H"  # uint16 per attribuut index


def serialize_relation(
        class_id: int,
        object_id: str,
        version: str,
        path_indices: Tuple[int, ...]
) -> bytes:
    obj_id_bytes = object_id.encode("utf-8")
    ver_bytes = version.encode("utf-8") if version else b''

    parts = [
        struct.pack(HEADER_FORMAT, class_id),

        struct.pack(LENGTH_FORMAT, len(obj_id_bytes)),
        obj_id_bytes,

        struct.pack(LENGTH_FORMAT, len(ver_bytes)),
        ver_bytes,

        struct.pack(PATH_LEN_FORMAT, len(path_indices)),
        b''.join(struct.pack(PATH_INDEX_FORMAT, idx) for idx in path_indices)
    ]
    return b''.join(parts)


def deserialize_relation(data: bytes) -> Tuple[int, str, str, Tuple[int, ...]]:
    offset = 0

    # class_id
    (class_id,) = struct.unpack_from(HEADER_FORMAT, data, offset)
    offset += struct.calcsize(HEADER_FORMAT)

    # object_id
    (obj_len,) = struct.unpack_from(LENGTH_FORMAT, data, offset)
    offset += struct.calcsize(LENGTH_FORMAT)
    object_id = data[offset:offset + obj_len].decode("utf-8")
    offset += obj_len

    # version
    (ver_len,) = struct.unpack_from(LENGTH_FORMAT, data, offset)
    offset += struct.calcsize(LENGTH_FORMAT)
    version = data[offset:offset + ver_len].decode("utf-8")
    offset += ver_len

    # path
    (path_len,) = struct.unpack_from(PATH_LEN_FORMAT, data, offset)
    offset += struct.calcsize(PATH_LEN_FORMAT)
    path_indices = tuple(
        struct.unpack_from(PATH_INDEX_FORMAT, data, offset + i * 2)[0]
        for i in range(path_len)
    )

    return class_id, object_id, version, path_indices


def get_numeric_path(obj: Tid, target: Any, path: list[int] | None = None) -> list[int] | None:
    """Find the numeric path to `target` inside `obj`."""
    if path is None:
        path = []

    # Als target zelf het object is, pad teruggeven
    if obj is target:
        return path

    if hasattr(obj, "__annotations__"):
        # Door attributes lopen in volgorde van __annotations__
        for idx, (attr_name, _) in enumerate(get_type_hints(obj.__class__).items(), start=1):
            val = getattr(obj, attr_name)
            sub_path = get_numeric_path(val, target, path + [idx])
            if sub_path is not None:
                return sub_path

    if isinstance(obj, list):
        for idx, item in enumerate(obj, start=1):
            sub_path = get_numeric_path(item, target, path + [idx])
            if sub_path is not None:
                return sub_path

    return None


def class_to_idx(clazz: Tid) -> int:
    return netex.__all__.index(clazz.__name__)

def idx_to_class_name(idx: int) -> str:
    return netex.__all__[idx]

from functools import lru_cache


@lru_cache(maxsize=None)
def _dc_field_names(cls: type) -> tuple[str, ...]:
    return tuple(cls.__dataclass_fields__.keys())


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
    field_names = _dc_field_names(obj.__class__)
    for col_idx, field_name in enumerate(field_names):
        mydepth[-1] = col_idx
        v = getattr(obj, field_name, None)
        if v is not None:
            if v.__class__ in netex.set_ref_types:  # type: ignore
                yield v, tuple(mydepth)

            else:
                if hasattr(v, "__dataclass_fields__") and v.__class__.__name__ in netex.set_all:  # type: ignore
                    if hasattr(v, "id"):
                        yield v, tuple(mydepth)
                    yield from recursive_attributes(v, mydepth)
                elif v.__class__ in (list, tuple):
                    mydepth.append(0)
                    for j, x in enumerate(v):
                        mydepth[-1] = j
                        if x is not None:
                            if x.__class__ in netex.set_ref_types:  # type: ignore
                                yield x, tuple(mydepth)  # TODO: mydepth result is incorrect when list() but not as iterator
                            elif hasattr(x, "__dataclass_fields__") and x.__class__.__name__ in netex.set_all:  # type: ignore
                                if hasattr(x, "id"):
                                    yield x, tuple(mydepth)
                                yield from recursive_attributes(x, mydepth)
                    mydepth.pop()
    mydepth.pop()

def navigate_object(obj: Any, path_indices: Tuple[int, ...]) -> Any:
    """
    Volg path_indices door obj zonder ooit strings te reconstrueren.
    - Als obj een dataclass/annotated class is, gebruik __annotations__ volgorde.
    - Als obj een list/tuple is, gebruik index direct.
    """

    for idx in path_indices:
        if hasattr(obj, "__dataclass_fields__"):
            field_names = _dc_field_names(obj.__class__)
            attr_name = field_names[idx]
            obj = getattr(obj, attr_name)
        elif isinstance(obj, (list, tuple)):
            # Numeriek direct indexeren
            obj = obj[idx]
        else:
            raise TypeError(f"Cannot navigate into object of type {type(obj)}")
    return obj

def update_embedded_referencing(serializer: Serializer, deserialized: Tid) -> Generator[tuple[bool, type[Tid], str, str, type[Tid], str, str, str], None, None]:
    assert deserialized.id is not None, "deserialised.id must not be none"

    for obj, path in recursive_attributes(deserialized, []):
        if hasattr(obj, "id") and obj.id is not None:
            if obj.__class__ in serializer.interesting_classes:
                assert obj.id is not None, "Object.id must not be none"
                yield (
                    True,
                    deserialized.__class__,
                    deserialized.id,
                    getattr(deserialized, "version", "any"),
                    obj.__class__,
                    obj.id,
                    getattr(obj, "version", "any"),
                    path
                )

        elif hasattr(obj, "ref"):
            assert obj.ref is not None, "Object ref must not be none"
            if obj.name_of_ref_class is None:
                # Hack, because NeTEx does not define the default name of ref class yet
                if obj.__class__.__name__.endswith("RefStructure"):
                    obj.name_of_ref_class = obj.__class__.__name__[0:-12]
                elif obj.__class__.__name__.endswith("Ref"):
                    obj.name_of_ref_class = obj.__class__.__name__[0:-3]

            if obj.name_of_ref_class not in serializer.name_object:
                log_once(logging.WARN, "unknown name_of_ref_class", "Reference Class cannot be found in serializer")
                continue

            yield (
                False,
                deserialized.__class__,
                deserialized.id,
                getattr(deserialized, "version", "any"),
                # The object that contains the reference
                serializer.name_object[obj.name_of_ref_class],  # The object that the reference is towards
                obj.ref,
                getattr(obj, "version", "any"),
                path
            )