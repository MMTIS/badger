import struct
from typing import Tuple, get_type_hints

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
    ver_bytes = version.encode("utf-8")

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

def get_numeric_path(obj, target, path=None):
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

