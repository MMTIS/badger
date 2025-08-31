import string
from typing import Any, cast

import lz4.frame
from cloudpickle import cloudpickle

from domain.utils import get_object_name
from domain.netex.services.model_typing import Tid
from storage.interface import Serializer


class ByteSerializer(Serializer):
    SEPARATOR = ord("-")
    SPECIAL_CHAR = ord("*")
    WORD_MASK = "#"
    class_idx: dict[type, bytes]

    def set_class_idx(self, class_idx: dict[type, bytes]) -> None:
        """ This mapping assures that the stored indices in the database, matches the lookup. """
        self.class_idx = class_idx

    @staticmethod
    def encode_string(value: str, obj_name: str | None) -> bytes:
        """Encodes a string by replacing special characters."""
        value = value.upper()
        return bytes(
            (ord(char) if char in string.ascii_uppercase or char in string.digits or char == ByteSerializer.WORD_MASK else ByteSerializer.SPECIAL_CHAR)
            for char in value
        )

    @staticmethod
    def full_key_to_idx(full_key: bytes) -> tuple[bytes, bytes]:
        full_int = int.from_bytes(full_key, 'little')
        class_idx = (full_int >> 32).to_bytes(2, 'little')
        key = (full_int & 0xFFFFFFFF).to_bytes(4, 'little')
        return class_idx, key

    def encode_key(self, id: str, version: str | None, clazz: type[Tid], include_clazz: bool = False) -> bytes:
        obj_name = get_object_name(clazz).upper()
        encoded_bytes = bytearray()

        encoded_bytes.extend(ByteSerializer.encode_string(id, obj_name))
        encoded_bytes.append(ByteSerializer.SEPARATOR)

        if version is not None and version != "any":
            encoded_bytes.extend(ByteSerializer.encode_string(version, obj_name))
        encoded_bytes.append(ByteSerializer.SEPARATOR)

        if include_clazz:
            encoded_bytes.extend(self.class_idx[clazz])

        return bytes(encoded_bytes)

    def split_key(self, key: bytes) -> list[bytes]:
        return key.split(bytes([ByteSerializer.SEPARATOR]))

    def marshall(self, obj: Any, clazz: type[Tid]) -> bytes:
        return cast(bytes, lz4.frame.compress(cloudpickle.dumps(obj)))

    def unmarshall(self, obj: bytes, clazz: type[Tid]) -> Tid:
        return cast(Tid, cloudpickle.loads(lz4.frame.decompress(obj)))
