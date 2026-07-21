from __future__ import annotations

from domain.netex.services.model_typing import Tid
from domain.netex import EntityInVersionStructure
from storage.keycodec.interface import KeyCodec

SEPARATOR = 0
SEPARATOR_BYTES = bytes((SEPARATOR,))


class BaseLineKeyCodec(KeyCodec):
    def _encode(self, id: str, version: str, clazz: type[Tid]) -> bytes:
        id_bytes = id.encode()
        version_bytes = version.encode()
        class_byte = self.class_byte[clazz]

        size = len(id_bytes) + 1 + len(version_bytes) + 1 + len(class_byte)

        result = bytearray(size)

        offset: int = 0

        result[offset : offset + len(id_bytes)] = id_bytes
        offset += len(id_bytes)

        result[offset] = SEPARATOR
        offset += 1

        result[offset : offset + len(version_bytes)] = version_bytes
        offset += len(version_bytes)

        result[offset] = SEPARATOR
        offset += 1

        result[offset:] = class_byte

        return bytes(result)

    def _encode_id_version(self, id: str, version: str) -> bytes:
        id_bytes = id.encode()
        version_bytes = version.encode()
        size = len(id_bytes) + 1 + len(version_bytes) + 1

        result = bytearray(size)

        offset: int = 0

        result[offset : offset + len(id_bytes)] = id_bytes
        offset += len(id_bytes)

        result[offset] = SEPARATOR
        offset += 1

        result[offset : offset + len(version_bytes)] = version_bytes
        offset += len(version_bytes)

        result[offset] = SEPARATOR
        return bytes(result)

    def encode_key(self, id: str, version: str | None, clazz: type[Tid] | None, include_clazz: bool = False) -> bytes:
        if not version:
            id_bytes = id.encode()
            return id_bytes + SEPARATOR_BYTES

        if not clazz:
            return self._encode_id_version(id, version)

        return self._encode(id, version, clazz)

    def encode_obj(self, obj: EntityInVersionStructure, include_clazz: bool = False) -> bytes:
        assert obj.id is not None

        if not obj.version:
            id_bytes = obj.id.encode()
            return id_bytes + SEPARATOR_BYTES

        if not include_clazz:
            return self._encode_id_version(obj.id, obj.version)

        return self.encode_key(obj.id, obj.version, obj.__class__)

    def split_key(self, key: bytes) -> list[bytes]:
        return key.split(SEPARATOR_BYTES)
