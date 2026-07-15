from __future__ import annotations

from typing import Any

from storage.keycodec.interface import KeyCodec

class BaseLineKeyCodec(KeyCodec):
    def encode(self, key: EntityInVersionStructure) -> bytes:
        id_bytes = key.id.encode()
        version_bytes = key.version.encode()
        class_byte = self.class_byte[key.__class__]

        size = len(id_bytes) + 1 + len(version_bytes) + 1 + len(class_byte)

        result = bytearray(size)

        offset = 0

        result[offset:offset + len(id_bytes)] = id_bytes
        offset += len(id_bytes)

        result[offset] = 0
        offset += 1

        result[offset:offset + len(version_bytes)] = version_bytes
        offset += len(version_bytes)

        result[offset] = 0
        offset += 1

        result[offset:] = class_byte

        return bytes(result)

    def _encode_id_version(self, key: EntityInVersionStructure) -> bytes:
        id_bytes = id.encode()
        version_bytes = key.version.encode()
        size = len(id_bytes) + 1 + len(version_bytes) + 1

        result = bytearray(size)

        offset = 0

        result[offset:offset + len(id_bytes)] = id_bytes
        offset += len(id_bytes)

        result[offset] = 0
        offset += 1

        result[offset:offset + len(version_bytes)] = version_bytes
        offset += len(version_bytes)

        result[offset] = 0
        return bytes(result)


    def prefix(
        self,
        key: EntityInVersionStructure,
        version: bool = True,
        class_idx: bool = True,
    ) -> bytes:

        if not version:
            id_bytes = key.id.encode()
            return id_bytes + b'\0'

        if not class_idx:
            return self._encode_id_version(key)

        return self.encode(key)