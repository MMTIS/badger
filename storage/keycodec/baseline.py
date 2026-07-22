from __future__ import annotations

from storage.keycodec.interface import KeyCodec

SEPARATOR = 10
SEPARATOR_BYTES = bytes((SEPARATOR,))


class BaseLineKeyCodec(KeyCodec):
    @staticmethod
    def encode_key_idx(id: str, version: str | None, clazz_idx: bytes) -> bytes:
        id_bytes = id.encode()
        version_bytes = version.encode() if version else b""

        size = len(id_bytes) + 1 + len(version_bytes) + 1 + len(clazz_idx)

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

        result[offset:] = clazz_idx

        return bytes(result)

    @staticmethod
    def _encode_id_version(id: str, version: str) -> bytes:
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

    # TODO
    @staticmethod
    def prefix(id: str, version: str | None, clazz_idx: bytes | None, include_clazz: bool = False) -> bytes:
        if not version:
            id_bytes = id.encode()
            return id_bytes + SEPARATOR_BYTES

        if not clazz_idx:
            return BaseLineKeyCodec._encode_id_version(id, version)

        return BaseLineKeyCodec.encode_key_idx(id, version, clazz_idx)

    @staticmethod
    def split_key(key: bytes) -> list[bytes]:
        return key.split(SEPARATOR_BYTES)
