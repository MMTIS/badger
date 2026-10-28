import struct
from typing import Any, cast

from domain.netex import EntityStructure, EntityInVersion
from domain.netex.services.model_typing import Tid
from storage.interface import Serializer
from storage.keycodec.interface import KeyCodec
from storage.keycodec.baseline import BaseLineKeyCodec
from storage.objectserializer.codecs.path import decode_path, encode_path
from storage.objectserializer.interface import ObjectSerializer
from storage.objectserializer.codecs.lz4 import Lz4Codec
from storage.objectserializer.cloudpickle.serializer import CloudPickleSerializer
from storage.objectserializer.pipeline import PipelineSerializer
from collections.abc import Sequence

SEPARATOR = 13
SEPARATOR_BYTES = bytes((SEPARATOR,))

class CombinedSerializer(Serializer):
    key_codec: type[KeyCodec]
    object_serializer: ObjectSerializer

    def __init__(
        self,
        classes: set[type[EntityStructure]],
        key_codec: type[KeyCodec] | None = None,
        object_serializer: ObjectSerializer | None = None,
    ):
        super().__init__(classes)
        self.key_codec = key_codec if key_codec else BaseLineKeyCodec
        self.object_serializer = object_serializer if object_serializer else PipelineSerializer(object_serializer=CloudPickleSerializer(), codecs=[Lz4Codec()])

    def encode_key_clazz(self, id: str, version: str | None, clazz: EntityStructure | None) -> bytes:
        if version and clazz:
            return self.key_codec.encode_key_idx(id, version, self.clazz_idx[clazz])
        else:
            return self.key_codec.encode_prefix(id, version, self.clazz_idx[clazz] if clazz else None)

    def encode_key_idx(self, id: str, version: str | None, clazz_idx: bytes) -> bytes:
        return self.key_codec.encode_key_idx(id, version, clazz_idx)

    def decode_key_idx(self, key: bytes) -> tuple[bytes, bytes, bytes]:
        return self.key_codec.split_key(key)

    def encode_prefix(self, id: str, version: str | None = None, clazz_idx: bytes | None = None) -> bytes:
        return self.key_codec.encode_prefix(id, version, clazz_idx)

    def encode_ref_path(self, id: str, version: str | None, clazz: type[EntityInVersion] | None, path: Sequence[int], referencing_clazz: type[EntityInVersion]) -> bytes:
        UINT32 = struct.Struct(">I")

        clazz_bytes = self.clazz_idx[clazz] if clazz else b''
        referenced_key = self.encode_key_idx(
            id, version, clazz_bytes
        )

        referencing_idx = self.clazz_idx[referencing_clazz]
        encoded_path = encode_path(path)

        return (
            UINT32.pack(len(referenced_key))
            + referenced_key
            + UINT32.pack(len(referencing_idx))
            + referencing_idx
            + UINT32.pack(len(encoded_path))
            + encoded_path
        )

    def decode_ref_path(
            self,
            data: bytes,
    ) -> tuple[str, str | None, type | None, tuple[int, ...], type]:
        UINT32 = struct.Struct(">I")
        offset = 0

        def read_uint32() -> int:
            nonlocal offset
            if len(data) - offset < UINT32.size:
                raise ValueError("Truncated ref path")
            value = UINT32.unpack_from(data, offset)[0]
            offset += UINT32.size
            return value

        def read_bytes(length: int) -> bytes:
            nonlocal offset
            if length > len(data) - offset:
                raise ValueError("Truncated ref path")
            value = data[offset: offset + length]
            offset += length
            return value

        key = read_bytes(read_uint32())
        id, version, clazz_idx = self.decode_key_idx(key)
        referencing_idx = read_bytes(read_uint32())
        path = decode_path(read_bytes(read_uint32()))

        if offset != len(data):
            raise ValueError("Unexpected trailing bytes")

        try:
            return (
                id.decode(),
                version.decode(),
                self.idx_clazz[clazz_idx] if clazz_idx != b'' else None,
                path,
                self.idx_clazz[referencing_idx],
            )
        except KeyError as exc:
            raise ValueError("Unknown class index") from exc

    def split_key(self, key: bytes) -> tuple[bytes, bytes, bytes]:
        return self.key_codec.split_key(key)

    def split_key_str(self, key: bytes) -> tuple[str, str, bytes]:
        return self.key_codec.split_key_str(key)

    def split_key_path(self, key_path: bytes) -> tuple[tuple[str, str, bytes], Sequence[int]]:
        key, path = key_path.split(SEPARATOR_BYTES, 1)
        return self.split_key_str(key), decode_path(path)

    def marshall(self, obj: Any, clazz: type[Tid]) -> bytes:
        return self.object_serializer.dumps(obj)

    def unmarshall(self, obj: bytes, clazz: type[Tid]) -> Tid:
        return cast(Tid, self.object_serializer.loads(obj))
