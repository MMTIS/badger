import os
from typing import Any, cast

from domain.netex import EntityStructure
from domain.netex.services.model_typing import Tid
from storage.interface import Serializer
from storage.keycodec.interface import KeyCodec
from storage.keycodec.baseline import BaseLineKeyCodec
from storage.objectserializer.interface import ObjectSerializer
from storage.objectserializer.codecs.lz4 import Lz4Codec
from storage.objectserializer.cloudpickle.serializer import CloudPickleSerializer
from storage.objectserializer.msgspec.serializer import MsgspecSerializer
from storage.objectserializer.pipeline import PipelineSerializer


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
        if object_serializer is None:
            backend = os.getenv("BADGER_SERIALIZER", "cloudpickle").lower()
            if backend == "msgspec":
                base_serializer: ObjectSerializer = MsgspecSerializer()
            else:
                base_serializer = CloudPickleSerializer()
            object_serializer = PipelineSerializer(object_serializer=base_serializer, codecs=[Lz4Codec()])
        self.object_serializer = object_serializer

    def encode_key_idx(self, id: str, version: str | None, clazz_idx: bytes) -> bytes:
        return self.key_codec.encode_key_idx(id, version, clazz_idx)

    def encode_prefix(self, id: str, version: str | None = None, clazz_idx: bytes | None = None) -> bytes:
        return self.key_codec.encode_prefix(id, version, clazz_idx)

    def split_key(self, key: bytes) -> tuple[bytes, bytes, bytes]:
        return self.key_codec.split_key(key)

    def marshall(self, obj: Any, clazz: type[Tid]) -> bytes:
        return self.object_serializer.dumps(obj)

    def unmarshall(self, obj: bytes, clazz: type[Tid]) -> Tid:
        try:
            return cast(Tid, self.object_serializer.loads(obj, clazz=clazz))
        except TypeError:
            return cast(Tid, self.object_serializer.loads(obj))

