from __future__ import annotations

from typing import Sequence, Any

from storage.objectserializer.interface import (
    ByteCodec,
    ObjectSerializer,
)


class PipelineSerializer(ObjectSerializer):
    """
    Serializer pipeline.

    Object:
        Serializer
        ↓
        ByteCodec(s)
        ↓
        bytes
    """

    def __init__(
        self,
        object_serializer: ObjectSerializer,
        codecs: Sequence[ByteCodec] = (),
    ):
        self._object_serializer = object_serializer
        self._codecs = tuple(codecs)

    def dumps(self, obj: Any) -> bytes:
        data = self._object_serializer.dumps(obj)

        for codec in self._codecs:
            data = codec.encode(data)

        return data

    def loads(self, data: bytes) -> Any:
        for codec in reversed(self._codecs):
            data = codec.decode(data)

        return self._object_serializer.loads(data)
