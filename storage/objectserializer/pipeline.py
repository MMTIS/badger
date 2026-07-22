from __future__ import annotations

from typing import Generic, Sequence, TypeVar

from storage.objectserializer.interface import (
    ByteCodec,
    ObjectSerializer,
)

T = TypeVar("T")


class PipelineSerializer(Generic[T], ObjectSerializer[T]):
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
        object_serializer: ObjectSerializer[T],
        codecs: Sequence[ByteCodec] = (),
    ):
        self._object_serializer = object_serializer
        self._codecs = tuple(codecs)

    def dumps(self, obj: T) -> bytes:
        data = self._object_serializer.dumps(obj)

        for codec in self._codecs:
            data = codec.encode(data)

        return data

    def loads(self, data: bytes) -> T:
        for codec in reversed(self._codecs):
            data = codec.decode(data)

        return self._object_serializer.loads(data)
