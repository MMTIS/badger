from __future__ import annotations

from typing import Generic, Sequence, TypeVar

from storage.serializer.interface import (
    ByteCodec,
    Serializer,
)


T = TypeVar("T")


class PipelineSerializer(Generic[T], Serializer[T]):
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
        serializer: Serializer[T],
        codecs: Sequence[ByteCodec] = (),
    ):
        self._serializer = serializer
        self._codecs = tuple(codecs)

    def dumps(self, obj: T) -> bytes:
        data = self._serializer.dumps(obj)

        for codec in self._codecs:
            data = codec.encode(data)

        return data

    def loads(self, data: bytes) -> T:
        for codec in reversed(self._codecs):
            data = codec.decode(data)

        return self._serializer.loads(data)