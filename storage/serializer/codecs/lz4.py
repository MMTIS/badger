from __future__ import annotations

import lz4.frame

from storage.serializer.interface import ByteCodec


class Lz4Codec(ByteCodec):
    """
    LZ4 compression codec.
    """

    def __init__(
        self,
        compression_level: int = 0,
    ):
        self._compression_level = compression_level

    def encode(self, data: bytes) -> bytes:
        return lz4.frame.compress(
            data,
            compression_level=self._compression_level,
        )

    def decode(self, data: bytes) -> bytes:
        return lz4.frame.decompress(data)