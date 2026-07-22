from __future__ import annotations

from typing import cast

import lz4.frame

from storage.objectserializer.interface import ByteCodec


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
        return cast(
            bytes,
            lz4.frame.compress(
                data,
                compression_level=self._compression_level,
            ),
        )

    def decode(self, data: bytes) -> bytes:
        return cast(bytes, lz4.frame.decompress(data))
