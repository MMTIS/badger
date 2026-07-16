from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar


T = TypeVar("T")


class Serializer(Generic[T], ABC):
    """
    Converts Python objects to bytes and back.
    """

    @abstractmethod
    def dumps(self, obj: T) -> bytes:
        """
        Serialize an object.
        """
        raise NotImplementedError

    @abstractmethod
    def loads(self, data: bytes) -> T:
        """
        Deserialize bytes.
        """
        raise NotImplementedError


class ByteCodec(ABC):
    """
    Transforms bytes into bytes.

    Examples:
    - Compression
    - Encryption
    - Checksums
    - Framing
    """

    @abstractmethod
    def encode(self, data: bytes) -> bytes:
        """
        Transform outgoing bytes.
        """
        raise NotImplementedError

    @abstractmethod
    def decode(self, data: bytes) -> bytes:
        """
        Restore incoming bytes.
        """
        raise NotImplementedError