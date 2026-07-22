from abc import ABC, abstractmethod


class KeyCodec(ABC):
    @staticmethod
    @abstractmethod
    def encode_key_idx(id: str, version: str | None, clazz_idx: bytes) -> bytes: ...

    @staticmethod
    @abstractmethod
    def split_key(key: bytes) -> list[bytes]: ...
