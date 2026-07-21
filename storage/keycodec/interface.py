from abc import ABC, abstractmethod
from domain.netex.model import EntityInVersionStructure
from domain.netex.services.model_typing import Tid


class KeyCodec(ABC):
    class_byte: dict[type, bytes]

    def __init__(self, class_byte: dict[type[Tid], bytes]):
        self.class_byte = class_byte

    @abstractmethod
    def encode_key(self, id: str, version: str | None, clazz: type[Tid] | None, include_clazz: bool = True) -> bytes:
        pass

    @abstractmethod
    def encode_obj(self, obj: EntityInVersionStructure, include_clazz: bool = True) -> bytes:
        pass

    @abstractmethod
    def split_key(self, key: bytes) -> list[bytes]:
        pass
