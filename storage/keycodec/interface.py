from abc import ABC, abstractmethod
from dataclasses import dataclass
from domain.netex.model import EntityInVersionStructure

class KeyCodec(ABC):
    class_byte: dict[type, bytes]

    def __init__(self, class_byte: dict):
        self.class_byte = class_byte

    @abstractmethod
    def encode(self, key: EntityInVersionStructure) -> bytes:
        """
        Convert logical object identity to MDBX key.
        """
        pass

    @abstractmethod
    def prefix(
        self,
        key: EntityInVersionStructure,
        version: bool = True,
        class_idx: bool = True,
    ) -> bytes:
        """
        Generate MDBX prefix for cursor iteration.
        """
        pass