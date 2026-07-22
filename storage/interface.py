from abc import abstractmethod
from typing import Any, Iterable, Generator

from domain.utils import get_object_name
from domain.netex.model import EntityStructure


class Storage:
    @abstractmethod
    def clean(self) -> None: ...

    @abstractmethod
    def insert_objects_on_queue(self, klass: type[EntityStructure], objects: Iterable[EntityStructure], empty: bool = False) -> None: ...

    @abstractmethod
    def db_names(self) -> dict[bytes, type]: ...

    @abstractmethod
    def scan_objects(self, clazz: type[EntityStructure], start_key: bytes | None, limit: int) -> Generator[bytes, None, None]: ...

    @abstractmethod
    def load_references_by_clazz_key(self, clazz: type, key: bytes, inwards: bool) -> Generator[tuple[type, bytes], None, None]: ...

    @abstractmethod
    def load_references_by_object(self, obj: EntityStructure, inwards: bool) -> Generator[tuple[type, bytes], None, None]: ...


class Serializer:
    name_object: dict[str, type[EntityStructure]]
    class_idx: dict[type[EntityStructure], bytes]

    def __init__(self, classes: list[type[EntityStructure]]) -> None:
        self.name_object = {get_object_name(x): x for x in classes}

    def set_class_idx(self, class_idx: dict[type, bytes]) -> None:
        """This mapping assures that the stored indices in the database, matches the lookup."""
        self.class_idx = class_idx

    @abstractmethod
    def split_key(self, key: bytes) -> list[bytes]: ...

    @abstractmethod
    def encode_key_idx(self, id: str, version: str | None, clazz_idx: bytes) -> bytes: ...

    def encode_key(self, id: str, version: str | None, clazz: type[EntityStructure]) -> bytes:
        return self.encode_key_idx(id, version, self.class_idx[clazz])

    def encode_obj(self, obj: EntityStructure) -> bytes:
        assert obj.id is not None
        version = obj.version if hasattr(obj, "version") else None
        return self.encode_key_idx(obj.id, version, self.class_idx[obj.__class__])

    @abstractmethod
    def marshall(self, obj: Any, clazz: type[EntityStructure]) -> Any: ...

    @abstractmethod
    def unmarshall(self, obj: Any, clazz: type[EntityStructure]) -> EntityStructure: ...

    @staticmethod
    def full_key_to_clazz_idx(full_key: bytes) -> tuple[bytes, bytes]:
        # Original implementation
        # full_int = int.from_bytes(full_key, 'little')
        # class_idx = (full_int >> 32).to_bytes(2, 'little')
        # key = (full_int & 0xFFFFFFFF).to_bytes(4, 'little')
        # return class_idx, key
        return full_key[4:6], full_key[:4]

    @staticmethod
    def full_key_to_clazz(full_key: bytes) -> bytes:
        return full_key[4:6]

    @staticmethod
    def full_key_to_idx(full_key: bytes) -> bytes:
        return full_key[:4]

    @staticmethod
    def get_fullkey_by_class_idx(idx: bytes, this_class_idx: bytes) -> bytes:
        # Original implementation
        # this_class_idx = self.class_idx[clazz]
        # full_key = ((int.from_bytes(this_class_idx, 'little') << 32) | key).to_bytes(8, 'little')
        return idx + this_class_idx.ljust(4, b'\x00')

    def get_fullkey(self, idx: bytes, clazz: type[EntityStructure]) -> bytes:
        this_class_idx = self.class_idx[clazz]
        return Serializer.get_fullkey_by_class_idx(idx, this_class_idx)
