from abc import abstractmethod
from typing import Any, Iterable, Generator, Optional

from domain.utils import get_object_name
from domain.netex.model import EntityStructure, VersionOfObjectRefStructure
from domain.netex.services.model_typing import Tid


class Storage:
    @abstractmethod
    def clean(self) -> None: ...

    @abstractmethod
    def insert_objects_on_queue(self, clazz: type[EntityStructure], objects: Iterable[EntityStructure], empty: bool = False) -> None: ...

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
    idx_clazz: dict[bytes, type[EntityStructure]]
    clazz_idx: dict[type[EntityStructure], bytes]
    clazz_name_idx: dict[str, bytes]

    def __init__(self, classes: set[type[EntityStructure]]) -> None:
        self.name_object = {get_object_name(x): x for x in classes}

    def set_clazz_idx(self, clazz_idx: dict[type, bytes], idx_clazz: dict[bytes, type[EntityStructure]]) -> None:
        """This mapping assures that the stored indices in the database, matches the lookup."""
        self.clazz_idx = clazz_idx
        self.idx_clazz = idx_clazz
        # We use the direct str value, instead of NameOfClass due to each reference has its own
        # enumeration. Hence ScheduledStopPointRef only points to {ScheduledStopPoint, FareScheduledStopPoint}.
        self.clazz_name_idx = {get_object_name(clazz): idx for clazz, idx in self.clazz_idx.items()}

    @abstractmethod
    def split_key(self, key: bytes) -> tuple[bytes, bytes, bytes]: ...

    @abstractmethod
    def encode_key_idx(self, id: str, version: str | None, clazz_idx: bytes) -> bytes: ...

    def class_idx_by_name(self, clazz_name: str) -> Optional[bytes]:
        return self.clazz_name_idx.get(clazz_name, None)

    def class_by_name(self, clazz_name: str) -> Optional[type[EntityStructure]]:
        clazz_idx = self.class_idx_by_name(clazz_name)
        if clazz_idx:
            return self.idx_clazz[clazz_idx]
        return None

    def encode_key(self, id: str, version: str | None, clazz: type[EntityStructure]) -> bytes:
        return self.encode_key_idx(id, version, self.clazz_idx[clazz])

    def encode_obj(self, obj: EntityStructure) -> bytes:
        assert obj.id is not None
        version = obj.version if hasattr(obj, "version") else None
        return self.encode_key_idx(obj.id, version, self.clazz_idx[obj.__class__])

    def encode_ref(self, ref: VersionOfObjectRefStructure) -> bytes:
        assert ref.ref is not None
        assert ref.name_of_ref_class is not None
        version = ref.version if hasattr(ref, "version") else None
        return self.encode_key_idx(ref.ref, version, self.clazz_name_idx[ref.name_of_ref_class.value])

    @abstractmethod
    def encode_prefix(self, id: str, version: str | None = None, clazz_idx: bytes | None = None) -> bytes: ...

    @abstractmethod
    def marshall(self, obj: Any, clazz: type[EntityStructure]) -> Any: ...

    @abstractmethod
    def unmarshall(self, obj: Any, clazz: type[Tid]) -> Tid: ...

    @staticmethod
    def full_key_to_clazz_idx(full_key: bytes) -> tuple[bytes, bytes]:
        # Original implementation
        # full_int = int.from_bytes(full_key, 'little')
        # clazz_idx = (full_int >> 32).to_bytes(2, 'little')
        # key = (full_int & 0xFFFFFFFF).to_bytes(4, 'little')
        # return clazz_idx, key
        return full_key[4:6], full_key[:4]

    @staticmethod
    def full_key_to_clazz(full_key: bytes) -> bytes:
        return full_key[4:6]

    @staticmethod
    def full_key_to_idx(full_key: bytes) -> bytes:
        return full_key[:4]

    @staticmethod
    def get_fullkey_by_clazz_idx(idx: bytes, this_clazz_idx: bytes) -> bytes:
        # Original implementation
        # this_clazz_idx = self.clazz_idx[clazz]
        # full_key = ((int.from_bytes(this_clazz_idx, 'little') << 32) | key).to_bytes(8, 'little')
        return idx + this_clazz_idx.ljust(4, b'\x00')

    def get_fullkey(self, idx: bytes, clazz: type[EntityStructure]) -> bytes:
        this_clazz_idx = self.clazz_idx[clazz]
        return Serializer.get_fullkey_by_clazz_idx(idx, this_clazz_idx)
