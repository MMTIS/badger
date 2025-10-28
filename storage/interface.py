from abc import abstractmethod
from typing import Any, Iterable, Generator

from domain.netex.model import EntityStructure
from domain.utils import get_object_name
from domain.netex.services.model_typing import Tid


class Storage:
    @abstractmethod
    def clean(self) -> None: ...

    @abstractmethod
    def insert_objects_on_queue(self, klass: type[Tid], objects: Iterable[Tid], empty: bool = False) -> None: ...

    @abstractmethod
    def db_names(self) -> dict[bytes, type]: ...

    @abstractmethod
    def scan_objects(self, clazz: type[Tid], start_key: bytes | None, limit: int) -> Generator[bytes, None, None]: ...

    @abstractmethod
    def load_references_by_clazz_key(self, clazz: type, key: bytes, inwards: bool) -> Generator[tuple[type, bytes], None, None]: ...

    @abstractmethod
    def load_references_by_object(self, obj: Tid, inwards: bool) -> Generator[tuple[type, bytes], None, None]: ...


class Serializer:
    name_object: dict[str, type[EntityStructure]] = {}

    def __init__(self, classes: list[type[EntityStructure]]) -> None:
        self.name_object = {get_object_name(x): x for x in classes}

    @abstractmethod
    def split_key(self, key: bytes) -> list[bytes]: ...

    @abstractmethod
    def encode_key(self, id: str, version: str | None, clazz: type[Tid], include_clazz: bool = False) -> Any: ...

    @abstractmethod
    def marshall(self, obj: Any, clazz: type[Tid]) -> Any: ...

    @abstractmethod
    def unmarshall(self, obj: Any, clazz: type[Tid]) -> Tid: ...
