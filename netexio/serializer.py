from abc import abstractmethod
from typing import TypeVar, Any

from netex import EntityStructure
from utils.utils import get_object_name, get_boring_classes, get_interesting_classes
import netex

netex.set_all = frozenset(netex.__all__)  # type: ignore # This is the true performance step

T = TypeVar("T")
Tid = TypeVar("Tid", bound=EntityStructure)


class Serializer:
    def __init__(self) -> None:
        self.name_object: dict[str, type[Any]] = {}
        for clazz in get_boring_classes():
            self.name_object[get_object_name(clazz)] = clazz

        (
            self.clean_element_names,
            self.interesting_element_names,
            self.interesting_classes,
        ) = get_interesting_classes()
        for i in range(0, len(self.interesting_element_names)):
            # TODO: Validate duplicates, below will only make sure we overwrite with first order members
            self.name_object[self.interesting_element_names[i]] = (
                self.interesting_classes[i]
            )

    @staticmethod
    @abstractmethod
    def encode_key(
        id: str | None, version: str | None, clazz: type[Tid], include_clazz: bool = False
    ) -> Any: ...

    @abstractmethod
    def marshall(self, xml: Any, clazz: type[Tid]) -> Any: ...

    @abstractmethod
    def unmarshall(self, obj: Any, clazz: type[Tid]) -> Tid: ...
