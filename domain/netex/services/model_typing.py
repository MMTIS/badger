from typing import TypeVar

from domain.netex.model import EntityStructure, EntityInVersionStructure, VersionOfObjectRefStructure

Tid = TypeVar("Tid", bound=EntityStructure)
Tver = TypeVar("Tver", bound=EntityInVersionStructure)
Tref = TypeVar("Tref", bound=VersionOfObjectRefStructure)
