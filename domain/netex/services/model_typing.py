from typing import TypeVar

from domain.netex.model import EntityInVersionStructure, EntityStructure, VersionOfObjectRefStructure

Tid = TypeVar("Tid", bound=EntityStructure)
Tver = TypeVar("Tver", bound=EntityInVersionStructure)
Tref = TypeVar("Tref", bound=VersionOfObjectRefStructure)
