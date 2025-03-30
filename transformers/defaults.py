from dataclasses import fields, is_dataclass, Field, MISSING
from typing import Any, TypeVar

T = TypeVar("T")

# Cache standaardwaarden per klasse om herhaald zoeken te vermijden
_dataclass_defaults_cache: dict[type[Any], dict[str, Any]] = {}


def get_dataclass_defaults(cls: type[T]) -> dict[str, Any]:
    """Haalt een dictionary op met standaardwaarden van een dataclass."""
    if cls not in _dataclass_defaults_cache:
        _dataclass_defaults_cache[cls] = {
            f.name: f.default if f.default is not MISSING else None for f in fields(cls) if isinstance(f, Field)  # Voor mypy type-nauwkeurigheid
        }
    return _dataclass_defaults_cache[cls]


def set_default(obj: T, field_name: str) -> None:
    """Stelt de standaardwaarde in voor een veld als deze None is of ontbreekt, met O(1) lookup."""
    if not is_dataclass(obj):
        raise ValueError("Object is geen instantie van een dataclass.")

    defaults = get_dataclass_defaults(type(obj))

    if field_name not in defaults:
        raise AttributeError(f"Field '{field_name}' bestaat niet in {type(obj).__name__}")

    setattr(obj, field_name, defaults[field_name])
