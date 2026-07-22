from typing import Any

from domain.netex.model import MultilingualString, TextType


def get_or_none(data: dict[str, Any], key: str) -> Any | None:
    """Return value for key if present and not None, else None."""
    return data.get(key) if data.get(key) is not None else None


def getOptionalString(name: str | None, default: str | None = None) -> MultilingualString | None:
    if name is not None:
        return MultilingualString(content=[TextType(value=name)])
    elif default is not None:
        return MultilingualString(content=[TextType(value=default)])

    return None


def getRequiredString(name: str | None, default: str | None = None) -> MultilingualString:
    if name is not None:
        return MultilingualString(content=[TextType(value=name)])

    assert default is not None
    return MultilingualString(content=[TextType(value=default)])


def getShortName(name: str) -> str:
    if len(name) > 8:
        return ''.join([x[0].upper() for x in name.split(' ')])
    return name
