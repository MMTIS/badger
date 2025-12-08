from dataclasses import is_dataclass, fields
from typing import Any, get_origin, Union, get_args
from zoneinfo import ZoneInfo

from xsdata.models.datatype import XmlTime, XmlDateTime


class XmlTimeZoned(XmlTime):
    """Extended XmlTime with explicit timezone."""

    zoneinfo: ZoneInfo | None

    def __new__(
        cls, hour: int, minute: int, second: int, fractional_second: int = 0, offset: int | None = None, zoneinfo: ZoneInfo | None = None
    ) -> "XmlTimeZoned":
        instance = super().__new__(cls, hour, minute, second, fractional_second, offset)
        instance.zoneinfo = zoneinfo
        return instance


class XmlDateTimeZoned(XmlDateTime):
    """Extended XmlTime with explicit timezone."""

    zoneinfo: ZoneInfo | None

    def __new__(
        cls,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
        fractional_second: int = 0,
        offset: int | None = None,
        zoneinfo: ZoneInfo | None = None,
    ) -> "XmlDateTimeZoned":
        instance = super().__new__(cls, year, month, day, hour, minute, second, fractional_second, offset)
        instance.zoneinfo = zoneinfo
        return instance

def is_xml_time_type(t: type) -> bool:
    """Checks for the type XmlTime is, including Optional[XmlTime]."""
    return t is XmlTime or (get_origin(t) is Union and XmlTime in get_args(t))


def is_xml_date_time_type(t: type) -> bool:
    """Checks for the type XmlTime is, including Optional[XmlTime]."""
    return t is XmlTime or (get_origin(t) is Union and XmlTime in get_args(t))


def convert_xml_time(value: XmlTime | XmlDateTime, zoneinfo: ZoneInfo) -> Any:
    """Transform XmlTime to XmlTimeZone When applicable."""
    if type(value) is XmlTime:
        return XmlTimeZoned(value.hour, value.minute, value.second, value.fractional_second, value.offset, zoneinfo)

    if type(value) is XmlDateTime:
        return XmlDateTimeZoned(value.year, value.month, value.day, value.hour, value.minute, value.second, value.fractional_second, value.offset, zoneinfo)

    return value


def replace_xml_time_with_timezone(obj: Any, zoneinfo: ZoneInfo) -> None:
    """Replace alle XmlTime instances by XmlTimeZone, recursive in dataclasses and lists."""
    if not is_dataclass(obj):
        return

    for field in fields(obj):
        value = getattr(obj, field.name)

        # Algemene verwerking zonder duplicatie
        if isinstance(value, (XmlTime, XmlDateTime, list, tuple)) or is_dataclass(value):
            object.__setattr__(obj, field.name, recursive_replace(value, zoneinfo))


def class_contains_xml_time(cls: Any) -> bool:
    """Recursieve functie om te bepalen of een dataclass ergens een XmlTime bevat."""
    if not is_dataclass(cls):
        return False

    for field in fields(cls):
        field_type = field.type
        if is_xml_time_type(field_type):  # Direct een XmlTime of Optional[XmlTime]
            return True
        if is_xml_date_time_type(field_type):  # Direct een XmlTime of Optional[XmlTime]
            return True
        if get_origin(field_type) is list:  # Lijst met dataclass-objecten
            field_type = get_args(field_type)[0]
        if is_dataclass(field_type) and class_contains_xml_time(field_type):  # Recursief checken
            return True
    return False


def recursive_replace(value: Any, zoneinfo: ZoneInfo) -> Any:
    """Recursive replacement of XmlTime in lists, tuples and dataclasses."""
    if type(value) in (XmlTime, XmlDateTime):
        return convert_xml_time(value, zoneinfo)

    if type(value) is list:
        return [recursive_replace(v, zoneinfo) for v in value]

    if type(value) is tuple:
        return tuple(recursive_replace(v, zoneinfo) for v in value)

    if is_dataclass(value):
        # Applies directly
        replace_xml_time_with_timezone(value, zoneinfo)

    return value