import math
import datetime
from xsdata.models.datatype import XmlTime, XmlDateTime, XmlDate


def gtfs_date(d: str) -> datetime.datetime:
    return datetime.datetime(year=int(str(d)[0:4]), month=int(str(d)[4:6]), day=int(str(d)[6:8]))


def noonTimeToNeTEx(time: str) -> tuple[XmlTime, int]:
    hour, minute, second = time.split(':')
    hour_i = int(hour)
    day_offset = int(math.floor(hour_i / 24))
    return (XmlTime(hour=hour_i % 24, minute=int(minute), second=int(second)), day_offset)


def date_to_xmldatetime(d: datetime.date) -> XmlDateTime:
    x = datetime.datetime.combine(d, datetime.datetime.min.time())
    return XmlDateTime.from_datetime(x)


def date_to_xmldate(d: datetime.date) -> XmlDate:
    return XmlDate.from_date(d)
