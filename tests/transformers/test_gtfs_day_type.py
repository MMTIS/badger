import unittest
from datetime import date, datetime

from xsdata.models.datatype import XmlDate, XmlDateTime

import utils.netex_monkeypatching  # noqa: F401
from domain.netex.model import (
    DayOfWeekEnumeration as D,
    DayType,
    DayTypeAssignment,
    PropertiesOfDayRelStructure,
    PropertyOfDay,
    UicOperatingPeriod,
    UicOperatingPeriodRef,
)
from transformers.gtfs import gtfs_day_type


def _dates(valid_day_bits: str) -> tuple[set[date], set[date]]:
    """Removed and added dates for a Monday-Saturday day type, Thursday 2026-10-01 until Sunday 2026-10-11."""
    days = [D.MONDAY, D.TUESDAY, D.WEDNESDAY, D.THURSDAY, D.FRIDAY, D.SATURDAY]
    day_type = DayType(id="dt", version="1", properties=PropertiesOfDayRelStructure(property_of_day=[PropertyOfDay(days_of_week=days)]))
    period = UicOperatingPeriod(
        id="p",
        version="1",
        from_operating_day_ref_or_from_date=XmlDateTime.from_datetime(datetime(2026, 10, 1)),
        to_operating_day_ref_or_to_date=XmlDateTime.from_datetime(datetime(2026, 10, 11)),
        valid_day_bits=valid_day_bits,
    )
    dta = DayTypeAssignment(
        id="dta",
        version="1",
        order=1,
        is_available=True,
        day_type_ref="dt",
        uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date=UicOperatingPeriodRef(ref="p", version="1"),
    )

    _, assignments, _ = gtfs_day_type(day_type, [dta], [], [period], [])

    removed, added = set(), set()
    for a in assignments:
        value = a.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date
        if isinstance(value, XmlDate):
            (added if a.is_available is not False else removed).add(value.to_date())
    return removed, added


class TestGtfsDayTypeValidDayBits(unittest.TestCase):
    def test_holes_become_removed_dates(self) -> None:
        self.assertEqual(_dates("11001111100"), ({date(2026, 10, 3), date(2026, 10, 10)}, set()))

    def test_day_outside_the_weekly_pattern_becomes_an_added_date(self) -> None:
        self.assertEqual(_dates("11011111100"), ({date(2026, 10, 3), date(2026, 10, 10)}, {date(2026, 10, 4)}))

    def test_no_holes_gives_no_dates(self) -> None:
        self.assertEqual(_dates("11101111110"), (set(), set()))
