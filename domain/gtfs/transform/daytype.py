from typing import cast

import duckdb

from domain.gtfs.transform.datetime import date_to_xmldate, gtfs_date, date_to_xmldatetime
from domain.netex.model import (
    DayType,
    Codespace,
    DayTypeRef,
    DayTypeAssignment,
    OperatingPeriod,
    DayOfWeekEnumeration,
    PrivateCodes,
    PrivateCode,
    PropertiesOfDayRelStructure,
    PropertyOfDay,
    OperatingPeriodRef,
)
from domain.netex.services.ids import getId
from domain.netex.services.refs import getFakeRef, getRef


def get_service_id_dt(codespace: Codespace, service_id: str) -> str:
    if ':DayType:' in service_id:
        return service_id
    if ':AvailabilityCondition:' in service_id:
        return service_id.replace(':AvailabilityCondition:', ':DayType:')
    else:
        return getId(codespace, DayType, service_id)


def get_service_id_dta(codespace: Codespace, service_id: str) -> str:
    if ':DayTypeAssignment:' in service_id:
        return service_id
    else:
        return getId(codespace, DayTypeAssignment, service_id)


def get_service_id_op(codespace: Codespace, service_id: str) -> str:
    if ':OperatingPeriod:' in service_id:
        return service_id
    else:
        return getId(codespace, OperatingPeriod, service_id)


def getDayTypes(con: duckdb.DuckDBPyConnection, codespace: Codespace, version: str) -> tuple[list[DayType], list[DayTypeAssignment], list[OperatingPeriod]]:
    day_types = []
    day_type_assignments = []
    operating_periods = []
    fake_day_type_ids = set([])

    with con.cursor() as cur:
        exceptions_sql = "SELECT service_id, exception_type, date FROM calendar_dates ORDER BY date, exception_type;"
        cur.execute(exceptions_sql)

        while True:
            row = cur.fetchone()
            if row is None:
                break

            (
                service_id,
                exception_type,
                date,
            ) = row

            if exception_type in (1, 2):
                day_type_id = getFakeRef(get_service_id_dt(codespace, service_id), DayTypeRef, version)
                fake_day_type_ids.add(service_id)
                day_type_assignments.append(
                    DayTypeAssignment(
                        id=f"{get_service_id_dt(codespace, service_id).replace('DayType', 'DayTypeAssignment')}_{str(date)}_{str(exception_type)}",
                        version=version,
                        day_type_ref=day_type_id,
                        uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date=date_to_xmldate(gtfs_date(date)),
                        is_available=True if exception_type == 1 else False,
                    )
                )

        day_type_sql = (
            "SELECT service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date FROM calendar ORDER BY service_id;"
        )
        cur.execute(day_type_sql)
        while True:
            row = cur.fetchone()
            if row is None:
                break

            (
                service_id,
                monday,
                tuesday,
                wednesday,
                thursday,
                friday,
                saturday,
                sunday,
                start_date,
                end_date,
            ) = row

            days_of_week = []
            if monday == 1:
                days_of_week.append(DayOfWeekEnumeration.MONDAY)
            if tuesday == 1:
                days_of_week.append(DayOfWeekEnumeration.TUESDAY)
            if wednesday == 1:
                days_of_week.append(DayOfWeekEnumeration.WEDNESDAY)
            if thursday == 1:
                days_of_week.append(DayOfWeekEnumeration.THURSDAY)
            if friday == 1:
                days_of_week.append(DayOfWeekEnumeration.FRIDAY)
            if saturday == 1:
                days_of_week.append(DayOfWeekEnumeration.SATURDAY)
            if sunday == 1:
                days_of_week.append(DayOfWeekEnumeration.SUNDAY)

            day_type = DayType(
                id=get_service_id_dt(codespace, service_id),
                version=version,
                private_codes=PrivateCodes(private_code=[PrivateCode(type_value="service_id", value=service_id)]),
                properties=PropertiesOfDayRelStructure(property_of_day=[PropertyOfDay(days_of_week=days_of_week)]),
            )
            day_types.append(day_type)
            try:
                fake_day_type_ids.remove(service_id)
            except KeyError:
                pass

            operating_period = OperatingPeriod(
                id=get_service_id_op(codespace, service_id),
                version=version,
                from_operating_day_ref_or_from_date=date_to_xmldatetime(gtfs_date(start_date)),
                to_operating_day_ref_or_to_date=date_to_xmldatetime(gtfs_date(end_date)),
            )
            operating_periods.append(operating_period)

            day_type_assignments.append(
                DayTypeAssignment(
                    id=get_service_id_dta(codespace, service_id),
                    version=version,
                    day_type_ref=getFakeRef(get_service_id_dt(codespace, service_id), DayTypeRef, version),
                    uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date=cast(OperatingPeriodRef, getRef(operating_period)),
                )
            )

    # GTFS supports the implicit creation of calendar based on an inclusive set of exceptions.
    # Here we explicitly create the missing DayType that groups the DayTypeAssignment.
    for service_id in fake_day_type_ids:
        day_type = DayType(
            id=get_service_id_dt(codespace, service_id),
            version=version,
            private_codes=PrivateCodes(private_code=[PrivateCode(type_value="service_id", value=service_id)]),
        )
        day_types.append(day_type)

    return day_types, day_type_assignments, operating_periods
