import warnings
from typing import List, Set, Dict

import dateutil.rrule
from xsdata.models.datatype import XmlDateTime, XmlDate

from netex import AvailabilityCondition, DayType, DayOfWeekEnumeration, UicOperatingPeriod, ServiceJourney, \
    ServiceCalendar, Codespace, DayTypeAssignment, OperatingPeriodRef, DayTypesRelStructure, OperatingDaysRelStructure, \
    ValidityConditionsRelStructure, AvailabilityConditionRef, DayTypeRefsRelStructure, OperatingPeriodsRelStructure, \
    DayTypeAssignmentsRelStructure
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY

from utils.refs import getIndex, getRef, getId


class ServiceCalendarEPIPFrame:
    availability_conditions: List[AvailabilityCondition]
    codespace: Codespace

    @staticmethod
    def mapDayOfWeekToWeekday(day_of_week: DayOfWeekEnumeration) -> List[int]:
        match day_of_week:
            case DayOfWeekEnumeration.EVERYDAY:
                return [-1]
            case DayOfWeekEnumeration.MONDAY:
                return [dateutil.rrule.MO]
            case DayOfWeekEnumeration.TUESDAY:
                return [dateutil.rrule.TU]
            case DayOfWeekEnumeration.WEDNESDAY:
                return [dateutil.rrule.WE]
            case DayOfWeekEnumeration.THURSDAY:
                return [dateutil.rrule.TH]
            case DayOfWeekEnumeration.FRIDAY:
                return [dateutil.rrule.FR]
            case DayOfWeekEnumeration.SATURDAY:
                return [dateutil.rrule.SA]
            case DayOfWeekEnumeration.SUNDAY:
                return [dateutil.rrule.SU]
            case DayOfWeekEnumeration.WEEKDAYS:
                # This option is deprecated due to its conflicts within different locales
                return [dateutil.rrule.MO, dateutil.rrule.TU, dateutil.rrule.WE, dateutil.rrule.TH, dateutil.rrule.FR]
            case DayOfWeekEnumeration.WEEKEND:
                # This option is deprecated due to its conflicts within different locales
                return [dateutil.rrule.SA, dateutil.rrule.SU]

    @staticmethod
    def mapDaysOfWeekToByWeekday(days_of_week: List[DayOfWeekEnumeration]):
        byweekday = set([])
        for day_of_week in days_of_week:
            mapping = ServiceCalendarEPIPFrame.mapDayOfWeekToWeekday(day_of_week)
            if mapping[0] == -1:
                return []
            else:
                byweekday = byweekday.union(mapping)
        return byweekday

    """
    This function flattens a single AvailabilityCondition into a set of either available or unavailable dates.    
    """
    @staticmethod
    def flattenAvailabilityCondition(availability_condition: AvailabilityCondition) -> (Set[datetime], List[DayOfWeekEnumeration], bool):
        operational_dates: datetime
        days_of_week: List[DayOfWeekEnumeration]
        days_of_week = []

        if availability_condition.day_types is not None and len(availability_condition.day_types.day_type_ref_or_day_type) > 1:
            warnings.warn(f"AvailabilityCondition {availability_condition.id} has multiple DayTypes, we cannot handle this (yet), as we don't know the intention. Only the first DayType is being evaluated.")

        if availability_condition.day_types is None or len(availability_condition.day_types.day_type_ref_or_day_type) == 0:
            if len(availability_condition.valid_day_bits) > 0:
                operational_dates = [availability_condition.from_date.to_datetime() + timedelta(days=i) for i in range(0, len(availability_condition.valid_day_bits)) if availability_condition.valid_day_bits[i] == '1']
            else:
                warnings.warn(
                    f"AvailabilityCondition {availability_condition.id} is using an unknown date limitation.")
                return None

        elif len(availability_condition.day_types.day_type_ref_or_day_type) >= 1:
            day_type: DayType
            day_type = availability_condition.day_types.day_type_ref_or_day_type[0]
            days_of_week = day_type.properties.property_of_day[0].days_of_week
            byweekday = ServiceCalendarEPIPFrame.mapDaysOfWeekToByWeekday(
                day_type.properties.property_of_day[0].days_of_week)

            operational_dates = list(rrule(DAILY, byweekday=byweekday, dtstart=availability_condition.from_date.to_datetime(),
                       until=availability_condition.to_date.to_datetime()))

        return (set(operational_dates), days_of_week, availability_condition.is_available or True)

    """
    This function combines a list of flattened availability conditions
    and flattens it further into a single (positive) set of dates expressed.
    """
    @staticmethod
    def positiveAvailabilityCondition(availability_conditions: List[AvailabilityCondition]) -> (List[datetime], List[DayOfWeekEnumeration]):
        positive_dates: Set[datetime]
        negative_dates: Set[datetime]
        positive_dates = set([])
        negative_dates = set([])
        days_of_week = []

        for availability_condition in availability_conditions:
            output = ServiceCalendarEPIPFrame.flattenAvailabilityCondition(availability_condition)
            if output is not None:
                operational_dates, days_of_week, available = ServiceCalendarEPIPFrame.flattenAvailabilityCondition(availability_condition)
                if available:
                    positive_dates = positive_dates.union(operational_dates)
                else:
                    negative_dates = negative_dates.union(operational_dates)

        if len(negative_dates) > 0:
            positive_dates -= negative_dates

        return (list(sorted(positive_dates)), days_of_week)

    @staticmethod
    def valid_days_to_bits(valid_days: list[datetime]) -> str:
        valid_days = sorted(valid_days)
        return ''.join([str((valid_days[0] + timedelta(days=i) in valid_days) * 1) for i in range(0, (valid_days[-1] - valid_days[0]).days + 1)])

    """
    This function expects a flattened AvailabilityCondition, which it exports into:
        DayType, UicOperatingPeriod and DayTypeAssignment
    """
    def availabilityConditionsToServiceCalendar(self, service_journeys: list[ServiceJourney], availability_conditions: list[AvailabilityCondition]) -> ServiceCalendar:
        availability_conditions = getIndex(availability_conditions)
        service_journey: ServiceJourney
        uic_operating_periods: List[UicOperatingPeriod]
        day_type_assignments: List[DayTypeAssignment]
        day_type_assignment: DayTypeAssignment
        day_types: Dict[str, DayType]
        day_type: DayType

        uic_operating_periods = []
        day_type_assignments = []
        day_types = {}

        for service_journey in service_journeys:
            acs = []

            for ac in service_journey.validity_conditions_or_valid_between:
                ac: ValidityConditionsRelStructure
                for a in ac.choice:
                    if isinstance(a, AvailabilityConditionRef):
                        acs.append(availability_conditions[a.ref])
                    elif isinstance(a, AvailabilityCondition):
                        acs.append(a)
                    else:
                        warnings.warn(f"Unhandled ValidityCondition in {service_journey.id}")

            # TODO: this will fail in extreme cases
            day_type_id = acs[0].id.replace('AvailabilityCondition', 'DayType')

            if day_type_id not in day_types:
                valid_days, days_of_week = ServiceCalendarEPIPFrame.positiveAvailabilityCondition(acs)

                uic_operating_period = UicOperatingPeriod(id=acs[0].id.replace('AvailabilityCondition', 'UicOperatingPeriod'), version=acs[0].version,
                                    derived_from_object_ref=acs[0].id, derived_from_version_ref_attribute=acs[0].version,
                                   from_operating_day_ref_or_from_date=XmlDateTime.from_datetime(valid_days[0]),
                                   to_operating_day_ref_or_to_date=XmlDateTime.from_datetime(valid_days[-1]),
                                   valid_day_bits=ServiceCalendarEPIPFrame.valid_days_to_bits(valid_days),
                                   days_of_week=days_of_week)
                uic_operating_periods.append(uic_operating_period)

                day_type = DayType(id=day_type_id, version=service_journey.version,
                    derived_from_object_ref=service_journey.id, derived_from_version_ref_attribute=service_journey.version)
                day_types[day_type_id] = day_type

                day_type_assignment = DayTypeAssignment(id=acs[0].id.replace('AvailabilityCondition', 'DayTypeAssignment'), version=acs[0].version,
                                                        order=1,
                                                        derived_from_object_ref=acs[0].id, derived_from_version_ref_attribute=acs[0].version,
                                                        uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date=getRef(uic_operating_period, OperatingPeriodRef),
                                                        day_type_ref=getRef(day_type)
                                                        )
                day_type_assignments.append(day_type_assignment)

            day_type = day_types[day_type_id]
            service_journey.day_types = DayTypeRefsRelStructure(day_type_ref = [getRef(day_type)])

        from_date: datetime
        to_date: datetime
        from_date = min([uic.from_operating_day_ref_or_from_date.to_datetime() for uic in uic_operating_periods])
        to_date = max([uic.to_operating_day_ref_or_to_date.to_datetime() for uic in uic_operating_periods])

        return ServiceCalendar(id=getId(ServiceCalendar, self.codespace, "ServiceCalendar"),
                               version=service_journeys[0].version,
                               from_date=XmlDate.from_date(from_date.date()), to_date=XmlDate.from_date(to_date.date()),
                               day_types=DayTypesRelStructure(day_type_ref_or_day_type=list(day_types.values())),
                               operating_periods=OperatingPeriodsRelStructure(uic_operating_period_ref_or_operating_period_ref_or_operating_period_or_uic_operating_period=uic_operating_periods),
                               day_type_assignments=DayTypeAssignmentsRelStructure(day_type_assignment=day_type_assignments))

    def __init__(self, codespace: Codespace):
        self.codespace = codespace