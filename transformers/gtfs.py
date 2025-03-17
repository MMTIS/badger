import hashlib
import logging
import sys
import warnings
from datetime import timedelta, datetime, time
from typing import Generator, TypeVar, Iterable, Any

from xsdata.models.datatype import XmlDate, XmlDateTime

from transformers.callsprofile import CallsProfile
from netex import Line, Branding, Operator, Authority, ResponsibilitySet, StakeholderRoleTypeEnumeration, \
    ServiceJourney, TemplateServiceJourney, \
    ServiceJourneyPattern, TimeDemandType, RouteView, RouteRef, Route, ValidityConditionsRelStructure, \
    AvailabilityCondition, AvailabilityConditionRef, DayType, DayTypeAssignment, OperatingPeriod, ServiceCalendar, \
    DayTypesRelStructure, OperatingPeriodRef, UicOperatingPeriodRef, OperatingDay, DayTypeRef, \
    OperatingDaysRelStructure, OperatingDayRefStructure, UicOperatingPeriod, OperatingDayRef, \
    PropertiesOfDayRelStructure, PropertyOfDay, DayOfWeekEnumeration, DayTypeRefsRelStructure, Version
from netexio.database import Database
from netexio.dbaccess import load_generator, load_local, copy_table
from transformers.nordicprofile import NordicProfile
from utils.refs import getRef, getIndex, getIndexByGroup, getId
from transformers.daytype import get_day_type_from_availability_condition, datetime_weekday_to_dow
from utils.utils import project, chain
from utils.aux_logging import log_all, log_once

import utils.netex_monkeypatching

T = TypeVar("T")

GTFS_CLASSES = { "Codespace", "StopPlace", "ScheduledStopPoint", "Authority", "Operator", "Line", "DestinationDisplay",
                 "ServiceJourney", "ServiceJourneyPattern", "PassengerStopAssignment", "AvailabilityCondition",
                 "DayTypeAssignment", "UicOperatingPeriod", "TemplateServiceJourney", "Branding",
                 "InterchangeRule", "ServiceJourneyInterchange, JourneyMeeting" }

def gtfs_operator_line_memory(db_read: Database, db_write: Database, generator_defaults: dict):
    operator: Operator
    print(sys._getframe().f_code.co_name)
    lines: list[Line] = load_local(db_read, Line)
    operators_local: list[Operator] = load_local(db_read, Operator)

    operators: dict[str, Operator] = {}
    for line in lines:
        # GTFS only has the concept agency (operator) hence all variants will become OperatorRef.
        if line.branding_ref is not None:
            branding: Branding = db_read.get_single(Branding, line.branding_ref.ref, line.branding_ref.version)
            operator = project(branding, Operator)
            operators[operator.id] = operator
            line.operator_ref = getRef(operator)

        elif line.operator_ref is not None:
            if line.operator_ref.ref not in operators:
                operator = db_read.get_single(Operator, line.operator_ref.ref, line.operator_ref.version)
                operators[operator.id] = operator
                line.operator_ref = getRef(operator)

        elif line.authority_ref is not None:
            authority: Authority = db_read.get_single(Authority, line.authority_ref.ref, line.authority_ref.version)
            operator = project(authority, Operator)
            operators[operator.id] = operator
            line.operator_ref = getRef(operator)

        elif line.responsibility_set_ref_attribute is not None:
            # TODO: ResponsibilitySet to Operator/Authority should be a separate function
            responsibility_set: ResponsibilitySet = db_read.get_single(ResponsibilitySet, line.responsibility_set_ref_attribute)
            if responsibility_set is not None and responsibility_set.roles is not None:
                for role_assignment in responsibility_set.roles.responsibility_role_assignment:
                    if StakeholderRoleTypeEnumeration.OPERATION in role_assignment.stakeholder_role_type or StakeholderRoleTypeEnumeration.OPERATION_1 in role_assignment.stakeholder_role_type:
                        operator = db_read.get_single(db_read.get_class_by_name(role_assignment.responsible_organisation_ref.name_of_ref_class) if role_assignment.responsible_organisation_ref.name_of_ref_class else Operator, role_assignment.responsible_organisation_ref.ref, role_assignment.responsible_organisation_ref.version)
                        operators[operator.id] = operator
                        line.operator_ref = getRef(operator)

        if line.operator_ref is None:
            if len(operators_local) == 1:
                operator = operators_local[0]
                operators[operator.id] = operator
                line.operator_ref = getRef(operator)

            else:
                # Is there any ServiceJourney -> OperatorRef relationship?
                # TODO: Now we would like to filter on a property such as LineRef, but that may obviously be unavailable...
                # TODO: Implement the reference chain by the reference table, so it finds the object of interest, by joins (inna-search)
                # TODO: Iff multiple OperatorRefs are pointing to the same LineRef, the LineRef must be duplicated for GTFS export.
                log_all(logging.INFO,f"Operator not found for {line}. Implement Innasearch.")
                pass

        line.authority_ref = None
        line.responsibility_set_ref_attribute = None
        line.branding_ref = None
        line.type_of_line_ref = None
        line.type_of_service_ref = None
        line.type_of_product_category_ref = None
        line.types_of_payment_method = None

        if line.operator_ref is None:
            log_all(logging.ERROR,f"Line {line.id} does not have an operator_ref assigned.")

    db_write.insert_objects_on_queue(Operator, list(operators.values()), True)
    db_write.insert_objects_on_queue(Line, lines, True)

# TODO: move to separate file (calls profiles)
def  add_calls(db_read: Database, sj: ServiceJourney) -> ServiceJourney:
    if sj.calls:
        return sj
    else:
        sjp: ServiceJourneyPattern = db_read.get_single(ServiceJourneyPattern, sj.journey_pattern_ref.ref)
        if sjp is None:
            log_all(logging.ERROR,"No SJP")

        else:
            # TODO: very costly to do it here for every ServiceJourneyPattern
            if sj.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view is None:
                if sjp.route_ref_or_route_view is not None:
                    if isinstance(sjp.route_ref_or_route_view, RouteView):
                        sj.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view = sjp.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view
                    elif isinstance(sjp.route_ref_or_route_view, RouteRef):
                        sj.route_ref = sjp.route_ref_or_route_view
                        route: Route = db_read.get_single(Route, sjp.route_ref_or_route_view.ref)
                        sj.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view = route.line_ref

            if sj.passing_times:
                CallsProfile.getCallsFromTimetabledPassingTimes(sj, sjp)
                sj.journey_pattern_ref = None
                sj.passing_times = None
                return sj

            elif sj.time_demand_type_ref:
                tdt: TimeDemandType = db_read.get_single(TimeDemandType, sj.time_demand_type_ref.ref)
                CallsProfile.getCallsFromTimeDemandType(sj, sjp, tdt)
                sj.journey_pattern_ref = None
                sj.time_demand_type_ref = None
                return sj

            else:
                log_all(logging.ERROR,"Unimplemented method to calls")

def calendars_to_daytype(db_read: Database, sj: ServiceJourney) -> DayTypeRefsRelStructure | ValidityConditionsRelStructure:
    vcs: set[str] = set()
    vcs2: list[AvailabilityCondition|AvailabilityConditionRef] = []

    # We take validity conditions as priority
    if sj.validity_conditions_or_valid_between is not None and len(sj.validity_conditions_or_valid_between) > 0:
        for vc in sj.validity_conditions_or_valid_between:
            if isinstance(vc, ValidityConditionsRelStructure):
                for vci in vc.choice:
                    if isinstance(vci, AvailabilityCondition):
                        if vci.id not in vcs:
                            vcs2.append(vci)
                        vcs.add(vci.id)
                    elif isinstance(vci, AvailabilityConditionRef):
                        if vci.ref not in vcs:
                            vcs2.append(vci)
                        vcs.add(vci.ref)
                    else:
                        log_once(logging.WARN, "vc-3","Unimplemented other validity condition on ServiceJourney")  # TODO: Only report once

        vcsl = list(sorted(vcs))

        if len(vcsl) == 0:
            log_once(logging.WARN, "ac-2",f"No availability conditions at all for {sj.id}")

        elif len(vcsl) > 1:
            ref = hashlib.md5((';'.join(vcsl)).encode('utf-8')).hexdigest()[0:5]
            sj.day_types = DayTypeRefsRelStructure(day_type_ref=[DayTypeRef(ref=ref, version=sj.version)])

        else:
            sj.day_types = DayTypeRefsRelStructure(day_type_ref=[DayTypeRef(ref=vcsl[0].replace(":AvailabilityCondition:", ":DayType:"), version=sj.version)])

        sj.validity_conditions_or_valid_between = None
        return ValidityConditionsRelStructure(choice=vcs2)

    elif sj.day_types is not None:
        if len(sj.day_types.day_type_ref) > 1:
            ref = hashlib.md5((';'.join([dt.ref for dt in sj.day_types.day_type_ref])).encode('utf-8')).hexdigest()[0:5]
            sj.day_types.day_type_ref = [DayTypeRef(ref=ref, version=sj.version)]
        # else:
        # This would be a no op, the DayType can be reused.

        return sj.day_types

import copy

def gtfs_day_type(day_type: DayType, day_type_assignments: list[DayTypeAssignment], operating_days: list[OperatingDay], uic_operating_periods: list[UicOperatingPeriod], operating_periods: list[OperatingPeriod]) -> tuple[DayType, list[DayTypeAssignment], OperatingPeriod]:
    # The GTFS day type consists of a
    # DayType (having properties of day)
    # DayTypeAssignment (Directly assigning XmlDates)

    # TODO: The when the input has multiple operating_periods, this directly means we either need to:
    #  - split the service_id (we don't want that)
    #  - apply negative dates for non-overlapping periods

    # TODO: reduce the enormous complexity and duplications in the function below

    if len(uic_operating_periods) > 1 or len(operating_periods) > 1:
        log_once(logging.WARN, "op","We can't handle multiple operating periods yet, only considering the first!")

    my_day_type_assignments = []
    my_operating_period = None
    operating_days = getIndex(operating_days)
    uic_operating_periods = getIndex(uic_operating_periods)
    operating_periods = getIndex(operating_periods)
    pod = None

    if day_type.properties is not None and len(day_type.properties.property_of_day) > 0:
        # If the properties are set, this means we would be able to make a valid calendars.txt entry we are now going to search for either an OperatingPeriod or UicOperatingPeriod
        pod = day_type.properties.property_of_day[0].days_of_week
        if DayOfWeekEnumeration.EVERYDAY not in pod:
            pod = [DayOfWeekEnumeration.MONDAY, DayOfWeekEnumeration.TUESDAY, DayOfWeekEnumeration.WEDNESDAY, DayOfWeekEnumeration.THURSDAY, DayOfWeekEnumeration.FRIDAY, DayOfWeekEnumeration.SATURDAY, DayOfWeekEnumeration.SUNDAY]

        # TODO: Implement the handling of multiple periods
        for dta in day_type_assignments:
            if dta.is_available != False:
                if isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date,
                              UicOperatingPeriodRef) or (
                        isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date,
                                   OperatingPeriodRef) or dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.name_of_ref_class == 'UicOperatingPeriod'):
                    my_operating_period = project(uic_operating_periods[dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.ref], OperatingPeriod)
                    dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date = getRef(my_operating_period)
                    my_day_type_assignments.append(dta)
                    break

                elif isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, OperatingPeriodRef):
                    my_operating_period = operating_periods[dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.ref]
                    my_day_type_assignments.append(dta)
                    break

        if my_operating_period is None:
            # We cannot handle a period, without a period...
            pod = None

    if pod is None:
        # Only intereted in the positive days
        for dta in day_type_assignments:
            if dta.is_available in (None, True):
                if isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, UicOperatingPeriodRef) or (
                        isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, OperatingPeriodRef) or dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.name_of_ref_class == 'UicOperatingPeriod'):
                    uic_operating_period = uic_operating_periods[
                        dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.ref]
                    for dt in NordicProfile.getOperationalDates(uic_operating_period):
                        dtai = copy.deepcopy(dta)
                        dtai.id += '_' + str(dt).replace('-', '')
                        dtai.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date = XmlDate.from_date(dt)
                        my_day_type_assignments.append(dtai)

                elif isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, OperatingDayRef):
                    operating_day: OperatingDay = operating_days[dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.ref]
                    dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date = XmlDate.from_date(operating_day.calendar_date)
                    my_day_type_assignments.append(dta)

                elif isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, XmlDate):
                    my_day_type_assignments.append(dta)

    else:
        for dta in day_type_assignments:
            if dta.is_available in (None, True):
                if isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date,
                              UicOperatingPeriodRef) or (
                        isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date,
                                   OperatingPeriodRef) and dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.name_of_ref_class == 'UicOperatingPeriod'):

                    uic_operating_period = uic_operating_periods[
                        dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.ref]

                    for i in range(0, len(uic_operating_period.valid_day_bits)):
                        dt = (uic_operating_period.from_operating_day_ref_or_from_date.to_datetime() + timedelta(
                            days=i)).date()

                        if uic_operating_period.valid_day_bits[i] == '1':
                            if datetime_weekday_to_dow(dt.weekday()) not in pod:
                                # Extra date
                                dta = copy.deepcopy(dta)
                                dta.id += '_' + str(dt.date()).replace('-', '')
                                dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date = XmlDate.from_date(dt.date())
                                my_day_type_assignments.append(dta)

                        elif pod is not None and uic_operating_period.valid_day_bits[i] == '0':
                            if datetime_weekday_to_dow(dt.weekday()) in pod:
                                # Removed date
                                dta = copy.deepcopy(dta)
                                dta.id += '_' + str(dt.date()).replace('-', '')
                                dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date = XmlDate.from_date(dt.date())
                                dta.is_available = False
                                my_day_type_assignments.append(dta)

                elif isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, OperatingDayRef):
                    operating_day: OperatingDay = operating_days[dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.ref]
                    dt = operating_day.calendar_date.date()
                    if datetime_weekday_to_dow(dt.weekday()) not in pod:
                        dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date = XmlDate.from_date(operating_day.calendar_date)
                        my_day_type_assignments.append(dta)

                elif isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, XmlDate):
                    my_day_type_assignments.append(dta)

            else:
                if isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date,
                              UicOperatingPeriodRef) or (
                        isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date,
                                   OperatingPeriodRef) or dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.name_of_ref_class == 'UicOperatingPeriod'):
                    uic_operating_period = uic_operating_periods[
                        dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date]

                    for i in range(0, len(uic_operating_period.valid_day_bits)):
                        dt = (uic_operating_period.from_operating_day_ref_or_from_date.to_datetime() + timedelta(
                            days=i)).date()

                        if uic_operating_period.valid_day_bits[i] == '1':
                            if datetime_weekday_to_dow(dt.weekday()) in pod:
                                # Removed date
                                dta = copy.deepcopy(dta)
                                dta.id += '_' + str(dt.date()).replace('-', '')
                                dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date = XmlDate.from_date(dt.date())
                                my_day_type_assignments.append(dta)

                        # elif pod is not None and uic_operating_period.valid_day_bits[i] == '0':
                        # In my understanding adding dates here would totally not make sense

                elif isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, OperatingDayRef):
                    operating_day: OperatingDay = operating_days[
                        dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.ref]
                    dt = operating_day.calendar_date.date()
                    if datetime_weekday_to_dow(dt.weekday()) in pod:
                        dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date = XmlDate.from_date(operating_day.calendar_date)
                        my_day_type_assignments.append(dta)

                elif isinstance(dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, XmlDate):
                    my_day_type_assignments.append(dta)

    return (day_type, my_day_type_assignments, my_operating_period)


def gtfs_generate_deprecated_version(db_write: Database) -> Version:
    my_from = None
    my_to = None
    last_op = None
    for op in load_generator(db_write, OperatingPeriod):
        op: OperatingPeriod
        dt = op.from_operating_day_ref_or_from_date.to_datetime().date()
        if my_from is None or my_from > dt:
            my_from = dt
            last_op = op

        dt = op.to_operating_day_ref_or_to_date.to_datetime().date()
        if my_to is None or my_to < dt:
            my_to = dt
            last_op = op

    for dta in load_generator(db_write, DayTypeAssignment):
        xml_date = dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date
        if isinstance(xml_date, XmlDate):
            dt = xml_date.to_date()
            if my_from is None or my_from > dt:
                my_from = dt
                last_op = dta

            if my_to is None or my_to < dt:
                my_to = dt
                last_op = dta

    if last_op is not None:
        version: Version = project(last_op, Version)
        version.start_date = XmlDateTime.from_datetime(datetime.combine(my_from, time.min))
        version.end_date = XmlDateTime.from_datetime(datetime.combine(my_to, time.min))
        db_write.insert_one_object(version)
    else:
        warnings.warn("No calendars at all?")

def gtfs_sj_processing(db_read: Database, db_write: Database):
    calendar_combinations = set()
    all_day_type_assignments = set()
    all_operating_days = set()
    all_uic_operating_period = set()

    def query_sj(db_read: Database) -> Iterator[ServiceJourney]:
        _load_generator = load_generator(db_read, ServiceJourney)
        sj: ServiceJourney
        for sj in _load_generator:
            add_calls(db_read, sj)
            calendar_combinations.add(calendars_to_daytype(db_read, sj))
            sj.route_ref = None # TODO: #112
            yield sj

            """
            dts = load_local(db_read, DayType, embedding=True)
            dta = load_local(db_read, DayTypeAssignment, embedding=True)
            ops = load_local(db_read, OperatingPeriod, embedding=True)
            uic_ops = load_local(db_read, UicOperatingPeriod, embedding=True)

            # N AvailabilityCondition
            avc = load_local(db_read, AvailabilityCondition, embedding=True)
            """

    log_all(logging.INFO, "Processing ServiceJourneys")
    db_write.insert_objects_on_queue(ServiceJourney, query_sj(db_read))

    def query_daytype(db_read, calendar_combinations):
        dtas = getIndexByGroup(load_local(db_read, DayTypeAssignment, cursor=True, embedding=True),'day_type_ref.ref')

        for option in calendar_combinations:
            if isinstance(option, ValidityConditionsRelStructure):
                if len(option.choice) == 1:
                    if isinstance(option.choice[0], AvailabilityConditionRef):
                        availability_condition = load_local(db_read, AvailabilityCondition, limit=1, filter=option.choice[0].ref, cursor=True, embedding=True)[0]
                    elif isinstance(option.choice[0], AvailabilityCondition):
                        availability_condition = option.choice[0]
                    else:
                        availability_condition = None
                        log_once(logging.WARN, "vc-2","We cannot yet handle other validity conditions")

                    day_type, day_type_assignments, operating_days, uic_operating_period = get_day_type_from_availability_condition(db_read, availability_condition)
                    day_type, day_type_assignments, operating_period = gtfs_day_type(day_type, day_type_assignments, operating_days, [uic_operating_period], [])
                    if operating_period is not None:
                        operating_period = [operating_period]

                    yield day_type, day_type_assignments, operating_period
                else:
                    # TODO
                    log_once(logging.WARN, "ac","We cannot yet handle availability condition aggregation")

            elif isinstance(option, DayTypeRefsRelStructure):
                if len(option.day_type_ref) == 1:
                    day_type = load_local(db_read, DayType, limit=1, filter=option.day_type_ref[0].ref, embedding=True)[0]
                    day_type_assignments = dtas[day_type.id]
                    uic_operating_periods = []
                    operating_periods = []
                    operating_days = []
                    for dta in day_type_assignments:
                        ref = dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date

                        # TODO: Fix this kind of pattern by abstracting the reference fetching
                        if isinstance(ref, UicOperatingPeriodRef) or (isinstance(ref, OperatingPeriodRef) and ref.name_of_ref_class == 'UicOperatingPeriod'):
                            uic_operating_periods.append(load_local(db_read, UicOperatingPeriod, limit=1, filter=ref.ref, cursor=True, embedding=True)[0])
                        elif isinstance(ref, OperatingPeriodRef):
                            r = load_local(db_read, OperatingPeriod, limit=1, filter=ref.ref, cursor=True, embedding=True)
                            if len(r) > 0:
                                operating_periods.append(r[0])
                            else:
                                # TODO: we must be able to query child classes directly.
                                r = load_local(db_read, UicOperatingPeriod, limit=1, filter=ref.ref, cursor=True, embedding=True)
                                if len(r) > 0:
                                    uic_operating_periods.append(r[0])
                                else:
                                    log_all(logging.ERROR, f"{ref} cannot be found.")
                        elif isinstance(ref, OperatingDayRef):
                            operating_days.append(load_local(db_read, OperatingDay, limit=1, filter=ref.ref, cursor=True, embedding=True)[0])

                    if len(uic_operating_periods) > 0 or len(operating_days) > 0:
                        day_type, day_type_assignments, operating_period = gtfs_day_type(day_type, day_type_assignments, operating_days, uic_operating_periods, operating_periods)
                        if operating_period is not None:
                            operating_period = [operating_period]

                        yield day_type, day_type_assignments, operating_period
                    else:
                        yield day_type, day_type_assignments, operating_periods

                else:
                    # TODO
                    log_once(logging.WARN, "dt-1", "We cannot yet handle day type aggregation")

    log_all(logging.INFO, "Processing Calendars")
    for day_type, day_type_assignments, operating_periods in query_daytype(db_read, calendar_combinations):
        db_write.insert_one_object(day_type)
        db_write.insert_objects_on_queue(DayTypeAssignment, day_type_assignments)
        if operating_periods is not None and len(operating_periods) > 0:
            db_write.insert_objects_on_queue(OperatingPeriod, operating_periods)

    # db_write.block_until_done()

def gtfs_calls_generator(db_read: Database, db_write: Database, generator_defaults: dict):
    def query_sj(db_read: Database) -> Generator:
        _load_generator = load_generator(db_read, ServiceJourney)
        sj: ServiceJourney
        for sj in _load_generator:
            if sj.calls:
                yield sj
            else:
                sjp: ServiceJourneyPattern = db_read.get_single(ServiceJourneyPattern, sj.journey_pattern_ref.ref)
                if sjp is None:
                    log_all(logging.ERROR,"No SJP")

                else:
                    # TODO: very costly to do it here for every ServiceJourneyPattern
                    if sj.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view is None:
                        if sjp.route_ref_or_route_view is not None:
                            if isinstance(sjp.route_ref_or_route_view, RouteView):
                                sj.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view = sjp.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view
                            elif isinstance(sjp.route_ref_or_route_view, RouteRef):
                                sj.route_ref = sjp.route_ref_or_route_view
                                route: Route = db_read.get_single(Route, sjp.route_ref_or_route_view.ref)
                                sj.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view = route.line_ref

                    if sj.passing_times:
                        CallsProfile.getCallsFromTimetabledPassingTimes(sj, sjp)
                        yield sj

                    elif sj.time_demand_type_ref:
                        tdt: TimeDemandType = db_read.get_single(TimeDemandType, sj.time_demand_type_ref.ref)
                        CallsProfile.getCallsFromTimeDemandType(sj, sjp, tdt)
                        yield sj

                    else:
                        log_all(logging.ERROR,"Unimplemented method to calls")


    db_write.insert_objects_on_queue(ServiceJourney, query_sj(db_read))

    def query_tsj(db_read: Database) -> Generator:
        _load_generator = load_generator(db_read, TemplateServiceJourney)
        tsj: TemplateServiceJourney
        for tsj in _load_generator:
            if tsj.calls:
                pass
            else:
                sjp: ServiceJourneyPattern = db_read.get_single(ServiceJourneyPattern, tsj.journey_pattern_ref.ref)
                if sjp is None:
                    log_all(logging.ERROR,"No SJP")

                elif tsj.passing_times:
                    CallsProfile.getCallsFromTimetabledPassingTimes(tsj, sjp)
                    yield tsj

                elif tsj.time_demand_type_ref:
                    tdt: TimeDemandType = db_read.get_single(TimeDemandType, tsj.time_demand_type_ref.ref)
                    CallsProfile.getCallsFromTimeDemandType(tsj, sjp, tdt)
                    yield tsj

                else:
                    log_all(logging.ERROR,"Unimplemented method to calls")


    db_write.insert_objects_on_queue(TemplateServiceJourney, query_tsj(db_read))

def day_type_assignment_to_ac(day_type_assignment: DayTypeAssignment, day_type: DayType, ops: dict[str, OperatingPeriod | UicOperatingPeriod], ods: dict[str, OperatingDay]):
    od: OperatingDay
    ac: AvailabilityCondition = project(day_type_assignment, AvailabilityCondition)
    ac.is_available = day_type_assignment.is_available
    ac.day_types = DayTypesRelStructure(day_type_ref_or_day_type=[getRef(day_type)])

    # Most specific class first
    if isinstance(day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, UicOperatingPeriodRef) or (isinstance(day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, OperatingPeriodRef) or day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.name_of_ref_class == 'UicOperatingPeriod'):
        uic_operating_period_ref = day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date
        uic_operating_period: UicOperatingPeriod = ops[uic_operating_period_ref.ref]

        if isinstance(uic_operating_period.from_operating_day_ref_or_from_date, OperatingDayRefStructure):
            od = ods[uic_operating_period.from_operating_day_ref_or_from_date.ref]
            ac.from_date = od.calendar_date.to_datetime()
        else:
            ac.from_date = uic_operating_period.from_operating_day_ref_or_from_date

        if isinstance(uic_operating_period.to_operating_day_ref_or_to_date, OperatingDayRefStructure):
            od = ods[uic_operating_period.to_operating_day_ref_or_to_date.ref]
            ac.to_date = od.calendar_date.to_datetime()
        else:
            ac.to_date = uic_operating_period.to_operating_day_ref_or_to_date

        ac.valid_day_bits = uic_operating_period.valid_day_bits

        if len(uic_operating_period.days_of_week) > 0:
            day_type_derived: DayType = project(day_type, DayType)
            day_type_derived.id = ac.id.replace(':AvailabilityCondition:', ':DayType:') + '_1'
            day_type_derived.properties = PropertiesOfDayRelStructure(property_of_day=[PropertyOfDay(days_of_week=uic_operating_period.days_of_week)])

            ac.day_types = DayTypesRelStructure(day_type_ref_or_day_type=[day_type_derived])

    elif isinstance(day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, OperatingPeriodRef):
        operating_period_ref = day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date
        operating_period: OperatingPeriod = ops[operating_period_ref.ref]

        if isinstance(operating_period.from_operating_day_ref_or_from_date, OperatingDayRefStructure):
            od = ods[operating_period.from_operating_day_ref_or_from_date.ref]
            ac.from_date = od.calendar_date.to_datetime()
        else:
            ac.from_date = operating_period.from_operating_day_ref_or_from_date

        if isinstance(operating_period.to_operating_day_ref_or_to_date, OperatingDayRefStructure):
            od = ods[operating_period.to_operating_day_ref_or_to_date.ref]
            ac.to_date = od.calendar_date.to_datetime()
        else:
            ac.to_date = operating_period.to_operating_day_ref_or_to_date

    elif isinstance(day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, OperatingDayRef):
        operating_day_ref = day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date
        ac.operating_days = OperatingDaysRelStructure(operating_day_ref_or_operating_day=[ods[operating_day_ref.ref]])

    elif isinstance(day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, XmlDate):
        xml_date = day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date
        ac.operating_days = OperatingDaysRelStructure(operating_day_ref_or_operating_day=[OperatingDay(id=day_type.id.replace('DayType', 'OperatingDay') + '_' + str(xml_date), calendar_date=xml_date)])

    return ac

def service_calendar_to_availability_conditions(service_calendar: ServiceCalendar) -> Generator[AvailabilityCondition, None, None]:
    dtas = getIndexByGroup(service_calendar.day_type_assignments.day_type_assignment, 'day_type_ref') if service_calendar.day_type_assignments else {}
    ops = getIndex(service_calendar.operating_periods.uic_operating_period_ref_or_operating_period_ref_or_operating_period_or_uic_operating_period) if service_calendar.operating_periods else {} # TODO: This does not differentiate
    ods = getIndex(service_calendar.operating_days.operating_day_ref_or_operating_day) if service_calendar.operating_days else {}

    for day_type in service_calendar.day_types.day_type_ref_or_day_type:
        day_type_ref = getRef(day_type) if isinstance(day_type, DayType) else day_type

        for dta in dtas.get(day_type_ref, []):
            ac = day_type_assignment_to_ac(dta, day_type, ops, ods)
            yield ac, day_type_ref

def apply_availability_conditions_via_day_type_ref(db_read: Database, db_write: Database):
    def query_sc(mapping: dict):
        for service_calendar in load_generator(db_read, ServiceCalendar):
            for ac, day_type_ref in service_calendar_to_availability_conditions(service_calendar):
                if day_type_ref in mapping:
                    mapping[day_type_ref].append(getRef(ac))
                else:
                    mapping[day_type_ref] = [getRef(ac)]

                yield ac

    def query_sj(db_read: Database, mapping: dict, clazz: T) -> Generator:
        for service_journey in load_generator(db_read, clazz):
            service_journey: clazz
            if service_journey.day_types:
                for day_type_ref in service_journey.day_types.day_type_ref:
                    if day_type_ref in mapping:
                        if service_journey.validity_conditions_or_valid_between is None:
                            service_journey.validity_conditions_or_valid_between = ValidityConditionsRelStructure(choice=mapping[day_type_ref])
                        elif isinstance(service_journey.validity_conditions_or_valid_between, ValidityConditionsRelStructure):
                            service_journey.validity_conditions_or_valid_between.choice += mapping[day_type_ref]
            yield service_journey



    #with Database("/tmp/fake-mem.duckdb", read_only=False) as db_mem:
    mapping = {}

    copy_table(db_read, db_write, [AvailabilityCondition])
    db_write.insert_objects_on_queue(AvailabilityCondition, query_sc(mapping))
    # The point is here that we now "normalise" everything to availability conditions, but this isn't the target format yet.

    db_write.insert_objects_on_queue(DayType, load_generator(db_read, DayType))
    db_write.insert_objects_on_queue(ServiceJourney, query_sj(db_read, mapping, ServiceJourney))
    db_write.insert_objects_on_queue(TemplateServiceJourney, query_sj(db_read, mapping, TemplateServiceJourney))

def gtfs_calendar2(service_id: str, day_type: DayType, operating_period: OperatingPeriod) -> Iterable[tuple[dict[str, Any] | None, dict[str, Any]] | None]:
    yield tuple(({'service_id': service_id,
                  'monday': int(DayOfWeekEnumeration.MONDAY in day_type.properties.property_of_day[0].days_of_week),
                  'tuesday': int(DayOfWeekEnumeration.TUESDAY in day_type.properties.property_of_day[0].days_of_week),
                  'wednesday': int(DayOfWeekEnumeration.WEDNESDAY in day_type.properties.property_of_day[0].days_of_week),
                  'thursday': int(DayOfWeekEnumeration.THURSDAY in day_type.properties.property_of_day[0].days_of_week),
                  'friday': int(DayOfWeekEnumeration.FRIDAY in day_type.properties.property_of_day[0].days_of_week),
                  'saturday': int(DayOfWeekEnumeration.SATURDAY in day_type.properties.property_of_day[0].days_of_week),
                  'sunday': int(DayOfWeekEnumeration.SUNDAY in day_type.properties.property_of_day[0].days_of_week),
                  'start_date': str(
                      operating_period.from_operating_day_ref_or_from_date.to_datetime().date()).replace('-',
                                                                                                             ''),
                  'end_date': str(
                      operating_period.to_operating_day_ref_or_to_date.to_datetime().date()).replace('-', '')},
                 None,))


def gtfs_calendar(service_id: str, uic_operating_period: UicOperatingPeriod):
    yield tuple(({'service_id': service_id,
                  'monday': int(DayOfWeekEnumeration.MONDAY in uic_operating_period.days_of_week),
                  'tuesday': int(DayOfWeekEnumeration.TUESDAY in uic_operating_period.days_of_week),
                  'wednesday': int(DayOfWeekEnumeration.WEDNESDAY in uic_operating_period.days_of_week),
                  'thursday': int(DayOfWeekEnumeration.THURSDAY in uic_operating_period.days_of_week),
                  'friday': int(DayOfWeekEnumeration.FRIDAY in uic_operating_period.days_of_week),
                  'saturday': int(DayOfWeekEnumeration.SATURDAY in uic_operating_period.days_of_week),
                  'sunday': int(DayOfWeekEnumeration.SUNDAY in uic_operating_period.days_of_week),
                  'start_date': str(
                      uic_operating_period.from_operating_day_ref_or_from_date.to_datetime().date()).replace('-',
                                                                                                             ''),
                  'end_date': str(
                      uic_operating_period.to_operating_day_ref_or_to_date.to_datetime().date()).replace('-', '')},
                 None,))

def netex_to_python_weekday(days_of_week: list[DayOfWeekEnumeration]) -> set[int]:
    return set({
        0 if DayOfWeekEnumeration.SUNDAY in days_of_week else None,
        1 if DayOfWeekEnumeration.MONDAY in days_of_week else None,
        2 if DayOfWeekEnumeration.TUESDAY in days_of_week else None,
        3 if DayOfWeekEnumeration.WEDNESDAY in days_of_week else None,
        4 if DayOfWeekEnumeration.THURSDAY in days_of_week else None,
        5 if DayOfWeekEnumeration.FRIDAY in days_of_week else None,
        6 if DayOfWeekEnumeration.SATURDAY in days_of_week else None})

def gtfs_calendar_and_dates2(db_read: Database, day_type: DayType, day_type_assignments: Iterable[DayTypeAssignment]) -> Iterable[tuple[dict[str, Any] | None, dict[str, Any] | None]]:
    if day_type.private_codes:
        service_ids = [private_code.value for private_code in day_type.private_codes.private_code if
                       private_code.type_value == 'service_id']
        service_id = service_ids[0] if len(service_ids) > 0 else day_type.id
    else:
        service_id = day_type.id

    assert service_id is not None, f"{day_type.id}, service_id must not be None"

    for day_type_assignment in day_type_assignments:
        if isinstance(day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, XmlDate):
            yield tuple((None, {'service_id': service_id,
                                'date': str(day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.to_date()).replace('-', ''),
                                'exception_type': 2 if day_type_assignment.is_available is not None and day_type_assignment.is_available == False else 1 },))
        elif isinstance(day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, OperatingPeriodRef):
            operating_period: OperatingPeriod = db_read.get_single(OperatingPeriod, day_type_assignment.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date.ref)
            if operating_period:
                yield from gtfs_calendar2(service_id, day_type, operating_period)

def gtfs_calendar_and_dates(db_read: Database, day_type_ref: DayTypeRef, day_type_assignments: list[DayTypeAssignment]):
    day_type: DayType = db_read.get_single(DayTypeAssignment, day_type_ref.ref)
    day_type_assignments: list[DayTypeAssignment] = list(day_type_assignments)

    if day_type.private_codes:
        service_ids = [private_code.value for private_code in day_type.private_codes.private_code if
                       private_code.type_value == 'service_id']
        service_id = service_ids[0] if len(service_ids) > 0 else day_type.id
    else:
        service_id = day_type.id

    if len(day_type_assignments) == 1 and day_type_assignments[0].is_available:
        # The provided bitstring will be completely materialised
        if isinstance(day_type_assignments[0].uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, UicOperatingPeriodRef):
            uic_operating_period: UicOperatingPeriod = db_read.get_single(UicOperatingPeriod, day_type_assignments[0].uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date)

            if uic_operating_period.days_of_week:
                yield from gtfs_calendar(service_id, uic_operating_period)

                weekdays = netex_to_python_weekday(uic_operating_period.days_of_week)

                # Here we will do an attempt to actually normalise the output
                if uic_operating_period.valid_day_bits:
                    from_date = uic_operating_period.from_operating_day_ref_or_from_date.to_datetime().date()
                    to_date = uic_operating_period.to_operating_day_ref_or_to_date.to_datetime().date()
                    # TODO: Check (from_date - to_date) + 1 == len(uic_operating_period.valid_day_bits)

                    list_by_weekdays = set({y for y in [from_date+timedelta(days=x) for x in range((to_date-from_date).days+1)] if y.weekday() in weekdays})
                    list_by_validdaybits_positive = set({(uic_operating_period.from_operating_day_ref_or_from_date.to_datetime() + timedelta(days=i)).date() for i in range(0, len(uic_operating_period.valid_day_bits)) if uic_operating_period.valid_day_bits[i] == '1'})
                    list_by_validdaybits_negative = set({(uic_operating_period.from_operating_day_ref_or_from_date.to_datetime() + timedelta(days=i)).date() for i in range(0, len(uic_operating_period.valid_day_bits)) if uic_operating_period.valid_day_bits[i] == '0'})

                    only_extra_dates = list_by_validdaybits_positive - list_by_weekdays
                    only_removed_dates = list_by_validdaybits_negative.intersection(list_by_weekdays)

                    for extra_date in only_extra_dates:
                        yield tuple((None, {'service_id': service_id, 'date': str(
                            extra_date.replace('-', '')), 'exception_type': 1},))

                    for removed_date in only_removed_dates:
                        yield tuple((None, {'service_id': service_id, 'date': str(
                            removed_date.replace('-', '')), 'exception_type': 2},))

            elif uic_operating_period.valid_day_bits:
                for i in range(0, len(uic_operating_period.valid_day_bits)):
                    if uic_operating_period.valid_day_bits[i] == '1':
                        yield tuple((None, {'service_id': service_id, 'date': str((uic_operating_period.from_operating_day_ref_or_from_date.to_datetime() +
                                                                                   timedelta(days=i)).date().replace('-', '')),
                                                                                  'exception_type': 1},))

    elif len(day_type_assignments) == 2:
        if day_type_assignments[0].is_available == True:
            positive = day_type_assignments[0]
            negative = day_type_assignments[1] if day_type_assignments[1].is_available == False else None
        else:
            positive = day_type_assignments[1] if day_type_assignments[1].is_available == True else None
            negative = day_type_assignments[0]

        if positive is None or negative is None:
            log_once(logging.WARN,"dt","Two day type assignments, not matching GTFS Profile")
            return

        if isinstance(positive.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, UicOperatingPeriodRef):
            uic_operating_period_positive: UicOperatingPeriod = db_read.get_single(UicOperatingPeriod, positive.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date)
        else:
            uic_operating_period_positive = None

        if isinstance(negative.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date, UicOperatingPeriodRef):
            uic_operating_period_negative: UicOperatingPeriod = db_read.get_single(UicOperatingPeriod, negative.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date)
        else:
            uic_operating_period_negative = None

        if uic_operating_period_positive is None or uic_operating_period_negative is None:
            log_once(logging.WARN,"uic","No dual UicOperatingPeriod found, not matching GTFS Profile")
            return

        if uic_operating_period_positive.days_of_week:
            yield from gtfs_calendar(service_id, uic_operating_period_positive)

            # weekdays = netex_to_python_weekday(uic_operating_period_positive.days_of_week)

            # We won't normalise the output, since it is obviously that the provided wanted to give us this
            if uic_operating_period_positive.valid_day_bits:
                # from_date = uic_operating_period_positive.from_operating_day_ref_or_from_date.to_datetime().date()
                # to_date = uic_operating_period_positive.to_operating_day_ref_or_to_date.to_datetime().date()
                # TODO: Check (from_date - to_date) + 1 == len(uic_operating_period.valid_day_bits)

                list_by_validdaybits_positive = set(
                    {(uic_operating_period_positive.from_operating_day_ref_or_from_date.to_datetime() + timedelta(days=i)).date() for i in
                     range(0, len(uic_operating_period_positive.valid_day_bits)) if uic_operating_period_positive.valid_day_bits[i] == '1'})

                for extra_date in list_by_validdaybits_positive:
                    yield tuple((None, {'service_id': service_id, 'date': str(
                        extra_date.replace('-', '')), 'exception_type': 1},))

            if uic_operating_period_negative.valid_day_bits:
                list_by_validdaybits_negative = set(
                    {(uic_operating_period_negative.from_operating_day_ref_or_from_date.to_datetime() + timedelta(days=i)).date() for i in
                     range(0, len(uic_operating_period_negative.valid_day_bits)) if uic_operating_period_negative.valid_day_bits[i] == '1'}) # 1 because it is a negated!

                for removed_date in list_by_validdaybits_negative:
                    yield tuple((None, {'service_id': service_id, 'date': str(
                        removed_date.replace('-', '')), 'exception_type': 2},))

"""
def gtfs_calendar_generator(db_read: Database, db_write: Database, generator_defaults: dict):
    # This functions purpose is to transform calendars from NeTEx in such way it can be referenced by GTFS.
    # GTFS only has a validity on a Trip, hence the list of calendars is limited by the validity structure
    # referenced from SericeJourney and TemplateServiceJourney.
    # Given that we don't know how the structure looks like, it may be an AvailabilityCondition or a DayType
    # and further references thereof we take the following algorithm:
    #
    # Input:
    # 1. The combination of DayType + AvailabilityCondition(s) will form the unique service_id (extremely complex case where availabilitycondition or validbetween overrides the DayType)
    # 2. All pair ids will be collected, (de)normalisation is not an issue
    #
    # Output for GTFS:
    # 0. Our intermediate representation of calender.txt, calendar_dates.txt are two AvailabilityConditions (inclusive, and exclusive) and a separate one with a DayOfWeek.
    #    a GTFS service_id may be recovered from PrivateCode.
    # 1. If a DayType (DayOfWeek) has been provided, this will form a unique calendar.txt entry (mo-tue-wed-thu-fri-sat-sun)
    # 2. If both inclusive and exclusive are provided, we assume that the positive bits are the only entries that actually matter,
    #    we can warn for ambiguity, with respect to DayType not being coherent or both inclusive as exclusive values for the same date.
    #
    # ServiceCalendar to AvailabilityConditions

    def query_sj(db_read: Database) -> Generator:
        # DayType 1:N DayTypeAssignment 1:1 OperatingPeriodRef
        dts = load_local(db_read, DayType, embedding=True)
        dta = load_local(db_read, DayTypeAssignment, embedding=True)
        ops = load_local(db_read, OperatingPeriod, embedding=True)

        # N AvailabilityCondition
        avc = load_local(db_read, AvailabilityCondition, embedding=True)

        _load_generator = chain(load_generator(db_read, ServiceJourney), load_generator(db_read, TemplateServiceJourney))
        vcs: set[str] = set()
        day_type_refs: set[str] = set()

        sj: ServiceJourney
        for sj in _load_generator:
            if isinstance(sj.validity_conditions_or_valid_between, ValidityConditionsRelStructure):
                for vc in sj.validity_conditions_or_valid_between.choice:
                    if isinstance(vc, AvailabilityCondition):
                        vcs.add(vc.id)
                    elif isinstance(vc, AvailabilityConditionRef):
                        vcs.add(vc.ref)
                    else:
                        log_once(logging.WARN,"validity","Unimplemented other validity condition on ServiceJourney") # TODO: Only report once

            if sj.day_types is not None:
                for day_type_ref in sj.day_types.day_type_ref:
                    day_type_refs.add(day_type_ref.ref)
"""