from datetime import datetime, timedelta
from typing import Generator

from xsdata.models.datatype import XmlDate, XmlDateTime

from callsprofile import CallsProfile
from netex import ServiceJourney, DestinationDisplay, NaturalLanguageStringStructure, MultilingualString, Call, \
    ArrivalStructure, OperatingDay, DepartureStructure, ServiceJourneyPattern, TimeDemandType, LineRef, DirectionRef
from netexio.database import Database
from netexio.dbaccess import load_generator, get_single
from siri import DatedVehicleJourneyStructure, DatedCall, AimedDepartureTime, AimedArrivalTime, Order, StopPointRef, VehicleJourneyRef

# TODO: update for lmdb

def siri_language_string(multilingualstring: MultilingualString):
    return NaturalLanguageStringStructure(value=multilingualstring.value, lang=multilingualstring.lang)

def siri_aimed_arrival_time(arrival: ArrivalStructure, operating_day: OperatingDay, tzinfo) -> AimedArrivalTime:
    return AimedArrivalTime(value=XmlDateTime.from_datetime(
        datetime.combine(operating_day.calendar_date.to_date(), arrival.time.to_time(), tzinfo) + timedelta(
            days=arrival.day_offset or 0)))

def siri_aimed_deparutre_time(departure: DepartureStructure, operating_day: OperatingDay, tzinfo) -> AimedDepartureTime:
    return AimedDepartureTime(value=XmlDateTime.from_datetime(
        datetime.combine(operating_day.calendar_date.to_date(), departure.time.to_time(), tzinfo) + timedelta(
            days=departure.day_offset or 0)))

def siri_dated_call(call: Call, operating_day: OperatingDay, tzinfo) -> DatedCall:
    return DatedCall(aimed_arrival_time=siri_aimed_arrival_time(call.arrival, operating_day, tzinfo) if call.arrival else None,
                     aimed_departure_time=siri_aimed_deparutre_time(call.departure, operating_day, tzinfo)  if call.departure else None,
                     order=Order(value=call.order),
                     stop_point_ref=StopPointRef(value=call.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view.ref),
                     )

def siri_destination_display_generator(db_read: Database) -> Generator[NaturalLanguageStringStructure, None, None]:
    for destination_display in load_generator(db_read, DestinationDisplay):
        destination_display: DestinationDisplay
        yield siri_language_string(destination_display.front_text)

def siri_dated_vehicle_journey_generator(db_read: Database, operating_day: OperatingDay, line_ref: LineRef, direction_ref: DirectionRef, tzinfo) -> Generator[DatedVehicleJourneyStructure, None, None]:
    for service_journey in load_generator(db_read, ServiceJourney):
        # TODO: Implement a filter based on ServiceCalendar / AvailabilityCondition based on the provided operating_day, line_ref, direction_ref

        # TODO: Make more generic, also for GTFS
        if service_journey.calls:
            pass
        elif service_journey.passing_times:
            service_journey_pattern = db_read.get_single(ServiceJourneyPattern, service_journey.journey_pattern_ref.ref, service_journey.journey_pattern_ref.version)
            CallsProfile.getCallsFromTimetabledPassingTimes(service_journey, service_journey_pattern)
        elif service_journey.time_demand_type_ref:
            service_journey_pattern = db_read.get_single(ServiceJourneyPattern, service_journey.journey_pattern_ref.ref, service_journey.journey_pattern_ref.version)
            time_demand_type = db_read.get_single(TimeDemandType, service_journey.time_demand_type_ref.ref, service_journey.time_demand_type_ref.version)
            CallsProfile.getCallsFromTimeDemandType(service_journey, service_journey_pattern, time_demand_type)

        service_journey: ServiceJourney
        if service_journey.calls:
            dated_calls = DatedVehicleJourneyStructure.DatedCalls(dated_call=[siri_dated_call(call, operating_day, tzinfo) for call in service_journey.calls.call])
            yield DatedVehicleJourneyStructure(
                line_ref=service_journey.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view.ref, # TODO: Implement function that actually captures what we want from a choice
                framed_vehicle_journey_ref_or_vehicle_journey_ref=VehicleJourneyRef(value=service_journey.id),
                dated_calls=dated_calls,
                monitored=service_journey.monitored)