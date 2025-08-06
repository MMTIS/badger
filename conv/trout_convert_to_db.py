import logging
from decimal import Decimal, ROUND_UP
from typing import Generator
import datetime
from itertools import chain

from xsdata.models.datatype import XmlTime, XmlDateTime, XmlDuration

from netex import Operator, MultilingualString, DataSource, DestinationDisplay, PresentationStructure, Line, \
    PrivateCode, PrivateCodes, OperatorRef, AllVehicleModesOfTransportEnumeration, StopArea, \
    SimplePointVersionStructure, LocationStructure2, TopographicPlaceView, ScheduledStopPoint, PrivateCodeStructure, \
    StopAreaRefsRelStructure, StopAreaRefStructure, PointRefsRelStructure, ScheduledStopPointRef, ServiceJourneyPattern, \
    RouteView, LineRef, ValidBetween, ValidityConditionsRelStructure, ValidityCondition, ValidDuring, \
    TimebandsRelStructure, TimebandVersionedChildStructure, AvailabilityConditionRef, \
    PointsInJourneyPatternRelStructure, StopPointInJourneyPattern, ServiceJourney, ServiceJourneyPatternRef, \
    TypeOfProductCategoryRef, TimetabledPassingTimesRelStructure, TimetabledPassingTime, DestinationDisplayRef, \
    TypeOfProductCategory, AvailabilityCondition, TimeDemandTypeRef, TimeDemandType, JourneyRunTimesRelStructure, \
    JourneyWaitTimesRelStructure, JourneyWaitTime, JourneyRunTime, TimingLinkRef, SiteConnection, \
    TransferDurationStructure, SiteConnectionEndStructure, DefaultConnection, DefaultConnectionEndStructure, \
    StopAreaRef, DefaultInterchange, Connection, ConnectionEndStructure
from netexio.database import Database
from netexio.pickleserializer import MyPickleSerializer
from utils.aux_logging import prepare_logger, log_all

import trout.trout_pb2_grpc
import trout.trout_pb2
from trout.tryeartimetable_pb2 import TYearTimetable, CallFlags

def load_from_file(yeartimetable: str) -> TYearTimetable:
    timetable = TYearTimetable()
    with open(yeartimetable, "rb") as f:
        timetable.ParseFromString(f.read())
    return timetable

def get_optional_string(string) -> str | None:
    if string == '':
        return None
    return string

def get_presentation(color: str, colorText: str) -> PresentationStructure | None:
    if color == '' and colorText == '':
        return None

    return PresentationStructure(colour=color if color == '' else None, text_colour=colorText if colorText == '' else None)

def get_destinationdisplays(tt: TYearTimetable) -> Generator[DestinationDisplay, None, None]:
    for destination_idx in range(0, len(tt.destinations)):
        destination = tt.destinations[destination_idx]
        yield DestinationDisplay(id=str(destination_idx), version=str(tt.exportTimestamp), name=MultilingualString(value=tt.stringPool[destination.destination]),
                                 presentation=get_presentation(tt.stringPool[destination.color], tt.stringPool[destination.colorText])
                                 )

def get_operators(tt: TYearTimetable) -> Generator[Operator, None, None]:
    for operator in tt.operators:
        yield Operator(id=tt.stringPool[operator.id], version=str(tt.exportTimestamp), name=MultilingualString(value=tt.stringPool[operator.name]))

def get_transport_mode(mode) -> AllVehicleModesOfTransportEnumeration:
    match mode:
        case 0:
            return AllVehicleModesOfTransportEnumeration.UNKNOWN
        case 1:
            return AllVehicleModesOfTransportEnumeration.TRAM
        case 2:
            return AllVehicleModesOfTransportEnumeration.METRO
        case 3:
            return AllVehicleModesOfTransportEnumeration.RAIL
        case 4:
            return AllVehicleModesOfTransportEnumeration.BUS
        case 5:
            return AllVehicleModesOfTransportEnumeration.WATER

    return AllVehicleModesOfTransportEnumeration.UNKNOWN

def get_lines(tt: TYearTimetable) -> Generator[Line, None, None]:
    for line in tt.lines:
        yield Line(id=tt.stringPool[line.id], version=str(tt.exportTimestamp),
                   name=MultilingualString(value=tt.stringPool[line.code]),
                   short_name=MultilingualString(value=tt.stringPool[line.code]),
                   # presentation=get_presentation(tt.stringPool[line.color], tt.stringPool[line.colorText]),
                   operator_ref=OperatorRef(ref=tt.stringPool[tt.operators[line.operatorIdx].id], version=str(tt.exportTimestamp)),
                   transport_mode=get_transport_mode(line.mode))

def get_stopareas(tt: TYearTimetable) -> Generator[StopArea, None, None]:
    for stoparea_idx in range(0, len(tt.stopAreas)):
        stoparea = tt.stopAreas[stoparea_idx]
        if stoparea_idx == len(tt.stopAreas) - 1:
            members = [ScheduledStopPointRef(ref=tt.stringPool[stoppoint.id], version=str(tt.exportTimestamp)) for stoppoint
                       in tt.stopPoints[stoparea.stopPointRefFirstIdx:]]
        else:
            stoparea_next = tt.stopAreas[stoparea_idx + 1]
            members = [ScheduledStopPointRef(ref=tt.stringPool[stoppoint.id], version=str(tt.exportTimestamp)) for stoppoint
                       in tt.stopPoints[stoparea.stopPointRefFirstIdx:stoparea_next.stopPointRefFirstIdx]]

        yield StopArea(id=tt.stringPool[stoparea.id], version=str(tt.exportTimestamp), name=MultilingualString(value=tt.stringPool[stoparea.name]),
                       centroid=SimplePointVersionStructure(location=LocationStructure2(longitude=Decimal(stoparea.longitude).quantize(Decimal('.00001'), rounding=ROUND_UP), latitude=Decimal(stoparea.latitude).quantize(Decimal('.00001'), rounding=ROUND_UP))) if stoparea.latitude != 0 and stoparea.longitude != 0 else None,
                       members=PointRefsRelStructure(point_ref_or_infrastructure_point_ref_or_activation_point_ref_or_timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref_or_route_point_ref=members),
                       topographic_place_ref_or_topographic_place_view=TopographicPlaceView(name=MultilingualString(value=tt.stringPool[stoparea.town])) if tt.stringPool[stoparea.town] != '' else None)

def get_scheduledstoppoint(tt: TYearTimetable) -> Generator[ScheduledStopPoint, None, None]:
    for stoppoint in tt.stopPoints:
        yield ScheduledStopPoint(id=tt.stringPool[stoppoint.id], version=str(tt.exportTimestamp),
                       name=MultilingualString(value=tt.stringPool[stoppoint.name]),
                       location=LocationStructure2(longitude=Decimal(stoppoint.longitude).quantize(Decimal('.00001'), rounding=ROUND_UP), latitude=Decimal(stoppoint.latitude).quantize(Decimal('.00001'), rounding=ROUND_UP))  if stoppoint.latitude != 0 and stoppoint.longitude != 0 else None,
                       short_stop_code=PrivateCodeStructure(value=tt.stringPool[stoppoint.platformCode]) if tt.stringPool[stoppoint.platformCode] != '' else None,
                       stop_areas=StopAreaRefsRelStructure(stop_area_ref=[StopAreaRefStructure(ref=tt.stringPool[tt.stopAreas[stoppoint.stopAreaIdx].id], version=str(tt.exportTimestamp))]),
                       topographic_place_ref_or_topographic_place_view=TopographicPlaceView(
                           name=MultilingualString(value=tt.stringPool[stoppoint.town]) if tt.stringPool[stoppoint.town] != '' else None))

def get_xml_time_from_int(seconds: int) -> tuple[XmlTime, int]:
    # Seconds to HH:MM:SS
    hh = seconds // 3600
    mm = (seconds % 3600) // 60
    ss = seconds % 60
    do = hh // 24

    return XmlTime(hh, mm, ss), do

"""
JourneyPattern in trout is actually a StopArea Pattern, does not have a NeTEx equivalent, unless you would make ScheduledStopPoints out of the StopAreas, or suggest that PointRef could also point to StopArea
def get_journeypattern(tt: TYearTimetable) -> Generator[ServiceJourneyPattern, None, None]:
    for journeypattern_idx in range(0, len(tt.journeyPatterns)):
        journeypattern = tt.journeyPatterns[journeypattern_idx]
        if journeypattern_idx == len(tt.journeyPatterns) - 1:
            tt.journeyPatternPoints
        else:
            journeypattern_next = tt.journeypattern[journeypattern_idx + 1]


        start_time, start_time_day_offset = get_xml_time_from_int(journeypattern.startTime)
        end_time, end_time_day_offset = get_xml_time_from_int(journeypattern.endTime)

        yield ServiceJourneyPattern(id=tt.stringPool[journeypattern.id], version=str(tt.exportTimestamp),
                                    validity_conditions_or_valid_between=[ValidityConditionsRelStructure(choice=[AvailabilityConditionRef(ref=journeypattern.validityPatternIdx, version=str(tt.exportTimestamp)), ValidDuring(id=str(journeypattern_idx), version=str(tt.exportTimestamp), timebands=TimebandsRelStructure(timeband_ref_or_timeband=[TimebandVersionedChildStructure(start_time_or_start_event=start_time, end_time_or_end_event_or_day_offset_or_duration=[end_time, end_time_day_offset])]))])],
                                    route_ref_or_route_view=RouteView(flexible_line_ref_or_line_ref_or_line_view=LineRef(ref=tt.stringPool[tt.lines[journeypattern.lineIdx]], version=str(tt.exportTimestamp))),
                                    )
"""

def is_bit_set(value: int, bit_index: int):
    return (value & (1 << (bit_index - 1))) != 0

def get_servicejourneypattern(tt: TYearTimetable) -> Generator[ServiceJourneyPattern, None, None]:
    """We assume the worst and allow for full normalisation between JourneyPattern and StopPattern, but this does not guarantee that unused JourneyPatterns are also exported."""
    done = set([])
    for vehiclejourney in tt.vehicleJourneys:
        my_id = f"{vehiclejourney.journeyPatternIdx}-{vehiclejourney.stopPatternIdx}"
        if my_id in done:
            continue

        done.add(my_id)

        stoppattern = tt.stopPatterns[vehiclejourney.stopPatternIdx]
        journeypattern = tt.journeyPatterns[vehiclejourney.journeyPatternIdx]
        points_in_sequence = [StopPointInJourneyPattern(id=f"{vehiclejourney.journeyPatternIdx}-{vehiclejourney.stopPatternIdx + stoppoint_idx_idx}", version=str(tt.exportTimestamp),
                                                        for_boarding=is_bit_set(tt.journeyPatternPoints[journeypattern.journeyPatternPointFirstIdx + stoppoint_idx_idx].flags, CallFlags.CALL_BOARDING),
                                                        for_alighting=is_bit_set(tt.journeyPatternPoints[journeypattern.journeyPatternPointFirstIdx + stoppoint_idx_idx].flags, CallFlags.CALL_ALIGHTING),
                                                        scheduled_stop_point_ref=ScheduledStopPointRef(ref=tt.stringPool[tt.stopPoints[stoppattern.stopIdx[stoppoint_idx_idx]].id], version=str(tt.exportTimestamp)),
                                                        destination_display_ref_or_destination_display_view=DestinationDisplayRef(ref=str(tt.journeyPatternPoints[journeypattern.journeyPatternPointFirstIdx + stoppoint_idx_idx].destinationIdx), version=str(tt.exportTimestamp))
                                                        ) for stoppoint_idx_idx in range(0, len(stoppattern.stopIdx))]
        yield ServiceJourneyPattern(id=my_id, version=str(tt.exportTimestamp),
                                    points_in_sequence=PointsInJourneyPatternRelStructure(point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern=points_in_sequence))

def get_timedemandtype(tt: TYearTimetable) -> Generator[TimeDemandType, None, None]:
    done = set([])
    for vehiclejourney in tt.vehicleJourneys:
        my_id = f"{vehiclejourney.stopPatternIdx}-{vehiclejourney.stopTimeIdx}"
        if my_id in done:
            continue

        done.add(my_id)

        stoppattern = tt.stopPatterns[vehiclejourney.stopPatternIdx]

        journey_run_time = [JourneyRunTime(id=f"{vehiclejourney.stopPatternIdx + stoppoint_idx_idx}-{vehiclejourney.stopTimeIdx + stoppoint_idx_idx}", version=str(tt.exportTimestamp),
                                           run_time=XmlDuration(value=f"PT{tt.arrivalTimes[vehiclejourney.stopTimeIdx + stoppoint_idx_idx + 1] - tt.departureTimes[vehiclejourney.stopTimeIdx + stoppoint_idx_idx]}S"),
                                           timing_link_ref=TimingLinkRef(ref=f"{stoppattern.stopIdx[stoppoint_idx_idx]}-{stoppattern.stopIdx[stoppoint_idx_idx + 1]}", version=str(tt.exportTimestamp))
                                           ) for stoppoint_idx_idx in range(0, len(stoppattern.stopIdx) - 1)]
        journey_wait_time = [JourneyWaitTime(id=f"{vehiclejourney.stopTimeIdx + stoppoint_idx_idx}", version=str(tt.exportTimestamp),
                                             wait_time=XmlDuration(value=f"PT{tt.departureTimes[vehiclejourney.stopTimeIdx + stoppoint_idx_idx]-tt.arrivalTimes[vehiclejourney.stopTimeIdx + stoppoint_idx_idx]}S"),
                                             timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref=ScheduledStopPointRef(ref=f"{stoppattern.stopIdx[stoppoint_idx_idx]}", version=str(tt.exportTimestamp)),
                                             ) for stoppoint_idx_idx in range(0, len(stoppattern.stopIdx)) if tt.departureTimes[vehiclejourney.stopTimeIdx + stoppoint_idx_idx] > tt.arrivalTimes[vehiclejourney.stopTimeIdx + stoppoint_idx_idx]]

        yield TimeDemandType(id=my_id, version=str(tt.exportTimestamp),
                             run_times=JourneyRunTimesRelStructure(journey_run_time=journey_run_time),
                             wait_times=JourneyWaitTimesRelStructure(journey_wait_time=journey_wait_time) if len(journey_wait_time) > 0 else None)


def get_productcategory(tt: TYearTimetable) -> Generator[TypeOfProductCategory, None, None]:
    for productcategory in tt.productCategories:
        if tt.stringPool[productcategory.code] == '':
            continue
        yield TypeOfProductCategory(id=tt.stringPool[productcategory.code], version=str(tt.exportTimestamp), name=MultilingualString(value=tt.stringPool[productcategory.code]))

def get_vehiclejourney(tt: TYearTimetable) -> Generator[ServiceJourney, None, None]:
    for vehiclejourney_idx in range(0, len(tt.vehicleJourneys)):
        vehiclejourney = tt.vehicleJourneys[vehiclejourney_idx]
        vj_departure_time, vj_departure_day_offset = get_xml_time_from_int(vehiclejourney.departureTime)

        # passing_times = []
        # for idx in range(vehiclejourney.stopTimeIdx, vehiclejourney.stopTimeIdx + len(tt.stopPatterns[vehiclejourney.stopPatternIdx].stopIdx)):
        #     arrival_time, arrival_time_offset = get_xml_time_from_int(vehiclejourney.departureTime + tt.arrivalTimes[idx])
        #     departure_time, departure_time_offset = get_xml_time_from_int(vehiclejourney.departureTime + tt.departureTimes[idx])
        #
        #     passing_time = TimetabledPassingTime(id=str(vehiclejourney_idx), version=str(tt.exportTimestamp),
        #                                          arrival_time=arrival_time, arrival_day_offset=arrival_time_offset if arrival_time_offset > 0 else None,
        #                                          departure_time=departure_time, departure_day_offset=departure_time_offset if departure_time_offset > 0 else None)
        #     passing_times.append(passing_time)

        yield ServiceJourney(id=tt.stringPool[vehiclejourney.id], version=str(tt.exportTimestamp),
                             flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=LineRef(ref=tt.stringPool[tt.lines[tt.journeyPatterns[vehiclejourney.journeyPatternIdx].lineIdx].id], version=str(tt.exportTimestamp)),
                             departure_time=vj_departure_time, departure_day_offset=vj_departure_day_offset if vj_departure_day_offset != 0 else None,
                             private_codes=PrivateCodes(private_code=[PrivateCode(value=str(vehiclejourney.number))]),
                             name=MultilingualString(value=tt.stringPool[vehiclejourney.name]) if tt.stringPool[vehiclejourney.name] != '' else None,
                             time_demand_type_ref=TimeDemandTypeRef(ref=f"{vehiclejourney.stopPatternIdx}-{vehiclejourney.stopTimeIdx}", version=str(tt.exportTimestamp)),
                             # passing_times=TimetabledPassingTimesRelStructure(timetabled_passing_time=passing_times),
                             type_of_product_category_ref=TypeOfProductCategoryRef(ref=tt.productCategories[vehiclejourney.productCategoryIdx].code, version=str(tt.exportTimestamp)) if tt.productCategories[vehiclejourney.productCategoryIdx].code != '' else None,
                             journey_pattern_ref=ServiceJourneyPatternRef(ref=str(vehiclejourney.stopPatternIdx), version=str(tt.exportTimestamp)),
                             validity_conditions_or_valid_between=[ValidityConditionsRelStructure(choice=[
                                 AvailabilityConditionRef(ref=str(vehiclejourney.validityPatternIdx), version=str(tt.exportTimestamp))])],
                             )

def get_connections(tt: TYearTimetable) -> Generator[Connection, None, None]:
    for stoppoint_idx in range(0, len(tt.stopPoints)):
        if stoppoint_idx == len(tt.stopPoints) - 1:
            connections = tt.connections[tt.stopPoints[stoppoint_idx].connectionFirstIdx:]
        else:
            connections = tt.connections[tt.stopPoints[stoppoint_idx].connectionFirstIdx:tt.stopPoints[stoppoint_idx + 1].connectionFirstIdx]

        for connection in connections:
            # yield onnection(id=f"{tt.stringPool[tt.stopPoints[stoppoint_idx].id]}-{tt.stringPool[tt.stopPoints[connection.toStopPointIdx].id]}", version=str(tt.exportTimestamp),
            yield Connection(id=f"SP-{stoppoint_idx}-{connection.toStopPointIdx}", version=str(tt.exportTimestamp),
                                 transfer_duration=TransferDurationStructure(default_duration=XmlDuration(value=f"PT{connection.duration}S")),
                                 walk_transfer_duration=TransferDurationStructure(default_duration=XmlDuration(value=f"PT{connection.walkDuration}S")),
                                 from_value=ConnectionEndStructure(scheduled_stop_point_ref_or_vehicle_meeting_point_ref=ScheduledStopPointRef(ref=tt.stringPool[tt.stopPoints[stoppoint_idx].id], version=str(tt.exportTimestamp))),
                                 to=ConnectionEndStructure(scheduled_stop_point_ref_or_vehicle_meeting_point_ref=ScheduledStopPointRef(ref=tt.stringPool[tt.stopPoints[connection.toStopPointIdx].id],version=str(tt.exportTimestamp))),
                                 )


def get_footpaths(tt: TYearTimetable) -> Generator[SiteConnection, None, None]:
    for stoparea_idx in range(0, len(tt.stopAreas)):
        if stoparea_idx == len(tt.stopAreas) - 1:
            footpaths = tt.footpaths[tt.stopAreas[stoparea_idx].footpathFirstIdx:]
        else:
            footpaths = tt.footpaths[tt.stopAreas[stoparea_idx].footpathFirstIdx:tt.stopAreas[stoparea_idx + 1].footpathFirstIdx]

        for footpath in footpaths:
            # yield SiteConnection(id=f"{tt.stringPool[tt.stopAreas[stoparea_idx].id]}-{tt.stringPool[tt.stopAreas[footpath.toStopAreaIdx].id]}", version=str(tt.exportTimestamp),
            yield SiteConnection(id=f"SA-{stoparea_idx}-{footpath.toStopAreaIdx}", version=str(tt.exportTimestamp),
                                    distance=Decimal(footpath.distance),
                                    from_value=SiteConnectionEndStructure(stop_area_ref=StopAreaRef(ref=tt.stringPool[tt.stopAreas[stoparea_idx].id], version=str(tt.exportTimestamp))),
                                    to=SiteConnectionEndStructure(stop_area_ref=StopAreaRef(ref=tt.stringPool[tt.stopPoints[footpath.toStopAreaIdx].id], version=str(tt.exportTimestamp))),
                                 )


def get_validitypatterns(tt: TYearTimetable) -> Generator[AvailabilityCondition, None, None]:
    dt_from = datetime.datetime.fromtimestamp(tt.validFrom, datetime.UTC)
    dt_thru = datetime.datetime.fromtimestamp(tt.validThru, datetime.UTC)
    from_date = XmlDateTime(dt_from.year, dt_from.month, dt_from.day, 0, 0, 0)
    to_date = XmlDateTime(dt_thru.year, dt_thru.month, dt_thru.day, 0, 0, 0)

    for validitypattern_idx in range(0, len(tt.validityPatterns)):
        validitypattern = tt.validityPatterns[validitypattern_idx]
        valid_day_bits = ''.join(f'{byte:08b}' for byte in validitypattern)

        local_dt_thru = dt_from  + datetime.timedelta(days=len(valid_day_bits) - 1)
        local_to_date = XmlDateTime(local_dt_thru.year, local_dt_thru.month, local_dt_thru.day, 0, 0, 0)

        yield AvailabilityCondition(id=str(validitypattern_idx), version=str(tt.exportTimestamp), from_date=from_date, to_date=local_to_date, valid_day_bits=valid_day_bits)

def main(yeartimetable: str, target_database_file: str) -> None:
    timetable = load_from_file(yeartimetable)

    with Database(target_database_file, serializer=MyPickleSerializer(compression=True), readonly=False) as db_write:
        db_write.insert_objects_on_queue(Operator, get_operators(timetable))
        db_write.insert_objects_on_queue(Line, get_lines(timetable), empty=True)
        db_write.insert_objects_on_queue(StopArea, get_stopareas(timetable))
        db_write.insert_objects_on_queue(ScheduledStopPoint, get_scheduledstoppoint(timetable))
        db_write.insert_objects_on_queue(TimeDemandType, get_timedemandtype(timetable), empty=True)
        db_write.insert_objects_on_queue(ServiceJourney, get_vehiclejourney(timetable), empty=True)

        db_write.insert_objects_on_queue(DestinationDisplay, get_destinationdisplays(timetable), empty=True)
        db_write.insert_objects_on_queue(ServiceJourneyPattern, get_servicejourneypattern(timetable), empty=True)
        db_write.insert_objects_on_queue(TypeOfProductCategory, get_productcategory(timetable), empty=True)
        db_write.insert_objects_on_queue(AvailabilityCondition, get_validitypatterns(timetable), empty=True)
        db_write.insert_objects_on_queue(Connection, get_connections(timetable), empty=True)
        db_write.insert_objects_on_queue(SiteConnection, get_footpaths(timetable), empty=True)


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description='Trout YearTimetable to NeTEx')
    parser.add_argument('yeartimetable', type=str, help='yeartimetable.pb file to import, for example: yeartimetable.pb')
    parser.add_argument(
        "target",
        type=str,
        help="lmdb file to overwrite and store contents of the transformation.",
    )
    parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.yeartimetable, args.target)
    except Exception as e:
        log_all(logging.ERROR, f'{e}  {traceback.format_exc()}')
        raise e
