from decimal import Decimal
from typing import Generator

import duckdb
from domain.gtfs.transform.datetime import noonTimeToNeTEx
from domain.gtfs.transform.daytype import get_service_id_dt
from domain.gtfs.transform.directiontype import directionToNeTEx
from domain.gtfs.transform.limitationstatus import wheelchairToNeTEx
from domain.gtfs.transform.line import get_route_id
from domain.gtfs.transform.luggagecarriage import bicyclesToNeTEx
from domain.gtfs.transform.string import getOptionalString
from domain.netex.model import (
    Codespace,
    ServiceJourney,
    AccessibilityAssessment,
    ServiceFacilitySet,
    Call,
    JourneyPatternView,
    DestinationDisplayView,
    MultilingualString,
    TextType,
    Block,
    BlockRef,
    ServiceFacilitySetsRelStructure,
    LuggageCarriageFacilityList,
    CallsRelStructure,
    ScheduledStopPoint,
    OnwardServiceLinkView,
    ScheduledStopPointRef,
    ArrivalStructure,
    DepartureStructure,
    Line,
    LineRef,
    PrivateCodes,
    PrivateCode,
    DayTypeRefsRelStructure,
    DayTypeRef,
)
from domain.netex.services.ids import getId
from domain.netex.services.refs import getFakeRef


def get_trip_id(codespace: Codespace, trip_id: str) -> str:
    if ':ServiceJourney:' in trip_id:
        return trip_id
    else:
        return getId(codespace, ServiceJourney, trip_id)


def get_trip_id_aa(codespace: Codespace, trip_id: str) -> str:
    if ':ServiceJourney:' in trip_id:
        return trip_id.replace(':ServiceJourney:', ':AccessibilityAssessment:')
    else:
        return getId(codespace, AccessibilityAssessment, trip_id)


def get_trip_id_sfs(codespace: Codespace, trip_id: str) -> str:
    if ':ServiceJourney:' in trip_id:
        return trip_id.replace(':ServiceJourney:', ':ServiceFacilitySet:')
    else:
        return getId(codespace, ServiceFacilitySet, trip_id)


def get_trip_id_call(codespace: Codespace, trip_id: str, sequence: int) -> str:
    if ':ServiceJourney:' in trip_id:
        return trip_id.replace(':ServiceJourney:', ':Call:') + '_' + str(sequence)
    elif ':TemplateServiceJourney:' in trip_id:
        return trip_id.replace(':TemplateServiceJourney:', ':Call:') + '_' + str(sequence)
    else:
        return getId(codespace, Call, trip_id) + '_' + str(sequence)


def trips_iter(
    con: duckdb.DuckDBPyConnection, batch_size: int = 100_000
) -> Generator[tuple[str, list[tuple[str, str, str, str, str, int, str, str, int, int, str, str, str, str, int, int, int, int]]], None, None]:
    with con.cursor() as cur:
        cursor = cur.execute(
            """
                    SELECT
                    route_id, trip_id, service_id, trip_short_name, trip_headsign, direction_id, block_id, shape_id, wheelchair_accessible, bikes_allowed,
                    stop_headsign, stop_id, arrival_time, departure_time, shape_dist_traveled, drop_off_type, pickup_type, stop_sequence
                    FROM trip_times
                """
        )

        current_trip: str | None = None
        current_block: list[tuple[str, str, str, str, str, int, str, str, int, int, str, str, str, str, int, int, int, int]] = []

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                if current_trip is not None:
                    yield current_trip, current_block
                break

            for row in rows:
                trip_id = row[1]
                if trip_id != current_trip:
                    if current_trip is not None:
                        yield current_trip, current_block
                    current_trip = trip_id
                    current_block = []
                current_block.append(row)


def getServiceJourneys(con: duckdb.DuckDBPyConnection, codespace: Codespace, version: str) -> Generator[ServiceJourney, None, None]:
    # shape_used = set([])

    with con.cursor() as cur:
        # This query does fill up the disk, but also prevents that a long running query eats all the memory in the Python Cache of DuckDB...
        cur.execute(
            "CREATE TABLE IF NOT EXISTS trip_times AS SELECT * FROM trips JOIN stop_times USING (trip_id) WHERE trip_id NOT IN (SELECT trip_id FROM frequencies) ORDER BY trip_id, stop_sequence;"
        )

        for trip_id, stops in trips_iter(cur):
            (
                route_id,
                trip_id,
                service_id,
                trip_short_name,
                trip_headsign,
                direction_id,
                block_id,
                shape_id,
                wheelchair_accessible,
                bikes_allowed,
                stop_headsign,
                stop_id,
                arrival_time,
                departure_time,
                shape_dist_traveled,
                drop_off_type,
                pickup_type,
                stop_sequence,
            ) = stops[0]

            journey_pattern_view = (
                JourneyPatternView(
                    destination_display_ref_or_destination_display_view=DestinationDisplayView(
                        name=MultilingualString(content=[TextType(value=trip_headsign)]), front_text=MultilingualString(content=[TextType(value=trip_headsign)])
                    )
                )
                if trip_headsign
                else None
            )

            accessibility_assessment = (
                AccessibilityAssessment(
                    id=get_trip_id_aa(codespace, trip_id), version=version, mobility_impaired_access=wheelchairToNeTEx(wheelchair_accessible)
                )
                if wheelchair_accessible
                else None
            )

            block_ref = getFakeRef(getId(codespace, Block, block_id), BlockRef, None, "EXTERNAL") if block_id else None

            luggage_carriage_facility_list = [bicyclesToNeTEx(bikes_allowed)]

            facilities = None
            if len(luggage_carriage_facility_list) > 0:
                facilities = ServiceFacilitySetsRelStructure(
                    restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
                        ServiceFacilitySet(
                            id=get_trip_id_sfs(codespace, trip_id),
                            version=version,
                            luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
                        )
                    ]
                )

            calls = CallsRelStructure()

            prev_call = None
            prev_shape_traveled = 0
            prev_order = 1

            for (
                route_id,
                trip_id,
                service_id,
                trip_short_name,
                trip_headsign,
                direction_id,
                block_id,
                shape_id,
                wheelchair_accessible,
                bikes_allowed,
                stop_headsign,
                stop_id,
                arrival_time,
                departure_time,
                shape_dist_traveled,
                drop_off_type,
                pickup_type,
                stop_sequence,
            ) in stops:
                destination_display_view = (
                    DestinationDisplayView(
                        name=MultilingualString(content=[TextType(value=stop_headsign)]), front_text=MultilingualString(content=[TextType(value=stop_headsign)])
                    )
                    if stop_headsign
                    else None
                )

                from_point_ref = getId(codespace, ScheduledStopPoint, stop_id)
                arrival_time_xml, arrival_dayoffset = noonTimeToNeTEx(arrival_time)
                departure_time_xml, departure_dayoffset = noonTimeToNeTEx(departure_time)

                if prev_call and shape_dist_traveled is not None:
                    distance = shape_dist_traveled - prev_shape_traveled
                    prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=Decimal(distance))
                pickup = pickup_type if pickup_type else 0
                drop_off = drop_off_type if drop_off_type else 0
                call = Call(
                    id=get_trip_id_call(codespace, trip_id, stop_sequence),
                    version=version,
                    fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
                        from_point_ref, ScheduledStopPointRef, version
                    ),
                    destination_display_ref_or_destination_display_view=destination_display_view,
                    arrival=ArrivalStructure(time=arrival_time_xml, day_offset=arrival_dayoffset, for_alighting=bool(drop_off != 1)),
                    departure=DepartureStructure(time=departure_time_xml, day_offset=departure_dayoffset, for_boarding=bool(pickup != 1)),
                    request_stop=bool(pickup == 2 or pickup == 3 or drop_off == 2 or drop_off == 3),
                    order=prev_order,
                )  # stop_sequence is non-negative integer

                calls.call.append(call)

                prev_call = call
                if shape_dist_traveled:
                    prev_shape_traveled = shape_dist_traveled
                prev_order += 1

            service_journey = ServiceJourney(
                id=get_trip_id(codespace, trip_id),
                version=version,
                flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(getId(codespace, Line, route_id), LineRef, version),
                private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_id, type_value="trip_id")]),
                short_name=getOptionalString(trip_short_name),
                day_types=DayTypeRefsRelStructure(day_type_ref=[getFakeRef(get_service_id_dt(codespace, service_id), DayTypeRef, version)]),
                journey_pattern_view=journey_pattern_view,
                direction_type=directionToNeTEx(direction_id),
                block_ref=block_ref,
                accessibility_assessment=accessibility_assessment,
                facilities=facilities,
                # link_sequence_projection_ref_or_link_sequence_projection=lsp,
                calls=calls,
            )

            yield service_journey

        cur.execute("DROP TABLE trip_times;")

        # route_ref = None
        # lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
        # shape_id = get_or_none(shape_ids, i)
        # if shape_id is not None:
        #     if shape_id in shape_used:
        #         lsp = getFakeRef(getId(LinkSequenceProjection, self.codespace, shape_id), LinkSequenceProjectionRef, self.version.version)
        #     else:
        #         lsps = self.getLineStrings(
        #             {
        #                 'query': (
        #                     """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
        #                 ),
        #                 'parameters': (shape_id,),
        #             }
        #         )
        #         if len(lsps) > 0:
        #             lsp = lsps[0]
        #
        #        shape_used.add(shape_id)
