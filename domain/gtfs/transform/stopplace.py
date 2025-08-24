from decimal import Decimal
from typing import cast, Union

import duckdb

from domain.gtfs.transform.limitationstatus import wheelchairToNeTEx
from domain.gtfs.transform.string import getRequiredString, getOptionalString
from domain.netex.model import (
    Codespace,
    StopPlace,
    PublicCodeStructure,
    PrivateCodes,
    PrivateCode,
    Locale,
    ZoneRefStructure,
    AccessibilityAssessment,
    InfoLinksRelStructure,
    InfoLink,
    TypeOfInfoLinkEnumeration,
    SimplePointVersionStructure,
    LocationStructure2,
    PassengerStopAssignment,
    QuaysRelStructure,
    LevelRef,
    ScheduledStopPointRef,
    ScheduledStopPoint,
    TaxiStandRef,
    QuayRef,
    Quay,
    SiteEntrancesRelStructure,
    StopPlaceEntrance,
    AccessSpacesRelStructure,
    AccessSpace,
)
from domain.netex.services.ids import getId
from domain.netex.services.refs import getFakeRef, getRef


def getStopPlaces(con: duckdb.DuckDBPyConnection, codespace: Codespace, version: str) -> tuple[list[StopPlace], list[PassengerStopAssignment]]:
    stop_places_sql = """SELECT DISTINCT stop_id, stop_name, stop_lat, stop_lon, stop_code, stop_desc, zone_id, stop_url, location_type, parent_station, wheelchair_boarding, stop_timezone, platform_code, level_id FROM stops ORDER BY COALESCE(parent_station, '') ASC, stop_id;"""
    stop_places: dict[str, StopPlace] = {}
    passenger_stop_assignments: list[PassengerStopAssignment] = []

    with con.cursor() as cur:
        cur.execute(stop_places_sql)

        while True:
            row = cur.fetchone()
            if row is None:
                break

            (
                stop_id,
                stop_name,
                stop_lat,
                stop_lon,
                stop_code,
                stop_desc,
                zone_id,
                stop_url,
                location_type,
                parent_station,
                wheelchair_boarding,
                stop_timezone,
                platform_code,
                level_id,
            ) = row

            # Every stop that does not have a parent_station, will become a StopPlace
            if parent_station is None:
                stop_place = StopPlace(
                    id=getId(codespace, StopPlace, stop_id),
                    version=version,
                    name=getRequiredString(stop_name),
                    public_code=PublicCodeStructure(value=stop_code) if stop_code is not None else None,
                    description=getOptionalString(stop_desc),
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_id, type_value="stop_id")]) if location_type == 1 else None,
                    locale=Locale(time_zone=stop_timezone) if stop_timezone is not None else None,
                    parent_zone_ref=ZoneRefStructure(ref=zone_id, version_ref="EXTERNAL") if zone_id is not None else None,
                    accessibility_assessment=(
                        AccessibilityAssessment(
                            id=getId(codespace, AccessibilityAssessment, 'StopPlace_' + stop_id),
                            version=version,
                            mobility_impaired_access=wheelchairToNeTEx(wheelchair_boarding),
                        )
                        if wheelchair_boarding is not None
                        else None
                    ),
                    info_links=(
                        InfoLinksRelStructure(info_link=[InfoLink(type_of_info_link=[TypeOfInfoLinkEnumeration.RESOURCE], value=stop_url)])
                        if stop_url is not None
                        else None
                    ),
                    centroid=SimplePointVersionStructure(
                        location=LocationStructure2(latitude=Decimal(str(stop_lat)), longitude=Decimal(str(stop_lon)), srs_name="urn:ogc:def:crs:EPSG::4326")
                    ),
                )
                stop_places[stop_place.id] = stop_place  # type: ignore

            else:
                stop_place_id = getId(codespace, StopPlace, parent_station)
                if stop_place_id in stop_places:
                    stop_place = stop_places[getId(codespace, StopPlace, parent_station)]
                else:
                    # Last resort, fake an instance if it does not exist.
                    stop_place = StopPlace(
                        id=stop_place_id,
                        version=version,
                        name=getRequiredString(stop_name),
                        public_code=PublicCodeStructure(value=stop_code) if stop_code is not None else None,
                        description=getOptionalString(stop_desc),
                        private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_id, type_value="stop_id")]) if location_type == 1 else None,
                        locale=Locale(time_zone=stop_timezone) if stop_timezone is not None else None,
                        parent_zone_ref=ZoneRefStructure(ref=zone_id, version_ref="EXTERNAL") if zone_id is not None else None,
                        accessibility_assessment=(
                            AccessibilityAssessment(
                                id=getId(codespace, AccessibilityAssessment, 'StopPlace_' + stop_id),
                                version=version,
                                mobility_impaired_access=wheelchairToNeTEx(wheelchair_boarding),
                            )
                            if wheelchair_boarding is not None
                            else None
                        ),
                        info_links=(
                            InfoLinksRelStructure(info_link=[InfoLink(type_of_info_link=[TypeOfInfoLinkEnumeration.RESOURCE], value=stop_url)])
                            if stop_url is not None
                            else None
                        ),
                        centroid=SimplePointVersionStructure(
                            location=LocationStructure2(
                                latitude=Decimal(str(stop_lat)), longitude=Decimal(str(stop_lon)), srs_name="urn:ogc:def:crs:EPSG::4326"
                            )
                        ),
                    )
                    stop_places[stop_place_id] = stop_place

            if location_type == 1:
                # Nothing to do, we already created the StopPlace
                continue

            if location_type == 0 or location_type == 4:  # TODO: Check!
                # Stop or Platform or BoardingArea, to Quay
                if stop_place.quays is None:
                    stop_place.quays = QuaysRelStructure()

                quay = Quay(
                    id=getId(codespace, Quay, stop_id),
                    version=version,
                    name=getRequiredString(stop_name),
                    public_code=PublicCodeStructure(value=stop_code) if stop_code is not None else None,
                    description=getOptionalString(stop_desc),
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_id, type_value="stop_id")]),
                    parent_zone_ref=ZoneRefStructure(ref=zone_id, version_ref="EXTERNAL") if zone_id is not None else None,
                    accessibility_assessment=(
                        AccessibilityAssessment(
                            id=getId(codespace, AccessibilityAssessment, stop_id),
                            version=version,
                            mobility_impaired_access=wheelchairToNeTEx(wheelchair_boarding),
                        )
                        if wheelchair_boarding is not None
                        else None
                    ),
                    info_links=(
                        InfoLinksRelStructure(info_link=[InfoLink(type_of_info_link=[TypeOfInfoLinkEnumeration.RESOURCE], value=stop_url)])
                        if stop_url is not None
                        else None
                    ),
                    centroid=SimplePointVersionStructure(
                        location=LocationStructure2(latitude=Decimal(str(stop_lat)), longitude=Decimal(str(stop_lon)), srs_name="urn:ogc:def:crs:EPSG::4326")
                    ),
                    level_ref=LevelRef(ref=level_id, version=version) if level_id is not None else None,
                )

                stop_place.quays.taxi_stand_ref_or_quay_ref_or_quay.append(quay)

                passenger_stop_assignment = PassengerStopAssignment(
                    id=getId(codespace, PassengerStopAssignment, stop_id),
                    version=version,
                    fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point=getFakeRef(
                        getId(codespace, ScheduledStopPoint, stop_id), ScheduledStopPointRef, version
                    ),
                    taxi_stand_ref_or_quay_ref_or_quay=cast(Union[TaxiStandRef, QuayRef, Quay], getRef(quay)),
                )
                passenger_stop_assignments.append(passenger_stop_assignment)

            elif location_type == 2:
                # Entrance or Exit
                if stop_place.entrances is None:
                    stop_place.entrances = SiteEntrancesRelStructure()

                stop_place_entrance = StopPlaceEntrance(
                    id=getId(codespace, StopPlaceEntrance, stop_id),
                    version=version,
                    name=getRequiredString(stop_name),
                    public_code=PublicCodeStructure(value=stop_code) if stop_code is not None else None,
                    description=getOptionalString(stop_desc),
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_id, type_value="stop_id")]),
                    parent_zone_ref=ZoneRefStructure(ref=zone_id, version_ref="EXTERNAL") if zone_id is not None else None,
                    accessibility_assessment=(
                        AccessibilityAssessment(
                            id=getId(codespace, AccessibilityAssessment, stop_id),
                            version=version,
                            mobility_impaired_access=wheelchairToNeTEx(wheelchair_boarding),
                        )
                        if wheelchair_boarding is not None
                        else None
                    ),
                    info_links=(
                        InfoLinksRelStructure(info_link=[InfoLink(type_of_info_link=[TypeOfInfoLinkEnumeration.RESOURCE], value=stop_url)])
                        if stop_url is not None
                        else None
                    ),
                    centroid=SimplePointVersionStructure(
                        location=LocationStructure2(latitude=Decimal(str(stop_lat)), longitude=Decimal(str(stop_lon)), srs_name="urn:ogc:def:crs:EPSG::4326")
                    ),
                    level_ref=LevelRef(ref=level_id, version=version) if level_id is not None else None,
                )

                stop_place.entrances.parking_entrance_ref_or_entrance_ref_or_entrance.append(stop_place_entrance)

            elif location_type == 3:
                # Generic Node
                if stop_place.access_spaces is None:
                    stop_place.access_spaces = AccessSpacesRelStructure()

                access_space = AccessSpace(
                    id=getId(codespace, AccessSpace, stop_id),
                    version=version,
                    name=getRequiredString(stop_name),
                    description=getOptionalString(stop_desc),
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_id, type_value="stop_id")]),
                    parent_zone_ref=ZoneRefStructure(ref=zone_id, version_ref="EXTERNAL") if zone_id is not None else None,
                    accessibility_assessment=(
                        AccessibilityAssessment(
                            id=getId(codespace, AccessibilityAssessment, stop_id),
                            version=version,
                            mobility_impaired_access=wheelchairToNeTEx(wheelchair_boarding),
                        )
                        if wheelchair_boarding is not None
                        else None
                    ),
                    info_links=(
                        InfoLinksRelStructure(info_link=[InfoLink(type_of_info_link=[TypeOfInfoLinkEnumeration.RESOURCE], value=stop_url)])
                        if stop_url is not None
                        else None
                    ),
                    centroid=SimplePointVersionStructure(
                        location=LocationStructure2(latitude=Decimal(str(stop_lat)), longitude=Decimal(str(stop_lon)), srs_name="urn:ogc:def:crs:EPSG::4326")
                    ),
                    level_ref=LevelRef(ref=level_id, version=version) if level_id is not None else None,
                )

                stop_place.access_spaces.access_space_ref_or_access_space.append(access_space)

    return list(stop_places.values()), passenger_stop_assignments
