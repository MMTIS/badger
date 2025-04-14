import datetime
import hashlib
import math
from _decimal import Decimal
from typing import List, TypeVar, Any, Iterable, Generator, cast, Union

import duckdb
import numpy
from pandas._libs.missing import NAType
import pandas as pd
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig
from xsdata.models.datatype import XmlDateTime, XmlTime, XmlDate, XmlDuration

from transformers.callsprofile import CallsProfile
from netexio.database import Database
from netexio.pickleserializer import MyPickleSerializer
from netex import (
    Codespace,
    DataSource,
    MultilingualString,
    Version,
    VersionFrameDefaultsStructure,
    VersionTypeEnumeration,
    LocaleStructure,
    SystemOfUnits,
    Operator,
    ContactStructure,
    Locale,
    LanguageUsageStructure,
    LanguageUseEnumeration,
    Line,
    PresentationStructure,
    AllVehicleModesOfTransportEnumeration,
    PrivateCode,
    OperationalContext,
    ResourceFrame,
    DataSourcesInFrameRelStructure,
    OrganisationsInFrameRelStructure,
    OperationalContextsInFrameRelStructure,
    ServiceFrame,
    LinesInFrameRelStructure,
    OperatorRef,
    StopArea,
    LocationStructure2,
    SimplePointVersionStructure,
    PrivateCodeStructure,
    ScheduledStopPoint,
    StopAreaRefsRelStructure,
    StopAreaRefStructure,
    StopAreasInFrameRelStructure,
    ScheduledStopPointsInFrameRelStructure,
    AvailabilityCondition,
    DestinationDisplayView,
    ScheduledStopPointRef,
    Call,
    ArrivalStructure,
    DepartureStructure,
    CallsRelStructure,
    ValidityConditionsRelStructure,
    AvailabilityConditionRef,
    BlockRef,
    DirectionTypeEnumeration,
    AccessibilityAssessment,
    LimitationStatusEnumeration,
    LineRef,
    JourneyPatternView,
    ServiceJourney,
    OnwardServiceLinkView,
    Route,
    RoutePoint,
    PointsOnRouteRelStructure,
    RoutePointRef,
    PointOnRoute,
    RouteLink,
    DayTypesRelStructure,
    DayType,
    PropertiesOfDayRelStructure,
    PropertyOfDay,
    DayOfWeekEnumeration,
    Block,
    ServiceFacilitySetsRelStructure,
    ServiceFacilitySet,
    LuggageCarriageEnumeration,
    LinkSequenceProjection,
    LinkSequenceProjectionRef,
    LineString,
    PosList,
    CodespaceRefStructure,
    DataSourceRefStructure,
    LuggageCarriageFacilityList,
    StopPlace,
    ZoneRefStructure,
    InfoLinksRelStructure,
    InfoLink,
    TypeOfInfoLinkEnumeration,
    QuaysRelStructure,
    SiteEntrancesRelStructure,
    Quay,
    StopPlaceEntrance,
    LevelRef,
    AccessSpacesRelStructure,
    AccessSpace,
    PassengerStopAssignment,
    TemplateServiceJourney,
    FrequencyGroupsRelStructure,
    HeadwayJourneyGroup,
    JourneyFrequencyGroupVersionStructure,
    InterchangeRule,
    InterchangeRuleParameterStructure,
    LineInDirectionRef,
    EmptyType2,
    StopPlaceRef,
    ServiceJourneyRefStructure,
    PrivateCodes,
    DayTypeAssignment,
    DayTypeRefsRelStructure,
    DayTypeRef,
    OperatingPeriod,
    PublicCodeStructure,
    GeneralFrame,
    DirectionType,
    HeadwayJourneyGroupRef,
    RhythmicalJourneyGroupRef,
    RhythmicalJourneyGroup,
    FlexibleLine,
    RetailConsortium,
    ServicedOrganisation,
    GeneralOrganisation,
    ManagementAgent,
    TravelAgent,
    OtherOrganisation,
    OnlineServiceOperator,
    Authority,
    ValidityRuleParameterRef,
    ValidityTriggerRef,
    ValidityConditionRef,
    ValidBetween,
    SimpleAvailabilityCondition,
    ValidDuring,
    ValidityRuleParameter,
    ValidityTrigger,
    ValidityCondition,
    OperatingPeriodRef,
    RoutePointRefStructure,
    TaxiStandRef,
    QuayRef,
    FlexibleLineRef,
    RouteLinkRefStructure,
)

from utils.refs import getRef, getIndex, getBitString2, getFakeRef, getOptionalString, getId, getRequiredString
from utils.aux_logging import log_all, prepare_logger
import logging

T = TypeVar("T", bound=Any)


def get_or_none(items: list[Any] | None, i: int, cast_clazz: type[T] | None = None) -> Any | None:
    if items is None or i >= len(items) or isinstance(items[i], NAType):
        return None

    if cast_clazz is not None:
        if not isinstance(cast_clazz, type) or cast_clazz is object:
            raise TypeError(f"Invalid cast class: {cast_clazz}")

        return cast_clazz(items[i])

    return items[i]


def gtfs_date(d: str) -> datetime.datetime:
    return datetime.datetime(year=int(str(d)[0:4]), month=int(str(d)[4:6]), day=int(str(d)[6:8]))


def date_to_xmldatetime(d: datetime.date) -> XmlDateTime:
    x = datetime.datetime.combine(d, datetime.datetime.min.time())
    return XmlDateTime.from_datetime(x)


def date_to_xmldate(d: datetime.date) -> XmlDate:
    return XmlDate.from_date(d)


class GtfsNeTexProfile(CallsProfile):
    @staticmethod
    def getShortName(name: str) -> str:
        if len(name) > 8:
            return ''.join([x[0].upper() for x in name.split(' ')])
        return name

    def getCodespaceAndDataSource(self) -> tuple[Codespace, DataSource, Version, VersionFrameDefaultsStructure]:
        feed_info_sql = """select * from feed_info limit 1;"""

        with self.conn.cursor() as cur:
            cur.execute(feed_info_sql)
            df = cur.df()

            short_name = self.getShortName(df['feed_publisher_name'][0])
            codespace_name = short_name.replace(' ', '')

            codespace = Codespace(
                id="{}:Codespace:{}".format(codespace_name, codespace_name),
                xmlns=codespace_name,
                xmlns_url=df['feed_publisher_url'][0],
                description=df['feed_publisher_name'][0],
            )

            start_date = datetime.datetime.combine(gtfs_date(df['feed_start_date'][0]), datetime.datetime.min.time())
            end_date = datetime.datetime.combine(gtfs_date(df['feed_end_date'][0]), datetime.datetime.min.time())

            version = Version(
                id="{}:Version:{}".format(codespace_name, df['feed_version'][0]),
                version=df['feed_version'][0] if df['feed_version'][0] not in ('', None) else str(datetime.date.today()).replace('-', ''),
                start_date=XmlDateTime.from_datetime(start_date),
                end_date=XmlDateTime.from_datetime(end_date),
                version_type=VersionTypeEnumeration.BASELINE,
            )

            data_source = DataSource(
                id="{}:DataSource:{}".format(codespace_name, codespace_name),
                version=version.version,
                name=MultilingualString(value=df['feed_publisher_name'][0]),
                short_name=MultilingualString(value=short_name),
                description=MultilingualString(value=df['feed_publisher_name'][0]),
            )

            frame_defaults = VersionFrameDefaultsStructure(
                default_codespace_ref=cast(CodespaceRefStructure, getRef(codespace, CodespaceRefStructure)),
                default_data_source_ref=cast(DataSourceRefStructure, getRef(data_source, DataSourceRefStructure)),
                default_locale=LocaleStructure(default_language=df['feed_lang'][0]),
                default_location_system="urn:ogc:def:crs:EPSG::4326",
                default_system_of_units=SystemOfUnits.SI_METRES,
            )

            return (codespace, data_source, version, frame_defaults)

    def getResourceFrame(self, operators: list[Operator], id: str = "ResourceFrame") -> ResourceFrame:
        resource_frame = ResourceFrame(id=getId(ResourceFrame, self.codespace, id), version=self.version.version)
        resource_frame.data_sources = DataSourcesInFrameRelStructure(data_source=[self.data_source])
        # resource_frame.zones = ZonesInFrameRelStructure(transport_administrative_zone=[transport_administrative_zone])
        resource_frame.organisations = OrganisationsInFrameRelStructure(
            organisation_or_transport_organisation=cast(
                list[
                    RetailConsortium
                    | ServicedOrganisation
                    | GeneralOrganisation
                    | ManagementAgent
                    | TravelAgent
                    | OtherOrganisation
                    | OnlineServiceOperator
                    | Authority
                    | Operator
                ],
                operators,
            )
        )
        resource_frame.operational_contexts = OperationalContextsInFrameRelStructure(operational_context=self.getOperationalContexts())
        # resource_frame.vehicle_types = VehicleTypesInFrameRelStructure(compound_train_or_train_or_vehicle_type=getVehicleTypes(codespace))
        # resource_frame.vehicles = VehiclesInFrameRelStructure(train_element_or_vehicle=getVehicles(codespace))
        return resource_frame

    def get_agency_id(self, agency_id: str) -> str:
        if ':Operator:' in agency_id:
            return agency_id
        else:
            return getId(Operator, self.codespace, agency_id)

    def getOperators(self, agency_sql: dict[str, str] = {'query': """select * from agency;"""}) -> list[Operator]:
        results = []

        with self.conn.cursor() as cur:
            cur.execute(**agency_sql)
            df = cur.df()

            agency_ids = df.get('agency_id')
            agency_names = df.get('agency_name')
            agency_timezones = df.get('agency_timezone')
            agency_langs = df.get('agency_lang')
            agency_phones = df.get('agency_phone')
            agency_urls = df.get('agency_url')
            agency_emails = df.get('agency_email')

            for i in range(0, len(agency_ids)):
                language: str | None = get_or_none(agency_langs, i)
                if language:
                    languages = LocaleStructure.Languages(
                        language_usage=[LanguageUsageStructure(language=language, language_use=[LanguageUseEnumeration.NORMALLY_USED])]
                    )
                else:
                    languages = None

                operator = Operator(
                    id=self.get_agency_id(agency_ids[i]),
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=agency_ids[i], type_value="agency_id")]),
                    version=self.version.version,
                    name=MultilingualString(value=agency_names[i]),
                    locale=Locale(time_zone=agency_timezones[i], languages=languages),
                    customer_service_contact_details=ContactStructure(
                        url=agency_urls[i], phone=get_or_none(agency_phones, i), email=get_or_none(agency_emails, i)
                    ),
                )
                results.append(operator)

        return results

    @staticmethod
    def gtfsRouteTypeToNeTEx(route_type: int | None) -> AllVehicleModesOfTransportEnumeration | None:
        if route_type == 0:
            return AllVehicleModesOfTransportEnumeration.TRAM
        elif route_type == 1:
            return AllVehicleModesOfTransportEnumeration.METRO
        elif route_type == 2:
            return AllVehicleModesOfTransportEnumeration.RAIL
        elif route_type == 3:
            return AllVehicleModesOfTransportEnumeration.BUS
        elif route_type == 4:
            return AllVehicleModesOfTransportEnumeration.WATER
        elif route_type == 5 or route_type == 7:
            return AllVehicleModesOfTransportEnumeration.FUNICULAR
        elif route_type == 6:
            return AllVehicleModesOfTransportEnumeration.CABLEWAY
        elif route_type == 11:
            return AllVehicleModesOfTransportEnumeration.TROLLEY_BUS

        return None

    def getOperationalContexts(self) -> list[OperationalContext]:
        operational_contexts = []

        operational_contexts_sql = """select distinct route_type from routes;"""

        with self.conn.cursor() as cur:
            cur.execute(operational_contexts_sql)
            df = cur.df()

            for i in range(0, len(df['route_type'])):
                operational_context = OperationalContext(
                    id=getId(OperationalContext, self.codespace, df['route_type'][i]),
                    version=self.version.version,
                    vehicle_mode=self.gtfsRouteTypeToNeTEx(df['route_type'][i]),
                )
                operational_contexts.append(operational_context)

        return operational_contexts

    def get_route_id(self, route_id: str) -> str:
        if ':Line:' in route_id:
            return route_id
        else:
            return getId(Line, self.codespace, route_id)

    def getLines(self) -> list[Line]:
        lines = []

        lines_sql = """select routes.* from routes;"""

        with self.conn.cursor() as cur:
            cur.execute(lines_sql)
            df = cur.df()

            route_ids = df.get('route_id')
            route_text_colors = df.get('route_text_color')
            route_colors = df.get('route_color')
            route_long_names = df.get('route_long_name')
            route_short_names = df.get('route_short_name')
            route_descs = df.get('route_desc')
            route_types = df.get('route_type')
            route_urls = df.get('route_url')
            agency_ids = df.get('agency_id')

            for i in range(0, len(route_ids)):
                presentation = None
                if get_or_none(route_colors, i) is not None or get_or_none(route_text_colors, i) is not None:
                    presentation = PresentationStructure(
                        colour=get_or_none(route_colors, i), text_colour=get_or_none(route_text_colors, i), background_colour=get_or_none(route_colors, i)
                    )

                agency_id = get_or_none(agency_ids, i)
                operator_ref = None
                if agency_id is not None:
                    operator_ref = getFakeRef(self.get_agency_id(agency_id), OperatorRef, self.version.version)

                line = Line(
                    id=self.get_route_id(route_ids[i]),
                    version=self.version.version,
                    name=getRequiredString(route_long_names[i], route_short_names[i]),
                    short_name=getOptionalString(get_or_none(route_short_names, i)),
                    description=getOptionalString(get_or_none(route_descs, i)),
                    transport_mode=self.gtfsRouteTypeToNeTEx(get_or_none(route_types, i)),
                    presentation=presentation,
                    url=get_or_none(route_urls, i),
                    operator_ref=operator_ref,
                    public_code=PublicCodeStructure(value=route_short_names[i]) if route_short_names[i] is not None else None,
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=route_ids[i], type_value="route_id")]),
                )
                lines.append(line)

        return lines

    def get_stop_id_sa(self, stop_id: str) -> str:
        if ':StopArea:' in stop_id:
            return stop_id
        elif ':StopPlace:' in stop_id:
            return stop_id.replace(':StopPlace:', ':StopArea:')
        else:
            return getId(StopArea, self.codespace, stop_id)

    def getStopAreas(
        self, stop_area_sql: dict[str, str] = {'query': """select distinct * from stops where location_type = 1 order by stop_id;"""}
    ) -> list[StopArea]:
        stop_areas = []

        with self.conn.cursor() as cur:
            cur.execute(**stop_area_sql)
            df = cur.df()

            stop_ids = df.get('stop_id')
            stop_names = df.get('stop_name')
            stop_lats = df.get('stop_lat')
            stop_lons = df.get('stop_lon')
            stop_codes = df.get('stop_code')
            stop_descs = df.get('stop_desc')
            # zone_ids = df.get('zone_id')
            # stop_urls = df.get('stop_url')
            # location_types = df.get('location_type')
            # parent_stations = df.get('parent_station')
            # wheelchair_boardings = df.get('wheelchair_boarding')
            # stop_timezones = df.get('stop_timezone')
            # platform_codes = df.get('platform_code')

            for i in range(0, len(stop_ids)):
                description = getOptionalString(get_or_none(stop_descs, i))

                stop_area = StopArea(
                    id=self.get_stop_id_sa(stop_ids[i]),
                    version=self.version.version,
                    name=MultilingualString(value=stop_names[i]),
                    public_code=PublicCodeStructure(value=stop_codes[i]) if stop_codes[i] is not None else None,
                    description=[description] if description is not None else [],  # TODO: list insanity for description
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_ids[i], type_value="stop_id")]),
                    centroid=SimplePointVersionStructure(
                        location=LocationStructure2(
                            latitude=Decimal(str(stop_lats[i])), longitude=Decimal(str(stop_lons[i])), srs_name="urn:ogc:def:crs:EPSG::4326"
                        )
                    ),
                )
                stop_areas.append(stop_area)

        return stop_areas

    def get_stop_id(self, stop_id: str) -> str:
        if ':ScheduledStopPoint:' in stop_id:
            return stop_id
        elif ':Quay:' in stop_id:
            return stop_id.replace(':Quay:', ':ScheduledStopPoint:')
        else:
            return getId(ScheduledStopPoint, self.codespace, stop_id)

    def getScheduledStopPoints(
        self,
        stop_areas_input: Iterable[StopArea],
        scheduled_stop_points_sql: dict[str, str] = {
            'query': """select distinct * from stops where location_type = 0 or location_type is null order by stop_id;"""
        },
    ) -> list[ScheduledStopPoint]:
        stop_areas: dict[object, StopArea] = getIndex(stop_areas_input)

        scheduled_stop_points: list[ScheduledStopPoint] = []
        # passenger_stop_assignments: list[PassengerStopAssignment] = []

        with self.conn.cursor() as cur:
            cur.execute(**scheduled_stop_points_sql)
            df = cur.df()

            stop_ids = df.get('stop_id')
            stop_names = df.get('stop_name')
            stop_lats = df.get('stop_lat')
            stop_lons = df.get('stop_lon')
            stop_codes = df.get('stop_code')
            stop_descs = df.get('stop_desc')
            # zone_ids = df.get('zone_id')
            stop_urls = df.get('stop_url')
            # location_types = df.get('location_type')
            parent_stations = df.get('parent_station')
            # wheelchair_boardings = df.get('wheelchair_boarding')
            # stop_timezones = df.get('stop_timezone')
            platform_codes = df.get('platform_code')

            for i in range(0, len(stop_ids)):
                short_stop_code = None
                platform_code = get_or_none(platform_codes, i)
                if platform_code is not None:
                    short_stop_code = PrivateCodeStructure(value=platform_code, type_value='platform_code')

                location = LocationStructure2(
                    longitude=Decimal(str(stop_lons[i])) if stop_lons[i] is not None else None,
                    latitude=Decimal(str(stop_lats[i])) if stop_lats[i] is not None else None,
                    srs_name="urn:ogc:def:crs:EPSG::4326",
                )

                my_stop_areas = None
                parent_station = get_or_none(parent_stations, i)
                if parent_station is not None:
                    stop_area_ref = getId(StopArea, self.codespace, parent_station)
                    if stop_area_ref not in stop_areas:
                        # TODO: Implement the logger here too
                        print(f"Parent {parent_station} not found, faking it.")
                        my_stop_areas = StopAreaRefsRelStructure(stop_area_ref=[getFakeRef(stop_area_ref, StopAreaRefStructure, self.version.version)])
                    else:
                        my_stop_areas = StopAreaRefsRelStructure(
                            stop_area_ref=[cast(StopAreaRefStructure, getRef(stop_areas[stop_area_ref], StopAreaRefStructure))]
                        )

                scheduled_stop_point = ScheduledStopPoint(
                    id=self.get_stop_id(stop_ids[i]),
                    version=self.version.version,
                    name=MultilingualString(value=stop_names[i]),
                    description=getOptionalString(get_or_none(stop_descs, i)),
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_ids[i], type_value="stop_id")]),
                    short_stop_code=short_stop_code,
                    public_code=PublicCodeStructure(value=stop_codes[i]) if stop_codes[i] is not None else None,
                    url=get_or_none(stop_urls, i),
                    location=location,
                    stop_areas=my_stop_areas,
                )
                scheduled_stop_points.append(scheduled_stop_point)

                """
                stop_place_ref = None
                if row['stopplacecoderef'] is not None:
                    stop_place_ref = getFakeRef(row['stopplacecoderef'], StopPlaceRef, "any")

                quay_ref = None
                if row['quaycoderef'] is not None:
                    quay_ref = getFakeRef(row['quaycoderef'], QuayRef, "any")

                if stop_place_ref is not None or quay_ref is not None:
                    passenger_stop_assignment = PassengerStopAssignment(
                        scheduled_stop_point_ref=getRef(scheduled_stop_point, ScheduledStopPointRef),
                        stop_place_ref=stop_place_ref,
                        quay_ref=quay_ref, order=1)
                    setIdVersion(passenger_stop_assignment, codespace, row['id'], version)
                    passenger_stop_assignments.append(passenger_stop_assignment)
                """

        return scheduled_stop_points

    # TODO: implement
    def getStopPlaces(
        self, stop_places_sql: dict[str, str] = {'query': """select distinct * from stops order by coalesce(parent_station, '') asc, stop_id;"""}
    ) -> tuple[list[StopPlace], list[PassengerStopAssignment]]:
        stop_places = {}
        passenger_stop_assignments = []
        with self.conn.cursor() as cur:
            cur.execute(**stop_places_sql)
            df = cur.df()

            stop_ids = df.get('stop_id')
            stop_names = df.get('stop_name')
            stop_lats = df.get('stop_lat')
            stop_lons = df.get('stop_lon')
            stop_codes = df.get('stop_code')
            stop_descs = df.get('stop_desc')
            zone_ids = df.get('zone_id')
            stop_urls = df.get('stop_url')
            location_types = df.get('location_type')
            parent_stations = df.get('parent_station')
            wheelchair_boardings = df.get('wheelchair_boarding')
            stop_timezones = df.get('stop_timezone')
            # platform_codes = df.get('platform_code')
            level_ids = df.get('level_id')

            for i in range(0, len(stop_ids)):
                # Every stop that does not have a parent_station, will become a StopPlace
                if parent_stations[i] is None:
                    description = getOptionalString(stop_descs[i])

                    stop_place = StopPlace(
                        id=getId(StopPlace, self.codespace, stop_ids[i]),
                        version=self.version.version,
                        name=MultilingualString(value=stop_names[i]),
                        public_code=PublicCodeStructure(value=stop_codes[i]) if stop_codes[i] is not None else None,
                        description=[description] if description else [],  # TODO: description list crazyness
                        private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_ids[i], type_value="stop_id")]) if location_types[i] == 1 else None,
                        locale=Locale(time_zone=stop_timezones[i]) if stop_timezones[i] is not None else None,
                        parent_zone_ref=ZoneRefStructure(ref=zone_ids[i], version_ref="EXTERNAL") if zone_ids[i] is not None else None,
                        accessibility_assessment=(
                            AccessibilityAssessment(
                                id=getId(AccessibilityAssessment, self.codespace, 'StopPlace_' + stop_ids[i]),
                                version=self.version.version,
                                mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_boardings[i]),
                            )
                            if not isinstance(wheelchair_boardings[i], NAType)
                            else None
                        ),
                        info_links=(
                            InfoLinksRelStructure(info_link=[InfoLink(type_of_info_link=[TypeOfInfoLinkEnumeration.RESOURCE], value=stop_urls[i])])
                            if stop_urls[i] is not None
                            else None
                        ),
                        centroid=SimplePointVersionStructure(
                            location=LocationStructure2(
                                latitude=Decimal(str(stop_lats[i])), longitude=Decimal(str(stop_lons[i])), srs_name="urn:ogc:def:crs:EPSG::4326"
                            )
                        ),
                    )
                    stop_places[stop_place.id] = stop_place
                else:
                    stop_place_id = getId(StopPlace, self.codespace, parent_stations[i])
                    if stop_place_id in stop_places:
                        stop_place = stop_places[getId(StopPlace, self.codespace, parent_stations[i])]
                    else:
                        description = getOptionalString(stop_descs[i])

                        # Last resort, fake an instance if it does not exist.
                        stop_place = StopPlace(
                            id=stop_place_id,
                            version=self.version.version,
                            name=MultilingualString(value=stop_names[i]),
                            public_code=PublicCodeStructure(value=stop_codes[i]) if stop_codes[i] is not None else None,
                            description=[description] if description else [],  # TODO: description list crazyness
                            private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_ids[i], type_value="stop_id")]) if location_types[i] == 1 else None,
                            locale=Locale(time_zone=stop_timezones[i]) if stop_timezones[i] is not None else None,
                            parent_zone_ref=ZoneRefStructure(ref=zone_ids[i], version_ref="EXTERNAL") if zone_ids[i] is not None else None,
                            accessibility_assessment=(
                                AccessibilityAssessment(
                                    id=getId(AccessibilityAssessment, self.codespace, 'StopPlace_' + stop_ids[i]),
                                    version=self.version.version,
                                    mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_boardings[i]),
                                )
                                if not isinstance(wheelchair_boardings[i], NAType)
                                else None
                            ),
                            info_links=(
                                InfoLinksRelStructure(info_link=[InfoLink(type_of_info_link=[TypeOfInfoLinkEnumeration.RESOURCE], value=stop_urls[i])])
                                if stop_urls[i] is not None
                                else None
                            ),
                            centroid=SimplePointVersionStructure(
                                location=LocationStructure2(
                                    latitude=Decimal(str(stop_lats[i])), longitude=Decimal(str(stop_lons[i])), srs_name="urn:ogc:def:crs:EPSG::4326"
                                )
                            ),
                        )
                        stop_places[stop_place_id] = stop_place

                if location_types[i] == 1:
                    # Nothing to do, we already created the StopPlace
                    continue

                if location_types[i] != location_types[i] or location_types[i] == 0 or location_types[i] == 4:
                    # Stop or Platform or BoardingArea, to Quay
                    if stop_place.quays is None:
                        stop_place.quays = QuaysRelStructure()

                    description = getOptionalString(stop_descs[i])

                    quay = Quay(
                        id=getId(Quay, self.codespace, stop_ids[i]),
                        version=self.version.version,
                        name=MultilingualString(value=stop_names[i]),
                        public_code=PublicCodeStructure(value=stop_codes[i]) if stop_codes[i] is not None else None,
                        description=[description] if description else [],  # TODO: description list crazyness
                        private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_ids[i], type_value="stop_id")]),
                        parent_zone_ref=ZoneRefStructure(ref=zone_ids[i], version_ref="EXTERNAL") if zone_ids[i] is not None else None,
                        accessibility_assessment=(
                            AccessibilityAssessment(
                                id=getId(AccessibilityAssessment, self.codespace, stop_ids[i]),
                                version=self.version.version,
                                mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_boardings[i]),
                            )
                            if not isinstance(wheelchair_boardings[i], NAType)
                            else None
                        ),
                        info_links=(
                            InfoLinksRelStructure(info_link=[InfoLink(type_of_info_link=[TypeOfInfoLinkEnumeration.RESOURCE], value=stop_urls[i])])
                            if stop_urls[i] is not None
                            else None
                        ),
                        centroid=SimplePointVersionStructure(
                            location=LocationStructure2(
                                latitude=Decimal(str(stop_lats[i])), longitude=Decimal(str(stop_lons[i])), srs_name="urn:ogc:def:crs:EPSG::4326"
                            )
                        ),
                        level_ref=LevelRef(ref=level_ids[0], version=self.version.version) if level_ids[i] is not None else None,
                    )

                    stop_place.quays.taxi_stand_ref_or_quay_ref_or_quay.append(quay)

                    passenger_stop_assignment = PassengerStopAssignment(
                        id=getId(PassengerStopAssignment, self.codespace, stop_ids[i]),
                        version=self.version.version,
                        order=1,
                        fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point=getFakeRef(
                            getId(ScheduledStopPoint, self.codespace, stop_ids[i]), ScheduledStopPointRef, self.version.version
                        ),
                        taxi_stand_ref_or_quay_ref_or_quay=cast(Union[TaxiStandRef, QuayRef, Quay], getRef(quay)),
                    )
                    passenger_stop_assignments.append(passenger_stop_assignment)

                elif location_types[i] == 2:
                    # Entrance or Exit
                    if stop_place.entrances is None:
                        stop_place.entrances = SiteEntrancesRelStructure()

                    description = getOptionalString(stop_descs[i])

                    stop_place_entrance = StopPlaceEntrance(
                        id=getId(StopPlaceEntrance, self.codespace, stop_ids[i]),
                        version=self.version.version,
                        name=MultilingualString(value=stop_names[i]),
                        public_code=PublicCodeStructure(value=stop_codes[i]) if stop_codes[i] is not None else None,
                        description=[description] if description else [],  # TODO: description list crazyness
                        private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_ids[i], type_value="stop_id")]),
                        parent_zone_ref=ZoneRefStructure(ref=zone_ids[i], version_ref="EXTERNAL") if zone_ids[i] is not None else None,
                        accessibility_assessment=(
                            AccessibilityAssessment(
                                id=getId(AccessibilityAssessment, self.codespace, stop_ids[i]),
                                version=self.version.version,
                                mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_boardings[i]),
                            )
                            if not isinstance(wheelchair_boardings[i], NAType)
                            else None
                        ),
                        info_links=(
                            InfoLinksRelStructure(info_link=[InfoLink(type_of_info_link=[TypeOfInfoLinkEnumeration.RESOURCE], value=stop_urls[i])])
                            if stop_urls[i] is not None
                            else None
                        ),
                        centroid=SimplePointVersionStructure(
                            location=LocationStructure2(
                                latitude=Decimal(str(stop_lats[i])), longitude=Decimal(str(stop_lons[i])), srs_name="urn:ogc:def:crs:EPSG::4326"
                            )
                        ),
                        level_ref=LevelRef(ref=level_ids[0], version=self.version.version) if level_ids[i] is not None else None,
                    )

                    stop_place.entrances.parking_entrance_ref_or_entrance_ref_or_entrance.append(stop_place_entrance)

                elif location_types[i] == 3:
                    # Generic Node
                    if stop_place.access_spaces is None:
                        stop_place.access_spaces = AccessSpacesRelStructure()

                    description = getOptionalString(stop_descs[i])

                    access_space = AccessSpace(
                        id=getId(AccessSpace, self.codespace, stop_ids[i]),
                        version=self.version.version,
                        name=MultilingualString(value=stop_names[i]),
                        description=[description] if description else [],  # TODO: description list crazyness
                        private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_ids[i], type_value="stop_id")]),
                        parent_zone_ref=ZoneRefStructure(ref=zone_ids[i], version_ref="EXTERNAL") if zone_ids[i] is not None else None,
                        accessibility_assessment=(
                            AccessibilityAssessment(
                                id=getId(AccessibilityAssessment, self.codespace, stop_ids[i]),
                                version=self.version.version,
                                mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_boardings[i]),
                            )
                            if not isinstance(wheelchair_boardings[i], NAType)
                            else None
                        ),
                        info_links=(
                            InfoLinksRelStructure(info_link=[InfoLink(type_of_info_link=[TypeOfInfoLinkEnumeration.RESOURCE], value=stop_urls[i])])
                            if stop_urls[i] is not None
                            else None
                        ),
                        centroid=SimplePointVersionStructure(
                            location=LocationStructure2(
                                latitude=Decimal(str(stop_lats[i])), longitude=Decimal(str(stop_lons[i])), srs_name="urn:ogc:def:crs:EPSG::4326"
                            )
                        ),
                        level_ref=LevelRef(ref=level_ids[0], version=self.version.version) if level_ids[i] is not None else None,
                    )

                    stop_place.access_spaces.access_space_ref_or_access_space.append(access_space)

        return list(stop_places.values()), passenger_stop_assignments

    def get_shape_id(self, shape_id: str) -> str:
        if ':Route:' in shape_id:
            return shape_id
        else:
            return getId(Route, self.codespace, shape_id)

    def get_shape_id_lsp(self, shape_id: str) -> str:
        if ':Route:' in shape_id:
            return shape_id.replace(':Route:', ':LinkSequenceProjection:')
        else:
            return getId(LinkSequenceProjection, self.codespace, shape_id)

    #
    # def getPaths(self):
    #     pl = PathLink()
    #
    #
    def getRoutes(self) -> tuple[list[Route], list[RoutePoint], list[RouteLink]]:
        lines = getIndex(self.lines)

        shape_route_mapping = {}

        # Within NeTEx it is not possible to have a route (GTFS-shape) pointing to multiple lines (GTFS-route)
        shape_route_sql = """select distinct shape_id, array_agg(distinct route_id) as route_ids from trips where shape_id is not null group by shape_id;"""
        with self.conn.cursor() as cur:
            cur.execute(shape_route_sql)

            df = cur.df()
            shape_ids = df.get('shape_id')
            route_ids = df.get('route_ids')

            for i in range(0, len(shape_ids)):
                if len(route_ids[i]) > 0:
                    shape_route_mapping[shape_ids[i]] = [(shape_ids[i] + '-' + x, x) for x in route_ids[i]]
                else:
                    # Stale route, why should we add them at all?
                    shape_route_mapping[shape_ids[i]] = [
                        (
                            shape_ids[i],
                            None,
                        )
                    ]

        shape_sql = """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes order by shape_id, shape_pt_sequence, shape_dist_traveled;"""

        routes = {}
        route_points = []
        route_links = []

        with self.conn.cursor() as cur:
            cur.execute(shape_sql)

            df = cur.df()
            shape_ids = df.get('shape_id')
            shape_pt_lats = df.get('shape_pt_lat')
            shape_pt_lons = df.get('shape_pt_lon')
            shape_pt_sequences = df.get('shape_pt_sequence')
            shape_dist_traveleds = df.get('shape_dist_traveled')

            prev_order = 1
            prev_route = None
            prev_distance = 0
            prev_route_point = None
            prev_shape_id = None

            for i in range(0, len(shape_ids)):
                route_id = self.get_shape_id(shape_ids[i])
                route_point_id = route_id.replace(":Route:", ":RoutePoint:")
                route_link_id = route_id.replace(":Route:", ":RouteLink:")

                route_point = RoutePoint(
                    id=f"{route_point_id}-{shape_pt_sequences[i]}",
                    version=self.version.version,
                    location=LocationStructure2(
                        longitude=Decimal(str(shape_pt_lons[i])), latitude=Decimal(str(shape_pt_lats[i])), srs_name="urn:ogc:def:crs:EPSG::4326"
                    ),
                )
                route_points.append(route_point)

                if shape_ids[i] == prev_shape_id:
                    assert prev_route_point is not None and prev_route is not None

                    # It is the same route, and still being extended
                    distance = None
                    if shape_dist_traveleds[i]:
                        distance = shape_dist_traveleds[i] - prev_distance

                    route_link = RouteLink(
                        id=f"{route_link_id}-{shape_pt_sequences[i]}",
                        version=self.version.version,
                        from_point_ref=cast(RoutePointRefStructure, getRef(prev_route_point)),
                        to_point_ref=cast(RoutePointRefStructure, getRef(route_point)),
                        distance=Decimal(str(distance)),
                    )
                    route_links.append(route_link)

                    for route in prev_route:
                        assert route.points_in_sequence is not None
                        route.points_in_sequence.point_on_route[-1].onward_route_link_ref = cast(RouteLinkRefStructure, getRef(route_link))

                else:
                    prev_order = 1
                    prev_route = []
                    prev_distance = 0
                    prev_route_point = None
                    prev_shape_id = None

                    route_ids = shape_route_mapping.get(shape_ids[i], [(shape_ids[i], None)])
                    for route_id, line_id in route_ids:
                        route = Route(id=self.get_shape_id(route_id), version=self.version.version)
                        route.private_code = PrivateCode(value=shape_ids[i], type_value="shape_id")
                        route.points_in_sequence = PointsOnRouteRelStructure()
                        if line_id:
                            line = lines[self.get_route_id(line_id)]  # TODO: Validate
                            route.line_ref = cast(Union[FlexibleLineRef, LineRef], getRef(line, LineRef))

                        routes[route_id] = route
                        prev_route.append(route)

                for route in prev_route:
                    route_point_id = self.get_shape_id(route_id).replace(":Route:", ":RoutePoint:")

                    point_on_route = PointOnRoute(
                        id=f"{route_point_id}-{shape_pt_sequences[i]}",
                        version=self.version.version,
                        order=prev_order,
                        point_ref_or_infrastructure_point_ref_or_activation_point_ref_or_timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref_or_route_point_ref=cast(
                            RoutePointRef, getRef(route_point, RoutePointRef)
                        ),
                    )  # shape_pt_sequence is non-negative integer

                    assert route.points_in_sequence is not None
                    route.points_in_sequence.point_on_route.append(point_on_route)

                prev_shape_id = shape_ids[i]
                prev_route_point = route_point
                prev_distance = shape_dist_traveleds[i]
                prev_order += 1

        return (list(routes.values()), route_points, route_links)

    def getLineStrings(
        self,
        shape_sql: dict[str, Any] = {
            'query': (
                """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
            )
        },
    ) -> List[LinkSequenceProjection]:
        link_sequence_projection = []

        with self.conn.cursor() as cur:
            cur.execute(**shape_sql)

            df = cur.df()
            shape_ids = df.get('shape_id')
            shape_pt_lats = df.get('shape_pt_lat')
            shape_pt_lons = df.get('shape_pt_lon')
            # shape_pt_sequences = df.get('shape_pt_sequence')
            shape_dist_traveleds = df.get('shape_dist_traveled')

            prev_distance: int | None = 0
            prev_shape_id = None
            pos_list = []

            for i in range(0, len(shape_ids)):
                if shape_ids[i] != prev_shape_id and prev_shape_id is not None:
                    de_distance = None
                    if prev_distance is not None and not numpy.isnan(prev_distance):
                        de_distance = Decimal(prev_distance)

                    link_sequence_projection_id = self.get_shape_id_lsp(prev_shape_id)
                    pos_list_result = PosList(value=pos_list)  # type: ignore
                    link_sequence_projection.append(
                        LinkSequenceProjection(
                            id=link_sequence_projection_id,
                            version=self.version.version,
                            distance=de_distance,
                            points_or_line_string=LineString(
                                id=link_sequence_projection_id.replace(':', "_"),
                                srs_name="urn:ogc:def:crs:EPSG::4326",
                                srs_dimension=2,
                                pos_or_point_property_or_pos_list=[pos_list_result],
                            ),
                        )
                    )
                    pos_list = []
                    prev_distance = 0

                pos_list += [Decimal(str(shape_pt_lats[i])), Decimal(str(shape_pt_lons[i]))]

                prev_shape_id = shape_ids[i]
                prev_distance = get_or_none(shape_dist_traveleds, i)

            if len(pos_list) > 0:
                de_distance = None
                if prev_distance is not None and not numpy.isnan(prev_distance):
                    de_distance = Decimal(str(prev_distance))

                result_pos_list = PosList(value=pos_list)  # type: ignore

                if prev_shape_id:
                    link_sequence_projection_id = self.get_shape_id_lsp(prev_shape_id)
                    link_sequence_projection.append(
                        LinkSequenceProjection(
                            id=link_sequence_projection_id,
                            version=self.version.version,
                            distance=de_distance,
                            points_or_line_string=LineString(
                                id=link_sequence_projection_id.replace(":", "_"),
                                srs_name="urn:ogc:def:crs:EPSG::4326",
                                srs_dimension=2,
                                pos_or_point_property_or_pos_list=[result_pos_list],
                            ),
                        )
                    )

        return link_sequence_projection

    def getServiceFrame(
        self, lines: list[Line], stop_areas: list[StopArea], scheduled_stop_points: list[ScheduledStopPoint], id: str = "ServiceFrame"
    ) -> ServiceFrame:
        if lines is None:
            lines = self.lines

        if stop_areas is None:
            stop_areas = self.stop_areas

        if scheduled_stop_points is None:
            scheduled_stop_points = self.scheduled_stop_points

        service_frame = ServiceFrame(id=getId(ServiceFrame, self.codespace, id), version=self.version.version)
        # service_frame.prerequisites.resource_frame_ref
        # setIdVersion(service_frame, self.codespace, "ServiceFrame", self.version)
        service_frame.lines = LinesInFrameRelStructure(line=cast(list[FlexibleLine | Line], lines))

        stop_areas = sorted(stop_areas, key=lambda x: x.id or -1)
        if stop_areas:
            service_frame.stop_areas = StopAreasInFrameRelStructure(stop_area=stop_areas)

        scheduled_stop_points = sorted(scheduled_stop_points, key=lambda x: x.id or -1)
        service_frame.scheduled_stop_points = ScheduledStopPointsInFrameRelStructure(scheduled_stop_point=scheduled_stop_points)

        #     """
        #     destination_displays = getDestinationDisplays(codespace, version)
        #     stop_areas = getStopAreas(codespace, version)
        #     scheduled_stop_points, passenger_stop_assignments = getScheduledStopPoints(codespace, version, route_points,
        #                                                                                stop_areas)
        #     service_journey_patterns, timing_links, service_journey_patterns_transport_mode = getServiceJourneyPatterns(
        #         codespace, version, routes, lines, scheduled_stop_points, operational_contexts, destination_displays)
        #
        #     service_journey_patterns = sorted(service_journey_patterns, key=lambda x: x.id)
        #     timing_links = sorted(timing_links, key=lambda x: x.id)
        #     """
        #
        #     self.stop_areas = sorted(self.stop_areas, key=lambda x: x.id)
        #     self.scheduled_stop_points = sorted(self.scheduled_stop_points, key=lambda x: x.id)
        #
        #     """
        #     destination_displays = sorted(destination_displays, key=lambda x: x.id)
        #     passenger_stop_assignments = sorted(passenger_stop_assignments, key=lambda x: x.id)
        #
        #     """
        #     self.route_points = sorted(self.route_points, key=lambda x: x.id)
        #     service_frame.route_points = RoutePointsInFrameRelStructure(route_point=self.route_points)
        #
        #     self.route_links = sorted(self.route_links, key=lambda x: x.id)
        #     service_frame.route_links = RouteLinksInFrameRelStructure(route_link=self.route_links)
        #
        #    self.routes = sorted(self.routes, key=lambda x: x.id)
        #    service_frame.routes = RoutesInFrameRelStructure(route=self.routes)
        #
        #     if self.stop_areas:
        #         service_frame.stop_areas = StopAreasInFrameRelStructure(stop_area=self.stop_areas)
        #
        #     service_frame.scheduled_stop_points = ScheduledStopPointsInFrameRelStructure(
        #         scheduled_stop_point=self.scheduled_stop_points)
        #
        #     if self.timing_links:
        #         service_frame.timing_links = TimingLinksInFrameRelStructure(timing_link=self.timing_links)
        #
        #     if self.time_demand_types:
        #         self.time_demand_types = sorted(self.time_demand_types, key=lambda x: x.id)
        #         service_frame.time_demand_types = TimeDemandTypesInFrameRelStructure(time_demand_type=self.time_demand_types)
        #
        #     """
        #     service_frame.destination_displays = DestinationDisplaysInFrameRelStructure(
        #         destination_display=destination_displays)
        #     service_frame.stop_assignments = StopAssignmentsInFrameRelStructure(
        #         passenger_stop_assignment=passenger_stop_assignments)
        #     """
        #
        #     if self.service_journey_patterns:
        #         service_frame.journey_patterns = JourneyPatternsInFrameRelStructure(choice=self.service_journey_patterns)
        #
        #
        return service_frame

    def get_service_id_ac(self, service_id: str) -> str:
        if ':AvailabilityCondition:' in service_id:
            return service_id
        elif ':DayType:' in service_id:
            return service_id.replace(':DayType:', ':AvailabilityCondition:')
        else:
            return getId(AvailabilityCondition, self.codespace, service_id)

    def get_service_id_dt(self, service_id: str) -> str:
        if ':DayType:' in service_id:
            return service_id
        if ':AvailabilityCondition:' in service_id:
            return service_id.replace(':AvailabilityCondition:', ':DayType:')
        else:
            return getId(DayType, self.codespace, service_id)

    def getAvailabilityConditions(
        self,
        availability_condition_sql: dict[str, str] = {'query': """select * from calendar order by service_id;"""},
        exceptions_sql: dict[str, str] = {
            'query': """select service_id, exception_type, array_agg(date order by date) as dates from calendar_dates group by service_id, exception_type;"""
        },
    ) -> list[AvailabilityCondition]:
        availability_conditions = []

        with self.conn.cursor() as cur:
            cur.execute(**exceptions_sql)
            exceptions_df = cur.df()
            exceptions: dict[str, list[AvailabilityCondition]] = {}

            service_ids = exceptions_df.get('service_id')
            for i in range(0, len(service_ids)):
                exception_type = int(exceptions_df['exception_type'][i])
                if exception_type in (1, 2):
                    ac = AvailabilityCondition(
                        id=self.get_service_id_ac(service_ids[i]) + '_' + str(exception_type),
                        private_codes=PrivateCodes(private_code=[PrivateCode(type_value="service_id", value=service_ids[i])]),
                        version=self.version.version,
                        is_available=exception_type == 1,
                        from_date=date_to_xmldatetime(gtfs_date(exceptions_df['dates'][i][0])),
                        to_date=date_to_xmldatetime(gtfs_date(exceptions_df['dates'][i][-1])),
                        valid_day_bits=getBitString2([gtfs_date(d) for d in exceptions_df['dates'][i]]),
                    )
                    exception = exceptions.get(service_ids[i], [])
                    exception.append(ac)
                    exceptions[service_ids[i]] = exception
                    availability_conditions.append(ac)

            cur.execute(**availability_condition_sql)
            df = cur.df()

            service_ids = df.get('service_id')
            mondays = df.get('monday')
            tuesdays = df.get('tuesday')
            wednesdays = df.get('wednesday')
            thursdays = df.get('thursday')
            fridays = df.get('friday')
            saturdays = df.get('saturday')
            sundays = df.get('sunday')
            start_dates = df.get('start_date')
            end_dates = df.get('end_date')

            for i in range(0, len(service_ids)):
                days_of_week = []
                if mondays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.MONDAY)
                if tuesdays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.TUESDAY)
                if wednesdays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.WEDNESDAY)
                if thursdays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.THURSDAY)
                if fridays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.FRIDAY)
                if saturdays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.SATURDAY)
                if sundays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.SUNDAY)

                availability_conditions.append(
                    AvailabilityCondition(
                        id=self.get_service_id_ac(service_ids[i]),
                        version=self.version.version,
                        private_codes=PrivateCodes(private_code=[PrivateCode(type_value="service_id", value=service_ids[i])]),
                        is_available=True,
                        from_date=date_to_xmldatetime(gtfs_date(start_dates[i])),
                        to_date=date_to_xmldatetime(gtfs_date(end_dates[i])),
                        day_types=DayTypesRelStructure(
                            day_type_ref_or_day_type=[
                                DayType(
                                    id=self.get_service_id_dt(service_ids[i]),
                                    version=self.version.version,
                                    properties=PropertiesOfDayRelStructure(property_of_day=[PropertyOfDay(days_of_week=days_of_week)]),
                                )
                            ]
                        ),
                    )
                )

        return availability_conditions

    def getDayTypes(
        self,
        day_type_sql: dict[str, str] = {'query': """select * from calendar order by service_id;"""},
        exceptions_sql: dict[str, str] = {'query': """select service_id, exception_type, date from calendar_dates order by date, exception_type;"""},
    ) -> tuple[list[DayType], list[DayTypeAssignment], list[OperatingPeriod]]:
        day_types = []
        day_type_assignments = []
        operating_periods = []
        fake_day_type_ids = set([])

        with self.conn.cursor() as cur:
            cur.execute(**exceptions_sql)
            exceptions_df = cur.df()

            service_ids = exceptions_df.get('service_id')
            for i in range(0, len(service_ids)):
                exception_type = int(exceptions_df['exception_type'][i])
                if exception_type in (1, 2):
                    day_type_id = getFakeRef(self.get_service_id_dt(service_ids[i]), DayTypeRef, self.version.version)
                    fake_day_type_ids.add(service_ids[i])
                    day_type_assignments.append(
                        DayTypeAssignment(
                            id=f"{self.get_service_id_dt(service_ids[i]).replace('DayType', 'DayTypeAssignment')}_{str(exceptions_df['date'][i])}_{str(exception_type)}",
                            version=self.version.version,
                            order=1,
                            day_type_ref=day_type_id,
                            uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date=date_to_xmldate(gtfs_date(exceptions_df['date'][i])),
                            is_available=True if exception_type == 1 else False,
                        )
                    )

            cur.execute(**day_type_sql)
            df = cur.df()

            service_ids = df.get('service_id')
            mondays = df.get('monday')
            tuesdays = df.get('tuesday')
            wednesdays = df.get('wednesday')
            thursdays = df.get('thursday')
            fridays = df.get('friday')
            saturdays = df.get('saturday')
            sundays = df.get('sunday')
            start_dates = df.get('start_date')
            end_dates = df.get('end_date')

            for i in range(0, len(service_ids)):
                days_of_week = []
                if mondays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.MONDAY)
                if tuesdays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.TUESDAY)
                if wednesdays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.WEDNESDAY)
                if thursdays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.THURSDAY)
                if fridays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.FRIDAY)
                if saturdays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.SATURDAY)
                if sundays[i] == 1:
                    days_of_week.append(DayOfWeekEnumeration.SUNDAY)

                day_type = DayType(
                    id=self.get_service_id_dt(service_ids[i]),
                    version=self.version.version,
                    private_codes=PrivateCodes(private_code=[PrivateCode(type_value="service_id", value=service_ids[i])]),
                    properties=PropertiesOfDayRelStructure(property_of_day=[PropertyOfDay(days_of_week=days_of_week)]),
                )
                day_types.append(day_type)
                try:
                    fake_day_type_ids.remove(service_ids[i])
                except KeyError:
                    pass

                operating_period = OperatingPeriod(
                    id=self.get_service_id_dt(service_ids[i]).replace('DayType', 'OperatingPeriod'),
                    version=self.version.version,
                    from_operating_day_ref_or_from_date=date_to_xmldatetime(gtfs_date(start_dates[i])),
                    to_operating_day_ref_or_to_date=date_to_xmldatetime(gtfs_date(end_dates[i])),
                )
                operating_periods.append(operating_period)

                day_type_assignments.append(
                    DayTypeAssignment(
                        id=self.get_service_id_dt(service_ids[i]).replace('DayType', 'DayTypeAssignment'),
                        version=self.version.version,
                        order=1,
                        day_type_ref=getFakeRef(self.get_service_id_dt(service_ids[i]), DayTypeRef, self.version.version),
                        uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date=cast(OperatingPeriodRef, getRef(operating_period)),
                    )
                )

        for service_id in fake_day_type_ids:
            day_type = DayType(
                id=self.get_service_id_dt(service_id),
                version=self.version.version,
                private_codes=PrivateCodes(private_code=[PrivateCode(type_value="service_id", value=service_id)]),
            )
            day_types.append(day_type)

        return day_types, day_type_assignments, operating_periods

    def getInterchangeRules(
        self,
        transfers_sql: dict[str, str] = {
            'query': (
                """select transfers.*, from_stop.location_type as from_stop_location_type, to_stop.location_type as to_stop_location_type from transfers join stops as from_stop on (from_stop_id = from_stop.stop_id) join stops as to_stop on (to_stop_id = to_stop.stop_id) order by from_route_id, to_route_id, from_trip_id, to_trip_id, from_stop_id, to_stop_id;"""
            )
        },
    ) -> Generator[InterchangeRule, None, None]:
        # from_stop_id, to_stop_id, from_route_id, to_route_id, from_trip_id, to_trip_id, transfer_type, min_transfer_time:
        with self.conn.cursor() as cur:
            cur.execute(**transfers_sql)
            transfers_df = cur.df()

            from_stop_ids = transfers_df.get('from_stop_id')
            from_stop_location_types = transfers_df.get('from_stop_location_type')
            to_stop_ids = transfers_df.get('to_stop_id')
            to_stop_location_types = transfers_df.get('to_stop_location_type')
            from_route_ids = transfers_df.get('from_route_id')
            to_route_ids = transfers_df.get('to_route_id')
            from_trip_ids = transfers_df.get('from_trip_id')
            to_trip_ids = transfers_df.get('from_trip_id')
            transfer_types = transfers_df.get('transfer_type')
            min_transfer_times = transfers_df.get('min_transfer_time')

            for i in range(0, len(transfer_types)):
                feeder_filter = InterchangeRuleParameterStructure(
                    scheduled_stop_point_ref=(
                        getFakeRef(getId(ScheduledStopPoint, self.codespace, from_stop_ids[i]), ScheduledStopPointRef, self.version.version)
                        if from_stop_location_types[i] == 0
                        else None
                    ),
                    stop_place_ref=(
                        getFakeRef(getId(StopPlace, self.codespace, from_stop_ids[i]), StopPlaceRef, self.version.version)
                        if from_stop_location_types[i] == 1
                        else None
                    ),
                    all_lines_or_lines_in_direction_refs_or_line_in_direction_ref=(
                        [
                            LineInDirectionRef(line_ref=getFakeRef(getId(Line, self.codespace, from_route_ids[i]), LineRef, self.version.version)),
                        ]
                        if from_route_ids[i]
                        else [EmptyType2(value='')]
                    ),
                    service_journey_ref_or_journey_designator_or_service_designator=(
                        getFakeRef(getId(ServiceJourney, self.codespace, from_trip_ids[i]), ServiceJourneyRefStructure, self.version.version)
                        if from_trip_ids[i]
                        else None
                    ),
                )

                distributor_filter = InterchangeRuleParameterStructure(
                    scheduled_stop_point_ref=(
                        getFakeRef(getId(ScheduledStopPoint, self.codespace, to_stop_ids[i]), ScheduledStopPointRef, self.version.version)
                        if to_stop_location_types[i] == 0
                        else None
                    ),
                    stop_place_ref=(
                        getFakeRef(getId(StopPlace, self.codespace, to_stop_ids[i]), StopPlaceRef, self.version.version)
                        if to_stop_location_types[i] == 1
                        else None
                    ),
                    all_lines_or_lines_in_direction_refs_or_line_in_direction_ref=(
                        [
                            LineInDirectionRef(line_ref=getFakeRef(getId(Line, self.codespace, to_route_ids[i]), LineRef, self.version.version)),
                        ]
                        if to_route_ids[i]
                        else [EmptyType2(value='')]
                    ),
                    service_journey_ref_or_journey_designator_or_service_designator=(
                        getFakeRef(getId(ServiceJourney, self.codespace, to_trip_ids[i]), ServiceJourneyRefStructure, self.version.version)
                        if to_trip_ids[i]
                        else None
                    ),
                )

                id = getId(
                    InterchangeRule,
                    self.codespace,
                    hashlib.md5(
                        (
                            ';'.join(
                                [
                                    str(from_stop_ids[i]),
                                    str(to_stop_ids[i]),
                                    str(from_route_ids[i]),
                                    str(to_route_ids[i]),
                                    str(from_trip_ids[i]),
                                    str(to_trip_ids[i]),
                                    str(transfer_types[i]),
                                    str(min_transfer_times[i]),
                                ]
                            )
                        ).encode('utf-8')
                    ).hexdigest()[0:5],
                )

                if pd.isna(transfer_types[i]) or transfer_types[i] == 0 :
                    # Recommended
                    yield InterchangeRule(
                        advertised=True, id=id, version=self.version.version, feeder_filter=feeder_filter, distributor_filter=distributor_filter
                    )

                elif transfer_types[i] == 1:
                    # Timed transfer point between two routes
                    yield InterchangeRule(
                        advertised=True,
                        planned=True,
                        guaranteed=True,
                        id=id,
                        version=self.version.version,
                        feeder_filter=feeder_filter,
                        distributor_filter=distributor_filter,
                    )

                elif transfer_types[i] == 2:
                    # Transfer requires a minimum amount of time
                    yield InterchangeRule(
                        advertised=True,
                        planned=True,
                        minimum_transfer_time=XmlDuration(value=f"PT{int(min_transfer_times[i])}S") if min_transfer_times[i] else None,
                        id=id,
                        version=self.version.version,
                        feeder_filter=feeder_filter,
                        distributor_filter=distributor_filter,
                    )

                elif transfer_types[i] == 3:
                    # Transfers are not possible
                    yield InterchangeRule(exclude=True, id=id, version=self.version.version, feeder_filter=feeder_filter, distributor_filter=distributor_filter)

                elif transfer_types[i] == 4:
                    # In-seat transfer
                    yield InterchangeRule(
                        advertised=True,
                        planned=True,
                        stay_seated=True,
                        guaranteed=True,
                        id=id,
                        version=self.version.version,
                        feeder_filter=feeder_filter,
                        distributor_filter=distributor_filter,
                    )

                elif transfer_types[i] == 5:
                    # In-seat transfer not allowed
                    yield InterchangeRule(
                        stay_seated=False, id=id, version=self.version.version, feeder_filter=feeder_filter, distributor_filter=distributor_filter
                    )

    @staticmethod
    def noonTimeToNeTEx(time: str) -> tuple[XmlTime, int]:
        hour, minute, second = time.split(':')
        hour_i = int(hour)
        day_offset = int(math.floor(hour_i / 24))
        return (XmlTime(hour=hour_i % 24, minute=int(minute), second=int(second)), day_offset)

    @staticmethod
    def directionToNeTEx(direction_id: int | None) -> DirectionType | None:
        if direction_id is None:
            return None

        elif direction_id == 1:
            return DirectionType(value=DirectionTypeEnumeration.INBOUND)

        return DirectionType(value=DirectionTypeEnumeration.OUTBOUND)

    @staticmethod
    def wheelchairToNeTEx(wheelchair_accessible: int) -> LimitationStatusEnumeration:
        if wheelchair_accessible == 1:
            return LimitationStatusEnumeration.TRUE

        elif wheelchair_accessible == 2:
            return LimitationStatusEnumeration.FALSE

        return LimitationStatusEnumeration.UNKNOWN

    @staticmethod
    def bicyclesToNeTEx(bikes_allowed: int) -> LuggageCarriageEnumeration:
        if bikes_allowed == 1:
            return LuggageCarriageEnumeration.CYCLES_ALLOWED

        elif bikes_allowed == 2:
            return LuggageCarriageEnumeration.NO_CYCLES

        return LuggageCarriageEnumeration.UNKNOWN

    def gtfs_shape_to_linestring(self, shape_sql: dict[str, str] = {'query': """select * from shapes order by shape_id;"""}) -> None:

        with self.conn.cursor() as cur:
            cur.execute(**shape_sql)

    def get_trip_id(self, trip_id: str) -> str:
        if ':ServiceJourney:' in trip_id:
            return trip_id
        else:
            return getId(ServiceJourney, self.codespace, trip_id)

    def get_trip_id_aa(self, trip_id: str) -> str:
        if ':ServiceJourney:' in trip_id:
            return trip_id.replace(':ServiceJourney:', ':AccessibilityAssessment:')
        else:
            return getId(AccessibilityAssessment, self.codespace, trip_id)

    def get_trip_id_sfs(self, trip_id: str) -> str:
        if ':ServiceJourney:' in trip_id:
            return trip_id.replace(':ServiceJourney:', ':ServiceFacilitySet:')
        else:
            return getId(ServiceFacilitySet, self.codespace, trip_id)

    def get_trip_id_call(self, trip_id: str, sequence: int) -> str:
        if ':ServiceJourney:' in trip_id:
            return trip_id.replace(':ServiceJourney:', ':Call:') + '_' + str(sequence)
        elif ':TemplateServiceJourney:' in trip_id:
            return trip_id.replace(':TemplateServiceJourney:', ':Call:') + '_' + str(sequence)
        else:
            return getId(Call, self.codespace, trip_id) + '_' + str(sequence)

    def getServiceJourneys(
        self,
        availability_conditions_input: list[AvailabilityCondition],
        trips_sql: dict[str, str] = {'query': """select * from trips where trip_id not in (select trip_id from frequencies) order by trip_id;"""},
        stop_times_sql: dict[str, str] = {'query': """select * from stop_times order by trip_id, stop_sequence;"""},
    ) -> Generator[ServiceJourney, None, None]:
        service_journey: ServiceJourney | None = None

        availability_conditions = getIndex(availability_conditions_input)

        service_journeys = {}
        shape_used = set([])

        with self.conn.cursor() as cur:
            cur.execute(**trips_sql)

            df = cur.df()

            route_ids = df.get('route_id')
            trip_ids = df.get('trip_id')
            service_ids = df.get('service_id')
            trip_short_names = df.get('trip_short_name')
            trip_headsigns = df.get('trip_headsign')
            # route_short_names = df.get('route_short_name')
            direction_ids = df.get('direction_id')
            block_ids = df.get('block_id')
            shape_ids = df.get('shape_id')
            wheelchair_accessibles = df.get('wheelchair_accessible')
            # trip_bikes_alloweds = df.get('trip_bikes_allowed')
            bikes_alloweds = df.get('bikes_allowed')
            # ticketing_trip_ids = df.get('ticketing_trip_id')
            # ticketing_types = df.get('ticketing_type')

            for i in range(0, len(route_ids)):
                availability_condition_key = self.get_service_id_ac(service_ids[i])

                availability_conditions_journey = [
                    availability_conditions.get(availability_condition_key, None),
                    availability_conditions.get(availability_condition_key + "_1", None),
                    availability_conditions.get(availability_condition_key + "_2", None),
                ]

                journey_pattern_view = None
                if trip_headsigns[i] is not None:
                    journey_pattern_view = JourneyPatternView(
                        destination_display_ref_or_destination_display_view=DestinationDisplayView(
                            name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
                        )
                    )

                accessibility_assessment = None
                if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
                    accessibility_assessment = AccessibilityAssessment(
                        id=self.get_trip_id_aa(trip_ids[i]),
                        version=self.version.version,
                        mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
                    )

                block_ref = None
                if block_ids[i] is not None:
                    block_ref = getFakeRef(getId(Block, self.codespace, block_ids[i]), BlockRef, None, "EXTERNAL")

                # route_ref = None
                lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
                shape_id = get_or_none(shape_ids, i)
                if shape_id is not None:
                    if shape_id in shape_used:
                        lsp = getFakeRef(self.get_shape_id_lsp(shape_id), LinkSequenceProjectionRef, self.version.version)
                    else:
                        lsps = self.getLineStrings(
                            {
                                'query': (
                                    """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
                                ),
                                'parameters': (shape_id,),
                            }
                        )
                        if len(lsps) > 0:
                            lsp = lsps[0]

                        shape_used.add(shape_id)

                luggage_carriage_facility_list = []
                facitities = None
                bikes_allowed = get_or_none(bikes_alloweds, i)
                if bikes_allowed is not None:
                    if bikes_allowed == 1:
                        luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
                    elif bikes_allowed == 2:
                        luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)

                if len(luggage_carriage_facility_list) > 0:
                    facitities = ServiceFacilitySetsRelStructure(
                        restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
                            ServiceFacilitySet(
                                id=self.get_trip_id_sfs(trip_ids[i]),
                                version=self.version.version,
                                luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
                            )
                        ]
                    )

                service_journey = ServiceJourney(
                    id=self.get_trip_id(trip_ids[i]),
                    version=self.version.version,
                    flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(self.get_route_id(route_ids[i]), LineRef, self.version.version),
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
                    short_name=getOptionalString(get_or_none(trip_short_names, i)),
                    validity_conditions_or_valid_between=[
                        ValidityConditionsRelStructure(
                            choice=cast(
                                list[
                                    Union[
                                        AvailabilityConditionRef,
                                        ValidityRuleParameterRef,
                                        ValidityTriggerRef,
                                        ValidityConditionRef,
                                        ValidBetween,
                                        SimpleAvailabilityCondition,
                                        ValidDuring,
                                        AvailabilityCondition,
                                        ValidityRuleParameter,
                                        ValidityTrigger,
                                        ValidityCondition,
                                    ]
                                ],
                                [getRef(x, AvailabilityConditionRef) for x in availability_conditions_journey if x is not None],
                            )
                        )
                    ],
                    journey_pattern_view=journey_pattern_view,
                    direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
                    block_ref=block_ref,
                    accessibility_assessment=accessibility_assessment,
                    facilities=facitities,
                    link_sequence_projection_ref_or_link_sequence_projection=lsp,
                )

                service_journeys[trip_ids[i]] = service_journey

        with self.conn.cursor() as cur:
            cur.execute(**stop_times_sql)
            trip_id = None
            service_journey = None
            prev_call = None
            prev_shape_traveled = 0
            prev_order = 1

            df = cur.df()

            trip_ids = df.get('trip_id')
            stop_headsigns = df.get('stop_headsign')
            stop_ids = df.get('stop_id')
            arrival_times = df.get('arrival_time')
            departure_times = df.get('departure_time')
            shape_dist_traveleds = df.get('shape_dist_traveled')
            drop_off_types = df.get('drop_off_type')
            pickup_types = df.get('pickup_type')
            stop_sequences = df.get('stop_sequence')

            for i in range(0, len(trip_ids)):
                if trip_ids[i] != trip_id:
                    if trip_id is not None and service_journey is not None:
                        yield service_journey
                        service_journey.calls = None  # Free memory

                    trip_id = trip_ids[i]
                    service_journey = service_journeys[trip_id]
                    service_journey.calls = CallsRelStructure()
                    prev_call = None
                    prev_shape_traveled = 0
                    prev_order = 1

                destination_display_view = None
                stop_headsign = get_or_none(stop_headsigns, i)
                if stop_headsign is not None:
                    destination_display_view = DestinationDisplayView(
                        name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
                    )

                from_point_ref = getId(ScheduledStopPoint, self.codespace, stop_ids[i])
                arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[i])
                departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[i])

                shape_dist_traveled = get_or_none(shape_dist_traveleds, i)
                if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
                    distance = shape_dist_traveled - prev_shape_traveled
                    prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)

                call = Call(
                    id=self.get_trip_id_call(trip_ids[i], stop_sequences[i]),
                    version=self.version.version,
                    fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
                        from_point_ref, ScheduledStopPointRef, self.version.version
                    ),
                    destination_display_ref_or_destination_display_view=destination_display_view,
                    arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off_types[i] != 1)),
                    departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup_types[i] != 1)),
                    request_stop=bool(pickup_types[i] == 2 or pickup_types[i] == 3 or drop_off_types[i] == 2 or drop_off_types[i] == 3),
                    order=prev_order,
                )  # stop_sequence is non-negative integer

                assert service_journey is not None and service_journey.calls is not None
                service_journey.calls.call.append(call)

                prev_call = call
                if shape_dist_traveled:
                    prev_shape_traveled = shape_dist_traveled
                prev_order += 1

            if trip_id is not None and service_journey is not None:
                yield service_journey

    # TODO: Shares too much code, clean up
    def getServiceJourneysDayType(
        self,
        trips_sql: dict[str, str] = {'query': """select * from trips where trip_id not in (select trip_id from frequencies) order by trip_id;"""},
        stop_times_sql: dict[str, str] = {'query': """select * from stop_times order by trip_id, stop_sequence;"""},
    ) -> Generator[ServiceJourney, None, None]:
        service_journey: ServiceJourney | None
        service_journeys = {}
        shape_used = set([])

        with self.conn.cursor() as cur:
            cur.execute(**trips_sql)

            df = cur.df()

            route_ids = df.get('route_id')
            trip_ids = df.get('trip_id')
            service_ids = df.get('service_id')
            trip_short_names = df.get('trip_short_name')
            trip_headsigns = df.get('trip_headsign')
            # route_short_names = df.get('route_short_name')
            direction_ids = df.get('direction_id')
            block_ids = df.get('block_id')
            shape_ids = df.get('shape_id')
            wheelchair_accessibles = df.get('wheelchair_accessible')
            # trip_bikes_alloweds = df.get('trip_bikes_allowed')
            bikes_alloweds = df.get('bikes_allowed')
            # ticketing_trip_ids = df.get('ticketing_trip_id')
            # ticketing_types = df.get('ticketing_type')

            for i in range(0, len(route_ids)):
                journey_pattern_view = None
                if trip_headsigns[i] is not None:
                    journey_pattern_view = JourneyPatternView(
                        destination_display_ref_or_destination_display_view=DestinationDisplayView(
                            name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
                        )
                    )

                accessibility_assessment = None
                if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
                    accessibility_assessment = AccessibilityAssessment(
                        id=self.get_trip_id_aa(trip_ids[i]),
                        version=self.version.version,
                        mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
                    )

                block_ref = None
                if block_ids[i] is not None:
                    block_ref = getFakeRef(getId(Block, self.codespace, block_ids[i]), BlockRef, None, "EXTERNAL")

                # route_ref = None
                lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
                shape_id = get_or_none(shape_ids, i)
                if shape_id is not None:
                    if shape_id in shape_used:
                        lsp = getFakeRef(self.get_shape_id_lsp(shape_id), LinkSequenceProjectionRef, self.version.version)
                    else:
                        lsps = self.getLineStrings(
                            {
                                'query': (
                                    """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
                                ),
                                'parameters': (shape_id,),
                            }
                        )
                        if len(lsps) > 0:
                            lsp = lsps[0]

                        shape_used.add(shape_id)

                luggage_carriage_facility_list = []
                facitities = None
                bikes_allowed = get_or_none(bikes_alloweds, i)
                if bikes_allowed is not None:
                    if bikes_allowed == 1:
                        luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
                    elif bikes_allowed == 2:
                        luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)

                if len(luggage_carriage_facility_list) > 0:
                    facitities = ServiceFacilitySetsRelStructure(
                        restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
                            ServiceFacilitySet(
                                id=self.get_trip_id_sfs(trip_ids[i]),
                                version=self.version.version,
                                luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
                            )
                        ]
                    )

                service_journey = ServiceJourney(
                    id=self.get_trip_id(trip_ids[i]),
                    version=self.version.version,
                    flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(self.get_route_id(route_ids[i]), LineRef, self.version.version),
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
                    short_name=getOptionalString(get_or_none(trip_short_names, i)),
                    day_types=DayTypeRefsRelStructure(day_type_ref=[getFakeRef(self.get_service_id_dt(service_ids[i]), DayTypeRef, self.version.version)]),
                    journey_pattern_view=journey_pattern_view,
                    direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
                    block_ref=block_ref,
                    accessibility_assessment=accessibility_assessment,
                    facilities=facitities,
                    link_sequence_projection_ref_or_link_sequence_projection=lsp,
                )

                service_journeys[trip_ids[i]] = service_journey

        with self.conn.cursor() as cur:
            cur.execute(**stop_times_sql)
            trip_id = None
            service_journey = None
            prev_call = None
            prev_shape_traveled = 0
            prev_order = 1

            df = cur.df()

            trip_ids = df.get('trip_id')
            stop_headsigns = df.get('stop_headsign')
            stop_ids = df.get('stop_id')
            arrival_times = df.get('arrival_time')
            departure_times = df.get('departure_time')
            shape_dist_traveleds = df.get('shape_dist_traveled')
            drop_off_types = df.get('drop_off_type')
            pickup_types = df.get('pickup_type')
            stop_sequences = df.get('stop_sequence')

            for i in range(0, len(trip_ids)):
                if trip_ids[i] != trip_id:
                    if trip_id is not None and service_journey is not None:
                        yield service_journey

                    trip_id = trip_ids[i]
                    service_journey = service_journeys[trip_id]
                    service_journey.calls = CallsRelStructure()
                    prev_call = None
                    prev_shape_traveled = 0
                    prev_order = 1

                destination_display_view = None
                stop_headsign = get_or_none(stop_headsigns, i)
                if stop_headsign is not None:
                    destination_display_view = DestinationDisplayView(
                        name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
                    )

                from_point_ref = getId(ScheduledStopPoint, self.codespace, stop_ids[i])
                arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[i])
                departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[i])

                shape_dist_traveled = get_or_none(shape_dist_traveleds, i)
                if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
                    distance = shape_dist_traveled - prev_shape_traveled
                    prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)

                call = Call(
                    id=self.get_trip_id_call(trip_ids[i], stop_sequences[i]),
                    version=self.version.version,
                    fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
                        from_point_ref, ScheduledStopPointRef, self.version.version
                    ),
                    destination_display_ref_or_destination_display_view=destination_display_view,
                    arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off_types[i] != 1)),
                    departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup_types[i] != 1)),
                    request_stop=bool(pickup_types[i] == 2 or pickup_types[i] == 3 or drop_off_types[i] == 2 or drop_off_types[i] == 3),
                    order=prev_order,
                )  # stop_sequence is non-negative integer

                assert service_journey is not None and service_journey.calls is not None
                service_journey.calls.call.append(call)

                prev_call = call
                if shape_dist_traveled:
                    prev_shape_traveled = shape_dist_traveled
                prev_order += 1

            if trip_id is not None and service_journey is not None:
                yield service_journey

    def getServiceJourneys2(
        self,
        availability_conditions_input: list[AvailabilityCondition],
        trips_sql: dict[str, str] = {'query': """select * from trips where trip_id not in (select trip_id from frequencies) order by trip_id;"""},
    ) -> Generator[ServiceJourney, None, None]:
        availability_conditions = getIndex(availability_conditions_input)

        shape_used = set([])

        with self.conn.cursor() as cur:
            cur.execute(**trips_sql)

            df = cur.df()

            route_ids = df.get('route_id')
            trip_ids = df.get('trip_id')
            service_ids = df.get('service_id')
            trip_short_names = df.get('trip_short_name')
            trip_headsigns = df.get('trip_headsign')
            # route_short_names = df.get('route_short_name')
            direction_ids = df.get('direction_id')
            block_ids = df.get('block_id')
            shape_ids = df.get('shape_id')
            wheelchair_accessibles = df.get('wheelchair_accessible')
            # trip_bikes_alloweds = df.get('trip_bikes_allowed')
            bikes_alloweds = df.get('bikes_allowed')
            # ticketing_trip_ids = df.get('ticketing_trip_id')
            # ticketing_types = df.get('ticketing_type')

            for i in range(0, len(route_ids)):
                availability_condition_key = getId(AvailabilityCondition, self.codespace, service_ids[i])

                availability_conditions_journey = [
                    availability_conditions.get(availability_condition_key, None),
                    availability_conditions.get(availability_condition_key + "_1", None),
                    availability_conditions.get(availability_condition_key + "_2", None),
                ]

                journey_pattern_view = None
                if trip_headsigns[i] is not None:
                    journey_pattern_view = JourneyPatternView(
                        destination_display_ref_or_destination_display_view=DestinationDisplayView(
                            name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
                        )
                    )

                accessibility_assessment = None
                if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
                    accessibility_assessment = AccessibilityAssessment(
                        id=self.get_trip_id_aa(trip_ids[i]),
                        version=self.version.version,
                        mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
                    )

                block_ref = None
                if block_ids[i] is not None:
                    block_ref = getFakeRef(getId(Block, self.codespace, block_ids[i]), BlockRef, None, "EXTERNAL")

                # route_ref = None
                lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
                shape_id = get_or_none(shape_ids, i)
                if shape_id is not None:
                    if shape_id in shape_used:
                        lsp = getFakeRef(getId(LinkSequenceProjection, self.codespace, shape_id), LinkSequenceProjectionRef, self.version.version)
                    else:
                        lsps = self.getLineStrings(
                            {
                                'query': (
                                    """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
                                ),
                                'parameters': (shape_id,),
                            }
                        )
                        if len(lsps) > 0:
                            lsp = lsps[0]

                        shape_used.add(shape_id)

                luggage_carriage_facility_list = []
                facitities = None
                bikes_allowed = get_or_none(bikes_alloweds, i)
                if bikes_allowed is not None:
                    if bikes_allowed == 1:
                        luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
                    elif bikes_allowed == 2:
                        luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)

                if len(luggage_carriage_facility_list) > 0:
                    facitities = ServiceFacilitySetsRelStructure(
                        restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
                            ServiceFacilitySet(
                                id=self.get_trip_id_sfs(trip_ids[i]),
                                version=self.version.version,
                                luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
                            )
                        ]
                    )

                calls = CallsRelStructure()

                with self.conn.cursor() as cur2:
                    cur2.execute(**{'query': """select * from stop_times where trip_id = ? order by stop_sequence;""", 'parameters': (trip_ids[i],)})
                    # trip_id = None
                    # service_journey = None
                    prev_call = None
                    prev_shape_traveled = 0
                    prev_order = 1

                    df2 = cur2.df()

                    stop_headsigns = df2.get('stop_headsign')
                    stop_ids = df2.get('stop_id')
                    arrival_times = df2.get('arrival_time')
                    departure_times = df2.get('departure_time')
                    shape_dist_traveleds = df2.get('shape_dist_traveled')
                    drop_off_types = df2.get('drop_off_type')
                    pickup_types = df2.get('pickup_type')
                    stop_sequences = df2.get('stop_sequence')

                    for index_j in range(0, len(stop_ids)):
                        destination_display_view = None
                        stop_headsign = get_or_none(stop_headsigns, index_j)
                        if stop_headsign is not None:
                            destination_display_view = DestinationDisplayView(
                                name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
                            )

                        from_point_ref = getId(ScheduledStopPoint, self.codespace, stop_ids[index_j])
                        arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[index_j])
                        departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[index_j])

                        shape_dist_traveled = get_or_none(shape_dist_traveleds, index_j)
                        if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
                            distance = shape_dist_traveled - prev_shape_traveled
                            prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)

                        call = Call(
                            id=self.get_trip_id_call(trip_ids[i], stop_sequences[index_j]),
                            version=self.version.version,
                            fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
                                from_point_ref, ScheduledStopPointRef, self.version.version
                            ),
                            destination_display_ref_or_destination_display_view=destination_display_view,
                            arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off_types[index_j] != 1)),
                            departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup_types[index_j] != 1)),
                            request_stop=bool(
                                pickup_types[index_j] == 2 or pickup_types[index_j] == 3 or drop_off_types[index_j] == 2 or drop_off_types[index_j] == 3
                            ),
                            order=prev_order,
                        )  # stop_sequence is non-negative integer

                        assert calls is not None
                        calls.call.append(call)

                        prev_call = call
                        if shape_dist_traveled:
                            prev_shape_traveled = shape_dist_traveled
                        prev_order += 1

                service_journey = ServiceJourney(
                    id=self.get_trip_id(trip_ids[i]),
                    version=self.version.version,
                    flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(
                        getId(Line, self.codespace, route_ids[i]), LineRef, self.version.version
                    ),
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
                    short_name=getOptionalString(get_or_none(trip_short_names, i)),
                    validity_conditions_or_valid_between=[
                        ValidityConditionsRelStructure(
                            choice=cast(
                                list[
                                    Union[
                                        AvailabilityConditionRef,
                                        ValidityRuleParameterRef,
                                        ValidityTriggerRef,
                                        ValidityConditionRef,
                                        ValidBetween,
                                        SimpleAvailabilityCondition,
                                        ValidDuring,
                                        AvailabilityCondition,
                                        ValidityRuleParameter,
                                        ValidityTrigger,
                                        ValidityCondition,
                                    ]
                                ],
                                [getRef(x, AvailabilityConditionRef) for x in availability_conditions_journey if x is not None],
                            )
                        )
                    ],
                    journey_pattern_view=journey_pattern_view,
                    direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
                    block_ref=block_ref,
                    accessibility_assessment=accessibility_assessment,
                    facilities=facitities,
                    link_sequence_projection_ref_or_link_sequence_projection=lsp,
                    calls=calls,
                )

                yield service_journey

    def getServiceJourneys2DayType(
        self, trips_sql: dict[str, str] = {'query': """select * from trips where trip_id not in (select trip_id from frequencies) order by trip_id;"""}
    ) -> Generator[ServiceJourney, None, None]:

        shape_used = set([])

        with (self.conn.cursor() as cur):
            cur.execute(**trips_sql)

            df = cur.df()

            route_ids = df.get('route_id')
            trip_ids = df.get('trip_id')
            service_ids = df.get('service_id')
            trip_short_names = df.get('trip_short_name')
            trip_headsigns = df.get('trip_headsign')
            # route_short_names = df.get('route_short_name')
            direction_ids = df.get('direction_id')
            block_ids = df.get('block_id')
            shape_ids = df.get('shape_id')
            wheelchair_accessibles = df.get('wheelchair_accessible')
            # trip_bikes_alloweds = df.get('trip_bikes_allowed')
            bikes_alloweds = df.get('bikes_allowed')
            # ticketing_trip_ids = df.get('ticketing_trip_id')
            # ticketing_types = df.get('ticketing_type')

            for i in range(0, len(route_ids)):
                journey_pattern_view = None
                if trip_headsigns[i] is not None:
                    journey_pattern_view = JourneyPatternView(
                        destination_display_ref_or_destination_display_view=DestinationDisplayView(
                            name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
                        )
                    )

                accessibility_assessment = None
                if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
                    accessibility_assessment = AccessibilityAssessment(
                        id=self.get_trip_id_aa(trip_ids[i]),
                        version=self.version.version,
                        mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
                    )

                block_ref = None
                if block_ids[i] is not None:
                    block_ref = getFakeRef(getId(Block, self.codespace, block_ids[i]), BlockRef, None, "EXTERNAL")

                # route_ref = None
                lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
                shape_id = get_or_none(shape_ids, i)
                if shape_id is not None:
                    if shape_id in shape_used:
                        lsp = getFakeRef(getId(LinkSequenceProjection, self.codespace, shape_id), LinkSequenceProjectionRef, self.version.version)
                    else:
                        lsps = self.getLineStrings(
                            {
                                'query': (
                                    """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
                                ),
                                'parameters': (shape_id,),
                            }
                        )
                        if len(lsps) > 0:
                            lsp = lsps[0]

                        shape_used.add(shape_id)

                luggage_carriage_facility_list = []
                facitities = None
                bikes_allowed = get_or_none(bikes_alloweds, i)
                if bikes_allowed is not None:
                    if bikes_allowed == 1:
                        luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
                    elif bikes_allowed == 2:
                        luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)

                if len(luggage_carriage_facility_list) > 0:
                    facitities = ServiceFacilitySetsRelStructure(
                        restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
                            ServiceFacilitySet(
                                id=self.get_trip_id_sfs(trip_ids[i]),
                                version=self.version.version,
                                luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
                            )
                        ]
                    )

                calls = CallsRelStructure()

                with self.conn.cursor() as cur2:
                    cur2.execute(**{'query': """select * from stop_times where trip_id = ? order by stop_sequence;""", 'parameters': (trip_ids[i],)})
                    # trip_id = None
                    # service_journey = None
                    prev_call = None
                    prev_shape_traveled = 0
                    prev_order = 1

                    df2 = cur2.df()

                    stop_headsigns = df2.get('stop_headsign')
                    stop_ids = df2.get('stop_id')
                    arrival_times = df2.get('arrival_time')
                    departure_times = df2.get('departure_time')
                    shape_dist_traveleds = df2.get('shape_dist_traveled')
                    drop_off_types = df2.get('drop_off_type')
                    pickup_types = df2.get('pickup_type')
                    stop_sequences = df2.get('stop_sequence')

                    for index_j in range(0, len(stop_ids)):
                        destination_display_view = None
                        stop_headsign = get_or_none(stop_headsigns, index_j)
                        if stop_headsign is not None:
                            destination_display_view = DestinationDisplayView(
                                name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
                            )

                        from_point_ref = getId(ScheduledStopPoint, self.codespace, stop_ids[index_j])
                        arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[index_j])
                        departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[index_j])

                        shape_dist_traveled = get_or_none(shape_dist_traveleds, index_j)
                        if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
                            distance = shape_dist_traveled - prev_shape_traveled
                            prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)
                        pickup = 0 if pd.isna(pickup_types[index_j]) else pickup_types[index_j]
                        drop_off= 0 if pd.isna(drop_off_types[index_j]) else drop_off_types[index_j]
                        call = Call(
                            id=self.get_trip_id_call(trip_ids[i], stop_sequences[index_j]),
                            version=self.version.version,
                            fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
                                from_point_ref, ScheduledStopPointRef, self.version.version
                            ),
                            destination_display_ref_or_destination_display_view=destination_display_view,
                            arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off != 1)),
                            departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup != 1)),
                            request_stop=bool( pickup == 2 or pickup == 3 or drop_off == 2 or drop_off == 3   ),
                            order=prev_order,
                        )  # stop_sequence is non-negative integer

                        calls.call.append(call)

                        prev_call = call
                        if shape_dist_traveled:
                            prev_shape_traveled = shape_dist_traveled
                        prev_order += 1

                service_journey = ServiceJourney(
                    id=self.get_trip_id(trip_ids[i]),
                    version=self.version.version,
                    flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(
                        getId(Line, self.codespace, route_ids[i]), LineRef, self.version.version
                    ),
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
                    short_name=getOptionalString(get_or_none(trip_short_names, i)),
                    day_types=DayTypeRefsRelStructure(day_type_ref=[getFakeRef(self.get_service_id_dt(service_ids[i]), DayTypeRef, self.version.version)]),
                    journey_pattern_view=journey_pattern_view,
                    direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
                    block_ref=block_ref,
                    accessibility_assessment=accessibility_assessment,
                    facilities=facitities,
                    link_sequence_projection_ref_or_link_sequence_projection=lsp,
                    calls=calls,
                )

                yield service_journey

    def get_trip_id_tsj(self, trip_id: str) -> str:
        if ':TemplateServiceJourney:' in trip_id:
            return trip_id
        if ':ServiceJourney:' in trip_id:
            return trip_id.replace(':ServiceJourney:', ':TemplateServiceJourney:')
        else:
            return getId(TemplateServiceJourney, self.codespace, trip_id)

    def getTemplateServiceJourneys(
        self,
        availability_conditions_input: list[AvailabilityCondition],
        trips_sql: dict[str, str] = {'query': """select * from trips WHERE trip_id IN (SELECT trip_id FROM frequencies) order by trip_id;"""},
    ) -> Generator[TemplateServiceJourney, None, None]:
        availability_conditions = getIndex(availability_conditions_input)

        shape_used = set([])

        with self.conn.cursor() as cur:
            cur.execute(**trips_sql)

            df = cur.df()

            route_ids = df.get('route_id')
            trip_ids = df.get('trip_id')
            service_ids = df.get('service_id')
            trip_short_names = df.get('trip_short_name')
            trip_headsigns = df.get('trip_headsign')
            # route_short_names = df.get('route_short_name')
            direction_ids = df.get('direction_id')
            block_ids = df.get('block_id')
            shape_ids = df.get('shape_id')
            wheelchair_accessibles = df.get('wheelchair_accessible')
            # trip_bikes_alloweds = df.get('trip_bikes_allowed')
            bikes_alloweds = df.get('bikes_allowed')
            # ticketing_trip_ids = df.get('ticketing_trip_id')
            # ticketing_types = df.get('ticketing_type')

            for i in range(0, len(route_ids)):
                availability_condition_key = getId(AvailabilityCondition, self.codespace, service_ids[i])

                availability_conditions_journey = [
                    availability_conditions.get(availability_condition_key, None),
                    availability_conditions.get(availability_condition_key + "_1", None),
                    availability_conditions.get(availability_condition_key + "_2", None),
                ]

                journey_pattern_view = None
                if trip_headsigns[i] is not None:
                    journey_pattern_view = JourneyPatternView(
                        destination_display_ref_or_destination_display_view=DestinationDisplayView(
                            name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
                        )
                    )

                accessibility_assessment = None
                if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
                    accessibility_assessment = AccessibilityAssessment(
                        id=self.get_trip_id_aa(trip_ids[i]),
                        version=self.version.version,
                        mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
                    )

                block_ref = None
                if block_ids[i] is not None:
                    block_ref = getFakeRef(getId(Block, self.codespace, block_ids[i]), BlockRef, None)

                # route_ref = None
                lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
                shape_id = get_or_none(shape_ids, i)
                if shape_id is not None:
                    if shape_id in shape_used:
                        lsp = getFakeRef(getId(LinkSequenceProjection, self.codespace, shape_id), LinkSequenceProjectionRef, self.version.version)
                    else:
                        lsps = self.getLineStrings(
                            {
                                'query': (
                                    """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
                                ),
                                'parameters': (shape_id,),
                            }
                        )
                        if len(lsps) > 0:
                            lsp = lsps[0]

                        shape_used.add(shape_id)

                luggage_carriage_facility_list = []
                facitities = None
                bikes_allowed = get_or_none(bikes_alloweds, i)
                if bikes_allowed is not None:
                    if bikes_allowed == 1:
                        luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
                    elif bikes_allowed == 2:
                        luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)

                if len(luggage_carriage_facility_list) > 0:
                    facitities = ServiceFacilitySetsRelStructure(
                        restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
                            ServiceFacilitySet(
                                id=self.get_trip_id_sfs(trip_ids[i]),
                                version=self.version.version,
                                luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
                            )
                        ]
                    )

                calls = CallsRelStructure()

                with self.conn.cursor() as cur2:
                    cur2.execute(**{'query': """select * from stop_times where trip_id = ? order by stop_sequence;""", 'parameters': (trip_ids[i],)})
                    # trip_id = None
                    # service_journey = None
                    prev_call = None
                    prev_shape_traveled: int | None = 0
                    prev_order = 1

                    df2 = cur2.df()

                    stop_headsigns = df2.get('stop_headsign')
                    stop_ids = df2.get('stop_id')
                    arrival_times = df2.get('arrival_time')
                    departure_times = df2.get('departure_time')
                    shape_dist_traveleds = df2.get('shape_dist_traveled')
                    drop_off_types = df2.get('drop_off_type')
                    pickup_types = df2.get('pickup_type')
                    stop_sequences = df2.get('stop_sequence')

                    for index_j in range(0, len(stop_ids)):
                        destination_display_view = None
                        stop_headsign = get_or_none(stop_headsigns, index_j)
                        if stop_headsign is not None:
                            destination_display_view = DestinationDisplayView(
                                name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
                            )

                        from_point_ref = getId(ScheduledStopPoint, self.codespace, stop_ids[index_j])
                        arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[index_j])
                        departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[index_j])

                        shape_dist_traveled = get_or_none(shape_dist_traveleds, index_j)
                        if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
                            distance = shape_dist_traveled - prev_shape_traveled
                            prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)

                        call = Call(
                            id=self.get_trip_id_call(trip_ids[i], stop_sequences[index_j]),
                            version=self.version.version,
                            fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
                                from_point_ref, ScheduledStopPointRef, self.version.version
                            ),
                            destination_display_ref_or_destination_display_view=destination_display_view,
                            arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off_types[index_j] != 1)),
                            departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup_types[index_j] != 1)),
                            request_stop=bool(
                                pickup_types[index_j] == 2 or pickup_types[index_j] == 3 or drop_off_types[index_j] == 2 or drop_off_types[index_j] == 3
                            ),
                            order=prev_order,
                        )  # stop_sequence is non-negative integer

                        calls.call.append(call)

                        prev_call = call
                        if shape_dist_traveled:
                            prev_shape_traveled = shape_dist_traveled
                        prev_order += 1

                with self.conn.cursor() as cur3:
                    cur3.execute(**{'query': """select * from frequencies where trip_id = ?;""", 'parameters': (trip_ids[i],)})
                    df3 = cur3.df()

                    start_times = df3.get('start_time')
                    end_times = df3.get('end_time')
                    headway_secs = df3.get('headway_secs')
                    exact_times = df3.get('exact_times')

                    hjgs = []

                    for index_j in range(0, len(start_times)):
                        start_time, start_dayoffset = self.noonTimeToNeTEx(start_times[index_j])
                        end_time, end_dayoffset = self.noonTimeToNeTEx(end_times[index_j])

                        jfgvs = [
                            JourneyFrequencyGroupVersionStructure.FirstDayOffset(value=start_dayoffset),
                            JourneyFrequencyGroupVersionStructure.LastDepartureTime(value=end_time),
                            JourneyFrequencyGroupVersionStructure.LastDayOffset(value=end_dayoffset),
                        ]

                        hjgs.append(
                            HeadwayJourneyGroup(
                                id=getId(HeadwayJourneyGroup, self.codespace, trip_ids[i] + '_' + start_times[index_j].replace(':', '')),
                                first_departure_time=start_time,
                                first_day_offset_or_last_departure_time_or_last_day_offset_or_first_arrival_time_or_last_arrival_time=cast(
                                    list[
                                        Union[
                                            "JourneyFrequencyGroupVersionStructure.FirstDayOffset",
                                            "JourneyFrequencyGroupVersionStructure.LastDepartureTime",
                                            "JourneyFrequencyGroupVersionStructure.LastDayOffset",
                                            "JourneyFrequencyGroupVersionStructure.FirstArrivalTime",
                                            "JourneyFrequencyGroupVersionStructure.LastArrivalTime",
                                        ]
                                    ],
                                    jfgvs,
                                ),
                                scheduled_headway_interval=XmlDuration(value=f'PT{headway_secs[index_j]}S') if exact_times[index_j] == 1 else None,
                                minimum_headway_interval=XmlDuration(value=f'PT{headway_secs[index_j]}S') if exact_times[index_j] == 0 else None,
                            )
                        )

                template_service_journey = TemplateServiceJourney(
                    id=getId(TemplateServiceJourney, self.codespace, trip_ids[i]),
                    version=self.version.version,
                    flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(
                        getId(Line, self.codespace, route_ids[i]), LineRef, self.version.version
                    ),
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
                    short_name=getOptionalString(get_or_none(trip_short_names, i)),
                    validity_conditions_or_valid_between=[
                        ValidityConditionsRelStructure(
                            choice=cast(
                                list[
                                    Union[
                                        AvailabilityConditionRef,
                                        ValidityRuleParameterRef,
                                        ValidityTriggerRef,
                                        ValidityConditionRef,
                                        ValidBetween,
                                        SimpleAvailabilityCondition,
                                        ValidDuring,
                                        AvailabilityCondition,
                                        ValidityRuleParameter,
                                        ValidityTrigger,
                                        ValidityCondition,
                                    ]
                                ],
                                [getRef(x, AvailabilityConditionRef) for x in availability_conditions_journey if x is not None],
                            )
                        )
                    ],
                    journey_pattern_view=journey_pattern_view,
                    direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
                    block_ref=block_ref,
                    accessibility_assessment=accessibility_assessment,
                    facilities=facitities,
                    link_sequence_projection_ref_or_link_sequence_projection=lsp,
                    calls=calls,
                    frequency_groups=FrequencyGroupsRelStructure(
                        headway_journey_group_ref_or_headway_journey_group_or_rhythmical_journey_group_ref_or_rhythmical_journey_group=cast(
                            list[Union[HeadwayJourneyGroupRef, HeadwayJourneyGroup, RhythmicalJourneyGroupRef, RhythmicalJourneyGroup]], hjgs
                        )
                    ),
                )
                yield template_service_journey

    def getTemplateServiceJourneysDayType(
        self, trips_sql: dict[str, str] = {'query': """select * from trips WHERE trip_id IN (SELECT trip_id FROM frequencies) order by trip_id;"""}
    ) -> Generator[TemplateServiceJourney, None, None]:
        shape_used = set([])

        with self.conn.cursor() as cur:
            cur.execute(**trips_sql)

            df = cur.df()

            route_ids = df.get('route_id')
            trip_ids = df.get('trip_id')
            service_ids = df.get('service_id')
            trip_short_names = df.get('trip_short_name')
            trip_headsigns = df.get('trip_headsign')
            # route_short_names = df.get('route_short_name')
            direction_ids = df.get('direction_id')
            block_ids = df.get('block_id')
            shape_ids = df.get('shape_id')
            wheelchair_accessibles = df.get('wheelchair_accessible')
            # trip_bikes_alloweds = df.get('trip_bikes_allowed')
            bikes_alloweds = df.get('bikes_allowed')
            # ticketing_trip_ids = df.get('ticketing_trip_id')
            # ticketing_types = df.get('ticketing_type')

            for i in range(0, len(route_ids)):
                journey_pattern_view = None
                if trip_headsigns[i] is not None:
                    journey_pattern_view = JourneyPatternView(
                        destination_display_ref_or_destination_display_view=DestinationDisplayView(
                            name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
                        )
                    )

                accessibility_assessment = None
                if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
                    accessibility_assessment = AccessibilityAssessment(
                        id=self.get_trip_id_aa(trip_ids[i]),
                        version=self.version.version,
                        mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
                    )

                block_ref = None
                if block_ids[i] is not None:
                    block_ref = getFakeRef(getId(Block, self.codespace, block_ids[i]), BlockRef, None)

                # route_ref = None
                lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
                shape_id = get_or_none(shape_ids, i)
                if shape_id is not None:
                    if shape_id in shape_used:
                        lsp = getFakeRef(getId(LinkSequenceProjection, self.codespace, shape_id), LinkSequenceProjectionRef, self.version.version)
                    else:
                        lsps = self.getLineStrings(
                            {
                                'query': (
                                    """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
                                ),
                                'parameters': (shape_id,),
                            }
                        )
                        if len(lsps) > 0:
                            lsp = lsps[0]

                        shape_used.add(shape_id)

                luggage_carriage_facility_list = []
                facitities = None
                bikes_allowed = get_or_none(bikes_alloweds, i)
                if bikes_allowed is not None:
                    if bikes_allowed == 1:
                        luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
                    elif bikes_allowed == 2:
                        luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)

                if len(luggage_carriage_facility_list) > 0:
                    facitities = ServiceFacilitySetsRelStructure(
                        restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
                            ServiceFacilitySet(
                                id=self.get_trip_id_sfs(trip_ids[i]),
                                version=self.version.version,
                                luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
                            )
                        ]
                    )

                calls = CallsRelStructure()

                with self.conn.cursor() as cur2:
                    cur2.execute(**{'query': """select * from stop_times where trip_id = ? order by stop_sequence;""", 'parameters': (trip_ids[i],)})
                    prev_call: Call | None = None
                    prev_shape_traveled: int = 0
                    prev_order = 1

                    df2 = cur2.df()

                    stop_headsigns = df2.get('stop_headsign')
                    stop_ids = df2.get('stop_id')
                    arrival_times = df2.get('arrival_time')
                    departure_times = df2.get('departure_time')
                    shape_dist_traveleds = df2.get('shape_dist_traveled')
                    drop_off_types = df2.get('drop_off_type')
                    pickup_types = df2.get('pickup_type')
                    stop_sequences = df2.get('stop_sequence')

                    for index_j in range(0, len(stop_ids)):
                        destination_display_view = None
                        stop_headsign = get_or_none(stop_headsigns, index_j)
                        if stop_headsign is not None:
                            destination_display_view = DestinationDisplayView(
                                name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
                            )

                        from_point_ref = getId(ScheduledStopPoint, self.codespace, stop_ids[index_j])
                        arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[index_j])
                        departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[index_j])

                        shape_dist_traveled = get_or_none(shape_dist_traveleds, index_j)
                        if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
                            distance = shape_dist_traveled - prev_shape_traveled
                            prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)

                        call = Call(
                            id=self.get_trip_id_call(trip_ids[i], stop_sequences[index_j]),
                            version=self.version.version,
                            fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
                                from_point_ref, ScheduledStopPointRef, self.version.version
                            ),
                            destination_display_ref_or_destination_display_view=destination_display_view,
                            arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off_types[index_j] != 1)),
                            departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup_types[index_j] != 1)),
                            request_stop=bool(
                                pickup_types[index_j] == 2 or pickup_types[index_j] == 3 or drop_off_types[index_j] == 2 or drop_off_types[index_j] == 3
                            ),
                            order=prev_order,
                        )  # stop_sequence is non-negative integer

                        calls.call.append(call)

                        prev_call = call
                        if shape_dist_traveled:
                            prev_shape_traveled = shape_dist_traveled
                        prev_order += 1

                with self.conn.cursor() as cur3:
                    cur3.execute(**{'query': """select * from frequencies where trip_id = ?;""", 'parameters': (trip_ids[i],)})
                    df3 = cur3.df()

                    start_times = df3.get('start_time')
                    end_times = df3.get('end_time')
                    headway_secs = df3.get('headway_secs')
                    exact_times = df3.get('exact_times')

                    hjgs = []

                    for index_j in range(0, len(start_times)):
                        start_time, start_dayoffset = self.noonTimeToNeTEx(start_times[index_j])
                        end_time, end_dayoffset = self.noonTimeToNeTEx(end_times[index_j])

                        jfgvs = [
                            JourneyFrequencyGroupVersionStructure.FirstDayOffset(value=start_dayoffset),
                            JourneyFrequencyGroupVersionStructure.LastDepartureTime(value=end_time),
                            JourneyFrequencyGroupVersionStructure.LastDayOffset(value=end_dayoffset),
                        ]

                        hjgs.append(
                            HeadwayJourneyGroup(
                                id=getId(HeadwayJourneyGroup, self.codespace, trip_ids[i] + '_' + start_times[index_j].replace(':', '')),
                                first_departure_time=start_time,
                                first_day_offset_or_last_departure_time_or_last_day_offset_or_first_arrival_time_or_last_arrival_time=cast(
                                    list[
                                        Union[
                                            "JourneyFrequencyGroupVersionStructure.FirstDayOffset",
                                            "JourneyFrequencyGroupVersionStructure.LastDepartureTime",
                                            "JourneyFrequencyGroupVersionStructure.LastDayOffset",
                                            "JourneyFrequencyGroupVersionStructure.FirstArrivalTime",
                                            "JourneyFrequencyGroupVersionStructure.LastArrivalTime",
                                        ]
                                    ],
                                    jfgvs,
                                ),
                                scheduled_headway_interval=XmlDuration(value=f'PT{headway_secs[index_j]}S') if exact_times[index_j] == 1 else None,
                                minimum_headway_interval=XmlDuration(value=f'PT{headway_secs[index_j]}S') if exact_times[index_j] == 0 else None,
                            )
                        )

                template_service_journey = TemplateServiceJourney(
                    id=getId(TemplateServiceJourney, self.codespace, trip_ids[i]),
                    version=self.version.version,
                    flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(
                        getId(Line, self.codespace, route_ids[i]), LineRef, self.version.version
                    ),
                    private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
                    short_name=getOptionalString(get_or_none(trip_short_names, i)),
                    day_types=DayTypeRefsRelStructure(day_type_ref=[getFakeRef(self.get_service_id_dt(service_ids[i]), DayTypeRef, self.version.version)]),
                    journey_pattern_view=journey_pattern_view,
                    direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
                    block_ref=block_ref,
                    accessibility_assessment=accessibility_assessment,
                    facilities=facitities,
                    link_sequence_projection_ref_or_link_sequence_projection=lsp,
                    calls=calls,
                    frequency_groups=FrequencyGroupsRelStructure(
                        headway_journey_group_ref_or_headway_journey_group_or_rhythmical_journey_group_ref_or_rhythmical_journey_group=cast(
                            list[Union[HeadwayJourneyGroupRef, HeadwayJourneyGroup, RhythmicalJourneyGroupRef, RhythmicalJourneyGroup]], hjgs
                        )
                    ),
                )
                yield template_service_journey

    def database(self, con: Database) -> None:
        con.insert_objects_on_queue(Line, self.lines)

        # This still sucks :-) shape is in every ServiceJourney now
        # in order to solve it, we must find the route point that matches the
        # shape point exactly, but if the GTFS shape is provided as an abstract
        # shape there may not be a one-to-one RouteLink.
        #
        # self.routes, self.route_points, self.route_links = self.getRoutes()
        # write_objects(con, self.route_points, True, True)
        # write_objects(con, self.route_links, True, True)
        # write_objects(con, self.routes, True, True)

        con.insert_objects_on_queue(Codespace, [self.codespace])
        con.insert_objects_on_queue(DataSource, [self.data_source])
        con.insert_objects_on_queue(Version, [self.version])

        gf = GeneralFrame(id="Defaults", version="any", frame_defaults=self.frame_defaults)
        con.insert_objects_on_queue(GeneralFrame, [gf])

        con.insert_objects_on_queue(Operator, self.getOperators())

        stop_areas = self.getStopAreas()
        con.insert_objects_on_queue(StopArea, stop_areas)
        con.insert_objects_on_queue(ScheduledStopPoint, self.getScheduledStopPoints(stop_areas))
        del stop_areas

        stop_places, passenger_stop_assignments = self.getStopPlaces()
        con.insert_objects_on_queue(StopPlace, stop_places)
        con.insert_objects_on_queue(PassengerStopAssignment, passenger_stop_assignments)
        del stop_places
        del passenger_stop_assignments

        day_types, day_type_assignments, operating_periods = self.getDayTypes()
        con.insert_objects_on_queue(DayType, day_types)
        con.insert_objects_on_queue(DayTypeAssignment, day_type_assignments)
        con.insert_objects_on_queue(OperatingPeriod, operating_periods)

        # availability_conditions = self.getAvailabilityConditions()
        # write_objects(con, availability_conditions, empty=True, many=True)

        con.insert_objects_on_queue(ServiceJourney, self.getServiceJourneys2DayType())
        con.insert_objects_on_queue(TemplateServiceJourney, self.getTemplateServiceJourneysDayType())

        con.insert_objects_on_queue(InterchangeRule, self.getInterchangeRules())

    codespace: Codespace
    data_source: DataSource
    version: Version
    frame_defaults: VersionFrameDefaultsStructure

    def __init__(self, conn: Any, serializer: XmlSerializer):
        self.conn = conn
        self.serializer = serializer

        self.ns_map = {'': 'http://www.netex.org.uk/netex', 'gml': 'http://www.opengis.net/gml/3.2'}
        self.codespace, self.data_source, self.version, self.frame_defaults = self.getCodespaceAndDataSource()
        self.lines = self.getLines()


def main(database_gtfs: str, database_netex: str) -> None:
    serializer_config = SerializerConfig(ignore_default_attributes=True)
    serializer_config.pretty_print = True
    serializer_config.ignore_default_attributes = True
    serializer = XmlSerializer(config=serializer_config)

    gtfs = GtfsNeTexProfile(conn=duckdb.connect(database=database_gtfs, read_only=True), serializer=serializer)

    with Database(database_netex, serializer=MyPickleSerializer(compression=True), readonly=False) as db_write:
        assert gtfs.version.version is not None
        db_write.insert_metadata_on_queue([("GTFS", gtfs.version.version, gtfs.frame_defaults)])
        gtfs.database(db_write)


if __name__ == '__main__':
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description='Convert a GTFS database to a NeTEx database')
    parser.add_argument('gtfs', type=str, help='GTFS database to convert, for example: gtfs-import.duckdb')
    parser.add_argument('database', type=str, help='DuckDB file to overwrite and store contents of the conversion.')
    parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.gtfs, args.database)
    except Exception as e:
        log_all(logging.ERROR, f'{e} {traceback.format_exc()}')
        raise e
