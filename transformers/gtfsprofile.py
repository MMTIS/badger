import csv
import datetime
import warnings
import zipfile
from typing import List, Union, Any, Iterable, TypeVar, Literal
import io
import logging
from pyproj import Transformer
from decimal import Decimal

from transformers.projection import project_location_4326

from utils.utils import to_seconds

from netex import Line, MultilingualString, AllVehicleModesOfTransportEnumeration, InfoLinksRelStructure, \
    ScheduledStopPoint, StopPlace, AccessibilityAssessment, LimitationStatusEnumeration, TariffZoneRefsRelStructure, \
    PrivateCode, PrivateCodeStructure, Quay, PresentationStructure, Authority, Branding, Operator, ServiceJourney, \
    ServiceJourneyPattern, LineRefStructure, RouteView, StopArea, StopAreaRef, StopPlaceRef, Route, RouteLink, \
    ServiceLink, PublicCodeStructure, StopPlaceEntrance, TemplateServiceJourney, HeadwayJourneyGroup, \
    JourneyFrequencyGroupVersionStructure, InterchangeRule, ServiceJourneyInterchange, JourneyMeeting, \
    AvailabilityCondition, DayType, DayOfWeekEnumeration, EmptyType2, UicOperatingPeriod, DataManagedObjectStructure, \
    VersionOfObjectRefStructure

import operator as operator_f
from utils.aux_logging import log_once
from configuration import defaults

T = TypeVar("T")

gtfs_id_lookup = {} # TODO: better way?


class GtfsProfile:
    empty_stop_time = {'trip_id': None, 'arrival_time': None, 'departure_time': None, 'stop_id': None,
                       'stop_sequence': None, 'stop_headsign': None, 'pickup_type': None, 'drop_off_type': None,
                       'continuous_pickup': None, 'continuous_drop_off': None, 'shape_dist_traveled': None, 'timepoint': None}

    empty_trip = {'route_id': None, 'service_id': None, 'trip_id': None, 'trip_headsign': None, 'trip_short_name': None,
                  'direction_id': None, 'block_id': None, 'shape_id': None, 'wheelchair_accessible': None, 'bikes_allowed': None}

    empty_shapes = {'shape_id': None, 'shape_pt_lat': None, 'shape_pt_lon': None, 'shape_pt_sequence': None, 'shape_dist_traveled': None}

    @staticmethod
    def writeToFile(filename: str, data: List[dict[str, Any]], write_header: bool=False) -> None:
        mode = 'a'
        if write_header:
            mode = 'w'

        if len(data) > 0:
            with open(filename, mode) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
                if write_header:
                    writer.writeheader()
                if data[0][list(data[0].keys())[0]] is not None:
                    writer.writerows(data)

    @staticmethod
    def writeToZipFile(archive: zipfile.ZipFile, filename: str, data: List[dict[str, Any]], write_header: bool=False) -> None:
        mode = 'a'
        if write_header:
            mode = 'w'

        if len(data) > 0:
            with archive.open(filename, mode) as f:
                csvfile = io.TextIOWrapper(f, 'utf-8', newline='')
                writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
                if write_header:
                    writer.writeheader()
                if data[0][list(data[0].keys())[0]] is not None:
                    writer.writerows(data)
                csvfile.close()

    @staticmethod
    def getOptionalMultilingualString(multilingual_string: MultilingualString | List[MultilingualString] | None) -> str | None:
        if isinstance(multilingual_string, List):
            if len(multilingual_string) > 0:
                multilingual_string = multilingual_string[0]
            else:
                multilingual_string = None

        if multilingual_string is not None:
            return multilingual_string.value

        return None

    @staticmethod
    def getOptionalPrivateCode(private_code: PrivateCodeStructure | PrivateCode | PublicCodeStructure | None) -> str | None:
        if private_code is not None:
            return private_code.value

        return None

    @staticmethod
    def projectVehicleModeToRouteType(vehicle_mode: AllVehicleModesOfTransportEnumeration) -> int:
        if vehicle_mode == AllVehicleModesOfTransportEnumeration.TRAM:
            return 0
        elif vehicle_mode == AllVehicleModesOfTransportEnumeration.METRO:
            return 1
        elif vehicle_mode == AllVehicleModesOfTransportEnumeration.RAIL:
            return 2
        elif vehicle_mode == AllVehicleModesOfTransportEnumeration.BUS:
            return 3
        elif vehicle_mode in (AllVehicleModesOfTransportEnumeration.WATER, AllVehicleModesOfTransportEnumeration.FERRY):
            return 4

        # We don't have a Cable Tram in NeTEx route_type = 5?

        elif vehicle_mode == AllVehicleModesOfTransportEnumeration.CABLEWAY:
            return 6
        elif vehicle_mode == AllVehicleModesOfTransportEnumeration.FUNICULAR:
            return 7
        elif vehicle_mode == AllVehicleModesOfTransportEnumeration.TROLLEY_BUS:
            return 11

        # We don't have a Monorail in NeTEx route_type = 11?

        return 0

    @staticmethod
    def getInfoLinkWithUrl(info_links: InfoLinksRelStructure | None) -> str | None:
        if info_links is not None:
            # TODO: we would like to sort this based on some kind of priority, based on type_of_info_link
            for info_link in info_links.info_link:
                return info_link.value
        return None

    @staticmethod
    def getOptionalPresentation(presentation: PresentationStructure | None, attrib: str) -> str | None:
        if presentation is not None:
            op=getattr(presentation, attrib, '')
            if op is not None:
                if hasattr(op, 'hex'): # TODO: check why sometimes we don't get hex.
                    return str(op.hex())
                else:
                    return op
        return None

    @staticmethod
    def getOriginalGtfsId(dmo: DataManagedObjectStructure, id_attribute: str) -> str:
        global gtfs_id_lookup

        if dmo.private_codes:
            ids = [private_code.value for private_code in dmo.private_codes.private_code if
                           private_code.type_value == id_attribute]
            original_id = ids[0] if len(ids) > 0 else dmo.id
        else:
            original_id = dmo.id

        gtfs_id_lookup[dmo.id] = original_id
        return original_id

    @staticmethod
    def getOriginalGtfsIdFromRef(vor: VersionOfObjectRefStructure | str) -> str: # TODO: refactor, this is not correct
        global gtfs_id_lookup
        if hasattr(vor, "ref"):
            return gtfs_id_lookup.get(vor.ref, vor.ref)
        else:
            return gtfs_id_lookup.get(vor, vor)

        # return gtfs_id_lookup.get(getattr(vor, "ref", vor), vor)

    @staticmethod
    def projectAuthorityToAgency(authority: Authority) -> dict[str, Any]:
        agency = {'agency_id': GtfsProfile.getOriginalGtfsId(authority, 'agency_id'),
                  'agency_name': GtfsProfile.getOptionalMultilingualString(authority.name) or GtfsProfile.getOptionalMultilingualString(authority.short_name),
                  'agency_url': GtfsProfile.getOrNone(authority, "contact_details.url") or defaults["authority"],  # TODO: FrameDefaults
                  'agency_timezone': GtfsProfile.getOrNone(authority, "locale.time_zone") or defaults["timezone"],  # TODO: FrameDefaults
                  'agency_lang': GtfsProfile.getOrNone(authority, "locale.default_language"),
                  'agency_phone': GtfsProfile.getOrNone(authority, "contact_details.phone"),
                  'agency_fare_url': '',
                  'agency_email': GtfsProfile.getOrNone(authority, "contact_details.email")
                  }
        return agency
    
    @staticmethod
    def getOrNone(object: T, attr: str, default: Any=None) -> Any | None:
        if object is None:
            return None
        try:
            return operator_f.attrgetter(attr)(object)
        except:
            pass
        return default

    @staticmethod
    def projectOperatorToAgency(operator: Operator) -> dict[str, Any]:
        agency = {'agency_id': GtfsProfile.getOriginalGtfsId(operator, 'agency_id'),
                  'agency_name': GtfsProfile.getOptionalMultilingualString(operator.name) or GtfsProfile.getOptionalMultilingualString(operator.short_name),
                  'agency_url': GtfsProfile.getOrNone(operator, "contact_details.url") or 'http://openov.nl/', # TODO: FrameDefaults
                  'agency_timezone': GtfsProfile.getOrNone(operator, "locale.time_zone") or 'Europe/Amsterdam', # TODO: FrameDefaults
                  'agency_lang': GtfsProfile.getOrNone(operator, "locale.default_language"),
                  'agency_phone': GtfsProfile.getOrNone(operator, "contact_details.phone"),
                  'agency_fare_url': '',
                  'agency_email': GtfsProfile.getOrNone(operator, "contact_details.email")
                  }
        return agency

    @staticmethod
    def projectBrandingToAgency(branding: Branding) -> dict[str, Any]:
        agency = {'agency_id': GtfsProfile.getOriginalGtfsId(branding, 'agency_id'),
                  'agency_name': GtfsProfile.getOptionalMultilingualString(branding.name) or GtfsProfile.getOptionalMultilingualString(branding.short_name),
                  'agency_url': GtfsProfile.getOrNone(branding, 'url') or 'http://openov.nl/', # TODO: FrameDefaults
                  'agency_timezone': GtfsProfile.getOrNone(branding, "locale.time_zone") or 'Europe/Amsterdam', # TODO: FrameDefaults
                  'agency_lang': GtfsProfile.getOrNone(branding, "locale.default_language"),
                  'agency_phone': GtfsProfile.getOrNone(branding, "contact_details.phone"),
                  'agency_fare_url': '',
                  'agency_email': GtfsProfile.getOrNone(branding, "contact_details.email")
                  }
        return agency

    # @staticmethod
    # def projectBrandingToAgency(branding: Branding) -> dict:
    #    agency = {'agency_id': branding.id,
    #              'agency_name': GtfsProfile.getOptionalMultilingualString(branding.name) or GtfsProfile.getOptionalMultilingualString(branding.short_name),
    #              'agency_url': branding.url,
    #              'agency_timezone': '',
    #              'agency_lang': authority.locale.default_language,
    #              'agency_phone': authority.contact_details.phone,
    #              'agency_fare_url': '',
    #              'agency_email': authority.contact_details.email
    #              }

    @staticmethod
    def projectJourneyMeetingToTransfer(journey_meeting: JourneyMeeting) -> Iterable[dict[str, Any]]:
        # TODO: Technically we need to take into AvailabityCondition

        for connecting_stop_point_ref in journey_meeting.connecting_stop_point_ref:
            stop_id = connecting_stop_point_ref

            from_trip_id = None
            if journey_meeting.from_journey_ref is not None:
                from_trip_id = journey_meeting.from_journey_ref
            else:
                from_trip_id = None

            to_trip_id = None
            if journey_meeting.to_journey_ref is not None:
                to_trip_id = journey_meeting.to_journey_ref
            else:
                to_trip_id = None

            transfer_type = 1

            transfer = {'from_stop_id': GtfsProfile.getOriginalGtfsIdFromRef(stop_id),
                        'to_stop_id': GtfsProfile.getOriginalGtfsIdFromRef(stop_id),
                        'from_route_id': None,
                        'to_route_id': None,
                        'from_trip_id': GtfsProfile.getOriginalGtfsIdFromRef(from_trip_id) if from_trip_id is not None else None,
                        'to_trip_id': GtfsProfile.getOriginalGtfsIdFromRef(to_trip_id) if to_trip_id is not None else None,
                        'transfer_type': transfer_type,
                        'min_transfer_time': None
                        }

            yield transfer

    @staticmethod
    def projectServiceJourneyInterchangeToTransfer(service_journey_interchange: ServiceJourneyInterchange) -> dict[str, Any]:
        # TODO: Technically we need to take into a account both visit number and AvailabityCondition

        from_stop_id = None
        if service_journey_interchange.from_point_ref is not None:
            from_stop_id = service_journey_interchange.from_point_ref

        to_stop_id = None
        if service_journey_interchange.to_point_ref is not None:
            to_stop_id = service_journey_interchange.to_point_ref

        from_trip_id = None
        if service_journey_interchange.from_journey_ref is not None:
            from_trip_id = service_journey_interchange.from_journey_ref

        to_trip_id = None
        if service_journey_interchange.to_journey_ref is not None:
            to_trip_id = service_journey_interchange.to_journey_ref

        transfer_type = 0

        if service_journey_interchange.guaranteed:
            transfer_type = 1

        if service_journey_interchange.minimum_transfer_time:
            transfer_type = 2

        elif service_journey_interchange.standard_transfer_time:
            transfer_type = 2

        if service_journey_interchange.stay_seated == True:
            transfer_type = 4

        elif service_journey_interchange.stay_seated == False:
            transfer_type = 5

        min_transfer_time = None
        if service_journey_interchange.minimum_transfer_time:
            min_transfer_time = to_seconds(service_journey_interchange.minimum_transfer_time)
        elif service_journey_interchange.standard_transfer_time:
            min_transfer_time = to_seconds(service_journey_interchange.standard_transfer_time)
        else:
            min_transfer_time = None

        # TODO
        transfer = {'from_stop_id': GtfsProfile.getOriginalGtfsIdFromRef(from_stop_id),
                    'to_stop_id': GtfsProfile.getOriginalGtfsIdFromRef(to_stop_id),
                    'from_route_id': None,
                    'to_route_id': None,
                    'from_trip_id': GtfsProfile.getOriginalGtfsIdFromRef(from_trip_id),
                    'to_trip_id': GtfsProfile.getOriginalGtfsIdFromRef(to_trip_id),
                    'transfer_type': transfer_type,
                    'min_transfer_time': (mtt := int(min_transfer_time)) if min_transfer_time is not None else ''
                    }

        return transfer

    @staticmethod
    def projectInterchangeRuleToTransfer(interchange_rule: InterchangeRule) -> dict[str, Any]:
        # TODO: Technically we need to take into a account AvailabilityCondition

        assert interchange_rule.feeder_filter is not None, f"{interchange_rule} does not have a FeedFilter"
        assert interchange_rule.distributor_filter is not None, f"{interchange_rule} does not have a DistributorFilter"

        from_stop_id = None
        if interchange_rule.feeder_filter.stop_place_ref is not None:
            from_stop_id = interchange_rule.feeder_filter.stop_place_ref
        elif interchange_rule.feeder_filter.scheduled_stop_point_ref is not None:
            from_stop_id = interchange_rule.feeder_filter.scheduled_stop_point_ref

        to_stop_id = None
        if interchange_rule.distributor_filter.stop_place_ref is not None:
            to_stop_id = interchange_rule.distributor_filter.stop_place_ref
        elif interchange_rule.distributor_filter.scheduled_stop_point_ref is not None:
            to_stop_id = interchange_rule.distributor_filter.scheduled_stop_point_ref

        from_route_id = None
        if interchange_rule.feeder_filter.all_lines_or_lines_in_direction_refs_or_line_in_direction_ref is not None and isinstance(interchange_rule.feeder_filter.all_lines_or_lines_in_direction_refs_or_line_in_direction_ref, EmptyType2):
            from_route_id = interchange_rule.feeder_filter.all_lines_or_lines_in_direction_refs_or_line_in_direction_ref[0].line_ref

        to_route_id = None
        if interchange_rule.distributor_filter.all_lines_or_lines_in_direction_refs_or_line_in_direction_ref is not None and isinstance(interchange_rule.distributor_filter.all_lines_or_lines_in_direction_refs_or_line_in_direction_ref, EmptyType2):
            to_route_id = interchange_rule.distributor_filter.all_lines_or_lines_in_direction_refs_or_line_in_direction_ref[0].line_ref

        from_trip_id = None
        if interchange_rule.feeder_filter.service_journey_ref_or_journey_designator_or_service_designator is not None:
            from_trip_id = interchange_rule.feeder_filter.service_journey_ref_or_journey_designator_or_service_designator

        to_trip_id = None
        if interchange_rule.distributor_filter.service_journey_ref_or_journey_designator_or_service_designator is not None:
            to_trip_id = interchange_rule.distributor_filter.service_journey_ref_or_journey_designator_or_service_designator

        transfer_type = 0

        if interchange_rule.guaranteed:
            transfer_type = 1

        if interchange_rule.minimum_transfer_time:
            transfer_type = 2

        elif interchange_rule.standard_transfer_time:
            transfer_type = 2

        if interchange_rule.exclude:
            transfer_type = 3

        if interchange_rule.stay_seated == True:
            transfer_type = 4

        elif interchange_rule.stay_seated is False:
            transfer_type = 5

        if interchange_rule.minimum_transfer_time:
            min_transfer_time = to_seconds(interchange_rule.minimum_transfer_time)
        elif interchange_rule.standard_transfer_time:
            min_transfer_time = to_seconds(interchange_rule.standard_transfer_time)
        else:
            min_transfer_time = None

        # TODO
        transfer = {'from_stop_id': GtfsProfile.getOriginalGtfsIdFromRef(from_stop_id) if from_stop_id is not None else None,
                    'to_stop_id': GtfsProfile.getOriginalGtfsIdFromRef(to_stop_id) if to_stop_id is not None else None,
                    'from_route_id': GtfsProfile.getOriginalGtfsIdFromRef(from_route_id) if from_route_id is not None else None,
                    'to_route_id': GtfsProfile.getOriginalGtfsIdFromRef(to_route_id) if to_route_id is not None else None,
                    'from_trip_id': GtfsProfile.getOriginalGtfsIdFromRef(from_trip_id) if from_trip_id is not None else None,
                    'to_trip_id': GtfsProfile.getOriginalGtfsIdFromRef(to_trip_id) if to_trip_id is not None else None,
                    'transfer_type': transfer_type,
                    'min_transfer_time': (mtt := int(min_transfer_time)) if min_transfer_time is not None else ''
                    }

        return transfer

    @staticmethod
    def projectLineToRoute(line: Line) -> dict[str, Any]:
        if line.branding_ref is not None:
            agency_id = line.branding_ref.ref
        elif line.operator_ref is not None:
            agency_id = line.operator_ref.ref
        elif line.authority_ref is not None:
            agency_id = line.authority_ref.ref
        else:
            assert False, f"Can't handle {line.id} because no agency can be found."

        route = {'route_id': GtfsProfile.getOriginalGtfsId(line, 'route_id'),
                 'agency_id': GtfsProfile.getOriginalGtfsIdFromRef(agency_id),
                 'route_short_name': GtfsProfile.getOptionalPrivateCode(line.public_code), # This is used as VehicleType or PublicCode
                 'route_long_name': GtfsProfile.getOptionalMultilingualString(line.name), # This is used as destination
                 'route_desc': GtfsProfile.getOptionalMultilingualString(line.description),
                 'route_type': GtfsProfile.projectVehicleModeToRouteType(line.transport_mode),
                 'route_url': GtfsProfile.getInfoLinkWithUrl(line.document_links),
                 'route_color': GtfsProfile.getOptionalPresentation(line.presentation, 'colour'),
                 'route_text_color': GtfsProfile.getOptionalPresentation(line.presentation, 'text_colour'),
                 'route_sort_order': '',
                 'continuous_pickup': '',
                 'continuous_drop_off': '',
                 'network_id': ''}

        return route

    @staticmethod
    def getTariffZoneFromScheduledStopPoint(tariff_zones: TariffZoneRefsRelStructure | None) -> str:
        if tariff_zones and len(tariff_zones.tariff_zone_ref) > 0 and tariff_zones.tariff_zone_ref[0].ref is not None:
            return tariff_zones.tariff_zone_ref[0].ref

        return ''

    @staticmethod
    def getStopAreaFromScheduledStopPoint(scheduled_stop_point: ScheduledStopPoint) -> str:
        if scheduled_stop_point.stop_areas and len(scheduled_stop_point.stop_areas.stop_area_ref) > 0 and scheduled_stop_point.stop_areas.stop_area_ref[0].ref is not None:
            return scheduled_stop_point.stop_areas.stop_area_ref[0].ref

        return ''

    @staticmethod
    def getWheelchairAccess(accessibility_assessment: AccessibilityAssessment | None) -> int:
        if accessibility_assessment is not None:
            if accessibility_assessment.mobility_impaired_access == LimitationStatusEnumeration.TRUE:
                return 1
            elif accessibility_assessment.mobility_impaired_access == LimitationStatusEnumeration.FALSE:
                return 2

        return 0

    @staticmethod
    def projectScheduledStopPointToStop(scheduled_stop_point: ScheduledStopPoint, parent: StopPlace | StopArea | None, transformer: Transformer | None = None) -> dict[str, Any] | None:
        # TODO: parent_station could be obtained from StopPlace or StopArea

        if scheduled_stop_point.location is None:
            log_once(logging.WARNING,"gtfsprofile",f"SSP {scheduled_stop_point.id} does not have a location.")
            # TODO: Maybe by parent?
            return None

        if transformer:
            latitude, longitude = transformer.transform(scheduled_stop_point.location.pos.value[0], scheduled_stop_point.location.pos.value[1])
        else:
            project_location_4326(scheduled_stop_point.location)
            latitude, longitude = scheduled_stop_point.location.latitude, scheduled_stop_point.location.longitude

        stop = {'stop_id': GtfsProfile.getOriginalGtfsId(scheduled_stop_point, 'stop_id'),
                'stop_code': GtfsProfile.getOptionalPrivateCode(scheduled_stop_point.public_code),
                'stop_name': GtfsProfile.getOptionalMultilingualString(scheduled_stop_point.name) or GtfsProfile.getOptionalMultilingualString(scheduled_stop_point.short_name),
                'stop_desc': GtfsProfile.getOptionalMultilingualString(scheduled_stop_point.description),
                'stop_lat': round(latitude, 7),
                'stop_lon': round(longitude, 7),
                'zone_id': GtfsProfile.getTariffZoneFromScheduledStopPoint(scheduled_stop_point.tariff_zones),
                'stop_url': scheduled_stop_point.url or '',
                'location_type': 0,
                'parent_station': (parent_station := GtfsProfile.getOriginalGtfsId(parent, 'stop_id')) if parent is not None else None,
                'stop_timezone': '',
                'wheelchair_boarding': '',
                'level_id': '',
                'platform_code': scheduled_stop_point.short_stop_code
        }

        return stop

    @staticmethod
    def projectStopEntranceToStop(stop_entrance: StopPlaceEntrance, parent: StopPlace, transformer: Transformer | None = None) -> dict[str, Any]:
        # TODO: parent_station could be obtained from StopPlace or StopArea

        if stop_entrance.centroid is None or stop_entrance.centroid.location is None:
            log_once(logging.WARNING,"gtfsprofile: StopPlaceEntrance",f"StopPlaceEntrance {stop_entrance.id} does not have a location or centroid.")
            # TODO: Maybe by parent?
            return None

        if transformer:
            latitude, longitude = transformer.transform(stop_entrance.centroid.location.pos.value[0], stop_entrance.centroid.location.pos.value[1])
        else:
            project_location_4326(stop_entrance.centroid.location)
            latitude, longitude = stop_entrance.centroid.location.latitude, stop_entrance.centroid.location.longitude

        stop = {'stop_id': GtfsProfile.getOriginalGtfsId(stop_entrance, 'stop_id'),
                'stop_code': GtfsProfile.getOptionalPrivateCode(stop_entrance.public_code),
                'stop_name': GtfsProfile.getOptionalMultilingualString(stop_entrance.name) or GtfsProfile.getOptionalMultilingualString(stop_entrance.short_name),
                'stop_desc': GtfsProfile.getOptionalMultilingualString(stop_entrance.description),
                'stop_lat': round(latitude, 7),
                'stop_lon': round(longitude, 7),
                'zone_id': GtfsProfile.getTariffZoneFromScheduledStopPoint(stop_entrance.tariff_zones),
                'stop_url': stop_entrance.url or '',
                'location_type': 2,
                'parent_station': GtfsProfile.getOriginalGtfsId(parent, 'stop_id') if parent is not None else None,
                'stop_timezone': '',
                'wheelchair_boarding': '',
                'level_id': '',
                'platform_code': ''
        }

        return stop

    @staticmethod
    def temporaryDayTypeServiceId(day_type: DayType) -> str:
        return GtfsProfile.getOriginalGtfsId(day_type, 'service_id')

    @staticmethod
    def getCalendarAndCalendarDates(service_id: str, availability_condition: AvailabilityCondition) -> Iterable[tuple[dict[str, Any] | Literal[None], dict[str, Any] | Literal[None]]]:
        if availability_condition.valid_day_bits is not None:
            assert availability_condition.from_date is not None, f"{availability_condition.id} must have a FromDate when ValidDayBits are used."
            operational_dates = [availability_condition.from_date.to_datetime() + datetime.timedelta(days=i) for i in range(0, len(availability_condition.valid_day_bits)) if availability_condition.valid_day_bits[i] == '1']
            for date in operational_dates:
                yield tuple((None, {'service_id': service_id, 'date': str(date.date()).replace('-', ''), 'exception_type': 1 if (True if availability_condition.is_available is None else availability_condition.is_available) else 2},))

        elif availability_condition.day_types is not None and len(availability_condition.day_types.day_type_ref_or_day_type) == 1 and isinstance(availability_condition.day_types.day_type_ref_or_day_type[0], DayType):
            day_type: DayType = availability_condition.day_types.day_type_ref_or_day_type[0]
            days_of_week = day_type.properties.property_of_day[0].days_of_week

            yield tuple(({'service_id': service_id,
                   'monday': int(DayOfWeekEnumeration.MONDAY in days_of_week),
                   'tuesday': int(DayOfWeekEnumeration.TUESDAY in days_of_week),
                   'wednesday': int(DayOfWeekEnumeration.WEDNESDAY in days_of_week),
                   'thursday': int(DayOfWeekEnumeration.THURSDAY in days_of_week),
                   'friday': int(DayOfWeekEnumeration.FRIDAY in days_of_week),
                   'saturday': int(DayOfWeekEnumeration.SATURDAY in days_of_week),
                   'sunday': int(DayOfWeekEnumeration.SUNDAY in days_of_week),
                   'start_date': str(availability_condition.from_date.to_datetime().date()).replace('-', ''),
                   'end_date': str(availability_condition.to_date.to_datetime().date()).replace('-', '')}, None,))

        else:
            warnings.warn("This availability condition does not match the GTFS profile")

    @staticmethod
    def getCalendarDates(service_id: str, dates: List[datetime.datetime], is_available: bool = True) -> Iterable[dict[str, Any]]:
        exception_type = 1 if is_available else 2
        for date in dates:
            yield {'service_id': service_id, 'date': str(date).replace('-', ''), 'exception_type': exception_type}

    @staticmethod
    def getLineRef(service_journey: ServiceJourney, service_journey_pattern: ServiceJourneyPattern | None) -> LineRefStructure | None:
        if service_journey.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view is not None:
            if isinstance(service_journey.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view, LineRefStructure):
                return service_journey.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view

        elif service_journey_pattern is not None:
            if service_journey.journey_pattern_ref.ref == service_journey_pattern.id:
                if isinstance(service_journey_pattern.route_ref_or_route_view, RouteView):
                    if isinstance(service_journey_pattern.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view, LineRefStructure):
                        return service_journey_pattern.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view

        return None

    @staticmethod
    def projectServiceJourneyToTrip(service_journey: ServiceJourney | TemplateServiceJourney, service_journey_pattern: ServiceJourneyPattern | None) -> dict[str, Any]:
        trip = {'route_id': GtfsProfile.getOriginalGtfsIdFromRef(GtfsProfile.getLineRef(service_journey, service_journey_pattern)),
                'service_id': GtfsProfile.getOriginalGtfsIdFromRef(service_journey.day_types.day_type_ref[0]),  # TODO: Guard for duplicates, and AvailabilityCondition
                'trip_id': GtfsProfile.getOriginalGtfsId(service_journey, 'trip_id'),
                'trip_headsign': '', # service_journey.destination.destination_display_ref,
                'trip_short_name': '',
                'direction_id': '',
                'block_id': GtfsProfile.getOrNone(service_journey, "block_ref.ref"),
                'shape_id': '', # TODO: GtfsProfile.getOrNone(service_journey, "route_ref.ref"),
                'wheelchair_accessible': GtfsProfile.getWheelchairAccess(service_journey.accessibility_assessment),
                'bikes_allowed': '' # TODO
                }

        return trip

    @staticmethod
    def projectTemplateServiceJourneyToFrequency(template_service_journey: TemplateServiceJourney) -> Iterable[dict[str, Any]]:
        for frequency_group in template_service_journey.frequency_groups.headway_journey_group_ref_or_headway_journey_group_or_rhythmical_journey_group_ref_or_rhythmical_journey_group:
            if isinstance(frequency_group, HeadwayJourneyGroup):
                first_day_offset = None
                last_day_offset = None
                last_departure_time = None
                for element in frequency_group.first_day_offset_or_last_departure_time_or_last_day_offset_or_first_arrival_time_or_last_arrival_time:
                    if isinstance(element, JourneyFrequencyGroupVersionStructure.FirstDayOffset):
                        first_day_offset = element
                    if isinstance(element, JourneyFrequencyGroupVersionStructure.LastDayOffset):
                        last_day_offset = element
                    if isinstance(element, JourneyFrequencyGroupVersionStructure.LastArrivalTime):
                        last_arrival_time = element.value
                    if isinstance(element, JourneyFrequencyGroupVersionStructure.LastDepartureTime):
                        last_departure_time = element.value

                if last_departure_time is None:
                    warnings.warn("We can't handle LastArrivalTime yet.")
                    continue

                if frequency_group.scheduled_headway_interval is None:
                    warnings.warn("We can't handle NonExactTimes yet.")
                    continue

                headway_secs = datetime.timedelta(days=frequency_group.scheduled_headway_interval.days or 0,
                                   hours=frequency_group.scheduled_headway_interval.hours or 0,
                                   minutes=frequency_group.scheduled_headway_interval.minutes or 0,
                                   seconds=frequency_group.scheduled_headway_interval.seconds or 0) # Technically not correct, practically: yes

                frequency = {'trip_id': GtfsProfile.getOriginalGtfsId(template_service_journey, 'trip_id'),
                        'start_time': GtfsProfile.addDayOffset(frequency_group.first_departure_time.to_time(), first_day_offset),
                        'end_time': GtfsProfile.addDayOffset(last_departure_time, last_day_offset),
                        'headway_secs': int(headway_secs.total_seconds()),
                        'exact_times': int(frequency_group.scheduled_headway_interval is not None)
                        }

                yield frequency

    @staticmethod
    def addDayOffset(time: datetime.time, day_offset: int | None) -> str:
        if day_offset is None:
            return str(time)

        h, m, s = str(time).split(':')
        h_i = int(h) + (24 * day_offset)
        return f"{h_i:02d}:{m}:{s}"

    @staticmethod
    def projectServiceJourneyToStopTimes(service_journey: ServiceJourney | TemplateServiceJourney) -> Iterable[dict[str, Any]]:
        for call in service_journey.calls.call:
            if call.arrival is None:
                call.arrival = call.departure
            elif call.departure is None:
                call.departure = call.arrival

            arrival_time = GtfsProfile.addDayOffset(call.arrival.time, call.arrival.day_offset)
            departure_time = GtfsProfile.addDayOffset(call.departure.time, call.departure.day_offset)
            if arrival_time is None or arrival_time=="None":
                arrival_time = departure_time
            elif departure_time is None or departure_time=="None":
                departure_time = arrival_time
            stop_time = {'trip_id': GtfsProfile.getOriginalGtfsId(service_journey, 'trip_id'),
                         'arrival_time': arrival_time,
                         'departure_time': departure_time,
                         'stop_id': GtfsProfile.getOriginalGtfsIdFromRef(call.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view),
                         'stop_sequence': call.order,
                         'stop_headsign': '',
                         'pickup_type': '', # TODO
                         'drop_off_type': '',  # TODO
                         'continuous_pickup': '',
                         'continuous_drop_off': '',
                         'shape_dist_traveled': '',
                         'timepoint': 1
                    }
            yield stop_time

    @staticmethod
    def projectStopAreaStop(stop_area: StopArea, transformer: Transformer | None = None) -> dict[str, Any]:
        # TODO: parent_station could be obtained from StopPlace or StopArea
        if transformer:
            latitude, longitude = transformer.transform(stop_area.centroid.location.pos.value[0], stop_area.centroid.location.pos.value[1])
        else:
            assert stop_area.centroid.location is not None, f'StopArea without coordinate {stop_area.id}'
            project_location_4326(stop_area.centroid.location)
            latitude, longitude = stop_area.centroid.location.latitude, stop_area.centroid.location.longitude

        stop = {'stop_id': GtfsProfile.getOriginalGtfsId(stop_area, 'stop_id'),
                'stop_code': GtfsProfile.getOptionalPrivateCode(stop_area.public_code),
                'stop_name': GtfsProfile.getOptionalMultilingualString(stop_area.name) or GtfsProfile.getOptionalMultilingualString(stop_area.short_name),
                'stop_desc': GtfsProfile.getOptionalMultilingualString(stop_area.description),
                'stop_lat': round(latitude, 7),
                'stop_lon': round(longitude, 7),
                'zone_id': '',
                'stop_url': '',
                'location_type': 1, # Station
                'parent_station': '',
                'stop_timezone': GtfsProfile.getOrNone(stop_area, 'locale.time_zone'),
                'wheelchair_boarding': '',
                'level_id': '', # stop_place.levels.level_ref_or_level,
                'platform_code': ''
        }

        return stop

    @staticmethod
    def projectStopPlaceToStop(stop_place: StopPlace, transformer: Transformer | None = None) -> dict[str, Any]:
        # TODO: Maybe remove?
        if transformer:
            latitude, longitude = transformer.transform(stop_place.centroid.location.pos.value[0], stop_place.centroid.location.pos.value[1])
        else:
            assert stop_place.centroid.location is not None, f'StopPlace without coordinate {stop_place.public_code} - {stop_place.name}'
            project_location_4326(stop_place.centroid.location)
            latitude, longitude = stop_place.centroid.location.latitude, stop_place.centroid.location.longitude

        stop = {'stop_id': GtfsProfile.getOriginalGtfsId(stop_place, 'stop_id'),
                'stop_code': GtfsProfile.getOptionalPrivateCode(stop_place.public_code),
                'stop_name': GtfsProfile.getOptionalMultilingualString(stop_place.name) or GtfsProfile.getOptionalMultilingualString(stop_place.short_name),
                'stop_desc': GtfsProfile.getOptionalMultilingualString(stop_place.description),
                'stop_lat': round(latitude, 7),
                'stop_lon': round(longitude, 7),
                'zone_id': '',
                'stop_url': '',
                'location_type': 1, # Station
                'parent_station': '',
                'stop_timezone': GtfsProfile.getOrNone(stop_place, 'locale.time_zone'),
                'wheelchair_boarding': '',
                'level_id': stop_place.levels.level_ref_or_level if stop_place.levels is not None and stop_place.levels.level_ref_or_level is not None else '',
                'platform_code': ''
        }

        return stop

    @staticmethod
    def projectQuayStop(stop_place: StopPlace, with_quays: bool = False, transformer: Transformer | None = None) -> Iterable[dict[str, Any]]:
        # TODO: parent_station could be obtained from StopPlace or StopArea
        if transformer:
            latitude, longitude = transformer.transform(stop_place.centroid.location.pos.value[0], stop_place.centroid.location.pos.value[1])
        else:
            assert stop_place.centroid.location is not None, f'StopPlace without coordinate {stop_place.public_code} - {stop_place.name}'
            project_location_4326(stop_place.centroid.location)
            latitude, longitude = stop_place.centroid.location.latitude, stop_place.centroid.location.longitude

        stop = {'stop_id': GtfsProfile.getOriginalGtfsId(stop_place, 'stop_id'),
                'stop_code': GtfsProfile.getOptionalPrivateCode(stop_place.public_code),
                'stop_name': GtfsProfile.getOptionalMultilingualString(stop_place.name) or GtfsProfile.getOptionalMultilingualString(stop_place.short_name),
                'stop_desc': GtfsProfile.getOptionalMultilingualString(stop_place.description),
                'stop_lat': round(latitude, 7),
                'stop_lon': round(longitude, 7),
                'zone_id': GtfsProfile.getTariffZoneFromScheduledStopPoint(stop_place.tariff_zones),
                'stop_url': stop_place.url or '',
                'location_type': 1, # Station
                'parent_station': '',
                'stop_timezone': GtfsProfile.getOrNone(stop_place, 'locale.time_zone'),
                'wheelchair_boarding': GtfsProfile.getWheelchairAccess(stop_place.accessibility_assessment),
                'level_id': '', # stop_place.levels.level_ref_or_level,
                'platform_code': ''
        }

        yield stop

        if stop_place.quays is not None and with_quays:
            for quay in stop_place.quays.taxi_stand_ref_or_quay_ref_or_quay:
                if not isinstance(quay, Quay):
                    continue

                assert quay.centroid.location is not None, f"{quay.id}, GTFS requires a location per Quay"

                if transformer:
                    latitude, longitude = transformer.transform(quay.centroid.location.pos.value[0],
                                                                quay.centroid.location.pos.value[1])
                else:
                    project_location_4326(quay.centroid.location)
                    latitude, longitude = quay.centroid.location.latitude, quay.centroid.location.longitude

                stop = {'stop_id': GtfsProfile.getOriginalGtfsId(quay, 'stop_id'),
                        'stop_code': GtfsProfile.getOptionalPrivateCode(quay.public_code),
                        'stop_name': GtfsProfile.getOptionalMultilingualString(quay.name) or GtfsProfile.getOptionalMultilingualString(quay.short_name),
                        'stop_desc': GtfsProfile.getOptionalMultilingualString(quay.description),
                        'stop_lat': round(latitude, 7),
                        'stop_lon': round(longitude, 7),
                        'zone_id': GtfsProfile.getTariffZoneFromScheduledStopPoint(quay.tariff_zones),
                        'stop_url': quay.url or '',
                        'location_type': 0,  # Platform
                        'parent_station': GtfsProfile.getOriginalGtfsId(stop_place, 'stop_id'),
                        'stop_timezone': GtfsProfile.getOrNone(stop_place, 'locale.time_zone'),
                        'wheelchair_boarding': GtfsProfile.getWheelchairAccess(quay.accessibility_assessment),
                        'level_id': '',  # stop_place.levels.level_ref_or_level,
                        'platform_code': quay.short_code
                        }

                yield stop
        # else:
        #    print(stop_place.id)

    @staticmethod
    def projectRouteLinksToShapes(route: Route, route_links: List[RouteLink], transformer: Transformer | None = None) -> Iterable[dict[str, Any]]:
        sequence = 0
        distance: Decimal = Decimal(0)
        distance_keep: Decimal = Decimal(0)

        for route_link in route_links[0:-1]:
            # TODO: handle variants (posList, pos array)
            # TODO: Add transformer
            l = route_link.line_string.pos_or_point_property_or_pos_list[0].value
            dimensions = route_link.line_string.srs_dimension or 2
            for i in range(0, len(l) - dimensions, dimensions):
                if transformer:
                    latitude, longitude = transformer.transform(l[i], l[i + 1])

                else:
                    latitude, longitude = l[i], l[i + 1]

                shape_point = {'shape_id': GtfsProfile.getOriginalGtfsId(route, 'shape_id'),
                        'shape_pt_lat': round(latitude, 7),
                        'shape_pt_lon': round(longitude, 7),
                        'shape_pt_sequence': sequence,
                        'shape_dist_traveled': (d := distance) if distance > 0 else ''
                }

                sequence += 1
                distance = Decimal(-1)

                yield shape_point

            if route_link.distance is not None and distance_keep >= 0:
                distance_keep += route_link.distance
                distance = distance_keep
            else:
                distance_keep = Decimal(-1)

        l = route_links[-1].line_string.pos_or_point_property_or_pos_list[0].value
        dimensions = route_links[-1].line_string.srs_dimension or 2
        for i in range(0, len(l), dimensions):
            if transformer:
                latitude, longitude = transformer.transform(l[i], l[i + 1])

            else:
                latitude, longitude = l[i], l[i + 1]

            shape_point = {'shape_id': GtfsProfile.getOriginalGtfsId(route, 'shape_id'),
                           'shape_pt_lat': round(latitude, 7),
                           'shape_pt_lon': round(longitude, 7),
                           'shape_pt_sequence': sequence,
                           'shape_dist_traveled': (d := distance) if distance > 0 else ''
                           }

            sequence += 1
            distance = Decimal(-1)

            yield shape_point

    @staticmethod
    def projectServiceLinksToShapes(service_journey_pattern: ServiceJourneyPattern, service_links: List[ServiceLink], transformer: Transformer | None = None) -> Iterable[dict[str, Any]]:
        sequence = 0
        distance: Decimal = Decimal(0)
        distance_keep: Decimal = Decimal(0)

        for route_link in service_links[0:-1]:
            # TODO: handle variants (posList, pos array)
            l = route_link.line_string.pos_or_point_property_or_pos_list[0].value
            dimensions = route_link.line_string.srs_dimension or 2
            for i in range(0, len(l) - dimensions, dimensions):
                if transformer:
                    latitude, longitude = transformer.transform(l[i], l[i + 1])

                else:
                    latitude, longitude = l[i], l[i + 1]

                shape_point = {'shape_id': GtfsProfile.getOriginalGtfsId(service_journey_pattern, 'shape_id'),
                        'shape_pt_lat': round(latitude, 7),
                        'shape_pt_lon': round(longitude, 7),
                        'shape_pt_sequence': sequence,
                        'shape_dist_traveled': (d := distance) if distance > 0 else ''
                }

                sequence += 1
                distance = Decimal(-1)

                yield shape_point

            if route_link.distance is not None and distance_keep >= 0:
                distance_keep += route_link.distance
                distance = distance_keep
            else:
                distance_keep = Decimal(-1)

        l = service_links[-1].line_string.pos_or_point_property_or_pos_list[0].value
        dimensions = service_links[-1].line_string.srs_dimension or 2
        for i in range(0, len(l), dimensions):
            if transformer:
                latitude, longitude = transformer.transform(l[i], l[i + 1])

            else:
                latitude, longitude = l[i], l[i + 1]

            shape_point = {'shape_id': GtfsProfile.getOriginalGtfsId(service_journey_pattern, 'shape_id'),
                           'shape_pt_lat': round(latitude, 7),
                           'shape_pt_lon': round(longitude, 7),
                           'shape_pt_sequence': sequence,
                           'shape_dist_traveled': (d := distance) if distance > 0 else ''
                           }

            sequence += 1
            distance = Decimal(-1)

            yield shape_point
