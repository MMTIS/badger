import datetime
import glob
import re
import warnings
from itertools import groupby
from typing import List

from xsdata.formats.dataclass.context import XmlContext
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.parsers.config import ParserConfig
from xsdata.formats.dataclass.parsers.handlers import LxmlEventHandler

import lxml
import hashlib

from transformers.callsprofile import CallsProfile
from netexio import dbaccess
from netexio.database import Database
from netexio.dbaccess import load_local, load_generator
from transformers.gtfsprofile import GtfsProfile
from netex import Line, StopPlace, Codespace, ScheduledStopPoint, LocationStructure2, PassengerStopAssignment, \
    Authority, Operator, Branding, UicOperatingPeriod, DayTypeAssignment, ServiceJourney, ServiceJourneyPattern, \
    DataSource, StopPlaceEntrance, TemplateServiceJourney, InterchangeRule, ServiceJourneyInterchange, JourneyMeeting, \
    AvailabilityCondition, DayType, PropertiesOfDayRelStructure, DayOfWeekEnumeration, OperatingPeriod, Version
from netexio.pickleserializer import MyPickleSerializer
from transformers.nordicprofile import NordicProfile
from utils.refs import getId, getRef, getIndex

from decimal import Decimal

from transformers.timetabledpassingtimesprofile import TimetablePassingTimesProfile

import csv

import zipfile
from utils.aux_logging import *
import traceback
from configuration import defaults
from transformers.gtfs import gtfs_calendar_and_dates, gtfs_calendar_and_dates2


def extract(archive, database: str):
    agencies = {}
    used_agencies = set([])
    routes = {}
    stops = {}
    psas = {}
    uic_operating_periods = {}
    max_date = str(datetime.date.today()).replace('-', '')
    calendars = {}
    calendar_dates = {}

    # We can't append in a ZipFile
    # GtfsProfile.writeToZipFile(archive, 'trips.txt', [GtfsProfile.empty_trip], write_header=True)
    # GtfsProfile.writeToZipFile(archive, 'stop_times.txt', [GtfsProfile.empty_stop_time], write_header=True)

    with Database(database, serializer=MyPickleSerializer(compression=True), readonly=True) as db_read:
        datasources: List[DataSource] = load_local(db_read, DataSource, 1)
        codespaces: List[Codespace] = load_local(db_read, Codespace, 1)

        for authority in load_generator(db_read, Authority):
            agency = GtfsProfile.projectAuthorityToAgency(authority)
            agencies[agency['agency_id']] = agency

        for operator in load_generator(db_read, Operator):
            agency = GtfsProfile.projectOperatorToAgency(operator)
            agencies[agency['agency_id']] = agency

        stop_places = getIndex(load_local(db_read, StopPlace))
        quay_to_sp = {}
        for stop_place in stop_places.values():
            stop_place: StopPlace
            if stop_place.quays:
                for quay in stop_place.quays.taxi_stand_ref_or_quay_ref_or_quay:
                    # TODO: Replace with proper checks based on object type.
                    if hasattr(quay,"id"):
                        quay_to_sp[quay.id] = stop_place
                    else:
                        quay_to_sp[quay.ref] = stop_place

        psas = {}
        for psa in load_generator(db_read, PassengerStopAssignment):
            psa: PassengerStopAssignment
            if psa.taxi_rank_ref_or_stop_place_ref_or_stop_place is not None:
                sp = stop_places.get(psa.taxi_rank_ref_or_stop_place_ref_or_stop_place.ref, None)
                if sp is not None:
                    psas[psa.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point.ref] = sp
            elif psa.taxi_stand_ref_or_quay_ref_or_quay is not None:
                sp = quay_to_sp.get(psa.taxi_stand_ref_or_quay_ref_or_quay.ref, None)
                if sp is not None:
                    psas[psa.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point.ref] = sp

        # TODO: GTFS does not support Branding, so in order to facilitate it we will make it a separate Agency
        # A branding must have an 'original' agency or authority
        # for branding in load_generator(db_read, Branding):
        #     agency = GtfsProfile.projectBrandingToAgency(branding)
        #     agencies[agency['agency_id']] = agency

        for scheduled_stop_point in load_local(db_read, ScheduledStopPoint):
            stop_place = psas.get(scheduled_stop_point.id, None)
            if stop_place is not None:
                stop_place_ref = getRef(stop_place)
                stop = GtfsProfile.projectStopPlaceToStop(stop_place)
                stops[stop['stop_id']] = stop
            else:
                stop_place_ref = None

            stop = GtfsProfile.projectScheduledStopPointToStop(scheduled_stop_point, stop_place)
            if stop is not None:
                stops[stop['stop_id']] = stop
                if stop_place is not None:
                    if stop_place.entrances is not None:
                        for entrance in stop_place.entrances.parking_entrance_ref_or_entrance_ref_or_entrance:
                            if isinstance(entrance, StopPlaceEntrance):
                                stop = GtfsProfile.projectStopEntranceToStop(entrance, stop_place)
                                if stop is not None:
                                    stops[stop['stop_id']] = stop

        for line in load_generator(db_read, Line):
            route = GtfsProfile.projectLineToRoute(line)
            if route is not None:
                routes[route['route_id']] = route
                used_agencies.add(route['agency_id'])

        GtfsProfile.writeToZipFile(archive,'agency.txt', [y for x, y in agencies.items() if x in used_agencies], write_header=True)
        GtfsProfile.writeToZipFile(archive,'routes.txt', list(routes.values()), write_header=True)
        GtfsProfile.writeToZipFile(archive,'stops.txt', list(stops.values()), write_header=True)

        # GTFS Calendar and GTFS Calendar Dates
        # A trip in GTFS points to a single service_id, this is analogue to ServiceJourney and DayTypeRef.
        # A DayTypeAssignment is a relationship to a DayTypeRef; being either Available or Unavailable with a "Date" is analogue to calendar_dates.txt
        # A DayTypeAssignment is a relationship to a DayTypeRef: being Available with an "OperatingPeriod" is analogue to calendar.txt

        day_types: dict[str, DayType] = getIndex(load_local(db_read, DayType))

        # TODO: replace
        for day_type in load_generator(db_read, DayType):
            GtfsProfile.temporaryDayTypeServiceId(day_type)

        for day_type_ref, day_type_assignments in groupby(load_generator(db_read, DayTypeAssignment), key=lambda day_type_assignment: day_type_assignment.day_type_ref):
            day_type: DayType = day_types[day_type_ref.ref]
            day_type_assignments: list[DayTypeAssignment] = list(day_type_assignments)

            if day_type.private_codes:
                service_ids = [private_code.value for private_code in day_type.private_codes.private_code if private_code.type_value == 'service_id']
                service_id = service_ids[0] if len(service_ids) > 0 else day_type.id
            else:
                service_id = day_type.id

            exceptions = []
            for calendar, calendar_date in gtfs_calendar_and_dates2(db_read, day_type, day_type_assignments):
                if calendar is not None:
                    calendars[service_id] = calendar

                if calendar_date is not None:
                    exceptions.append(calendar_date)

            l = calendar_dates.get(service_id, [])
            calendar_dates[service_id] = l + exceptions

        if len(calendars.values()) > 0:
            GtfsProfile.writeToZipFile(archive, 'calendar.txt', list(calendars.values()), write_header=True)

            for calendar in calendars.values():
                if calendar['end_date'] > max_date:
                    max_date = calendar['end_date']

        for service_id, exceptions in calendar_dates.items():
            for calendar_date in exceptions:
                if calendar_date['date'] > max_date:
                    max_date = calendar_date['date']

        GtfsProfile.writeToZipFile(archive,'calendar_dates.txt', [item for row in calendar_dates.values() for item in row], write_header=True)

        trips = []
        stop_times = []
        service_journey_patterns = load_local(db_read, ServiceJourneyPattern)

        service_journey_patterns_index = getIndex(service_journey_patterns)

        for service_journey in load_generator(db_read, ServiceJourney):
            service_journey_pattern = service_journey_patterns_index.get(service_journey.journey_pattern_ref.ref) if service_journey.journey_pattern_ref else None
            trip = GtfsProfile.projectServiceJourneyToTrip(service_journey, service_journey_pattern)
            trips.append(trip)

            CallsProfile.getCallsFromTimetabledPassingTimes(service_journey, service_journey_pattern)
            stop_times += list(GtfsProfile.projectServiceJourneyToStopTimes(service_journey))

        frequencies = []
        for template_service_journey in load_generator(db_read, TemplateServiceJourney):
            service_journey_pattern = service_journey_patterns_index.get(template_service_journey.journey_pattern_ref.ref) if template_service_journey.journey_pattern_ref else None
            trip = GtfsProfile.projectServiceJourneyToTrip(template_service_journey, service_journey_pattern)
            trips.append(trip)

            CallsProfile.getCallsFromTimetabledPassingTimes(service_journey, service_journey_pattern)
            stop_times += list(GtfsProfile.projectServiceJourneyToStopTimes(service_journey))

            frequencies += list(GtfsProfile.projectTemplateServiceJourneyToFrequency(template_service_journey))

        # TODO: maybe do this per trip?
        GtfsProfile.writeToZipFile(archive,'trips.txt', trips, write_header=True)
        if len(frequencies) > 0:
            GtfsProfile.writeToZipFile(archive, 'frequencies.txt', frequencies, write_header=True)

        GtfsProfile.writeToZipFile(archive,'stop_times.txt', stop_times, write_header=True)
        trips = frequencies = stop_times = None

        routes = agency = stops = None


        transfers = [GtfsProfile.projectInterchangeRuleToTransfer(transfer) for transfer in load_generator(db_read, InterchangeRule)] + [GtfsProfile.projectServiceJourneyInterchangeToTransfer(transfer) for transfer in load_generator(db_read, ServiceJourneyInterchange)] + [GtfsProfile.projectServiceJourneyMeeting(transfer) for transfer in load_generator(db_read, JourneyMeeting)]

        transfers = list({(v['from_stop_id'], v['to_stop_id'], v['from_route_id'], v['to_route_id'], v['from_trip_id'], v['to_trip_id'], v['transfer_type'], v['min_transfer_time']):v for v in transfers}.values())

        GtfsProfile.writeToZipFile(archive, 'transfers.txt', transfers, write_header=True)

        # TODO: This concept is deprecated, we need to store the CompositeFrame ValidBetween on something.
        versions = load_local(db_read, Version)

        GtfsProfile.writeToZipFile(archive,'feed_info.txt', [{
            'feed_publisher_name': datasources[0].name.value if len(datasources) > 0 else defaults["feed_publisher_name"],
            'feed_publisher_url': codespaces[0].xmlns_url if len(codespaces) > 0 else defaults["feed_publisher_url"],
            'feed_lang': 'en', # TODO
            'default_lang': 'en', # TODO
            'feed_start_date': str(versions[0].start_date.to_datetime().date()).replace('-', '') if len(versions) > 0 else '',
            'feed_end_date': str(versions[0].end_date.to_datetime().date()).replace('-', '') if len(versions) > 0 else '',
            'feed_version': str(datetime.date.today()).replace('-', ''),
            'feed_contact_email': '',
            'feed_contact_url': ''
        }], write_header=True)

def main(netex,gtfs):
    with zipfile.ZipFile(gtfs, 'w') as archive:
        extract(archive, netex)


if __name__ == '__main__':
    import argparse
    argument_parser = argparse.ArgumentParser(description='Convert prepared lmdb database into GTFS')
    argument_parser.add_argument('netex', type=str, help='The lmdb database')
    argument_parser.add_argument('gtfs', type=str, help='The output gtfs filename')
    argument_parser.add_argument('--log_file', type=str, required=False, help='the logfile') # TODO: use logger in this file
    args = argument_parser.parse_args()
    main(args.netex, args.gtfs)

