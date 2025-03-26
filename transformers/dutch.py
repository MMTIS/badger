import sys
from functools import partial
from typing import Generator, Dict, Any

from netexio.database import Database
from netexio.dbaccess import load_generator, load_local
from netex import ScheduledStopPoint, Codespace, Version, ServiceJourney
from multiprocessing import Pool

from utils.refs import getIndex
from transformers.timedemandtypesprofile import TimeDemandTypesProfile
from transformers.projection import project_location

import time


def dutch_scheduled_stop_point_generator(db_read: Database, db_write: Database, generator_defaults: dict[str, Any], pool: Pool):
    print(sys._getframe().f_code.co_name)

    def process(ssp: ScheduledStopPoint):
        project_location(ssp.location, "EPSG:28992", quantize='1.0')
        return ssp

    def query(db_read: Database, generator_defaults: dict[str, Any]) -> Generator:
        _load_generator = load_generator(db_read, ScheduledStopPoint)
        for ssp in pool.imap_unordered(partial(process, generator_defaults=generator_defaults), _load_generator, chunksize=100):
            yield ssp

    db_write.insert_objects_on_queue(ScheduledStopPoint, query(db_read), True)


def dutch_scheduled_stop_point_memory(db_read: Database, db_write: Database, generator_defaults: dict[str, Any]):
    print(sys._getframe().f_code.co_name)

    scheduled_stop_points = load_local(db_read, ScheduledStopPoint)
    for ssp in scheduled_stop_points:
        ssp: ScheduledStopPoint
        ssp.stop_areas = None
        if ssp.location is not None:
            project_location(ssp.location, "EPSG:28992", quantize='1.0')
        else:
            print(f"ScheduledStopPoint {ssp.id} does not have a location.")

    db_write.insert_objects_on_queue(ScheduledStopPoint, scheduled_stop_points, True)


def dutch_service_journey_pattern_time_demand_type_memory(db_read: Database, db_write: Database):
    print(sys._getframe().f_code.co_name)

    codespaces = load_local(db_read, Codespace, 1)
    versions = load_local(db_read, Version, 1)
    ssps: Dict[str, ScheduledStopPoint] = getIndex(load_local(db_read, ScheduledStopPoint))

    tdtp = TimeDemandTypesProfile(codespace=codespaces[0], version=versions[0])

    i = 0

    now = time.time()
    print("_load_generator: ", now, int(0))
    _load_generator = load_generator(db_read, ServiceJourney)

    _prev = now
    now = time.time()
    print("for loop: ", now, int(now - _prev))
    for sj in _load_generator:
        sjp = tdtp.getServiceJourneyPatternGenerator(db_read, db_write, sj, ssps)
        tdtp.getTimeDemandTypeGenerator(db_read, db_write, sj, ssps)
        sj.calls = None
        db_write.insert_one_object(sj)
        i += 1
        if i % 100 == 0:
            _prev = now
            now = time.time()
            print("\r", "ServiceJourney", str(i), str(now), str(int(now - _prev)))
    print("\n")
    _prev = now
    now = time.time()
    print("done: ", now, int(now - _prev))
