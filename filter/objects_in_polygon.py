import logging
from typing import Generator

from numpy.f2py.crackfortran import sourcecodeform
from shapely.geometry import Point, Polygon, shape
from shapely.prepared import prep

import utils.utils
from netex import LocationStructure2, ScheduledStopPoint, ServiceJourneyPattern, TimingLink, ServiceLink, RouteLink, \
    Route, ServiceJourney, Line, TemplateServiceJourney
from netexio.database import Database, Tid
from netexio.dbaccess import load_generator, load_referencing_inwards, load_referencing, load_local
from netexio.pickleserializer import MyPickleSerializer
from transformers.projection import project_location
from utils.aux_logging import prepare_logger, log_all

import json


# Move these transformations somewhere else
def fromNeTEx(location_structure: LocationStructure2 | None) -> Point | None:
    # In these function we expect EPSG:4326 / WGS84

    if not location_structure:
        return None

    if location_structure.longitude and location_structure.latitude:
        return Point(float(location_structure.longitude), float(location_structure.latitude))

    elif location_structure.pos and location_structure.pos.value:
        # This operation actually changes the structure.
        project_location(location_structure, "EPSG:4326")
        return Point(float(location_structure.pos.value[1]), float(location_structure.pos.value[0]))

    return None

def recursive_lookup(db: Database, clazz: type[Tid], id: str, version: str, resolved_ssp: set[str], resolved_sjp: set[str], resolved_timing_links: set[str], resolved_service_links: set[str], resolved_route_point: set[str], resolved_line: set[str], resolved_route: set[str], resolved_service_journey: set[str]) -> Generator[Line, None, None]:
    for parent_id, parent_version, parent_class, _path in load_referencing(db, clazz, id, version):
        if parent_class == 'Route':
            if parent_id not in resolved_route:
                resolved_route.add(parent_id)
                yield from recursive_lookup(db, Route, parent_id, parent_version, resolved_ssp, resolved_sjp, resolved_timing_links, resolved_service_links, resolved_route_point, resolved_line, resolved_route, resolved_service_journey)
        elif parent_class == 'Line':
            if parent_id not in resolved_line:
                resolved_line.add(parent_id)
                line = db.get_single(Line, parent_id, parent_version)
                if line:
                    yield line

def recursive_inwards_lookup(db: Database, clazz: type[Tid], id: str, version: str, resolved_ssp: set[str], resolved_sjp: set[str], resolved_timing_links: set[str], resolved_service_links: set[str], resolved_route_point: set[str], resolved_line: set[str], resolved_route: set[str], resolved_service_journey: set[str]) -> Generator[Line, None, None]:
    # This is going to result into: ServiceJourneyPatterns, TimingLinks, ServiceLinks, RoutePoints (via Projection)
    for parent_id, parent_version, parent_class, _path in load_referencing_inwards(db, clazz, id, version):
        if parent_class == 'ServiceJourneyPattern':
            if parent_id not in resolved_sjp:
                resolved_sjp.add(parent_id)
                yield from recursive_inwards_lookup(db, ServiceJourneyPattern, parent_id, parent_version, resolved_ssp, resolved_sjp, resolved_timing_links, resolved_service_links, resolved_route_point, resolved_line, resolved_route, resolved_service_journey)
                yield from recursive_lookup(db, ServiceJourneyPattern, parent_id, parent_version, resolved_ssp, resolved_sjp, resolved_timing_links, resolved_service_links, resolved_route_point, resolved_line, resolved_route, resolved_service_journey)
        elif parent_class == 'TimingLink':
            if parent_id not in resolved_timing_links:
                resolved_timing_links.add(parent_id)
                yield from recursive_inwards_lookup(db, TimingLink, parent_id, parent_version, resolved_ssp, resolved_sjp, resolved_timing_links, resolved_service_links, resolved_route_point, resolved_line, resolved_route, resolved_service_journey)
        elif parent_class == 'ServiceLink':
            if parent_id not in resolved_service_links:
                resolved_service_links.add(parent_id)
                yield from recursive_inwards_lookup(db, ServiceLink, parent_id, parent_version, resolved_ssp, resolved_sjp, resolved_timing_links, resolved_service_links, resolved_route_point, resolved_line, resolved_route, resolved_service_journey)
        elif parent_class == 'RouteLink':
            if parent_id not in resolved_route_point:
                resolved_route_point.add(parent_id)
                yield from recursive_inwards_lookup(db, RouteLink, parent_id, parent_version, resolved_ssp, resolved_sjp, resolved_timing_links, resolved_service_links, resolved_route_point, resolved_line, resolved_route, resolved_service_journey)
        elif parent_class == 'Route':
            if parent_id not in resolved_route:
                resolved_route.add(parent_id)
                yield from recursive_lookup(db, Route, parent_id, parent_version, resolved_ssp, resolved_sjp, resolved_timing_links, resolved_service_links, resolved_route_point, resolved_line, resolved_route, resolved_service_journey)
        elif parent_class == 'Line':
            if parent_id not in resolved_line:
                resolved_line.add(parent_id)
                line = db.get_single(Line, parent_id, parent_version)
                if line:
                    yield line
        elif parent_class == 'ServiceJourney':
            if parent_id not in resolved_service_journey:
                resolved_service_journey.add(parent_id)
                yield from recursive_lookup(db, ServiceJourney, parent_id, parent_version, resolved_ssp, resolved_sjp, resolved_timing_links, resolved_service_links, resolved_route_point, resolved_line, resolved_route, resolved_service_journey)



def main(source_database_file: str, geojson_file: str):
    resolved_ssp: set[str] = set([])
    resolved_sjp: set[str] = set([])
    resolved_timing_links: set[str] = set([])
    resolved_service_links: set[str] = set([])
    resolved_route_point: set[str] = set([])
    resolved_line: set[str] = set([])
    resolved_route: set[str] = set([])
    resolved_service_journey: set[str] = set([])

    polygon = prep(shape(json.load(open(geojson_file, 'r'))['features'][0]))
    lines = []
    stops = []
    with Database(source_database_file, serializer=MyPickleSerializer(compression=True), readonly=True) as db_read:
        for ssp in load_generator(db_read, ScheduledStopPoint):
            # resolved_ssp.add(ssp.id)
            point = fromNeTEx(ssp.location)
            # stops.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [point.x, point.y]}, "properties": {"name": ssp.name.value}})
            if polygon.contains(point):
                if 'NL' in ssp.id:
                    lines += list(recursive_inwards_lookup(db_read, ScheduledStopPoint, ssp.id, ssp.version, resolved_ssp, resolved_sjp, resolved_timing_links, resolved_service_links, resolved_route_point, resolved_line, resolved_route, resolved_service_journey))

        ssps = set([])
        for line in lines:
            for parent_id, parent_version, parent_class, _path in load_referencing_inwards(db_read, Line, line.id, line.version):
                if parent_class == 'ServiceJourney':
                    for parent_id, parent_version, parent_class, _path in load_referencing(db_read, ServiceJourney, parent_id, parent_version):
                        if parent_class == 'ScheduledStopPoint':
                            ssp = db_read.get_single(ScheduledStopPoint, parent_id, parent_version)
                            if ssp.id not in ssps:
                                ssps.add(ssp.id)
                                point = fromNeTEx(ssp.location)
                                stops.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [point.x, point.y]}, "properties": {"name": ssp.name.value}})



        print(len(lines))

        json.dump({ "type": "FeatureCollection", "features": stops }, open("/tmp/stops2.geojson", 'w'))

        stops = []
        for ssp in load_local(db_read, ScheduledStopPoint):
            point = fromNeTEx(ssp.location)
            stops.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [point.x, point.y]}, "properties": {"name": ssp.name.value}})

        json.dump({"type": "FeatureCollection", "features": stops}, open("/tmp/stopsall.geojson", 'w'))

if __name__ == '__main__':
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description='Filter a NeTEx database for a location')
    parser.add_argument('database', type=str, help='LMDB file to search.')
    parser.add_argument('geojson', type=str, help='GeoJSON file with polygon to select.')
    parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.database, args.geojson)
    except Exception as e:
        log_all(logging.ERROR, f'{e} {traceback.format_exc()}')
        raise e


