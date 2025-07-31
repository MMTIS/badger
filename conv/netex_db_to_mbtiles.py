import copy
import json
from typing import Generator, Tuple, Iterator
import gzip

import netex
from netex import LocationStructure2, LineString, ScheduledStopPoint, Polygon, MultiSurface, RouteLink, PosList, Route, \
    Line
from netexio.database import Database, Tid
from netexio.dbaccess import recursive_attributes, load_referencing
from netexio.pickleserializer import MyPickleSerializer
from transformers.projection import get_all_geo_elements, reprojection
from shapely.geometry import shape, mapping, box
from pymbtiles import MBtiles, Tile
import pathlib
from pyproj import Transformer
from shapely.geometry import LineString, box
from shapely.ops import split

from utils.utils import get_object_name
from collections import defaultdict


def chunk_list(lst, dimension):
    return [lst[i:i + dimension] for i in range(0, len(lst), dimension)]

def to_feature(deserialized: Tid, clazz) -> Generator[dict, None, None]:
    for obj, path in recursive_attributes(deserialized, []):
        if isinstance(obj, netex.LocationStructure2):
            feature = {
                "geometry": {
                    "type": "Point",
                    "coordinates": obj.pos.value,
                },
                "properties": {"name": getattr(getattr(deserialized, 'name', None), 'value', None), "id": deserialized.id},
                "class": clazz
            }
            yield feature

        elif isinstance(obj, netex.LineString):
            if isinstance(obj.pos_or_point_property_or_pos_list[0], PosList):
                pos_list = obj.pos_or_point_property_or_pos_list[0]
                feature = {
                    "geometry": {
                        "type": "LineString",
                        "coordinates": chunk_list(pos_list.value, pos_list.srs_dimension),
                    },
                    "properties": {"name": getattr(getattr(deserialized, 'name', None), 'value', None),
                                   "id": deserialized.id},
                    "class": clazz
                }
                yield feature

        elif isinstance(obj, netex.Polygon):
            # TODO: Polygon
            pass

        elif isinstance(obj, netex.MultiSurface):
            pass

            # if obj.surface_members:
            #     for surface_member in obj.surface_member:
            #        if surface_member.polygon:
            #            project_polygon(surface_member.polygon, crs_to)
            #    if obj.surface_members.polygon:
            #        for polygon in obj.surface_members.polygon:
            #            if polygon:
            #                project_polygon(polygon, crs_to)

        # TODO: Do something intelligent with the relationship between objects
        # A routelink is in used for routes, routes are in use for lines, it would make sense
        # to tag the routelink with all the (deep) references it has, being route and therefore line.

        # For ServiceLink (in the context of EPIP) we can assign which ServiceJourneyPatterns
        # use the ServiceLink, and also find the scope.

from rtree import index as rtree_index
from collections import defaultdict
from mapbox_vector_tile import encode

def mercator_to_tile_coords_tms(x: float, y: float, zoom: int, extent: int = 4096) ->  Tuple[int, int, int, int, int]:
    """
    Zet EPSG:3857-coördinaat om naar (tile_x, tile_y) in TMS en relatieve positie (rel_x, rel_y) binnen de tile.

    Parameters:
        x (float): X-coördinaat in meters (EPSG:3857)
        y (float): Y-coördinaat in meters (EPSG:3857)
        zoom (int): Zoomniveau
        extent (int): Extent van de tile (standaard 4096)

    Returns:
        (tile_x, tile_y): positie van de tegel (TMS-conventie)
        (rel_x, rel_y): relatieve pixelpositie binnen de tile (tussen 0 en extent)
    """
    half_size = 20037508.342789244  # meter
    world_size = 2 * half_size

    # Normaliseer naar [0, 1]
    u = (x + half_size) / world_size
    v = (y + half_size) / world_size  # Let op: y-as omhoog voor TMS

    scale = 2 ** zoom
    tile_x = int(u * scale)
    tile_y_xyz = int((1.0 - v) * scale)
    tile_y_tms = (scale - 1) - tile_y_xyz  # spiegelen t.o.v. XYZ

    # Offset binnen tile
    rel_x = int((u * scale - tile_x) * extent)
    rel_y_xyz = int(((1.0 - v) * scale - tile_y_xyz) * extent)
    rel_y_tms = extent - 1 - rel_y_xyz  # spiegelen binnen tile

    return (tile_x, tile_y_tms, zoom, [rel_x, rel_y_tms])


def linestring_to_tile_segments_clipped(
        coordinates: list,
        zoom: int,
        extent: int = 4096
) -> Iterator[tuple[int, int, int, dict[str, list[tuple[int, int]]]]]:
    """
    Split een LineString in segmenten per tegel, geclipt op tegelgrenzen en omgerekend naar lokale tile-coördinaten.

    Parameters:
        line (LineString): In EPSG:3857
        zoom (int): Zoomniveau
        extent (int): Tegelresolutie (standaard 4096)

    Yields:
        Tuple met (tile_x, tile_y, zoom, attribuutdict), waarbij 'geometry' een lijst bevat van (x, y) punten binnen de tile.
    """

    line = LineString(coordinates)
    if zoom < 12:
        meters_per_pixel = 156543.03 / (2 ** zoom)
        simplified = line.simplify(meters_per_pixel, preserve_topology=True)
        line = simplified

    if line.is_empty or len(line.coords) < 2:
        return  # Lege of puntvormige lijnen overslaan

    # Wereldbreedte in meters
    half_size = 20037508.342789244
    world_size = 2 * half_size
    scale = 2 ** zoom
    tile_size = world_size / scale  # in meters

    minx, miny, maxx, maxy = line.bounds
    tile_min_x = int((minx + half_size) / tile_size)
    tile_max_x = int((maxx + half_size) / tile_size)
    tile_min_y = int((miny + half_size) / tile_size)
    tile_max_y = int((maxy + half_size) / tile_size)

    for tile_x in range(tile_min_x, tile_max_x + 1):
        for tile_y in range(tile_min_y, tile_max_y + 1):
            # TMS y-coördinaat spiegelen
            # tile_y_tms = (scale - 1) - tile_y

            # Bepaal tile-bounds in EPSG:3857
            tile_min_mx = -half_size + tile_x * tile_size
            tile_max_mx = tile_min_mx + tile_size
            tile_min_my = -half_size + tile_y * tile_size
            tile_max_my = tile_min_my + tile_size
            tile_box = box(tile_min_mx, tile_min_my, tile_max_mx, tile_max_my)

            clipped = line.intersection(tile_box)
            if clipped.is_empty:
                continue

            if clipped.geom_type == "LineString":
                segments = [clipped]
            elif clipped.geom_type == "MultiLineString":
                segments = [g for g in clipped.geoms if len(g.coords) >= 2]
            else:
                continue  # Geen lijnvormige geometrie over

            for seg in segments:
                rel_coords = []
                for x, y in seg.coords:
                    # Normaliseer naar lokale tile
                    u = (x - tile_min_mx) / tile_size
                    v = (y - tile_min_my) / tile_size
                    rel_x = int(u * extent)
                    rel_y = int(v * extent)
                    # rel_y = extent - 1 - int(v * extent)  # y-as spiegelen voor TMS
                    rel_coords.append((rel_x, rel_y))

                # print(rel_coords)
                # Filter segmenten met minder dan 2 unieke punten
                if len(rel_coords) >= 2 and len(set(rel_coords)) >= 2:
                    yield (
                        tile_x,
                        tile_y,
                        zoom,
                        rel_coords
                    )


def features_to_mvt_tile(features: list[dict]) -> bytes:
    layers = defaultdict(list)

    for feature in features:
        layer_name = feature.get("class")
        layers[layer_name].append(feature)

    return encode([
        {"name": name, "features": feats}
        for name, feats in layers.items()
    ])

def main(database: str, output_filename: str) -> None:
    rtree = rtree_index.Index()
    object_map = {}  # id → geometry

    tile_index = defaultdict(list)
    minx = 50000000
    maxx = -50000000
    miny = 50000000
    maxy = -50000000
    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

    with Database(database, MyPickleSerializer(compression=True), readonly=True) as db_read:
        """
        for clazz in db_read.tables(exclusively=set((ScheduledStopPoint,))):
        # for clazz in db_read.tables(exclusively=set(get_all_geo_elements())):
            src_db = db_read.open_database(clazz, readonly=True)
            if not src_db:
                continue

            with db_read.env.begin(db=src_db, buffers=True, write=False) as src_txn:
                cursor = src_txn.cursor()

                for key, value in cursor:
                    # transformed_value = db.serializer.marshall(reprojection(db.serializer.unmarshall(value, clazz), crs_to), clazz)
                    # TODO: We may not want to expose our internal task queue.
                    # db.task_queue.put((LmdbActions.WRITE, src_db, key, transformed_value))
                    for feature in to_feature(reprojection(db_read.serializer.unmarshall(value, clazz), "EPSG:3857")):

                    # for feature in to_feature(db_read.serializer.unmarshall(value, clazz)):
                        print(feature)
                        x, y = feature['geometry']['coordinates']  # EPSG:3857
                        minx = min(minx, x)
                        maxx = max(maxx, x)
                        miny = min(miny, y)
                        maxy = max(maxy, y)

                        for z in range(8, 19):
                            rx, ry, tile_x, tile_y_tms, zoom = mercator_to_tile_coords_tms(float(x), float(y), z)
                            newfeature = copy.deepcopy(feature)
                            newfeature['geometry']['coordinates'] = [rx, ry]
                            tile_index[(zoom, tile_x, tile_y_tms)].append(newfeature)


        """
        for clazz in db_read.tables(exclusively=set((RouteLink,))):
            class_name = get_object_name(clazz)
            src_db = db_read.open_database(clazz, readonly=True)
            if not src_db:
                continue

            with db_read.env.begin(db=src_db, buffers=True, write=False) as src_txn:
                cursor = src_txn.cursor()

                for key, value in cursor:
                    for feature in to_feature(reprojection(db_read.serializer.unmarshall(value, clazz), "EPSG:3857"), class_name):
                        # print(feature)

                        for x, y in feature['geometry']['coordinates']:
                            minx = min(minx, x)
                            maxx = max(maxx, x)
                            miny = min(miny, y)
                            maxy = max(maxy, y)

                        for z in range(7, 14):
                            for tile_x, tile_y_tms, zoom, coordinates in linestring_to_tile_segments_clipped(feature['geometry']['coordinates'], z):
                                newfeature = copy.deepcopy(feature)
                                newfeature['geometry']['coordinates'] = coordinates
                                tile_index[(zoom, tile_x, tile_y_tms)].append(newfeature)

                                # print(newfeature)


    with MBtiles(output_filename, mode="w") as out:
        for (z, x, y), features in tile_index.items():
            out.write_tile(z, x, y, gzip.compress(features_to_mvt_tile(features)))

        centerx, centery = transformer.transform(minx + (maxx - minx) / 2, miny + (maxy - miny) / 2)
        minxy = transformer.transform(minx, miny)
        maxxy = transformer.transform(maxx, maxy)

        out.meta = {
            "name": "NeTEx",
            "type": "overlay",
            "format": "pbf",
            "version": "2",
            "minzoom": 8,
            "maxzoom": 18,
            'bounds': f"{minxy[0]:.3f},{minxy[1]:.3f},{maxxy[0]:.3f},{maxxy[1]:.3f}",
            'center': f"{centerx:.3f},{centery:.3f},13",
            "description": "Scheduled stops exported as vector tiles",
            "tilestats": json.dumps({
                "layerCount": 1,
                "layers": [
                    {
                        "count": 2,
                        "geometry": "LineString",
                        "layer": "RouteLink"
                    }
                ]
            }),
            "json": json.dumps({
                "vector_layers": [{
                    "id": "RouteLink",
                    "description": "Layer with route links",
                    "minzoom": 8,
                    "maxzoom": 18,
                    "fields": {"id": "String", "name": "String"}
                }]
            })
        }

        # print(out.meta)

# def routelink_to_link(database: str):
#    with Database(database, MyPickleSerializer(compression=True), readonly=True) as db_read:
#        for reference_id, reference_version, reference_class, path in load_referencing(db_read, Route):
#            if reference_class == Line:



if __name__ == '__main__':
    import argparse

    argument_parser = argparse.ArgumentParser(description='Export any lmdb file into a NeTEx GeneralFrame')
    argument_parser.add_argument('database', type=str, help='lmdb to be read from')
    argument_parser.add_argument('output_filename', type=str, help='The output XML file')
    args = argument_parser.parse_args()

    main(args.database, args.output_filename)
