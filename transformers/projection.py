import functools
import logging
from decimal import Decimal, ROUND_HALF_UP
from itertools import chain
from typing import Any, Generator

from pyproj import Transformer
from pyproj.exceptions import CRSError

from utils.aux_logging import log_once
from utils.mro_attributes import list_attributes
from netex import Polygon, PosList, Pos, LocationStructure2, LineString, SimplePointVersionStructure, MultiSurface
from netexio.database import Database, LmdbActions
from netexio.dbaccess import recursive_attributes
from utils.utils import get_interesting_classes

from netexio.serializer import Serializer
from utils.utils import get_object_name

transformers: dict[str, Transformer] = {}

GEO_CLASSES = {LocationStructure2, LineString, Polygon, MultiSurface}

def get_all_geo_elements() -> Generator[Any, None, None]:
    classes = get_interesting_classes()
    clean_element_names, interesting_element_names, interesting_classes = classes
    for clazz_parent in interesting_classes:
        attrs = list_attributes(clazz_parent)
        for attr in attrs:
            clazz = attr[3].type
            if clazz is not None and hasattr(clazz, '_name'):
                if clazz._name == 'Optional':
                    optional = True
                    clazz_resolved = [x for x in clazz.__args__ if x is not None.__class__][0]
                else:
                    clazz_resolved = clazz

                if clazz_resolved in GEO_CLASSES:
                    yield clazz_parent
                    break

def reprojection(deserialized: object, crs_to: str) -> Any:
    # TODO: This function would walk over the class iteratively.
    # A general optimisation would be to precompute the paths within
    # a class to directly have a list (per class) of possible location targets
    for obj, path in recursive_attributes(deserialized, []):
        if isinstance(obj, LocationStructure2):
            project_location(obj, crs_to)

        elif isinstance(obj, LineString):
            if obj.srs_name == crs_to:
                continue

            transformer = get_transformer_by_srs_name(obj, crs_to)
            if transformer is not None:
                project_linestring2(transformer, obj)
                obj.srs_name = crs_to

        elif isinstance(obj, Polygon):
            project_polygon(obj, crs_to)

        elif isinstance(obj, MultiSurface):
            for member in obj.surface_member:
                project_polygon(member.polygon, crs_to)
            for member in obj.surface_members.polygon:
                project_polygon(member, crs_to)

            obj.srs_name = crs_to

    return deserialized


def reprojection_update(db: Database, crs_to: str, batch_size=100_000, max_mem=4 * 1024 ** 3):
    # Within this function we are reading and writing towards the target database.
    # This effectively means that if we would need to resize for whatever reason,
    # we cannot hold the cursor since access has to be disabled.
    # We will first validate that we do have remaining capacity.
    db.guard_free_space(0.10)

    for clazz in db.tables(exclusively=set(get_all_geo_elements())):
        src_db = db.open_db(clazz)
        if not src_db:
            continue

        src_db = db.open_db(clazz)
        with db.env.begin(db=src_db, buffers=True, write=False) as src_txn:
            cursor = src_txn.cursor()

            for key, value in cursor:
                # transformed_value = db.serializer.marshall(reprojection(db.serializer.unmarshall(value, clazz), crs_to), clazz)
                # TODO: We may not want to expose our internal task queue.
                # db.task_queue.put((LmdbActions.WRITE, src_db, key, transformed_value))
                db.insert_one_object(reprojection(db.serializer.unmarshall(value, clazz), crs_to))

def get_transformer_by_srs_name(location, crs_to) -> Transformer:
    if hasattr(location, 'pos') and location.pos is not None:
        srs_name = location.pos.srs_name or location.srs_name or 'urn:ogc:def:crs:EPSG::4326'
    else:
        srs_name = location.srs_name or 'urn:ogc:def:crs:EPSG::4326'

    if srs_name == crs_to:
        return None

    mapping = f"{srs_name}_{crs_to}"
    transformer = transformers.get(mapping, None)
    if transformer is None:
        try:
            transformer = Transformer.from_crs(srs_name, crs_to) # TODO: Test if we can use accuracy instead of quantitize later
        except CRSError:
            # TODO: Implement logging rule that handles error
            log_once(logging.ERROR, f"Unknown transformation {srs_name} for {crs_to}", f"Unknown transformation {srs_name} for {crs_to}, we now assume WGS84, and hope the target is available")
            transformer = Transformer.from_crs('urn:ogc:def:crs:EPSG::4326', crs_to)
            pass
        except:
            log_once(logging.ERROR, f"Unknown transformation {srs_name} for {crs_to}", f"Unknown transformation {srs_name} for {crs_to}, we now assume WGS84, and hope the target is available")
            transformer = Transformer.from_crs('urn:ogc:def:crs:EPSG::4326', crs_to)
            pass

        transformers[mapping] = transformer
    return transformer

def project_location_4326(location: LocationStructure2, quantize: str='0.000001') -> None:
    crs_to = 'urn:ogc:def:crs:EPSG::4326'
    if location.pos is not None:
        transformer = get_transformer_by_srs_name(location, crs_to)
        if transformer is not None:
            latitude, longitude = transformer.transform(location.pos.value[0], location.pos.value[1])
        else:
            latitude = location.pos.value[0]
            longitude = location.pos.value[1]

        location.longitude = Decimal(longitude).quantize(Decimal(quantize), ROUND_HALF_UP)
        location.latitude = Decimal(latitude).quantize(Decimal(quantize), ROUND_HALF_UP)
        location.srs_name = crs_to
        location.pos = None

    elif location.srs_name not in (None, 'EPSG:4326', 'urn:ogc:def:crs:EPSG::4326'):
        print("TODO: Crazy not WGS84")

def project_location(location: LocationStructure2, crs_to: str, quantize: str='0.000001') -> None:
    if location.srs_name == crs_to:
        return

    if location.pos is not None:
        transformer = get_transformer_by_srs_name(location, crs_to)
        if transformer is not None:
            x, y = transformer.transform(location.pos.value[0], location.pos.value[1])
            x = Decimal(x).quantize(Decimal(quantize), ROUND_HALF_UP)
            y = Decimal(y).quantize(Decimal(quantize), ROUND_HALF_UP)
            location.srs_name = crs_to
            location.pos = Pos(value=[x, y], srs_name=crs_to, srs_dimension=2)

    elif location.longitude is not None and location.latitude is not None:
        transformer = get_transformer_by_srs_name(location, crs_to)
        if transformer is not None:
            x, y = transformer.transform(location.latitude, location.longitude)
            x = Decimal(x).quantize(Decimal(quantize), ROUND_HALF_UP)
            y = Decimal(y).quantize(Decimal(quantize), ROUND_HALF_UP)
            location.srs_name = crs_to
            location.pos = Pos(value=[x, y], srs_name=crs_to, srs_dimension=2)

    else:
        print("TODO: Crazy not WGS84")


def project_linestring2(transformer: Transformer, linestring: LineString) -> None:
    # TODO: Refactor arguments
    srs_dimension = linestring.srs_dimension if hasattr(linestring, 'srs_dimension') and linestring.srs_dimension else 2
    xx = []
    yy = []
    zz = []
    if isinstance(linestring.pos_or_point_property_or_pos_list[0], PosList):
        xx = linestring.pos_or_point_property_or_pos_list[0].value[0::srs_dimension]
        yy = linestring.pos_or_point_property_or_pos_list[0].value[1::srs_dimension]
        if srs_dimension >= 2:
            zz = linestring.pos_or_point_property_or_pos_list[0].value[2::srs_dimension]
    elif isinstance(linestring.pos_or_point_property_or_pos_list[0], Pos):
        xx = [pos.value[0] for pos in linestring.pos_or_point_property_or_pos_list]
        yy = [pos.value[1] for pos in linestring.pos_or_point_property_or_pos_list]
        if srs_dimension >= 2:
            zz = [pos.value[2] for pos in linestring.pos_or_point_property_or_pos_list]

    if srs_dimension == 2:
        pxx, pyy = transformer.transform(xx, yy)
        linestring.pos_or_point_property_or_pos_list = [PosList(value=[Decimal(value).quantize(Decimal('0.000001'), ROUND_HALF_UP) for value in chain(*zip(pxx, pyy))], srs_dimension=srs_dimension)]
    elif srs_dimension == 3:
        pxx, pyy, pzz = transformer.transform(xx, yy, zz)
        linestring.pos_or_point_property_or_pos_list = [PosList(value=[Decimal(value).quantize(Decimal('0.000001'), ROUND_HALF_UP) for value in chain(*zip(pxx, pyy, pzz))], srs_dimension=srs_dimension)]

    # TODO: I would really want to apply the crs_to here

def project_polygon(polygon: Polygon, crs_to: str) -> None:
    if polygon.srs_name == crs_to:
        return

    mapping = f"{polygon.srs_name}_{crs_to}"
    transformer = transformers.get(mapping, Transformer.from_crs(polygon.srs_name, crs_to))
    transformers[mapping] = transformer
    project_linestring2(transformer, polygon.exterior.linear_ring)
    for interior in polygon.interior:
        project_linestring2(transformer, interior)
    if crs_to == 'EPSG:4326':
        polygon.exterior.linear_ring.pos_or_point_property_or_pos_list[0].value = [Decimal(value).quantize(Decimal('0.000001'), ROUND_HALF_UP) for value in polygon.exterior.linear_ring.pos_or_point_property_or_pos_list[0].value]
        for interior in polygon.interior:
            interior.linear_ring.pos_or_point_property_or_pos_list[0].value = [
                Decimal(value).quantize(Decimal('0.000001'), ROUND_HALF_UP) for value in
                interior.linear_ring.pos_or_point_property_or_pos_list[0].value]
    polygon.srs_name = crs_to
