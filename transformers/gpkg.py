from collections import defaultdict
from dataclasses import fields
from pathlib import Path
from types import NoneType
from typing import Any, cast

from osgeo import osr, ogr
from osgeo.gdal import Dataset

from domain.netex.model import (
    StopPlace,
    Quay,
    EquipmentPlace,
    Level,
    SimplePointVersionStructure,
    MultiSurface,
    LiftEquipment,
    Polygon,
    AccessSpace,
    SiteComponentVersionStructure,
    RampEquipment,
    AccessibilityLimitation,
    EscalatorEquipment,
    TravelatorEquipment,
    SiteFacilitySet,
)
from domain.utils import get_object_name
from storage.lxml.serialization.xmlserializer import MyXmlSerializer
from storage.mdbx.core.implementation import MdbxStorage

mapping = {SimplePointVersionStructure: ogr.wkbPoint, Polygon: ogr.wkbPolygon, MultiSurface: ogr.wkbMultiPolygon, NoneType: ogr.wkbPoint}
mapping_name = {SimplePointVersionStructure: '_POINT', Polygon: '_POLYGON', MultiSurface: '_MULTIPOLYGON', NoneType: '_POINT'}


def force_3d(geom: ogr.Geometry, z_value: int) -> ogr.Geometry:
    """
    Return a copy of the given geometry with a fixed Z coordinate applied.
    Works for Point, LineString, Polygon, MultiPolygon, etc.
    """
    if geom is None:
        return None

    gtype = geom.GetGeometryType()

    # Simple Point
    if gtype == ogr.wkbPoint:
        x, y, *_ = geom.GetPoint()
        g3d = ogr.Geometry(ogr.wkbPoint25D)
        g3d.AddPoint(x, y, z_value)
        return g3d

    # LineString
    elif gtype == ogr.wkbLineString:
        g3d = ogr.Geometry(ogr.wkbLineString25D)
        for i in range(geom.GetPointCount()):
            x, y, *_ = geom.GetPoint(i)
            g3d.AddPoint(x, y, z_value)
        return g3d

    # Polygon
    elif gtype == ogr.wkbPolygon:
        g3d = ogr.Geometry(ogr.wkbPolygon25D)
        for i in range(geom.GetGeometryCount()):
            ring2d = geom.GetGeometryRef(i)
            ring3d = ogr.Geometry(ogr.wkbLinearRing)
            for j in range(ring2d.GetPointCount()):
                x, y, *_ = ring2d.GetPoint(j)
                ring3d.AddPoint(x, y, z_value)
            g3d.AddGeometry(ring3d)
        return g3d

    # Multi-geometries (MultiPoint, MultiLineString, MultiPolygon, GeometryCollection)
    elif geom.GetGeometryCount() > 0:
        g3d = ogr.Geometry(gtype)  # behoud type (bijv. MultiPolygon)
        for i in range(geom.GetGeometryCount()):
            sub_geom = geom.GetGeometryRef(i)
            g3d.AddGeometry(force_3d(sub_geom, z_value))
        return g3d

    # fallback: return clone
    return geom.Clone()


def createLayers(storage: MdbxStorage, ds: Dataset):
    # First pass, see what types are around
    grouped_geo = defaultdict(set)

    for k, stopPlace in storage.iter_objects(StopPlace):
        collected_equipments: dict[str, Any] = {}

        # Collect all the locations equipment could be placed
        collected_equipment_places: dict[str, EquipmentPlace] = {}

        if stopPlace.equipment_places:
            for equipment_place in [
                equipment_place
                for equipment_place in stopPlace.equipment_places.equipment_place_ref_or_equipment_place
                if isinstance(equipment_place, EquipmentPlace)
            ]:
                collected_equipment_places[equipment_place.id] = equipment_place

                if equipment_place.place_equipments:
                    for place_equipment in equipment_place.place_equipments.choice:
                        if hasattr(place_equipment, "ref"):
                            actual_equipment = collected_equipments[place_equipment.ref]
                        else:
                            collected_equipments[place_equipment.id] = place_equipment
                            actual_equipment = place_equipment
                        actual_equipment.extensions = equipment_place

        if stopPlace.place_equipments:
            for place_equipment in stopPlace.place_equipments.choice:
                if hasattr(place_equipment, "ref"):
                    actual_equipment = collected_equipments[place_equipment.ref]
                else:
                    collected_equipments[place_equipment.id] = place_equipment
                    actual_equipment = place_equipment
                actual_equipment.extensions = stopPlace

        if stopPlace.access_spaces:
            for access_space in [
                access_space for access_space in stopPlace.access_spaces.access_space_ref_or_access_space if isinstance(access_space, AccessSpace)
            ]:
                if access_space.equipment_places:
                    for equipment_place in [
                        equipment_place
                        for equipment_place in access_space.equipment_places.equipment_place_ref_or_equipment_place
                        if isinstance(equipment_place, EquipmentPlace)
                    ]:
                        collected_equipment_places[equipment_place.id] = equipment_place

                        if equipment_place.place_equipments:
                            for place_equipment in equipment_place.place_equipments.choice:
                                if hasattr(place_equipment, "ref"):
                                    actual_equipment = collected_equipments[place_equipment.ref]
                                else:
                                    collected_equipments[place_equipment.id] = place_equipment
                                    actual_equipment = place_equipment
                                actual_equipment.extensions = equipment_place

                if access_space.place_equipments:
                    for place_equipment in access_space.place_equipments.choice:
                        if hasattr(place_equipment, "ref"):
                            actual_equipment = collected_equipments[place_equipment.ref]
                        else:
                            collected_equipments[place_equipment.id] = place_equipment
                            actual_equipment = place_equipment
                        actual_equipment.extensions = access_space

                geom_class = access_space.polygon_or_multi_surface.__class__ if access_space.polygon_or_multi_surface else access_space.centroid.__class__
                grouped_geo[access_space.__class__].add(geom_class)

        if stopPlace.quays:
            for quay in [quay for quay in stopPlace.quays.taxi_stand_ref_or_quay_ref_or_quay if isinstance(quay, Quay)]:
                if quay.equipment_places:
                    for equipment_place in [
                        equipment_place
                        for equipment_place in quay.equipment_places.equipment_place_ref_or_equipment_place
                        if isinstance(equipment_place, EquipmentPlace)
                    ]:
                        collected_equipment_places[equipment_place.id] = equipment_place

                        if equipment_place.place_equipments:
                            for place_equipment in equipment_place.place_equipments.choice:
                                if hasattr(place_equipment, "ref"):
                                    actual_equipment = collected_equipments[place_equipment.ref]
                                else:
                                    collected_equipments[place_equipment.id] = place_equipment
                                    actual_equipment = place_equipment
                                actual_equipment.extensions = equipment_place

                if quay.place_equipments:
                    for place_equipment in quay.place_equipments.choice:
                        if hasattr(place_equipment, "ref"):
                            actual_equipment = collected_equipments[place_equipment.ref]
                        else:
                            collected_equipments[place_equipment.id] = place_equipment
                            actual_equipment = place_equipment
                        actual_equipment.extensions = quay

                geom_class = quay.polygon_or_multi_surface.__class__ if quay.polygon_or_multi_surface else quay.centroid.__class__
                grouped_geo[quay.__class__].add(geom_class)

        geom_class = stopPlace.polygon_or_multi_surface.__class__ if stopPlace.polygon_or_multi_surface else stopPlace.centroid.__class__
        grouped_geo[stopPlace.__class__].add(geom_class)

        for e in collected_equipments.values():
            geom_class = e.extensions.polygon_or_multi_surface.__class__ if e.extensions.polygon_or_multi_surface else e.extensions.centroid.__class__
            grouped_geo[e.__class__].add(geom_class)

    # TODO: fetch this from the objects
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(28992)  # RD 2D

    layers: dict[str, ogr.Layer] = {}
    for equipment_class, geometry_classes in grouped_geo.items():
        count = sum(1 for x in geometry_classes if x is not None)
        for geo_class in geometry_classes:
            if geo_class is not NoneType:
                key = get_object_name(equipment_class) + mapping_name[geo_class]
                name = key if count > 1 else get_object_name(equipment_class)
                layers[key] = ds.CreateLayer(name, srs, mapping[geo_class])

                field_map = field_maps[equipment_class]
                for f, (_, ftype) in field_map.items():
                    layers[key].CreateField(ogr.FieldDefn(f, ftype))

        if len(geometry_classes) == 1 and NoneType in geometry_classes:
            key = get_object_name(equipment_class) + mapping_name[NoneType]
            name = key if count > 1 else get_object_name(equipment_class)
            layers[key] = ds.CreateLayer(name, srs, ogr.wkbPoint)

            field_map = field_maps[equipment_class]
            for f, (_, ftype) in field_map.items():
                layers[key].CreateField(ogr.FieldDefn(f, ftype))

    return layers


myxmlserializer = MyXmlSerializer([])


def get_boolean(value: Any):
    if value is None:
        return value
    return 1 if value.name == 'TRUE' else 0 if value.name == 'FALSE' else None


field_maps: dict[Any:str] = {}

stopplace_field_map = {
    "name": ("name.content[0]", ogr.OFTString),
    "private_code": ("private_code.", ogr.OFTString),
    "accessibility_assessment": ("accessibility_assessment.", ogr.OFTInteger),
}

for f in fields(AccessibilityLimitation):
    if f.name in ("WheelchairAccess", "StepFreeAccess", "LevelAccessIntoVehicle", "VisualSignsAvailable", "TactileGuidanceAvailable"):
        stopplace_field_map[f.name] = ("accessibility_assessment.limitations.accessibility_limitation." + f.name, ogr.OFTInteger)

for f in fields(SiteFacilitySet):
    if f.name in ("MobilityFacilityList", "SanitaryFacilityList", "TicketingFacilityList", "AccessFacilityList"):
        stopplace_field_map[f.name] = ("facilities.site_facility_set_ref_or_site_facility_set[0]" + f.name, ogr.OFTString)

field_maps[StopPlace] = stopplace_field_map

quay_field_map = {
    "name": ("name.content[0]", ogr.OFTString),
    "public_code": ("public_code.value", ogr.OFTString),
    "quay_type": ("quay_type", ogr.OFTString),
    "parent_quay_ref": ("parent_quay_ref", ogr.OFTString),
    "accessibility_assessment": ("accessibility_assessment.mobility_impaired_access", ogr.OFTInteger),
    "level": ("level", ogr.OFTInteger),
}

field_maps[Quay] = quay_field_map

accesspace_field_map = {
    "private_code": ("private_code.value", ogr.OFTString),
    "access_space_type": ("access_space_type", ogr.OFTString),
    "passage_type": ("passage_type", ogr.OFTString),
    "level": ("level", ogr.OFTInteger),
}

field_maps[AccessSpace] = accesspace_field_map

liftequipment_field_map = {
    "private_code": ("private_code.value", ogr.OFTString),
    "public_code": ("public_code.value", ogr.OFTString),
    "description": ("description", ogr.OFTString),
    "width": ("width", ogr.OFTReal),
    "depth": ("depth", ogr.OFTReal),
    "maximumload": ("maximumload", ogr.OFTInteger),
    "braille_buttons": ("braille_buttons", ogr.OFTInteger),
    "through_loader": ("through_loader", ogr.OFTInteger),
    "tactile_actuators": ("tactile_actuators", ogr.OFTInteger),
    "accoustic_announcements": ("accoustic_announcements", ogr.OFTInteger),
}

field_maps[LiftEquipment] = liftequipment_field_map

escalatorequipment_field_map = {
    "private_code": ("private_code.value", ogr.OFTString),
    "public_code": ("public_code.value", ogr.OFTString),
    "description": ("description", ogr.OFTString),
}

field_maps[EscalatorEquipment] = escalatorequipment_field_map

rampequipment_field_map = {
    "private_code": ("private_code.value", ogr.OFTString),
    "public_code": ("public_code.value", ogr.OFTString),
    "description": ("description", ogr.OFTString),
}

field_maps[RampEquipment] = rampequipment_field_map

travelatorequipment_field_map = {
    "private_code": ("private_code.value", ogr.OFTString),
    "public_code": ("public_code.value", ogr.OFTString),
    "description": ("description", ogr.OFTString),
}

field_maps[TravelatorEquipment] = travelatorequipment_field_map


def add_feature(obj: Any, layers: dict[str, ogr.Layer]):
    global mapping_name

    if isinstance(obj, StopPlace):
        sp: StopPlace = cast(StopPlace, obj)
        geom_class = obj.polygon_or_multi_surface.__class__ if obj.polygon_or_multi_surface else obj.centroid.__class__
        key = get_object_name(obj.__class__) + mapping_name[geom_class]
        layer = layers[key]
        feature = ogr.Feature(layer.GetLayerDefn())

        feature.SetField("name", sp.name.content[0])
        feature.SetField("private_code", sp.private_code.value)
        feature.SetField("accessibility_assessment", get_boolean(sp.accessibility_assessment.mobility_impaired_access))

        if sp.accessibility_assessment.limitations:
            for f in fields(sp.accessibility_assessment.limitations.accessibility_limitation):
                name = f.name
                if name in ("WheelchairAccess", "StepFreeAccess", "LevelAccessIntoVehicle", "VisualSignsAvailable", "TactileGuidanceAvailable"):
                    value = getattr(sp.accessibility_assessment.limitations.accessibility_limitation, name)
                    feature.SetField(name, get_boolean(value))

        if sp.facilities and len(sp.facilities.site_facility_set_ref_or_site_facility_set) > 0:
            for f in fields(sp.facilities.site_facility_set_ref_or_site_facility_set[0]):
                name = f.name
                if name in ("MobilityFacilityList", "SanitaryFacilityList", "TicketingFacilityList", "AccessFacilityList"):
                    value = getattr(sp.facilities.site_facility_set_ref_or_site_facility_set[0], name)
                    feature.SetField(name, value)

        if sp.polygon_or_multi_surface:
            feature.SetGeometry(ogr.CreateGeometryFromGML(myxmlserializer.marshall(sp.polygon_or_multi_surface, None)))

        elif sp.centroid:
            feature.SetGeometry(ogr.CreateGeometryFromGML("<gml:Point>" + myxmlserializer.marshall(sp.centroid.location.pos, None) + "</gml:Point>"))

    elif isinstance(obj, Quay):
        q: Quay = cast(Quay, obj)
        level: int = cast(int, q.extensions)
        geom_class = obj.polygon_or_multi_surface.__class__ if obj.polygon_or_multi_surface else obj.centroid.__class__
        key = get_object_name(obj.__class__) + mapping_name[geom_class]
        layer = layers[key]
        feature = ogr.Feature(layer.GetLayerDefn())

        feature.SetField("name", q.name.content[0])
        feature.SetField("public_code", q.public_code.value if q.public_code else None)
        feature.SetField("quay_type", q.quay_type.name)
        feature.SetField("parent_quay_ref", q.parent_quay_ref.ref if q.parent_quay_ref else None)
        feature.SetField("accessibility_assessment", get_boolean(q.accessibility_assessment.mobility_impaired_access) if q.accessibility_assessment else None)
        feature.SetField("level", level)

        if q.accessibility_assessment and q.accessibility_assessment.limitations:
            for f in fields(q.accessibility_assessment.limitations.accessibility_limitation):
                name = f.name
                if f.name in ("WheelchairAccess", "StepFreeAccess", "LevelAccessIntoVehicle", "VisualSignsAvailable", "TactileGuidanceAvailable"):
                    value = getattr(q.accessibility_assessment.limitations.accessibility_limitation, name)
                    feature.SetField(name, get_boolean(value))

        if level is None:
            if q.polygon_or_multi_surface:
                feature.SetGeometry(ogr.CreateGeometryFromGML(myxmlserializer.marshall(q.polygon_or_multi_surface, None)))

            elif q.centroid:
                feature.SetGeometry(ogr.CreateGeometryFromGML("<gml:Point>" + myxmlserializer.marshall(q.centroid.location.pos, None) + "</gml:Point>"))

        else:
            if q.polygon_or_multi_surface:
                geom = ogr.CreateGeometryFromGML(myxmlserializer.marshall(q.polygon_or_multi_surface, None))
                feature.SetGeometry(force_3d(geom, level))

            elif q.centroid:
                geom = ogr.CreateGeometryFromGML("<gml:Point>" + myxmlserializer.marshall(q.centroid.location.pos, None) + "</gml:Point>")
                feature.SetGeometry(force_3d(geom, level))

    elif isinstance(obj, AccessSpace):
        access_space: AccessSpace = cast(AccessSpace, obj)
        level: int = cast(int, access_space.extensions)
        geom_class = obj.polygon_or_multi_surface.__class__ if obj.polygon_or_multi_surface else obj.centroid.__class__
        key = get_object_name(obj.__class__) + mapping_name[geom_class]
        layer = layers[key]
        feature = ogr.Feature(layer.GetLayerDefn())

        feature.SetField("private_code", access_space.private_code.value if obj.private_code else None)
        feature.SetField("access_space_type", access_space.access_space_type.name)
        feature.SetField("passage_type", access_space.passage_type.name)
        feature.SetField("level", level)

        if level is None:
            if access_space.polygon_or_multi_surface:
                feature.SetGeometry(ogr.CreateGeometryFromGML(myxmlserializer.marshall(access_space.polygon_or_multi_surface, None)))

            elif access_space.centroid:
                feature.SetGeometry(
                    ogr.CreateGeometryFromGML("<gml:Point>" + myxmlserializer.marshall(access_space.centroid.location.pos, None) + "</gml:Point>")
                )

        else:
            if access_space.polygon_or_multi_surface:
                geom = ogr.CreateGeometryFromGML(myxmlserializer.marshall(access_space.polygon_or_multi_surface, None))
                feature.SetGeometry(force_3d(geom, level))

            elif access_space.centroid:
                geom = ogr.CreateGeometryFromGML("<gml:Point>" + myxmlserializer.marshall(access_space.centroid.location.pos, None) + "</gml:Point>")
                feature.SetGeometry(force_3d(geom, level))

    else:
        parent, level = cast(tuple[SiteComponentVersionStructure, int], obj.extensions) if obj.extensions else (None, None)
        geom_class = parent.polygon_or_multi_surface.__class__ if parent.polygon_or_multi_surface else parent.centroid.__class__
        key = get_object_name(obj.__class__) + mapping_name[geom_class]
        layer = layers[key]
        feature = ogr.Feature(layer.GetLayerDefn())

        if isinstance(obj, LiftEquipment):
            feature.SetField("private_code", obj.private_code.value if obj.private_code else None)
            feature.SetField("public_code", obj.public_code.value if obj.public_code else None)
            feature.SetField("description", obj.description.content[0] if obj.description else None)
            feature.SetField("width", float(obj.width) if obj.width else None)
            feature.SetField("depth", float(obj.depth) if obj.depth else None)
            feature.SetField("maximumload", float(obj.maximum_load) if obj.maximum_load else None)
            feature.SetField("braille_buttons", 1 if obj.braille_buttons is True else 0 if obj.braille_buttons is False else None)
            feature.SetField("through_loader", 1 if obj.through_loader is True else 0 if obj.through_loader is False else None)
            feature.SetField("tactile_actuators", 1 if obj.tactile_actuators is True else 0 if obj.tactile_actuators is False else None)
            feature.SetField("accoustic_announcements", 1 if obj.accoustic_announcements is True else 0 if obj.accoustic_announcements is False else None)

        elif isinstance(obj, EscalatorEquipment) or isinstance(obj, RampEquipment) or isinstance(obj, TravelatorEquipment):
            feature.SetField("private_code", obj.private_code.value if obj.private_code else None)
            feature.SetField("public_code", obj.public_code.value if obj.public_code else None)
            feature.SetField("description", obj.description.content[0] if obj.description else None)

        if parent is None:
            pass
        if level is None:
            if parent.polygon_or_multi_surface:
                feature.SetGeometry(ogr.CreateGeometryFromGML(myxmlserializer.marshall(parent.polygon_or_multi_surface, None)))

            elif parent.centroid:
                feature.SetGeometry(ogr.CreateGeometryFromGML("<gml:Point>" + myxmlserializer.marshall(parent.centroid.location.pos, None) + "</gml:Point>"))

        else:
            if parent.polygon_or_multi_surface:
                geom = ogr.CreateGeometryFromGML(myxmlserializer.marshall(parent.polygon_or_multi_surface, None))
                feature.SetGeometry(force_3d(geom, level))

            elif parent.centroid:
                geom = ogr.CreateGeometryFromGML("<gml:Point>" + myxmlserializer.marshall(parent.centroid.location.pos, None) + "</gml:Point>")
                feature.SetGeometry(force_3d(geom, level))

    layer.CreateFeature(feature)


def stopPlaceToLayers(stopPlace: StopPlace, layers: dict[str, ogr.Layer]):
    levels: dict[str, Level] = (
        {level.id: level for level in [level for level in stopPlace.levels.level_ref_or_level if isinstance(level, Level)]} if stopPlace.levels else {}
    )

    collected_equipments: dict[str, Any] = {}

    # Collect all the locations equipment could be placed
    collected_equipment_places: dict[str, EquipmentPlace] = {}

    if stopPlace.equipment_places:
        for equipment_place in [
            equipment_place
            for equipment_place in stopPlace.equipment_places.equipment_place_ref_or_equipment_place
            if isinstance(equipment_place, EquipmentPlace)
        ]:
            collected_equipment_places[equipment_place.id] = equipment_place

            if equipment_place.place_equipments:
                for place_equipment in equipment_place.place_equipments.choice:
                    if hasattr(place_equipment, "ref"):
                        actual_equipment = collected_equipments[place_equipment.ref]
                    else:
                        collected_equipments[place_equipment.id] = place_equipment
                        actual_equipment = place_equipment
                    actual_equipment.extensions = (equipment_place, None)

    if stopPlace.place_equipments:
        for place_equipment in stopPlace.place_equipments.choice:
            if hasattr(place_equipment, "ref"):
                actual_equipment = collected_equipments[place_equipment.ref]
            else:
                collected_equipments[place_equipment.id] = place_equipment
                actual_equipment = place_equipment
            actual_equipment.extensions = (stopPlace, None)

    if stopPlace.access_spaces:
        for access_space in [
            access_space for access_space in stopPlace.access_spaces.access_space_ref_or_access_space if isinstance(access_space, AccessSpace)
        ]:
            level = levels[access_space.level_ref.ref].relative_level_order if access_space.level_ref else None
            if access_space.equipment_places:
                for equipment_place in [
                    equipment_place
                    for equipment_place in access_space.equipment_places.equipment_place_ref_or_equipment_place
                    if isinstance(equipment_place, EquipmentPlace)
                ]:
                    collected_equipment_places[equipment_place.id] = equipment_place

                    if equipment_place.place_equipments:
                        for place_equipment in equipment_place.place_equipments.choice:
                            if hasattr(place_equipment, "ref"):
                                actual_equipment = collected_equipments[place_equipment.ref]
                            else:
                                collected_equipments[place_equipment.id] = place_equipment
                                actual_equipment = place_equipment
                            actual_equipment.extensions = (equipment_place, level)

            if access_space.place_equipments:
                for place_equipment in access_space.place_equipments.choice:
                    if hasattr(place_equipment, "ref"):
                        actual_equipment = collected_equipments[place_equipment.ref]
                    else:
                        collected_equipments[place_equipment.id] = place_equipment
                        actual_equipment = place_equipment
                    actual_equipment.extensions = (access_space, level)

            # geom_class = access_space.polygon_or_multi_surface.__class__ if access_space.polygon_or_multi_surface else access_space.centroid.__class__
            # grouped_geo[access_space.__class__].add(geom_class)
            access_space.extensions = level
            add_feature(access_space, layers)

    if stopPlace.quays:
        for quay in [quay for quay in stopPlace.quays.taxi_stand_ref_or_quay_ref_or_quay if isinstance(quay, Quay)]:
            level = levels[quay.level_ref.ref].relative_level_order if quay.level_ref else None
            if quay.equipment_places:
                for equipment_place in [
                    equipment_place
                    for equipment_place in quay.equipment_places.equipment_place_ref_or_equipment_place
                    if isinstance(equipment_place, EquipmentPlace)
                ]:
                    collected_equipment_places[equipment_place.id] = equipment_place

                    if equipment_place.place_equipments:
                        for place_equipment in equipment_place.place_equipments.choice:
                            if hasattr(place_equipment, "ref"):
                                actual_equipment = collected_equipments[place_equipment.ref]
                            else:
                                collected_equipments[place_equipment.id] = place_equipment
                                actual_equipment = place_equipment
                            actual_equipment.extensions = (equipment_place, level)

            if quay.place_equipments:
                for place_equipment in quay.place_equipments.choice:
                    if hasattr(place_equipment, "ref"):
                        actual_equipment = collected_equipments[place_equipment.ref]
                    else:
                        collected_equipments[place_equipment.id] = place_equipment
                        actual_equipment = place_equipment
                    actual_equipment.extensions = (quay, level)

            # geom_class = quay.polygon_or_multi_surface.__class__ if quay.polygon_or_multi_surface else quay.centroid.__class__
            # grouped_geo[quay.__class__].add(geom_class)
            quay.extensions = level
            add_feature(quay, layers)

    # geom_class = stopPlace.polygon_or_multi_surface.__class__ if stopPlace.polygon_or_multi_surface else stopPlace.centroid.__class__
    # grouped_geo[stopPlace.__class__].add(geom_class)
    add_feature(stopPlace, layers)

    for e in collected_equipments.values():
        add_feature(e, layers)


if __name__ == "__main__":
    driver = ogr.GetDriverByName("GPKG")
    ds = driver.CreateDataSource("/tmp/nl-epiap.gpkg")

    with MdbxStorage(Path("/tmp/epiap.mdbx"), readonly=True) as storage:
        layers = createLayers(storage, ds)

        for k, stopPlace in storage.iter_objects(StopPlace):
            stopPlaceToLayers(stopPlace, layers)
