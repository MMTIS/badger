from netexio.database import Database
from netexio.pickleserializer import MyPickleSerializer
from transformers.projection import reprojection_update, get_all_geo_elements

with Database("/tmp/unittest.lmdb", serializer=MyPickleSerializer(compression=True), readonly=False) as db_write:
    # stop_places = load_local(db_write, StopPlace, embedding=False)
    # reprojection(stop_places[0], "urn:ogc:def:crs:EPSG::4326")
    print(list(get_all_geo_elements()))
    reprojection_update(db_write, "urn:ogc:def:crs:EPSG::4326")
