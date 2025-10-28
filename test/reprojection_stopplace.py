from pathlib import Path

from domain.netex.services.recursive_attributes import get_all_geo_elements
from storage.mdbx.core.implementation import MdbxStorage
from transformers.projection import reprojection_update

with MdbxStorage(Path("/tmp/wsf.mdbx"), readonly=False) as db:
    print(list(get_all_geo_elements()))

    with db.env.rw_transaction() as txn:
        db.insert_any_object_on_queue(txn, reprojection_update(db, txn, "urn:ogc:def:crs:EPSG::4326"))
        txn.commit()
