# TODO: It would be interesting to take the objects not being in the EPIP classes, remove the references from the objects that reference them.
from collections import defaultdict
from typing import Any

from netexio.attributes import update_attr
from netexio.database import Database
from netexio.dbaccess import load_referencing_inwards
from netexio.pickleserializer import MyPickleSerializer
from transformers.references import split_path
from utils.profiles import EPIP_CLASSES

with Database("/tmp/unittest.lmdb", serializer=MyPickleSerializer(compression=True), readonly=False) as db_write:
    result: dict[tuple[str, str, Any], list[str]] = defaultdict(list)

    # TODO: For now EPIP
    removable_classes = db_write.tables() - EPIP_CLASSES
    for removable_class in removable_classes:
        for parent_id, parent_version, parent_class, path in load_referencing_inwards(db_write, removable_class):
            parent_klass: type[Any] = db_write.get_class_by_name(parent_class)  # TODO: refactor at load_referencing_*
            if parent_klass in EPIP_CLASSES:
                # Aggregate all parent_ids, so we prevent concurrency issues, and the cost of deserialisation and serialisation
                key = (parent_id, parent_version, parent_klass)
                result[key].append(path)

    # TODO: Once removed the export should have less elements in the GeneralFrame, and only the relevant extra elements
    for key, paths in result.items():
        parent_id, parent_version, parent_klass = key
        obj = db_write.get_single(parent_klass, parent_id, parent_version)
        if obj:
            for path in paths:
                split = split_path(path)
                update_attr(obj, split, None)
            db_write.insert_one_object(obj)
