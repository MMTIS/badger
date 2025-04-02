# https://github.com/skinkie/reference/issues/271
from typing import Any

from cloudpickle import cloudpickle

from netexio.database import Database
from netexio.pickleserializer import MyPickleSerializer
from utils.debug import print_embedding, print_embedding_inverse, print_referencing, print_referencing_inwards

with Database("/tmp/wsf-epip-line-wsf.lmdb", MyPickleSerializer(compression=True), readonly=True) as db_read:
    print_embedding(db_read)
    print_embedding_inverse(db_read)
    print_referencing(db_read)
    print_referencing_inwards(db_read)

    # This would be a very unusual way, because it would also require to update the base object.
    with db_read.env.begin(write=False) as txn:
        with txn.cursor(db_read.db_embedding_inverse) as cursor_embedding_inverse, txn.cursor(db_read.db_embedding) as cursor_embedding:
            inv_key = b'TIMETABLEDPASSINGTIME-WSF*#*V*B*21*48*00*2-20250328'
            if cursor_embedding_inverse.set_key(inv_key):
                for inv_value in cursor_embedding_inverse.iternext_dup():
                    parent_clazz, parent_id, parent_version, embedding_path = cloudpickle.loads(inv_value)
                    parent_class: type[Any] = db_read.get_class_by_name(parent_clazz)
                    print("inv", inv_key, parent_clazz, parent_id, parent_version, embedding_path)

                    key = db_read.serializer.encode_key(parent_id, parent_version, parent_class, True)
                    if cursor_embedding.set_range(key):
                        for value in cursor_embedding.iternext_dup():
                            clazz, ref, version, path = cloudpickle.loads(value)
                            check_class: type[Any] = db_read.get_class_by_name(clazz)
                            check_inv_key = db_read.serializer.encode_key(ref, version, check_class, True)
                            if check_inv_key == inv_key:
                                print("resolve", key, clazz, ref, version, path)

    # This would be the typical way
    with db_read.env.begin(write=False) as txn:
        with txn.cursor(db_read.db_embedding_inverse) as cursor_embedding_inverse, txn.cursor(db_read.db_embedding) as cursor_embedding:
            key = b'SERVICEJOURNEY-WSF*#*V*B*21*48*00-20250328'
            if cursor_embedding.set_key(key):
                for value in cursor_embedding.iternext_dup():
                    clazz, ref, version, path = cloudpickle.loads(value)
                    check_class = db_read.get_class_by_name(clazz)
                    print("resolve", key, clazz, ref, version, path)

                    inv_key = db_read.serializer.encode_key(ref, version, check_class, True)
                    if cursor_embedding_inverse.set_range(inv_key):
                        for inv_value in cursor_embedding_inverse.iternext_dup():
                            parent_clazz, parent_id, parent_version, embedding_path = cloudpickle.loads(inv_value)
                            parent_class = db_read.get_class_by_name(parent_clazz)

                            check_key = db_read.serializer.encode_key(parent_id, parent_version, parent_class, True)
                            if check_key == key:
                                print("inv", inv_key, parent_clazz, parent_id, parent_version, embedding_path)
                                # cursor_embedding_inverse.delete()

                    # cursor_embedding.delete()

    with db_read.env.begin(write=False) as txn:
        with txn.cursor(db_read.db_referencing_inwards) as cursor_referencing_inwards, txn.cursor(db_read.db_referencing) as cursor_referencing:
            key = b'SCHEDULEDSTOPPOINT-WSF*#*B-20250328'
            if cursor_referencing.set_key(key):
                for value in cursor_referencing.iternext_dup():
                    clazz, ref, version, path = cloudpickle.loads(value)
                    check_class = db_read.get_class_by_name(clazz)
                    print("resolve", key, clazz, ref, version, path)

                    inv_key = db_read.serializer.encode_key(ref, version, check_class, True)
                    if cursor_referencing_inwards.set_range(inv_key):
                        for inv_value in cursor_referencing_inwards.iternext_dup():
                            parent_clazz, parent_id, parent_version, embedding_path = cloudpickle.loads(inv_value)
                            parent_class = db_read.get_class_by_name(parent_clazz)

                            check_key = db_read.serializer.encode_key(parent_id, parent_version, parent_class, True)
                            if check_key == key:
                                print("inv", inv_key, parent_clazz, parent_id, parent_version, embedding_path)
                                # cursor_referencing_inwards.delete()

                    # cursor_embedding.delete()
