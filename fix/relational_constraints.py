import logging

import lmdb
from tqdm import tqdm

from netexio.binaryserializer import only_references
from netexio.database import Database, Tid
from netexio.pickleserializer import MyPickleSerializer
from utils.aux_logging import prepare_logger, log_all
from utils.utils import get_object_name


def main(source_database_file: str):
    # This function tries to resolve invalid and missing references, typically due to a missing nameOfRefClass.
    # We can make our own life much easier, if we would resolve invalid versions as well.

    # missing_references: dict[str, set[tuple[str, str, str, str]]] = {}
    # all_ids: dict[str, type[Tid]] = {}

    with (Database(source_database_file, MyPickleSerializer(compression=True), readonly=False) as source_db):
        for database_name, clazz in source_db.list_databases():
            with (source_db.env.begin(buffers=False, write=False) as txn_read,
                source_db.env.begin(buffers=False, write=True) as txn_write):
                db_idx = source_db.env.open_db(b"_id_idx", txn=txn_read)
                db_referencing: lmdb._Database = source_db.env.open_db(b"_referencing", create=True, dupsort=True, txn=txn_write)
                db_referencing_inwards: lmdb._Database = source_db.env.open_db(b"_referencing_inwards", create=True, dupsort=True, txn=txn_write)

                txn_write.drop(db=db_referencing)
                txn_write.drop(db=db_referencing_inwards)

                db_referencing: lmdb._Database = source_db.env.open_db(b"_referencing", create=True, dupsort=True, txn=txn_write)
                db_referencing_inwards: lmdb._Database = source_db.env.open_db(b"_referencing_inwards", create=True, dupsort=True, txn=txn_write)

                db = source_db.env.open_db(get_object_name(clazz).encode('utf-8'), txn=txn_read)
                if not db:
                    continue

                stat = txn_read.stat(db)
                entries = stat["entries"]


                with (
                    txn_read.cursor(db=db) as cursor,
                    txn_read.cursor(db=db_idx) as cursor_idx,
                    tqdm(
                        total=entries,
                        desc=database_name,
                        bar_format="{desc:<25} {bar} {n_fmt:>6}/{total_fmt:<6} [{elapsed}<{remaining}]",
                        unit="entry",
                    ) as pbar,
                ):
                    for idx, value in cursor:
                        obj = source_db.serializer.unmarshall(value, clazz)
                        for clazz, obj_id, obj_version, in only_references(source_db.serializer, obj):
                            key = source_db.serializer.encode_key(obj_id, obj_version, clazz, include_clazz=True)

                            value = cursor_idx.get(key)

                            if not value:
                                key_prefix = source_db.serializer.encode_key(obj_id, obj_version, clazz, False)
                                if cursor_idx.set_range(key_prefix):
                                    while bytes(cursor_idx.key()).startswith(key_prefix):
                                        value = cursor_idx.value()
                                        break

                                if value:
                                    pass
                                    # clazz source_db.serializer.name_object[idx_to_class_name(cursor.key().split(ord("-"))[-1])]
                                    # update the xml here too

                            if value:
                                # pass
                                txn_write.put(idx, value, db=db_referencing)
                                txn_write.put(value, idx, db=db_referencing_inwards)

                        pbar.update(1)
                        """
                        # id = source_db.serializer.encode_key(obj.id, obj.version if hasattr(obj, 'version') else None, clazz)
                        # buffer.append(id)
                        # txn_write.put(id, obj_idx.to_bytes(4, 'little'))
                        # obj_idx += 1
                        #

                # Deze halen we direct uit db_idx
                # with source_db.env.begin(db=db_name, buffers=False, write=False) as src2_txn:
                #    cursor = src2_txn.cursor()
                #    for key, value in cursor:
                #        obj = source_db.serializer.unmarshall(value, clazz)
                #        if obj.id in all_ids:
                #            # We cannot be sure it is unique
                #            if clazz != all_ids[obj.id]:
                ##                all_ids[obj.id] = None
                 #       else:
                 #           all_ids[obj.id] = netex.__all__.index(clazz.__name__)

            # Deze halen we direct uit db_embedding
            # with source_db.env.begin(db=source_db.db_embedding, write=False) as txn:
            #    with txn.cursor() as cursor:
            #        for key, value in cursor:
            #            clazz, ref, version, path = cloudpickle.loads(value)
            #            clazz = source_db.serializer.name_object[clazz]
            #            if ref in all_ids:
            #                if clazz != all_ids[ref]:
            #                    all_ids[ref] = None
            #            else:
            #                all_ids[ref] = clazz

    
    with (Database(source_database_file, MyPickleSerializer(compression=True), readonly=False) as source_db):
        # Group by key
        for key, needles in missing_references.items():
            changed = False
            # Nu willen we hier dus de verwijzer naar dit object ophalen
            # De key komt overeen met clazz-objectid, maar dat is nog niet de database naam.
            decoded = source_db.serializer.decode_key(key)
            if decoded is None:
                continue

            parent_clazz, parent_key = decoded
            parent_obj = source_db.get_single_by_key(parent_clazz, parent_key)

            for needle in needles:
                object_clazz, object_id, object_version, embedding_path = needle
                # ref_key = source_db.serializer.encode_key(object_id, object_version, source_db.serializer.name_object[object_clazz], include_clazz=True)

                if object_id in all_ids:
                    result = all_ids[object_id]
                    if result is None:
                        print(f"{object_id} is ambigious.")
                        continue

                    # Check if check_referencing, might help
                    split = split_path(embedding_path)
                    attribute = resolve_attr(parent_obj, split)
                    attribute.name_of_ref_class = get_object_name(result)
                    # attribute.version = parent_obj.version
                    changed = True

            if changed:
                source_db.insert_one_object(parent_obj, False, False)

    with (Database(source_database_file, MyPickleSerializer(compression=True), readonly=False) as source_db):
        source_db.redo_all_embedding_and_references()
    """

if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Check an LMDB for missing references")
    parser.add_argument("source", type=str, help="lmdb file to use as input.")
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.source)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e