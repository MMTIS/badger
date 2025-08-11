import logging

import cloudpickle

from netexio.attributes import resolve_attr
from netexio.database import Database, Tid
from netexio.pickleserializer import MyPickleSerializer
from transformers.references import split_path
from utils.aux_logging import prepare_logger, log_all
from utils.utils import get_object_name


def main(source_database_file: str):
    # This function tries to resolve invalid and missing references, typically due to a missing nameOfRefClass.
    # We can make our own life much easier, if we would resolve invalid versions as well.

    missing_references: dict[str, set[tuple[str, str, str, str]]] = {}
    all_ids: dict[str, type[Tid]] = {}

    with (Database(source_database_file, MyPickleSerializer(compression=True), readonly=True) as source_db):
        with source_db.env.begin(buffers=False, write=False) as txn:
            cursor = txn.cursor(source_db.db_referencing)
            # all_referenced_elements = set([])
            for key, value in cursor:
                needle = cloudpickle.loads(value)
                object_clazz, object_id, object_version, embedding_path = needle
                # if needle in all_referenced_elements:
                #    continue

                # all_referenced_elements.add(needle)
                object_clazz = source_db.get_class_by_name(object_clazz)
                lookup_key = source_db.serializer.encode_key(object_id, object_version, object_clazz)
                if not source_db.check_object_by_key(object_clazz, lookup_key):
                    if key not in missing_references:
                        missing_references[key] = set()
                    missing_references[key].add(needle)

            if len(missing_references) == 0:
                print("No missing references found")
                return

            cursor.close()

            for _database_name, clazz in source_db.list_databases():
                db_name = source_db.open_database(clazz, readonly=True)
                if not db_name:
                    continue

                with source_db.env.begin(db=db_name, buffers=False, write=False) as src2_txn:
                    cursor = src2_txn.cursor()
                    for key, value in cursor:
                        obj = source_db.serializer.unmarshall(value, clazz)
                        if obj.id in all_ids:
                            # We cannot be sure it is unique
                            all_ids[obj.id] = None
                        else:
                            all_ids[obj.id] = clazz

            with source_db.env.begin(db=source_db.db_embedding, write=False) as txn:
                with txn.cursor() as cursor:
                    for key, value in cursor:
                        clazz, ref, version, path = cloudpickle.loads(value)
                        if ref in all_ids:
                            all_ids[ref] = None
                        else:
                            all_ids[ref] = source_db.serializer.name_object[clazz]

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
                # print(parent_obj.id, needles)
                source_db.insert_one_object(parent_obj, False)

    with (Database(source_database_file, MyPickleSerializer(compression=True), readonly=False) as source_db):
        # source_db._resize_env(2*1024**3)
        source_db.redo_all_embedding_and_references()


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