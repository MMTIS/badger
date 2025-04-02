from cloudpickle import cloudpickle

from netexio.database import Database


def print_embedding_inverse(db: Database) -> None:
    print("embedding_inverse")
    with db.env.begin(db=db.db_embedding_inverse, write=False) as txn:
        with txn.cursor() as cursor:
            for key, value in cursor:
                parent_clazz, parent_id, parent_version, embedding_path = cloudpickle.loads(value)
                print(key, parent_clazz, parent_id, parent_version, embedding_path)


def print_embedding(db: Database) -> None:
    print("embedding")
    with db.env.begin(db=db.db_embedding, write=False) as txn:
        with txn.cursor() as cursor:
            for key, value in cursor:
                clazz, ref, version, path = cloudpickle.loads(value)
                print("embedding", key, clazz, ref, version, path)


def print_referencing_inwards(db: Database) -> None:
    print("referencing_inwards")
    with db.env.begin(db=db.db_referencing_inwards, write=False) as txn:
        with txn.cursor() as cursor:
            for key, value in cursor:
                parent_class, parent_id, parent_version, path = cloudpickle.loads(value)
                print("referencing_inwards", key, parent_class, parent_id, parent_version, path)


def print_referencing(db: Database) -> None:
    print("referencing")
    with db.env.begin(db=db.db_referencing, write=False) as txn:
        with txn.cursor() as cursor:
            for key, value in cursor:
                referencing_class, referencing_id, referencing_version, path = cloudpickle.loads(value)
                print("referencing", key, referencing_class, referencing_id, referencing_version, path)
