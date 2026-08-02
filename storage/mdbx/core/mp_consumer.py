import queue
from mdbx.mdbx import Env
from storage.mdbx.core.const import DB_UNRESOLVED, DB_UNRESOLVED_FLAGS, DB_ID_IDX, DB_ID_IDX_FLAGS, DB_REFERENCE_OUTWARD, DB_REFERENCE_OUTWARD_FLAGS
from storage.keycodec.relation import RelationKeyCodec


# Placing the consumer in a separate file is only important when mp.Process would spawn, not when it work fork
def consumer(queue: queue.Queue, path: str, max_dbs: int) -> None:
    env = Env(
        path,
        maxdbs=max_dbs,
    )

    while True:
        i: int = 0
        with env.rw_transaction() as txn:
            db_unresolved = txn.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            db_id_idx = txn.open_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
            db_reference_outward = txn.open_map(name=DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)

            # Optimisation: only once open a database (table)
            dbis = {}

            while True:
                try:
                    item = queue.get(timeout=1)  # probeer een nieuw item
                except Exception:
                    # timeout → commit transaction
                    txn.commit()
                    break

                if item is True:
                    # explicitly asked for a commit
                    txn.commit()
                    break

                elif item is None:
                    # queue received terminal, commit and stop
                    txn.commit()
                    dbis = {}
                    return

                if isinstance(item, list):
                    batch_items = item
                else:
                    batch_items = [item]

                for single_item in batch_items:
                    my_id, value, this_clazz_idx, unresolved = single_item

                    # First: check if the id already exists, then we must overwrite.
                    full_key = db_id_idx.get(txn, my_id)
                    if full_key is not None:
                        idx = full_key[:4]
                        try:
                            db_reference_outward.delete(txn, full_key)
                        except:  # noqa: E722
                            pass
                    else:
                        idx = db_id_idx.get_sequence(txn, 1).to_bytes(4, 'little')
                        full_key = RelationKeyCodec.get_fullkey_by_clazz_idx(idx, this_clazz_idx)

                    for unresolved_value in unresolved:
                        resolved_idx = db_id_idx.get(txn, unresolved_value)
                        if resolved_idx:
                            db_reference_outward.put(txn, full_key, resolved_idx)
                        else:
                            db_unresolved.put(txn, full_key, unresolved_value)

                    dbi = dbis.get(this_clazz_idx)
                    if dbi is None:
                        dbi = dbis[this_clazz_idx] = txn.create_map(this_clazz_idx)

                    dbi.put(txn, idx, value)
                    db_id_idx.put(txn, my_id, full_key)
                    i += 1

                if i > 5000:
                    txn.commit()
                    break
