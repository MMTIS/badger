from domain.netex.model import PassengerStopAssignment, StopPlace, StopPlaceRef, QuaysRelStructure, Quay, QuayRef
from storage.mdbx.core.implementation import DB_UNRESOLVED, DB_UNRESOLVED_FLAGS
from storage.mdbx.core.references import resolve, resolve_embeddings_index
from tests.base import MdbxStorageTestCase


class TestDoubleReferences(MdbxStorageTestCase):
    def test_object_can_make_double_references(self) -> None:
        """
        Issue #141 described a problem where two unresolved references from the same object failed to resolve
        """

        psa = PassengerStopAssignment(
            id="psa1",
            version="1",
            taxi_rank_ref_or_stop_place_ref_or_stop_place=StopPlaceRef(ref="sp1", version="1"),
            taxi_stand_ref_or_quay_ref_or_quay=QuayRef(ref="q1", version="1"),
        )

        sp = StopPlace(id="sp1", version="1", quays=QuaysRelStructure(taxi_stand_ref_or_quay_ref_or_quay=[Quay(id="q1", version="1")]))

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [psa, sp])
            txn_write.commit()

        with self.storage.env.ro_transaction() as txn_read:
            db_unresolved = txn_read.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            self.assertEqual(db_unresolved.get_stat(txn_read).ms_entries, 2)

        resolve(self.storage)

        with self.storage.env.ro_transaction() as txn_read:
            db_unresolved = txn_read.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            self.assertEqual(db_unresolved.get_stat(txn_read).ms_entries, 1)

        resolve_embeddings_index(self.storage)

        with self.storage.env.ro_transaction() as txn_read:
            db_unresolved = txn_read.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            self.assertEqual(db_unresolved.get_stat(txn_read).ms_entries, 0)
