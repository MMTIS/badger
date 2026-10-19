from typing import cast

from domain.netex import ScheduledStopPointRef, FareScheduledStopPoint, NameOfClass
from domain.netex.model import PassengerStopAssignment, StopPlace, StopPlaceRef, QuaysRelStructure, Quay, QuayRef
from storage.mdbx.core.references import resolve, resolve_embeddings_index
from tests.base import MdbxStorageTestCase


class TestResolve(MdbxStorageTestCase):
    def test_name_of_ref_class_is_updated(self) -> None:
        """
        In case of a reference with an invalid, missing, but allowed nameOfRefClass, check if we correctly resolved the situation.
        """

        psa = PassengerStopAssignment(
            id="psa1",
            version="1",
            fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point=ScheduledStopPointRef(ref="fssp1", version="1")
        )

        fssp = FareScheduledStopPoint(id="fssp1", version="1")

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [psa, fssp])
            txn_write.commit()

        resolve(self.storage)

        with self.storage.env.ro_transaction() as txn_read:
            psa_loaded: PassengerStopAssignment
            _key, psa_loaded = self.storage.load_object_by_id_version(txn_read, "psa1", PassengerStopAssignment, "1")
            self.assertEqual(psa_loaded.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point.name_of_ref_class, NameOfClass.FARE_SCHEDULED_STOP_POINT)


    def test_version_is_updated(self) -> None:
        """
        In case of a version which does not exist, check if we correctly resolved the situation.
        """

        psa = PassengerStopAssignment(
            id="psa1",
            version="1",
            taxi_rank_ref_or_stop_place_ref_or_stop_place=StopPlaceRef(ref="sp1", version="1"),
            taxi_stand_ref_or_quay_ref_or_quay=QuayRef(ref="q1", version="1"),
        )

        sp = StopPlace(id="sp1", version="2", quays=QuaysRelStructure(taxi_stand_ref_or_quay_ref_or_quay=[Quay(id="q1", version="2")]))

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [psa, sp])
            txn_write.commit()

        resolve(self.storage)
        resolve_embeddings_index(self.storage)

        with self.storage.env.ro_transaction() as txn_read:
            psa_loaded: PassengerStopAssignment
            _key, psa_loaded = self.storage.load_object_by_id_version(txn_read, "psa1", PassengerStopAssignment, "1")
            self.assertEqual(psa_loaded.taxi_rank_ref_or_stop_place_ref_or_stop_place.version, "2")
            self.assertEqual(psa_loaded.taxi_stand_ref_or_quay_ref_or_quay.version, "2")
