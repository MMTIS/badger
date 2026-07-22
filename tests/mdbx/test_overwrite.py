from domain.netex.model import ScheduledStopPoint, MultilingualString, TextType

from tests.base import MdbxStorageTestCase


class TestOverwrite(MdbxStorageTestCase):
    def test_inserting_same_id_version_twice_overwrites(self) -> None:
        ssp1 = ScheduledStopPoint(id="1", version="1", name=MultilingualString(content=[TextType(value="ssp1")]))
        ssp2 = ScheduledStopPoint(id="1", version="1", name=MultilingualString(content=[TextType(value="ssp2")]))

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [ssp1])
            self.storage.insert_any_object_on_queue(txn_write, [ssp2])
            txn_write.commit()

        with self.storage.env.ro_transaction() as txn_read:
            stored = list(self.storage.iter_only_objects(txn_read, ScheduledStopPoint))

        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0], ssp2)
