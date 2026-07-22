from domain.netex.model import DayType, DayTypesRelStructure, ServiceCalendar

from storage.mdbx.core.references import resolve_embeddings_iterable

from tests.base import MdbxStorageTestCase


class TestEmbeddings(MdbxStorageTestCase):
    def test_embedded_object_can_be_resolved_and_promoted(self) -> None:
        day_type = DayType(id="dt1", version="1")
        calendar = ServiceCalendar(id="sc1", version="1", day_types=DayTypesRelStructure(day_type_ref_or_day_type_dummy=[day_type]))

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [calendar])
            txn_write.commit()

        with self.storage.env.ro_transaction() as txn_read:
            result = self.storage.load_object_by_id_version(txn_read, "dt1", DayType, "1")
            self.assertIsNone(result)

        with self.storage.env.ro_transaction() as txn_read:
            embeddings = [
                embedded
                for _key, _parent, (_embedded_key, embedded, _path) in resolve_embeddings_iterable(
                    self.storage, txn_read, ServiceCalendar, interesting_classes={DayType}
                )
            ]

        self.assertEqual(embeddings, [day_type])

        # Promote the embedded object to a first-class object.
        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, embeddings)
            txn_write.commit()

        with self.storage.env.ro_transaction() as txn_read:
            result = self.storage.load_object_by_id_version(txn_read, "dt1", DayType, "1")
            self.assertIsNotNone(result)
            assert result is not None
            _full_key, promoted = result

        self.assertEqual(promoted, day_type)

    def test_load_object_by_id_version_returns_none_for_missing_item(self) -> None:
        with self.storage.env.ro_transaction() as txn_read:
            self.assertIsNone(self.storage.load_object_by_id_version(txn_read, "missing", DayType, "1"))
            self.assertIsNone(self.storage.load_object_by_id_version(txn_read, "missing", DayType))
