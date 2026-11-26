import unittest
from domain.netex.model import NoticeAssignment, NoticeRef, VersionOfObjectRefStructure, Notice, ScheduledStopPoint, MultilingualString, TextType, NameOfClass, ServiceJourneyInterchange, ScheduledStopPointRefStructure
from tests.base import MdbxStorageTestCase
from domain.netex.services.recursive_attributes import only_references
from storage.mdbx.core.implementation import DB_UNRESOLVED, DB_UNRESOLVED_FLAGS
from storage.mdbx.core.references import resolve, resolve_embeddings_index

class TestAbstractReference(MdbxStorageTestCase):
    def test_abstract_reference_from_point(self):
        ssp1 = ScheduledStopPoint(id="ssp1", version="1")
        sji1 = ServiceJourneyInterchange(id="sji1", from_point_ref=ScheduledStopPointRefStructure(ref=ssp1.id, version=ssp1.version))

        result = list(only_references(sji1, self.storage.serializer))
        self.assertEqual(result[0][0], ssp1.__class__)
        self.assertEqual(result[0][1], ssp1.id)
        self.assertEqual(result[0][2], ssp1.version)
        # path is ommitted as check
        self.assertEqual(result[0][4], sji1.from_point_ref.__class__)

    def test_abstract_reference_name_of_ref_class(self):
        ssp1 = ScheduledStopPoint(id="ssp1", version="1")
        n1 = Notice(id="n1", version="1", text=MultilingualString(content=[TextType(value="Notice Text")]))
        na1 = NoticeAssignment(id="na1", version="1",
                               notice_ref_or_group_of_notices_ref_or_notice=NoticeRef(ref=n1.id, version=n1.version),
                               noticed_object_ref=VersionOfObjectRefStructure(ref=ssp1.id, version=ssp1.version, name_of_ref_class=NameOfClass.SCHEDULED_STOP_POINT))

        result = list(only_references(na1, self.storage.serializer))
        print(result)

        self.assertEqual(result[0][0], n1.__class__)
        self.assertEqual(result[0][1], n1.id)
        self.assertEqual(result[0][2], n1.version)
        # path is ommitted as check
        self.assertEqual(result[0][4], na1.notice_ref_or_group_of_notices_ref_or_notice.__class__)

        self.assertEqual(result[1][0], ssp1.__class__)
        self.assertEqual(result[1][1], ssp1.id)
        self.assertEqual(result[1][2], ssp1.version)
        self.assertEqual(result[1][4], na1.noticed_object_ref.__class__)

        with self.storage.env.rw_transaction() as txn_write:
            # Deliberately in the wrong order, so we must resolve them.
            self.storage.insert_any_object_on_queue(txn_write, [na1, ssp1, n1])
            txn_write.commit()

        with self.storage.env.ro_transaction() as txn_read:
            db_unresolved = txn_read.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            self.assertEqual(db_unresolved.get_stat(txn_read).ms_entries, 2)

        resolve(self.storage)

        with self.storage.env.ro_transaction() as txn_read:
            db_unresolved = txn_read.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            self.assertEqual(db_unresolved.get_stat(txn_read).ms_entries, 0)

        # with self.storage.env.ro_transaction() as txn_read:
        #    db_unresolved = txn_read.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
        #    self.assertEqual(db_unresolved.get_stat(txn_read).ms_entries, 2)


if __name__ == '__main__':
    unittest.main()
