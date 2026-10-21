from domain.netex import NameOfClass
from domain.netex.model import (
    DayType,
    DayTypesRelStructure,
    ServiceCalendar,
    ServiceJourneyPattern,
    StopPointInJourneyPattern,
    PointsInJourneyPatternRelStructure,
    NoticeAssignmentsRelStructure,
    NoticeAssignment,
    Notice,
    NoticeRef,
    ResponsibilitySet,
    ResponsibilityRoleAssignment,
    ResponsibilityRoleAssignmentsRelStructure,
    Authority,
    OrganisationRefStructure,
    MultilingualString,
    TextType,
)

from storage.mdbx.core.references import resolve_embeddings_iterable, resolve_embeddings_index, resolve
from storage.mdbx.core.implementation import DB_UNRESOLVED, DB_UNRESOLVED_FLAGS
from tests.base import MdbxStorageTestCase


class TestEmbeddings(MdbxStorageTestCase):
    def test_ambigious_embedding(self) -> None:
        """
        This unittest addresses a real world example of an id that is a stringwise duplicate two different objects,
        which is allowed according to the NeTEx specification. And a missing name_of_ref_class to identify in which
        database the object can be found. What this case makes special is that the definition of the referenced object
        is equal to the current object. When wrongly implemented the OrganisationRefStructure may point to the
        ResponsibilitySet.

        1. We test that the reference towards the unknown object has been set
        2. We test that a single reference exists in our metadata
        3. We test that, this reference points to the created Authority object
        """
        # https://github.com/MMTIS/badger/issues/169
        rs = ResponsibilitySet(id="IT:ITH1:Authority:apb:", version="any",
                               roles=ResponsibilityRoleAssignmentsRelStructure(responsibility_role_assignment=[
                                   ResponsibilityRoleAssignment(id="IT:ITH1:Authority:apb:", version="any",
                                                                responsible_organisation_ref=OrganisationRefStructure(ref="IT:ITH1:Authority:apb:", version="any")),
                               ]))

        authority = Authority(id="IT:ITH1:Authority:apb:", version="any", name=MultilingualString(content=[TextType(value="Authority")]))

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [rs, authority])
            txn_write.commit()

        resolve(self.storage)

        with self.storage.env.ro_transaction() as txn_read:
            authority_loaded: Authority
            rs_loaded: ResponsibilitySet
            full_key, rs_loaded = self.storage.load_object_by_id_version(txn_read, "IT:ITH1:Authority:apb:", ResponsibilitySet, "any")
            self.assertEqual(NameOfClass.AUTHORITY, rs_loaded.roles.responsibility_role_assignment[0].responsible_organisation_ref.name_of_ref_class)

            references = list(self.storage.load_references_by_clazz_full_key(txn_read, full_key, False))
            self.assertEqual(1, len(references))

            full_key_, authority_loaded = self.storage.load_object_by_full_key(txn_read, full_key)
            self.assertEqual("IT:ITH1:Authority:apb:", authority_loaded.id)

    def test_ambigious_embedding_any_version(self) -> None:
        """
        This unittest takes the inability to provide high quality data and recover from it one step further.
        In case that the invalid, or "any" version is used. It must also correctly resolve the relationship towards
        the authority.
        """

        # https://github.com/MMTIS/badger/issues/169
        rs2 = ResponsibilitySet(id="IT:ITH1:Authority:apb2:", version="any",
                               roles=ResponsibilityRoleAssignmentsRelStructure(responsibility_role_assignment=[
                                   ResponsibilityRoleAssignment(id="IT:ITH1:Authority:apb2:", version="any",
                                                                responsible_organisation_ref=OrganisationRefStructure(ref="IT:ITH1:Authority:apb2:", version="any")),
                               ]))
        authority2 = Authority(id="IT:ITH1:Authority:apb2:", version="1", name=MultilingualString(content=[TextType(value="Authority")]))

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [rs2, authority2])
            txn_write.commit()

        resolve(self.storage)

        with self.storage.env.ro_transaction() as txn_read:
            authority_loaded: Authority
            rs_loaded: ResponsibilitySet
            full_key, rs_loaded = self.storage.load_object_by_id_version(txn_read, "IT:ITH1:Authority:apb2:", ResponsibilitySet, "any")
            self.assertEqual(NameOfClass.AUTHORITY, rs_loaded.roles.responsibility_role_assignment[0].responsible_organisation_ref.name_of_ref_class)

            references = list(self.storage.load_references_by_clazz_full_key(txn_read, full_key, False))
            self.assertEqual(1, len(references))

            full_key_, authority_loaded = self.storage.load_object_by_full_key(txn_read, full_key)
            self.assertEqual("IT:ITH1:Authority:apb2:", authority_loaded.id)

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

    def test_embedding_instance_in_found(self) -> None:
        """
        Here we test if an embedding can be resolved, which is an instance, but not a list.
        """

        sjp1 = ServiceJourneyPattern(
            id="sjp1",
            version="1",
            points_in_sequence=PointsInJourneyPatternRelStructure(
                point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern=[
                    StopPointInJourneyPattern(
                        id="spijp1",
                        version="1",
                        notice_assignments=NoticeAssignmentsRelStructure(
                            sales_notice_assignment_or_notice_assignment_or_notice_assignment_view=[
                                NoticeAssignment(id="na1", version="1", notice_ref_or_group_of_notices_ref_or_notice=Notice(id="n1", version="1"))
                            ]
                        ),
                    ),
                    StopPointInJourneyPattern(
                        id="spijp2",
                        version="1",
                        notice_assignments=NoticeAssignmentsRelStructure(
                            sales_notice_assignment_or_notice_assignment_or_notice_assignment_view=[
                                NoticeAssignment(id="na2", version="1", notice_ref_or_group_of_notices_ref_or_notice=NoticeRef(ref="n1", version="1"))
                            ]
                        ),
                    ),
                    StopPointInJourneyPattern(
                        id="spijp3",
                        version="1",
                        notice_assignments=NoticeAssignmentsRelStructure(
                            sales_notice_assignment_or_notice_assignment_or_notice_assignment_view=[
                                NoticeAssignment(id="na3", version="1", notice_ref_or_group_of_notices_ref_or_notice=NoticeRef(ref="n2", version="1"))
                            ]
                        ),
                    ),
                ]
            ),
        )

        sjp2 = ServiceJourneyPattern(
            id="sjp2",
            version="1",
            points_in_sequence=PointsInJourneyPatternRelStructure(
                point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern=[
                    StopPointInJourneyPattern(
                        id="spijp4",
                        version="1",
                        notice_assignments=NoticeAssignmentsRelStructure(
                            sales_notice_assignment_or_notice_assignment_or_notice_assignment_view=[
                                NoticeAssignment(id="na3", version="1", notice_ref_or_group_of_notices_ref_or_notice=Notice(id="n2", version="1"))
                            ]
                        ),
                    ),
                    StopPointInJourneyPattern(
                        id="spijp5",
                        version="1",
                    ),
                ]
            ),
        )

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [sjp1, sjp2])
            txn_write.commit()

        with self.storage.env.ro_transaction() as txn_read:
            db_unresolved = txn_read.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            self.assertEqual(db_unresolved.get_stat(txn_read).ms_entries, 1)

        resolve_embeddings_index(self.storage)

        with self.storage.env.ro_transaction() as txn_read:
            db_unresolved = txn_read.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            self.assertEqual(db_unresolved.get_stat(txn_read).ms_entries, 0)
