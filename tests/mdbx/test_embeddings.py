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
)

from storage.mdbx.core.references import resolve_embeddings_iterable, resolve_embeddings_index

from storage.mdbx.core.implementation import DB_UNRESOLVED, DB_UNRESOLVED_FLAGS

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
                            sales_notice_assignment_or_notice_assignment_or_notice_assignment_view=NoticeAssignment(
                                id="na1", version="1", notice_ref_or_group_of_notices_ref_or_notice=Notice(id="n1", version="1")
                            )
                        ),
                    ),
                    StopPointInJourneyPattern(
                        id="spijp2",
                        version="1",
                        notice_assignments=NoticeAssignmentsRelStructure(
                            sales_notice_assignment_or_notice_assignment_or_notice_assignment_view=NoticeAssignment(
                                id="na1", version="1", notice_ref_or_group_of_notices_ref_or_notice=NoticeRef(ref="n1", version="1")
                            )
                        ),
                    ),
                ]
            ),
        )

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [sjp1])
            txn_write.commit()

        with self.storage.env.ro_transaction() as txn_read:
            db_unresolved = txn_read.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            self.assertEqual(db_unresolved.get_stat(txn_read).ms_entries, 1)

        resolve_embeddings_index(self.storage)

        with self.storage.env.ro_transaction() as txn_read:
            db_unresolved = txn_read.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            self.assertEqual(db_unresolved.get_stat(txn_read).ms_entries, 0)
