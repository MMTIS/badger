from domain.netex.model import Line, Route, ServiceJourneyPattern

from tests.base import MdbxStorageTestCase


class TestFetchAllReferencesByClass(MdbxStorageTestCase):
    def test_fetch_all_references_by_class_yields_referenced_objects(self) -> None:
        line, route, sjp = self.make_line_route_sjp()

        with self.storage.env.rw_transaction() as txn_write:
            # Referenced objects first, so references resolve at insert time.
            self.storage.insert_any_object_on_queue(txn_write, [line, route, sjp])
            txn_write.commit()

        with self.storage.env.ro_transaction() as txn_read:
            fetched = list(self.storage.fetch_all_references_by_class(txn_read, {ServiceJourneyPattern}))

        # The direct reference (Route) and its transitive reference (Line) must both surface.
        fetched_ids = {(obj.__class__, obj.id) for obj in fetched}
        self.assertIn((Route, "r1"), fetched_ids)
        self.assertIn((Line, "l1"), fetched_ids)
        self.assertNotIn((ServiceJourneyPattern, "sjp1"), fetched_ids)
