from domain.netex.model import Route, ServiceJourneyPattern

from tests.base import MdbxStorageTestCase


class TestInwardReferences(MdbxStorageTestCase):
    def test_inward_references_yield_referencing_objects(self) -> None:
        line, route, sjp = self.make_line_route_sjp()

        with self.storage.env.rw_transaction() as txn_write:
            # Referenced objects first, so the outward edges (and the inward index
            # they back) are populated at insert time.
            self.storage.insert_any_object_on_queue(txn_write, [line, route, sjp])
            txn_write.commit()

        with self.storage.env.ro_transaction() as txn_read:
            # Route references Line -> Line is referenced *inwards* by Route.
            line_referrers = {(type(obj), obj.id) for obj in self.storage.load_references_by_object_values(txn_read, line, inwards=True)}
            # ServiceJourneyPattern references Route -> Route is referenced inwards by the SJP.
            route_referrers = {(type(obj), obj.id) for obj in self.storage.load_references_by_object_values(txn_read, route, inwards=True)}

        self.assertIn((Route, "r1"), line_referrers)
        self.assertIn((ServiceJourneyPattern, "sjp1"), route_referrers)

    def test_inward_references_empty_for_unreferenced_object(self) -> None:
        line, route, sjp = self.make_line_route_sjp()

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [line, route, sjp])
            txn_write.commit()

        with self.storage.env.ro_transaction() as txn_read:
            # Nothing references the ServiceJourneyPattern at the top of the chain.
            sjp_referrers = list(self.storage.load_references_by_object_values(txn_read, sjp, inwards=True))

        self.assertEqual(sjp_referrers, [])
