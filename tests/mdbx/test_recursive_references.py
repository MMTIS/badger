from domain.netex.model import (
    Line,
    LineRef,
    MultilingualString,
    Route,
    RouteRef,
    RouteRefsRelStructure,
    ServiceJourneyPattern,
    TextType,
)

from storage.mdbx.core.references import resolve, resolve_embeddings_index

from tests.base import MdbxStorageTestCase


class TestRecursiveReferences(MdbxStorageTestCase):
    def test_dfs_walks_outward_reference_chain(self) -> None:
        line, route, sjp = self.make_line_route_sjp()

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [line, route, sjp])
            txn_write.commit()

        resolve(self.storage)
        resolve_embeddings_index(self.storage)

        with self.storage.env.rw_transaction() as txn_write:
            self.storage._index_references_inwards(txn_write, force=True)
            txn_write.commit()

        with self.storage.env.ro_transaction() as txn_read:
            result = self.storage.load_object_by_id_version(txn_read, "sjp1", ServiceJourneyPattern, "1")
            self.assertIsNotNone(result)
            assert result is not None
            full_key, loaded_sjp = result
            self.assertEqual(loaded_sjp, sjp)

            visited = list(self.storage.load_references_by_object_values_dfs(txn_read, [full_key]))

        visited_ids = {(obj.__class__, obj.id) for obj in visited}
        self.assertGreaterEqual(visited_ids, {(ServiceJourneyPattern, "sjp1"), (Route, "r1"), (Line, "l1")})

    def test_dfs_terminates_on_cyclic_references(self) -> None:
        # Line <-> Route reference each other, forming a cycle.
        line = Line(
            id="l1",
            version="1",
            name=MultilingualString(content=[TextType(value="Line l1")]),
            routes=RouteRefsRelStructure(route_ref=[RouteRef(ref="r1", version="1")]),
        )
        route = Route(id="r1", version="1", line_ref=LineRef(ref="l1", version="1"))

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [line, route])
            txn_write.commit()

        # One direction stays unresolved at insert time; resolve() wires the back-edge,
        # so DB_REFERENCE_OUTWARD now holds both edges of the cycle.
        resolve(self.storage)

        with self.storage.env.ro_transaction() as txn_read:
            result = self.storage.load_object_by_id_version(txn_read, "l1", Line, "1")
            self.assertIsNotNone(result)
            assert result is not None
            full_key, _ = result
            visited = list(self.storage.load_references_by_object_values_dfs(txn_read, [full_key]))

        visited_ids = [(obj.__class__, obj.id) for obj in visited]
        # Terminates despite the cycle, and `visited` yields each object exactly once.
        self.assertEqual(set(visited_ids), {(Line, "l1"), (Route, "r1")})
        self.assertEqual(len(visited_ids), 2)
