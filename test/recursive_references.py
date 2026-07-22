import unittest
from pathlib import Path

from domain.netex.model import (
    ServiceJourney,
    Line,
    NoticeAssignment,
    PassengerStopAssignment,
    Route,
    ServiceJourneyPattern,
)
from storage.mdbx.core.implementation import MdbxStorage

class MyTestCase(unittest.TestCase):
    def test_something(self):
        # self.assertEqual(True, False)  # add assertion here
        with MdbxStorage(Path("/tmp/wsf.mdbx")) as source_db:
            with source_db.env.ro_transaction() as txn_read:
                # full_key, sj = source_db.load_object_by_id_version(txn_read, "NL:WSF:ServiceJourney:V_B_16-18-00", ServiceJourney)
                # for obj in source_db.load_references_by_object_values_dfs(txn_read, full_key):
                #    print(obj)

                full_key, sj = source_db.load_object_by_id_version(txn_read, "NL:WSF:ServiceJourneyPattern:V-B", ServiceJourneyPattern)
                for obj in source_db.load_references_by_object_values_dfs(txn_read, [full_key], {ServiceJourneyPattern}):
                    print(obj)

                # full_key, sj = source_db.load_object_by_id_version(txn_read, "NL:WSF:Line:WSF", Line)
                # for obj in source_db.load_references_by_object_values_dfs(txn_read, full_key, {Line, NoticeAssignment, PassengerStopAssignment, Route, ServiceJourneyPattern}):
                #    print(obj)


if __name__ == '__main__':
    unittest.main()