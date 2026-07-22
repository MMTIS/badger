import unittest
from pathlib import Path

from domain.netex.model import ScheduledStopPoint, ServiceJourneyPattern
from storage.mdbx.core.implementation import MdbxStorage


class MyTestCase(unittest.TestCase):
    def test_something(self):
        with MdbxStorage(Path("/tmp/wpd.mdbx")) as source_db:
            with source_db.env.ro_transaction() as txn_read:
                print(list(source_db.fetch_all_references_by_class(txn_read, [ServiceJourneyPattern])))

if __name__ == '__main__':
    unittest.main()
