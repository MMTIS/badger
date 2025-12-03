import unittest
from pathlib import Path

from domain.netex.model import ScheduledStopPoint, ServiceJourneyPattern
from storage.mdbx.core.implementation import MdbxStorage


class MyTestCase(unittest.TestCase):
    def test_something(self):
        l = [ScheduledStopPoint(id="1", version="1")]
        with MdbxStorage(Path("/tmp/overwrite.mdbx"), readonly=False) as source_db:
            with source_db.env.rw_transaction() as txn_write:
                source_db.insert_any_object_on_queue(txn_write, l)
                source_db.insert_any_object_on_queue(txn_write, l)
                txn_write.commit()

if __name__ == '__main__':
    unittest.main()