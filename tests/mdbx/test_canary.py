import unittest
from tests.base import MdbxStorageTestCase
from mdbx.mdbx import MDBXCanary


class TestCanary(MdbxStorageTestCase):

    @unittest.expectedFailure
    def test_canary_put(self) -> None:
        with self.storage.env.rw_transaction() as txn:
            canary = MDBXCanary()
            canary.x = 1
            canary.y = 2
            canary.z = 3

            txn.put_canary(canary)
            txn.commit()

        with self.storage.env.ro_transaction() as txn:
            canary = txn.get_canary()

            self.assertEqual(canary.x, 1)
            self.assertEqual(canary.y, 2)
            self.assertEqual(canary.z, 3)
