from domain.netex.model import ScheduledStopPoint, Line, StopPlace, Notice, MultilingualString, TextType
from collections.abc import Generator
from storage.mdbx.core.implementation import MdbxStorage
from tests.base import MdbxStorageMPTestCase
from domain.netex.services.model_typing import Tid


def generator(clazz: Tid, n: int) -> Generator[Tid, None, None]:
    for i in range(1, n + 1):
        yield clazz(id=f"{clazz.__name__}:{i}", version=str(i), name=MultilingualString(content=[TextType(value=str(i))]))


class TestMultiProcessing(MdbxStorageMPTestCase):
    def test_uni_processing(self) -> None:
        with MdbxStorage(self.target, readonly=False) as storage:
            with storage.env.rw_transaction() as txn:
                storage.insert_any_object_on_queue(txn, generator(ScheduledStopPoint, 1100))
                storage.insert_any_object_on_queue(txn, generator(StopPlace, 1001))
                storage.insert_any_object_on_queue(txn, generator(Notice, 10))
                storage.insert_any_object_on_queue(txn, generator(Line, 1))
                txn.commit()

            with storage.env.ro_transaction() as txn:
                self.assertEqual(storage.count_objects(txn, ScheduledStopPoint), 1100)
                self.assertEqual(storage.count_objects(txn, StopPlace), 1001)
                self.assertEqual(storage.count_objects(txn, Notice), 10)
                self.assertEqual(storage.count_objects(txn, Line), 1)
