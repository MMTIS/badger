from pathlib import Path
from domain.netex.model import ScheduledStopPoint, Line, StopPlace, Notice, MultilingualString, TextType, EntityInVersionStructure
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections.abc import Generator
from storage.mdbx.core.implementation_queue import MdbxStorageQueue
from storage.mdbx.core.implementation import MdbxStorage
from storage.mdbx.core.implementation_mp import MdbxStorageMP
from tests.base import MdbxStorageMPTestCase
from domain.netex.services.model_typing import Tver
import queue

n_proc = 5


def generator(clazz: type[Tver], n: int) -> Generator[Tver, None, None]:
    for i in range(1, n + 1):
        yield clazz(id=f"{clazz.__name__}:{i}", version=str(i), name=MultilingualString(content=[TextType(value=str(i))]))


def parse_and_enqueue(target: Path, queue: queue.Queue, clazz: type[EntityInVersionStructure], n: int) -> None:
    """Runs in a subprocess: enqueue objects."""
    with MdbxStorageQueue(target, queue) as storage:
        storage.insert_any_object_on_queue(None, generator(clazz, n))


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

    def test_multi_processing(self) -> None:
        import multiprocessing as mp

        fork_ctx = mp.get_context("fork")
        with ProcessPoolExecutor(max_workers=n_proc, mp_context=fork_ctx) as executor:
            with MdbxStorageMP(self.target, readonly=False) as storage:
                futures = []
                futures.append(executor.submit(parse_and_enqueue, self.target, storage.queue, ScheduledStopPoint, 1100))
                futures.append(executor.submit(parse_and_enqueue, self.target, storage.queue, StopPlace, 1001))
                futures.append(executor.submit(parse_and_enqueue, self.target, storage.queue, Notice, 10))
                futures.append(executor.submit(parse_and_enqueue, self.target, storage.queue, Line, 1))

                for future in as_completed(futures):
                    _res = future.result()

                storage.queue.put(None)

        # The context is exited, so writer process has joined and all data is committed.
        with MdbxStorage(self.target, readonly=True) as storage:
            with storage.env.ro_transaction() as txn:
                self.assertEqual(storage.count_objects(txn, ScheduledStopPoint), 1100)
                self.assertEqual(storage.count_objects(txn, StopPlace), 1001)
                self.assertEqual(storage.count_objects(txn, Notice), 10)
                self.assertEqual(storage.count_objects(txn, Line), 1)
