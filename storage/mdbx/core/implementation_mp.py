from types import TracebackType
from typing import Optional, Type, Literal, Self
import multiprocessing as mp
import queue
import sys
from pathlib import Path

from storage.mdbx.core.implementation_queue import MdbxStorageQueue
from storage.mdbx.core.mp_consumer import consumer


class MdbxStorageMP(MdbxStorageQueue):
    queue: queue.Queue[list[tuple[bytes, bytes, bytes, tuple[bytes, ...]]] | None]

    def __init__(self, path: Path, queue: Optional[queue.Queue] = None, readonly: bool = True):
        self.ctx = mp.get_context('fork') if sys.platform != 'win32' else mp.get_context('spawn')
        self.manager = self.ctx.Manager()
        self.queue = queue if queue is not None else self.manager.Queue(maxsize=10000)
        super().__init__(path, self.queue, readonly)

    def __enter__(self) -> Self:
        # This should take care of the ground work, like new database, we loose that Env because that must be done in the consumer
        super().__enter__()

        if not self.readonly:
            self.writer = self.ctx.Process(target=consumer, args=(self.queue, self.path.as_posix(), self.max_dbs))
            self.writer.start()

        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> Literal[False]:
        if self.writer.is_alive():
            self.queue.put(None)
            self.writer.join()

        return super().__exit__(exception_type, exception_value, exception_traceback)
