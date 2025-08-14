import time
from typing import Callable

from netex import ServiceJourney
from netexio.database import Database
from netexio.dbaccess import update_embedded_referencing, load_local
from netexio.pickleserializer import MyPickleSerializer


def _drain(gen):
    n = 0
    for _ in gen:
        n += 1
    return n

def bench(run: Callable[[], int], repeat: int = 10000) -> tuple[float, int, float]:
    # warmup
    run()
    best = float("inf")
    last_count = None
    total = time.perf_counter()
    for _ in range(repeat):
        t0 = time.perf_counter()
        count = run()
        dt = time.perf_counter() - t0
        best = min(best, dt)
        last_count = count
    total = time.perf_counter() - total
    return best, last_count, total

sj: ServiceJourney
serializer = MyPickleSerializer(compression=True)
with Database("/storage/compressed/avv.lmdb", serializer, readonly=True) as source_db:
    sj = load_local(source_db, ServiceJourney, 1)[0]

best, count, total = bench(lambda: _drain(update_embedded_referencing(serializer, sj)))
print(best, count, total)

# for a, b, x, y, z, u, v, w in update_embedded_referencing(serializer, sj):
#     print(a, b, x, y, z, u, v, w)