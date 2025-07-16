from netex import ScheduledStopPoint, MultilingualString
from netexio.database import Database
from netexio.pickleserializer import MyPickleSerializer

if __name__ == "__main__":
    with Database(
        "/tmp/test.lmdb",
        MyPickleSerializer(compression=True),
        readonly=False,
        initial_size=1000 * 1024,
    ) as lmdb_db:
        # Implement the growing test
        db_handle = lmdb_db.open_database(ScheduledStopPoint)
        for j in range(0, 1000):
            items = [
                ScheduledStopPoint(id=str(i * j), name=MultilingualString(value="a" * 3072)) for i in range(0, 1024)
            ]
            lmdb_db.insert_objects_on_queue(ScheduledStopPoint, items)
