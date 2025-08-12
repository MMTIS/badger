import threading


class SignaledCursor:
    def __init__(self, env, db, clazz, db_instance):
        self.env = env
        self.db = db
        self.clazz = clazz
        self.db_instance = db_instance
        self.last_seen_version = db_instance.resize_version

    def __iter_cursor_from_key(self, start_key=None):
        with self.env.begin(db=self.db) as txn:
            cursor = txn.cursor()
            if start_key is not None:
                if cursor.set_key(start_key):
                    cursor.next()
            for key, value in cursor:
                yield key, self.db_instance.serializer.unmarshall(value, self.clazz)

    def __iter__(self):
        start_key = None
        while True:
            for key, obj in self.__iter_cursor_from_key(start_key):
                if self.last_seen_version != self.db_instance.resize_version:
                    print("Restart event is set, waiting for resize")
                    self.db_instance.resize_done_event.wait()
                    print("Resized!")
                    break  # restart transaction

                yield obj
                start_key = key

            else:
                break
