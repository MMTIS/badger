import unittest
from pathlib import Path

from storage.mdbx.core.implementation import MdbxStorage
from storage.mdbx.core.references import resolve_embeddings_iterable
from domain.netex.model import ServiceCalendar, UicOperatingPeriod, DayType, ServiceCalendarFrame
from domain.netex.indexes.inverse_class import collect_classes_index


class MyTestCase(unittest.TestCase):
    def test_something(self):
        # self.assertEqual(True, False)  # add assertion here
        with MdbxStorage(Path("/tmp/wsf.mdbx")) as source_db:
            with source_db.env.ro_transaction() as txn_read:
                # used_classes_in_database = set(source_db.db_names(txn_read).values())
                # index = collect_classes_index(used_classes_in_database, scope_classes=set([UicOperatingPeriod, DayType]))
                # clazzes: set[type] = set().union(*index.values())

                # for clazz in clazzes:
                #    for _, _, embedding in resolve_embeddings_iterable(source_db, txn_read, clazz):
                #        print(embedding)

                def all_embeddings():
                    for _, _, embedding in resolve_embeddings_iterable(source_db, txn_read, Servicecalendar):
                        yield embedding

                source_db.insert_any_object_on_queue(txn_write, all_embeddings)



                # for clazz in source_db.db_names_iter(txn_read):
                #    for _, _, embedding in resolve_embeddings_iterable(source_db, txn_read, clazz):
                #        print(embedding)

if __name__ == '__main__':
    unittest.main()
