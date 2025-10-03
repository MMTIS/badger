from pathlib import Path

from domain.netex.model import (
    AuthorityRef,
    Authority,
    MultilingualString,
    Operator,
)
"""
from storage.mdbx.core.implementation import MdbxStorage

with MdbxStorage(Path("/tmp/unittest.mdbx"), readonly=False) as storage:
    storage.insert_objects_on_queue(Authority, [Authority(id="1", version="1", name=MultilingualString(content=["test"]))], False)
    storage.insert_objects_on_queue(Operator, [Operator(id="1", version="1", name=MultilingualString(content=["test"]))], False)

with MdbxStorage(Path("/tmp/unittest.mdbx"), readonly=True) as storage:
    with storage.env.ro_transaction() as ro_txn:
        ref = AuthorityRef(ref="1", version="1", name_of_ref_class="Authority")
        authority = list(storage.load_object_by_reference(ro_txn, ref))
        print(authority)

        ref = AuthorityRef(ref="1", version="1")
        test = list(storage.load_object_by_reference(ro_txn, ref))
        print(test)
"""