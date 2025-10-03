from pathlib import Path

from domain.netex.model import RouteRef, AuthorityRef
from storage.mdbx.core.implementation import MdbxStorage

with MdbxStorage(Path("/mnt/storage/compressed//NL_BASIS_20250928.mdbx"), readonly=True) as storage:
    with storage.env.ro_transaction() as ro_txn:
        ref = AuthorityRef(ref="NL:DOVA:Authority:VRA", version="any", name_of_ref_class="Authority")
        authority = storage.load_object_by_reference(ro_txn, ref)
        print(authority)

        ref = AuthorityRef(ref="NL:DOVA:Authority:VRA", version="any")
        authority = storage.load_object_by_reference(ro_txn, ref)
        print(authority)