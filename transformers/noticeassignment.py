from collections.abc import Generator
from collections import defaultdict
from typing import Dict, Any, cast

from mdbx.mdbx import TXN
from domain.netex.model import ServiceJourneyPattern, Direction, MultilingualString, DirectionRef, TextType, Block, VehicleTypeRef, ServiceJourney, VehicleType, ServiceJourneyPattern, ServiceJourneyInterchange, Line, NoticeAssignment, NoticeAssignmentsRelStructure, EntityInVersion, VersionOfObjectRefStructure
from domain.netex.services.ids import getId
from domain.netex.services.refs import getRef

from storage.mdbx.core.implementation import MdbxStorage

def _hash(self: VersionOfObjectRefStructure) -> int:
    return hash((self.ref, self.version, self.name_of_ref_class))


def _eq(
    self: VersionOfObjectRefStructure,
    other: object,
) -> bool:
    if not isinstance(other, VersionOfObjectRefStructure):
        return NotImplemented

    return (
        self.ref == other.ref
        and self.version == other.version
        and self.name_of_ref_class == other.name_of_ref_class
    )


VersionOfObjectRefStructure.__hash__ = _hash
VersionOfObjectRefStructure.__eq__ = _eq

# We pull the block & vehicletype from the original data, and servicejourney from
def add_noticeassignment_to_elements_and_apply(
    db_read: MdbxStorage, txn_read: TXN,
    db_write: MdbxStorage, txn_write: TXN, generator_defaults: dict[str, Any]
) -> Generator[ServiceJourney | VehicleType, None, None]:
    obj_notice_assignment: defaultdict[
        VersionOfObjectRefStructure, list[NoticeAssignment]
    ] = defaultdict(list)

    def query(db_read: MdbxStorage, txn_read: TXN) -> None:
        for notice_assignment in db_read.iter_only_objects(txn_read, NoticeAssignment):
            if notice_assignment.noticed_object_ref is not None and notice_assignment.noticed_object_ref.name_of_ref_class is not None:
                ref = notice_assignment.noticed_object_ref
                if ref is not None and ref.name_of_ref_class is not None:
                    obj_notice_assignment[ref].append(notice_assignment)

    query(db_read, txn_read)

    def process(db_write: MdbxStorage, txn_write: TXN) -> Generator[EntityInVersion, None, None]:
        for ref, lna in obj_notice_assignment.items():
            obj = db_write.load_object_by_reference(txn_write, ref)
            if hasattr(obj, "notice_assignments"):
                obj.notice_assignments = NoticeAssignmentsRelStructure(sales_notice_assignment_or_notice_assignment_or_notice_assignment_view=lna)
                yield obj

    yield from process(db_write, txn_write)
