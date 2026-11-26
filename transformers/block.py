from collections.abc import Generator
from typing import Dict, Any, cast

from mdbx.mdbx import TXN
from domain.netex.model import ServiceJourneyPattern, Direction, MultilingualString, DirectionRef, TextType, Block, VehicleTypeRef, ServiceJourney, VehicleType, ServiceJourneyRef
from domain.netex.services.ids import getId
from domain.netex.services.refs import getRef

from storage.mdbx.core.implementation import MdbxStorage

# TODO: Apply on all references ap riori

def _hash(self: VehicleTypeRef) -> int:
    return hash((self.ref, self.version, self.name_of_ref_class))


def _eq(
    self: VehicleTypeRef,
    other: object,
) -> bool:
    if not isinstance(other, VehicleTypeRef):
        return NotImplemented

    return (
        self.ref == other.ref
        and self.version == other.version
        and self.name_of_ref_class == other.name_of_ref_class
    )


VehicleTypeRef.__hash__ = _hash
VehicleTypeRef.__eq__ = _eq


# We pull the block & vehicletype from the original data, and servicejourney from
def add_vehicletype_from_blocks_and_apply(
    db_read: MdbxStorage, txn_read: TXN,
    db_write: MdbxStorage, txn_write: TXN, generator_defaults: dict[str, Any]
) -> Generator[ServiceJourney | VehicleType, None, None]:
    vehicle_types_sjs: Dict[str, VehicleTypeRef] = dict()

    def process_block(block: Block) -> tuple[str, VehicleTypeRef]:
        # I want to break free from string refs
        if block.vehicle_type_ref_or_train_ref is not None and isinstance(block.vehicle_type_ref_or_train_ref, VehicleTypeRef):
            if block.journeys:
                for sjr in block.journeys.choice:
                    if isinstance(sjr, ServiceJourneyRef):
                        yield sjr.ref, block.vehicle_type_ref_or_train_ref

    def query1(db_read: MdbxStorage, txn_read: TXN) -> None:
        for block in db_read.iter_only_objects(txn_read, Block):
            vehicle_types_sjs.update(process_block(block))

    def query(db_read: MdbxStorage, txn_read: TXN, db_write: MdbxStorage, txn_write: TXN) -> Generator[ServiceJourney | VehicleType, None, None]:
        vehicle_types: set[str] = set()
        for service_journey in db_write.iter_only_objects(txn_write, ServiceJourney):
            if service_journey.id in vehicle_types_sjs:
                service_journey.vehicle_type_ref_or_train_ref = vehicle_types_sjs[service_journey.id]
                vehicle_types.add(vehicle_types_sjs[service_journey.id].ref)
                yield service_journey

        # This is the old, but better, way
        for vehicle_type in db_read.iter_only_objects(txn_read, VehicleType):
            if vehicle_type.id in vehicle_types:
                yield vehicle_type

    query1(db_read, txn_read)
    yield from query(db_read, txn_read, db_write, txn_write)
