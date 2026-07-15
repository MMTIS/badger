from typing import Dict, Any, Generator, cast

from mdbx.mdbx import TXN
from domain.netex.model import ServiceJourneyPattern, Direction, MultilingualString, DirectionRef, DirectionType, \
    TextType
from domain.netex.services.ids import getId
from domain.netex.services.refs import getRef
from domain.netex.services.model_typing import Tid

from storage.mdbx.core.implementation import MdbxStorage


def infer_directions_from_sjps_and_apply(db_read: MdbxStorage, txn: TXN, generator_defaults: dict[str, Any]) -> Generator[ServiceJourneyPattern | Direction, None, None]:
    directions: Dict[str, Direction] = {}
    direction_refs: Dict[str, DirectionRef | None] = {}

    def process(sjp: ServiceJourneyPattern, generator_defaults: dict[str, Any]) -> ServiceJourneyPattern | None:
        if sjp.direction_type is not None and sjp.direction_ref_or_direction_view is None:
            key = str(sjp.direction_type.value.value)
            direction: Direction | None = directions.get(key, None)
            if direction is None:
                direction = Direction(
                    id=getId(generator_defaults['codespace'], Direction, key),
                    version=sjp.version,
                    # name=MultilingualString(content=[TextType(value=key)]),
                    name=MultilingualString(content=[key]),
                    direction_type=sjp.direction_type,
                )
                directions[key] = direction
                direction_refs[key] = cast(DirectionRef, getRef(direction))
            sjp.direction_ref_or_direction_view = direction_refs[key]
            return sjp

        return None

    def query(db_read: MdbxStorage) -> Generator[ServiceJourneyPattern, None, None]:
        for sjp in db_read.iter_only_objects(txn, ServiceJourneyPattern):
            new_sjp = process(sjp, generator_defaults)
            if new_sjp is not None:
                yield new_sjp

    yield from query(db_read)
    yield from directions.values()
