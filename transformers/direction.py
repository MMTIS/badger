from typing import Dict, Any, Generator, cast, Tuple, Optional

from mdbx.mdbx import TXN
from domain.netex.model import ServiceJourneyPattern, Direction, MultilingualString, DirectionRef, DirectionType
from domain.netex.services.ids import getId
from domain.netex.services.refs import getRef
from domain.netex.services.model_typing import Tid

from storage.mdbx.core.implementation import MdbxStorage


def infer_directions_from_sjps_and_apply(db_read: MdbxStorage, txn: TXN, generator_defaults: dict[str, Any]) -> Generator[ServiceJourneyPattern | Direction, None, None]:
    directions: Dict[str, Direction] = {}
    direction_refs: Dict[str, DirectionRef | None] = {}

    def process(sjp: ServiceJourneyPattern, generator_defaults: dict[str, Any]) -> Tuple[Optional[ServiceJourneyPattern], Optional[Direction]] :
        # TODO we should perhaps think about, if we should invent Direction. It is not mandatory.
        if sjp.direction_type is not None and sjp.direction_ref_or_direction_view is None:
            key = str(sjp.direction_type.value)
            direction: Direction | None = directions.get(key, None)
            new_direction=None
            if direction is None:
                if directions.get(key) is None:
                    direction = Direction(
                        id=getId(generator_defaults['codespace'], Direction, key),
                        version='any',
                        name=MultilingualString(content=[key]),
                        direction_type=sjp.direction_type,
                    )
                    directions[key] = direction
                    new_direction=direction
                direction_refs[key] = cast(DirectionRef, getRef(direction))
            sjp.direction_ref_or_direction_view = direction_refs[key]
            return sjp, new_direction

        return None, None

    def query(db_read: MdbxStorage) -> Generator[ServiceJourneyPattern, None, None]:
        for sjp in db_read.iter_only_objects(txn, ServiceJourneyPattern):
            new_sjp,new_direction = process(sjp, generator_defaults)
            if new_direction is not None:
                yield new_direction
            if new_sjp is not None:
                yield new_sjp


    yield from query(db_read)

