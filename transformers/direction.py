from typing import Dict, Generator

from netexio.database import Database
from netexio.dbaccess import load_generator
from netex import ServiceJourneyPattern, Direction, MultilingualString, DirectionRef
from utils.refs import getId, getRef


def infer_directions_from_sjps_and_apply(db_read: Database, db_write: Database, generator_defaults: dict):
    directions: Dict[str, Direction] = {}
    direction_refs: Dict[str, DirectionRef] = {}

    def process(sjp: ServiceJourneyPattern, generator_defaults: dict):
        if sjp.direction_type is not None and sjp.direction_ref_or_direction_view is None:
            key = str(sjp.direction_type.value)
            direction: Direction = directions.get(key, None)
            if direction is None:
                direction = Direction(id=getId(Direction, generator_defaults['codespace'], key),
                                      version='any',
                                      name=MultilingualString(value=key),
                                      direction_type=sjp.direction_type.value)
                directions[key] = direction
                direction_refs[key] = getRef(direction)
            sjp.direction_ref_or_direction_view = direction_refs[key]
            return sjp

    def query(db_read: Database) -> Generator:
        _load_generator = load_generator(db_read, ServiceJourneyPattern)
        for sjp in _load_generator:
            new_sjp = process(sjp, generator_defaults)
            if new_sjp is not None:
                yield new_sjp

    db_write.guard_free_space(0.10)
    db_write.insert_objects_on_queue(ServiceJourneyPattern, query(db_read))
    db_write.insert_objects_on_queue(Direction, list(directions.values()), True)