from operator import attrgetter
from typing import Iterable

from domain.netex.services.model_typing import Tid


def getIndex(objects: Iterable[Tid], attr: str | None = None) -> dict[object, Tid]:
    if not attr:
        return {x.id: x for x in objects}

    f = attrgetter(attr)  # TODO: change with our own attrgetter that understands lists
    return {f(x): x for x in objects}

def getIndexNew(objects: Iterable[tuple[bytes, Tid]], attr: str | None = None) -> dict[object, Tid]:
    if not attr:
        return {x[1].id: x[1] for x in objects}

    f = attrgetter(attr)  # TODO: change with our own attrgetter that understands lists
    return {f(x[1]): x[1] for x in objects}
