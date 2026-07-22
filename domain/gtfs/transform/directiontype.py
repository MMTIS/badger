from domain.netex.model import DirectionTypeEnumeration, DirectionType


def directionToNeTEx(direction_id: int | None) -> DirectionType | None:
    if direction_id is None:
        return None

    elif direction_id == 1:
        return DirectionType(value=DirectionTypeEnumeration.INBOUND)

    return DirectionType(value=DirectionTypeEnumeration.OUTBOUND)
