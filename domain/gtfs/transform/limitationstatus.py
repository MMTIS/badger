from domain.netex.model import LimitationStatusEnumeration


def wheelchairToNeTEx(wheelchair_accessible: int) -> LimitationStatusEnumeration:
    if wheelchair_accessible == 1:
        return LimitationStatusEnumeration.TRUE

    elif wheelchair_accessible == 2:
        return LimitationStatusEnumeration.FALSE

    return LimitationStatusEnumeration.UNKNOWN
