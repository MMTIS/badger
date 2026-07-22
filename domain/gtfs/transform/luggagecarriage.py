from domain.netex.model import LuggageCarriageEnumeration


def bicyclesToNeTEx(bikes_allowed: int) -> LuggageCarriageEnumeration:
    if bikes_allowed == 1:
        return LuggageCarriageEnumeration.CYCLES_ALLOWED

    elif bikes_allowed == 2:
        return LuggageCarriageEnumeration.NO_CYCLES

    return LuggageCarriageEnumeration.UNKNOWN
