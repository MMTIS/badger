from domain.netex.model import AllPublicTransportModesEnumeration


def gtfsRouteTypeToNeTEx(route_type: int | None) -> AllPublicTransportModesEnumeration | None:
    if route_type == 0:
        return AllPublicTransportModesEnumeration.TRAM
    elif route_type == 1:
        return AllPublicTransportModesEnumeration.METRO
    elif route_type == 2:
        return AllPublicTransportModesEnumeration.RAIL
    elif route_type == 3:
        return AllPublicTransportModesEnumeration.BUS
    elif route_type == 4:
        return AllPublicTransportModesEnumeration.WATER
    elif route_type == 5 or route_type == 7:
        return AllPublicTransportModesEnumeration.FUNICULAR
    elif route_type == 6:
        return AllPublicTransportModesEnumeration.CABLEWAY
    elif route_type == 11:
        return AllPublicTransportModesEnumeration.TROLLEY_BUS

    return None
