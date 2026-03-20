from typing import Generator

import duckdb

from domain.gtfs.transform.string import getRequiredString, getOptionalString
from domain.gtfs.transform.operator import get_agency_id
from domain.gtfs.transform.transporttype import gtfsRouteTypeToNeTEx
from domain.netex.model import Line, PresentationStructure, OperatorRef, Codespace, PublicCodeStructure, PrivateCodes, PrivateCode
from domain.netex.services.ids import getId
from domain.netex.services.refs import getFakeRef


def get_route_id(codespace: Codespace, route_id: str) -> str:
    if ':Line:' in route_id:
        return route_id
    return getId(codespace, Line, route_id)


def getLines(con: duckdb.DuckDBPyConnection, codespace: Codespace, version: str) -> Generator[Line, None, None]:
    lines_sql = (
        """SELECT route_id, route_short_name, route_long_name, route_desc, route_type, route_url, agency_id, route_color, route_text_color FROM routes;"""
    )

    with con.cursor() as cur:
        cur.execute(lines_sql)

        while True:
            row = cur.fetchone()
            if row is None:
                break

            (
                route_id,
                route_short_name,
                route_long_name,
                route_desc,
                route_type,
                route_url,
                agency_id,
                route_color,
                route_text_color,
            ) = row

            presentation = None
            if route_color is not None or route_text_color is not None:
                presentation = PresentationStructure(
                    colour=route_color,
                    text_colour=route_text_color,
                    background_colour=route_color,
                )

            agency_id = agency_id
            operator_ref = None
            if agency_id is not None:
                operator_ref = getFakeRef(get_agency_id(codespace, agency_id), OperatorRef, version)

            line = Line(
                id=get_route_id(codespace, route_id),
                version=version,
                name=getRequiredString(route_long_name, route_short_name),
                short_name=getOptionalString(route_short_name),
                description=getOptionalString(route_desc),
                transport_mode=gtfsRouteTypeToNeTEx(route_type),
                presentation=presentation,
                url=route_url,
                operator_ref=operator_ref,
                public_code=PublicCodeStructure(value=route_short_name) if route_short_name else None,
                private_codes=PrivateCodes(private_code=[PrivateCode(value=route_id, type_value="route_id")]),
            )

            yield line
