from decimal import Decimal
from typing import Iterable, Generator, cast

import duckdb

from domain.gtfs.transform.string import getRequiredString, getOptionalString
from domain.netex.indexes.byid import getIndex
from domain.netex.model import (
    ScheduledStopPoint,
    Codespace,
    StopArea,
    PrivateCodeStructure,
    LocationStructure2,
    PrivateCodes,
    PrivateCode,
    PublicCodeStructure,
    StopAreaRefsRelStructure,
    StopAreaRefStructure,
)
from domain.netex.services.ids import getId
from domain.netex.services.refs import getFakeRef, getRef


def get_stop_id(codespace: Codespace, stop_id: str) -> str:
    if ':ScheduledStopPoint:' in stop_id:
        return stop_id
    elif ':Quay:' in stop_id:
        return stop_id.replace(':Quay:', ':ScheduledStopPoint:')
    else:
        return getId(codespace, ScheduledStopPoint, stop_id)


def getScheduledStopPoints(
    con: duckdb.DuckDBPyConnection, codespace: Codespace, version: str, stop_areas_input: Iterable[StopArea]
) -> Generator[ScheduledStopPoint, None, None]:
    ssp_sql = "SELECT DISTINCT stop_id, stop_name, stop_lat, stop_lon, stop_code, stop_desc, zone_id, stop_url, location_type, parent_station, wheelchair_boarding, stop_timezone, platform_code FROM stops WHERE location_type = 0 OR location_type IS NULL ORDER BY stop_id;"

    stop_areas: dict[object, StopArea] = getIndex(stop_areas_input)

    with con.cursor() as cur:
        cur.execute(ssp_sql)

        while True:
            row = cur.fetchone()
            if row is None:
                break

            (
                stop_id,
                stop_name,
                stop_lat,
                stop_lon,
                stop_code,
                stop_desc,
                zone_id,
                stop_url,
                location_type,
                parent_station,
                wheelchair_boarding,
                stop_timezone,
                platform_code,
            ) = row

            my_stop_areas = None
            if parent_station is not None:
                stop_area_ref = getId(codespace, StopArea, parent_station)
                if stop_area_ref not in stop_areas:
                    # TODO: Implement the logger here too
                    print(f"Parent {parent_station} not found, faking it.")
                    my_stop_areas = StopAreaRefsRelStructure(stop_area_ref=[getFakeRef(stop_area_ref, StopAreaRefStructure, version)])
                else:
                    my_stop_areas = StopAreaRefsRelStructure(
                        stop_area_ref=[cast(StopAreaRefStructure, getRef(stop_areas[stop_area_ref], StopAreaRefStructure))]
                    )

            scheduled_stop_point = ScheduledStopPoint(
                id=get_stop_id(codespace, stop_id),
                version=version,
                name=getRequiredString(stop_name),
                description=getOptionalString(stop_desc),
                private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_id, type_value="stop_id")]),
                short_stop_code=PrivateCodeStructure(value=platform_code, type_value='platform_code') if platform_code else None,
                public_code=PublicCodeStructure(value=stop_code) if stop_code is not None else None,
                url=stop_url,
                location=(
                    LocationStructure2(
                        longitude=Decimal(str(stop_lon)),
                        latitude=Decimal(str(stop_lat)),
                        srs_name="urn:ogc:def:crs:EPSG::4326",
                    )
                    if stop_lon is not None and stop_lat is not None
                    else None
                ),
                stop_areas=my_stop_areas,
            )

            yield scheduled_stop_point
