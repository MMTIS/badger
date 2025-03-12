import logging

from utils.aux_logging import prepare_logger, log_all, log_once
import random
import time
import zipfile

import folium
import pandas as pd
from folium.plugins import MarkerCluster
import traceback


# Generate a random dark color
def generate_random_dark_color() -> str:
    r = random.randint(0, 200)  # Random red component (0-128)
    g = random.randint(0, 200)  # Random green component (0-128)
    b = random.randint(0, 200)  # Random blue component (0-128)
    return "#%02x%02x%02x" % (r, g, b)


def main(gtfs_zip_file: str, map_file: str, limitation: int) -> None:
    limitation = int(limitation)
    # Read GTFS files using pandas
    # Read the GTFS files directly from the ZIP archive using pandas
    start_time = time.time()
    with zipfile.ZipFile(gtfs_zip_file, "r") as zip_ref:
        df_routes = pd.read_csv(
            zip_ref.open("routes.txt"), usecols=["route_id", "route_short_name"]
        )
        df_stops = pd.read_csv(
            zip_ref.open("stops.txt"),
            usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"],
        )
        df_trips = pd.read_csv(
            zip_ref.open("trips.txt"), usecols=["route_id", "trip_id", "trip_headsign"]
        )
        df_stop_times = pd.read_csv(
            zip_ref.open("stop_times.txt"),
            usecols=["trip_id", "stop_id", "stop_sequence"],
        )
    end_time = time.time()
    print("files read in " + str(round(end_time - start_time, 2)))

    # Create a map using Leaflet
    map_center = [
        47.368650,
        8.539183,
    ]  # df_stops['stop_lat'].mean(), df_stops['stop_lon'].mean()]
    m = folium.Map(location=map_center, zoom_start=16)
    start_time = time.time()
    print("basemap created in " + str(round(start_time - end_time, 2)))

    # Add markers for each stop - create a dictionary for fast stop lookups
    stops_dict = df_stops.set_index("stop_id")[["stop_lat", "stop_lon"]].T.to_dict(
        "list"
    )
    stop_dict_list = list(stops_dict.keys())
    stop_dict_list_len_range = range(len(stop_dict_list))
    stop_name_dict = df_stops.set_index("stop_id")[["stop_name"]].T.to_dict("list")
    stop_id_duplicates = []

    marker_cluster = MarkerCluster(
        name="Stops", overlay=True, control=True, show=True, icon_create_function=None
    )  # type: ignore
    marker_cluster.add_to(m)

    for i in stop_dict_list_len_range:
        if limitation and (i % limitation != 0):
            continue

        stop_id = stop_dict_list[i]
        (lat, lon) = stops_dict[stop_id]

        if stop_id in stop_id_duplicates:
            continue

        j = i + 1
        for j in stop_dict_list_len_range:
            # this loop ensures that we do not have duplicate markers (no "single parents")
            stop_id_inner = stop_dict_list[j]
            (lat_inner, lon_inner) = stops_dict[stop_id_inner]

            if (lat == lat_inner) and (lon == lon_inner):
                if stop_id == stop_id_inner:
                    continue
                elif stop_id != stop_id_inner:
                    if str(stop_id_inner).startswith("Parent"):
                        stop_id_duplicates.append(stop_id_inner)
                        break

        folium.Marker(
            location=[lat, lon],
            popup=str(stop_id) + ":" + str(stop_name_dict[stop_id]),  # stop_name
            name=str(stop_id) + ":" + str(stop_name_dict[stop_id]),  # as above
        ).add_to(marker_cluster)

    marker_cluster.add_to(m)

    end_time = time.time()
    print("markers added for each stop in " + str(round(end_time - start_time, 2)))

    # Create dictionaries for trips creation as well
    route_dict = df_routes.set_index("route_id")[["route_short_name"]].T.to_dict()

    trips_dict = (
        df_trips.groupby("route_id")["trip_id"]
        .agg(list)
        .reset_index()
        .set_index("route_id")["trip_id"]
        .to_dict()
    )

    trips_names_dict = (
        df_trips.groupby("route_id")["trip_headsign"]
        .agg(list)
        .reset_index()
        .set_index("route_id")["trip_headsign"]
        .to_dict()
    )

    stop_times_dict = (
        df_stop_times.groupby("trip_id")["stop_id"]
        .agg(list)
        .reset_index()
        .set_index("trip_id")["stop_id"]
        .to_dict()
    )

    # Add trips to map
    stop_coords_list: list[list[tuple[float, float]]] = []
    stop_coords_list_str: list[str] = []
    route_names: list[str] = []

    r = 0
    timer = 0
    for route_id in route_dict.keys():
        if r % 1000 == 0 and not limitation:
            print(str(r) + " of " + str(len(route_dict)))
            timer = int(time.time())

        route_name_dict = route_dict[route_id]

        if limitation and (r % limitation == 0):
            handle_trips_for_route(
                trips_dict,
                trips_names_dict,
                route_id,
                stop_times_dict,
                stops_dict,
                stop_coords_list,
                stop_coords_list_str,
                route_names,
                route_name_dict,
            )
        elif not limitation:
            handle_trips_for_route(
                trips_dict,
                trips_names_dict,
                route_id,
                stop_times_dict,
                stops_dict,
                stop_coords_list,
                stop_coords_list_str,
                route_names,
                route_name_dict,
            )

        r = r + 1
        if r % 1000 == 0 and not limitation:
            print("r in " + str(time.time() - timer))

    start_time = time.time()
    print("routes prepared in " + str(round(start_time - end_time, 2)))

    trips_group = folium.FeatureGroup(
        name="Trips", overlay=True, control=True, show=False
    ).add_to(m)

    for i in range(len(stop_coords_list)):
        p = folium.PolyLine(
            locations=stop_coords_list[i],
            tooltip=route_names[i],
            smooth_factor=10,
            color=generate_random_dark_color(),
        )  # type: ignore
        p.add_to(trips_group)

    folium.LayerControl().add_to(m)
    start_time = time.time()
    print("polylines created in: " + str(round(start_time - end_time, 2)))

    # Save the map to an HTML file
    m.save(map_file)
    print("map created in: " + str(round(time.time() - end_time, 2)))


def handle_trips_for_route(
    trips_dict: dict[str, list[str]],
    trips_names_dict: dict[str, list[str]],
    route_id: str,
    stop_times_dict: dict[str, list[str]],
    stops_dict: dict[str, tuple[float, float]],
    stop_coords_list: list[list[tuple[float, float]]],
    stop_coords_list_str: list[str],
    route_names: list[str],
    route_name_dict: dict[str, str],
) -> None:
    if trips_dict.get(route_id) is None:
        log_once(logging.ERROR, "mapping", f"No trips for route {route_id}")
        return
    trips = trips_dict.get(route_id)
    if trips is not None:
        for trip_id in trips:
            stop_coords: list[tuple[float, float]] = []
            trip_name = trips_names_dict[route_id][trips_dict[route_id].index(trip_id)]

            for stop_id in stop_times_dict[trip_id]:
                if stops_dict.get(stop_id) is None:
                    log_once(
                        logging.ERROR,
                        "no coordinates",
                        f"no coordinates available: {trip_id} - {stop_id}",
                    )
                else:
                    stop_coord: tuple[float, float] = stops_dict[stop_id]

                    if stop_coord:
                        stop_coords.append(stop_coord)

            # remove full duplicate lines or sub-lines we stringify the arrays for efficiency
            stop_coords_str = array_of_array_to_string(stop_coords)
            stop_coords_list_range = range(len(stop_coords_list))
            no_sub = False

            for i in stop_coords_list_range:
                stop_coords_list_i_str = stop_coords_list_str[i]

                if (stop_coords_str in stop_coords_list_i_str) or (
                    stop_coords_list_i_str in stop_coords_str
                ):
                    no_sub = True
                    break
            if not no_sub:
                stop_coords_list.append(stop_coords)
                stop_coords_list_str.append(array_of_array_to_string(stop_coords))
                if isinstance(trip_name, (int, float)):
                    route_names.append(route_name_dict["route_short_name"])
                else:
                    route_names.append(
                        str(route_name_dict["route_short_name"])
                        + " to "
                        + str(trip_name)
                    )


def array_of_array_to_string(array_of_arrays: list[tuple[float, float]]) -> str:
    return "".join(f"[{x}, {y}]" for x, y in array_of_arrays)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Writes an html file for GTFS with leaflet to show stops and trips"
    )
    parser.add_argument("gtfs_zip_file", type=str, help="GTFS zip file")
    parser.add_argument("map_file", type=str, help="output file (.html)")
    parser.add_argument(
        "--limitation",
        type=int,
        required=False,
        help="output every <argument> route (all trips of route)",
    )
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        if args.limitation:
            main(args.gtfs_zip_file, args.map_file, args.limitation)
        else:
            main(args.gtfs_zip_file, args.map_file, 1)
    except Exception as e:
        log_all(logging.ERROR, f"{e}" + traceback.format_exc())
        raise e
