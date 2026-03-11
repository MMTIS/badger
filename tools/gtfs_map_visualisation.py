import logging
from dataclasses import dataclass

from folium import PolyLine
from pandas import DataFrame

from utils.aux_logging import prepare_logger, log_all
import random
import time
import zipfile

import folium
import pandas as pd
from folium.plugins import MarkerCluster
import traceback
import sys
import unittest

logger = logging.getLogger(__name__)
# logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)

GTFS_TRIPS_FILE_NAME = "trips.txt"
GTFS_STOPS_FILE_NAME = "stops.txt"
GTFS_STOP_TIMES_FILE_NAME = "stop_times.txt"

@dataclass
class Trip:
    trip_id: str
    trip_headsign: str
    route_id: str
    stop_coordinates: list[tuple[float, float]]
    stop_names: list[str]
    stop_ids: list[str]

class GtfsTripsAggregator:

    def __init__(self, df_trips: pd.DataFrame, df_stops: pd.DataFrame, df_stop_times: pd.DataFrame, max_routes : int = 10, route_id : str = None) -> None:
        self.max_routes = max_routes
        self.route_id = route_id
        self.df_trips = df_trips
        self.df_stops = df_stops
        self.df_stop_times = df_stop_times

    def aggregate_trips(self) -> list[Trip]:
        trips = []

        filtered_trips = self.filter(self.df_trips, self.df_stop_times, route_id=self.route_id, max_routes=self.max_routes)
        logger.debug("After all filters - remaining {} trips.".format(filtered_trips.index.size))
        for trip_id, trip_row in filtered_trips.iterrows():
            stop_coordinates = []
            stop_lons = []
            stop_lats = []
            stop_names = []
            stop_ids = []
            trip_stop_times = self.df_stop_times[self.df_stop_times["trip_id"] == trip_row["trip_id"]]
            trip_stop_times = pd.merge(trip_stop_times, self.df_stops, on="stop_id")
            for idx, stop_row in trip_stop_times.iterrows():
                stop_coordinates.append((stop_row["stop_lat"], stop_row["stop_lon"]))
                stop_lons.append(stop_row["stop_lon"])
                stop_lats.append(stop_row["stop_lat"])
                stop_names.append(stop_row["stop_name"])
                stop_ids.append(stop_row["stop_id"])

            trip = Trip(
                trip_id=str(trip_row["trip_id"]),
                trip_headsign=trip_row['trip_headsign'],
                route_id=trip_row['route_id'],
                stop_coordinates=stop_coordinates,
                stop_names=stop_names,
                stop_ids=stop_ids)

            trips.append(trip)

        return trips

    def filter(self, df_trips: DataFrame, df_stop_times: DataFrame, route_id: str = None, max_routes: int = 10) -> DataFrame:
        """
        Filters trips to show on the map.
        """
        if route_id is not None:
            filtered = self.filter_by_route(df_trips, route_id)
            logging.debug("Filtered by route_id=%s - remaining %d trips.",route_id, filtered.index.size)
        else:
            filtered = self.filter_by_number_of_routes(df_trips, max_routes)
            logging.debug("Filtered by max_routes=%d - remaining %d trips.", max_routes, filtered.index.size)
        filtered = self.filter_keeping_longest_trips_per_route(filtered, df_stop_times)
        logging.debug("Filtered keeping longest trips per route - remaining %d trips.", filtered.index.size)
        filtered = self.filter_keeping_one_trip_per_route(filtered)
        logging.debug("Filtered keeping one trip per route - remaining %d trips.", filtered.index.size)
        return filtered

    def filter_by_route(self, df: pd.DataFrame, route_id: str) -> pd.DataFrame:
        """
        Filters rows keeping only records with the given route_id.
        """
        return df[df["route_id"] == route_id]

    def filter_by_number_of_routes(self, df: DataFrame, max_routes: int) -> DataFrame:
        """
        Filters rows keeping only the given number of routes.
        """
        selected_routes = df['route_id'].drop_duplicates().head(max_routes)
        return df[df['route_id'].isin(selected_routes)].copy()

    def filter_keeping_longest_trips_per_route(self, df: DataFrame, df_stop_times: DataFrame) -> DataFrame:
        """
        Filters rows keeping only the longest trips of routes.
        """
        # get trip lengths
        trip_lengths = df_stop_times.groupby("trip_id")["stop_id"].count()
        df_trip_lengths = pd.DataFrame(trip_lengths.values, index=trip_lengths.index, columns=["length"])
        # create complete trips DataFrame with length column
        df_trips_with_length = pd.merge(df, df_trip_lengths, on="trip_id")
        df_trips_with_length["trip_id"] = df_trips_with_length.index
        # get max length of each route
        max_length_of_routes = df_trips_with_length.groupby("route_id")["length"].max()
        max_length_of_routes = pd.DataFrame(max_length_of_routes.values, index=max_length_of_routes.index, columns=["max_length"])
        # create complete trips DataFrame with length and max_length columns
        all = pd.merge(df_trips_with_length, max_length_of_routes, on="route_id")
        # filter
        return all[all['length'].eq(all['max_length'])]

    def filter_keeping_one_trip_per_route(self, df: DataFrame) -> DataFrame:
        """
        Filters rows keeping only one trip per route.
        """
        idx = df.groupby('route_id')['trip_id'].idxmin()
        return df.loc[idx].reset_index(drop=True)

class TripsMapGenerator:
    def __init__(self):
        pass

    def generate_random_dark_color(self) -> str:
        """
        Generates a color value of a random dark color.
        """
        r = random.randint(0, 200)  # Random red component (0-128)
        g = random.randint(0, 200)  # Random green component (0-128)
        b = random.randint(0, 200)  # Random blue component (0-128)
        return "#%02x%02x%02x" % (r, g, b)

    def generate_map(self, trips: list[Trip]) -> folium.Map:
        """
        Generates a folium map with all provided trips.
        """
        trips_map = folium.Map(zoom_start=16, tiles="OpenStreetMap")

        bounds = self.calculate_bounds(trips)
        logging.debug("Map bounds: %s", bounds)
        trips_map.fit_bounds(bounds, padding=(20, 20), max_zoom=16)

        lines_group = folium.FeatureGroup(
            name="Trips", overlay=True, control=True, show=True
        ).add_to(trips_map)

        for trip in trips:
            marker_cluster = self._create_stop_markers(trip)
            marker_cluster.add_to(trips_map)
            poly_line = self._create_poly_line(trip)
            poly_line.add_to(lines_group)
        return trips_map

    def calculate_bounds(self, trips: list[Trip]) -> tuple[tuple[float, float], tuple[float, float]]:
        """
        Calculates the bounds for the map (south-west and north-east corners), so that each marker is inside the map.
        """
        maximums = (0, 0)
        minimums = (180, 180)
        for trip in trips:
            for coord in trip.stop_coordinates:
                maximums = max(maximums, coord)
                minimums = min(minimums, coord)
        return maximums, minimums

    def _create_poly_line(self, trip: Trip) -> PolyLine:
        return folium.PolyLine(
            locations=trip.stop_coordinates,
            tooltip="route_id=<b>{}</b>".format(trip.trip_id)+"<br/>"+"head_sign={}".format(trip.trip_headsign),
            smooth_factor=10,
            color=self.generate_random_dark_color(),
        )

    def _create_stop_markers(self, trip: Trip) -> MarkerCluster:
        marker_cluster = MarkerCluster(
            name="Stops", overlay=True, control=True, show=True, icon_create_function=None
        )  # type: ignore
        for i in range(len(trip.stop_ids)):
            stop_label = "stop_id=<b>{}</b>\nstop_name={}".format(trip.stop_ids[i], trip.stop_names[i])
            folium.Marker(location=trip.stop_coordinates[i],
                          popup=stop_label,
                          name=stop_label).add_to(marker_cluster)
        return marker_cluster


def main(path_to_zip: str, max_routes: int = 10, route_id: str = None, debug: bool = False, map_file: str = "gtfs-map.html") -> None:
    """
    main function
    """
    configure_logging(debug=debug)
    start_time = time.time()

    with zipfile.ZipFile(path_to_zip, "r") as zip_ref:
        logging.debug("Reading from %s",path_to_zip)
        df_trips = pd.read_csv(zip_ref.open(GTFS_TRIPS_FILE_NAME), usecols=["route_id", "trip_id", "trip_headsign"],
                               index_col="trip_id")
        logging.debug("Got %d records from %s.", len(df_trips.index), GTFS_TRIPS_FILE_NAME)

        df_stops = pd.read_csv(zip_ref.open(GTFS_STOPS_FILE_NAME), usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"],
                               index_col="stop_id")
        logging.debug("Got %d records from %s.", len(df_stops.index), GTFS_STOPS_FILE_NAME)

        df_stop_times = pd.read_csv(zip_ref.open(GTFS_STOP_TIMES_FILE_NAME), usecols=["trip_id", "stop_id", "stop_sequence"])
        logging.debug("Got %d records from %s.", len(df_stop_times.index), GTFS_STOP_TIMES_FILE_NAME)

    trips = (GtfsTripsAggregator(df_trips, df_stops, df_stop_times, max_routes = max_routes, route_id = route_id)
             .aggregate_trips())

    logging.debug("Aggregated %d trip(s).", len(trips))

    m = TripsMapGenerator().generate_map(trips)
    # Save the map to an HTML file
    logging.debug("Saving map file to %s.", map_file)
    m.save(map_file)
    logging.info("Map created in %.2f seconds.", round(time.time() - start_time, 2))

def configure_logging(debug: bool = True) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        force=True,  # ensure reconfiguration even if something set a handler already
    )

def cli(argv=None):
    """
    Command line interface for gtfs_map_visualisation.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description="Writes an html file for GTFS with leaflet to show stops and trips"
    )
    parser.add_argument("gtfs_zip_file", type=str, help="GTFS zip file")
    parser.add_argument(
        "--map_file",
        type=str,
        required=False,
        default="gtfs-map.html",
        help="Output file (.html)")
    parser.add_argument(
        "--max_routes",
        type=int,
        default=10,
        required=False,
        help="Maximum number of routes to show on the map.",
    )
    parser.add_argument(
        "--route_id",
        type=str,
        required=False,
        help="route_id of route to show on the map.",
    )
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    parser.add_argument(
        "--debug",
        type=bool,
        help="Enable debug mode",
        required=False)
    args = parser.parse_args()
    prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.gtfs_zip_file,
             max_routes = args.max_routes,
             debug = args.debug,
             map_file = args.map_file,
             route_id = args.route_id)

    except Exception as e:
        log_all(logging.ERROR, f"{e}" + traceback.format_exc())
        raise e

class GtfsTripsAggregatorTest(unittest.TestCase):

    # Example 1 - one route with one trip
    single_trip_trips = {
        "trip_id": ["trip"],
        "route_id": ["route"],
        "trip_headsign": ["headsign"]
    }
    single_trip_stops = {
        "stop_id": ["stop1", "stop2"],
        "stop_name": ["stop-name1", "stop-name2"],
        "stop_lat": [1, 2],
        "stop_lon": [1, 2]
    }
    single_trip_stop_times = {
        "trip_id": ["trip", "trip"],
        "stop_id": ["stop1", "stop2"],
        "stop_sequence": [1, 2]
    }

    # Example 2 - route with two trips
    single_route_trips = {
        "trip_id": ["trip1", "trip2"],
        "route_id": ["route", "route"],
        "trip_headsign": ["headsign", "headsign"]
    }
    single_route_stops = {
        "stop_id": ["stop1", "stop2", "stop3"],
        "stop_name": ["stop-name1", "stop-name2", "stop-name3"],
        "stop_lat": [1, 2, 3],
        "stop_lon": [1, 2, 3]
    }
    single_route_stop_times = {
        "trip_id": ["trip1", "trip1", "trip2", "trip2", "trip2"],
        "stop_id": ["stop1", "stop2", "stop1", "stop2", "stop3"],
        "stop_sequence": [1, 2, 1, 2, 3]
    }

    # Example 3 - two routes
    many_routes_trips = {
        "trip_id": ["trip1", "trip2", "trip3"],
        "route_id": ["route1", "route1","route2"],
        "trip_headsign": ["headsign1", "headsign1","headsign2"]
    }
    many_routes_stops = {
        "stop_id": ["stop1", "stop2", "stop3", "stop4","stop5","stop6"],
        "stop_name": ["stop-name1", "stop-name2", "stop-name3","stop-name4", "stop-name5", "stop-name6"],
        "stop_lat": [1, 2, 3, 4 , 5 ,6],
        "stop_lon": [1, 2, 3, 4, 5, 6]
    }
    many_routes_stop_times = {
        "trip_id": ["trip1", "trip1", "trip2", "trip2", "trip2", "trip3","trip3","trip3"],
        "stop_id": ["stop1", "stop2", "stop1", "stop2", "stop3", "stop4","stop5","stop6"],
        "stop_sequence": [1, 2, 1, 2, 3, 1, 2, 3]
    }

    def test_single_trip_WHEN_aggregate_EXPECT_one_trip(self):
        trips = self._aggregate(self.single_trip_trips, self.single_trip_stops, self.single_trip_stop_times)
        self.assertEqual(len(trips), 1)
        self.assertEqual(trips[0].trip_headsign,"headsign")
        self.assertEqual(trips[0].trip_id, "trip")
        self.assertEqual(trips[0].route_id, "route")
        self.assertEqual(len(trips[0].stop_ids), 2)
        self.assertEqual(len(trips[0].stop_names), 2)

    def test_multiple_trips_WHEN_aggregate_EXPECT_one_trip(self):
        trips = self._aggregate(self.single_route_trips, self.single_route_stops, self.single_route_stop_times)
        self.assertEqual(len(trips), 1)

    def test_multiple_routes_WHEN_filter_by_route_with_one_trip_EXPECT_one_trip(self):
        trips = self._aggregate(self.many_routes_trips, self.many_routes_stops,
                                self.many_routes_stop_times, route_id="route2")
        self.assertEqual(len(trips), 1)

    def test_multiple_routes_WHEN_max_routes_1_EXPECT_one_trip(self):
        trips = self._aggregate(self.many_routes_trips, self.many_routes_stops,
                                self.many_routes_stop_times, max_routes=0)
        self.assertEqual(len(trips), 0)

    def test_multiple_routes_WHEN_max_routes_1_EXPECT_trip(self):
        trips = self._aggregate(self.many_routes_trips, self.many_routes_stops,
                                    self.many_routes_stop_times, max_routes=1)
        self.assertTrue(len(trips) > 0)

    def _aggregate(self, trips, stops, stop_times, route_id: str = None, max_routes: int = 10) -> list[Trip]:
        df_trips = pd.DataFrame(trips).set_index("trip_id")
        df_stops = pd.DataFrame(stops).set_index("stop_id")
        df_stop_times = pd.DataFrame(stop_times)
        return (GtfsTripsAggregator(df_trips, df_stops, df_stop_times, route_id=route_id, max_routes=max_routes)
                 .aggregate_trips())


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # No arguments -> run tests in this module
        # Using argv=[sys.argv[0]] avoids unittest trying to parse any args
        configure_logging(debug=True)
        unittest.main(argv=[sys.argv[0]])
    else:
        cli()