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
logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)

@dataclass
class Trip:
    trip_id: str
    trip_headsign: str
    route_id: str
    stop_coordinates: list[tuple[float, float]]
    stop_names: list[str]
    stop_ids: list[str]

class GtfsTripsAggregator:

    def __init__(self, df_trips: pd.DataFrame, df_stops: pd.DataFrame, df_stop_times: pd.DataFrame, limitation=0):
        self.limitation = limitation
        self.df_trips = df_trips
        self.df_stops = df_stops
        self.df_stop_times = df_stop_times

    def aggregate_trips(self) -> list[Trip]:
        trips = []

        filtered = self.filter_keeping_longest_trips_per_route()
        filtered = self.filter_keeping_one_trip_per_route(filtered)

        # TODO trips filtern, so dass nur der jeweils längste Trip pro Route übrig bleibt

        for trip_id, trip_row in filtered.iterrows():
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

    def filter_keeping_longest_trips_per_route(self) -> DataFrame:
        """
        Filters rows keeping only the longest trips of routes.
        """
        # get trip lengths
        trip_lengths = self.df_stop_times.groupby("trip_id")["stop_id"].count()
        df_trip_lengths = pd.DataFrame(trip_lengths.values, index=trip_lengths.index, columns=["length"])
        # create complete trips DataFrame with length column
        df_trips_with_length = pd.merge(self.df_trips, df_trip_lengths, on="trip_id")
        df_trips_with_length["trip_id"] = df_trips_with_length.index
        # get max length of each route
        max_length_of_routes = df_trips_with_length.groupby("route_id")["length"].max()
        max_length_of_routes = pd.DataFrame(max_length_of_routes.values, index=max_length_of_routes.index, columns=["max_length"])
        # create complete trips DataFrame with length and max_length columns
        all = pd.merge(df_trips_with_length, max_length_of_routes, on="route_id")
        # filter
        return all[all['length'].eq(all['max_length'])]

    def filter_keeping_one_trip_per_route(self, df_trips: DataFrame) -> DataFrame:
        """
        Filters rows keeping only one trip per route.
        """
        idx = df_trips.groupby('route_id')['trip_id'].idxmin()
        return df_trips.loc[idx].reset_index(drop=True)

class TripsMapGenerator:
    def __init__(self, limitation):
        self.limitation = limitation

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
            tooltip=trip.trip_headsign,
            smooth_factor=10,
            color=self.generate_random_dark_color(),
        )

    def _create_stop_markers(self, trip: Trip) -> MarkerCluster:
        marker_cluster = MarkerCluster(
            name="Stops", overlay=True, control=True, show=True, icon_create_function=None
        )  # type: ignore
        for i in range(len(trip.stop_ids)):
            stop_label = trip.stop_ids[i] + ":" + trip.stop_names[i]
            folium.Marker(location=trip.stop_coordinates[i],
                          popup=stop_label,
                          name=stop_label).add_to(marker_cluster)
        return marker_cluster


def main(path_to_zip: str, map_file: str, limitation: int) -> None:
    start_time = time.time()

    with zipfile.ZipFile(path_to_zip, "r") as zip_ref:
        df_trips = pd.read_csv(zip_ref.open("trips.txt"), usecols=["route_id", "trip_id", "trip_headsign"],
                               index_col="trip_id")
        df_stops = pd.read_csv(zip_ref.open("stops.txt"), usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"],
                               index_col="stop_id")
        df_stop_times = pd.read_csv(zip_ref.open("stop_times.txt"), usecols=["trip_id", "stop_id", "stop_sequence"])

    trips = (GtfsTripsAggregator(df_trips, df_stops, df_stop_times, limitation)
             .aggregate_trips())

    m = TripsMapGenerator(limitation).generate_map(trips)
    # Save the map to an HTML file
    m.save(map_file)
    print("map created in: " + str(round(start_time - time.time(), 2)))

def cli(argv=None):
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
    prepare_logger(logging.INFO, args.log_file)
    try:
        if args.limitation:
            main(args.gtfs_zip_file, args.map_file, args.limitation)
        else:
            main(args.gtfs_zip_file, args.map_file, 1)
    except Exception as e:
        log_all(logging.ERROR, f"{e}" + traceback.format_exc())
        raise e

class GtfsTripsAggregatorTest(unittest.TestCase):
    logger = logging.getLogger(__name__)

    def test_single_trip_WHEN_aggregate_EXPECT_trip(self):
        trips_data = {
            "trip_id": ["trip1"],
            "route_id": ["route1"],
            "trip_headsign": ["headsign1"]
        }
        stops_data = {
            "stop_id": ["stop1", "stop2"],
            "stop_name": ["stop-name1", "stop-name2"],
            "stop_lat": [1, 2],
            "stop_lon": [1, 2]
        }
        stop_times_data = {
            "trip_id": ["trip1", "trip1"],
            "stop_id": ["stop1", "stop2"],
            "stop_sequence": [1, 2]
        }

        trips = self._aggregate(trips_data, stops_data, stop_times_data)

        self.assertEqual(len(trips), 1)
        self.assertEqual(trips[0].trip_headsign,"headsign1")
        self.assertEqual(trips[0].trip_id, "trip1")
        self.assertEqual(trips[0].route_id, "route1")
        self.assertEqual(len(trips[0].stop_ids), 2)
        self.assertEqual(len(trips[0].stop_names), 2)

    def test_multiple_trips_WHEN_aggregate_EXPECT_one_trip(self):
        trips_data = {
            "trip_id": ["trip1","trip2"],
            "route_id": ["route","route"],
            "trip_headsign": ["headsign","headsign"]
        }
        stops_data = {
            "stop_id": ["stop1", "stop2","stop3"],
            "stop_name": ["stop-name1", "stop-name2","stop-name3"],
            "stop_lat": [1, 2, 3],
            "stop_lon": [1, 2, 3]
        }
        stop_times_data = {
            "trip_id": ["trip1", "trip1","trip2","trip2","trip2"],
            "stop_id": ["stop1", "stop2","stop1","stop2","stop3"],
            "stop_sequence": [1, 2, 1, 2, 3]
        }
        trips = self._aggregate(trips_data, stops_data, stop_times_data)
        self.assertEqual(len(trips), 1)


    def _aggregate(self, trips_data, stops_data, stop_times_data) -> list[Trip]:
        df_trips = pd.DataFrame(trips_data).set_index("trip_id")
        df_stops = pd.DataFrame(stops_data).set_index("stop_id")
        df_stop_times = pd.DataFrame(stop_times_data)
        return (GtfsTripsAggregator(df_trips, df_stops, df_stop_times, 0)
                 .aggregate_trips())


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # No arguments -> run tests in this module
        # Using argv=[sys.argv[0]] avoids unittest trying to parse any args
        unittest.main(argv=[sys.argv[0]])
    else:
        cli()