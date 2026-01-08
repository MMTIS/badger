# import datetime
# import hashlib
# from decimal import Decimal
#
# from typing import List, TypeVar, Any, Iterable, Generator, cast, Union
#
# import numpy
# from pandas._libs.missing import NAType
# import pandas as pd
# from xsdata.formats.dataclass.serializers import XmlSerializer
# from xsdata.models.datatype import XmlDateTime, XmlTime, XmlDate, XmlDuration
#
# from domain.gtfs.services.duckdb_to_storage import to_storage
# from domain.gtfs.transform.transporttype import gtfsRouteTypeToNeTEx
# from storage.mdbx.core.implementation import MdbxStorage
# from transformers.callsprofile import CallsProfile
#
#
# from pathlib import Path
#
# from utils.utils import get_interesting_classes, get_boring_classes
# from utils.refs import getRef, getIndex, getBitString2, getFakeRef, getOptionalString, getId, getRequiredString
# from utils.aux_logging import log_all, prepare_logger
# import logging
#
# T = TypeVar("T", bound=Any)
#
#
# def get_or_none(items: list[Any] | None, i: int, cast_clazz: type[T] | None = None) -> Any | None:
#     if items is None or i >= len(items) or isinstance(items[i], NAType):
#         return None
#
#     if cast_clazz is not None:
#         if not isinstance(cast_clazz, type) or cast_clazz is object:
#             raise TypeError(f"Invalid cast class: {cast_clazz}")
#
#         return cast_clazz(items[i])
#
#     return items[i]
#
#
# def gtfs_date(d: str) -> datetime.datetime:
#     return datetime.datetime(year=int(str(d)[0:4]), month=int(str(d)[4:6]), day=int(str(d)[6:8]))
#
#
# class GtfsNeTexProfile(CallsProfile):
#
#     def getCodespaceAndDataSource(self) -> tuple[Codespace, DataSource, Version, VersionFrameDefaultsStructure]:
#         feed_info_sql = """select * from feed_info limit 1;"""
#
#         with self.conn.cursor() as cur:
#             cur.execute(feed_info_sql)
#             df = cur.df()
#
#             short_name = self.getShortName(df['feed_publisher_name'][0])
#             codespace_name = short_name.replace(' ', '')
#
#             codespace = Codespace(
#                 id="{}:Codespace:{}".format(codespace_name, codespace_name),
#                 xmlns=codespace_name,
#                 xmlns_url=df['feed_publisher_url'][0],
#                 description=df['feed_publisher_name'][0],
#             )
#
#             start_date = datetime.datetime.combine(gtfs_date(df['feed_start_date'][0]), datetime.datetime.min.time())
#             end_date = datetime.datetime.combine(gtfs_date(df['feed_end_date'][0]), datetime.datetime.min.time())
#
#             version = Version(
#                 id="{}:Version:{}".format(codespace_name, df['feed_version'][0]),
#                 version=df['feed_version'][0] if df['feed_version'][0] not in ('', None) else str(datetime.date.today()).replace('-', ''),
#                 start_date=XmlDateTime.from_datetime(start_date),
#                 end_date=XmlDateTime.from_datetime(end_date),
#                 version_type=VersionTypeEnumeration.BASELINE,
#             )
#
#             data_source = DataSource(
#                 id="{}:DataSource:{}".format(codespace_name, codespace_name),
#                 version=version.version,
#                 name=MultilingualString(value=df['feed_publisher_name'][0]),
#                 short_name=MultilingualString(value=short_name),
#                 description=MultilingualString(value=df['feed_publisher_name'][0]),
#             )
#
#             frame_defaults = VersionFrameDefaultsStructure(
#                 default_codespace_ref=cast(CodespaceRefStructure, getRef(codespace, CodespaceRefStructure)),
#                 default_data_source_ref=cast(DataSourceRefStructure, getRef(data_source, DataSourceRefStructure)),
#                 default_locale=LocaleStructure(default_language=df['feed_lang'][0]),
#                 default_location_system="urn:ogc:def:crs:EPSG::4326",
#                 default_system_of_units=SystemOfUnits.SI_METRES,
#             )
#
#             return (codespace, data_source, version, frame_defaults)
#
#     def getResourceFrame(self, operators: list[Operator], id: str = "ResourceFrame") -> ResourceFrame:
#         resource_frame = ResourceFrame(id=getId(self.codespace, ResourceFrame,  id), version=self.version.version)
#         resource_frame.data_sources = DataSourcesInFrameRelStructure(data_source=[self.data_source])
#         # resource_frame.zones = ZonesInFrameRelStructure(transport_administrative_zone=[transport_administrative_zone])
#         resource_frame.organisations = OrganisationsInFrameRelStructure(
#             organisation_or_transport_organisation=cast(
#                 list[
#                     RetailConsortium
#                     | ServicedOrganisation
#                     | GeneralOrganisation
#                     | ManagementAgent
#                     | TravelAgent
#                     | OtherOrganisation
#                     | OnlineServiceOperator
#                     | Authority
#                     | Operator
#                 ],
#                 operators,
#             )
#         )
#         resource_frame.operational_contexts = OperationalContextsInFrameRelStructure(operational_context=self.getOperationalContexts())
#         # resource_frame.vehicle_types = VehicleTypesInFrameRelStructure(compound_train_or_train_or_vehicle_type=getVehicleTypes(codespace))
#         # resource_frame.vehicles = VehiclesInFrameRelStructure(train_element_or_vehicle=getVehicles(codespace))
#         return resource_frame
#
#     def getOperationalContexts(self) -> list[OperationalContext]:
#         operational_contexts = []
#
#         operational_contexts_sql = """select distinct route_type from routes;"""
#
#         with self.conn.cursor() as cur:
#             cur.execute(operational_contexts_sql)
#             df = cur.df()
#
#             for i in range(0, len(df['route_type'])):
#                 operational_context = OperationalContext(
#                     id=getId(self.codespace, OperationalContext,  df['route_type'][i]),
#                     version=self.version.version,
#                     vehicle_mode=gtfsRouteTypeToNeTEx(df['route_type'][i]),
#                 )
#                 operational_contexts.append(operational_context)
#
#         return operational_contexts
#
#     # TODO: implement
#
#     def get_shape_id(self, shape_id: str) -> str:
#         if ':Route:' in shape_id:
#             return shape_id
#         else:
#             return getId(self.codespace, Route, shape_id)
#
#     def get_shape_id_lsp(self, shape_id: str) -> str:
#         if ':Route:' in shape_id:
#             return shape_id.replace(':Route:', ':LinkSequenceProjection:')
#         else:
#             return getId( self.codespace, LinkSequenceProjection, shape_id)
#
#     #
#     # def getPaths(self):
#     #     pl = PathLink()
#     #
#     #
#     def getRoutes(self) -> tuple[list[Route], list[RoutePoint], list[RouteLink]]:
#         lines = getIndex(self.lines)
#
#         shape_route_mapping = {}
#
#         # Within NeTEx it is not possible to have a route (GTFS-shape) pointing to multiple lines (GTFS-route)
#         shape_route_sql = """select distinct shape_id, array_agg(distinct route_id) as route_ids from trips where shape_id is not null group by shape_id;"""
#         with self.conn.cursor() as cur:
#             cur.execute(shape_route_sql)
#
#             df = cur.df()
#             shape_ids = df.get('shape_id')
#             route_ids = df.get('route_ids')
#
#             for i in range(0, len(shape_ids)):
#                 if len(route_ids[i]) > 0:
#                     shape_route_mapping[shape_ids[i]] = [(shape_ids[i] + '-' + x, x) for x in route_ids[i]]
#                 else:
#                     # Stale route, why should we add them at all?
#                     shape_route_mapping[shape_ids[i]] = [
#                         (
#                             shape_ids[i],
#                             None,
#                         )
#                     ]
#
#         shape_sql = """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
#
#         routes = {}
#         route_points = []
#         route_links = []
#
#         with self.conn.cursor() as cur:
#             cur.execute(shape_sql)
#
#             df = cur.df()
#             shape_ids = df.get('shape_id')
#             shape_pt_lats = df.get('shape_pt_lat')
#             shape_pt_lons = df.get('shape_pt_lon')
#             shape_pt_sequences = df.get('shape_pt_sequence')
#             shape_dist_traveleds = df.get('shape_dist_traveled')
#
#             prev_order = 1
#             prev_route = None
#             prev_distance = 0
#             prev_route_point = None
#             prev_shape_id = None
#
#             for i in range(0, len(shape_ids)):
#                 route_id = self.get_shape_id(shape_ids[i])
#                 route_point_id = route_id.replace(":Route:", ":RoutePoint:")
#                 route_link_id = route_id.replace(":Route:", ":RouteLink:")
#
#                 route_point = RoutePoint(
#                     id=f"{route_point_id}-{shape_pt_sequences[i]}",
#                     version=self.version.version,
#                     location=LocationStructure2(
#                         longitude=Decimal(str(shape_pt_lons[i])), latitude=Decimal(str(shape_pt_lats[i])), srs_name="urn:ogc:def:crs:EPSG::4326"
#                     ),
#                 )
#                 route_points.append(route_point)
#
#                 if shape_ids[i] == prev_shape_id:
#                     assert prev_route_point is not None and prev_route is not None
#
#                     # It is the same route, and still being extended
#                     distance = None
#                     if shape_dist_traveleds[i]:
#                         distance = shape_dist_traveleds[i] - prev_distance
#
#                     route_link = RouteLink(
#                         id=f"{route_link_id}-{shape_pt_sequences[i]}",
#                         version=self.version.version,
#                         from_point_ref=cast(RoutePointRefStructure, getRef(prev_route_point)),
#                         to_point_ref=cast(RoutePointRefStructure, getRef(route_point)),
#                         distance=Decimal(str(distance)),
#                     )
#                     route_links.append(route_link)
#
#                     for route in prev_route:
#                         assert route.points_in_sequence is not None
#                         route.points_in_sequence.point_on_route[-1].onward_route_link_ref = cast(RouteLinkRefStructure, getRef(route_link))
#
#                 else:
#                     prev_order = 1
#                     prev_route = []
#                     prev_distance = 0
#                     prev_route_point = None
#                     prev_shape_id = None
#
#                     route_ids = shape_route_mapping.get(shape_ids[i], [(shape_ids[i], None)])
#                     for route_id, line_id in route_ids:
#                         route = Route(id=self.get_shape_id(route_id), version=self.version.version)
#                         route.private_code = PrivateCode(value=shape_ids[i], type_value="shape_id")
#                         route.points_in_sequence = PointsOnRouteRelStructure()
#                         if line_id:
#                             line = lines[self.get_route_id(line_id)]  # TODO: Validate
#                             route.line_ref = cast(Union[FlexibleLineRef, LineRef], getRef(line, LineRef))
#
#                         routes[route_id] = route
#                         prev_route.append(route)
#
#                 for route in prev_route:
#                     route_point_id = self.get_shape_id(route_id).replace(":Route:", ":RoutePoint:")
#
#                     point_on_route = PointOnRoute(
#                         id=f"{route_point_id}-{shape_pt_sequences[i]}",
#                         version=self.version.version,
#                         order=prev_order,
#                         point_ref_or_infrastructure_point_ref_or_activation_point_ref_or_timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref_or_route_point_ref=cast(
#                             RoutePointRef, getRef(route_point, RoutePointRef)
#                         ),
#                     )  # shape_pt_sequence is non-negative integer
#
#                     assert route.points_in_sequence is not None
#                     route.points_in_sequence.point_on_route.append(point_on_route)
#
#                 prev_shape_id = shape_ids[i]
#                 prev_route_point = route_point
#                 prev_distance = shape_dist_traveleds[i]
#                 prev_order += 1
#
#         return (list(routes.values()), route_points, route_links)
#
#     def getLineStrings(
#         self,
#         shape_sql: dict[str, Any] = {
#             'query': (
#                 """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
#             )
#         },
#     ) -> List[LinkSequenceProjection]:
#         link_sequence_projection = []
#
#         with self.conn.cursor() as cur:
#             cur.execute(**shape_sql)
#
#             df = cur.df()
#             shape_ids = df.get('shape_id')
#             shape_pt_lats = df.get('shape_pt_lat')
#             shape_pt_lons = df.get('shape_pt_lon')
#             # shape_pt_sequences = df.get('shape_pt_sequence')
#             shape_dist_traveleds = df.get('shape_dist_traveled')
#
#             prev_distance: int | None = 0
#             prev_shape_id = None
#             pos_list = []
#
#             for i in range(0, len(shape_ids)):
#                 if shape_ids[i] != prev_shape_id and prev_shape_id is not None:
#                     de_distance = None
#                     if prev_distance is not None and not numpy.isnan(prev_distance):
#                         de_distance = Decimal(prev_distance)
#
#                     link_sequence_projection_id = self.get_shape_id_lsp(prev_shape_id)
#                     pos_list_result = PosList(value=pos_list)  # type: ignore
#                     link_sequence_projection.append(
#                         LinkSequenceProjection(
#                             id=link_sequence_projection_id,
#                             version=self.version.version,
#                             distance=de_distance,
#                             points_or_line_string=LineString(
#                                 id=link_sequence_projection_id.replace(':', "_"),
#                                 srs_name="urn:ogc:def:crs:EPSG::4326",
#                                 srs_dimension=2,
#                                 pos_or_point_property_or_pos_list=[pos_list_result],
#                             ),
#                         )
#                     )
#                     pos_list = []
#                     prev_distance = 0
#
#                 pos_list += [Decimal(str(shape_pt_lats[i])), Decimal(str(shape_pt_lons[i]))]
#
#                 prev_shape_id = shape_ids[i]
#                 prev_distance = get_or_none(shape_dist_traveleds, i)
#
#             if len(pos_list) > 0:
#                 de_distance = None
#                 if prev_distance is not None and not numpy.isnan(prev_distance):
#                     de_distance = Decimal(str(prev_distance))
#
#                 result_pos_list = PosList(value=pos_list)  # type: ignore
#
#                 if prev_shape_id:
#                     link_sequence_projection_id = self.get_shape_id_lsp(prev_shape_id)
#                     link_sequence_projection.append(
#                         LinkSequenceProjection(
#                             id=link_sequence_projection_id,
#                             version=self.version.version,
#                             distance=de_distance,
#                             points_or_line_string=LineString(
#                                 id=link_sequence_projection_id.replace(":", "_"),
#                                 srs_name="urn:ogc:def:crs:EPSG::4326",
#                                 srs_dimension=2,
#                                 pos_or_point_property_or_pos_list=[result_pos_list],
#                             ),
#                         )
#                     )
#
#         return link_sequence_projection
#
#     def getServiceFrame(
#         self, lines: list[Line], stop_areas: list[StopArea], scheduled_stop_points: list[ScheduledStopPoint], id: str = "ServiceFrame"
#     ) -> ServiceFrame:
#         if lines is None:
#             lines = self.lines
#
#         if stop_areas is None:
#             stop_areas = self.stop_areas
#
#         if scheduled_stop_points is None:
#             scheduled_stop_points = self.scheduled_stop_points
#
#         service_frame = ServiceFrame(id=getId(self.codespace, ServiceFrame, id), version=self.version.version)
#         # service_frame.prerequisites.resource_frame_ref
#         # setIdVersion(service_frame, self.codespace, "ServiceFrame", self.version)
#         service_frame.lines = LinesInFrameRelStructure(line=cast(list[FlexibleLine | Line], lines))
#
#         stop_areas = sorted(stop_areas, key=lambda x: x.id or -1)
#         if stop_areas:
#             service_frame.stop_areas = StopAreasInFrameRelStructure(stop_area=stop_areas)
#
#         scheduled_stop_points = sorted(scheduled_stop_points, key=lambda x: x.id or -1)
#         service_frame.scheduled_stop_points = ScheduledStopPointsInFrameRelStructure(scheduled_stop_point=scheduled_stop_points)
#
#         #     """
#         #     destination_displays = getDestinationDisplays(codespace, version)
#         #     stop_areas = getStopAreas(codespace, version)
#         #     scheduled_stop_points, passenger_stop_assignments = getScheduledStopPoints(codespace, version, route_points,
#         #                                                                                stop_areas)
#         #     service_journey_patterns, timing_links, service_journey_patterns_transport_mode = getServiceJourneyPatterns(
#         #         codespace, version, routes, lines, scheduled_stop_points, operational_contexts, destination_displays)
#         #
#         #     service_journey_patterns = sorted(service_journey_patterns, key=lambda x: x.id)
#         #     timing_links = sorted(timing_links, key=lambda x: x.id)
#         #     """
#         #
#         #     self.stop_areas = sorted(self.stop_areas, key=lambda x: x.id)
#         #     self.scheduled_stop_points = sorted(self.scheduled_stop_points, key=lambda x: x.id)
#         #
#         #     """
#         #     destination_displays = sorted(destination_displays, key=lambda x: x.id)
#         #     passenger_stop_assignments = sorted(passenger_stop_assignments, key=lambda x: x.id)
#         #
#         #     """
#         #     self.route_points = sorted(self.route_points, key=lambda x: x.id)
#         #     service_frame.route_points = RoutePointsInFrameRelStructure(route_point=self.route_points)
#         #
#         #     self.route_links = sorted(self.route_links, key=lambda x: x.id)
#         #     service_frame.route_links = RouteLinksInFrameRelStructure(route_link=self.route_links)
#         #
#         #    self.routes = sorted(self.routes, key=lambda x: x.id)
#         #    service_frame.routes = RoutesInFrameRelStructure(route=self.routes)
#         #
#         #     if self.stop_areas:
#         #         service_frame.stop_areas = StopAreasInFrameRelStructure(stop_area=self.stop_areas)
#         #
#         #     service_frame.scheduled_stop_points = ScheduledStopPointsInFrameRelStructure(
#         #         scheduled_stop_point=self.scheduled_stop_points)
#         #
#         #     if self.timing_links:
#         #         service_frame.timing_links = TimingLinksInFrameRelStructure(timing_link=self.timing_links)
#         #
#         #     if self.time_demand_types:
#         #         self.time_demand_types = sorted(self.time_demand_types, key=lambda x: x.id)
#         #         service_frame.time_demand_types = TimeDemandTypesInFrameRelStructure(time_demand_type=self.time_demand_types)
#         #
#         #     """
#         #     service_frame.destination_displays = DestinationDisplaysInFrameRelStructure(
#         #         destination_display=destination_displays)
#         #     service_frame.stop_assignments = StopAssignmentsInFrameRelStructure(
#         #         passenger_stop_assignment=passenger_stop_assignments)
#         #     """
#         #
#         #     if self.service_journey_patterns:
#         #         service_frame.journey_patterns = JourneyPatternsInFrameRelStructure(choice=self.service_journey_patterns)
#         #
#         #
#         return service_frame
#
#     def get_service_id_ac(self, service_id: str) -> str:
#         if ':AvailabilityCondition:' in service_id:
#             return service_id
#         elif ':DayType:' in service_id:
#             return service_id.replace(':DayType:', ':AvailabilityCondition:')
#         else:
#             return getId(self.codespace, AvailabilityCondition, service_id)
#
#     def getAvailabilityConditions(
#         self,
#         availability_condition_sql: dict[str, str] = {'query': """select * from calendar order by service_id;"""},
#         exceptions_sql: dict[str, str] = {
#             'query': """select service_id, exception_type, array_agg(date order by date) as dates from calendar_dates group by service_id, exception_type;"""
#         },
#     ) -> list[AvailabilityCondition]:
#         availability_conditions = []
#
#         with self.conn.cursor() as cur:
#             cur.execute(**exceptions_sql)
#             exceptions_df = cur.df()
#             exceptions: dict[str, list[AvailabilityCondition]] = {}
#
#             service_ids = exceptions_df.get('service_id')
#             for i in range(0, len(service_ids)):
#                 exception_type = int(exceptions_df['exception_type'][i])
#                 if exception_type in (1, 2):
#                     ac = AvailabilityCondition(
#                         id=self.get_service_id_ac(service_ids[i]) + '_' + str(exception_type),
#                         private_codes=PrivateCodes(private_code=[PrivateCode(type_value="service_id", value=service_ids[i])]),
#                         version=self.version.version,
#                         is_available=exception_type == 1,
#                         from_date=date_to_xmldatetime(gtfs_date(exceptions_df['dates'][i][0])),
#                         to_date=date_to_xmldatetime(gtfs_date(exceptions_df['dates'][i][-1])),
#                         valid_day_bits=getBitString2([gtfs_date(d) for d in exceptions_df['dates'][i]]),
#                     )
#                     exception = exceptions.get(service_ids[i], [])
#                     exception.append(ac)
#                     exceptions[service_ids[i]] = exception
#                     availability_conditions.append(ac)
#
#             cur.execute(**availability_condition_sql)
#             df = cur.df()
#
#             service_ids = df.get('service_id')
#             mondays = df.get('monday')
#             tuesdays = df.get('tuesday')
#             wednesdays = df.get('wednesday')
#             thursdays = df.get('thursday')
#             fridays = df.get('friday')
#             saturdays = df.get('saturday')
#             sundays = df.get('sunday')
#             start_dates = df.get('start_date')
#             end_dates = df.get('end_date')
#
#             for i in range(0, len(service_ids)):
#                 days_of_week = []
#                 if mondays[i] == 1:
#                     days_of_week.append(DayOfWeekEnumeration.MONDAY)
#                 if tuesdays[i] == 1:
#                     days_of_week.append(DayOfWeekEnumeration.TUESDAY)
#                 if wednesdays[i] == 1:
#                     days_of_week.append(DayOfWeekEnumeration.WEDNESDAY)
#                 if thursdays[i] == 1:
#                     days_of_week.append(DayOfWeekEnumeration.THURSDAY)
#                 if fridays[i] == 1:
#                     days_of_week.append(DayOfWeekEnumeration.FRIDAY)
#                 if saturdays[i] == 1:
#                     days_of_week.append(DayOfWeekEnumeration.SATURDAY)
#                 if sundays[i] == 1:
#                     days_of_week.append(DayOfWeekEnumeration.SUNDAY)
#
#                 availability_conditions.append(
#                     AvailabilityCondition(
#                         id=self.get_service_id_ac(service_ids[i]),
#                         version=self.version.version,
#                         private_codes=PrivateCodes(private_code=[PrivateCode(type_value="service_id", value=service_ids[i])]),
#                         is_available=True,
#                         from_date=date_to_xmldatetime(gtfs_date(start_dates[i])),
#                         to_date=date_to_xmldatetime(gtfs_date(end_dates[i])),
#                         day_types=DayTypesRelStructure(
#                             day_type_ref_or_day_type=[
#                                 DayType(
#                                     id=self.get_service_id_dt(service_ids[i]),
#                                     version=self.version.version,
#                                     properties=PropertiesOfDayRelStructure(property_of_day=[PropertyOfDay(days_of_week=days_of_week)]),
#                                 )
#                             ]
#                         ),
#                     )
#                 )
#
#         return availability_conditions
#
#     def getInterchangeRules(
#         self,
#         transfers_sql: dict[str, str] = {
#             'query': (
#                 """select transfers.*, from_stop.location_type as from_stop_location_type, to_stop.location_type as to_stop_location_type from transfers join stops as from_stop on (from_stop_id = from_stop.stop_id) join stops as to_stop on (to_stop_id = to_stop.stop_id) order by from_route_id, to_route_id, from_trip_id, to_trip_id, from_stop_id, to_stop_id;"""
#             )
#         },
#     ) -> Generator[InterchangeRule, None, None]:
#         # from_stop_id, to_stop_id, from_route_id, to_route_id, from_trip_id, to_trip_id, transfer_type, min_transfer_time:
#         with self.conn.cursor() as cur:
#             cur.execute(**transfers_sql)
#             transfers_df = cur.df()
#
#             from_stop_ids = transfers_df.get('from_stop_id')
#             from_stop_location_types = transfers_df.get('from_stop_location_type')
#             to_stop_ids = transfers_df.get('to_stop_id')
#             to_stop_location_types = transfers_df.get('to_stop_location_type')
#             from_route_ids = transfers_df.get('from_route_id')
#             to_route_ids = transfers_df.get('to_route_id')
#             from_trip_ids = transfers_df.get('from_trip_id')
#             to_trip_ids = transfers_df.get('from_trip_id')
#             transfer_types = transfers_df.get('transfer_type')
#             min_transfer_times = transfers_df.get('min_transfer_time')
#
#             for i in range(0, len(transfer_types)):
#                 feeder_filter = InterchangeRuleParameterStructure(
#                     scheduled_stop_point_ref=(
#                         getFakeRef(getId(self.codespace, ScheduledStopPoint, from_stop_ids[i]), ScheduledStopPointRef, self.version.version)
#                         if from_stop_location_types[i] == 0
#                         else None
#                     ),
#                     stop_place_ref=(
#                         getFakeRef(getId(self.codespace, StopPlace, from_stop_ids[i]), StopPlaceRef, self.version.version)
#                         if from_stop_location_types[i] == 1
#                         else None
#                     ),
#                     all_lines_or_lines_in_direction_refs_or_line_in_direction_ref=(
#                         [
#                             LineInDirectionRef(line_ref=getFakeRef(getId(self.codespace, Line, from_route_ids[i]), LineRef, self.version.version)),
#                         ]
#                         if from_route_ids[i]
#                         else [EmptyType2(value='')]
#                     ),
#                     service_journey_ref_or_journey_designator_or_service_designator=(
#                         getFakeRef(getId(self.codespace, ServiceJourney, from_trip_ids[i]), ServiceJourneyRefStructure, self.version.version)
#                         if from_trip_ids[i]
#                         else None
#                     ),
#                 )
#
#                 distributor_filter = InterchangeRuleParameterStructure(
#                     scheduled_stop_point_ref=(
#                         getFakeRef(getId(self.codespace, ScheduledStopPoint, to_stop_ids[i]), ScheduledStopPointRef, self.version.version)
#                         if to_stop_location_types[i] == 0
#                         else None
#                     ),
#                     stop_place_ref=(
#                         getFakeRef(getId(self.codespace, StopPlace, to_stop_ids[i]), StopPlaceRef, self.version.version)
#                         if to_stop_location_types[i] == 1
#                         else None
#                     ),
#                     all_lines_or_lines_in_direction_refs_or_line_in_direction_ref=(
#                         [
#                             LineInDirectionRef(line_ref=getFakeRef(getId(self.codespace, Line, to_route_ids[i]), LineRef, self.version.version)),
#                         ]
#                         if to_route_ids[i]
#                         else [EmptyType2(value='')]
#                     ),
#                     service_journey_ref_or_journey_designator_or_service_designator=(
#                         getFakeRef(getId(self.codespace, ServiceJourney, to_trip_ids[i]), ServiceJourneyRefStructure, self.version.version)
#                         if to_trip_ids[i]
#                         else None
#                     ),
#                 )
#
#                 id = getId(self.codespace,
#                     InterchangeRule,
#                     hashlib.md5(
#                         (
#                             ';'.join(
#                                 [
#                                     str(from_stop_ids[i]),
#                                     str(to_stop_ids[i]),
#                                     str(from_route_ids[i]),
#                                     str(to_route_ids[i]),
#                                     str(from_trip_ids[i]),
#                                     str(to_trip_ids[i]),
#                                     str(transfer_types[i]),
#                                     str(min_transfer_times[i]),
#                                 ]
#                             )
#                         ).encode('utf-8')
#                     ).hexdigest()[0:5],
#                 )
#
#                 if pd.isna(transfer_types[i]) or transfer_types[i] == 0:
#                     # Recommended
#                     yield InterchangeRule(
#                         advertised=True, id=id, version=self.version.version, feeder_filter=feeder_filter, distributor_filter=distributor_filter
#                     )
#
#                 elif transfer_types[i] == 1:
#                     # Timed transfer point between two routes
#                     yield InterchangeRule(
#                         advertised=True,
#                         planned=True,
#                         guaranteed=True,
#                         id=id,
#                         version=self.version.version,
#                         feeder_filter=feeder_filter,
#                         distributor_filter=distributor_filter,
#                     )
#
#                 elif transfer_types[i] == 2:
#                     # Transfer requires a minimum amount of time
#                     yield InterchangeRule(
#                         advertised=True,
#                         planned=True,
#                         minimum_transfer_time=XmlDuration(value=f"PT{int(min_transfer_times[i])}S") if min_transfer_times[i] else None,
#                         id=id,
#                         version=self.version.version,
#                         feeder_filter=feeder_filter,
#                         distributor_filter=distributor_filter,
#                     )
#
#                 elif transfer_types[i] == 3:
#                     # Transfers are not possible
#                     yield InterchangeRule(exclude=True, id=id, version=self.version.version, feeder_filter=feeder_filter, distributor_filter=distributor_filter)
#
#                 elif transfer_types[i] == 4:
#                     # In-seat transfer
#                     yield InterchangeRule(
#                         advertised=True,
#                         planned=True,
#                         stay_seated=True,
#                         guaranteed=True,
#                         id=id,
#                         version=self.version.version,
#                         feeder_filter=feeder_filter,
#                         distributor_filter=distributor_filter,
#                     )
#
#                 elif transfer_types[i] == 5:
#                     # In-seat transfer not allowed
#                     yield InterchangeRule(
#                         stay_seated=False, id=id, version=self.version.version, feeder_filter=feeder_filter, distributor_filter=distributor_filter
#                     )
#
#     @staticmethod
#     def gtfs_shape_to_linestring(self, shape_sql: dict[str, str] = {'query': """select * from shapes order by shape_id;"""}) -> None:
#
#         with self.conn.cursor() as cur:
#             cur.execute(**shape_sql)
#
#     def getServiceJourneys(
#         self,
#         availability_conditions_input: list[AvailabilityCondition],
#         trips_sql: dict[str, str] = {'query': """select * from trips where trip_id not in (select trip_id from frequencies) order by trip_id;"""},
#         stop_times_sql: dict[str, str] = {'query': """select * from stop_times order by trip_id, stop_sequence;"""},
#     ) -> Generator[ServiceJourney, None, None]:
#         service_journey: ServiceJourney | None = None
#
#         availability_conditions = getIndex(availability_conditions_input)
#
#         service_journeys = {}
#         shape_used = set([])
#
#         with self.conn.cursor() as cur:
#             cur.execute(**trips_sql)
#
#             df = cur.df()
#
#             route_ids = df.get('route_id')
#             trip_ids = df.get('trip_id')
#             service_ids = df.get('service_id')
#             trip_short_names = df.get('trip_short_name')
#             trip_headsigns = df.get('trip_headsign')
#             # route_short_names = df.get('route_short_name')
#             direction_ids = df.get('direction_id')
#             block_ids = df.get('block_id')
#             shape_ids = df.get('shape_id')
#             wheelchair_accessibles = df.get('wheelchair_accessible')
#             # trip_bikes_alloweds = df.get('trip_bikes_allowed')
#             bikes_alloweds = df.get('bikes_allowed')
#             # ticketing_trip_ids = df.get('ticketing_trip_id')
#             # ticketing_types = df.get('ticketing_type')
#
#             for i in range(0, len(route_ids)):
#                 availability_condition_key = self.get_service_id_ac(service_ids[i])
#
#                 availability_conditions_journey = [
#                     availability_conditions.get(availability_condition_key, None),
#                     availability_conditions.get(availability_condition_key + "_1", None),
#                     availability_conditions.get(availability_condition_key + "_2", None),
#                 ]
#
#                 journey_pattern_view = None
#                 if trip_headsigns[i] is not None:
#                     journey_pattern_view = JourneyPatternView(
#                         destination_display_ref_or_destination_display_view=DestinationDisplayView(
#                             name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
#                         )
#                     )
#
#                 accessibility_assessment = None
#                 if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
#                     accessibility_assessment = AccessibilityAssessment(
#                         id=self.get_trip_id_aa(trip_ids[i]),
#                         version=self.version.version,
#                         mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
#                     )
#
#                 block_ref = None
#                 if block_ids[i] is not None:
#                     block_ref = getFakeRef(getId(self.codespace, Block, block_ids[i]), BlockRef, None, "EXTERNAL")
#
#                 # route_ref = None
#                 lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
#                 shape_id = get_or_none(shape_ids, i)
#                 if shape_id is not None:
#                     if shape_id in shape_used:
#                         lsp = getFakeRef(self.get_shape_id_lsp(shape_id), LinkSequenceProjectionRef, self.version.version)
#                     else:
#                         lsps = self.getLineStrings(
#                             {
#                                 'query': (
#                                     """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
#                                 ),
#                                 'parameters': (shape_id,),
#                             }
#                         )
#                         if len(lsps) > 0:
#                             lsp = lsps[0]
#
#                         shape_used.add(shape_id)
#
#                 luggage_carriage_facility_list = []
#                 facitities = None
#                 bikes_allowed = get_or_none(bikes_alloweds, i)
#                 if bikes_allowed is not None:
#                     if bikes_allowed == 1:
#                         luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
#                     elif bikes_allowed == 2:
#                         luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)
#
#                 if len(luggage_carriage_facility_list) > 0:
#                     facitities = ServiceFacilitySetsRelStructure(
#                         restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
#                             ServiceFacilitySet(
#                                 id=self.get_trip_id_sfs(trip_ids[i]),
#                                 version=self.version.version,
#                                 luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
#                             )
#                         ]
#                     )
#
#                 service_journey = ServiceJourney(
#                     id=self.get_trip_id(trip_ids[i]),
#                     version=self.version.version,
#                     flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(self.get_route_id(route_ids[i]), LineRef, self.version.version),
#                     private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
#                     short_name=getOptionalString(get_or_none(trip_short_names, i)),
#                     validity_conditions_or_valid_between=[
#                         ValidityConditionsRelStructure(
#                             choice=cast(
#                                 list[
#                                     Union[
#                                         AvailabilityConditionRef,
#                                         ValidityRuleParameterRef,
#                                         ValidityTriggerRef,
#                                         ValidityConditionRef,
#                                         ValidBetween,
#                                         SimpleAvailabilityCondition,
#                                         ValidDuring,
#                                         AvailabilityCondition,
#                                         ValidityRuleParameter,
#                                         ValidityTrigger,
#                                         ValidityCondition,
#                                     ]
#                                 ],
#                                 [getRef(x, AvailabilityConditionRef) for x in availability_conditions_journey if x is not None],
#                             )
#                         )
#                     ],
#                     journey_pattern_view=journey_pattern_view,
#                     direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
#                     block_ref=block_ref,
#                     accessibility_assessment=accessibility_assessment,
#                     facilities=facitities,
#                     link_sequence_projection_ref_or_link_sequence_projection=lsp,
#                 )
#
#                 service_journeys[trip_ids[i]] = service_journey
#
#         with self.conn.cursor() as cur:
#             cur.execute(**stop_times_sql)
#             trip_id = None
#             service_journey = None
#             prev_call = None
#             prev_shape_traveled = 0
#             prev_order = 1
#
#             df = cur.df()
#
#             trip_ids = df.get('trip_id')
#             stop_headsigns = df.get('stop_headsign')
#             stop_ids = df.get('stop_id')
#             arrival_times = df.get('arrival_time')
#             departure_times = df.get('departure_time')
#             shape_dist_traveleds = df.get('shape_dist_traveled')
#             drop_off_types = df.get('drop_off_type')
#             pickup_types = df.get('pickup_type')
#             stop_sequences = df.get('stop_sequence')
#
#             for i in range(0, len(trip_ids)):
#                 if trip_ids[i] != trip_id:
#                     if trip_id is not None and service_journey is not None:
#                         yield service_journey
#                         service_journey.calls = None  # Free memory
#
#                     trip_id = trip_ids[i]
#                     service_journey = service_journeys[trip_id]
#                     service_journey.calls = CallsRelStructure()
#                     prev_call = None
#                     prev_shape_traveled = 0
#                     prev_order = 1
#
#                 destination_display_view = None
#                 stop_headsign = get_or_none(stop_headsigns, i)
#                 if stop_headsign is not None:
#                     destination_display_view = DestinationDisplayView(
#                         name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
#                     )
#
#                 from_point_ref = getId(self.codespace, ScheduledStopPoint, stop_ids[i])
#                 arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[i])
#                 departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[i])
#
#                 shape_dist_traveled = get_or_none(shape_dist_traveleds, i)
#                 if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
#                     distance = shape_dist_traveled - prev_shape_traveled
#                     prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)
#
#                 call = Call(
#                     id=self.get_trip_id_call(trip_ids[i], stop_sequences[i]),
#                     version=self.version.version,
#                     fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
#                         from_point_ref, ScheduledStopPointRef, self.version.version
#                     ),
#                     destination_display_ref_or_destination_display_view=destination_display_view,
#                     arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off_types[i] != 1)),
#                     departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup_types[i] != 1)),
#                     request_stop=bool(pickup_types[i] == 2 or pickup_types[i] == 3 or drop_off_types[i] == 2 or drop_off_types[i] == 3),
#                     order=prev_order,
#                 )  # stop_sequence is non-negative integer
#
#                 assert service_journey is not None and service_journey.calls is not None
#                 service_journey.calls.call.append(call)
#
#                 prev_call = call
#                 if shape_dist_traveled:
#                     prev_shape_traveled = shape_dist_traveled
#                 prev_order += 1
#
#             if trip_id is not None and service_journey is not None:
#                 yield service_journey
#
#     # TODO: Shares too much code, clean up
#     def getServiceJourneysDayType(
#         self,
#         trips_sql: dict[str, str] = {'query': """select * from trips where trip_id not in (select trip_id from frequencies) order by trip_id;"""},
#         stop_times_sql: dict[str, str] = {'query': """select * from stop_times order by trip_id, stop_sequence;"""},
#     ) -> Generator[ServiceJourney, None, None]:
#         service_journey: ServiceJourney | None
#         service_journeys = {}
#         shape_used = set([])
#
#         with self.conn.cursor() as cur:
#             cur.execute(**trips_sql)
#
#             df = cur.df()
#
#             route_ids = df.get('route_id')
#             trip_ids = df.get('trip_id')
#             service_ids = df.get('service_id')
#             trip_short_names = df.get('trip_short_name')
#             trip_headsigns = df.get('trip_headsign')
#             # route_short_names = df.get('route_short_name')
#             direction_ids = df.get('direction_id')
#             block_ids = df.get('block_id')
#             shape_ids = df.get('shape_id')
#             wheelchair_accessibles = df.get('wheelchair_accessible')
#             # trip_bikes_alloweds = df.get('trip_bikes_allowed')
#             bikes_alloweds = df.get('bikes_allowed')
#             # ticketing_trip_ids = df.get('ticketing_trip_id')
#             # ticketing_types = df.get('ticketing_type')
#
#             for i in range(0, len(route_ids)):
#                 journey_pattern_view = None
#                 if trip_headsigns[i] is not None:
#                     journey_pattern_view = JourneyPatternView(
#                         destination_display_ref_or_destination_display_view=DestinationDisplayView(
#                             name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
#                         )
#                     )
#
#                 accessibility_assessment = None
#                 if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
#                     accessibility_assessment = AccessibilityAssessment(
#                         id=self.get_trip_id_aa(trip_ids[i]),
#                         version=self.version.version,
#                         mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
#                     )
#
#                 block_ref = None
#                 if block_ids[i] is not None:
#                     block_ref = getFakeRef(getId(self.codespace, Block, block_ids[i]), BlockRef, None, "EXTERNAL")
#
#                 # route_ref = None
#                 lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
#                 shape_id = get_or_none(shape_ids, i)
#                 if shape_id is not None:
#                     if shape_id in shape_used:
#                         lsp = getFakeRef(self.get_shape_id_lsp(shape_id), LinkSequenceProjectionRef, self.version.version)
#                     else:
#                         lsps = self.getLineStrings(
#                             {
#                                 'query': (
#                                     """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
#                                 ),
#                                 'parameters': (shape_id,),
#                             }
#                         )
#                         if len(lsps) > 0:
#                             lsp = lsps[0]
#
#                         shape_used.add(shape_id)
#
#                 luggage_carriage_facility_list = []
#                 facitities = None
#                 bikes_allowed = get_or_none(bikes_alloweds, i)
#                 if bikes_allowed is not None:
#                     if bikes_allowed == 1:
#                         luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
#                     elif bikes_allowed == 2:
#                         luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)
#
#                 if len(luggage_carriage_facility_list) > 0:
#                     facitities = ServiceFacilitySetsRelStructure(
#                         restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
#                             ServiceFacilitySet(
#                                 id=self.get_trip_id_sfs(trip_ids[i]),
#                                 version=self.version.version,
#                                 luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
#                             )
#                         ]
#                     )
#
#                 service_journey = ServiceJourney(
#                     id=self.get_trip_id(trip_ids[i]),
#                     version=self.version.version,
#                     flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(self.get_route_id(route_ids[i]), LineRef, self.version.version),
#                     private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
#                     short_name=getOptionalString(get_or_none(trip_short_names, i)),
#                     day_types=DayTypeRefsRelStructure(day_type_ref=[getFakeRef(self.get_service_id_dt(service_ids[i]), DayTypeRef, self.version.version)]),
#                     journey_pattern_view=journey_pattern_view,
#                     direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
#                     block_ref=block_ref,
#                     accessibility_assessment=accessibility_assessment,
#                     facilities=facitities,
#                     link_sequence_projection_ref_or_link_sequence_projection=lsp,
#                 )
#
#                 service_journeys[trip_ids[i]] = service_journey
#
#         with self.conn.cursor() as cur:
#             cur.execute(**stop_times_sql)
#             trip_id = None
#             service_journey = None
#             prev_call = None
#             prev_shape_traveled = 0
#             prev_order = 1
#
#             df = cur.df()
#
#             trip_ids = df.get('trip_id')
#             stop_headsigns = df.get('stop_headsign')
#             stop_ids = df.get('stop_id')
#             arrival_times = df.get('arrival_time')
#             departure_times = df.get('departure_time')
#             shape_dist_traveleds = df.get('shape_dist_traveled')
#             drop_off_types = df.get('drop_off_type')
#             pickup_types = df.get('pickup_type')
#             stop_sequences = df.get('stop_sequence')
#
#             for i in range(0, len(trip_ids)):
#                 if trip_ids[i] != trip_id:
#                     if trip_id is not None and service_journey is not None:
#                         yield service_journey
#
#                     trip_id = trip_ids[i]
#                     service_journey = service_journeys[trip_id]
#                     service_journey.calls = CallsRelStructure()
#                     prev_call = None
#                     prev_shape_traveled = 0
#                     prev_order = 1
#
#                 destination_display_view = None
#                 stop_headsign = get_or_none(stop_headsigns, i)
#                 if stop_headsign is not None:
#                     destination_display_view = DestinationDisplayView(
#                         name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
#                     )
#
#                 from_point_ref = getId(self.codespace, ScheduledStopPoint, stop_ids[i])
#                 arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[i])
#                 departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[i])
#
#                 shape_dist_traveled = get_or_none(shape_dist_traveleds, i)
#                 if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
#                     distance = shape_dist_traveled - prev_shape_traveled
#                     prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)
#
#                 call = Call(
#                     id=self.get_trip_id_call(trip_ids[i], stop_sequences[i]),
#                     version=self.version.version,
#                     fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
#                         from_point_ref, ScheduledStopPointRef, self.version.version
#                     ),
#                     destination_display_ref_or_destination_display_view=destination_display_view,
#                     arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off_types[i] != 1)),
#                     departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup_types[i] != 1)),
#                     request_stop=bool(pickup_types[i] == 2 or pickup_types[i] == 3 or drop_off_types[i] == 2 or drop_off_types[i] == 3),
#                     order=prev_order,
#                 )  # stop_sequence is non-negative integer
#
#                 assert service_journey is not None and service_journey.calls is not None
#                 service_journey.calls.call.append(call)
#
#                 prev_call = call
#                 if shape_dist_traveled:
#                     prev_shape_traveled = shape_dist_traveled
#                 prev_order += 1
#
#             if trip_id is not None and service_journey is not None:
#                 yield service_journey
#
#     def getServiceJourneys2(
#         self,
#         availability_conditions_input: list[AvailabilityCondition],
#         trips_sql: dict[str, str] = {'query': """select * from trips where trip_id not in (select trip_id from frequencies) order by trip_id;"""},
#     ) -> Generator[ServiceJourney, None, None]:
#         availability_conditions = getIndex(availability_conditions_input)
#
#         shape_used = set([])
#
#         with self.conn.cursor() as cur:
#             cur.execute(**trips_sql)
#
#             df = cur.df()
#
#             route_ids = df.get('route_id')
#             trip_ids = df.get('trip_id')
#             service_ids = df.get('service_id')
#             trip_short_names = df.get('trip_short_name')
#             trip_headsigns = df.get('trip_headsign')
#             # route_short_names = df.get('route_short_name')
#             direction_ids = df.get('direction_id')
#             block_ids = df.get('block_id')
#             shape_ids = df.get('shape_id')
#             wheelchair_accessibles = df.get('wheelchair_accessible')
#             # trip_bikes_alloweds = df.get('trip_bikes_allowed')
#             bikes_alloweds = df.get('bikes_allowed')
#             # ticketing_trip_ids = df.get('ticketing_trip_id')
#             # ticketing_types = df.get('ticketing_type')
#
#             for i in range(0, len(route_ids)):
#                 availability_condition_key = getId(self.codespace, AvailabilityCondition, service_ids[i])
#
#                 availability_conditions_journey = [
#                     availability_conditions.get(availability_condition_key, None),
#                     availability_conditions.get(availability_condition_key + "_1", None),
#                     availability_conditions.get(availability_condition_key + "_2", None),
#                 ]
#
#                 journey_pattern_view = None
#                 if trip_headsigns[i] is not None:
#                     journey_pattern_view = JourneyPatternView(
#                         destination_display_ref_or_destination_display_view=DestinationDisplayView(
#                             name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
#                         )
#                     )
#
#                 accessibility_assessment = None
#                 if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
#                     accessibility_assessment = AccessibilityAssessment(
#                         id=self.get_trip_id_aa(trip_ids[i]),
#                         version=self.version.version,
#                         mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
#                     )
#
#                 block_ref = None
#                 if block_ids[i] is not None:
#                     block_ref = getFakeRef(getId(self.codespace, Block, block_ids[i]), BlockRef, None, "EXTERNAL")
#
#                 # route_ref = None
#                 lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
#                 shape_id = get_or_none(shape_ids, i)
#                 if shape_id is not None:
#                     if shape_id in shape_used:
#                         lsp = getFakeRef(getId(self.codespace, LinkSequenceProjection, shape_id), LinkSequenceProjectionRef, self.version.version)
#                     else:
#                         lsps = self.getLineStrings(
#                             {
#                                 'query': (
#                                     """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
#                                 ),
#                                 'parameters': (shape_id,),
#                             }
#                         )
#                         if len(lsps) > 0:
#                             lsp = lsps[0]
#
#                         shape_used.add(shape_id)
#
#                 luggage_carriage_facility_list = []
#                 facitities = None
#                 bikes_allowed = get_or_none(bikes_alloweds, i)
#                 if bikes_allowed is not None:
#                     if bikes_allowed == 1:
#                         luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
#                     elif bikes_allowed == 2:
#                         luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)
#
#                 if len(luggage_carriage_facility_list) > 0:
#                     facitities = ServiceFacilitySetsRelStructure(
#                         restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
#                             ServiceFacilitySet(
#                                 id=self.get_trip_id_sfs(trip_ids[i]),
#                                 version=self.version.version,
#                                 luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
#                             )
#                         ]
#                     )
#
#                 calls = CallsRelStructure()
#
#                 with self.conn.cursor() as cur2:
#                     cur2.execute(**{'query': """select * from stop_times where trip_id = ? order by stop_sequence;""", 'parameters': (trip_ids[i],)})
#                     # trip_id = None
#                     # service_journey = None
#                     prev_call = None
#                     prev_shape_traveled = 0
#                     prev_order = 1
#
#                     df2 = cur2.df()
#
#                     stop_headsigns = df2.get('stop_headsign')
#                     stop_ids = df2.get('stop_id')
#                     arrival_times = df2.get('arrival_time')
#                     departure_times = df2.get('departure_time')
#                     shape_dist_traveleds = df2.get('shape_dist_traveled')
#                     drop_off_types = df2.get('drop_off_type')
#                     pickup_types = df2.get('pickup_type')
#                     stop_sequences = df2.get('stop_sequence')
#
#                     for index_j in range(0, len(stop_ids)):
#                         destination_display_view = None
#                         stop_headsign = get_or_none(stop_headsigns, index_j)
#                         if stop_headsign is not None:
#                             destination_display_view = DestinationDisplayView(
#                                 name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
#                             )
#
#                         from_point_ref = getId(self.codespace, ScheduledStopPoint, stop_ids[index_j])
#                         arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[index_j])
#                         departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[index_j])
#
#                         shape_dist_traveled = get_or_none(shape_dist_traveleds, index_j)
#                         if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
#                             distance = shape_dist_traveled - prev_shape_traveled
#                             prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)
#
#                         call = Call(
#                             id=self.get_trip_id_call(trip_ids[i], stop_sequences[index_j]),
#                             version=self.version.version,
#                             fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
#                                 from_point_ref, ScheduledStopPointRef, self.version.version
#                             ),
#                             destination_display_ref_or_destination_display_view=destination_display_view,
#                             arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off_types[index_j] != 1)),
#                             departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup_types[index_j] != 1)),
#                             request_stop=bool(
#                                 pickup_types[index_j] == 2 or pickup_types[index_j] == 3 or drop_off_types[index_j] == 2 or drop_off_types[index_j] == 3
#                             ),
#                             order=prev_order,
#                         )  # stop_sequence is non-negative integer
#
#                         assert calls is not None
#                         calls.call.append(call)
#
#                         prev_call = call
#                         if shape_dist_traveled:
#                             prev_shape_traveled = shape_dist_traveled
#                         prev_order += 1
#
#                 service_journey = ServiceJourney(
#                     id=self.get_trip_id(trip_ids[i]),
#                     version=self.version.version,
#                     flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(
#                         getId(self.codespace, Line, route_ids[i]), LineRef, self.version.version
#                     ),
#                     private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
#                     short_name=getOptionalString(get_or_none(trip_short_names, i)),
#                     validity_conditions_or_valid_between=[
#                         ValidityConditionsRelStructure(
#                             choice=cast(
#                                 list[
#                                     Union[
#                                         AvailabilityConditionRef,
#                                         ValidityRuleParameterRef,
#                                         ValidityTriggerRef,
#                                         ValidityConditionRef,
#                                         ValidBetween,
#                                         SimpleAvailabilityCondition,
#                                         ValidDuring,
#                                         AvailabilityCondition,
#                                         ValidityRuleParameter,
#                                         ValidityTrigger,
#                                         ValidityCondition,
#                                     ]
#                                 ],
#                                 [getRef(x, AvailabilityConditionRef) for x in availability_conditions_journey if x is not None],
#                             )
#                         )
#                     ],
#                     journey_pattern_view=journey_pattern_view,
#                     direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
#                     block_ref=block_ref,
#                     accessibility_assessment=accessibility_assessment,
#                     facilities=facitities,
#                     link_sequence_projection_ref_or_link_sequence_projection=lsp,
#                     calls=calls,
#                 )
#
#                 yield service_journey
#
#     def getServiceJourneys2DayType(
#         self, trips_sql: dict[str, str] = {'query': """select * from trips where trip_id not in (select trip_id from frequencies) order by trip_id;"""}
#     ) -> Generator[ServiceJourney, None, None]:
#
#         shape_used = set([])
#
#         with self.conn.cursor() as cur:
#             cur.execute(**trips_sql)
#
#             df = cur.df()
#
#             route_ids = df.get('route_id')
#             trip_ids = df.get('trip_id')
#             service_ids = df.get('service_id')
#             trip_short_names = df.get('trip_short_name')
#             trip_headsigns = df.get('trip_headsign')
#             # route_short_names = df.get('route_short_name')
#             direction_ids = df.get('direction_id')
#             block_ids = df.get('block_id')
#             shape_ids = df.get('shape_id')
#             wheelchair_accessibles = df.get('wheelchair_accessible')
#             # trip_bikes_alloweds = df.get('trip_bikes_allowed')
#             bikes_alloweds = df.get('bikes_allowed')
#             # ticketing_trip_ids = df.get('ticketing_trip_id')
#             # ticketing_types = df.get('ticketing_type')
#
#             for i in range(0, len(route_ids)):
#                 journey_pattern_view = None
#                 if trip_headsigns[i] is not None:
#                     journey_pattern_view = JourneyPatternView(
#                         destination_display_ref_or_destination_display_view=DestinationDisplayView(
#                             name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
#                         )
#                     )
#
#                 accessibility_assessment = None
#                 if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
#                     accessibility_assessment = AccessibilityAssessment(
#                         id=self.get_trip_id_aa(trip_ids[i]),
#                         version=self.version.version,
#                         mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
#                     )
#
#                 block_ref = None
#                 if block_ids[i] is not None:
#                     block_ref = getFakeRef(getId(self.codespace, Block, block_ids[i]), BlockRef, None, "EXTERNAL")
#
#                 # route_ref = None
#                 lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
#                 shape_id = get_or_none(shape_ids, i)
#                 if shape_id is not None:
#                     if shape_id in shape_used:
#                         lsp = getFakeRef(getId(self.codespace, LinkSequenceProjection, shape_id), LinkSequenceProjectionRef, self.version.version)
#                     else:
#                         lsps = self.getLineStrings(
#                             {
#                                 'query': (
#                                     """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
#                                 ),
#                                 'parameters': (shape_id,),
#                             }
#                         )
#                         if len(lsps) > 0:
#                             lsp = lsps[0]
#
#                         shape_used.add(shape_id)
#
#                 luggage_carriage_facility_list = []
#                 facitities = None
#                 bikes_allowed = get_or_none(bikes_alloweds, i)
#                 if bikes_allowed is not None:
#                     if bikes_allowed == 1:
#                         luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
#                     elif bikes_allowed == 2:
#                         luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)
#
#                 if len(luggage_carriage_facility_list) > 0:
#                     facitities = ServiceFacilitySetsRelStructure(
#                         restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
#                             ServiceFacilitySet(
#                                 id=self.get_trip_id_sfs(trip_ids[i]),
#                                 version=self.version.version,
#                                 luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
#                             )
#                         ]
#                     )
#
#                 calls = CallsRelStructure()
#
#                 with self.conn.cursor() as cur2:
#                     cur2.execute(**{'query': """select * from stop_times where trip_id = ? order by stop_sequence;""", 'parameters': (trip_ids[i],)})
#                     # trip_id = None
#                     # service_journey = None
#                     prev_call = None
#                     prev_shape_traveled = 0
#                     prev_order = 1
#
#                     df2 = cur2.df()
#
#                     stop_headsigns = df2.get('stop_headsign')
#                     stop_ids = df2.get('stop_id')
#                     arrival_times = df2.get('arrival_time')
#                     departure_times = df2.get('departure_time')
#                     shape_dist_traveleds = df2.get('shape_dist_traveled')
#                     drop_off_types = df2.get('drop_off_type')
#                     pickup_types = df2.get('pickup_type')
#                     stop_sequences = df2.get('stop_sequence')
#
#                     for index_j in range(0, len(stop_ids)):
#                         destination_display_view = None
#                         stop_headsign = get_or_none(stop_headsigns, index_j)
#                         if stop_headsign is not None:
#                             destination_display_view = DestinationDisplayView(
#                                 name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
#                             )
#
#                         from_point_ref = getId(self.codespace, ScheduledStopPoint, stop_ids[index_j])
#                         arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[index_j])
#                         departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[index_j])
#
#                         shape_dist_traveled = get_or_none(shape_dist_traveleds, index_j)
#                         if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
#                             distance = shape_dist_traveled - prev_shape_traveled
#                             prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)
#                         pickup = 0 if pd.isna(pickup_types[index_j]) else pickup_types[index_j]
#                         drop_off = 0 if pd.isna(drop_off_types[index_j]) else drop_off_types[index_j]
#                         call = Call(
#                             id=self.get_trip_id_call(trip_ids[i], stop_sequences[index_j]),
#                             version=self.version.version,
#                             fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
#                                 from_point_ref, ScheduledStopPointRef, self.version.version
#                             ),
#                             destination_display_ref_or_destination_display_view=destination_display_view,
#                             arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off != 1)),
#                             departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup != 1)),
#                             request_stop=bool(pickup == 2 or pickup == 3 or drop_off == 2 or drop_off == 3),
#                             order=prev_order,
#                         )  # stop_sequence is non-negative integer
#
#                         calls.call.append(call)
#
#                         prev_call = call
#                         if shape_dist_traveled:
#                             prev_shape_traveled = shape_dist_traveled
#                         prev_order += 1
#
#                 service_journey = ServiceJourney(
#                     id=self.get_trip_id(trip_ids[i]),
#                     version=self.version.version,
#                     flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(
#                         getId(self.codespace, Line, route_ids[i]), LineRef, self.version.version
#                     ),
#                     private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
#                     short_name=getOptionalString(get_or_none(trip_short_names, i)),
#                     day_types=DayTypeRefsRelStructure(day_type_ref=[getFakeRef(self.get_service_id_dt(service_ids[i]), DayTypeRef, self.version.version)]),
#                     journey_pattern_view=journey_pattern_view,
#                     direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
#                     block_ref=block_ref,
#                     accessibility_assessment=accessibility_assessment,
#                     facilities=facitities,
#                     link_sequence_projection_ref_or_link_sequence_projection=lsp,
#                     calls=calls,
#                 )
#
#                 yield service_journey
#
#     def getServiceJourneys2DayType2(
#         self, trip_ids_sql: dict[str, str] = {'query': """select trip_id from trips where trip_id not in (select trip_id from frequencies) order by trip_id;"""}
#     ) -> Generator[ServiceJourney, None, None]:
#
#         all_trip_ids = []
#
#         with self.conn.cursor() as cur:
#             cur.execute(**trip_ids_sql)
#             df = cur.df()
#             all_trip_ids = list(df.get('trip_id'))
#
#         shape_used = set([])
#
#         for trip_id in all_trip_ids:
#             trips_sql = {'query': """select * from trips where trip_id = ?;""", 'parameters': (trip_id,)}
#
#             with self.conn.cursor() as cur:
#                 cur.execute(**trips_sql)
#
#                 df = cur.df()
#
#                 route_ids = df.get('route_id')
#                 trip_ids = df.get('trip_id')
#                 service_ids = df.get('service_id')
#                 trip_short_names = df.get('trip_short_name')
#                 trip_headsigns = df.get('trip_headsign')
#                 # route_short_names = df.get('route_short_name')
#                 direction_ids = df.get('direction_id')
#                 block_ids = df.get('block_id')
#                 shape_ids = df.get('shape_id')
#                 wheelchair_accessibles = df.get('wheelchair_accessible')
#                 # trip_bikes_alloweds = df.get('trip_bikes_allowed')
#                 bikes_alloweds = df.get('bikes_allowed')
#                 # ticketing_trip_ids = df.get('ticketing_trip_id')
#                 # ticketing_types = df.get('ticketing_type')
#
#                 for i in range(0, len(route_ids)):
#                     journey_pattern_view = None
#                     if trip_headsigns[i] is not None:
#                         journey_pattern_view = JourneyPatternView(
#                             destination_display_ref_or_destination_display_view=DestinationDisplayView(
#                                 name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
#                             )
#                         )
#
#                     accessibility_assessment = None
#                     if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
#                         accessibility_assessment = AccessibilityAssessment(
#                             id=self.get_trip_id_aa(trip_ids[i]),
#                             version=self.version.version,
#                             mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
#                         )
#
#                     block_ref = None
#                     if block_ids[i] is not None:
#                         block_ref = getFakeRef(getId(self.codespace, Block, block_ids[i]), BlockRef, None, "EXTERNAL")
#
#                     # route_ref = None
#                     lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
#                     shape_id = get_or_none(shape_ids, i)
#                     if shape_id is not None:
#                         if shape_id in shape_used:
#                             lsp = getFakeRef(getId(self.codespace, LinkSequenceProjection, shape_id), LinkSequenceProjectionRef, self.version.version)
#                         else:
#                             lsps = self.getLineStrings(
#                                 {
#                                     'query': (
#                                         """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
#                                     ),
#                                     'parameters': (shape_id,),
#                                 }
#                             )
#                             if len(lsps) > 0:
#                                 lsp = lsps[0]
#
#                             shape_used.add(shape_id)
#
#                     luggage_carriage_facility_list = []
#                     facitities = None
#                     bikes_allowed = get_or_none(bikes_alloweds, i)
#                     if bikes_allowed is not None:
#                         if bikes_allowed == 1:
#                             luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
#                         elif bikes_allowed == 2:
#                             luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)
#
#                     if len(luggage_carriage_facility_list) > 0:
#                         facitities = ServiceFacilitySetsRelStructure(
#                             restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
#                                 ServiceFacilitySet(
#                                     id=self.get_trip_id_sfs(trip_ids[i]),
#                                     version=self.version.version,
#                                     luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
#                                 )
#                             ]
#                         )
#
#                     calls = CallsRelStructure()
#
#                     with self.conn.cursor() as cur2:
#                         cur2.execute(**{'query': """select * from stop_times where trip_id = ? order by stop_sequence;""", 'parameters': (trip_ids[i],)})
#                         # trip_id = None
#                         # service_journey = None
#                         prev_call = None
#                         prev_shape_traveled = 0
#                         prev_order = 1
#
#                         df2 = cur2.df()
#
#                         stop_headsigns = df2.get('stop_headsign')
#                         stop_ids = df2.get('stop_id')
#                         arrival_times = df2.get('arrival_time')
#                         departure_times = df2.get('departure_time')
#                         shape_dist_traveleds = df2.get('shape_dist_traveled')
#                         drop_off_types = df2.get('drop_off_type')
#                         pickup_types = df2.get('pickup_type')
#                         stop_sequences = df2.get('stop_sequence')
#
#                         for index_j in range(0, len(stop_ids)):
#                             destination_display_view = None
#                             stop_headsign = get_or_none(stop_headsigns, index_j)
#                             if stop_headsign is not None:
#                                 destination_display_view = DestinationDisplayView(
#                                     name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
#                                 )
#
#                             from_point_ref = getId(self.codespace, ScheduledStopPoint, stop_ids[index_j])
#                             arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[index_j])
#                             departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[index_j])
#
#                             shape_dist_traveled = get_or_none(shape_dist_traveleds, index_j)
#                             if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
#                                 distance = shape_dist_traveled - prev_shape_traveled
#                                 prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)
#                             pickup = 0 if pd.isna(pickup_types[index_j]) else pickup_types[index_j]
#                             drop_off = 0 if pd.isna(drop_off_types[index_j]) else drop_off_types[index_j]
#                             call = Call(
#                                 id=self.get_trip_id_call(trip_ids[i], stop_sequences[index_j]),
#                                 version=self.version.version,
#                                 fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
#                                     from_point_ref, ScheduledStopPointRef, self.version.version
#                                 ),
#                                 destination_display_ref_or_destination_display_view=destination_display_view,
#                                 arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off != 1)),
#                                 departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup != 1)),
#                                 request_stop=bool(pickup == 2 or pickup == 3 or drop_off == 2 or drop_off == 3),
#                                 order=prev_order,
#                             )  # stop_sequence is non-negative integer
#
#                             calls.call.append(call)
#
#                             prev_call = call
#                             if shape_dist_traveled:
#                                 prev_shape_traveled = shape_dist_traveled
#                             prev_order += 1
#
#                     service_journey = ServiceJourney(
#                         id=self.get_trip_id(trip_ids[i]),
#                         version=self.version.version,
#                         flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(
#                             getId(self.codespace, Line, route_ids[i]), LineRef, self.version.version
#                         ),
#                         private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
#                         short_name=getOptionalString(get_or_none(trip_short_names, i)),
#                         day_types=DayTypeRefsRelStructure(day_type_ref=[getFakeRef(self.get_service_id_dt(service_ids[i]), DayTypeRef, self.version.version)]),
#                         journey_pattern_view=journey_pattern_view,
#                         direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
#                         block_ref=block_ref,
#                         accessibility_assessment=accessibility_assessment,
#                         facilities=facitities,
#                         link_sequence_projection_ref_or_link_sequence_projection=lsp,
#                         calls=calls,
#                     )
#
#                     yield service_journey
#
#     def get_trip_id_tsj(self, trip_id: str) -> str:
#         if ':TemplateServiceJourney:' in trip_id:
#             return trip_id
#         if ':ServiceJourney:' in trip_id:
#             return trip_id.replace(':ServiceJourney:', ':TemplateServiceJourney:')
#         else:
#             return getId(self.codespace, TemplateServiceJourney, trip_id)
#
#     def getTemplateServiceJourneys(
#         self,
#         availability_conditions_input: list[AvailabilityCondition],
#         trips_sql: dict[str, str] = {'query': """select * from trips WHERE trip_id IN (SELECT trip_id FROM frequencies) order by trip_id;"""},
#     ) -> Generator[TemplateServiceJourney, None, None]:
#         availability_conditions = getIndex(availability_conditions_input)
#
#         shape_used = set([])
#
#         with self.conn.cursor() as cur:
#             cur.execute(**trips_sql)
#
#             df = cur.df()
#
#             route_ids = df.get('route_id')
#             trip_ids = df.get('trip_id')
#             service_ids = df.get('service_id')
#             trip_short_names = df.get('trip_short_name')
#             trip_headsigns = df.get('trip_headsign')
#             # route_short_names = df.get('route_short_name')
#             direction_ids = df.get('direction_id')
#             block_ids = df.get('block_id')
#             shape_ids = df.get('shape_id')
#             wheelchair_accessibles = df.get('wheelchair_accessible')
#             # trip_bikes_alloweds = df.get('trip_bikes_allowed')
#             bikes_alloweds = df.get('bikes_allowed')
#             # ticketing_trip_ids = df.get('ticketing_trip_id')
#             # ticketing_types = df.get('ticketing_type')
#
#             for i in range(0, len(route_ids)):
#                 availability_condition_key = getId(self.codespace, AvailabilityCondition, service_ids[i])
#
#                 availability_conditions_journey = [
#                     availability_conditions.get(availability_condition_key, None),
#                     availability_conditions.get(availability_condition_key + "_1", None),
#                     availability_conditions.get(availability_condition_key + "_2", None),
#                 ]
#
#                 journey_pattern_view = None
#                 if trip_headsigns[i] is not None:
#                     journey_pattern_view = JourneyPatternView(
#                         destination_display_ref_or_destination_display_view=DestinationDisplayView(
#                             name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
#                         )
#                     )
#
#                 accessibility_assessment = None
#                 if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
#                     accessibility_assessment = AccessibilityAssessment(
#                         id=self.get_trip_id_aa(trip_ids[i]),
#                         version=self.version.version,
#                         mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
#                     )
#
#                 block_ref = None
#                 if block_ids[i] is not None:
#                     block_ref = getFakeRef(getId(self.codespace, Block, block_ids[i]), BlockRef, None)
#
#                 # route_ref = None
#                 lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
#                 shape_id = get_or_none(shape_ids, i)
#                 if shape_id is not None:
#                     if shape_id in shape_used:
#                         lsp = getFakeRef(getId(self.codespace, LinkSequenceProjection, shape_id), LinkSequenceProjectionRef, self.version.version)
#                     else:
#                         lsps = self.getLineStrings(
#                             {
#                                 'query': (
#                                     """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
#                                 ),
#                                 'parameters': (shape_id,),
#                             }
#                         )
#                         if len(lsps) > 0:
#                             lsp = lsps[0]
#
#                         shape_used.add(shape_id)
#
#                 luggage_carriage_facility_list = []
#                 facitities = None
#                 bikes_allowed = get_or_none(bikes_alloweds, i)
#                 if bikes_allowed is not None:
#                     if bikes_allowed == 1:
#                         luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
#                     elif bikes_allowed == 2:
#                         luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)
#
#                 if len(luggage_carriage_facility_list) > 0:
#                     facitities = ServiceFacilitySetsRelStructure(
#                         restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
#                             ServiceFacilitySet(
#                                 id=self.get_trip_id_sfs(trip_ids[i]),
#                                 version=self.version.version,
#                                 luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
#                             )
#                         ]
#                     )
#
#                 calls = CallsRelStructure()
#
#                 with self.conn.cursor() as cur2:
#                     cur2.execute(**{'query': """select * from stop_times where trip_id = ? order by stop_sequence;""", 'parameters': (trip_ids[i],)})
#                     # trip_id = None
#                     # service_journey = None
#                     prev_call = None
#                     prev_shape_traveled: int | None = 0
#                     prev_order = 1
#
#                     df2 = cur2.df()
#
#                     stop_headsigns = df2.get('stop_headsign')
#                     stop_ids = df2.get('stop_id')
#                     arrival_times = df2.get('arrival_time')
#                     departure_times = df2.get('departure_time')
#                     shape_dist_traveleds = df2.get('shape_dist_traveled')
#                     drop_off_types = df2.get('drop_off_type')
#                     pickup_types = df2.get('pickup_type')
#                     stop_sequences = df2.get('stop_sequence')
#
#                     for index_j in range(0, len(stop_ids)):
#                         destination_display_view = None
#                         stop_headsign = get_or_none(stop_headsigns, index_j)
#                         if stop_headsign is not None:
#                             destination_display_view = DestinationDisplayView(
#                                 name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
#                             )
#
#                         from_point_ref = getId(self.codespace, ScheduledStopPoint, stop_ids[index_j])
#                         arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[index_j])
#                         departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[index_j])
#
#                         shape_dist_traveled = get_or_none(shape_dist_traveleds, index_j)
#                         if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
#                             distance = shape_dist_traveled - prev_shape_traveled
#                             prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)
#
#                         call = Call(
#                             id=self.get_trip_id_call(trip_ids[i], stop_sequences[index_j]),
#                             version=self.version.version,
#                             fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
#                                 from_point_ref, ScheduledStopPointRef, self.version.version
#                             ),
#                             destination_display_ref_or_destination_display_view=destination_display_view,
#                             arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off_types[index_j] != 1)),
#                             departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup_types[index_j] != 1)),
#                             request_stop=bool(
#                                 pickup_types[index_j] == 2 or pickup_types[index_j] == 3 or drop_off_types[index_j] == 2 or drop_off_types[index_j] == 3
#                             ),
#                             order=prev_order,
#                         )  # stop_sequence is non-negative integer
#
#                         calls.call.append(call)
#
#                         prev_call = call
#                         if shape_dist_traveled:
#                             prev_shape_traveled = shape_dist_traveled
#                         prev_order += 1
#
#                 with self.conn.cursor() as cur3:
#                     cur3.execute(**{'query': """select * from frequencies where trip_id = ?;""", 'parameters': (trip_ids[i],)})
#                     df3 = cur3.df()
#
#                     start_times = df3.get('start_time')
#                     end_times = df3.get('end_time')
#                     headway_secs = df3.get('headway_secs')
#                     exact_times = df3.get('exact_times')
#
#                     hjgs = []
#
#                     for index_j in range(0, len(start_times)):
#                         start_time, start_dayoffset = self.noonTimeToNeTEx(start_times[index_j])
#                         end_time, end_dayoffset = self.noonTimeToNeTEx(end_times[index_j])
#
#                         jfgvs = [
#                             JourneyFrequencyGroupVersionStructure.FirstDayOffset(value=start_dayoffset),
#                             JourneyFrequencyGroupVersionStructure.LastDepartureTime(value=end_time),
#                             JourneyFrequencyGroupVersionStructure.LastDayOffset(value=end_dayoffset),
#                         ]
#
#                         hjgs.append(
#                             HeadwayJourneyGroup(
#                                 id=getId(self.codespace, HeadwayJourneyGroup, trip_ids[i] + '_' + start_times[index_j].replace(':', '')),
#                                 first_departure_time=start_time,
#                                 first_day_offset_or_last_departure_time_or_last_day_offset_or_first_arrival_time_or_last_arrival_time=cast(
#                                     list[
#                                         Union[
#                                             "JourneyFrequencyGroupVersionStructure.FirstDayOffset",
#                                             "JourneyFrequencyGroupVersionStructure.LastDepartureTime",
#                                             "JourneyFrequencyGroupVersionStructure.LastDayOffset",
#                                             "JourneyFrequencyGroupVersionStructure.FirstArrivalTime",
#                                             "JourneyFrequencyGroupVersionStructure.LastArrivalTime",
#                                         ]
#                                     ],
#                                     jfgvs,
#                                 ),
#                                 scheduled_headway_interval=XmlDuration(value=f'PT{headway_secs[index_j]}S') if exact_times[index_j] == 1 else None,
#                                 minimum_headway_interval=XmlDuration(value=f'PT{headway_secs[index_j]}S') if exact_times[index_j] == 0 else None,
#                             )
#                         )
#
#                 template_service_journey = TemplateServiceJourney(
#                     id=getId(self.codespace, TemplateServiceJourney, trip_ids[i]),
#                     version=self.version.version,
#                     flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(
#                         getId(self.codespace, Line, route_ids[i]), LineRef, self.version.version
#                     ),
#                     private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
#                     short_name=getOptionalString(get_or_none(trip_short_names, i)),
#                     validity_conditions_or_valid_between=[
#                         ValidityConditionsRelStructure(
#                             choice=cast(
#                                 list[
#                                     Union[
#                                         AvailabilityConditionRef,
#                                         ValidityRuleParameterRef,
#                                         ValidityTriggerRef,
#                                         ValidityConditionRef,
#                                         ValidBetween,
#                                         SimpleAvailabilityCondition,
#                                         ValidDuring,
#                                         AvailabilityCondition,
#                                         ValidityRuleParameter,
#                                         ValidityTrigger,
#                                         ValidityCondition,
#                                     ]
#                                 ],
#                                 [getRef(x, AvailabilityConditionRef) for x in availability_conditions_journey if x is not None],
#                             )
#                         )
#                     ],
#                     journey_pattern_view=journey_pattern_view,
#                     direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
#                     block_ref=block_ref,
#                     accessibility_assessment=accessibility_assessment,
#                     facilities=facitities,
#                     link_sequence_projection_ref_or_link_sequence_projection=lsp,
#                     calls=calls,
#                     frequency_groups=FrequencyGroupsRelStructure(
#                         headway_journey_group_ref_or_headway_journey_group_or_rhythmical_journey_group_ref_or_rhythmical_journey_group=cast(
#                             list[Union[HeadwayJourneyGroupRef, HeadwayJourneyGroup, RhythmicalJourneyGroupRef, RhythmicalJourneyGroup]], hjgs
#                         )
#                     ),
#                 )
#                 yield template_service_journey
#
#     def getTemplateServiceJourneysDayType(
#         self, trips_sql: dict[str, str] = {'query': """select * from trips WHERE trip_id IN (SELECT trip_id FROM frequencies) order by trip_id;"""}
#     ) -> Generator[TemplateServiceJourney, None, None]:
#         shape_used = set([])
#
#         with self.conn.cursor() as cur:
#             cur.execute(**trips_sql)
#
#             df = cur.df()
#
#             route_ids = df.get('route_id')
#             trip_ids = df.get('trip_id')
#             service_ids = df.get('service_id')
#             trip_short_names = df.get('trip_short_name')
#             trip_headsigns = df.get('trip_headsign')
#             # route_short_names = df.get('route_short_name')
#             direction_ids = df.get('direction_id')
#             block_ids = df.get('block_id')
#             shape_ids = df.get('shape_id')
#             wheelchair_accessibles = df.get('wheelchair_accessible')
#             # trip_bikes_alloweds = df.get('trip_bikes_allowed')
#             bikes_alloweds = df.get('bikes_allowed')
#             # ticketing_trip_ids = df.get('ticketing_trip_id')
#             # ticketing_types = df.get('ticketing_type')
#
#             for i in range(0, len(route_ids)):
#                 journey_pattern_view = None
#                 if trip_headsigns[i] is not None:
#                     journey_pattern_view = JourneyPatternView(
#                         destination_display_ref_or_destination_display_view=DestinationDisplayView(
#                             name=MultilingualString(value=trip_headsigns[i]), front_text=MultilingualString(value=trip_headsigns[i])
#                         )
#                     )
#
#                 accessibility_assessment = None
#                 if wheelchair_accessibles is not None and not isinstance(wheelchair_accessibles[i], NAType):
#                     accessibility_assessment = AccessibilityAssessment(
#                         id=self.get_trip_id_aa(trip_ids[i]),
#                         version=self.version.version,
#                         mobility_impaired_access=self.wheelchairToNeTEx(wheelchair_accessibles[i]),
#                     )
#
#                 block_ref = None
#                 if block_ids[i] is not None:
#                     block_ref = getFakeRef(getId(self.codespace, Block, block_ids[i]), BlockRef, None)
#
#                 # route_ref = None
#                 lsp: LinkSequenceProjection | LinkSequenceProjectionRef | None = None
#                 shape_id = get_or_none(shape_ids, i)
#                 if shape_id is not None:
#                     if shape_id in shape_used:
#                         lsp = getFakeRef(getId(self.codespace, LinkSequenceProjection, shape_id), LinkSequenceProjectionRef, self.version.version)
#                     else:
#                         lsps = self.getLineStrings(
#                             {
#                                 'query': (
#                                     """select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled from shapes where shape_id = ? order by shape_id, shape_pt_sequence, shape_dist_traveled;"""
#                                 ),
#                                 'parameters': (shape_id,),
#                             }
#                         )
#                         if len(lsps) > 0:
#                             lsp = lsps[0]
#
#                         shape_used.add(shape_id)
#
#                 luggage_carriage_facility_list = []
#                 facitities = None
#                 bikes_allowed = get_or_none(bikes_alloweds, i)
#                 if bikes_allowed is not None:
#                     if bikes_allowed == 1:
#                         luggage_carriage_facility_list.append(LuggageCarriageEnumeration.CYCLES_ALLOWED)
#                     elif bikes_allowed == 2:
#                         luggage_carriage_facility_list.append(LuggageCarriageEnumeration.NO_CYCLES)
#
#                 if len(luggage_carriage_facility_list) > 0:
#                     facitities = ServiceFacilitySetsRelStructure(
#                         restricted_service_facility_set_ref_or_service_facility_set_ref_or_service_facility_set=[
#                             ServiceFacilitySet(
#                                 id=self.get_trip_id_sfs(trip_ids[i]),
#                                 version=self.version.version,
#                                 luggage_carriage_facility_list=LuggageCarriageFacilityList(value=luggage_carriage_facility_list),
#                             )
#                         ]
#                     )
#
#                 calls = CallsRelStructure()
#
#                 with self.conn.cursor() as cur2:
#                     cur2.execute(**{'query': """select * from stop_times where trip_id = ? order by stop_sequence;""", 'parameters': (trip_ids[i],)})
#                     prev_call: Call | None = None
#                     prev_shape_traveled: int = 0
#                     prev_order = 1
#
#                     df2 = cur2.df()
#
#                     stop_headsigns = df2.get('stop_headsign')
#                     stop_ids = df2.get('stop_id')
#                     arrival_times = df2.get('arrival_time')
#                     departure_times = df2.get('departure_time')
#                     shape_dist_traveleds = df2.get('shape_dist_traveled')
#                     drop_off_types = df2.get('drop_off_type')
#                     pickup_types = df2.get('pickup_type')
#                     stop_sequences = df2.get('stop_sequence')
#
#                     for index_j in range(0, len(stop_ids)):
#                         destination_display_view = None
#                         stop_headsign = get_or_none(stop_headsigns, index_j)
#                         if stop_headsign is not None:
#                             destination_display_view = DestinationDisplayView(
#                                 name=MultilingualString(value=stop_headsign), front_text=MultilingualString(value=stop_headsign)
#                             )
#
#                         from_point_ref = getId(self.codespace, ScheduledStopPoint, stop_ids[index_j])
#                         arrival_time, arrival_dayoffset = self.noonTimeToNeTEx(arrival_times[index_j])
#                         departure_time, departure_dayoffset = self.noonTimeToNeTEx(departure_times[index_j])
#
#                         shape_dist_traveled = get_or_none(shape_dist_traveleds, index_j)
#                         if prev_call and shape_dist_traveled is not None and not numpy.isnan(shape_dist_traveled):
#                             distance = shape_dist_traveled - prev_shape_traveled
#                             prev_call.onward_service_link_ref_or_onward_service_link_view = OnwardServiceLinkView(distance=distance)
#
#                         call = Call(
#                             id=self.get_trip_id_call(trip_ids[i], stop_sequences[index_j]),
#                             version=self.version.version,
#                             fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=getFakeRef(
#                                 from_point_ref, ScheduledStopPointRef, self.version.version
#                             ),
#                             destination_display_ref_or_destination_display_view=destination_display_view,
#                             arrival=ArrivalStructure(time=arrival_time, day_offset=arrival_dayoffset, for_alighting=bool(drop_off_types[index_j] != 1)),
#                             departure=DepartureStructure(time=departure_time, day_offset=departure_dayoffset, for_boarding=bool(pickup_types[index_j] != 1)),
#                             request_stop=bool(
#                                 pickup_types[index_j] == 2 or pickup_types[index_j] == 3 or drop_off_types[index_j] == 2 or drop_off_types[index_j] == 3
#                             ),
#                             order=prev_order,
#                         )  # stop_sequence is non-negative integer
#
#                         calls.call.append(call)
#
#                         prev_call = call
#                         if shape_dist_traveled:
#                             prev_shape_traveled = shape_dist_traveled
#                         prev_order += 1
#
#                 with self.conn.cursor() as cur3:
#                     cur3.execute(**{'query': """select * from frequencies where trip_id = ?;""", 'parameters': (trip_ids[i],)})
#                     df3 = cur3.df()
#
#                     start_times = df3.get('start_time')
#                     end_times = df3.get('end_time')
#                     headway_secs = df3.get('headway_secs')
#                     exact_times = df3.get('exact_times')
#
#                     hjgs = []
#
#                     for index_j in range(0, len(start_times)):
#                         start_time, start_dayoffset = self.noonTimeToNeTEx(start_times[index_j])
#                         end_time, end_dayoffset = self.noonTimeToNeTEx(end_times[index_j])
#
#                         jfgvs = [
#                             JourneyFrequencyGroupVersionStructure.FirstDayOffset(value=start_dayoffset),
#                             JourneyFrequencyGroupVersionStructure.LastDepartureTime(value=end_time),
#                             JourneyFrequencyGroupVersionStructure.LastDayOffset(value=end_dayoffset),
#                         ]
#
#                         hjgs.append(
#                             HeadwayJourneyGroup(
#                                 id=getId(self.codespace, HeadwayJourneyGroup, trip_ids[i] + '_' + start_times[index_j].replace(':', '')),
#                                 first_departure_time=start_time,
#                                 first_day_offset_or_last_departure_time_or_last_day_offset_or_first_arrival_time_or_last_arrival_time=cast(
#                                     list[
#                                         Union[
#                                             "JourneyFrequencyGroupVersionStructure.FirstDayOffset",
#                                             "JourneyFrequencyGroupVersionStructure.LastDepartureTime",
#                                             "JourneyFrequencyGroupVersionStructure.LastDayOffset",
#                                             "JourneyFrequencyGroupVersionStructure.FirstArrivalTime",
#                                             "JourneyFrequencyGroupVersionStructure.LastArrivalTime",
#                                         ]
#                                     ],
#                                     jfgvs,
#                                 ),
#                                 scheduled_headway_interval=XmlDuration(value=f'PT{headway_secs[index_j]}S') if exact_times[index_j] == 1 else None,
#                                 minimum_headway_interval=XmlDuration(value=f'PT{headway_secs[index_j]}S') if exact_times[index_j] == 0 else None,
#                             )
#                         )
#
#                 template_service_journey = TemplateServiceJourney(
#                     id=getId(self.codespace, TemplateServiceJourney, trip_ids[i]),
#                     version=self.version.version,
#                     flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view=getFakeRef(
#                         getId(self.codespace, Line, route_ids[i]), LineRef, self.version.version
#                     ),
#                     private_codes=PrivateCodes(private_code=[PrivateCode(value=trip_ids[i], type_value="trip_id")]),
#                     short_name=getOptionalString(get_or_none(trip_short_names, i)),
#                     day_types=DayTypeRefsRelStructure(day_type_ref=[getFakeRef(self.get_service_id_dt(service_ids[i]), DayTypeRef, self.version.version)]),
#                     journey_pattern_view=journey_pattern_view,
#                     direction_type=self.directionToNeTEx(get_or_none(direction_ids, i)),
#                     block_ref=block_ref,
#                     accessibility_assessment=accessibility_assessment,
#                     facilities=facitities,
#                     link_sequence_projection_ref_or_link_sequence_projection=lsp,
#                     calls=calls,
#                     frequency_groups=FrequencyGroupsRelStructure(
#                         headway_journey_group_ref_or_headway_journey_group_or_rhythmical_journey_group_ref_or_rhythmical_journey_group=cast(
#                             list[Union[HeadwayJourneyGroupRef, HeadwayJourneyGroup, RhythmicalJourneyGroupRef, RhythmicalJourneyGroup]], hjgs
#                         )
#                     ),
#                 )
#                 yield template_service_journey
#
#     def database(self, con: Database) -> None:
#         # This still sucks :-) shape is in every ServiceJourney now
#         # in order to solve it, we must find the route point that matches the
#         # shape point exactly, but if the GTFS shape is provided as an abstract
#         # shape there may not be a one-to-one RouteLink.
#         #
#         # self.routes, self.route_points, self.route_links = self.getRoutes()
#         # write_objects(con, self.route_points, True, True)
#         # write_objects(con, self.route_links, True, True)
#         # write_objects(con, self.routes, True, True)
#
#         con.insert_objects_on_queue(Codespace, [self.codespace])
#         con.insert_objects_on_queue(DataSource, [self.data_source])
#         con.insert_objects_on_queue(Version, [self.version])
#
#         gf = GeneralFrame(id="Defaults", version="any", frame_defaults=self.frame_defaults)
#         con.insert_objects_on_queue(GeneralFrame, [gf])
#
#         con.insert_objects_on_queue(Operator, self.getOperators())
#
#         con.insert_objects_on_queue(Line, self.lines)
#
#         stop_areas = self.getStopAreas()
#         con.insert_objects_on_queue(StopArea, stop_areas)
#         con.insert_objects_on_queue(ScheduledStopPoint, self.getScheduledStopPoints(stop_areas))
#         del stop_areas
#
#         stop_places, passenger_stop_assignments = self.getStopPlaces()
#         con.insert_objects_on_queue(StopPlace, stop_places)
#         con.insert_objects_on_queue(PassengerStopAssignment, passenger_stop_assignments)
#         del stop_places
#         del passenger_stop_assignments
#
#         day_types, day_type_assignments, operating_periods = self.getDayTypes()
#         con.insert_objects_on_queue(DayType, day_types)
#         con.insert_objects_on_queue(OperatingPeriod, operating_periods)
#         con.insert_objects_on_queue(DayTypeAssignment, day_type_assignments)
#
#         # availability_conditions = self.getAvailabilityConditions()
#         # write_objects(con, availability_conditions, empty=True, many=True)
#
#         # con.insert_objects_on_queue(ServiceJourney, self.getServiceJourneys2DayType())
#
#         con.insert_objects_on_queue(ServiceJourney, self.getServiceJourneys2DayType3())
#
#         # con.insert_objects_on_queue(TemplateServiceJourney, self.getTemplateServiceJourneysDayType())
#
#         # con.insert_objects_on_queue(InterchangeRule, self.getInterchangeRules())
#
#     codespace: Codespace
#     data_source: DataSource
#     version: Version
#     frame_defaults: VersionFrameDefaultsStructure
#
#     def __init__(self, conn: Any, serializer: XmlSerializer):
#         self.conn = conn
#         self.serializer = serializer
#
#         self.ns_map = {'': 'http://www.netex.org.uk/netex', 'gml': 'http://www.opengis.net/gml/3.2'}
#         self.codespace, self.data_source, self.version, self.frame_defaults = self.getCodespaceAndDataSource()
#         self.lines = self.getLines()
import logging
from pathlib import Path

from domain.gtfs.services.duckdb_to_storage import to_storage
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import prepare_logger, log_all


def gtfs_convert_to_db(database_gtfs: Path, database_netex: Path) -> None:
    with MdbxStorage(database_netex, readonly=False) as storage:
        to_storage(database_gtfs, storage)


def main(source: str, target: str) -> None:
    source_path = Path(source)
    if not source_path.exists():
        log_all(logging.ERROR, f"{source_path} does not exist.")

    else:
        gtfs_convert_to_db(source_path, Path(target))


if __name__ == '__main__':
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description='Convert a GTFS database to a NeTEx database')
    parser.add_argument('gtfs', type=str, help='GTFS database to convert, for example: gtfs-import.duckdb')
    parser.add_argument('database', type=str, help='Storage file to overwrite and store contents of the conversion.')
    parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.gtfs, args.database)
    except Exception as e:
        log_all(logging.ERROR, f'{e} {traceback.format_exc()}')
        raise e
