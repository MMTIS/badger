import copy
import hashlib
from decimal import Decimal
from typing import List, Dict, Generator

import utils.refs
import utils.utils
from netex import Codespace, ServiceLink, RouteLink, RoutePoint, RoutePointRefStructure, TimingLink, PointRefStructure, \
    TimingPoint, ScheduledStopPoint, TimingPointVersionStructure, ScheduledStopPointRefStructure, Route, \
    PointProjection, ServiceJourneyPattern, StopPointInJourneyPattern, PosList, ServiceLinkRefStructure
from netexio.database import Database
from netexio.dbaccess import load_local


class RoutesProfile:
    @staticmethod
    def route_point_projection(ssp: ScheduledStopPoint) -> Generator[RoutePointRefStructure, None, None]:
        if ssp.projections:
            for projection in ssp.projections.projection_ref_or_projection:
                if isinstance(projection, PointProjection):
                    if projection.project_to_point_ref:
                        if projection.project_to_point_ref.name_of_ref_class == 'RoutePoint':
                            yield utils.project(projection.project_to_point_ref, RoutePointRefStructure)

    @staticmethod
    def projectRouteToServiceLinks(db: Database, sjp: ServiceJourneyPattern, route: Route, route_point_projection: Dict[str, RoutePointRefStructure], generator_defaults: dict) -> Generator[ServiceLink, None, ServiceJourneyPattern]:
        # Two variants:
        # 1. pointsInSequence has PointONRoute/OnwardRouteLinkRef;
        # 2. linksInSequence
        # We aggregate to linksInSequence

        if route.points_in_sequence:
            links_in_sequence = [por.onward_route_link_ref for por in route.points_in_sequence.point_on_route if por.onward_route_link_ref]
            # TODO: #142
            route_links_in_sequence: List[RouteLink] = [load_local(db, RouteLink, filter=lis.ref, cursor=True, limit=1, cache=False)[0] for lis in links_in_sequence]
            route_i = 0

            if sjp.points_in_sequence:
                spijps = [spijp for spijp in sjp.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern if isinstance(spijp, StopPointInJourneyPattern)]
                for i in range(0, len(spijps) - 1):
                    from_ssp = spijps[i].scheduled_stop_point_ref
                    to_ssp = spijps[i + 1].scheduled_stop_point_ref

                    # Find a routelink that matches from + to (via route_point_projection)
                    combined_route_links = []
                    success = False
                    for j in range(route_i, len(route_links_in_sequence)):
                        if route_links_in_sequence[j].from_point_ref.ref != route_point_projection[from_ssp.ref].ref:
                            continue
                        else:
                            for k in range(j, len(route_links_in_sequence)):
                                combined_route_links.append(route_links_in_sequence[k])
                                if route_links_in_sequence[k].to_point_ref.ref != route_point_projection[to_ssp.ref].ref:
                                    continue
                                else:
                                    success = True
                                    route_i = k + 1
                                    break
                            break

                    if success:
                        sl = None
                        # TODO: create separate function
                        if len(combined_route_links) == 0:
                            continue
                        elif len(combined_route_links) == 1:
                            # This is magic, so ServiceLinks with the same SSPs and same LineString are not duplicated
                            m = hashlib.sha256()
                            m.update(('\n'.join([from_ssp.ref, to_ssp.ref, ' '.join([str(x) for x in combined_route_links[0].line_string.pos_or_point_property_or_pos_list[0].value])])).encode('utf-8'))

                            object_id = m.hexdigest()[0:8].upper()
                            combined_route_links[0].line_string.id = "LineString_" + object_id

                            sl = ServiceLink(id=refs.getId(ServiceLink, generator_defaults['codespace'], id=object_id),
                                             version=route.version,
                                             distance=combined_route_links[0].distance,
                                             from_point_ref=utils.project(from_ssp, ScheduledStopPointRefStructure), to_point_ref=utils.project(to_ssp, ScheduledStopPointRefStructure),
                                             line_string=combined_route_links[0].line_string,
                                             derived_from_object_ref=combined_route_links[0].id,
                                             derived_from_version_ref_attribute=combined_route_links[0].version)
                        else:
                            # TODO: pos to poslist, and assure single poslist
                            if isinstance(combined_route_links[0].line_string.pos_or_point_property_or_pos_list[0], PosList):
                                line_string = copy.deepcopy(combined_route_links[0].line_string)

                                # TODO: setup the defaults when importing
                                if line_string.srs_dimension is None:
                                    line_string.srs_dimension = 2

                                line_string_value = combined_route_links[0].line_string.pos_or_point_property_or_pos_list[0].value
                                distance: Decimal = Decimal(0)

                                for k in range(1, len(combined_route_links)):
                                    if (not isinstance(combined_route_links[k].line_string.pos_or_point_property_or_pos_list[0], PosList)) or \
                                            line_string.pos_or_point_property_or_pos_list[0].__class__ != combined_route_links[k].line_string.pos_or_point_property_or_pos_list[0].__class__ or \
                                            line_string.srs_name != combined_route_links[k].line_string.srs_name or \
                                            line_string.srs_dimension != (combined_route_links[k].line_string.srs_dimension or 2) or \
                                            len(combined_route_links[k].line_string.pos_or_point_property_or_pos_list) > 1:
                                        break
                                    elif line_string_value[-line_string.srs_dimension:0] != combined_route_links[k].line_string.pos_or_point_property_or_pos_list[0].value[0:line_string.srs_dimension]:
                                        break
                                    else:
                                        line_string_value += combined_route_links[k].line_string.pos_or_point_property_or_pos_list[0].value[line_string.srs_dimension:]
                                        distance += combined_route_links[k].distance

                                # This is magic, so ServiceLinks with the same SSPs and same LineString are not duplicated
                                m = hashlib.sha256()
                                m.update(('\n'.join([from_ssp.ref, to_ssp.ref, ' '.join([str(x) for x in line_string.pos_or_point_property_or_pos_list[0].value])])).encode('utf-8'))

                                object_id = m.hexdigest()[0:8].upper()
                                line_string.id = "LineString_" + object_id

                                sl = ServiceLink(id=refs.getId(ServiceLink, generator_defaults['codespace'], id=object_id),
                                                 version=route.version,
                                                 distance=distance,
                                                 from_point_ref=utils.project(from_ssp, ScheduledStopPointRefStructure), to_point_ref=utils.project(to_ssp, ScheduledStopPointRefStructure),
                                                 line_string=line_string,
                                                 derived_from_object_ref=route.id,
                                                 derived_from_version_ref_attribute=route.version)


                        if sl:
                            spijps[i].onward_service_link_ref = refs.getRef(sl, ServiceLinkRefStructure)

                            # TODO: Decide when to remove these things, when they are outside of the profile
                            spijps[i].onward_timing_link_ref = None
                            yield sl

                sjp.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern = spijps
                return sjp

    @staticmethod
    def projectServiceLinkToRouteLink(service_link: ServiceLink, route_point_refs: Dict[str, RoutePointRefStructure]) -> RouteLink:
        route_link: RouteLink = utils.project(service_link, RouteLink)

        # Because the input has 'non-compatible from and to' we have to project these as well
        # here we do assume that all points have been taken care of for us.
        route_link.from_point_ref = route_point_refs.get(route_link.from_point_ref.ref)
        route_link.to_point_ref = route_point_refs.get(route_link.to_point_ref.ref)

        return route_link

    @staticmethod
    def projectTimingLinkToRouteLink(timing_link: TimingLink, route_point_refs: Dict[str, RoutePointRefStructure]) -> RouteLink:
        route_link: RouteLink = utils.project(timing_link, RouteLink)

        # Because the input has 'non-compatible from and to' we have to project these as well
        # here we do assume that all points have been taken care of for us.
        route_link.from_point_ref = route_point_refs.get(route_link.from_point_ref.ref)
        route_link.to_point_ref = route_point_refs.get(route_link.to_point_ref.ref)

        return route_link


    # TODO: The input for this function is using a TimingPointVersionStructure, because a TimingLink may refer to a
    # ScheduledStopPoint or a TimingPoint. We are using str matching now on id (and ref), in our ideal case we
    # would use references that take in account the class of the object as well. For this we would need to have a
    # "magic" RefStructure that is consistently hashable.
    #
    # TODO: Ideally we would like to use the ContainerStructures as dicts.

    @staticmethod
    def projectLinksToRouteLinks(service_links: List[ServiceLink], timing_links: List[TimingLink], timing_points: List[TimingPointVersionStructure], route_points: List[RoutePoint], route_links: List[RouteLink]):
        route_links_index: Dict[str, RouteLink] = {}
        route_point_refs: Dict[str, RoutePointRefStructure] = {}

        route_points_index = refs.getIndex(route_points)
        timing_points_index = refs.getIndex(timing_points)

        for route_point in route_points_index:
            route_point_refs[route_point.id] = refs.getRef(route_point)

        for service_link in service_links:
            route_point: RoutePoint = utils.project(timing_points_index[service_link.from_point_ref.ref], RoutePoint)
            if route_point.id not in route_points_index:
                route_points_index[route_point.id] = route_point
                route_point_refs[route_point.id] = refs.getRef(route_point)

        for timing_link in timing_links:
            route_point: RoutePoint = utils.project(timing_points_index[timing_link.from_point_ref.ref], RoutePoint)
            if route_point.id not in route_points_index:
                route_points_index[route_point.id] = route_point
                route_point_refs[route_point.id] = refs.getRef(route_point)

        for service_link in service_links:
            route_link = RoutesProfile.projectServiceLinkToRouteLink(service_link, route_point_refs)
            if route_link.id in route_links_index:
                print(f"{route_link.id} is already part of the set.")

            route_links_index[route_link.id] = route_link

        for timing_link in timing_links:
            route_link = RoutesProfile.projectTimingLinkToRouteLink(timing_link, route_point_refs)
            if route_link.id in route_links_index:
                print(f"{route_link.id} is already part of the set.")

            route_links_index[route_link.id] = route_link

        route_links.clear()
        route_links += route_links_index.values()

