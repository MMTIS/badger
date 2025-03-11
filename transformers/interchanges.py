from typing import Generator

from netex import InterchangeRule, InterchangeRuleParameterStructure, StopAreaRef, StopPlaceRef, ScheduledStopPointRef, \
    StopPlace, PassengerStopAssignment, ServiceJourney, LineRef, FlexibleLineRef, ServiceJourneyPattern, RouteView, \
    Route, RouteRef, ServiceJourneyRef, FareScheduledStopPointRef, ServiceJourneyInterchange, \
    ServiceJourneyRefStructure, ScheduledStopPointRefStructure, VehicleJourneyRefStructure
from netexio.database import Database
from netexio.dbaccess import load_local, load_generator
from utils.utils import project, projectRef
from utils.aux_logging import *

def getIdOrRef(obj):
    if hasattr(obj, 'ref'):
        return obj.ref
    else:
        return obj.id

def stop_place_to_ssp(db: Database) -> dict[str, str]:
    # TODO: This function also takes the naive string approach, must be rewritten using NeTEx references
    result = {}
    for psa in load_generator(db, PassengerStopAssignment):
        psa: PassengerStopAssignment
        ssp_id = getIdOrRef(psa.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point)
        if psa.taxi_rank_ref_or_stop_place_ref_or_stop_place is not None:
            id = getIdOrRef(psa.taxi_rank_ref_or_stop_place_ref_or_stop_place)
            l = result.get(id, [])
            l.append(ssp_id)
            result[id] = l

        if psa.taxi_stand_ref_or_quay_ref_or_quay is not None:
            id = getIdOrRef(psa.taxi_stand_ref_or_quay_ref_or_quay)
            l = result.get(id, [])
            l.append(ssp_id)
            result[id] = l

    for sp in load_generator(db, StopPlace):
        sp: StopPlace
        parent_added = result.get(sp.id, False)

        if sp.quays is not None:
            for q in sp.quays.taxi_stand_ref_or_quay_ref_or_quay:
                id = getIdOrRef(q)
                if id in result and not parent_added:
                    parent_added = result[id] # This is also too naive, there maybe 1:N assignments
                    l = result.get(id, [])
                    l.append(parent_added)
                    result[sp.id] = l
                elif id not in result and parent_added:
                    l = result.get(id, [])
                    l.append(parent_added)
                    result[id] = l

    return result

def apply_line_ref_to_sj(db: Database):
    result_route = {}
    for route in load_generator(db, Route):
        route: Route
        if route.line_ref is not None:
            result_route[route.id] = route.line_ref

    result_sjp = {}
    for sjp in load_generator(db, ServiceJourneyPattern):
        sjp: ServiceJourneyPattern
        if sjp.route_ref_or_route_view is not None:
            if isinstance(sjp.route_ref_or_route_view, RouteView) and (isinstance(sjp.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view, LineRef) or isinstance(sjp.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view, FlexibleLineRef)):
                result_sjp[sjp.id] = sjp.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view

            elif isinstance(sjp.route_ref_or_route_view, RouteRef) and sjp.route_ref_or_route_view.ref in result_route:
                result_sjp[sjp.id] = result_route[sjp.route_ref_or_route_view.ref]

    for sj in load_generator(db, ServiceJourney):
        sj: ServiceJourney

        if sj.journey_pattern_ref is not None and sj.journey_pattern_ref.ref in result_sjp:
            sj.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view = result_sjp[sj.journey_pattern_ref.ref]

        elif sj.route_ref is not None and sj.route_ref.ref in result_route:
            sj.flexible_service_properties_ref_or_flexible_service_properties = result_route[sj.route_ref.ref]

        yield sj

def get_all_stops(db: Database, sj: ServiceJourney):
    if sj.calls is not None:
        for call in sj.calls.call:
            if isinstance(call.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view, FareScheduledStopPointRef) or isinstance(call.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view, ScheduledStopPointRef):
                yield call.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view
    elif sj.journey_pattern_ref is not None:
        sjps = load_local(db, ServiceJourneyPattern, limit=1, filter=sj.journey_pattern_ref.ref)
        if len(sjps) > 0:
            sjp: ServiceJourneyPattern = sjps[0]
            if sjp.points_in_sequence:
                for pis in sjp.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern:
                    yield pis.scheduled_stop_point_ref

def interchange_rules_to_service_journey_interchanges(db: Database) -> Generator[ServiceJourneyInterchange, None, None]:
    # Interchange Rules use StopPlaces, hence we need a mapping from StopPlace to all ScheduledStopPoints.
    # Given that ScheduledStopPoints may be mapped indirectly via Quay, that mapping should be done too.
    # For every StopPlace there will be a candidate ScheduledStopPoint list.

    # In this situation we can do a direct mapping:
    # scheduled_stop_point_ref + stop_point_ref

    # From our embedding table we can directly derive what strategy would be required

    sp_ssp = {}

    """
    sp = []
    for _, ref in load_references_generator(db, InterchangeRule):
        # Get all StopPlaces referenced by all InterchangeRules
        if isinstance(ref, StopPlaceRef):
            sp.append(ref)

        # TODO: handle potential other adjacent variants
        # Get all StopAreas referenced by all InterchangeRules
        # elif isinstance(ref, StopAreaRef):
        #    sa.append(ref)
    
    if len(sp) > 0:    
    """

    # StopPlaces have been referenced, we must now compute the StopPlace to ScheduledStopPoint hierarchy
    sp_ssp = stop_place_to_ssp(db)

    for interchange_rule in load_generator(db, InterchangeRule):
        interchange_rule: InterchangeRule

        if interchange_rule.feeder_filter.service_journey_ref_or_journey_designator_or_service_designator and isinstance(interchange_rule.feeder_filter.service_journey_ref_or_journey_designator_or_service_designator, ServiceJourneyRefStructure):
            sj_ref = interchange_rule.feeder_filter.service_journey_ref_or_journey_designator_or_service_designator

            if interchange_rule.feeder_filter.stop_place_ref is not None:
                all_ssps = sp_ssp.get(interchange_rule.feeder_filter.stop_place_ref.ref, [])

                sjs = load_local(db, ServiceJourney, filter=sj_ref.ref, cursor=True)
                if len(sjs) > 0:
                    for ssp_ref in get_all_stops(db, sjs[0]):
                        if ssp_ref.ref in all_ssps:
                            interchange_rule.feeder_filter.scheduled_stop_point_ref = ssp_ref
                            break
                else:
                    log_once(logging.WARN, "sj","We cannot find {sj_ref.ref} for {interchange_rule}.")

        else:
            log_once(logging.WARN, "sj-2","TODO: implement non-servicejourneyref interchange")


        if interchange_rule.distributor_filter.service_journey_ref_or_journey_designator_or_service_designator and isinstance(interchange_rule.distributor_filter.service_journey_ref_or_journey_designator_or_service_designator, ServiceJourneyRefStructure):
            sj_ref = interchange_rule.distributor_filter.service_journey_ref_or_journey_designator_or_service_designator

            if interchange_rule.distributor_filter.stop_place_ref is not None:
                all_ssps = sp_ssp.get(interchange_rule.distributor_filter.stop_place_ref.ref, [])

                sjs = load_local(db, ServiceJourney, filter=sj_ref.ref, cursor=True)
                if len(sjs) > 0:
                    for ssp_ref in get_all_stops(db, sjs[0]):
                        if ssp_ref.ref in all_ssps:
                            interchange_rule.distributor_filter.scheduled_stop_point_ref = ssp_ref
                            break
                else:
                    log_once(logging.WARN,"sj-2""We cannot find {sj_ref.ref} for {interchange_rule}.")

        else:
            log_once(logging.WARN, "sj-3","TODO: implement non-servicejourneyref interchange")

        if isinstance(interchange_rule.feeder_filter.service_journey_ref_or_journey_designator_or_service_designator, ServiceJourneyRefStructure) and \
            isinstance(interchange_rule.distributor_filter.service_journey_ref_or_journey_designator_or_service_designator, ServiceJourneyRefStructure) and \
            isinstance(interchange_rule.feeder_filter.scheduled_stop_point_ref, ScheduledStopPointRef) and \
            isinstance(interchange_rule.distributor_filter.scheduled_stop_point_ref, ScheduledStopPointRef):
            sji: ServiceJourneyInterchange = project(interchange_rule, ServiceJourneyInterchange,
                                                     from_journey_ref=projectRef(interchange_rule.feeder_filter.service_journey_ref_or_journey_designator_or_service_designator, VehicleJourneyRefStructure),
                                                     to_journey_ref=projectRef(interchange_rule.distributor_filter.service_journey_ref_or_journey_designator_or_service_designator, VehicleJourneyRefStructure),
                                                     from_point_ref=projectRef(interchange_rule.feeder_filter.scheduled_stop_point_ref, ScheduledStopPointRefStructure),
                                                     to_point_ref=projectRef(interchange_rule.distributor_filter.scheduled_stop_point_ref, ScheduledStopPointRefStructure))
            yield sji