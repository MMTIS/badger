from pathlib import Path
from typing import Generator, Any, Set

from mdbx.mdbx import TXN

from domain.netex.model import (
    Codespace,
    ScheduledStopPoint,
    Operator,
    ResponsibilitySet,
    Direction,
    DataSource,
    Authority,
    ValueSet,
    TransportAdministrativeZone,
    TopographicPlace,
    Network,
    DestinationDisplay,
    VehicleType,
    VersionFrameDefaultsStructure, ServiceJourneyPattern, StopPointInJourneyPattern, Line,
    TransportOrganisationRefsRelStructure, ServiceJourney, FlexibleLineRef, LineRef, RouteView, RouteRef, Route,
)
# from netexio.database import Database
# from netexio.dbaccess import setup_database, copy_table, missing_class_update, load_generator
# from netexio.pickleserializer import MyPickleSerializer

from storage.mdbx.core.implementation import MdbxStorage
# from utils.utils import get_interesting_classes
import logging


"""
from transformers.direction import infer_directions_from_sjps_and_apply
from transformers.projection import reprojection_update
from transformers.scheduledstoppoint import (
    infer_locations_from_quay_or_stopplace_and_apply,
)

from transformers.epip import (
    epip_line_memory,
    epip_site_frame_memory,
    epip_service_journey_generator,
    epip_service_journey_interchange,
    epip_interchange_rule,
    epip_service_calendar,
)
"""
# from utils.profiles import EPIP_CLASSES
from utils.aux_logging import log_all, prepare_logger
from configuration import defaults

generator_defaults = {
    "codespace": Codespace(id="codespace", xmlns=str(defaults["codespace"])),
    "version": defaults["version"],
}  # Invent something, that materialises the refs, so VersionFrameDefaultsStructure can be used


def epip_line_memory(source_db: MdbxStorage, txn_read: TXN, generator_defaults: dict[str, Any]) -> Generator[Line, None, None]:
    for _key, line in source_db.iter_objects(txn_read, Line):
        line.branding_ref = None
        line.type_of_service_ref = None
        line.type_of_product_category_ref = None
        if line.operator_ref is not None and line.authority_ref is not None:
            if generator_defaults.get('authority_reference', False):
                if line.additional_operators and line.additional_operators.transport_organisation_ref:
                    line.additional_operators.transport_organisation_ref.append(line.operator_ref)
                else:
                    line.additional_operators = TransportOrganisationRefsRelStructure(
                        transport_organisation_ref=[line.operator_ref])
                line.operator_ref = None
            else:
                if line.additional_operators and line.additional_operators.transport_organisation_ref:
                    line.additional_operators.transport_organisation_ref.append(line.authority_ref)
                else:
                    line.additional_operators = TransportOrganisationRefsRelStructure(
                        transport_organisation_ref=[line.authority_ref])
                line.authority_ref = None
        yield line


def epip_service_journey_generator(db_read: MdbxStorage, db_write: MdbxStorage, generator_defaults: dict[str, Any]) -> None:
    # print(sys._getframe().f_code.co_name)
    # sjps: Dict[str, ServiceJourneyPattern] = {}
    sjp_ids: Set[str] = set()
    availability_conditions_ids: Set[str] = set()
    day_types_ids: Set[str] = set()
    uic_operating_periods_ids: Set[str] = set()
    day_type_assignments_ids: Set[str] = set()

    # availability_conditions: Dict[str, AvailabilityCondition] = {}
    # day_types: Dict[str, DayType] = {}
    # uic_operating_periods: List[UicOperatingPeriod] = []
    # day_type_assignments: List[DayTypeAssignment] = []

    def recover_line_ref(service_journey: ServiceJourney, service_journey_pattern: ServiceJourneyPattern, db_read: MdbxStorage) -> None:
        sj_line_ref = None
        if service_journey.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view is not None and (
            isinstance(service_journey.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view, FlexibleLineRef)
            or isinstance(service_journey.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view, LineRef)
        ):
            sj_line_ref = service_journey.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view

        if service_journey_pattern.route_ref_or_route_view is not None:
            if isinstance(service_journey_pattern.route_ref_or_route_view, RouteView):
                if isinstance(service_journey_pattern.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view, LineRef) or isinstance(
                    service_journey_pattern.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view, FlexibleLineRef
                ):
                    # There is already an existing LineRef, overwriting is not smart.
                    pass
                else:
                    if sj_line_ref is not None:
                        service_journey_pattern.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view = sj_line_ref
                    else:
                        print("RouteView: Other options to recover line not available")

            elif isinstance(service_journey_pattern.route_ref_or_route_view, RouteRef):
                route: Route = db_read.load_object(
                    Route, service_journey_pattern.route_ref_or_route_view.ref, service_journey_pattern.route_ref_or_route_view.version
                )
                service_journey_pattern.route_ref_or_route_view = RouteView(flexible_line_ref_or_line_ref_or_line_view=route.line_ref)

                if service_journey_pattern.direction_type is None and route.direction_type is not None:
                    service_journey_pattern.direction_type = route.direction_type.value

                if service_journey_pattern.direction_ref_or_direction_view is None:
                    service_journey_pattern.direction_ref_or_direction_view = route.direction_ref

                if service_journey_pattern.distance is None:
                    route.distance = service_journey_pattern.distance

        else:
            service_journey_pattern.route_ref_or_route_view = RouteView(flexible_line_ref_or_line_ref_or_line_view=sj_line_ref)

    def process(sj: ServiceJourney, db_read: MdbxStorage, db_write: MdbxStorage, generator_defaults: dict[str, Any]) -> ServiceJourney:
        sj: ServiceJourney

        # Prototype, just: TimeDemandType -> PassingTimes
        service_journey_pattern: ServiceJourneyPattern = None

        if sj.passing_times:
            if sj.journey_pattern_ref.ref not in sjp_ids:
                service_journey_pattern = db_read.get_single(ServiceJourneyPattern, sj.journey_pattern_ref.ref, sj.journey_pattern_ref.version)

                # Since we don't do it ourselves, we might want to check the poor input offered.
                infer_id_and_order_and_apply(sj)

        elif sj.calls:
            if sj.journey_pattern_ref:
                pass
                # service_journey_pattern: ServiceJourneyPattern = db_read.get_single(ServiceJourneyPattern,
                #                                                            sj.journey_pattern_ref.ref,
                #                                                            sj.journey_pattern_ref.version)
            else:
                service_journey_pattern = service_journey_pattern_from_calls(sj, generator_defaults)
                sj.journey_pattern_ref = getRef(service_journey_pattern)

            sj.passing_times = TimetabledPassingTimesRelStructure(
                timetabled_passing_time=TimetablePassingTimesProfile.getTimetabledPassingtimesFromCalls(sj, service_journey_pattern)
            )

        elif sj.time_demand_type_ref:
            service_journey_pattern: ServiceJourneyPattern = db_read.get_single(
                ServiceJourneyPattern, sj.journey_pattern_ref.ref, sj.journey_pattern_ref.version
            )
            time_demand_type: TimeDemandType = db_read.get_single(TimeDemandType, sj.time_demand_type_ref.ref, sj.time_demand_type_ref.version)
            CallsProfile.getPassingTimesFromTimeDemandType(sj, service_journey_pattern, time_demand_type)

        # If we already know that this generated SJP already exists, we should not even add it.
        if sj.journey_pattern_ref.ref in sjp_ids:
            pass

        elif service_journey_pattern is None:
            log_all(logging.ERROR, f'No service journey pattern for journey: {sj} {sj.journey_pattern_ref}')

        if service_journey_pattern is not None and service_journey_pattern.id not in sjp_ids:

            # TODO: Here we need to add it to the new database
            # sjps[service_journey_pattern.id] = service_journey_pattern

            if len(route_point_projection) > 0:
                if isinstance(service_journey_pattern.route_ref_or_route_view, RouteRef):
                    routes: list[Route] = load_local(
                        db_read, Route, limit=1, filter_id=service_journey_pattern.route_ref_or_route_view.ref, cursor=True, cache=False
                    )
                    if len(routes) > 0:
                        for sl in RoutesProfile.projectRouteToServiceLinks(
                            db_read, service_journey_pattern, routes[0], route_point_projection, generator_defaults
                        ):
                            db_write.insert_one_object(sl)

            service_journey_pattern.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern = [
                pis
                for pis in service_journey_pattern.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern
                if isinstance(pis, StopPointInJourneyPattern)
            ]

            # Ater the Routes to ServiceLinks!
            recover_line_ref(sj, service_journey_pattern, db_read)

            # TODO Issue #242: handle LinkSequenceProjectionRef / LinkSequenceProjection

            db_write.insert_one_object(service_journey_pattern)

            # TODO: We might be able to avoid it if we work with prefix keys
            sjp_ids.add(service_journey_pattern.id)

        # service_journey_ac_to_day_type(sj, availability_conditions, day_types, uic_operating_periods, day_type_assignments)
        service_journey_ac_to_day_type(db_read, db_write, sj, availability_conditions_ids, day_types_ids, uic_operating_periods_ids, day_type_assignments_ids)

        # TODO: AvailabilityCondition -> Uic

        sj.validity_conditions_or_valid_between = []
        sj.time_demand_type_ref = None
        sj.key_list = None
        sj.private_code = None
        sj.train_numbers = None
        sj.extensions = None
        sj.notice_assignments = None
        sj.calls = None
        sj.link_sequence_projection_ref_or_link_sequence_projection = None
        sj.journey_pattern_view = None
        sj.direction_type = None

        # TODO: prevent caching altogether?
        # db_read.clean_cache()
        return sj

    def query(db_read: MdbxStorage, txn: TXN) -> Generator[ServiceJourney, None, None]:
        for _key, sj in db_read.iter_objects(txn, ServiceJourney):
            print(_key)
            # yield process(sj, db_read, txn, db_write, txn_write, generator_defaults)

        if False:
            yield ServiceJourney()

        # _load_generator = load_generator(db_read, ServiceJourney, embedding=False, cache=False)
        # for sj in _load_generator:
        #     yield process(sj, db_read, db_write, generator_defaults)
        # for sj in pool.imap_unordered(partial(process, read_database=read_database, write_database=write_database, generator_defaults=generator_defaults), _load_generator, chunksize=100):
        #     yield sj

    # TODO: At this point we should have a check to know if the ServiceJourneyPattern is geographically enabled, or not

    log_all(logging.INFO, "Indexing RoutePoint to ScheduledStopPoint ")
    # route_point_projection = {}
    # for ssp in load_generator(db_read, ScheduledStopPoint):
    #     rp_to_ssp = list(RoutesProfile.route_point_projection(ssp))
    #     if len(rp_to_ssp) > 0:
    #         route_point_projection[getRef(ssp).ref] = rp_to_ssp[0]

    # log_all(logging.INFO, "Indexing AvailabilityConditions " + str(memory_usage(-1, interval=.1, timeout=1)[0]))
    # vailability_conditions = getIndex(load_local(db_read, AvailabilityCondition))

    log_all(logging.INFO, "Service journeys for now ")
    # db_write.insert_objects_on_queue(ServiceJourney, query(db_read), True)

    with db_read.env.ro_transaction() as txn_read:
        db_write.insert_objects_on_queue(ServiceJourney, query(db_read, txn_read), True)


def main(source_database_file: Path, target_database_file: Path) -> None:
    with MdbxStorage(target_database_file,readonly=False) as target_db:
        with MdbxStorage(source_database_file, readonly=True) as source_db:
            epip_service_journey_generator(source_db, target_db, generator_defaults)

            with source_db.env.ro_transaction() as txn_read:
                pass
                """
                for clazz in [
                    Codespace,
                    Direction,
                    DataSource,
                    Authority,
                    Operator,
                    ValueSet,
                    TransportAdministrativeZone,
                    VehicleType,
                    ResponsibilitySet,
                    TopographicPlace,
                    Network,
                    DestinationDisplay,
                ]:
                    with target_db.env.rw_transaction() as txn_write:
                        # We need to have something like a backwards compatible copy,
                        # that takes the MultilingualString and only uses the features of NeTEx 1.3
                        # Obviously, much more expensive to check, likely want metadata that we are dealing with NeTex 2.0 as source

                        try:
                            source_db.copy_map(txn_read, target_db, txn_write, clazz)
                            txn_write.commit()
                        except:
                            pass
                """

                # target_db.insert_objects_on_queue(Line, epip_line_memory(source_db, txn_read, {}), True)




"""
    classes = get_interesting_classes(EPIP_CLASSES)

    with Database(
        target_database_file,
        serializer=MyPickleSerializer(compression=True),
        readonly=False,
        initial_size=8 * 1024 ** 3
    ) as target_db:
        # setup_database(target_db, classes, True)

        with Database(source_database_file, MyPickleSerializer(compression=True), readonly=True) as source_db:

            # TODO: make this more generic

            default_codespace: Codespace | None = None
            frame_defaults: VersionFrameDefaultsStructure
            for frame_defaults in source_db.get_metadata(None, None, VersionFrameDefaultsStructure):
                if default_codespace is None and frame_defaults.default_codespace_ref:
                    default_codespace_ref = frame_defaults.default_codespace_ref
                    default_codespace = source_db.get_single(Codespace, default_codespace_ref.ref, None)
                    if default_codespace:
                        generator_defaults['codespace'] = default_codespace

            log_all(logging.INFO, "Copy all tables as-is ")
            copy_table(
                source_db,
                target_db,
                [
                    Codespace,
                    Direction,
                    DataSource,
                    Authority,
                    Operator,
                    ValueSet,
                    TransportAdministrativeZone,
                    VehicleType,
                    ResponsibilitySet,
                    TopographicPlace,
                    Network,
                    DestinationDisplay,
                ],
                clean=True,
                embedding=True,
            )
            source_db.clean_cache()

            log_all(logging.INFO, "Copy lines, in EPIP style ")
            epip_line_memory(source_db, target_db, generator_defaults)
            source_db.clean_cache()

            log_all(logging.INFO, "Fix Quay / StopPlace locations ")
            infer_locations_from_quay_or_stopplace_and_apply(source_db, target_db, generator_defaults)
            source_db.clean_cache()
            # # epip_scheduled_stop_point_memory(target_db, target_db, generator_defaults)

            log_all(logging.INFO, "Investigate this site frame step ")
            epip_site_frame_memory(source_db, target_db, generator_defaults)
            source_db.clean_cache()

            log_all(logging.INFO, "Service journeys ")
            epip_service_journey_generator(source_db, target_db, generator_defaults, None, cache=False)
            source_db.clean_cache()

            log_all(logging.INFO, "Calendars ")
            epip_service_calendar(source_db, target_db, generator_defaults)
            source_db.clean_cache()

            log_all(logging.INFO, "ServiceJourneyInterchange additions ")
            epip_service_journey_interchange(source_db, target_db, generator_defaults)
            source_db.clean_cache()

            log_all(logging.INFO, "InterchangeRule additions ")
            epip_interchange_rule(source_db, target_db, generator_defaults)
            source_db.clean_cache()

            target_db.block_until_done()

            log_all(logging.INFO, "Infer directions from ServiceJourneyPatterns, and apply ")
            infer_directions_from_sjps_and_apply(target_db, target_db, generator_defaults)
            source_db.clean_cache()
            # TODO: epip_noticeassignment(source_db, target_db, generator_defaults)

            target_db.block_until_done()

            log_all(logging.INFO, "Copy remaining classes ")
            missing_class_update(source_db, target_db)

            target_db.block_until_done()

            log_all(logging.INFO, "Reprojection Update ")
            reprojection_update(target_db, "urn:ogc:def:crs:EPSG::4326")
"""

if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Transform the input into mandatory objects for the export of EPIP")
    parser.add_argument("source", type=str, help="lmdb file to use as input of the transformation.")
    parser.add_argument(
        "target",
        type=str,
        help="lmdb file to overwrite and store contents of the transformation.",
    )
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    source_path = Path(args.source)
    if not source_path.exists():
        log_all(logging.ERROR, "{source_path} does not exist.")

    else:
        try:
            main(source_path, Path(args.target))
        except Exception as e:
            log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
            raise e
