from pathlib import Path
from typing import Generator

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
    ServiceJourneyPattern,
    Line,
    ServiceJourney,
    ServiceLink,
    ServiceCalendar,
    Connection,
    DayType,
    DayTypeAssignment,
    DefaultConnection,
    FlexibleLine,
    Notice,
    PassengerStopAssignment,
    ServiceJourneyInterchange,
    SiteConnection,
    StopPlace,
    TariffZone,
    UicOperatingPeriod,
    QuayRef,
    EntityStructure,
    StopPlaceRef,
)
from domain.netex.services.refs import getRef

from storage.mdbx.core.implementation import MdbxStorage

import logging
from utils.aux_logging import log_all, prepare_logger
from configuration import defaults


from storage.mdbx.core.references import resolve_embeddings_iterable
from transformers.epip import (
    epip_service_calendar,
    epip_line_generator,
    epip_service_journey_generator,
    epip_service_journey_interchange,
)
from transformers.ivu import avv_service_journey_operator, avv_vehicle_type_short_name, avv_quay_name
from transformers.projection import reprojection_update
from transformers.scheduledstoppoint import infer_locations_from_quay_or_stopplace_and_apply


from transformers.direction import infer_directions_from_sjps_and_apply

"""
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

generator_defaults = {
    "codespace": Codespace(id="codespace", xmlns=str(defaults["codespace"])),
    "version": defaults["version"],
}  # Invent something, that materialises the refs, so VersionFrameDefaultsStructure can be used

# EPIP
other_referenced_classes: set[type[EntityStructure]] = {
    Authority,
    Connection,
    DayType,
    DayTypeAssignment,
    DataSource,
    DefaultConnection,
    DestinationDisplay,
    Direction,
    FlexibleLine,
    Line,
    Network,
    Notice,
    Operator,
    PassengerStopAssignment,
    ResponsibilitySet,
    ScheduledStopPoint,
    ServiceCalendar,
    ServiceJourney,
    ServiceJourneyInterchange,
    ServiceJourneyPattern,
    ServiceLink,
    SiteConnection,
    # StopPlace,
    TariffZone,
    TopographicPlace,
    TransportAdministrativeZone,
    UicOperatingPeriod,
    ValueSet,
    VehicleType,
}


def epip_db_to_db(source_database_file: Path, target_database_file: Path) -> None:
    with MdbxStorage(target_database_file, readonly=False) as target_db:
        with target_db.env.rw_transaction() as txn_write:
            with MdbxStorage(source_database_file, readonly=False) as source_db:
                # This will deembed anything on the ServiceCalendar
                with source_db.env.rw_transaction() as txn_write1:

                    def all_embeddings() -> Generator[EntityStructure, None, None]:
                        for _, _, embedding in resolve_embeddings_iterable(source_db, txn_write1, ServiceCalendar):
                            id, obj, path = embedding
                            yield obj

                    source_db.insert_any_object_on_queue(txn_write1, all_embeddings())
                    txn_write1.commit()

                with source_db.env.rw_transaction() as txn_write1:

                    def all_psas() -> Generator[PassengerStopAssignment, None, None]:
                        quay_sp = {
                            embedding[1].id: getRef(obj, StopPlaceRef) for key, obj, embedding in resolve_embeddings_iterable(source_db, txn_write1, StopPlace)
                        }

                        for psa in source_db.iter_only_objects(txn_write1, PassengerStopAssignment):
                            if psa.taxi_rank_ref_or_stop_place_ref_or_stop_place is not None:
                                continue

                            if isinstance(psa.taxi_stand_ref_or_quay_ref_or_quay, QuayRef):
                                sp_ref = quay_sp.get(psa.taxi_stand_ref_or_quay_ref_or_quay.ref, None)
                                if sp_ref:
                                    psa.taxi_rank_ref_or_stop_place_ref_or_stop_place = sp_ref

                            yield psa

                    source_db.insert_any_object_on_queue(txn_write1, all_psas())
                    txn_write1.commit()

                with source_db.env.ro_transaction() as txn_read:
                    # To facilitate that our target EPIP database will have all objects that are internally referenced,
                    # we will now take our original database, fetch all the references for the classes that we will have
                    # in the EPIP database, copied or created.
                    # TODO: We don't have to insert classes which are later directly copied or created,

                    target_db.insert_any_object_on_queue(txn_write, source_db.fetch_all_references_by_class(txn_read, other_referenced_classes, False))

                    for clazz in [
                        Codespace,
                        # Direction,
                        DataSource,
                        # Authority,
                        # Operator,
                        # ValueSet,
                        # TransportAdministrativeZone,
                        # VehicleType,
                        ResponsibilitySet,
                        # TopographicPlace,
                        # Network,
                        # DestinationDisplay,
                        PassengerStopAssignment,
                    ]:
                        # We need to have something like a backwards compatible copy,
                        # that takes the MultilingualString and only uses the features of NeTEx 1.3
                        # Obviously, much more expensive to check, likely want metadata that we are dealing with NeTex 2.0 as source

                        source_db.copy_map(txn_read, target_db, txn_write, clazz)

                    target_db.insert_any_object_on_queue(txn_write, epip_line_generator(source_db, txn_read, generator_defaults))

                    target_db.insert_any_object_on_queue(txn_write, infer_locations_from_quay_or_stopplace_and_apply(source_db, txn_read, generator_defaults))

                    target_db.insert_any_object_on_queue(txn_write, epip_service_journey_generator(source_db, txn_read, generator_defaults))

                    target_db.insert_any_object_on_queue(txn_write, epip_service_calendar(source_db, txn_read, generator_defaults))

                    target_db.insert_any_object_on_queue(txn_write, epip_service_journey_interchange(source_db, txn_read, generator_defaults))

                    # target_db.insert_any_object_on_queue(txn_write, epip_interchange_rule(source_db, txn_read, generator_defaults))

                    target_db.insert_any_object_on_queue(txn_write, infer_directions_from_sjps_and_apply(target_db, txn_write, generator_defaults))

                    # Reprojection update would copy the entire database, instead we would actually want to filter what we introduce,
                    # If we take a national stop registry it will have all the points, but we would want to limit this to the objects that
                    # we would have queried.
                    target_db.insert_any_object_on_queue(txn_write, reprojection_update(target_db, txn_write, "urn:ogc:def:crs:EPSG::4326"))

                    target_db.insert_any_object_on_queue(txn_write, avv_service_journey_operator(target_db, txn_write))
                    target_db.insert_any_object_on_queue(txn_write, avv_vehicle_type_short_name(target_db, txn_write))
                    target_db.insert_any_object_on_queue(txn_write, avv_quay_name(target_db, txn_write))

            txn_write.commit()


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


def main(source: str, target: str) -> None:
    source_path = Path(source)
    if not source_path.exists():
        log_all(logging.ERROR, f"{source_path} does not exist.")

    else:
        epip_db_to_db(source_path, Path(target))


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Transform the input into mandatory objects for the export of EPIP")
    parser.add_argument("source", type=str, help="mdbx file to use as input of the transformation.")
    parser.add_argument(
        "target",
        type=str,
        help="mdbx file to overwrite and store contents of the transformation.",
    )
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.source, args.target)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
