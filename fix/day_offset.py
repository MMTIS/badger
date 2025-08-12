import logging

from netex import ServiceJourney
from netexio.database import Database
from netexio.pickleserializer import MyPickleSerializer
from netexio.signaledcursor import SignaledCursor
from utils.aux_logging import prepare_logger, log_all

# TODO: there could be a situation where 'sometimes' the offset is set, we would not be able to handle that variant


def fix_calls(service_journey: ServiceJourney):
    changed = False
    if service_journey.calls is None:
        return changed

    # This takes care of an arrival-departure at a stop
    for i in range(0, len(service_journey.calls.call)):
        arrival_time = service_journey.calls.call[i].arrival.time if service_journey.calls.call[i].arrival else service_journey.calls.call[i].departure.time
        arrival_day_offset = (
            service_journey.calls.call[i].arrival.day_offset
            if service_journey.calls.call[i].arrival
            else service_journey.calls.call[i].departure.day_offset or 0
        )
        departure_time = (
            service_journey.calls.call[i].departure.time if service_journey.calls.call[i].departure else service_journey.calls.call[i - 1].arrival.time
        )
        departure_day_offset = (
            service_journey.calls.call[i].departure.day_offset
            if service_journey.calls.call[i].departure
            else service_journey.calls.call[i - 1].arrival.day_offset or 0
        )
        if departure_time < arrival_time and departure_day_offset <= arrival_day_offset:
            changed |= True
            # Update the current call
            if service_journey.calls.call[i].departure:
                service_journey.calls.call[i].departure.day_offset = (service_journey.calls.call[i].departure.day_offset or 0) + 1

            # And everything after it
            for j in range(i + 1, len(service_journey.calls.call)):
                if service_journey.calls.call[j].arrival:
                    service_journey.calls.call[j].arrival.day_offset = (service_journey.calls.call[j].arrival.day_offset or 0) + 1
                if service_journey.calls.call[j].departure:
                    service_journey.calls.call[j].departure.day_offset = (service_journey.calls.call[j].departure.day_offset or 0) + 1

    # This takes care of an departure-arrival (or variants) between stops
    for i in range(1, len(service_journey.calls.call)):
        departure_time = (
            service_journey.calls.call[i - 1].departure.time if service_journey.calls.call[i - 1].departure else service_journey.calls.call[i - 1].arrival.time
        )
        departure_day_offset = (
            service_journey.calls.call[i - 1].departure.day_offset
            if service_journey.calls.call[i - 1].departure
            else service_journey.calls.call[i - 1].arrival.day_offset or 0
        )
        arrival_time = service_journey.calls.call[i].arrival.time if service_journey.calls.call[i].arrival else service_journey.calls.call[i].departure.time
        arrival_day_offset = (
            service_journey.calls.call[i].arrival.day_offset
            if service_journey.calls.call[i].arrival
            else service_journey.calls.call[i].departure.day_offset or 0
        )

        if departure_time > arrival_time and departure_day_offset >= arrival_day_offset:
            changed |= True
            for j in range(i, len(service_journey.calls.call)):
                if service_journey.calls.call[j].arrival:
                    service_journey.calls.call[j].arrival.day_offset = (service_journey.calls.call[j].arrival.day_offset or 0) + 1
                if service_journey.calls.call[j].departure:
                    service_journey.calls.call[j].departure.day_offset = (service_journey.calls.call[j].departure.day_offset or 0) + 1

    return changed


def fix_passing_times(service_journey: ServiceJourney):
    changed = False
    if service_journey.passing_times is None:
        return changed

    # This takes care of an arrival-departure at a stop
    for i in range(0, len(service_journey.passing_times.timetabled_passing_time)):
        arrival_time = (
            service_journey.passing_times.timetabled_passing_time[i].arrival_time
            if service_journey.passing_times.timetabled_passing_time[i].arrival_time
            else service_journey.passing_times.timetabled_passing_time[i].departure_time
        )
        arrival_day_offset = (
            service_journey.passing_times.timetabled_passing_time[i].arrival_day_offset
            if service_journey.passing_times.timetabled_passing_time[i].arrival_day_offset
            else service_journey.passing_times.timetabled_passing_time[i].departure_day_offset
        ) or 0
        departure_time = (
            service_journey.passing_times.timetabled_passing_time[i].departure_time
            if service_journey.passing_times.timetabled_passing_time[i].departure_time
            else service_journey.passing_times.timetabled_passing_time[i - 1].arrival_time
        )
        departure_day_offset = (
            service_journey.passing_times.timetabled_passing_time[i].departure_day_offset
            if service_journey.passing_times.timetabled_passing_time[i].departure_time
            else service_journey.passing_times.timetabled_passing_time[i - 1].arrival_day_offset
        ) or 0
        if departure_time < arrival_time and departure_day_offset <= arrival_day_offset:
            changed |= True
            # Update the current call
            if service_journey.passing_times.timetabled_passing_time[i].departure_time:
                service_journey.passing_times.timetabled_passing_time[i].departure_day_offset = (
                    service_journey.passing_times.timetabled_passing_time[i].departure_day_offset or 0
                ) + 1

            # And everything after it
            for j in range(i + 1, len(service_journey.passing_times.timetabled_passing_time)):
                if service_journey.passing_times.timetabled_passing_time[j].arrival_day_offset:
                    service_journey.passing_times.timetabled_passing_time[j].arrival_day_offset = (
                        service_journey.passing_times.timetabled_passing_time[j].arrival_day_offset or 0
                    ) + 1
                if service_journey.passing_times.timetabled_passing_time[j].departure_day_offset:
                    service_journey.passing_times.timetabled_passing_time[j].departure_day_offset = (
                        service_journey.passing_times.timetabled_passing_time[j].departure_day_offset or 0
                    ) + 1

    # This takes care of an departure-arrival (or variants) between stops
    for i in range(1, len(service_journey.passing_times.timetabled_passing_time)):
        departure_time = (
            service_journey.passing_times.timetabled_passing_time[i - 1].departure_time
            if service_journey.passing_times.timetabled_passing_time[i - 1].departure_time
            else service_journey.passing_times.timetabled_passing_time[i - 1].arrival_time
        )
        departure_day_offset = (
            service_journey.passing_times.timetabled_passing_time[i - 1].departure_day_offset
            if service_journey.passing_times.timetabled_passing_time[i - 1].departure_day_offset
            else service_journey.passing_times.timetabled_passing_time[i - 1].arrival_day_offset
        ) or 0
        arrival_time = (
            service_journey.passing_times.timetabled_passing_time[i].arrival_time
            if service_journey.passing_times.timetabled_passing_time[i].arrival_time
            else service_journey.passing_times.timetabled_passing_time[i].departure_time
        )
        arrival_day_offset = (
            service_journey.passing_times.timetabled_passing_time[i].arrival_day_offset
            if service_journey.passing_times.timetabled_passing_time[i].arrival_day_offset
            else service_journey.passing_times.timetabled_passing_time[i].departure_day_offset
        ) or 0

        if departure_time > arrival_time and departure_day_offset >= arrival_day_offset:
            changed |= True
            for j in range(i, len(service_journey.passing_times.timetabled_passing_time)):
                if service_journey.passing_times.timetabled_passing_time[j].arrival_time:
                    service_journey.passing_times.timetabled_passing_time[j].arrival_day_offset = (
                        service_journey.passing_times.timetabled_passing_time[j].arrival_day_offset or 0
                    ) + 1
                if service_journey.passing_times.timetabled_passing_time[j].departure_time:
                    service_journey.passing_times.timetabled_passing_time[j].departure_day_offset = (
                        service_journey.passing_times.timetabled_passing_time[j].departure_day_offset or 0
                    ) + 1

    return changed


def main(source_database_file: str):
    # This function tries to resolve invalid time order within ServiceJourneys for both Passing Times and Calls.
    with Database(source_database_file, MyPickleSerializer(compression=True), readonly=False) as source_db:
        service_journey: ServiceJourney
        for service_journey in SignaledCursor(ServiceJourney, source_db, readonly=False):
            changed = False
            changed |= fix_calls(service_journey)
            changed |= fix_passing_times(service_journey)

            if changed:
                source_db.insert_one_object(service_journey, False, False)


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Check an LMDB for missing references")
    parser.add_argument("source", type=str, help="lmdb file to use as input.")
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.source)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
