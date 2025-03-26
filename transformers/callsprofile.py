import logging
from typing import List

from xsdata.models.datatype import XmlDuration, XmlTime

from netex import (
    ServiceJourney,
    StopPointInJourneyPattern,
    ServiceJourneyPattern,
    PointsInJourneyPatternRelStructure,
    ServiceJourneyPatternRef,
    TimingLink,
    Line,
    TimingPointRefStructure,
    TimingLinkRefStructure,
    TimingLinkRef,
    Call,
    DepartureStructure,
    ArrivalStructure,
    JourneyRunTime,
    JourneyWaitTime,
    PointInJourneyPatternRefStructure,
    OnwardTimingLinkView,
    TimeDemandType,
    JourneyRunTimesRelStructure,
    JourneyWaitTimesRelStructure,
    TimeDemandTypeRefStructure,
    RouteView,
    ScheduledStopPoint,
    Route,
    CallsRelStructure,
    TimingPointInJourneyPattern,
    ScheduledStopPointRef,
    ServiceLinkInJourneyPattern,
    TimingLinkInJourneyPattern,
    JourneyPatternWaitTimesRelStructure,
    TimetabledPassingTime,
    TimetabledPassingTimesRelStructure,
    TemplateServiceJourney,
)
from utils.refs import setIdVersion, getRef, getIndex
from utils.utils import project
from utils.aux_logging import log_print, log_once


class CallsProfile:
    @staticmethod
    def getArrival(spijp: StopPointInJourneyPattern, arrival: int, offset: int = 0) -> ArrivalStructure:
        day_offset = arrival // 86400
        hour = (arrival % 86400) // 3600
        minute = (arrival % 3600) // 60
        second = arrival % 60
        return ArrivalStructure(
            time=XmlTime(hour=hour, minute=minute, second=second, offset=offset),
            day_offset=day_offset,
            for_alighting=spijp.for_alighting,
            notice_assignments=spijp.notice_assignments,
        )

    @staticmethod
    def getArrivalTime(arrival: int, offset: int = 0) -> tuple[XmlTime, int]:
        day_offset = arrival // 86400
        hour = (arrival % 86400) // 3600
        minute = (arrival % 3600) // 60
        second = arrival % 60
        return XmlTime(hour=hour, minute=minute, second=second, offset=offset), day_offset

    def getDuration(duration: XmlDuration) -> int:
        if not duration:
            return 0

        return ((duration.days or 0) * 86400) + ((duration.hours or 0) * 3600) + ((duration.minutes or 0) * 60) + ((int(duration.seconds or 0)))

    @staticmethod
    def getDeparture(spijp: StopPointInJourneyPattern, departure: int, offset: int = 0) -> DepartureStructure:
        day_offset = departure // 86400
        hour = (departure % 86400) // 3600
        minute = (departure % 3600) // 60
        second = departure % 60
        return DepartureStructure(
            time=XmlTime(hour=hour, minute=minute, second=second, offset=offset),
            day_offset=day_offset,
            for_boarding=spijp.for_boarding,
            notice_assignments=spijp.notice_assignments,
        )

    @staticmethod
    def getDepartureTimeOffset(departure: int, offset: int = 0) -> tuple[XmlTime, int]:
        day_offset = departure // 86400
        hour = (departure % 86400) // 3600
        minute = (departure % 3600) // 60
        second = departure % 60
        return XmlTime(hour=hour, minute=minute, second=second, offset=offset), day_offset

    @staticmethod
    def getRunTime(departure: DepartureStructure, arrival: ArrivalStructure) -> int:
        assert departure.time is not None
        assert arrival.time is not None
        return ((arrival.day_offset or 0) * 86400 + arrival.time.hour * 3600 + arrival.time.minute * 60 + arrival.time.second) - (
            (departure.day_offset or 0) * 86400 + departure.time.hour * 3600 + departure.time.minute * 60 + departure.time.second
        )

    @staticmethod
    def getWaitTime(arrival: ArrivalStructure, departure: DepartureStructure) -> int:
        assert departure.time is not None
        assert arrival.time is not None
        return ((departure.day_offset or 0) * 86400 + departure.time.hour * 3600 + departure.time.minute * 60 + departure.time.second) - (
            (arrival.day_offset or 0) * 86400 + arrival.time.hour * 3600 + arrival.time.minute * 60 + arrival.time.second
        )

    @staticmethod
    def getDepartureTime(service_journey: ServiceJourney) -> int:
        assert service_journey.departure_time is not None
        return (
            (service_journey.departure_day_offset or 0) * 86400
            + service_journey.departure_time.hour * 3600
            + service_journey.departure_time.minute * 60
            + service_journey.departure_time.second
        )

    @staticmethod
    def getWaitTimesFromPointInJourneyPattern(wait_time_or_wait_times, time_demand_type: TimeDemandType = None) -> XmlDuration | None:
        if isinstance(wait_time_or_wait_times, XmlDuration):
            return wait_time_or_wait_times

        elif isinstance(wait_time_or_wait_times, JourneyPatternWaitTimesRelStructure):
            # TODO: Check for ambiguities, for example due to Timebands, TimeDemandType
            if time_demand_type is not None:
                for x in wait_time_or_wait_times.journey_pattern_wait_time_ref_or_journey_pattern_wait_time:
                    if x.time_demand_type_ref_or_timeband_ref.ref == time_demand_type.id:
                        return x.wait_time

                return wait_time_or_wait_times.journey_pattern_wait_time_ref_or_journey_pattern_wait_time[0].wait_time

            else:
                return wait_time_or_wait_times.journey_pattern_wait_time_ref_or_journey_pattern_wait_time[0].wait_time

        return None

    @staticmethod
    def getCallsFromTimeDemandType(service_journey: ServiceJourney, service_journey_pattern: ServiceJourneyPattern, time_demand_type: TimeDemandType) -> None:
        # If calls are present, we don't have to do anything
        if service_journey.calls is not None:
            return

        # Guard that the provided ServiceJourneyPattern equals to the ref
        if service_journey.journey_pattern_ref.ref != service_journey_pattern.id:
            return

        # Guard that the provided TimeDemandType equals to the ref
        if service_journey.time_demand_type_ref.ref != time_demand_type.id:
            return

        # TODO: Check for ambiguities, for example due to Timebands or points having the same name
        tdt_tl: dict[str, JourneyRunTime] = {}
        if time_demand_type.run_times:
            tdt_tl = getIndex(time_demand_type.run_times.journey_run_time, 'timing_link_ref.ref')

        tdt_point: dict[str, JourneyWaitTime] = {}
        if time_demand_type.wait_times:
            tdt_point = getIndex(
                time_demand_type.wait_times.journey_wait_time, 'timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref.ref'
            )

        if service_journey_pattern.links_in_sequence is not None:
            i = 0
            for lis in service_journey_pattern.links_in_sequence.service_link_in_journey_pattern_or_timing_link_in_journey_pattern:
                if isinstance(lis, ServiceLinkInJourneyPattern):
                    for journey_run_time in lis.run_times.journey_run_time:
                        tdt_tl[lis.service_link_ref.ref] = journey_run_time
                        # TODO: Guard begin point equeals assigned value
                        service_journey_pattern.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern[
                            i
                        ].onward_service_link_ref = lis.service_link_ref

                elif isinstance(lis, TimingLinkInJourneyPattern):
                    for journey_run_time in lis.run_times.journey_run_time:
                        tdt_tl[lis.timing_link_ref.ref] = journey_run_time
                        # TODO: Guard begin point equeals assigned value
                        service_journey_pattern.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern[
                            i
                        ].onward_timing_link_ref = lis.timing_link_ref

                i += 1

        departure: int = CallsProfile.getDepartureTime(service_journey)
        arrival: int = departure
        offset: int = service_journey.departure_time.offset

        calls = CallsRelStructure(call=[])

        # If there are no onward timing links, we must consider links in sequence
        for pis in service_journey_pattern.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern:
            wait_time = None
            run_time = None
            ssp_ref = None
            spijp = None
            # TODO: handle the VDV462 case, but then also handle the service_journey input
            # pis.wait_time_or_wait_times.journey_pattern_wait_time_ref_or_journey_pattern_wait_time
            if isinstance(pis, StopPointInJourneyPattern):
                ssp_ref = pis.scheduled_stop_point_ref
                spijp = pis

                wait_time = CallsProfile.getWaitTimesFromPointInJourneyPattern(pis.wait_time_or_wait_times, time_demand_type)

                if wait_time is None:
                    wait_time = tdt_point.get(pis.scheduled_stop_point_ref.ref, None)
                    if wait_time is not None:
                        wait_time = wait_time.wait_time

                if pis.onward_timing_link_ref:
                    run_time = tdt_tl[pis.onward_timing_link_ref.ref]
                elif pis.onward_service_link_ref:
                    run_time = tdt_tl[pis.onward_service_link_ref.ref]

            elif isinstance(pis, TimingPointInJourneyPattern):
                if isinstance(pis.timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref, ScheduledStopPointRef):
                    spijp = project(pis, StopPointInJourneyPattern)
                    ssp_ref = pis.timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref
                    spijp.for_alighting = True
                    spijp.for_boarding = True

                wait_time = tdt_point.get(pis.timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref.ref, None)
                if wait_time is not None:
                    wait_time = wait_time.wait_time

                if pis.onward_timing_link_ref:
                    run_time = tdt_tl[pis.onward_timing_link_ref.ref]

            departure += CallsProfile.getDuration(wait_time)
            if spijp is not None:
                call = Call(
                    id=service_journey.id.replace(":ServiceJourney:", ":Call:") + '-' + str(pis.order),
                    order=pis.order,
                    version=service_journey.version,
                    fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=ssp_ref,
                    arrival=CallsProfile.getArrival(spijp, arrival, offset),
                    departure=CallsProfile.getDeparture(spijp, departure, offset),
                )
                calls.call.append(call)

            if run_time is not None:
                arrival = departure + CallsProfile.getDuration(run_time.run_time)
                departure = arrival

        # for lis in service_journey_pattern.links_in_sequence.service_link_in_journey_pattern_or_timing_link_in_journey_pattern:
        # TODO: handle the VDV462 case
        # lis.run_times.journey_run_time
        # pass

        service_journey.calls = calls

    @staticmethod
    def getPassingTimesFromTimeDemandType(service_journey: ServiceJourney, service_journey_pattern: ServiceJourneyPattern, time_demand_type: TimeDemandType) -> None:
        # If calls are present, we don't have to do anything
        if service_journey.passing_times is not None:
            return

        # Guard that the provided ServiceJourneyPattern equals to the ref
        if service_journey.journey_pattern_ref.ref != service_journey_pattern.id:
            return

        # Guard that the provided TimeDemandType equals to the ref
        if service_journey.time_demand_type_ref.ref != time_demand_type.id:
            return

        # TODO: Check for ambiguities, for example due to Timebands or points having the same name
        tdt_tl: dict[str, JourneyRunTime] = {}
        if time_demand_type.run_times:
            tdt_tl = getIndex(time_demand_type.run_times.journey_run_time, 'timing_link_ref.ref')

        tdt_point: dict[str, JourneyWaitTime] = {}
        if time_demand_type.wait_times:
            tdt_point = getIndex(
                time_demand_type.wait_times.journey_wait_time, 'timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref.ref'
            )

        if service_journey_pattern.links_in_sequence is not None:
            i = 0
            for lis in service_journey_pattern.links_in_sequence.service_link_in_journey_pattern_or_timing_link_in_journey_pattern:
                if isinstance(lis, ServiceLinkInJourneyPattern):
                    if not hasattr(lis.run_times, 'journey_run_time'):
                        log_print("no run_times")
                        log_print(lis)
                    for journey_run_time in lis.run_times.journey_run_time:
                        tdt_tl[lis.service_link_ref.ref] = journey_run_time
                        # TODO: Guard begin point equals assigned value
                        service_journey_pattern.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern[
                            i
                        ].onward_service_link_ref = lis.service_link_ref

                elif isinstance(lis, TimingLinkInJourneyPattern):
                    for journey_run_time in lis.run_times.journey_run_time:
                        tdt_tl[lis.timing_link_ref.ref] = journey_run_time
                        # TODO: Guard begin point equeals assigned value
                        service_journey_pattern.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern[
                            i
                        ].onward_timing_link_ref = lis.timing_link_ref

                i += 1

        departure: int = CallsProfile.getDepartureTime(service_journey)
        arrival: int = departure
        offset: int = service_journey.departure_time.offset

        tpts = TimetabledPassingTimesRelStructure(timetabled_passing_time=[])

        # If there are no onward timing links, we must consider links in sequence
        for pis in service_journey_pattern.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern:
            wait_time = None
            run_time = None
            spijp = None
            # TODO: handle the VDV462 case, but then also handle the service_journey input
            # pis.wait_time_or_wait_times.journey_pattern_wait_time_ref_or_journey_pattern_wait_time
            if isinstance(pis, StopPointInJourneyPattern):
                spijp = pis

                wait_time = CallsProfile.getWaitTimesFromPointInJourneyPattern(pis.wait_time_or_wait_times, time_demand_type)

                if wait_time is None:
                    wait_time = tdt_point.get(pis.scheduled_stop_point_ref.ref, None)
                    if wait_time is not None:
                        wait_time = wait_time.wait_time

                if pis.onward_timing_link_ref:
                    run_time = tdt_tl[pis.onward_timing_link_ref.ref]
                elif pis.onward_service_link_ref:
                    run_time = tdt_tl[pis.onward_service_link_ref.ref]

            elif isinstance(pis, TimingPointInJourneyPattern):
                if isinstance(pis.timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref, ScheduledStopPointRef):
                    spijp = project(pis, StopPointInJourneyPattern)
                    spijp.for_alighting = True
                    spijp.for_boarding = True

                wait_time = tdt_point.get(pis.timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref.ref, None)
                if wait_time is not None:
                    wait_time = wait_time.wait_time

                if pis.onward_timing_link_ref:
                    run_time = tdt_tl[pis.onward_timing_link_ref.ref]

            departure += CallsProfile.getDuration(wait_time)
            if spijp is not None:
                arrival_time, arrival_day_offset = CallsProfile.getArrivalTime(arrival, offset)
                departure_time, departure_day_offset = CallsProfile.getDepartureTimeOffset(arrival, offset)
                timetabled_passing_time = TimetabledPassingTime(
                    id=service_journey.id.replace(":ServiceJourney:", ":TimetabledPassingTime:") + '-' + str(pis.order),
                    version=service_journey.version,
                    point_in_journey_pattern_ref=getRef(spijp),
                    arrival_time=arrival_time,
                    arrival_day_offset=arrival_day_offset,
                    departure_time=departure_time,
                    departure_day_offset=departure_day_offset,
                )
                tpts.timetabled_passing_time.append(timetabled_passing_time)

            if run_time is not None:
                arrival = departure + CallsProfile.getDuration(run_time.run_time)
                departure = arrival

        # for lis in service_journey_pattern.links_in_sequence.service_link_in_journey_pattern_or_timing_link_in_journey_pattern:
        # TODO: handle the VDV462 case
        # lis.run_times.journey_run_time
        # pass

        service_journey.passing_times = tpts

    @staticmethod
    def getCallsFromTimetabledPassingTimes(service_journey: ServiceJourney | TemplateServiceJourney, service_journey_pattern: ServiceJourneyPattern):
        if service_journey.calls is not None:
            return

        if service_journey.journey_pattern_ref.ref != service_journey_pattern.id:
            return

        ssp_refs = {}
        for pis in service_journey_pattern.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern:
            if isinstance(pis, StopPointInJourneyPattern):
                ssp_refs[pis.id] = pis.scheduled_stop_point_ref
            elif isinstance(pis, TimingPointInJourneyPattern):
                if isinstance(pis.timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref, ScheduledStopPointRef):
                    ssp_refs[pis.id] = pis.timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref
                else:
                    # TODO: implement TimingPointRef nameOfRefClass
                    pass

        calls = CallsRelStructure(call=[])
        order = 1
        for timetabled_passing_time in service_journey.passing_times.timetabled_passing_time:

            # TODO: This should NOT be done here. It should be done in preprocessing.
            if timetabled_passing_time.point_in_journey_pattern_ref is None:
                log_once(
                    logging.WARN,
                    "timetabledpassingtime lacks pointinjourneypattern",
                    "TimetabledPassingTime has no explicit reference to ServiceJourneyPattern",
                )
                if len(
                    service_journey_pattern.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern
                ) != len(service_journey.passing_times.timetabled_passing_time):
                    log_once(logging.WARN, "timetabledpassingtimes do not align", "TimetabledPassingTime do not align with ServiceJourneyPattern")
                    # Last resort would be to filter on StopPointInJourneyPattern and see if that aligns...
                else:
                    pis = (
                        service_journey_pattern.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern[
                            order - 1
                        ]
                    )
                    timetabled_passing_time.id = f"{service_journey.id.replace("ServiceJourney", "TimetabledPassingTime")}_{order}"
                    timetabled_passing_time.point_in_journey_pattern_ref = getRef(pis)

            call = Call(
                id=timetabled_passing_time.id.replace(":TimetabledPassingTime:", ":Call:"),
                order=order,
                version=timetabled_passing_time.version,
                fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view=ssp_refs[
                    timetabled_passing_time.point_in_journey_pattern_ref.ref
                ],
                arrival=ArrivalStructure(day_offset=timetabled_passing_time.arrival_day_offset, time=timetabled_passing_time.arrival_time),
                departure=DepartureStructure(day_offset=timetabled_passing_time.departure_day_offset, time=timetabled_passing_time.departure_time),
            )

            # GTFS has strange requirements
            if order == 1:
                if call.arrival.time is None:
                    assert call.departure.time is not None
                    call.arrival.day_offset = call.departure.day_offset
                    call.arrival.time = call.departure.time
                elif call.departure.time is None:
                    assert call.arrival.time is not None
                    call.departure.day_offset = call.arrival.day_offset
                    call.departure.time = call.arrival.time

            order += 1
            calls.call.append(call)

        # GTFS has strange requirements
        if call.arrival.time is None:
            call.arrival.day_offset = call.departure.day_offset
            call.arrival.time = call.departure.time
        elif call.departure.time is None:
            call.departure.day_offset = call.arrival.day_offset
            call.departure.time = call.arrival.time

        service_journey.calls = calls

    def getTimeDemandTypes(self):
        tdts = {}

        for sj in self.service_journeys:
            tdt = []
            parts = []
            parts2 = []
            calls = sj.calls.call
            for call_i in range(0, len(calls) - 1):
                run_time = self.getRunTime(calls[call_i].departure, calls[call_i + 1].arrival)
                wait_time = self.getWaitTime(calls[call_i].arrival, calls[call_i].departure)
                tl = calls[call_i].onward_timing_link_view.timing_link_ref
                ssp = calls[call_i].scheduled_stop_point_ref

                parts.append(tuple([run_time, wait_time, tl, ssp]))
                parts2.append(tuple([run_time, wait_time, tl.ref, ssp.ref]))

            tdt_hash = hash(tuple(parts2))
            if tdt_hash not in tdts:
                run_times = []
                part = 0
                for x in parts:
                    jrt = JourneyRunTime(run_time=XmlDuration("PT{}S".format(x[0])), timing_link_ref=getRef(x[2], TimingLinkRef))
                    setIdVersion(jrt, self.codespace, "{}-{}".format(tdt_hash % 65449, part), self.version)
                    run_times.append(jrt)
                    part += 1

                if len(run_times) == 0:
                    jounrney_run_times = None
                else:
                    jounrney_run_times = JourneyRunTimesRelStructure(journey_run_time=run_times)

                wait_times = []
                part = 0
                for x in parts:
                    if x[1] > 0:
                        jwt = JourneyWaitTime(
                            wait_time=XmlDuration("PT{}S".format(x[1])),
                            timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref=x[3],
                        )
                        wait_times.append(jwt)
                        part += 1

                if len(wait_times) == 0:
                    journey_wait_times = None
                else:
                    journey_wait_times = JourneyWaitTimesRelStructure(journey_wait_time=wait_times)

                tdt = TimeDemandType(run_times=jounrney_run_times, wait_times=journey_wait_times)
                setIdVersion(tdt, self.codespace, tdt_hash % 65449, self.version)
                tdts[tdt_hash] = tdt

            tdt = tdts[tdt_hash]
            sj.time_demand_type_ref = getRef(tdt, TimeDemandTypeRefStructure)
            sj.departure_time = calls[0].departure.time
            sj.departure_day_offset = calls[0].departure.day_offset

        return list(tdts.values())

    def getServiceJourneyPatterns(
        self, lines: List[Line], routes: List[Route], ssps: List[ScheduledStopPoint]
    ) -> (list[ServiceJourneyPattern], list[TimingLink]):
        lines = getIndex(self.lines)
        routes = getIndex(self.routes)
        ssp = getIndex(self.scheduled_stop_points)

        sjps = {}
        tls = {}
        tlsh = set([])

        for sj in self.service_journeys:
            spijps = []
            for call in sj.calls.call:
                spijp = StopPointInJourneyPattern(
                    scheduled_stop_point_ref=call.scheduled_stop_point_ref,
                    order=call.order,
                    for_alighting=call.arrival.for_alighting,
                    for_boarding=call.departure.for_boarding,
                    destination_display_ref_or_destination_display_view=call.destination_display_view,
                    derived_from_object_ref=call.id,
                )
                spijps.append(spijp)

            spijp_hash = hash(
                tuple([(spijp.scheduled_stop_point_ref.ref, spijp.for_alighting, spijp.for_boarding, spijp.destination_display_view) for spijp in spijps])
            )
            if spijp_hash not in sjps:
                sjp = ServiceJourneyPattern()

                if sj.journey_pattern_view:
                    mine = vars(sjp).keys()
                    for attr, value in vars(sj.journey_pattern_view).items():
                        if attr in mine:
                            setattr(sjp, attr, value)

                sjp.derived_from_object_ref = sj.id
                sjp.route_ref = sj.route_ref
                # TODO: remove this one, because it can be fully resolved
                sjp.route_view = RouteView(flexible_line_ref_or_line_ref_or_line_view=routes[sj.route_ref.ref].line_ref)
                sjp.points_in_sequence = PointsInJourneyPatternRelStructure(
                    point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern=spijps
                )
                setIdVersion(sjp, self.codespace, spijp_hash % 65449, self.version)
                for spijp in spijps:
                    setIdVersion(spijp, self.codespace, '{}-{}'.format(spijp_hash % 65449, spijp.order), self.version)

                sjps[spijp_hash] = sjp

                line = lines[sj.line_ref.ref]

                piss = sjp.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern
                for pis_i in range(0, len(piss) - 1):
                    tl_hash = hash(
                        tuple([line.operational_context_ref, piss[pis_i].scheduled_stop_point_ref.ref, piss[pis_i + 1].scheduled_stop_point_ref.ref])
                    )
                    if tl_hash not in tlsh:
                        timing_link = TimingLink(
                            derived_from_object_ref=piss[pis_i].id,
                            operational_context_ref=line.operational_context_ref,
                            from_point_ref=getRef(ssp[piss[pis_i].scheduled_stop_point_ref.ref], TimingPointRefStructure),
                            to_point_ref=getRef(ssp[piss[pis_i + 1].scheduled_stop_point_ref.ref], TimingPointRefStructure),
                        )
                        setIdVersion(timing_link, self.codespace, tl_hash % 65449, self.version)
                        tls[timing_link.id] = timing_link
                        tlsh.add(tl_hash)

                    piss[pis_i].onward_timing_link_ref = getRef(timing_link, TimingLinkRefStructure)

            sjp = sjps[spijp_hash]
            sj.service_journey_pattern_ref = getRef(sjp, ServiceJourneyPatternRef)
            piss = sjp.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern
            for pis_i in range(0, len(piss)):
                if piss[pis_i].onward_timing_link_ref:
                    tl = tls[piss[pis_i].onward_timing_link_ref.ref]
                    if sj.calls.call[pis_i].onward_service_link_view and sj.calls.call[pis_i].onward_service_link_view.distance:
                        tl.distance = sj.calls.call[pis_i].onward_service_link_view.distance

                    # TODO: eventually OnwardTimingLinkView would get a Distance

                    sj.calls.call[pis_i].onward_timing_link_view = OnwardTimingLinkView(
                        timing_link_ref=getRef(piss[pis_i].onward_timing_link_ref, TimingLinkRef)
                    )
                sj.calls.call[pis_i].point_in_journey_pattern_ref = getRef(piss[pis_i], PointInJourneyPatternRefStructure)

        return (list(sjps.values()), list(tls.values()))
