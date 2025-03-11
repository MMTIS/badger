from transformers.callsprofile import CallsProfile
from netex import ServiceJourney, ServiceJourneyPattern, StopPointInJourneyPattern, TimetabledPassingTime, \
    PointsInJourneyPatternRelStructure, Codespace, TimetabledPassingTimesRelStructure, \
    PointInJourneyPatternRef, ServiceJourneyPatternRef, Call, MultilingualString, RouteView, DestinationDisplayView, \
    JourneyPattern, TimingPointInJourneyPattern, TimeDemandType
from utils.refs import getRef, getIndex, getId
import sys
import hashlib


class TimetablePassingTimesProfile:
    codespace: Codespace
    service_journeys: list[ServiceJourney]

    def __init__(self, codespace, version, service_journeys, service_journey_patterns, time_demand_types):
        self.codespace = codespace
        # self.version = version
        self.service_journeys = service_journeys
        self.service_journey_patterns = service_journey_patterns
        self.time_demand_types = time_demand_types

    @staticmethod
    def mapCallToStopPointInJourneyPattern(call: Call, codespace: Codespace) -> StopPointInJourneyPattern:
        stop_point_in_journey_pattern = StopPointInJourneyPattern(
            id=getId(StopPointInJourneyPattern, codespace, f"-XXX-{call.order}"),
            version=call.version,
            order=call.order,
            derived_from_object_ref=call.id,
            derived_from_version_ref_attribute=call.version
        )
        stop_point_in_journey_pattern.scheduled_stop_point_ref = call.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view
        if call.arrival:
            stop_point_in_journey_pattern.for_alighting = call.arrival.for_alighting
        if call.departure:
            stop_point_in_journey_pattern.for_boarding = call.departure.for_boarding
        stop_point_in_journey_pattern.onward_timing_link_ref = call.onward_timing_link_view
        stop_point_in_journey_pattern.destination_display_ref_or_destination_display_view = call.destination_display_ref_or_destination_display_view
        stop_point_in_journey_pattern.onward_service_link_ref = call.onward_service_link_ref_or_onward_service_link_view  # TODO: select only the service_link_ref
        return stop_point_in_journey_pattern

    @staticmethod
    def mapCallToTimetabledPassingTime(call: Call, pattern: dict) -> TimetabledPassingTime:
        timetabled_passing_time = TimetabledPassingTime(id=call.id.replace(":Call:", ":TimetabledPassingTime:"),
                                                        version=call.version,
                                                        derived_from_object_ref=call.id,
                                                        derived_from_version_ref_attribute=call.version
                                                        )
        timetabled_passing_time.point_in_journey_pattern_ref = getRef(pattern[call.order])
        timetabled_passing_time.derived_from_object_ref = call.id
        timetabled_passing_time.derived_from_version_ref_attribute = call.version
        if call.arrival is not None:
            timetabled_passing_time.arrival_time = call.arrival.time
            timetabled_passing_time.arrival_day_offset = call.arrival.day_offset

        if call.departure is not None:
            timetabled_passing_time.departure_time = call.departure.time
            timetabled_passing_time.departure_day_offset = call.departure.day_offset
        return timetabled_passing_time

    @staticmethod
    def getDestination(destination_name):
        if destination_name is not None:
            if hasattr(destination_name, 'ref'):
                return destination_name.ref
            else:
                return destination_name.name
        else:
            return ''

    @staticmethod
    def getProperty(obj, attr):
        if obj is None:
            return ''
        return str(getattr(obj, attr, ''))

    @staticmethod
    def sjp_hash(points_in_sequence: PointsInJourneyPatternRelStructure):

        m = hashlib.sha256()

        # TODO: RouteView/LineRef may be different, and should therefore create a different SJP

        elements = []

        for pijp in points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern:
            if isinstance(pijp, StopPointInJourneyPattern):
                spijp: StopPointInJourneyPattern = pijp
                elements.append(TimetablePassingTimesProfile.getProperty(spijp.scheduled_stop_point_ref, 'ref') + ',' +
                                str(spijp.for_alighting) + ',' + str(spijp.for_boarding) + ',' +
                                TimetablePassingTimesProfile.getProperty(spijp.onward_timing_link_ref, 'ref') + ',' +
                                TimetablePassingTimesProfile.getProperty(spijp.onward_service_link_ref, 'ref') + ',' +
                                TimetablePassingTimesProfile.getDestination(
                                    spijp.destination_display_ref_or_destination_display_view))
            elif isinstance(pijp, TimingPointInJourneyPattern):
                tpijp: TimingPointInJourneyPattern = pijp
                elements.append(TimetablePassingTimesProfile.getProperty(
                    tpijp.timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref,
                    'ref') + ',' +
                                str(False) + ',' + str(False) + ',' +
                                TimetablePassingTimesProfile.getProperty(tpijp.onward_timing_link_ref, 'ref') + ',' +
                                '' + ',' +
                                '')

        key = ('\n'.join(elements)).encode('utf-8')

        m.update(key)
        return m.hexdigest()[0:8].upper()

        """
        spijp_hash = hash(
            tuple([(spijp.scheduled_stop_point_ref.ref, spijp.for_alighting,
                    spijp.for_boarding,
                    spijp.onward_timing_link_ref, spijp.onward_service_link_ref,
                    spijp.destination_display_ref_or_destination_display_view)
                   for spijp in points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern]))
        return ("%0.2X" % (spijp_hash**2))[0:8]
        """

    @staticmethod
    def getTimetabledPassingtimesFromCalls(sj: ServiceJourney, journey_pattern: JourneyPattern):
        pattern = {x.order: x for x in
                   journey_pattern.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern}

        ttpt = []
        for call in sj.calls.call:
            # TODO: do something with the different elements of the choice (Call, CallZ, DatedCall, DatedCallZ)
            pis = pattern[call.order]
            if isinstance(pis, TimingPointInJourneyPattern):
                if pis.timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref.ref != call.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view.ref:  # TODO: make sure we get the right one
                    print("{} order does not match {} order ({} vs {})".format(journey_pattern.id, sj.id,
                                                                               call.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view,
                                                                               pis.timing_point_ref_or_scheduled_stop_point_ref_or_parking_point_ref_or_relief_point_ref),
                          file=sys.stderr)
                else:
                    ttpt.append(TimetablePassingTimesProfile.mapCallToTimetabledPassingTime(call, pattern))

            else:
                if pis.scheduled_stop_point_ref.ref != call.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view.ref:  # TODO: make sure we get the right one
                    print("{} order does not match {} order ({} vs {})".format(journey_pattern.id, sj.id,
                                                                               call.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point_view,
                                                                               pis.scheduled_stop_point_ref),
                          file=sys.stderr)
                else:
                    ttpt.append(TimetablePassingTimesProfile.mapCallToTimetabledPassingTime(call, pattern))

        return ttpt

    @staticmethod
    def getTimetabledPassingTimesForServiceJourney(sj: ServiceJourney, codespace: Codespace, force=False,
                                                   clean=False) -> ServiceJourney:
        if sj.passing_times and not force:
            # We already have what we were looking for
            pass

        # if there are calls -> create service journey patterns
        elif sj.calls:
            service_journey_pattern: ServiceJourneyPattern = None
            if not sj.journey_pattern_ref:
                if len(sj.calls.call) <= 1:
                    print(f"{sj.id} has not enough calls.")

                else:
                    # TODO: replace with a function that can compute the hash of a service journey pattern without creating it first
                    call: Call
                    spijps = PointsInJourneyPatternRelStructure(
                        point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern=[
                            TimetablePassingTimesProfile.mapCallToStopPointInJourneyPattern(call, codespace) for call in
                            sj.calls.call])
                    spijp_hash = TimetablePassingTimesProfile.sjp_hash(spijps)
                    for x in spijps.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern:
                        x.id = x.id.replace('-XXX-', spijp_hash + '-')

                    """
                    spijp_hash = str(hash(
                        tuple([(spijp.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref.ref, spijp.for_alighting, spijp.for_boarding,
                                spijp.onward_timing_link_ref, spijp.onward_service_link_ref,
                                spijp.destination_display_ref_or_destination_display_view)
                               for spijp in spijps])))
                    """

                    service_journey_pattern = sjps.get(spijp_hash, None)
                    if len(spijps.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern) > 0 and service_journey_pattern is None:
                        service_journey_pattern = ServiceJourneyPattern(
                            id=getId(ServiceJourneyPattern, codespace, spijp_hash),
                            version=sj.version,
                            route_ref_or_route_view=RouteView(
                                flexible_line_ref_or_line_ref_or_line_view=sj.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view),
                            name=MultilingualString(value=spijp_hash),
                            derived_from_object_ref=sj.id,
                            derived_from_version_ref_attribute=sj.version,
                            points_in_sequence=spijps)

                        sjps[spijp_hash] = service_journey_pattern
                        existing_sjps[service_journey_pattern.id] = service_journey_pattern

                    sj.journey_pattern_ref = getRef(service_journey_pattern, ServiceJourneyPatternRef)

            if not service_journey_pattern:
                if sj.journey_pattern_ref:
                    service_journey_pattern = existing_sjps[sj.journey_pattern_ref.ref]

            if service_journey_pattern:
                ttpt = TimetablePassingTimesProfile.getTimetabledPassingtimesFromCalls(sj, service_journey_pattern)
                if len(ttpt) > 0:
                    sj.passing_times = TimetabledPassingTimesRelStructure(timetabled_passing_time=ttpt)

        # if there is a servicejourneypattern, departure time, timedemandtype it can be expanded
        elif sj.journey_pattern_ref and sj.departure_time and sj.time_demand_type_ref:
            pass

        # if there are servicejourneypattern, departure time, waittimes, runtimes they can be expanded
        elif sj.journey_pattern_ref and sj.departure_time and sj.run_times:
            pass

        if clean:
            sj.calls = None
            sj.wait_times = None
            sj.run_times = None
            sj.time_demand_type_ref = None
            sj.time_demand_types = None
            sj.validity_conditions_or_valid_between = None

    def getTimetabledPassingTimes(self, force=False, clean=False):
        sjps = {TimetablePassingTimesProfile.sjp_hash(sjp.points_in_sequence): sjp for sjp in
                self.service_journey_patterns}
        existing_sjps = getIndex(self.service_journey_patterns)
        existing_tdts = getIndex(self.time_demand_types)

        sj: ServiceJourney
        for sj in self.service_journeys:
            # if there is a servicejourneypattern, departure time, timedemandtype it can be expanded

            if sj.passing_times and not force:
                if clean:
                    sj.calls = None
                    sj.wait_times = None
                    sj.run_times = None
                    sj.time_demand_type_ref = None
                    sj.time_demand_types = None
                    sj.validity_conditions_or_valid_between = None

                continue

            if sj.calls is None and sj.journey_pattern_ref and sj.departure_time and sj.time_demand_type_ref:
                # For VDV462 we need to differentiate
                service_journey_pattern: ServiceJourneyPattern = existing_sjps[sj.journey_pattern_ref.ref]
                time_demand_type: TimeDemandType = existing_tdts[sj.time_demand_type_ref.ref]
                CallsProfile.getPassingTimesFromTimeDemandType(sj, service_journey_pattern, time_demand_type)

            # if there are calls -> create service journey patterns
            elif sj.calls:
                service_journey_pattern: ServiceJourneyPattern = None
                if not sj.journey_pattern_ref:
                    if len(sj.calls.call) <= 1:
                        print(f"{sj.id} has not enough calls.")

                    else:
                        # TODO: replace with a function that can compute the hash of a service journey pattern without creating it first
                        call: Call
                        spijps = PointsInJourneyPatternRelStructure(
                            point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern=[
                                self.mapCallToStopPointInJourneyPattern(call, self.codespace) for call in
                                sj.calls.call])
                        spijp_hash = TimetablePassingTimesProfile.sjp_hash(spijps)
                        for x in spijps.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern:
                            x.id = x.id.replace('-XXX-', spijp_hash + '-')

                        """
                        spijp_hash = str(hash(
                            tuple([(spijp.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref.ref, spijp.for_alighting, spijp.for_boarding,
                                    spijp.onward_timing_link_ref, spijp.onward_service_link_ref,
                                    spijp.destination_display_ref_or_destination_display_view)
                                   for spijp in spijps])))
                        """

                        service_journey_pattern = sjps.get(spijp_hash, None)
                        if len(spijps.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern) > 0 and service_journey_pattern is None:
                            service_journey_pattern = ServiceJourneyPattern(
                                id=getId(ServiceJourneyPattern, self.codespace, spijp_hash),
                                version=sj.version,
                                route_ref_or_route_view=RouteView(
                                    flexible_line_ref_or_line_ref_or_line_view=sj.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view),
                                name=MultilingualString(value=spijp_hash),
                                derived_from_object_ref=sj.id,
                                derived_from_version_ref_attribute=sj.version,
                                points_in_sequence=spijps)

                            sjps[spijp_hash] = service_journey_pattern
                            existing_sjps[service_journey_pattern.id] = service_journey_pattern

                        sj.journey_pattern_ref = getRef(service_journey_pattern, ServiceJourneyPatternRef)

                if not service_journey_pattern:
                    if sj.journey_pattern_ref:
                        service_journey_pattern = existing_sjps[sj.journey_pattern_ref.ref]

                if service_journey_pattern:
                    ttpt = TimetablePassingTimesProfile.getTimetabledPassingtimesFromCalls(sj, service_journey_pattern)
                    if len(ttpt) > 0:
                        sj.passing_times = TimetabledPassingTimesRelStructure(timetabled_passing_time=ttpt)


            # if there are servicejourneypattern, departure time, waittimes, runtimes they can be expanded
            elif sj.journey_pattern_ref and sj.departure_time and sj.run_times:
                pass

            if clean:
                sj.calls = None
                sj.wait_times = None
                sj.run_times = None
                sj.time_demand_type_ref = None
                sj.time_demand_types = None

        # TODO: only do this for the newly created ones
        # self.service_journey_patterns += sjps.values()
