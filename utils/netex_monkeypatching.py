from netex import (
    VersionOfObjectRefStructure,
    ServiceJourneyRef,
    ServiceJourneyPatternRef,
    ServiceJourney,
    ServiceJourneyPattern,
    Locale,
    LocaleStructure,
    OperatorRef,
    DayTypeRef,
    DayTypeRefsRelStructure,
    ValidityConditionsRelStructure,
    UicOperatingPeriodRef,
    OperatingPeriodRef,
    LineRef,
    QuayRef,
    ScheduledStopPointRef,
    EntityInVersion,
)


def ref_version_hash(self: VersionOfObjectRefStructure) -> int:
    assert self.ref is None
    return hash(self.ref + ";" + self.version or "any")


setattr(VersionOfObjectRefStructure, ".__hash__", ref_version_hash)
setattr(ServiceJourneyRef, "__hash__", ref_version_hash)
setattr(ServiceJourneyPatternRef, "__hash__", ref_version_hash)
setattr(OperatorRef, "__hash__", ref_version_hash)
setattr(DayTypeRef, "__hash__", ref_version_hash)
setattr(OperatingPeriodRef, "__hash__", ref_version_hash)
setattr(UicOperatingPeriodRef, "__hash__", ref_version_hash)
setattr(LineRef, "__hash__", ref_version_hash)
setattr(QuayRef, "__hash__", ref_version_hash)
setattr(ScheduledStopPointRef, "__hash__", ref_version_hash)


# TODO: the following hashes us version or 'any' to overcome (invalid) None situations, it would be better if we could create hashes that would capture the true NeTEx-any situation
def day_type_refs_hash(self: DayTypeRefsRelStructure) -> int:
    return hash(
        "\n".join([dtr.ref + ";" + (dtr.version or "any") for dtr in self.day_type_ref])
    )


setattr(DayTypeRefsRelStructure, "day_type_refs_hash", ref_version_hash)


def vc_refs_hash(self: ValidityConditionsRelStructure) -> int:
    refs = []
    for vc in self.choice:
        if hasattr(vc, "id"):
            assert vc.id is not None
            refs.append(vc.id + ";" + (vc.version or "any"))
        elif hasattr(vc, "ref"):
            assert vc.ref is not None
            refs.append(vc.ref + ";" + (vc.version or "any"))

    return hash("\n".join(refs))


setattr(ValidityConditionsRelStructure, "__hash__", vc_refs_hash)


def id_version_hash(self: EntityInVersion) -> int:
    assert self.id is not None and self.version is not None
    return hash(self.id + ";" + (self.version or "any"))


setattr(ServiceJourney, "__hash__", id_version_hash)
setattr(ServiceJourneyPattern, "__hash__", id_version_hash)


def hash_locale(self: Locale) -> int:
    return hash(
        (
            self.time_zone_offset,
            self.time_zone,
            self.summer_time_zone_offset,
            self.summer_time_zone,
            self.default_language,
        )
    )


setattr(Locale, "__hash__", hash_locale)
setattr(LocaleStructure, "__hash__", hash_locale)

"""
# First monkey patching test
def get_route(self, con) -> Route:
    return con.get_single(Route, self.ref, self.version)


setattr(RouteRefStructure, "get_route", ref_version_hash)


def get_routelink(self, con) -> RouteLink:
    return con.get_single(RouteLink, self.ref, self.version)


setattr(RouteLinkRefStructure, "get_routelink", ref_version_hash)


def get_scheduledstoppoint(self, con) -> ScheduledStopPoint:
    return con.get_single(ScheduledStopPoint, self.ref, self.version)


setattr(ScheduledStopPointRefStructure, "get_scheduledstoppoint", ref_version_hash)


def get_quay(self, con) -> Quay:
    return con.get_single(Quay, self.ref, self.version)


setattr(QuayRefStructure, "get_quay", ref_version_hash)


def get_stopplace(self, con) -> StopPlace:
    return con.get_single(StopPlace, self.ref, self.version)


setattr(StopPlaceRefStructure, "get_stopplace", ref_version_hash)


def get_timingpoint(self, con) -> TimingPoint | ScheduledStopPoint:
    if self.name_of_ref_class == 'TimingPoint':
        return con.get_single(TimingPoint, self.ref, self.version)
    elif self.name_of_ref_class == 'ScheduledStopPoint':
        return con.get_single(ScheduledStopPoint, self.ref, self.version)
    else:
        timing_point = con.get_single(TimingPoint, self.ref, self.version)
        if timing_point is not None:
            return timing_point
        else:
            return con.get_single(ScheduledStopPoint, self.ref, self.version)


setattr(TimingPointRefStructure, "get_timingpoint", ref_version_hash)


def get_servicelink(self, con) -> ServiceLink:
    return con.get_single(ServiceLink, self.ref, self.version)


setattr(ServiceLinkRefStructure, "get_servicelink", ref_version_hash)


def get_timinglink(self, con) -> TimingLink:
    return con.get_single(TimingLink, self.ref, self.version)


setattr(TimingLinkRefStructure, "get_timinglink", ref_version_hash)
"""
