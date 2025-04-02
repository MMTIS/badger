from netex import (
    Authority,
    AvailabilityCondition,
    Branding,
    Codespace,
    DataSource,
    DayType,
    DayTypeAssignment,
    DestinationDisplay,
    Direction,
    InterchangeRule,
    JourneyMeeting,
    Line,
    Notice,
    NoticeAssignment,
    Operator,
    PassengerStopAssignment,
    ResponsibilitySet,
    RoutePoint,
    ScheduledStopPoint,
    ServiceCalendar,
    ServiceJourney,
    ServiceJourneyInterchange,
    ServiceJourneyPattern,
    StopPlace,
    TemplateServiceJourney,
    TopographicPlace,
    TransportAdministrativeZone,
    UicOperatingPeriod,
    VehicleType,
)

SWISS_CLASSES = {
    AvailabilityCondition,
    Codespace,
    DestinationDisplay,
    Direction,
    Line,
    Operator,
    PassengerStopAssignment,
    ResponsibilitySet,
    ScheduledStopPoint,
    ServiceCalendar,
    ServiceJourney,
    StopPlace,
    TemplateServiceJourney,
    TopographicPlace,
    VehicleType,
}

GTFS_CLASSES = {
    Authority,
    AvailabilityCondition,
    Branding,
    Codespace,
    DayTypeAssignment,
    DestinationDisplay,
    InterchangeRule,
    JourneyMeeting,
    Line,
    Operator,
    PassengerStopAssignment,
    ScheduledStopPoint,
    ServiceJourney,
    ServiceJourneyInterchange,
    ServiceJourneyPattern,
    StopPlace,
    TemplateServiceJourney,
    UicOperatingPeriod,
}

EPIP_CLASSES = {
    AvailabilityCondition,
    Codespace,
    Line,
    DataSource,
    DayType,
    DayTypeAssignment,
    DestinationDisplay,
    Direction,
    Notice,
    NoticeAssignment,
    Operator,
    PassengerStopAssignment,
    ResponsibilitySet,
    RoutePoint,
    ScheduledStopPoint,
    ServiceCalendar,
    ServiceJourney,
    ServiceJourneyPattern,
    StopPlace,
    TransportAdministrativeZone,
    UicOperatingPeriod,
    VehicleType,
}
