# Variant 1
# ServiceJourney has sj.calls.call, the call has a ScheduledStopPointRef towards we do not want to retain, we filter the sj.calls.call not to include stops

# Variant 2 (This code may exists for TimingPoint removal)
# ServiceJourneyPattern has (Stopp)PointInJourneyPattern refering to ScheduledStopPoint
# We can remove a (Stop)PointInJourneyPattern, there may be an OnwardsTimingLink or OnwardsServiceLink, meaning we must extend the OnwardsServiceLink or OnwardsTimingLink at the previous stop (add distance, linestring, etc.)
# We now filter all sj.service_journey.passing_times.timetabled_passing_time for which no StopPointInJourneyPattern exists (or maintain a list of ids we have deleted)

# Variant 3 (This code may exists for TimingPoint removal)
# Only update the ServiceJourneyPattern like above
# Extend the JourneyRunTime for the extended TimingLinks, but include the soon to be removed wait time.
# Remove the WaitTimes at stops that are to be removed.