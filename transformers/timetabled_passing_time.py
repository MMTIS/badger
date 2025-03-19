from netex import ServiceJourney


# DEPRECATION WARNING
def infer_id_and_order_and_apply(service_journey: ServiceJourney) -> None:
    if service_journey.passing_times:
        if (
            len(
                [
                    pt
                    for pt in service_journey.passing_times.timetabled_passing_time
                    if pt.id is None or pt.version is None
                ]
            )
            > 0
        ):
            order = 1
            for pt in service_journey.passing_times.timetabled_passing_time:
                if pt.id is None:
                    assert service_journey.id is not None, "ServiceJourney without id"
                    pt.id = (
                        service_journey.id.replace(
                            "ServiceJourney", "TimetabledPassingTime"
                        )
                        + "-"
                        + str(order)
                    )
                if pt.version is None:
                    pt.version = service_journey.version

                order += 1

    # TODO: calls?
