"""The statistics behind every published reliability figure.

Each test here is a way a plausible-looking table would mislead, and most exist
because the naive version of the calculation is also the obvious one:

* averaging lateness per *arrival* ranks routes by how many stops they have;
* one badly delayed journey supplies thirty "very late" arrivals and looks
  like thirty separate problems;
* ranking stops by lateness always finds the end of the route, because a stop
  inherits every minute lost before it;
* a bus due 17:45 and seen 18:05, counted at 18:00, moves the delay out of the
  hour that caused it;
* dividing seconds by minutes produces "24 minutes lost per scheduled minute",
  which is what an unchecked unit does to a published figure. That one was
  real: it printed before this test existed.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import reliability_stats as rs                                  # noqa: E402


def obs(trip="T1", service="700", direction="westbound", stop="A", index=0,
        sched=36_000, late=0, timepoint=1, day="2026-09-16", estimated=False):
    """One arrival. `late` is seconds; negative is early."""
    return {"day": day, "trip_id": trip, "service": service,
            "direction": direction, "stop_name": stop, "atco": f"4400{stop}",
            "stop_index": index, "scheduled_secs": sched,
            "observed_secs": sched + late, "lateness_secs": late,
            "timepoint": timepoint, "journey_start": "10:00",
            "estimated": estimated}


def journey(trip, stops, late, service="700", direction="westbound", timepoint=1,
            estimated=False):
    """One bus's run: `stops` calls a minute apart, each `late` seconds late."""
    return [obs(trip=trip, service=service, direction=direction,
                stop=f"S{i:02d}", index=i, sched=36_000 + i * 60,
                late=late, timepoint=timepoint, estimated=estimated)
            for i in range(stops)]


# ── Two denominators, always ────────────────────────────────

def test_a_cell_counts_journeys_as_well_as_arrivals():
    rows = journey("A", 10, 0) + journey("B", 10, 0) + journey("C", 10, 0)
    cell = rs.stats(rows)
    assert cell["observations"] == 30
    assert cell["journeys"] == 3, "thirty stops on three buses read as thirty buses"


def test_one_bad_journey_cannot_carry_a_whole_service():
    # One journey 20 minutes late at 60 stops, five punctual journeys of three
    # stops each. By arrival the median is a disaster; by journey it is not,
    # and "one bad run" is the truth — which is what the per-journey figure says.
    rows = journey("BAD", 60, 20 * 60)
    for i in range(5):
        rows += journey(f"OK{i}", 3, 0)
    cell = rs.stats(rows)
    assert cell["median_secs"] == 20 * 60, "the arrival median should be dominated"
    assert cell["per_journey_median_secs"] == 0, \
        "one journey dominated the per-journey figure too"
    assert cell["journeys"] == 6


def test_a_long_route_is_not_ranked_worse_for_being_long():
    # Both services run six minutes late everywhere; one has twelve times the
    # stops. Any figure that ranks them differently is measuring route length.
    long_route = rs.stats(journey("L", 60, 6 * 60, service="700"))
    short_route = rs.stats(journey("S", 5, 6 * 60, service="2"))
    assert long_route["median_secs"] == short_route["median_secs"]
    assert long_route["not_on_time_share"] == short_route["not_on_time_share"]
    assert long_route["observations"] == 12 * short_route["observations"]


# ── Bands and the official definition ───────────────────────

def test_the_bands_are_the_official_boundaries():
    assert rs.band(-61) == "early" and rs.band(-60) == "on_time"
    assert rs.band(359) == "on_time" and rs.band(360) == "late"
    assert rs.band(900) == "late" and rs.band(901) == "very_late"


def test_shares_and_counts_agree():
    rows = journey("A", 10, 0) + journey("B", 10, 10 * 60)
    cell = rs.stats(rows)
    assert sum(cell["bands"].values()) == cell["observations"]
    assert abs(cell["on_time_share"] + cell["not_on_time_share"] - 1) < 1e-9
    assert cell["on_time_share"] == 0.5


# ── Which hour a delay belongs to ───────────────────────────

def test_delay_belongs_to_the_hour_the_bus_was_due():
    # Due 17:45, twenty minutes late, so seen at 18:05. Counted at 18:00 the
    # delay leaves the hour that caused it and the peak looks better than it is.
    row = obs(sched=17 * 3600 + 45 * 60, late=20 * 60)
    assert rs.scheduled_hour(row) == "17"
    assert rs.observed_hour(row) == "18", "the observed bucket is the other answer"


def test_grouping_by_hour_uses_the_scheduled_hour():
    rows = [obs(trip="A", sched=17 * 3600 + 45 * 60, late=20 * 60),
            obs(trip="B", sched=17 * 3600 + 50 * 60, late=20 * 60)]
    assert set(rs.group_stats(rows, rs.scheduled_hour)) == {"17"}


# ── Where the time goes ─────────────────────────────────────

def test_a_segment_measures_time_gained_not_lateness_carried():
    # The bus reaches A already two minutes down and loses six more by C. A
    # table of stop lateness would blame C for eight minutes; the segment
    # table says where the six went.
    rows = [obs(trip="J", stop="A", index=0, sched=36_000, late=120),
            obs(trip="J", stop="B", index=1, sched=36_060, late=120),
            obs(trip="J", stop="C", index=2, sched=36_120, late=480)]
    segs = {k[:3]: v for k, v in rs.segment_stats(rows).items()}
    assert segs[("A", "B", "westbound")]["median_gained_secs"] == 0
    assert segs[("B", "C", "westbound")]["median_gained_secs"] == 360


def test_a_segment_ratio_is_a_share_of_the_scheduled_time():
    # Six minutes lost on a ten-minute leg is 60% over the timetable, not
    # "36 minutes per scheduled minute" — which is what dividing seconds by
    # minutes printed before this test existed.
    rows = [obs(trip="J", stop="A", index=0, sched=36_000, late=0),
            obs(trip="J", stop="B", index=1, sched=36_600, late=360)]
    cell = {k[:3]: v for k, v in rs.segment_stats(rows).items()}[("A", "B", "westbound")]
    assert cell["scheduled_secs"] == 600
    assert cell["median_gained_secs"] == 360
    assert abs(cell["over_scheduled_share"] - 0.6) < 1e-9, \
        f"expected 0.6 of the scheduled time, got {cell['over_scheduled_share']}"


def test_the_two_sides_of_a_road_are_not_averaged_together():
    # The same pair of stop names in opposite directions is two roads with
    # different traffic; merging them hides whichever is worse.
    rows = [obs(trip="W", stop="A", index=0, sched=36_000, late=0, direction="westbound"),
            obs(trip="W", stop="B", index=1, sched=36_120, late=300, direction="westbound"),
            obs(trip="E", stop="A", index=0, sched=36_000, late=0, direction="eastbound"),
            obs(trip="E", stop="B", index=1, sched=36_120, late=0, direction="eastbound")]
    segs = {k[:3]: v for k, v in rs.segment_stats(rows).items()}
    assert ("A", "B", "westbound") in segs and ("A", "B", "eastbound") in segs
    assert segs[("A", "B", "westbound")]["median_gained_secs"] == 300
    assert segs[("A", "B", "eastbound")]["median_gained_secs"] == 0


def test_an_interpolated_stop_is_never_a_segment_endpoint():
    # The middle stop's time is GTFS's estimate, so a segment ending there
    # would measure the interpolation. A → C is the real leg.
    rows = [obs(trip="J", stop="A", index=0, sched=36_000, late=0, timepoint=1),
            obs(trip="J", stop="B", index=1, sched=36_060, late=600, timepoint=0),
            obs(trip="J", stop="C", index=2, sched=36_120, late=300, timepoint=1)]
    segs = {k[:3]: v for k, v in rs.segment_stats(rows).items()}
    assert list(segs) == [("A", "C", "westbound")]
    assert segs[("A", "C", "westbound")]["median_gained_secs"] == 300
    assert segs[("A", "C", "westbound")]["median_stops_apart"] == 2


def test_segments_can_be_split_by_the_hour_the_bus_traversed_them():
    # Time lost belongs to the traffic the bus sat in, so this bucket is
    # observed rather than scheduled — the opposite choice to punctuality.
    rows = [obs(trip="J", stop="A", index=0, sched=17 * 3600 + 55 * 60, late=0),
            obs(trip="J", stop="B", index=1, sched=17 * 3600 + 58 * 60, late=600)]
    keyed = {k[:4]: v for k, v in rs.segment_stats(rows, hour=True).items()}
    assert list(keyed) == [("A", "B", "westbound", "17")], \
        "the leg was entered at 17:55; its exit must not move the cohort into 18:00"


# ── Not publishing what is too thin ─────────────────────────

def test_thin_cells_are_held_back_and_counted():
    rows = (journey("A", 40, 0) + journey("B", 40, 0) + journey("C", 40, 0)
            + journey("D", 40, 0) + journey("E", 40, 0)
            + journey("THIN", 2, 0, service="19"))
    cells = rs.group_stats(rows, lambda r: r["service"])
    kept, thin = rs.suppress(cells)
    assert list(kept) == ["700"]
    assert list(thin) == ["19"], "a two-arrival service was publishable"
    assert thin["19"]["observations"] == 2, "the suppressed count is what says how thin"


def test_enough_arrivals_is_not_enough_journeys():
    # Forty arrivals from one bus is one bus's day, not a measurement of a
    # service — the second floor exists for exactly this shape.
    cells = rs.group_stats(journey("ONLY", 40, 0), lambda r: r["service"])
    kept, thin = rs.suppress(cells)
    assert not kept and list(thin) == ["700"]


# ── The mean, if you insist ─────────────────────────────────

def test_a_mean_never_travels_without_its_caveat():
    cell = rs.stats(journey("A", 10, 120), mean=True)
    assert cell["mean_secs"] == 120
    assert "censored" in cell["mean_caveat"]
    assert "mean_secs" not in rs.stats(journey("A", 10, 120)), \
        "the mean appeared without being asked for"


def test_a_cell_says_when_the_matching_window_is_pressing_on_it():
    # Arrivals piled against the bound mean a tail was cut off, so the figure
    # is a floor. Silence here would present a censored count as a measurement.
    rows = journey("A", 10, 0) + [obs(trip="B", late=-300), obs(trip="B", late=1500)]
    assert rs.stats(rows)["at_censoring_bound"] == 2
    assert rs.stats(journey("A", 10, 0))["at_censoring_bound"] == 0


# ── An interpolated time is not an observation ───────────────

def test_an_interpolated_arrival_is_not_counted_as_punctuality():
    # Its time is the lateness either side divided by the timetable, so counting
    # it measures this module rather than the bus. The daily summary has always
    # dropped these; the query tool and the rollups did not, so the same day
    # could be summarised two ways and both published as "the" figure.
    rows = journey("SEEN", 10, 0) + journey("GUESSED", 10, 20 * 60, estimated=True)
    cell = rs.stats(rows)
    assert cell["observations"] == 10, \
        f'{cell["observations"]} arrivals counted where 10 were seen'
    assert cell["journeys"] == 1
    assert cell["median_secs"] == 0, "interpolated lateness reached the median"
    assert cell["estimates_excluded"] == 10
    assert cell["measured_only"] is True


def test_including_estimates_has_to_be_asked_for_and_says_so():
    # The opt-in exists for exploring the data. A cell that came back with
    # estimates in it must not look like one that did not, or the flag is a
    # trap rather than a control.
    rows = journey("SEEN", 10, 0) + journey("GUESSED", 10, 20 * 60, estimated=True)
    cell = rs.stats(rows, include_estimates=True)
    assert cell["observations"] == 20
    assert cell["measured_only"] is False
    assert cell["estimates_excluded"] == 0


def test_a_cell_of_nothing_but_estimates_is_not_a_figure():
    # Publishing "median 0, from 30 arrivals" off thirty interpolations is the
    # worst case: it looks like the best-evidenced cell in the table.
    assert rs.stats(journey("GUESSED", 30, 0, estimated=True)) is None


def test_estimates_are_excluded_from_every_grouping_too():
    # The filter has to sit inside the cell, not at each call site. A caller
    # that forgot would produce a table whose rows disagree with its total.
    rows = journey("SEEN", 10, 0, service="700")
    rows += journey("GUESSED", 10, 20 * 60, service="700", estimated=True)
    rows += journey("ALSO", 10, 0, service="37")
    by_service = rs.group_stats(rows, lambda r: r["service"])
    assert by_service["700"]["observations"] == 10
    assert by_service["700"]["median_secs"] == 0
    assert by_service["37"]["observations"] == 10


def losing_time(trip, stops, per_stop, estimated=False):
    """A run that falls further behind at every stop, so each leg *gains* time.

    A uniform lateness would gain nothing between any two stops, which is the
    trap this pair of tests fell into first: the assertion held whether or not
    the filter was there, and certified the bug as fixed.
    """
    return [obs(trip=trip, stop=f"S{i:02d}", index=i, sched=36_000 + i * 60,
                late=i * per_stop, timepoint=1, estimated=estimated)
            for i in range(stops)]


def test_a_segment_is_not_timed_between_two_guesses():
    # A stop can be a genuine timing point *and* interpolated: the operator
    # commits to a time there and our bus was not seen at it. "Lateness gained"
    # between two such stops is arithmetic about arithmetic, and it lands in
    # the segment table a bus-priority ask would be built from.
    rows = []
    for i in range(6):
        rows += losing_time(f"SEEN{i}", 4, 0)              # keeps to time
    for i in range(6):
        rows += losing_time(f"GUESS{i}", 4, 300, estimated=True)   # 5 min a stop
    segments = {k[:3]: v for k, v in rs.segment_stats(rows).items()}
    assert segments, "the measured segments were dropped along with the guesses"
    for key, cell in segments.items():
        assert cell["median_gained_secs"] == 0, \
            f"{key} gained time that was never observed: {cell}"
        assert cell["measured_only"] is True

    # And the opt-in must actually reach segments, or the flag is decoration.
    explored = rs.segment_stats(rows, include_estimates=True)
    assert explored, "the opt-in does nothing"
    assert max(c["median_gained_secs"] for c in explored.values()) > 0, \
        "including estimates changed nothing, so the filter is not being applied"
