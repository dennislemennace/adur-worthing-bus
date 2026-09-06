"""Tests for the A-to-B journey lookup behind the ticket-boundary calculator.

Two layers are covered:

  * The pure plausibility helpers (`_plausible_span`, `path_has_time_gap`),
    which are what stop a midnight-wrapped trip from being served as a
    two-stop "journey" across the whole county.
  * The `Timetable.trips_connecting` / `stops_between` pair, exercised against
    the real SQLite build when one is present locally.

The DB-backed tests skip when data/timetable.sqlite is absent, so this file
still runs in a fresh checkout (the DB is a gitignored build artefact).

Run with:  pytest
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api.main import _from_time_of_day, _hhmm_to_secs  # noqa: E402
from api.timetable_db import (  # noqa: E402
    MAX_JOURNEY_SECS,
    MAX_LEG_GAP_SECS,
    Timetable,
    _plausible_span,
    path_has_time_gap,
)

DB_PATH = ROOT / "data" / "timetable.sqlite"
needs_db = pytest.mark.skipif(
    not DB_PATH.exists(),
    reason="data/timetable.sqlite is a build artefact and isn't present",
)


# ── Plausibility helpers (no DB needed) ─────────────────────

def _span(depart, arrive, from_seq=0, to_seq=10):
    return {
        "depart_secs": depart, "arrive_secs": arrive,
        "from_seq": from_seq, "to_seq": to_seq,
    }


def test_normal_span_is_plausible():
    # 40 minutes across 10 stops — an ordinary town-to-town run.
    assert _plausible_span(_span(32_400, 34_800)) is True


def test_backwards_span_is_rejected():
    assert _plausible_span(_span(34_800, 32_400)) is False


def test_zero_length_span_is_rejected():
    assert _plausible_span(_span(32_400, 32_400)) is False


def test_midnight_wrapped_span_is_rejected():
    # The real shape of the bug: consecutive stops (seq 3 -> 4) apparently
    # 22.8 hours apart, because the post-midnight tail was wrapped to ~0s.
    assert _plausible_span(_span(120, 82_200, from_seq=3, to_seq=4)) is False


def test_overlong_journey_is_rejected():
    assert _plausible_span(_span(0, MAX_JOURNEY_SECS + 1)) is False


def test_slow_rural_run_still_passes():
    # 3 stops over 80 minutes is slow but real; don't over-filter.
    assert _plausible_span(_span(0, 4_800, from_seq=0, to_seq=3)) is True


def test_path_gap_detects_wrapped_tail():
    stops = [
        {"dep_secs": 84_060}, {"dep_secs": 84_099}, {"dep_secs": 360},
    ]
    # 84_099 -> 360 goes backwards, which is not a *forward* gap...
    assert path_has_time_gap(stops) is False
    # ...but the forward jump is what the stitched span looks like:
    assert path_has_time_gap([{"dep_secs": 360}, {"dep_secs": 84_060}]) is True


def test_path_gap_ignores_normal_layovers():
    stops = [{"dep_secs": t} for t in (32_400, 32_520, 33_000, 33_600)]
    assert path_has_time_gap(stops) is False


def test_path_gap_tolerates_missing_times():
    assert path_has_time_gap([{"dep_secs": None}, {"dep_secs": 100}]) is False
    assert path_has_time_gap([{}, {}]) is False


def test_path_gap_boundary_is_exclusive():
    base = 30_000
    assert path_has_time_gap(
        [{"dep_secs": base}, {"dep_secs": base + MAX_LEG_GAP_SECS}]) is False
    assert path_has_time_gap(
        [{"dep_secs": base}, {"dep_secs": base + MAX_LEG_GAP_SECS + 1}]) is True


# ── Against the real timetable ──────────────────────────────

@pytest.fixture(scope="module")
def tt():
    table = Timetable(DB_PATH)
    if not table.ok():
        pytest.skip("timetable did not open")
    return table


@needs_db
def test_same_stop_returns_nothing(tt):
    atco = next(iter(tt.stops))
    assert tt.trips_connecting(atco, atco) == []


@needs_db
def test_unknown_stops_return_nothing(tt):
    real = next(iter(tt.stops))
    assert tt.trips_connecting("NOT-A-STOP", real) == []
    assert tt.trips_connecting(real, "NOT-A-STOP") == []


@needs_db
def test_connecting_trips_run_in_the_right_direction(tt):
    """Every option must reach `to` after `from`, never before."""
    trip_id, first, last, _ = next(iter(tt.service_endpoints("700")))
    stops = tt.trip_stops_for(trip_id)
    assert len(stops) > 5
    a, b = stops[0][1], stops[-1][1]

    found = tt.trips_connecting(a, b)
    assert found, "expected at least one direct trip along a 700 run"
    for t in found:
        assert t["to_seq"] > t["from_seq"]
        assert t["arrive_secs"] > t["depart_secs"]


@needs_db
def test_path_between_endpoints_is_not_truncated(tt):
    """The regression this whole filter exists for.

    A Worthing-to-Brighton journey must come back with the stops in between,
    not a two-stop hop produced by a midnight-wrapped trip.
    """
    trip_id, _, _, _ = next(iter(tt.service_endpoints("700")))
    stops = tt.trip_stops_for(trip_id)
    a, b = stops[0][1], stops[-1][1]

    options = tt.trips_connecting(a, b)
    assert options
    best = options[0]
    path = tt.stops_between(best["trip_id"], best["from_seq"], best["to_seq"])

    assert len(path) > 10, f"path collapsed to {len(path)} stops"
    assert path[0]["atco"] == a
    assert path[-1]["atco"] == b
    assert not path_has_time_gap(path)


@needs_db
def test_path_stops_carry_coordinates(tt):
    """The zone classifier needs lat/lon on every stop, with no extra lookups."""
    trip_id, _, _, _ = next(iter(tt.service_endpoints("700")))
    stops = tt.trip_stops_for(trip_id)
    options = tt.trips_connecting(stops[0][1], stops[-1][1])
    path = tt.stops_between(
        options[0]["trip_id"], options[0]["from_seq"], options[0]["to_seq"])

    for stop in path:
        assert stop["lat"] is not None and stop["lon"] is not None
        assert 50.0 < stop["lat"] < 51.5, "outside the plausible latitude band"
        assert -1.5 < stop["lon"] < 0.5, "outside the plausible longitude band"
        assert stop["name"]


@needs_db
def test_path_sequence_is_monotonic(tt):
    trip_id, _, _, _ = next(iter(tt.service_endpoints("700")))
    stops = tt.trip_stops_for(trip_id)
    options = tt.trips_connecting(stops[0][1], stops[-1][1])
    path = tt.stops_between(
        options[0]["trip_id"], options[0]["from_seq"], options[0]["to_seq"])
    seqs = [s["seq"] for s in path]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == len(seqs)


@needs_db
def test_noc_lookup_is_memoized_and_stable(tt):
    first = tt.noc_for_short_name("700")
    assert tt._noc_map is not None
    assert tt.noc_for_short_name("700") == first
    assert tt.noc_for_short_name("__no_such_route__") == ""


@needs_db
def test_limit_is_respected(tt):
    trip_id, _, _, _ = next(iter(tt.service_endpoints("700")))
    stops = tt.trip_stops_for(trip_id)
    got = tt.trips_connecting(stops[0][1], stops[-1][1], limit=3)
    assert len(got) <= 3


# ── Costing a journey at a fixed time of day ────────────────
#
# What a journey costs must not depend on which trip the timetable happens to
# list first. It used to: trips came back earliest-first, so a coastal pair was
# always answered with the 00:45 N700 — a night service carrying a £2
# supplement, and one the all-operator ticket is not valid on. Every quote was
# a night fare, asked at any hour.

def test_from_time_of_day_starts_at_the_anchor():
    trips = [
        {"depart_secs": 2700},    # 00:45
        {"depart_secs": 43500},   # 12:05
        {"depart_secs": 25200},   # 07:00
        {"depart_secs": 75600},   # 21:00
    ]
    order = [t["depart_secs"] for t in _from_time_of_day(trips, 12 * 3600)]
    assert order == [43500, 75600, 2700, 25200]


def test_from_time_of_day_keeps_everything_it_was_given():
    """Reordering, never filtering — a pair served only overnight still has to
    come back with something rather than looking unreachable."""
    trips = [{"depart_secs": s} for s in (2700, 5400, 9000)]
    assert len(_from_time_of_day(trips, 12 * 3600)) == 3


def test_from_time_of_day_places_past_midnight_trips_on_the_clock():
    """GTFS writes a trip continuing past midnight as 24:xx and beyond. 24:30
    is half past midnight, so it belongs with the small hours, not the evening."""
    trips = [
        {"depart_secs": 88200},   # 24:30 == 00:30
        {"depart_secs": 46800},   # 13:00
    ]
    assert [t["depart_secs"] for t in _from_time_of_day(trips, 12 * 3600)] == [46800, 88200]


def test_hhmm_to_secs():
    assert _hhmm_to_secs("00:00") == 0
    assert _hhmm_to_secs("12:00") == 43200
    assert _hhmm_to_secs("23:59") == 86340


@needs_db
def test_trips_connecting_can_start_from_a_time_of_day(tt):
    """The cap inside trips_connecting is what made this necessary: keeping the
    first forty trips of the service day threw the daytime ones away before the
    caller ever saw them."""
    trip_id, _, _, _ = next(iter(tt.service_endpoints("700")))
    stops = tt.trip_stops_for(trip_id)
    a, b = stops[0][1], stops[-1][1]

    noon = 12 * 3600
    from_noon = tt.trips_connecting(a, b, limit=5, from_secs=noon)
    if not from_noon:
        pytest.skip("no direct trips between these stops in this build")
    # Every returned trip is nearer noon, going forwards, than the earliest of
    # the day would be.
    first = from_noon[0]["depart_secs"] % 86400
    assert (first - noon) % 86400 <= (
        (tt.trips_connecting(a, b, limit=1)[0]["depart_secs"] % 86400) - noon) % 86400


# ── One-change itineraries ──────────────────────────────────
#
# The fare side could say a journey needs two tickets; it could not say what the
# journey *is*. These pin the two things a first version got wrong, both of
# which made it report no way at all between places people plainly travel
# between.

@needs_db
def test_a_change_can_be_a_short_walk(tt):
    """Southwick Square to Mile Oak is the case that set the walk radius.

    The 46 and the 1X do not share a stop near Portslade — the 1X is around the
    corner and the 1 is five minutes down the road. Requiring the identical
    stop, or even a same-named pair of poles, reported no way to travel between
    two places two miles apart in the same city.
    """
    week = tt.sample_week()
    got = tt.interchange_legs("4400AD0259", "149000006483", week["monday"])
    assert got is not None, (
        "no one-change itinerary Southwick Square -> Mile Oak Gardens; the "
        "change is a short walk, so requiring one stop misses it")
    assert len(got["legs"]) == 2
    assert got["walk_metres"] <= tt.INTERCHANGE_WALK_KM * 1000
    assert got["total_minutes"] > 0


@needs_db
def test_a_stop_only_served_outbound_has_no_arrival(tt):
    """Mile Oak Road Shops sits at seq 3-7 of route 1: buses start near there
    and run outbound to Whitehawk. You can board, not arrive. Reporting no
    itinerary is correct, and must not be 'fixed' by widening the search."""
    week = tt.sample_week()
    assert tt.interchange_legs("4400AD0259", "149000006479", week["monday"]) is None


@needs_db
def test_a_later_bus_can_be_the_one_that_connects(tt):
    """Keeping only the earliest arrival at each stop throws away the later one
    that actually makes the connection. Several are kept, so a pair with any
    valid connection resolves."""
    week = tt.sample_week()
    got = tt.interchange_legs("4400WO0253", "149000007677", week["monday"])
    assert got is not None, "Worthing to Hangleton is one change via the coast"
    one, two = got["legs"]
    assert one["service"] and two["service"]
    assert got["wait_minutes"] >= 0


@needs_db
def test_a_through_bus_gets_no_interchange(tt):
    """Where a direct service exists the endpoint returns options instead, so
    the search should never be asked — but it must also not invent a change
    between a stop and itself."""
    assert tt.interchange_legs("4400AD0117", "4400AD0117",
                               tt.sample_week()["monday"]) is None
