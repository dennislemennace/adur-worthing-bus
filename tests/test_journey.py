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
