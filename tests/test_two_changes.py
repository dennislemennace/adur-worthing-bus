"""Tests for journeys that take three buses.

The journey checker stopped at one change, deliberately, and for most pairs on
this coast that is enough. Sompting to Brighton Marina is not one of them: no
single change connects the two on any day, and the preset built to show that
drew two pins and a dashed line, while the fare panel went on to cost a journey
it could not describe. With the three buses known, the map can draw them and
the tickets can be judged against the buses actually taken.

Run with:  pytest
"""

import csv
import io
import sys
import zipfile
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import build_timetable as bt        # noqa: E402
import json_to_sqlite as j2s        # noqa: E402
from api.timetable_db import Timetable   # noqa: E402

# A line along the coast, well apart so no change can be made on foot except
# where one is meant to be.
STOPS = [
    ("4400A001", "Start",   50.830, -0.360),
    ("4400X001", "Change1", 50.825, -0.320),
    ("4400Y001", "Change2", 50.822, -0.200),
    ("4400Y002", "Change2 opposite", 50.8221, -0.2005),   # a short walk away
    ("4400B001", "End",     50.815, -0.100),
    ("4400C001", "Elsewhere", 50.812, -0.080),
]

TRIPS = [
    # A -> X
    ("T1",      "R16",  [(1, "4400A001", "12:26:00"), (2, "4400X001", "12:44:00")]),
    # X -> Y: one that leaves too soon after T1 arrives, one that connects.
    ("T2_SOON", "R700", [(1, "4400X001", "12:46:00"), (2, "4400Y001", "13:20:00")]),
    ("T2",      "R700", [(1, "4400X001", "12:49:00"), (2, "4400Y001", "13:31:00")]),
    # Y' -> B, from the pole across the road: the second change is a walk.
    ("T3",      "R47",  [(1, "4400Y002", "13:48:00"), (2, "4400B001", "14:10:00")]),
    # Y -> B but a wait of over an hour: not a connection.
    ("T3_LATE", "R47",  [(1, "4400Y001", "15:00:00"), (2, "4400B001", "15:22:00")]),
    # Y -> B one minute after T2 arrives. Faster than T3 if it could be caught,
    # which it cannot: this is what binds the *second* change's minimum.
    ("T3_TOO_SOON", "R47", [(1, "4400Y001", "13:32:00"), (2, "4400B001", "13:50:00")]),
    # Y -> Elsewhere, only after a wait of nearly an hour and a half. With no
    # better option to fall back on, this binds the second change's wait cap.
    ("T4_ONLY_LATE", "R47", [(1, "4400Y001", "15:00:00"), (2, "4400C001", "15:25:00")]),
]


def _csv(rows, header):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue()


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture(scope="module")
def tt(tmp_path_factory, monkeypatch_module):
    monkeypatch_module.setenv("SKIP_OSRM", "1")
    feed = tmp_path_factory.mktemp("gtfs") / "feed.zip"
    with zipfile.ZipFile(feed, "w") as z:
        z.writestr("agency.txt", _csv([("AG1", "Test Operator", "TSTO")],
                                      ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv(
            [("R16", "AG1", "16", "One", 3), ("R700", "AG1", "700", "Two", 3),
             ("R47", "AG1", "47", "Three", 3)],
            ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv([(t, r, "ALL", "Somewhere", "") for t, r, _ in TRIPS],
                                     ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", _csv(
            [(t, seq, sid, hms, hms) for t, _, calls in TRIPS for seq, sid, hms in calls],
            ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"]))
        z.writestr("calendar.txt", _csv(
            [("ALL", 1, 1, 1, 1, 1, 1, 1, "20260101", "20271231")],
            ["service_id", "monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday", "start_date", "end_date"]))
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(bt.parse_gtfs(str(feed)), out)
    return Timetable(out, allow_fetch=False)


DAY = date(2026, 9, 14)
NOON = 12 * 3600


def test_one_change_is_not_enough_here(tt):
    # The premise: this pair genuinely needs two changes.
    assert tt.interchange_legs("4400A001", "4400B001", DAY, NOON) is None


def test_three_buses_in_order(tt):
    got = tt.interchange_legs_two("4400A001", "4400B001", DAY, NOON)
    assert got is not None, "no two-change journey found for a pair that has one"
    assert [leg["service"] for leg in got["legs"]] == ["16", "700", "47"]
    assert len(got["changes"]) == 2


def test_a_bus_leaving_before_you_can_change_is_not_taken(tt):
    """T2_SOON leaves two minutes after T1 arrives, under the four a change needs."""
    got = tt.interchange_legs_two("4400A001", "4400B001", DAY, NOON)
    assert got["legs"][1]["depart"] == "12:49", got["legs"][1]


def test_the_second_change_can_be_a_walk(tt):
    got = tt.interchange_legs_two("4400A001", "4400B001", DAY, NOON)
    second = got["changes"][1]
    assert second["change_at"]["atco"] == "4400Y001"
    assert second["board_at"]["atco"] == "4400Y002"
    assert 0 < second["walk_metres"] <= tt.INTERCHANGE_WALK_KM * 1000


def test_an_hour_long_wait_is_not_a_connection(tt):
    got = tt.interchange_legs_two("4400A001", "4400B001", DAY, NOON)
    assert got["legs"][2]["depart"] == "13:48", "the 15:00 after a 90-minute wait was taken"
    assert all(c["wait_minutes"] <= 60 for c in got["changes"])


def test_the_totals_add_up(tt):
    got = tt.interchange_legs_two("4400A001", "4400B001", DAY, NOON)
    assert got["total_minutes"] == (14 * 60 + 10 - (12 * 60 + 26)), got["total_minutes"]


def test_a_one_change_itinerary_lists_its_change_too(tt):
    """The page draws changes from one list whatever the count, so the one-change
    result carries it as well as the older single fields."""
    got = tt.interchange_legs("4400X001", "4400B001", DAY, NOON)
    assert got is not None
    assert len(got["changes"]) == 1
    assert got["changes"][0]["change_at"] == got["change_at"]


# ── Several routes, so a page can compare them ──────────────
#
# The ticket view could only ever price the one itinerary it was handed, so it
# could not say that a journey is 54 minutes on two operators' tickets or 86 on
# one. These keep the search honest about returning *different* journeys rather
# than the same one several times over.


def test_the_best_option_is_the_one_the_old_call_returns(tt):
    """Existing callers must not notice. `interchange_legs` is now a thin
    wrapper, and if the two ever disagreed the map and the fares would be
    describing different journeys."""
    one = tt.interchange_legs("4400X001", "4400B001", DAY, NOON)
    many = tt.interchange_options("4400X001", "4400B001", DAY, NOON)
    assert many, "the multi-route search found nothing where the single one did"
    assert many[0] == one


def test_no_two_options_are_the_same_journey(tt):
    """Deduplicated on the services ridden.

    Keyed any more finely — by the change stop, say — the search returns the
    same two buses several times over, once per stop they happen to meet at,
    which is one journey to anybody making it and crowds out the routes that
    differ in the way that matters: which company you are paying.
    """
    many = tt.interchange_options("4400X001", "4400B001", DAY, NOON, limit=10)
    shapes = [tuple(leg["service"] for leg in o["legs"]) for o in many]
    assert len(shapes) == len(set(shapes)), shapes


def test_it_returns_no_more_than_asked_for(tt):
    assert len(tt.interchange_options("4400X001", "4400B001", DAY, NOON,
                                      limit=1)) <= 1


def test_a_pair_with_no_one_change_journey_gives_an_empty_list(tt):
    """Empty, not None. A caller iterating the result should not have to ask
    which kind of nothing it got."""
    assert tt.interchange_options("4400A001", "4400B001", DAY, NOON) == []


def test_asking_twice_costs_nothing(tt):
    """Memoised per timetable build. Every reader who opens the ticket view
    presses a preset, and on a shared tenth of a CPU the second press should not
    pay for the search again."""
    first = tt.interchange_options("4400X001", "4400B001", DAY, NOON)
    again = tt.interchange_options("4400X001", "4400B001", DAY, NOON)
    assert first is again, "the second call searched again instead of recalling"


def test_a_different_day_is_a_different_answer(tt):
    """The memo must not serve Monday's buses for a Sunday. It is keyed on the
    day as well as the pair — a cache that ignores the day is worse than none."""
    other = date(2026, 9, 13)      # the Sunday before DAY
    assert (tt.interchange_options("4400X001", "4400B001", other, NOON)
            is not tt.interchange_options("4400X001", "4400B001", DAY, NOON))


def test_the_second_change_needs_time_too(tt):
    """T3_TOO_SOON would reach the end twenty minutes sooner, one minute after
    the 700 arrives. The first change's minimum is tested above; this is the
    same rule at the second change, where it was not being exercised."""
    got = tt.interchange_legs_two("4400A001", "4400B001", DAY, NOON)
    assert got["legs"][2]["depart"] == "13:48", \
        f"a bus leaving one minute after arrival was taken: {got['legs'][2]}"


def test_no_itinerary_when_the_only_last_bus_is_over_an_hour_away(tt):
    """Nothing else goes to Elsewhere, so ignoring the cap would produce an
    itinerary rather than just a worse one."""
    assert tt.interchange_legs_two("4400A001", "4400C001", DAY, NOON) is None
