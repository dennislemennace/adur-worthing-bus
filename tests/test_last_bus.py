"""Tests for "the last direct bus home from central Brighton".

The night-service objective says an evening in Brighton ends early for anyone
living in Adur or Worthing. That is a claim about a time, so the stop panel now
states the time for the reader's own stop: the latest scheduled trip that
leaves a stop in central Brighton and later calls here.

Three ways to get it wrong, each of which gives a plausible-looking answer:

  * taking the latest trip that calls at the stop, whichever way it is going,
  * reading 24:05 as five past midnight at the *start* of the day, and
  * trusting a feed that writes a small-hours departure as 00:20 rather than
    24:20, which makes the latest bus of the night look like the earliest.

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

STOPS = [
    ("1490HUB1", "Old Steine",           50.8230, -0.1370),   # inside the box
    ("1490HUB2", "Churchill Square",     50.8245, -0.1450),   # inside the box
    ("4400SH01", "Shoreham High Street", 50.8320, -0.2750),   # the reader's stop
    ("4400LA01", "Lancing",              50.8270, -0.3250),
    ("4400WO01", "Worthing",             50.8100, -0.3700),
]

# (trip, route, service, [(seq, stop, time), ...])
TRIPS = [
    # Weekday: the last one. Arrives after midnight, departs before it.
    ("T_WK_LATE",  "R700",  "WK",  [(1, "1490HUB1", "23:50:00"), (2, "4400SH01", "24:25:00"),
                                    (3, "4400LA01", "24:40:00")]),
    # Weekday, earlier, calling at two hub stops.
    ("T_WK_EARLY", "R700",  "WK",  [(1, "1490HUB2", "22:00:00"), (2, "1490HUB1", "22:05:00"),
                                    (3, "4400SH01", "22:40:00")]),
    # Saturday: a 23:50 and a night bus at 24:05. The night bus is the last.
    ("T_SAT",      "R700",  "SAT", [(1, "1490HUB1", "23:50:00"), (2, "4400SH01", "24:25:00")]),
    ("T_NIGHT",    "RN700", "SAT", [(1, "1490HUB1", "24:05:00"), (2, "4400SH01", "24:40:00")]),
    # Friday only, and written the careless way: 00:20 for twenty past midnight.
    ("T_FRI_ZERO", "RN700", "FRI", [(1, "1490HUB2", "00:20:00"), (2, "4400SH01", "00:50:00")]),
    # Every day, the wrong way: leaves the reader's stop for Brighton.
    ("T_REVERSE",  "R700",  "ALL", [(1, "4400SH01", "23:59:00"), (2, "1490HUB1", "24:40:00")]),
    # Weekday, and not a night bus at all: the first 700 of the next morning,
    # which feeds are allowed to write as 28:37 on the day before. Found in the
    # real timetable, where it was offered as the last bus home at every stop.
    ("T_MORNING",  "R700",  "WK",  [(1, "1490HUB1", "28:37:00"), (2, "4400SH01", "29:10:00")]),
    # Every day, never goes near Brighton.
    ("T_NOHUB",    "R2",    "ALL", [(1, "4400WO01", "23:58:00"), (2, "4400SH01", "24:30:00")]),
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
            [("R700", "AG1", "700", "Coast", 3), ("RN700", "AG1", "N700", "Night", 3),
             ("R2", "AG1", "2", "Inland", 3)],
            ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv(
            [(t, r, s, "Somewhere", "") for t, r, s, _ in TRIPS],
            ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", _csv(
            [(t, seq, sid, hms, hms) for t, _, _, calls in TRIPS for seq, sid, hms in calls],
            ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"]))
        z.writestr("calendar.txt", _csv(
            [("WK",  1, 1, 1, 1, 1, 0, 0, "20260101", "20271231"),
             ("SAT", 0, 0, 0, 0, 0, 1, 0, "20260101", "20271231"),
             ("FRI", 0, 0, 0, 0, 1, 0, 0, "20260101", "20271231"),
             ("ALL", 1, 1, 1, 1, 1, 1, 1, "20260101", "20271231")],
            ["service_id", "monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday", "start_date", "end_date"]))
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(bt.parse_gtfs(str(feed)), out)
    return Timetable(out, allow_fetch=False)


WEEK_OF = date(2026, 9, 14)   # a Monday


def last(tt, stop="4400SH01"):
    return tt.last_bus_from_hub(stop, from_day=WEEK_OF)


def test_the_fixture_kept_the_brighton_stops(tt):
    # Everything below depends on the ingest keeping stops outside West Sussex
    # when a route that touches West Sussex calls at them.
    assert "1490HUB1" in tt.stops and "1490HUB2" in tt.stops


def test_the_weekday_answer_is_the_latest_trip_home(tt):
    mon = last(tt)["days"]["monday"]["day_bus"]
    assert mon["depart"] == "23:50", mon
    assert mon["after_midnight"] is False
    assert mon["service"] == "700"
    assert mon["from"] == "Old Steine"


def test_a_night_bus_after_midnight_is_reported_as_the_night_bus(tt):
    # 24:05 is the end of Saturday night, not the start of Saturday.
    sat = last(tt)["days"]["saturday"]
    assert sat["night_bus"]["depart"] == "00:05", sat
    assert sat["night_bus"]["after_midnight"] is True
    assert sat["night_bus"]["service"] == "N700"


def test_the_night_bus_does_not_hide_when_the_ordinary_buses_stop(tt):
    """One figure would be the night bus at every stop, which reads as a well
    served evening. The last ordinary bus is the number the objective is about."""
    sat = last(tt)["days"]["saturday"]
    assert sat["day_bus"]["depart"] == "23:50", sat
    assert sat["day_bus"]["service"] == "700"
    assert last(tt)["days"]["monday"]["night_bus"] is None, \
        "a weekday with no night bus was given one"


def test_a_small_hours_time_written_as_00_20_still_counts_as_the_latest(tt):
    # Friday has the 23:50 weekday trip and a 00:20 written without the 24.
    fri = last(tt)["days"]["friday"]["night_bus"]
    assert fri["depart"] == "00:20", f"the 00:20 night bus was read as the morning: {fri}"
    assert fri["after_midnight"] is True


def test_a_trip_going_the_other_way_is_not_a_way_home(tt):
    # T_REVERSE runs every day and calls at the stop at 23:59, later than any
    # weekday trip home, but it is heading into Brighton.
    days = last(tt)["days"]
    assert days["sunday"] is None, f"a trip towards Brighton was offered as the last bus home: {days['sunday']}"


def test_a_stop_no_brighton_bus_serves_has_no_answer(tt):
    assert all(v is None for v in last(tt, "4400WO01")["days"].values())


def test_a_stop_inside_central_brighton_is_not_asked(tt):
    result = last(tt, "1490HUB2")
    assert result["inside_hub"] is True
    assert result["days"] == {}


def test_the_answer_says_which_week_it_describes(tt):
    result = last(tt)
    assert result["week_of"] == "2026-09-14"


def test_the_first_bus_of_the_next_morning_is_not_the_last_bus_of_the_night(tt):
    """GTFS lets a service day run past 24:00, so the 04:37 morning bus can be
    written as 28:37 on the previous day. It is numerically the latest, and it is
    nobody's way home from an evening out: the night ends at NIGHT_ENDS_SECS."""
    mon = last(tt)["days"]["monday"]["day_bus"]
    assert mon["depart"] == "23:50", f"the next morning's first bus was offered as the last one home: {mon}"
