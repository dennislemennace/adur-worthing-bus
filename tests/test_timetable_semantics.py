"""Tests for what survives the GTFS pipeline.

Three properties, none of which a row count can see:

  * a service day runs past midnight, and 24:05 is not 00:05,
  * the order stops are called at is the feed's, not a re-derivation, and
  * a service with no calendar row does not therefore run every day.

The first two are tested by round-tripping a small GTFS archive through the
real ingest and the real SQLite converter, because the defect lived in the
seam between them: each half looked reasonable alone.

Run with:  pytest
"""

import csv
import io
import sqlite3
import sys
import threading
import zipfile
from datetime import date, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import build_timetable as bt      # noqa: E402
import json_to_sqlite as j2s      # noqa: E402


# ── A GTFS archive small enough to reason about ─────────────

# Inside the West Sussex prefix so the ingest keeps them, and inside the
# bounding box so nothing else filters them out.
STOPS = [
    ("4400A", "Stop A", 50.82, -0.37),
    ("4400B", "Stop B", 50.82, -0.30),
    ("4400C", "Stop C", 50.82, -0.20),
]

# The trip that breaks things: it starts before midnight and finishes after.
# GTFS says exactly this — hours run past 24 within a service day — and the
# order below is the order the buses call, whatever the clock says.
OVERNIGHT = [
    ("T_NIGHT", 1, "4400A", "23:55:00"),
    ("T_NIGHT", 2, "4400B", "24:05:00"),
    ("T_NIGHT", 3, "4400C", "24:10:00"),
]

# Two stops timed to the same minute: nothing but stop_sequence can order
# these, so sorting on departure time is a coin toss.
SAME_MINUTE = [
    ("T_TIE", 1, "4400A", "09:00:00"),
    ("T_TIE", 2, "4400B", "09:00:00"),
    ("T_TIE", 3, "4400C", "09:01:00"),
]

# Written to the file out of order, which a feed is entitled to do.
SHUFFLED = [
    ("T_SHUF", 3, "4400C", "14:20:00"),
    ("T_SHUF", 1, "4400A", "14:00:00"),
    ("T_SHUF", 2, "4400B", "14:10:00"),
]


# Two trips on two routes that share the number 5, run by different
# companies. A {short_name: noc} map has one slot for both of them.
SHARED_NUMBER = [
    ("T_FIVE_A", 1, "4400A", "10:00:00"),
    ("T_FIVE_A", 2, "4400B", "10:10:00"),
    ("T_FIVE_B", 1, "4400B", "11:00:00"),
    ("T_FIVE_B", 2, "4400C", "11:10:00"),
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
def db_path(gtfs_zip, tmp_path_factory, monkeypatch_module):
    monkeypatch_module.setenv("SKIP_OSRM", "1")
    tt = bt.parse_gtfs(str(gtfs_zip))
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(tt, out)
    return out


@pytest.fixture(scope="module")
def gtfs_zip(tmp_path_factory):
    path = tmp_path_factory.mktemp("gtfs") / "feed.zip"
    trips = sorted({r[0] for r in OVERNIGHT + SAME_MINUTE + SHUFFLED})
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("agency.txt", _csv(
            [("AG1", "Test Operator", "TSTO"),
             ("AG2", "Other Operator", "OTHR")],
            ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(
            STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        # R2 and R3 share the number 5. Nothing but the route each trip
        # belongs to can say which company runs it.
        z.writestr("routes.txt", _csv(
            [("R1", "AG1", "700", "Coast", 3),
             ("R2", "AG1", "5",   "Inland", 3),
             ("R3", "AG2", "5",   "Other 5", 3)],
            ["route_id", "agency_id", "route_short_name", "route_long_name",
             "route_type"]))
        z.writestr("trips.txt", _csv(
            [(t, "R1", "SVC1", "Somewhere", "") for t in trips]
            + [("T_FIVE_A", "R2", "SVC1", "Inland", ""),
               ("T_FIVE_B", "R3", "SVC1", "Other 5", "")],
            ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", _csv(
            [(t, seq, sid, hms, hms)
             for (t, seq, sid, hms) in OVERNIGHT + SAME_MINUTE + SHUFFLED
             + SHARED_NUMBER],
            ["trip_id", "stop_sequence", "stop_id", "arrival_time",
             "departure_time"]))
        z.writestr("calendar.txt", _csv(
            [("SVC1", 1, 1, 1, 1, 1, 1, 1, "20260101", "20271231")],
            ["service_id", "monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday", "start_date", "end_date"]))
    return path


@pytest.fixture(scope="module")
def db(db_path):
    """The archive above, through the real ingest and the real converter."""
    con = sqlite3.connect(db_path)
    yield con
    con.close()


def calls(con, trip_id):
    """The stops of one trip, in the order the database says they are called."""
    return list(con.execute(
        "SELECT s.stop_id, st.dep_secs FROM stop_times st "
        "JOIN trips t ON t.tid = st.tid JOIN stops s ON s.sid = st.sid "
        "WHERE t.trip_id = ? ORDER BY st.seq", (trip_id,)))


# ── Service-day time ────────────────────────────────────────

def test_a_time_past_midnight_is_not_folded_into_the_morning():
    # 24:05 is five past midnight *at the end of this service day*. Folding it
    # to 00:05 files it under the wrong day and sorts it before the 23:55 it
    # follows.
    assert bt._hms_to_secs("24:05:00") == 24 * 3600 + 5 * 60
    assert bt._hms_to_secs("25:30:00") == 25 * 3600 + 30 * 60
    assert bt._hms_to_secs("23:55:00") == 23 * 3600 + 55 * 60


def test_an_overnight_trip_keeps_its_order(db):
    assert [c[0] for c in calls(db, "T_NIGHT")] == ["4400A", "4400B", "4400C"]


def test_an_overnight_trip_keeps_its_times(db):
    times = dict(calls(db, "T_NIGHT"))
    assert times["4400A"] == 23 * 3600 + 55 * 60
    assert times["4400B"] == 24 * 3600 + 5 * 60, "24:05 was folded to 00:05"
    assert times["4400C"] == 24 * 3600 + 10 * 60


# ── Stop order is the feed's, not a re-derivation ───────────

def test_stops_timed_to_the_same_minute_keep_their_sequence(db):
    assert [c[0] for c in calls(db, "T_TIE")] == ["4400A", "4400B", "4400C"]


def test_rows_written_out_of_order_are_restored_by_sequence(db):
    assert [c[0] for c in calls(db, "T_SHUF")] == ["4400A", "4400B", "4400C"]


def test_the_trips_first_departure_is_the_first_stop_not_the_earliest(db):
    # trip_endpoints drives GTFS-RT matching. On an overnight trip the earliest
    # departure_time and the first stop are different rows.
    row = db.execute(
        "SELECT s.stop_id, te.first_secs FROM trip_endpoints te "
        "JOIN trips t ON t.tid = te.tid JOIN stops s ON s.sid = te.first_sid "
        "WHERE t.trip_id = 'T_NIGHT'").fetchone()
    assert row[0] == "4400A"
    assert row[1] == 23 * 3600 + 55 * 60


# ── Calendars ───────────────────────────────────────────────

def test_a_service_with_no_calendar_row_does_not_run_every_day():
    """GTFS allows a service defined only by `calendar_dates`. An unlisted
    date is not permission to run — it is the absence of permission."""
    from api.timetable_db import Timetable
    tt = Timetable.__new__(Timetable)
    tt.calendar = {}
    tt.calendar_dates = {"SVC_X": {"20260907": "1"}}

    assert tt.runs_on("SVC_X", date(2026, 9, 7)) is True, "the added date"
    assert tt.runs_on("SVC_X", date(2026, 9, 8)) is False, \
        "a date the service was never added to"
    assert tt.runs_on("SVC_UNKNOWN", date(2026, 9, 8)) is False, \
        "a service with no calendar and no exceptions at all"


# ── Departure boards span the service day, not the clock day ─

class _FakeTimetable:
    """Just enough Timetable for _departures_for_stop.

    One stop, one trip: the 23:55 from Stop A that reaches Stop B at 24:05 —
    five past midnight, still part of the previous day's service.
    """
    def __init__(self, calendar, calendar_dates):
        self.stops = {"4400B": {"name": "Stop B"}}
        self.trips = {"T_NIGHT": {"route_id": "R1", "service_id": "SVC1"}}
        self.routes = {"R1": {"short_name": "700", "noc": "TSTO"}}
        self.calendar = calendar
        self.calendar_dates = calendar_dates

    def stop_times_for(self, stop_id):
        if stop_id != "4400B":
            return []
        return [(24 * 3600 + 5 * 60, "T_NIGHT")]

    def runs_on(self, service_id, day):
        from api.timetable_db import service_runs_on
        return service_runs_on(self.calendar, self.calendar_dates,
                               service_id, day)


EVERY_DAY = {"SVC1": {
    "monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1",
    "friday": "1", "saturday": "1", "sunday": "1",
    "start_date": "20260101", "end_date": "20271231",
}}


def _board_at(monkeypatch, when):
    """The departure board for Stop B, with the clock frozen at `when`."""
    from datetime import datetime
    import api.main as main

    class FrozenDatetime(main.datetime):
        @classmethod
        def now(cls, tz=None):
            return when if tz is None else when.astimezone(tz)

    monkeypatch.setattr(main, "datetime", FrozenDatetime)
    return main._departures_for_stop(_FakeTimetable(EVERY_DAY, {}), "4400B")


def test_a_bus_after_midnight_shows_on_the_board_just_before_midnight(monkeypatch):
    # 23:50. The 00:05 departure is fifteen minutes away and inside the two
    # hour lookahead. Storing it as 00:05 put it fourteen hours in the past.
    from datetime import datetime
    from api.main import UK_TZ
    board = _board_at(monkeypatch, datetime(2026, 9, 8, 23, 50, tzinfo=UK_TZ))
    assert [d["service"] for d in board["departures"]] == ["700"], \
        "the bus fifteen minutes away was not on the board"
    assert board["departures"][0]["aimed_departure"].startswith("2026-09-09T00:05")


def test_a_bus_after_midnight_still_shows_just_after_midnight(monkeypatch):
    # 00:01 on the 9th. This departure belongs to the 8th's service day, so a
    # board that only ever looks at today's calendar cannot find it.
    from datetime import datetime
    from api.main import UK_TZ
    board = _board_at(monkeypatch, datetime(2026, 9, 9, 0, 1, tzinfo=UK_TZ))
    assert [d["service"] for d in board["departures"]] == ["700"], \
        "the previous service day was not consulted after midnight"


def test_an_exception_only_service_does_not_run_on_an_unlisted_date(monkeypatch):
    """The other half of the same bug: no base calendar meant "runs always"."""
    from datetime import datetime
    import api.main as main
    from api.main import UK_TZ

    when = datetime(2026, 9, 9, 12, 0, tzinfo=UK_TZ)

    class FrozenDatetime(main.datetime):
        @classmethod
        def now(cls, tz=None):
            return when if tz is None else when.astimezone(tz)

    monkeypatch.setattr(main, "datetime", FrozenDatetime)

    class Midday(_FakeTimetable):
        def stop_times_for(self, stop_id):
            return [(12 * 3600 + 30 * 60, "T_NIGHT")]

    # Added for 7 September only. Nothing says it runs on the 9th.
    tt = Midday({}, {"SVC1": {"20260907": "1"}})
    assert main._departures_for_stop(tt, "4400B")["departures"] == [], \
        "a service with no calendar row was reported as running today"

    # And it does run on the date it was actually added to.
    added = datetime(2026, 9, 7, 12, 0, tzinfo=UK_TZ)

    class FrozenAdded(main.datetime):
        @classmethod
        def now(cls, tz=None):
            return added if tz is None else added.astimezone(tz)

    monkeypatch.setattr(main, "datetime", FrozenAdded)
    assert len(main._departures_for_stop(tt, "4400B")["departures"]) == 1


# ── Operator identity ───────────────────────────────────────

def _timetable_over(db_path):
    """A Timetable bound to a local file, with no freshness fetch.

    The constructor would otherwise consider downloading a release asset,
    which a test has no business doing.
    """
    from api.timetable_db import Timetable
    tt = Timetable.__new__(Timetable)
    tt.db_path = Path(db_path)
    tt._con = sqlite3.connect(db_path)
    # Queries go through the per-thread connection; these are what __init__
    # would have set up.
    tt._local = threading.local()
    tt._generation = 0
    tt._noc_by_rid = None
    tt._noc_map = None
    tt.routes = {
        route_id: {"short_name": short, "_rid": rid}
        for rid, route_id, short in tt._con.execute(
            "SELECT rid, route_id, short_name FROM routes")
    }
    tt.trips = {
        trip_id: {"route_id": route_id}
        for trip_id, route_id in tt._con.execute(
            "SELECT t.trip_id, r.route_id FROM trips t JOIN routes r ON r.rid = t.rid")
    }
    return tt


def test_two_operators_sharing_a_route_number_keep_their_own_identity(db_path):
    """A route number is not a company.

    `noc_for_short_name` collapses the routes table into one
    {short_name: noc} map, so of two companies running a "5" only one
    survives — and the direct-journey response used that map to decide which
    operator's tickets are valid for the passenger's trip.
    """
    tt = _timetable_over(db_path)
    a = tt.trips["T_FIVE_A"]["route_id"]
    b = tt.trips["T_FIVE_B"]["route_id"]
    assert a != b, "fixture: the two trips must be on different routes"

    assert tt.noc_for_route(a) == "TSTO"
    assert tt.noc_for_route(b) == "OTHR"

    # The collapsed lookup cannot tell them apart: one answer for both.
    collapsed = tt.noc_for_short_name("5")
    assert collapsed in ("TSTO", "OTHR")
    assert tt.noc_for_route(a) != tt.noc_for_route(b), \
        "the two companies were given the same operator code"


def test_an_unknown_route_has_no_operator_rather_than_a_wrong_one(db_path):
    tt = _timetable_over(db_path)
    assert tt.noc_for_route("NO_SUCH_ROUTE") == ""


# ── A delayed bus is still a bus that has not left ──────────

def test_a_delayed_departure_survives_its_scheduled_time():
    """Scheduled 12:00, running ten minutes late, and it is 12:05.

    The passenger is standing at the stop and the bus has not arrived. The
    board dropped it, because the scheduled time was filtered against the
    clock before the prediction was applied — so the row vanished at exactly
    the moment it mattered most.
    """
    import api.main as main
    base = {
        "stop_name": "Marine Parade",
        "departures": [{
            "service": "700", "destination": "Brighton",
            "aimed_departure":    "2026-09-08T12:00:00+01:00",
            "expected_departure": "2026-09-08T12:10:00+01:00",
            "status": "Late", "delay_seconds": 600,
        }],
    }
    at_1205 = datetime(2026, 9, 8, 12, 5, tzinfo=main.UK_TZ)
    kept = main._drop_departed(base, now=at_1205)
    assert len(kept["departures"]) == 1, "a bus still due in five minutes was removed"


def test_a_bus_that_has_actually_gone_is_removed():
    import api.main as main
    base = {
        "stop_name": "Marine Parade",
        "departures": [
            {"service": "700", "aimed_departure": "2026-09-08T12:00:00+01:00",
             "expected_departure": None},
            {"service": "9", "aimed_departure": "2026-09-08T12:30:00+01:00",
             "expected_departure": None},
        ],
    }
    at_1205 = datetime(2026, 9, 8, 12, 5, tzinfo=main.UK_TZ)
    kept = main._drop_departed(base, now=at_1205)
    assert [d["service"] for d in kept["departures"]] == ["9"]


def test_an_early_running_bus_is_judged_on_its_expected_time_too():
    import api.main as main
    base = {"departures": [{
        "service": "700",
        "aimed_departure":    "2026-09-08T12:10:00+01:00",
        "expected_departure": "2026-09-08T12:02:00+01:00",   # ran early, gone
    }]}
    at_1205 = datetime(2026, 9, 8, 12, 5, tzinfo=main.UK_TZ)
    assert main._drop_departed(base, now=at_1205)["departures"] == []


# ── A schedule is worth serving on its own ──────────────────

def test_the_departure_board_does_not_require_a_live_vehicle_key():
    """`/api/departures` reads SQLite and, optionally, a predictions API.

    It never touches BODS — that key is for the live vehicle map — yet the
    route refused with 503 when it was absent. A local server with a perfectly
    good timetable returned nothing at all.
    """
    from fastapi.testclient import TestClient
    import api.main as main

    called = {}

    def fake_board(tt, stop_id):
        called["stop"] = stop_id
        return {"stop_name": "Test Stop", "departures": []}

    orig_key, orig_board = main.BODS_API_KEY, main._departures_for_stop
    main.BODS_API_KEY = ""
    main._departures_for_stop = fake_board
    try:
        with TestClient(main.app) as client:
            res = client.get("/api/departures", params={"stopId": "4400ZZ"})
        assert res.status_code != 503, \
            "the schedule was withheld because a live-vehicle key was missing"
    finally:
        main.BODS_API_KEY = orig_key
        main._departures_for_stop = orig_board


# ── The gate that would have caught all of this ─────────────

def test_the_release_check_passes_a_correctly_built_database(db_path):
    import check_timetable
    assert check_timetable.main(Path(db_path), allow_small=True) == 0


def test_the_release_check_rejects_a_database_with_folded_overnight_times(db_path, tmp_path):
    """The check that would have stopped the defect reaching a release.

    Reproduces what the old pipeline produced: every time modulo 86400, and
    stop order re-derived by sorting on it.
    """
    import shutil
    import check_timetable

    broken = tmp_path / "folded.sqlite"
    shutil.copy(db_path, broken)
    con = sqlite3.connect(broken)
    con.execute("UPDATE stop_times SET dep_secs = dep_secs % 86400")
    con.commit()
    con.close()

    assert check_timetable.main(broken, allow_small=True) == 1


def test_the_release_check_rejects_a_service_nothing_describes(db_path, tmp_path):
    import shutil
    import check_timetable

    broken = tmp_path / "uncalendared.sqlite"
    shutil.copy(db_path, broken)
    con = sqlite3.connect(broken)
    con.execute("DELETE FROM calendar")
    con.commit()
    con.close()

    assert check_timetable.main(broken, allow_small=True) == 1
