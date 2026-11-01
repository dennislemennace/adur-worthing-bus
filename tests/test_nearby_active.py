"""An empty board points to the nearest stops that have a bus coming.

"No services from this stop in the next 2 hours" leaves a reader at the kerb
with nowhere to go. So a board with nothing on it carries the three nearest
stops whose own board would show a bus (asked for 1 November 2026): each is
asked exactly as its board is, so a button never opens another empty board.

Run with:  pytest
"""

import csv
import io
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import build_timetable as bt        # noqa: E402
import json_to_sqlite as j2s        # noqa: E402
import api.main as main             # noqa: E402
from api.timetable_db import Timetable   # noqa: E402

LAT, LON = 50.8300, -0.2700
NS = 0.0009      # about 100 m of latitude
EW = 0.00142     # about 100 m of longitude here

STOPS = [
    ("4400NB000", "Empty Stop",   LAT, LON),             # the reader's stop
    ("4400NB100", "East Road",    LAT, LON + EW),        # 100 m east, a bus every 10 minutes
    ("4400NB200", "West Road",    LAT, LON - 2 * EW),    # 200 m west, first bus at 06:00 only
    ("4400NB300", "North Street", LAT + 3 * NS, LON),    # 300 m north
    ("4400NB400", "South Street", LAT - 4 * NS, LON),    # 400 m south
    ("4400NB500", "Fifth Avenue", LAT + 5 * NS, LON),    # 500 m north: the fourth with buses
    ("4400NB900", "Far Away",     LAT + 30 * NS, LON),   # 3 km: not a walk
]


def _hms(mins):
    return f"{mins // 60:02d}:{mins % 60:02d}:00"


def _every(prefix, route, first, gap_mins, n, stop):
    """`n` trips `gap_mins` apart from `stop`, each on to Far Away."""
    return [(f"{prefix}{i}", route, [(1, stop, _hms(first + i * gap_mins)),
                                      (2, "4400NB900", _hms(first + i * gap_mins + 20))])
            for i in range(n)]


TRIPS = [
    ("MORNING77", "R77", [(1, "4400NB000", "09:16:00"), (2, "4400NB900", "09:40:00")]),
    ("EARLY5", "R5", [(1, "4400NB200", "06:00:00"), (2, "4400NB900", "06:20:00")]),
    *_every("ONE", "R1", 7 * 60, 10, 80, "4400NB100"),
    *_every("TWO", "R2", 7 * 60, 15, 50, "4400NB300"),
    *_every("THREE", "R3", 7 * 60, 20, 40, "4400NB400"),
    *_every("FOUR", "R4", 7 * 60, 20, 40, "4400NB500"),
]
ROUTES = [("R77", "77"), ("R5", "5"), ("R1", "1"), ("R2", "2"), ("R3", "3"), ("R4", "4")]
TUESDAY_AFTERNOON = (2026, 10, 20, 14, 0)


def _csv(rows, header):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue()


@pytest.fixture(scope="module")
def tt(tmp_path_factory):
    mp = pytest.MonkeyPatch()
    mp.setenv("SKIP_OSRM", "1")
    feed = tmp_path_factory.mktemp("gtfs") / "feed.zip"
    with zipfile.ZipFile(feed, "w") as z:
        z.writestr("agency.txt", _csv([("SC", "Stagecoach", "SCSO")], ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv([(r, "SC", n, n, 3) for r, n in ROUTES],
                                      ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv([(t, r, "WK", "Far Away", "") for t, r, _ in TRIPS],
                                     ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", _csv(
            [(t, seq, sid, hms, hms) for t, _, calls in TRIPS for seq, sid, hms in calls],
            ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"]))
        z.writestr("calendar.txt", _csv([("WK", 1, 1, 1, 1, 1, 0, 0, "20260101", "20271231")],
                                        ["service_id", "monday", "tuesday", "wednesday", "thursday",
                                         "friday", "saturday", "sunday", "start_date", "end_date"]))
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(bt.parse_gtfs(str(feed)), out)
    yield Timetable(out, allow_fetch=False)
    mp.undo()


@pytest.fixture
def tuesday(monkeypatch):
    real = main.datetime

    class Clock(real):
        @classmethod
        def now(cls, tz=None):
            at = real(*TUESDAY_AFTERNOON, tzinfo=main.UK_TZ)
            return at.astimezone(tz) if tz else at.replace(tzinfo=None)

    monkeypatch.setattr(main, "datetime", Clock)
    main._cache.clear()
    yield Clock.now(main.UK_TZ)
    main._cache.clear()


def test_an_empty_board_is_offered_the_three_nearest_stops_with_a_bus_coming(tt, tuesday):
    got = main._nearby_active_stops(tt, "4400NB000")
    assert [s["atco"] for s in got] == ["4400NB100", "4400NB300", "4400NB400"], (
        "nearest first, skipping West Road, whose only bus is at 06:00, and stopping at three")
    east = got[0]
    assert east["name"] == "East Road"
    assert east["metres"] == 100 and east["direction"] == "east"
    assert east["routes"] == ["1"]
    assert east["next"].startswith("2026-10-20T14:00"), "the next bus there, from its own board"
    # Where it goes: two poles of one name, either side of the road, read
    # apart by it, and most of these stops are not in the site's stop list.
    assert east["destination"] == "Far Away"


def test_a_stop_beyond_a_walk_is_not_offered(tt, tuesday):
    # From West Road, Far Away is 2.7 km: the stops with buses that are a walk
    # are offered, and that one is not.
    got = main._nearby_active_stops(tt, "4400NB200")
    assert got and "4400NB900" not in [s["atco"] for s in got]
    assert all(s["metres"] <= main.NEARBY_ACTIVE_RADIUS for s in got)


def test_a_stop_whose_board_says_not_calling_is_not_offered(tt, tuesday, monkeypatch):
    real = main._apply_stop_closure

    def east_closed(payload, tt_, stop_id):
        if stop_id != "4400NB100":
            return real(payload, tt_, stop_id)
        return {**payload, "departures": [{**d, "status": "Not calling", "not_served": True}
                                          for d in payload["departures"]]}

    monkeypatch.setattr(main, "_apply_stop_closure", east_closed)
    got = main._nearby_active_stops(tt, "4400NB000")
    assert [s["atco"] for s in got] == ["4400NB300", "4400NB400", "4400NB500"]


def test_only_an_empty_board_carries_them(tt, tuesday):
    empty = main._with_nearby_active({"departures": []}, tt, "4400NB000")
    assert [s["atco"] for s in empty["nearby_active"]][:1] == ["4400NB100"]
    busy = {"departures": [{"service": "1"}]}
    assert main._with_nearby_active(busy, tt, "4400NB100") is busy, "a board with buses needs no directions"
