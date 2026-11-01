"""Route view's route picker: one operator's route, in full.

Asked for 1 November 2026: choose an operator, then a service number, and see
only that route, with its stops and timing points and when it runs. A number
alone is not a route: this area has a Brighton & Hove 1, a Stagecoach 1 and a
Metrobus 1, so the list of routes is of operator and number together, and a
route's detail is asked for by both.

Run with:  pytest
"""

import asyncio
import csv
import io
import sys
import zipfile
from datetime import date
from pathlib import Path

import pytest
from fastapi import HTTPException

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import build_timetable as bt        # noqa: E402
import json_to_sqlite as j2s        # noqa: E402
import api.main as main             # noqa: E402
from api.timetable_db import Timetable   # noqa: E402

# Compass's 19 out along one side of the road and back along the other.
OUT = [("4400RD001", "Holmbush Centre", 50.8300, -0.3000, 1),
       ("4400RD002", "Southlands Hospital", 50.8310, -0.2900, 0),
       ("4400RD003", "High Street", 50.8320, -0.2800, 1),
       ("4400RD004", "The Longshore", 50.8330, -0.2700, 1)]
BACK = [("4400RD014", "The Longshore", 50.8331, -0.2701, 1),
        ("4400RD013", "High Street", 50.8321, -0.2801, 1),
        ("4400RD011", "Holmbush Centre", 50.8301, -0.3001, 1)]
# Two companies' 1s, one stop each way apart.
ONES = [("4400RD101", "Stagecoach Stop", 50.8400, -0.3000), ("4400RD102", "Stagecoach End", 50.8410, -0.2900),
        ("4400RD201", "B&H Stop", 50.8250, -0.2000), ("4400RD202", "B&H End", 50.8260, -0.1900)]
TUESDAY = date(2026, 11, 3)


def _hms(mins):
    return f"{mins // 60:02d}:{mins % 60:02d}:00"


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
    trips, times = [], []
    for i in range(37):                      # 07:00 to 19:00, every 20 minutes
        start = 7 * 60 + i * 20
        trips.append((f"OUT{i}", "R19", "WK", "The Longshore", ""))
        times += [(f"OUT{i}", n + 1, s[0], _hms(start + 4 * n), s[4]) for n, s in enumerate(OUT)]
        trips.append((f"BACK{i}", "R19", "WK", "Holmbush Centre", ""))
        times += [(f"BACK{i}", n + 1, s[0], _hms(start + 5 + 4 * n), s[4]) for n, s in enumerate(BACK)]
    # One more after midnight, filed under the day before as GTFS does (24:30).
    trips.append(("LATE", "R19", "WK", "The Longshore", ""))
    times += [("LATE", n + 1, s[0], _hms(24 * 60 + 30 + 4 * n), s[4]) for n, s in enumerate(OUT)]
    for rid, a, b in (("RS1", "4400RD101", "4400RD102"), ("RB1", "4400RD201", "4400RD202")):
        trips.append((f"{rid}T", rid, "WK", "End", ""))
        times += [(f"{rid}T", 1, a, "09:00:00", 1), (f"{rid}T", 2, b, "09:10:00", 1)]
    feed = tmp_path_factory.mktemp("gtfs") / "feed.zip"
    with zipfile.ZipFile(feed, "w") as z:
        z.writestr("agency.txt", _csv([("CT", "Compass Travel", "COMT"), ("SC", "Stagecoach", "SCSO"),
                                       ("BH", "Brighton & Hove", "BHBC")],
                                      ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv([s[:4] for s in OUT + BACK] + ONES,
                                     ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv([("R19", "CT", "19", "19", 3), ("RS1", "SC", "1", "1", 3),
                                       ("RB1", "BH", "1", "1", 3)],
                                      ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv(trips, ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", _csv([(t, seq, sid, hms, hms, tp) for t, seq, sid, hms, tp in times],
                                          ["trip_id", "stop_sequence", "stop_id", "arrival_time",
                                           "departure_time", "timepoint"]))
        z.writestr("calendar.txt", _csv([("WK", 1, 1, 1, 1, 1, 1, 0, "20260101", "20271231")],
                                        ["service_id", "monday", "tuesday", "wednesday", "thursday",
                                         "friday", "saturday", "sunday", "start_date", "end_date"]))
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(bt.parse_gtfs(str(feed)), out)
    yield Timetable(out, allow_fetch=False)
    mp.undo()


BBOX = (50.80, 50.90, -0.40, -0.10)


def test_the_route_list_is_of_operator_and_number_together(tt):
    got = {(r["operator"], r["service"]) for r in tt.route_catalogue(BBOX)}
    assert {("SCSO", "1"), ("BHBC", "1"), ("COMT", "19")} <= got, got


def test_a_route_has_its_stops_in_order_each_way_with_the_timing_points(tt):
    r = tt.route_detail("19", "COMT", TUESDAY)
    out, back = r["directions"]
    assert out["to"] == "The Longshore" and back["to"] == "Holmbush Centre"
    assert [s["name"] for s in out["stops"]] == [s[1] for s in OUT]
    assert [s["timing"] for s in out["stops"]] == [True, False, True, True]
    assert [s["atco"] for s in back["stops"]] == [s[0] for s in BACK]
    assert len(out["line"]) >= 2 and all(len(p) == 2 for p in out["line"])


def test_a_route_says_when_it_runs(tt):
    r = tt.route_detail("19", "COMT", TUESDAY)
    assert r["days"] == ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    # The last of the day is the last before midnight. The 24:30 is counted
    # apart: read as a clock time, it made the 700's "last" 04:43.
    assert (r["first"], r["last"]) == ("07:00", "19:05")
    assert r["after_midnight"] == 1
    assert r["every_min"] == 20
    assert r["journeys"] == 75
    assert tt.route_detail("19", "COMT", date(2026, 11, 8))["journeys"] == 0, "not on Sundays"


def test_a_number_is_not_a_route_without_its_operator(tt):
    assert tt.route_detail("19", "SCSO", TUESDAY) is None
    assert tt.route_detail("1", "SCSO", TUESDAY)["directions"][0]["stops"][0]["name"] == "Stagecoach Stop"
    assert tt.route_detail("1", "BHBC", TUESDAY)["directions"][0]["stops"][0]["name"] == "B&H Stop"


def test_the_route_carries_its_own_disruptions_and_no_others(tt, monkeypatch):
    async def timetable():
        return tt

    async def no_state(wait=False):
        return {}

    def notice(op, line):
        return {"id": f"{op}-{line}", "summary": "Road closed", "lines": [{"operator": op, "line": line}],
                "operators": [], "stops": [], "journeys": []}

    monkeypatch.setattr(main, "_get_timetable", timetable)
    monkeypatch.setattr(main, "_disruptions_state", no_state)
    monkeypatch.setattr(main, "_disruptions_now", lambda _state: [notice("COMT", "19"), notice("SCSO", "700")])
    main._cache.clear()
    got = asyncio.run(main.get_route(operator="COMT", service="19"))
    assert [d["id"] for d in got["disruptions"]] == ["COMT-19"]


def test_the_api_serves_the_list_and_a_route(tt, monkeypatch):
    async def timetable():
        return tt
    monkeypatch.setattr(main, "_get_timetable", timetable)
    monkeypatch.setattr(main, "BBOX_MIN_LAT", BBOX[0])
    monkeypatch.setattr(main, "BBOX_MAX_LAT", BBOX[1])
    monkeypatch.setattr(main, "BBOX_MIN_LON", BBOX[2])
    monkeypatch.setattr(main, "BBOX_MAX_LON", BBOX[3])
    main._cache.clear()
    listed = asyncio.run(main.get_routes())
    assert {"operator": "COMT", "service": "19"} in [{k: r[k] for k in ("operator", "service")} for r in listed["routes"]]
    got = asyncio.run(main.get_route(operator="COMT", service="19"))
    assert got["service"] == "19" and len(got["directions"]) == 2
    with pytest.raises(HTTPException) as e:
        asyncio.run(main.get_route(operator="SCSO", service="19"))
    assert e.value.status_code == 404
