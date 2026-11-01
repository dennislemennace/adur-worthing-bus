"""The line a selected bus's journey follows, start to end.

A bus opened on the map draws its whole journey as a faint line, so the reader
sees where it has come from and where it is going (asked for 1 November 2026).
The line is the journey's own shape from the timetable, the same road-following
points Route view draws, or, for a journey with none, its stops joined up.

Run with:  pytest
"""

import asyncio
import csv
import io
import sys
import zipfile
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

STOPS = [
    ("4400TS001", "First Stop",  50.8300, -0.2700),
    ("4400TS002", "Middle Stop", 50.8310, -0.2650),
    ("4400TS003", "Last Stop",   50.8320, -0.2600),
]
# The shaped journey bends north between its stops, as a road would.
SHAPE = [(50.8300, -0.2700), (50.8340, -0.2675), (50.8310, -0.2650), (50.8350, -0.2625), (50.8320, -0.2600)]


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
    calls = [(1, "4400TS001", "10:00:00"), (2, "4400TS002", "10:05:00"), (3, "4400TS003", "10:10:00")]
    # TWIN calls where SHAPED does, an hour later, with no shape of its own;
    # PLAIN runs straight from first to last.
    twin = [(seq, sid, "11" + hms[2:]) for seq, sid, hms in calls]
    plain = [calls[0], (2, "4400TS003", "10:08:00")]
    feed = tmp_path_factory.mktemp("gtfs") / "feed.zip"
    with zipfile.ZipFile(feed, "w") as z:
        z.writestr("agency.txt", _csv([("SC", "Stagecoach", "SCSO")], ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv([("R1", "SC", "1", "1", 3)],
                                      ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv([("SHAPED", "R1", "WK", "Last Stop", "S1"), ("TWIN", "R1", "WK", "Last Stop", ""),
                                      ("PLAIN", "R1", "WK", "Last Stop", "")],
                                     ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", _csv(
            [(t, seq, sid, hms, hms) for t, cs in (("SHAPED", calls), ("TWIN", twin), ("PLAIN", plain))
             for seq, sid, hms in cs],
            ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"]))
        z.writestr("shapes.txt", _csv([("S1", lat, lon, i + 1) for i, (lat, lon) in enumerate(SHAPE)],
                                      ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"]))
        z.writestr("calendar.txt", _csv([("WK", 1, 1, 1, 1, 1, 1, 1, "20260101", "20271231")],
                                        ["service_id", "monday", "tuesday", "wednesday", "thursday",
                                         "friday", "saturday", "sunday", "start_date", "end_date"]))
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(bt.parse_gtfs(str(feed)), out)
    yield Timetable(out, allow_fetch=False)
    mp.undo()


def test_a_journey_with_a_shape_follows_its_road(tt):
    pts = tt.shape_for_trip("SHAPED")
    assert [tuple(round(c, 4) for c in p) for p in pts] == SHAPE


def test_a_journey_without_one_borrows_the_shape_of_the_same_stops(tt):
    # One journey in seven carries a shape in the real timetable, and almost
    # none of those on the road at a given moment do; the same stops in the
    # same order are the same road.
    pts = tt.shape_for_trip("TWIN")
    assert [tuple(round(c, 4) for c in p) for p in pts] == SHAPE


def test_a_journey_with_no_shape_to_borrow_joins_its_stops(tt):
    pts = tt.shape_for_trip("PLAIN")
    assert [tuple(round(c, 4) for c in p) for p in pts] == [(STOPS[0][2], STOPS[0][3]), (STOPS[2][2], STOPS[2][3])]


def test_an_unknown_journey_has_no_line(tt):
    assert tt.shape_for_trip("NOPE") is None


def test_the_api_serves_the_line(tt, monkeypatch):
    async def timetable():
        return tt
    monkeypatch.setattr(main, "_get_timetable", timetable)
    main._cache.clear()
    got = asyncio.run(main.get_trip_shape(tripId="SHAPED"))
    assert got["trip_id"] == "SHAPED" and got["service"] == "1"
    assert len(got["points"]) == len(SHAPE)
    with pytest.raises(HTTPException) as e:
        asyncio.run(main.get_trip_shape(tripId="NOPE"))
    assert e.value.status_code == 404
