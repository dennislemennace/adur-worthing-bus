"""What a bus runs next: the operator's block, from GTFS through the build to
Timetable.next_in_block. Stagecoach and Compass publish blocks; a trip with no
block (Brighton & Hove) has no next journey rather than a guessed one."""

import csv
import io
import sys
import zipfile
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import build_timetable as bt              # noqa: E402
import json_to_sqlite as j2s              # noqa: E402
from api.timetable_db import Timetable    # noqa: E402

STOPS = [("4400WO01", "Worthing Pier", 50.8100, -0.3700),
         ("1490HUB1", "Old Steine", 50.8230, -0.1370)]
# (trip, service, block, [(stop, time)])
TRIPS = [
    ("OUT_1000", "WK",  "B1", [("4400WO01", "10:00:00"), ("1490HUB1", "10:40:00")]),
    ("BACK_1050", "WK", "B1", [("1490HUB1", "10:50:00"), ("4400WO01", "11:30:00")]),
    ("BACK_1200", "WK", "B1", [("1490HUB1", "12:00:00"), ("4400WO01", "12:40:00")]),
    ("SAT_1045", "SAT", "B1", [("1490HUB1", "10:45:00"), ("4400WO01", "11:25:00")]),
    ("OTHER_BUS", "WK", "B2", [("1490HUB1", "10:42:00"), ("4400WO01", "11:22:00")]),
    ("NO_BLOCK", "WK",  "",   [("4400WO01", "10:05:00"), ("1490HUB1", "10:45:00")]),
]


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
        z.writestr("agency.txt", _csv([("AG1", "Stagecoach South", "SCSO")],
                                      ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv([("R700", "AG1", "700", "Coastliner", 3)],
                                      ["route_id", "agency_id", "route_short_name",
                                       "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv(
            [(t, "R700", s, "Brighton" if t.startswith("OUT") else "Worthing", "", b)
             for t, s, b, _ in TRIPS],
            ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id", "block_id"]))
        z.writestr("stop_times.txt", _csv(
            [(t, i + 1, sid, hms, hms) for t, _, _, calls in TRIPS for i, (sid, hms) in enumerate(calls)],
            ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"]))
        z.writestr("calendar.txt", _csv(
            [("WK", 1, 1, 1, 1, 1, 0, 0, "20260101", "20271231"),
             ("SAT", 0, 0, 0, 0, 0, 1, 0, "20260101", "20271231")],
            ["service_id", "monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday", "start_date", "end_date"]))
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(bt.parse_gtfs(str(feed)), out)
    yield Timetable(out, allow_fetch=False)
    mp.undo()


TUESDAY = date(2026, 9, 29)


def test_the_bus_runs_its_block_next_journey(tt):
    nxt = tt.next_in_block("OUT_1000", TUESDAY)
    assert nxt["trip_id"] == "BACK_1050", "another bus's journey, or a later one, was named"
    assert (nxt["service"], nxt["headsign"], nxt["from_name"]) == ("700", "Worthing", "Old Steine")
    assert nxt["depart_secs"] == 10 * 3600 + 50 * 60


def test_only_journeys_running_that_day_count(tt):
    # The Saturday 10:45 leaves sooner than the 10:50, so on a Tuesday it is
    # only the day that rules it out; on a Saturday it is the answer.
    assert tt.next_in_block("OUT_1000", TUESDAY)["trip_id"] == "BACK_1050"
    assert tt.next_in_block("OUT_1000", date(2026, 10, 3))["trip_id"] == "SAT_1045"


def test_a_trip_without_a_block_has_no_next_journey(tt):
    assert tt.next_in_block("NO_BLOCK", TUESDAY) is None
    assert tt.next_in_block("BACK_1200", TUESDAY) is None, "the last journey of the day"
