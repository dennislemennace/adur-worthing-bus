"""A bus whose ticket machine goes on naming a journey it finished long ago.

On 1 November 2026 Brighton & Hove's bus 212 named the 17:42 route 20 at
19:20, while at Open Market, where the 19:03 was due at 19:22. The board
showed it 77 minutes late on a journey long over, and its real journey again
below, as a timetable row: one bus, twice. The 19:03 had no bus claiming it.

The live views now move such a bus onto the journey it is driving, when all
of these hold: its claim has it 40 minutes or more behind; another journey
of the same route is due where it is, between 5 minutes early and 25 late;
and no bus claims that one. The nightly figures keep the claim as the
operator made it (asked for 1 November 2026).

Run with:  pytest
"""

import csv
import io
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import build_timetable as bt        # noqa: E402
import json_to_sqlite as j2s        # noqa: E402
import api.main as main             # noqa: E402
from api.timetable_db import Timetable   # noqa: E402

STOPS = [("4400SD001", "Bottom of Davey Drive", 50.8500, -0.1200),
         ("4400SD002", "Lewes Road", 50.8400, -0.1300),
         ("4400SD003", "Open Market", 50.8319, -0.1342),
         ("4400SD004", "London Road Shops", 50.8303, -0.1359),
         ("4400SD005", "Stonehurst Court", 50.8200, -0.1400)]
STEP = [0, 10, 19, 21, 36]          # minutes from the start to each stop, as the real 19:03


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
    journeys = [("J1742", "R20", 17 * 60 + 42), ("J1903", "R20", 19 * 60 + 3), ("J1823", "R20", 18 * 60 + 23),
                ("OTHER", "R21", 19 * 60 + 3)]
    trips = [(t, r, "ALL", "Stonehurst Court", "") for t, r, _ in journeys]
    times = [(t, n + 1, STOPS[n][0], _hms(start + STEP[n]), _hms(start + STEP[n]))
             for t, _r, start in journeys for n in range(len(STOPS))]
    feed = tmp_path_factory.mktemp("gtfs") / "feed.zip"
    with zipfile.ZipFile(feed, "w") as z:
        z.writestr("agency.txt", _csv([("BH", "Brighton & Hove", "BHBC")], ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv([("R20", "BH", "20", "20", 3), ("R21", "BH", "21", "21", 3)],
                                      ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv(trips, ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", _csv(times, ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"]))
        z.writestr("calendar.txt", _csv([("ALL", 1, 1, 1, 1, 1, 1, 1, "20260101", "20271231")],
                                        ["service_id", "monday", "tuesday", "wednesday", "thursday",
                                         "friday", "saturday", "sunday", "start_date", "end_date"]))
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(bt.parse_gtfs(str(feed)), out)
    yield Timetable(out, allow_fetch=False)
    mp.undo()


AT = datetime(2026, 11, 1, 19, 20, tzinfo=main.UK_TZ)


def bus(trip, ref="212", at=AT, stop=2):
    lat, lon = STOPS[stop][2], STOPS[stop][3]
    return {"vehicle_ref": ref, "operator_ref": "BHBC", "service_ref": "20", "declared_trip_id": trip,
            "latitude": lat, "longitude": lon, "bearing": None,
            "recorded_at": at.astimezone(main.timezone.utc).isoformat()}


def test_a_bus_that_outlived_its_journey_is_moved_onto_the_one_it_is_driving(tt):
    v = bus("J1742")
    main._attach_declared_journeys([v], tt, now_local=AT, move_outlived=True)
    assert v["trip_id"] == "J1903", "still 77 minutes late on the 17:42"
    assert v["trip_source"] == "feed_moved"
    assert abs(v["lateness_secs"]) <= 3 * 60
    assert "declared_journey_outlived" in v["identity_flags"]
    assert v["outlived_trip_id"] == "J1742"


def test_the_nightly_figures_keep_the_claim_as_the_operator_made_it(tt):
    v = bus("J1742")
    main._attach_declared_journeys([v], tt, now_local=AT)
    assert v["trip_id"] == "J1742" and v["trip_source"] == "feed"
    assert v["lateness_secs"] > 70 * 60


def test_a_journey_another_bus_claims_is_not_taken(tt):
    v, other = bus("J1742"), bus("J1903", ref="215")
    main._attach_declared_journeys([v, other], tt, now_local=AT, move_outlived=True)
    assert v["trip_id"] == "J1742", "two buses were put on the 19:03"
    assert other["trip_id"] == "J1903"


def test_a_bus_half_an_hour_late_keeps_its_journey(tt):
    # The 18:23 is due at Open Market at 18:42; at 19:16 the bus is 34
    # minutes late on it. Buses do run that late; the 19:03 is not yet due.
    at = datetime(2026, 11, 1, 19, 16, tzinfo=main.UK_TZ)
    v = bus("J1823", at=at)
    main._attach_declared_journeys([v], tt, now_local=at, move_outlived=True)
    assert v["trip_id"] == "J1823"


def test_another_route_due_here_is_not_the_bus_s_journey(tt):
    # Only route 21's journey is free at 19:22 when the 19:03 route 20 is
    # claimed; the bus stays on its own route's claim.
    v, other = bus("J1742"), bus("J1903", ref="215")
    main._attach_declared_journeys([v, other], tt, now_local=AT, move_outlived=True)
    assert v["trip_id"] != "OTHER"
