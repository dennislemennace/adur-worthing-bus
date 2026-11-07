"""Rail replacement buses matched to the journeys they run (api/rail_replacement.py).

On 7 November 2026 Coastway West was closed for the evening and Metrobus 6989
ran Southern's replacement buses, reporting itself only as route "Rail". These
check the match on that evening's shape: buses both ways every half hour
between Worthing and Brighton, scheduled by Realtime Trains, none tracked.
(tests/test_rail_replacement.py covers the 0B headcode that marks them.)

Run with:  pytest
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api import rail_replacement as rr  # noqa: E402

UK = ZoneInfo("Europe/London")
STATIONS = {s["crs"]: s for s in json.loads((ROOT / "data" / "rail_stations.json").read_text())["stations"]}
EAST = [("WRH", 0), ("EWR", 5), ("LAC", 15), ("SSE", 25), ("SWK", 30), ("FSG", 33), ("PLD", 38),
        ("AGT", 42), ("HOV", 45), ("BTN", 57)]


def journey(uid, start, calls, origin, destination):
    """A replacement journey as /api/rail-service gives it."""
    return {"uid": uid, "headcode": "0B00", "departureDate": start.date().isoformat(), "atocName": "Southern",
            "origin": [{"description": origin}], "destination": [{"description": destination}],
            "locations": [{"crs": crs, "description": STATIONS[crs]["name"],
                           "departure": {"scheduled": (start + timedelta(minutes=m)).strftime("%Y-%m-%dT%H:%M:%S")}}
                          for crs, m in calls]}


START = datetime(2026, 11, 7, 18, 38)
EASTBOUND = journey("X19791", START, EAST, "Worthing", "Brighton")
# The other way, due past the same place a few minutes later.
WESTBOUND = journey("X19969", datetime(2026, 11, 7, 18, 45),
                    [(crs, 57 - m) for crs, m in reversed(EAST)], "Brighton", "Worthing")
SERVICES = [EASTBOUND, WESTBOUND]

# Between Southwick and Fishersgate, as 6989 was at 19:04:43.
HERE = {"latitude": 50.832384, "longitude": -0.2357, "service_ref": "Rail"}
NOW = datetime(2026, 11, 7, 19, 4, 43, tzinfo=UK)
WEST_OF_HERE = (50.8320, -0.2480)
EAST_OF_HERE = (50.8330, -0.2250)


def test_a_bus_reporting_its_route_as_rail_is_a_replacement_bus():
    assert rr.is_rail_replacement({"service_ref": "Rail"})
    assert rr.is_rail_replacement({"service_ref": " RAIL "})
    assert not rr.is_rail_replacement({"service_ref": "700"})
    assert not rr.is_rail_replacement({})


def test_its_way_along_the_line_picks_the_journey_going_that_way():
    east = rr.match(HERE, SERVICES, STATIONS, NOW, previous=WEST_OF_HERE)
    assert east and east["uid"] == "X19791" and east["destination"] == "Brighton"
    west = rr.match(HERE, SERVICES, STATIONS, NOW, previous=EAST_OF_HERE)
    assert west and west["uid"] == "X19969" and west["destination"] == "Worthing"


def test_the_match_reads_as_upcoming_stops():
    m = rr.match(HERE, SERVICES, STATIONS, NOW, previous=WEST_OF_HERE)
    rows = m["upcoming_stops"]
    names = [r["stop_name"] for r in rows]
    assert names[0].startswith("Worthing") and names[-1].startswith("Brighton")
    nxt = [r for r in rows if r["is_next"]]
    assert len(nxt) == 1 and nxt[0]["crs"] == "FSG", "the next station is the one after where it is"
    assert all(r["passed"] for r in rows[:nxt[0]["seq"]]) and not any(r["passed"] for r in rows[nxt[0]["seq"]:])
    assert rows[-1]["is_terminus"]
    # Times ahead are never earlier than the timetable: a replacement bus
    # running early waits for its time.
    for r in rows:
        if not r["passed"]:
            assert r["expected"] >= r["scheduled"], r
    assert m["off_line_m"] < 500


def test_late_is_measured_where_the_bus_is_not_at_a_station():
    # 19:04:43, part of the way from Southwick (19:08) to Fishersgate
    # (19:11) on the timetable: a few minutes early.
    m = rr.match(HERE, [EASTBOUND], STATIONS, NOW, previous=WEST_OF_HERE)
    assert -6 * 60 < m["lateness_secs"] < -2 * 60, m["lateness_secs"]
    later = rr.match(HERE, [EASTBOUND], STATIONS, NOW + timedelta(minutes=10), previous=WEST_OF_HERE)
    assert 4 * 60 < later["lateness_secs"] < 8 * 60, later["lateness_secs"]


def test_no_match_rather_than_a_wrong_one():
    far = {**HERE, "latitude": 50.90, "longitude": -0.30}                         # on the Downs
    assert rr.match(far, SERVICES, STATIONS, NOW) is None
    assert rr.match(HERE, SERVICES, STATIONS, NOW + timedelta(hours=2)) is None   # nothing due near then
    assert rr.match({"service_ref": "Rail"}, SERVICES, STATIONS, NOW) is None     # no position


def test_candidates_are_the_buses_due_near_now_soonest_first():
    board = {"services": [
        {"uid": "B1", "mode": "bus", "departureDate": "2026-11-07", "departure": {"scheduled": "2026-11-07T19:10:00"}},
        {"uid": "B2", "mode": "bus", "departureDate": "2026-11-07", "arrival": {"scheduled": "2026-11-07T19:30:00"}},
        {"uid": "T1", "mode": "train", "departureDate": "2026-11-07", "departure": {"scheduled": "2026-11-07T19:05:00"}},
        {"uid": "B3", "mode": "bus", "departureDate": "2026-11-07", "departure": {"scheduled": "2026-11-07T23:00:00"}},
    ]}
    got = rr.candidates([board, board], NOW)
    assert got == [("B1", "2026-11-07"), ("B2", "2026-11-07")], got
