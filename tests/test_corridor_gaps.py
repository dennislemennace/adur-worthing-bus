"""Tests for the A259 westbound gap monitor.

The monitor tells someone at a stop on the coast road that the next bus is a
long way off, and on a campaign site it is the kind of statement that gets
screenshotted. Each way it can be wrong produces something that looks right:

  * counting the timetable's own evening gaps as a service failure,
  * counting a coach, or a bus on the other side of the road, as the bus
    somebody is waiting for, which hides a real gap,
  * trusting a heading of 0, which rules out every westbound bus and invents one,
  * reading a bus that has already gone by as one still to come,
  * losing the first buses of the morning, which feeds write as 28:40 on the
    previous day's service, and
  * reporting a gap when the real story is that the buses are not reporting.

The fixture is a straight coast road, heading a little south of west like the
real one, with a stop every minute and a bus every ten.

Run with:  pytest
"""

import csv
import io
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import build_timetable as bt             # noqa: E402
import json_to_sqlite as j2s             # noqa: E402
from api import corridor_gaps as cg      # noqa: E402
from api.timetable_db import Timetable   # noqa: E402

UK = ZoneInfo("Europe/London")
N_STOPS = 60
LAT0, LON0 = 50.840, -0.200
DLAT, DLON = -0.0008, -0.005         # ~360 m a stop, bearing ~256 degrees


def west(i):
    return f"4400TW{i:04d}"


def east(i):
    return f"4400TE{i:04d}"


def west_xy(i):
    return LAT0 + DLAT * i, LON0 + DLON * i


def east_xy(i):
    # The eastbound pole is across the road: close enough to be the nearest
    # stop to a westbound trip, which is the whole difficulty.
    lat, lon = west_xy(i)
    return lat + 0.0008, lon


WATCH = (
    (west(20), "Shoreham Port"),
    (west(30), "Shoreham High Street"),
    (west(50), "Beach Green Hotel"),
)

STOPS = ([(west(i), f"West {i}", *west_xy(i)) for i in range(N_STOPS)]
         + [(east(i), f"East {i}", *east_xy(i)) for i in range(N_STOPS)])


def hms(minutes):
    return f"{minutes // 60:02d}:{minutes % 60:02d}:00"


def westbound(trip_id, start_min, service="WK", route="R700"):
    return (trip_id, route, service, "Worthing",
            [(i + 1, west(i), hms(start_min + i)) for i in range(N_STOPS)])


TRIPS = []
for d in range(10 * 60, 15 * 60, 10):                  # daytime, every ten minutes
    TRIPS.append(westbound(f"W{d}", d))
    TRIPS.append((f"E{d}", "R700", "WK", "Brighton",
                  [(i + 1, east(N_STOPS - 1 - i), hms(d + i)) for i in range(N_STOPS)]))
    # A coach through the same poles, five minutes after each bus.
    TRIPS.append((f"C{d}", "R025", "WK", "London",
                  [(n + 1, west(i), hms(d + 5 + i)) for n, i in enumerate((0, 20, 30, 50, 59))]))
for d in range(18 * 60, 23 * 60, 30):                  # evening, every half hour
    TRIPS.append(westbound(f"V{d}", d))
# The first bus of the morning, written on the previous day's service as 28:40.
TRIPS.append(westbound("MORNING", 28 * 60 + 40))


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
        z.writestr("agency.txt", _csv([("AG1", "Coast Buses", "SCSO"),
                                       ("AG2", "Coach Co", "NATX")],
                                      ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv(
            [("R700", "AG1", "700", "Coast", 3), ("R025", "AG2", "025", "Coach", 3)],
            ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv(
            [(t, r, s, h, "") for t, r, s, h, _ in TRIPS],
            ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", _csv(
            [(t, seq, sid, x, x) for t, _, _, _, calls in TRIPS for seq, sid, x in calls],
            ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"]))
        z.writestr("calendar.txt", _csv(
            [("WK", 1, 1, 1, 1, 1, 0, 0, "20260101", "20271231")],
            ["service_id", "monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday", "start_date", "end_date"]))
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(bt.parse_gtfs(str(feed)), out)
    return Timetable(out, allow_fetch=False)


def at(hh, mm, day=16):
    """A Wednesday in September 2026, London time."""
    return datetime(2026, 9, day, hh, mm, tzinfo=UK)


def bus_at(stop_index, *, service="700", operator="SCSO", bearing=256.0,
           destination="Worthing", toward_next=0.0):
    """A westbound bus at a stop, or part of the way on towards the next one."""
    lat, lon = west_xy(stop_index)
    nlat, nlon = west_xy(stop_index + 1)
    return {"service_ref": service, "operator_ref": operator,
            "latitude": lat + (nlat - lat) * toward_next,
            "longitude": lon + (nlon - lon) * toward_next,
            "bearing": bearing, "destination": destination}


def on_time_fleet(now_min, skip=()):
    """One bus on every westbound trip already on the road, where it should be."""
    fleet = []
    for d in range(10 * 60, 15 * 60, 10):
        idx = now_min - d
        if 0 <= idx < N_STOPS and d not in skip:
            fleet.append(bus_at(idx))
    return fleet


def run(tt, when, vehicles):
    return cg.corridor_gaps(tt, vehicles, when, watchpoints=WATCH)


def stop(result, name):
    return next(s for s in result["stops"] if s["name"] == name)


NOON_30 = 12 * 60 + 30


# ── The fixture itself ──────────────────────────────────────

def test_the_fixture_kept_both_operators(tt):
    assert west(30) in tt.stops and east(30) in tt.stops
    routes = {r.get("short_name"): r for r in tt.routes.values()}
    assert {"700", "025"} <= routes.keys()


# ── Quiet hours ─────────────────────────────────────────────

@pytest.mark.parametrize("hh,mm,quiet", [
    (23, 29, False), (23, 30, True), (0, 0, True), (1, 15, True),
    (4, 29, True), (4, 30, False), (12, 0, False),
])
def test_quiet_hours_are_2330_to_0430_across_midnight(hh, mm, quiet):
    assert cg.is_quiet(at(hh, mm)) is quiet


def test_nothing_is_measured_in_quiet_hours(tt):
    r = run(tt, at(0, 30), on_time_fleet(NOON_30))
    assert r["active"] is False
    assert r["status"] == "quiet"
    assert "stops" not in r


# ── A normal service reads as normal ────────────────────────

def test_buses_running_to_timetable_raise_no_alert(tt):
    r = run(tt, at(12, 30), on_time_fleet(NOON_30))
    assert r["status"] == "normal", r.get("alert")
    assert r["alert"] is None
    high = stop(r, "Shoreham High Street")
    # The 12:00 is at the stop now, the 12:10 and 12:20 are on the way.
    assert [n["due"] for n in high["next"]] == ["12:30", "12:40", "12:50"]
    assert {n["source"] for n in high["next"]} == {"live"}
    assert high["longest_gap"]["minutes"] <= 10


def test_the_timetables_own_evening_gap_is_not_an_alert(tt):
    # Every half hour after 18:00. A fixed twenty-minute rule would call this
    # a failure every evening, and it is the timetable working as published.
    fleet = [bus_at(30), bus_at(0)]              # the 19:30 and the 20:00
    r = run(tt, at(20, 0), fleet)
    high = stop(r, "Shoreham High Street")
    assert high["longest_gap"]["minutes"] >= 29
    assert high["longest_gap"]["alert"] is False
    assert r["status"] == "normal"


def test_trips_not_yet_started_come_from_the_timetable(tt):
    # 10:05: the first bus of the day is five stops in; the 10:10 has not left.
    r = run(tt, at(10, 5), [bus_at(5)])
    port = stop(r, "Shoreham Port")
    assert [(n["due"], n["source"]) for n in port["next"]] == [
        ("10:20", "live"), ("10:30", "scheduled"), ("10:40", "scheduled")]


# ── A real gap is reported, and honestly ────────────────────

def test_two_missing_buses_make_an_alert_that_names_them(tt):
    r = run(tt, at(12, 30), on_time_fleet(NOON_30, skip={12 * 60 + 10, 12 * 60 + 20}))
    assert r["status"] == "alert", r["stops"]
    a = r["alert"]
    assert a["name"] == "Shoreham High Street"
    assert (a["from"], a["to"], a["minutes"]) == ("12:30", "13:00", 30)
    assert a["timetable_minutes"] == 10
    # Two scheduled buses with no tracked vehicle fall inside the gap. The
    # wording has to say so rather than calling it a wait.
    assert a["not_reporting"] == 2


def test_a_coach_does_not_fill_a_bus_gap(tt):
    fleet = on_time_fleet(NOON_30, skip={12 * 60 + 10, 12 * 60 + 20})
    fleet.append(bus_at(25, service="025", operator="NATX", destination="London"))
    r = run(tt, at(12, 30), fleet)
    assert r["status"] == "alert"
    services = {n["service"] for s in r["stops"] for n in s["next"]}
    assert services == {"700"}, services


def test_a_bus_across_the_road_does_not_fill_a_westbound_gap(tt):
    fleet = on_time_fleet(NOON_30, skip={12 * 60 + 10, 12 * 60 + 20})
    # An eastbound 700 level with where the missing 12:10 should be.
    lat, lon = east_xy(25)
    fleet.append({"service_ref": "700", "operator_ref": "SCSO", "latitude": lat,
                  "longitude": lon, "bearing": 76.0, "destination": "Brighton"})
    r = run(tt, at(12, 30), fleet)
    # Counted, it would shorten the gap to 25 minutes, still an alert. The
    # whole gap, and both missing buses, is what shows it was not counted.
    a = r["alert"]
    assert a and (a["from"], a["to"], a["not_reporting"]) == ("12:30", "13:00", 2), \
        "an eastbound bus was counted as westbound"


def test_a_heading_of_zero_is_no_heading(tt):
    fleet = [dict(v, bearing=0.0) for v in on_time_fleet(NOON_30)]
    r = run(tt, at(12, 30), fleet)
    assert r["status"] == "normal", r.get("alert")
    assert r["coverage"]["reporting"] == r["coverage"]["on_road"]


def test_a_bus_that_has_gone_by_is_not_still_to_come(tt):
    fleet = on_time_fleet(NOON_30, skip={12 * 60})
    fleet.append(bus_at(31))                     # the 12:00, a stop past High Street
    r = run(tt, at(12, 30), fleet)
    assert stop(r, "Shoreham High Street")["next"][0]["due"] == "12:40"


def test_a_bus_just_past_the_watchpoint_itself_has_gone_by(tt):
    fleet = on_time_fleet(NOON_30, skip={12 * 60})
    fleet.append(bus_at(30, toward_next=0.3))    # nearest stop is still High Street
    r = run(tt, at(12, 30), fleet)
    assert stop(r, "Shoreham High Street")["next"][0]["due"] == "12:40"


def test_the_first_morning_bus_written_past_midnight_counts(tt):
    # 04:45 on Wednesday: the 28:40 on Tuesday's service left five minutes ago.
    r = run(tt, at(4, 45), [bus_at(5)])
    assert r["active"] is True
    port = stop(r, "Shoreham Port")
    assert port["next"] and port["next"][0]["due"] == "05:00", port


# ── When the feed cannot support a judgement ────────────────

def test_too_few_buses_reporting_shows_nothing_rather_than_a_gap(tt):
    fleet = on_time_fleet(NOON_30)[:2]
    r = run(tt, at(12, 30), fleet)
    assert r["status"] == "unknown"
    assert r["reason"] == "low_coverage"
    assert "alert" not in r


def test_no_live_data_is_unknown_not_normal(tt):
    r = run(tt, at(12, 30), [])
    assert r["status"] == "unknown"
    assert r["reason"] == "no_live_data"


def test_the_answer_carries_its_method_and_caveats(tt):
    r = run(tt, at(12, 30), on_time_fleet(NOON_30))
    assert "scheduled running times" in r["method"]
    assert any("not sending its position" in c for c in r["caveats"])
    assert r["as_of"].startswith("2026-09-16T12:30")


# ── The endpoint ────────────────────────────────────────────

def _fixed_clock(monkeypatch, main, when):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return when.astimezone(tz) if tz else when
    monkeypatch.setattr(main, "datetime", Clock)
    monkeypatch.setattr(main, "cache_get", lambda key: None)
    monkeypatch.setattr(main, "cache_set", lambda *a, **k: None)


def test_quiet_hours_answer_without_fetching_anything(monkeypatch):
    import asyncio
    import api.main as main

    async def must_not_be_called():
        raise AssertionError("the monitor fetched data during quiet hours")

    _fixed_clock(monkeypatch, main, at(1, 0))
    monkeypatch.setattr(main, "_get_timetable", must_not_be_called)
    monkeypatch.setattr(main, "_get_vehicles_or_empty", must_not_be_called)
    body = asyncio.run(main.get_corridor_gaps())
    assert body["active"] is False and body["reason"] == "quiet_hours"


def test_without_a_bods_key_the_endpoint_says_unknown_rather_than_failing(monkeypatch, tt):
    import asyncio
    import api.main as main

    async def timetable():
        return tt

    async def must_not_be_called():
        raise AssertionError("fetched vehicles with no BODS key")

    _fixed_clock(monkeypatch, main, at(12, 30))
    monkeypatch.setattr(main, "BODS_API_KEY", "")
    monkeypatch.setattr(main, "_get_timetable", timetable)
    monkeypatch.setattr(main, "_get_vehicles_or_empty", must_not_be_called)
    body = asyncio.run(main.get_corridor_gaps())
    assert (body["status"], body["reason"]) == ("unknown", "no_live_data")
