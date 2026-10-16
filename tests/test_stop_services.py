"""Every service at a stop, its next times beyond the board, and the last bus
home from Brighton or from Worthing.

Built for the stop board's route buttons (every service calling there, even
with nothing in the next two hours, opening its next timetabled times on the
right day) and for Route view's stop panel (when and how often each service
calls). And two faults the Worthing last bus exposed in the old rule:

  * a National Express coach through Brighton at 03:35 was shown as the last
    "day bus" home at every stop on the coast;
  * the first 700 of the morning, at 03:45 from Worthing, was shown as the
    last bus of the evening, because GTFS files it under the day before.

Run with:  pytest
"""

import asyncio
import csv
import io
import sys
import zipfile
from datetime import date, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import build_timetable as bt        # noqa: E402
import json_to_sqlite as j2s        # noqa: E402
import api.main as main             # noqa: E402
from api.timetable_db import Timetable   # noqa: E402

STOPS = [
    ("1490HUB1", "Old Steine",      50.8230, -0.1370),   # central Brighton
    ("4400WO0254", "Marine Parade", 50.8100, -0.3681),   # Worthing, by the pier
    ("4400SH01", "High Street",     50.8320, -0.2750),   # the reader's stop
    ("4400SH02", "High Street",     50.8321, -0.2752),   # across the road
    ("4400LA01", "Lancing",         50.8270, -0.3250),
]


def _every(prefix, route, service, first, gap_mins, n, calls):
    """`n` trips `gap_mins` apart; `calls` are (seq, stop, minutes after the start)."""
    out = []
    for i in range(n):
        start = first + i * gap_mins
        out.append((f"{prefix}{i}", route, service,
                    [(seq, stop, f"{(start + m) // 60:02d}:{(start + m) % 60:02d}:00") for seq, stop, m in calls]))
    return out


TRIPS = [
    # The 700 from Brighton every 12 minutes through the day, and its last at 23:30.
    *_every("B", "R700", "WK", 7 * 60, 12, 61, [(1, "1490HUB1", 0), (2, "4400SH01", 25)]),
    ("B_LAST", "R700", "WK", [(1, "1490HUB1", "23:30:00"), (2, "4400SH01", "23:55:00")]),
    # A coach through Brighton in the small hours: no bus ticket's way home.
    ("COACH", "R025", "WK", [(1, "1490HUB1", "25:35:00"), (2, "4400SH01", "25:50:00")]),
    # From Worthing: the last 700 of the evening, a night bus, and the first
    # 700 of the next morning written on the same service day as 03:45.
    # Eastbound, so at the pole across the road: the last bus home is asked
    # of the place, both poles, as a passenger means it.
    *_every("W", "R700", "WK", 6 * 60, 30, 36, [(1, "4400WO0254", 0), (2, "4400SH02", 13)]),
    ("W_NIGHT", "RN700", "WK", [(1, "4400WO0254", "26:45:00"), (2, "4400SH02", "26:58:00")]),
    ("W_MORNING", "R700", "WK", [(1, "4400WO0254", "03:45:00"), (2, "4400SH02", "03:58:00")]),
    # Weekdays only, once a day: the route a Saturday board has no row for.
    ("SEVENTY7", "R77", "WK", [(1, "4400SH01", "09:16:00"), (2, "4400LA01", "09:40:00")]),
]

ROUTES = [("R700", "SC", "700"), ("RN700", "SC", "N700"), ("R77", "SC", "77"), ("R025", "NX", "025")]
SATURDAY_NIGHT = datetime(2026, 10, 17, 21, 0, tzinfo=main.UK_TZ)
WEEK_OF = date(2026, 10, 19)    # a Monday


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
        z.writestr("agency.txt", _csv([("SC", "Stagecoach", "SCSO"), ("NX", "National Express", "NATX")],
                                      ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv([(r, a, n, n, 3) for r, a, n in ROUTES],
                                      ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv([(t, r, s, "Lancing", "") for t, r, s, _ in TRIPS],
                                     ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", _csv(
            [(t, seq, sid, hms, hms) for t, _, _, calls in TRIPS for seq, sid, hms in calls],
            ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"]))
        z.writestr("calendar.txt", _csv([("WK", 1, 1, 1, 1, 1, 0, 0, "20260101", "20271231")],
                                        ["service_id", "monday", "tuesday", "wednesday", "thursday",
                                         "friday", "saturday", "sunday", "start_date", "end_date"]))
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(bt.parse_gtfs(str(feed)), out)
    yield Timetable(out, allow_fetch=False)
    mp.undo()


def test_the_last_bus_from_brighton_is_never_a_coach(tt):
    tue = tt.last_bus_from_hub("4400SH01", from_day=WEEK_OF)["days"]["tuesday"]
    assert tue["day_bus"]["service"] == "700", f"a coach was offered as the last bus home: {tue}"
    assert tue["day_bus"]["depart"] == "23:30"


def test_the_last_bus_from_worthing_is_the_evening_one_not_the_first_of_the_morning(tt):
    result = tt.last_bus_from_hub("4400SH01", from_day=WEEK_OF, hub="worthing")
    tue = result["days"]["tuesday"]
    assert tue["day_bus"]["depart"] == "23:30", f"the morning's 03:45 was offered as the last bus: {tue}"
    assert tue["day_bus"]["from"] == "Marine Parade"
    assert tue["night_bus"]["depart"] == "02:45"
    assert result["days"]["saturday"] is None


def test_a_marine_parade_stop_is_in_worthing_not_asked_about_it(tt):
    assert tt.last_bus_from_hub("4400WO0254", from_day=WEEK_OF, hub="worthing")["inside_hub"] is True
    assert tt.last_bus_from_hub("4400WO0254", from_day=WEEK_OF)["inside_hub"] is False


def test_every_service_at_a_stop_including_one_with_nothing_today(tt):
    # Asked on a Saturday: the 77 runs on weekdays only, so nothing today, but
    # Monday is within the week and it stops here.
    got = tt.services_at_stop("4400SH01", from_day=date(2026, 10, 17))
    assert {(s["service"], s["operator"]) for s in got} == {("77", "SCSO"), ("700", "SCSO"), ("025", "NATX")}
    assert [s["service"] for s in got] == ["025", "77", "700"], "not in timetable order"
    across = tt.services_at_stop("4400SH02", from_day=date(2026, 10, 17))
    assert [s["service"] for s in across] == ["700", "N700"], "the board is one pole, not the place"


def test_each_service_says_when_and_how_often_it_calls(tt):
    by = {s["service"]: s for s in tt.stop_services("4400SH01", from_day=WEEK_OF)}
    weekday = by["700"]["days"]["weekday"]
    assert weekday["every_min"] == 12, weekday
    assert weekday["first"] == "07:25"
    assert by["700"]["days"]["saturday"] is None
    once = by["77"]["days"]["weekday"]
    assert (once["count"], once["every_min"], once["first"]) == (1, None, "09:16")
    across = {s["service"]: s for s in tt.stop_services("4400SH02", from_day=WEEK_OF)}
    night = across["N700"]["days"]["weekday"]
    assert (night["first"], night["night"]) == ("", 1), "a night-only service was given daytime hours"


def test_the_next_times_of_a_weekday_route_asked_on_saturday_are_mondays(tt):
    result = main._next_departures(tt, "4400SH01", SATURDAY_NIGHT, "77")
    days = [d["aimed_departure"][:10] for d in result["departures"]]
    assert days[:3] == ["2026-10-19", "2026-10-20", "2026-10-21"], days
    assert all(d["service"] == "77" for d in result["departures"])
    assert result["departures"][0]["aimed_departure"].endswith("09:16:00+01:00")


def test_a_route_with_no_times_in_the_window_says_so(tt):
    result = main._next_departures(tt, "4400LA01", SATURDAY_NIGHT, "700")
    assert result["departures"] == [] and result["days_searched"] == 8


def test_the_endpoints_carry_the_new_fields(tt, monkeypatch):
    async def timetable():
        return tt

    async def passthrough(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(main, "_get_timetable", timetable)
    monkeypatch.setattr(main, "off_loop", passthrough)
    monkeypatch.setattr(main, "cache_get", lambda key: None)
    monkeypatch.setattr(main, "cache_set", lambda key, value, ttl: None)
    span = asyncio.run(main.get_stop_span(stopId="4400SH01", lat=None, lon=None))
    assert "last_from_worthing" in span and "worthing" in span["last_from_worthing"]["method"].lower()
    assert "central" not in span["last_from_brighton"]["method"].lower()
    assert {s["service"] for s in span["services"]} >= {"700", "77"}
    nxt = asyncio.run(main.get_next_departures(stopId="4400SH01", service="77", operator=None,
                                               lat=None, lon=None))
    assert nxt["service"] == "77" and nxt["source"] == "timetable"
    with pytest.raises(main.HTTPException):
        asyncio.run(main.get_next_departures(stopId="4400SH01", service="<script>", operator=None,
                                             lat=None, lon=None))
