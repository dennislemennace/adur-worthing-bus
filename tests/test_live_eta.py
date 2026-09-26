"""Estimates for the stops still ahead of a tracked bus (api/live_eta.py).

The Bus tab and the stop board both show them, so a regression here tells a
passenger the wrong time in two places at once. The method is deliberately
simple and stated to readers: a late bus stays as late, an early one waits at
its next timing point, and a stop whose estimate has gone is treated as passed.
"""

import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import api.main as main                                          # noqa: E402
from api import live_eta                                         # noqa: E402
from api.trip_match import LONDON                                # noqa: E402

START = 14 * 3600                     # the 14:00, calling every two minutes


class FakeTimetable:
    def __init__(self, trip_id="VJ_1400", start=START, n=8, timepoints=None):
        self.trips = {trip_id: {"headsign": "Worthing", "route_id": "R700"}}
        self.routes = {"R700": {"short_name": "700"}}
        self.stops = {f"STOP{i}": {"lat": 50.83 + i * 0.001, "lon": -0.27,
                                   "name": f"Stop {i}"} for i in range(n)}
        self._calls = [(start + i * 120, f"STOP{i}") for i in range(n)]
        self._tps = timepoints if timepoints is not None else [None] * n

    def trip_stops_for(self, trip_id):
        return self._calls if trip_id in self.trips else []

    def timepoints_by_call(self, trip_id):
        return self._tps if trip_id in self.trips else []


def at(hour, minute, second=0, day=18):
    return datetime(2026, 9, day, hour, minute, second, tzinfo=LONDON)


def declared(lateness, lat=50.832, trip="VJ_1400", **over):
    bus = {"vehicle_ref": "BUS-1", "latitude": lat, "longitude": -0.27,
           "trip_id": trip, "trip_source": "feed", "lateness_secs": lateness}
    bus.update(over)
    return bus


def expected_minutes_late(rows):
    return [None if r["expected"] is None else
            round((datetime.fromisoformat(r["expected"])
                   - datetime.fromisoformat(r["scheduled"])).total_seconds() / 60)
            for r in rows]


def test_a_late_bus_is_expected_as_late_at_every_stop_ahead():
    tt = FakeTimetable()
    rows = live_eta.project_trip(tt, "VJ_1400", declared(240), at(14, 8))
    ahead = [r for r in rows if not r["passed"]]
    assert ahead[0]["stop_id"] == "STOP2"
    assert set(expected_minutes_late(ahead)) == {4}, "a late bus was made to catch up"


def test_an_early_bus_waits_at_its_next_timing_point():
    # UK buses must not leave a timing point early. Carrying two minutes early
    # all the way to the terminus would send people to the stop for a bus that
    # will sit and wait for its time.
    tps = [1, 0, 0, 0, 1, 0, 0, 1]
    tt = FakeTimetable(timepoints=tps)
    rows = live_eta.project_trip(tt, "VJ_1400", declared(-120), at(14, 2))
    by_stop = dict(zip([r["stop_id"] for r in rows], expected_minutes_late(rows)))
    assert by_stop["STOP2"] == -2 and by_stop["STOP3"] == -2
    assert by_stop["STOP4"] == 0 and by_stop["STOP7"] == 0, by_stop


def test_early_carries_on_where_the_timetable_names_no_timing_points():
    rows = live_eta.project_trip(FakeTimetable(), "VJ_1400", declared(-120), at(14, 2))
    ahead = [r for r in rows if not r["passed"]]
    assert set(expected_minutes_late(ahead)) == {-2}


def test_the_last_stop_passed_leads_the_list():
    rows = live_eta.project_trip(FakeTimetable(), "VJ_1400", declared(0), at(14, 4))
    assert rows[0]["stop_id"] == "STOP1" and rows[0]["passed"]
    assert rows[1]["stop_id"] == "STOP2" and rows[1]["is_next"]
    assert sum(r["is_next"] for r in rows) == 1
    assert rows[-1]["is_terminus"] and rows[-1]["stop_id"] == "STOP7"


def test_a_bus_already_beyond_its_nearest_stop_is_next_at_the_following_one():
    # Nearest to Stop 2 but most of the way to Stop 3: Stop 2 is behind it.
    rows = live_eta.project_trip(FakeTimetable(), "VJ_1400",
                                 declared(0, lat=50.8324), at(14, 5))
    nxt = next(r for r in rows if r["is_next"])
    assert nxt["stop_id"] == "STOP3"


def test_a_stale_report_marks_the_stops_it_has_probably_passed():
    # The report puts the bus short of Stop 2 (due 14:04), but it is now 14:09.
    # Stops 2 and 3 were due over a minute ago: it has been by them. Stop 4,
    # due 14:08, is inside the minute's grace and still next.
    rows = live_eta.project_trip(FakeTimetable(), "VJ_1400", declared(0), at(14, 9))
    assert rows[0]["stop_id"] == "STOP3" and rows[0]["passed"]
    assert rows[1]["stop_id"] == "STOP4" and rows[1]["is_next"]


def test_an_inferred_journey_gets_stops_but_no_estimate():
    bus = declared(240, trip_source=None)
    rows = live_eta.project_trip(FakeTimetable(), "VJ_1400", bus, at(14, 8))
    assert rows and all(r["expected"] is None and r["lateness_secs"] is None for r in rows)


def test_a_lateness_for_another_journey_is_not_borrowed():
    # The feed named one journey; the stop list is for another. Its lateness
    # says nothing about this one.
    bus = declared(240, trip="VJ_OTHER")
    rows = live_eta.project_trip(FakeTimetable(), "VJ_1400", bus, at(14, 8))
    assert all(r["expected"] is None for r in rows)


def test_times_after_midnight_land_on_the_right_day():
    tt = FakeTimetable(trip_id="VJ_NIGHT", start=24 * 3600 + 20 * 60)
    rows = live_eta.project_trip(tt, "VJ_NIGHT", declared(0, trip="VJ_NIGHT"),
                                 at(0, 23, day=19))
    nxt = next(r for r in rows if r["is_next"])
    first = datetime.fromisoformat(nxt["scheduled"])
    assert (first.day, first.hour, first.minute) == (19, 0, 24), first


def test_times_before_midnight_seen_after_it_stay_on_the_evening_before():
    tt = FakeTimetable(trip_id="VJ_LATE", start=23 * 3600 + 50 * 60)
    rows = live_eta.project_trip(tt, "VJ_LATE", declared(600, trip="VJ_LATE"),
                                 at(0, 4, day=19))
    nxt = next(r for r in rows if r["is_next"])
    assert datetime.fromisoformat(nxt["scheduled"]).day == 18
    assert datetime.fromisoformat(nxt["expected"]) > at(0, 3, day=19)


def test_estimate_at_answers_only_for_a_stop_still_ahead():
    tt = FakeTimetable()
    bus = declared(180)
    now = at(14, 5)
    ahead = live_eta.estimate_at(tt, "VJ_1400", bus, "STOP5", at(14, 10), now)
    assert ahead and round(ahead["lateness_secs"] / 60) == 3
    behind = live_eta.estimate_at(tt, "VJ_1400", bus, "STOP1", at(14, 2), now)
    assert behind is None, "an estimate was given for a stop the bus has left"


# ── Wired into the API ──────────────────────────────────────

def _board(departures):
    return {"stop_name": "Stop 5", "departures": departures}


def _row(aimed, trip="VJ_1400", **over):
    row = {"service": "700", "operator": "SCSO", "destination": "Worthing",
           "aimed_departure": aimed.isoformat(), "expected_departure": None,
           "status": "Scheduled", "delay_seconds": None, "_trip_id": trip}
    row.update(over)
    return row


def _cached(vehicles):
    return lambda key: {"vehicles": vehicles} if key == "vehicles:recent" else None


def test_the_board_takes_an_estimate_from_the_bus_the_feed_names(monkeypatch):
    monkeypatch.setattr(main, "cache_get", _cached([declared(180)]))
    out = main._apply_own_feed_estimates(_board([_row(at(14, 10))]),
                                         FakeTimetable(), "STOP5", at(14, 5))
    dep = out["departures"][0]
    assert dep["live_source"] == "feed"
    assert dep["delay_seconds"] == 180 and dep["status"] == "Late"
    assert datetime.fromisoformat(dep["expected_departure"]) == at(14, 13)
    assert out["live"] is True


def test_a_prediction_from_transportapi_is_not_overwritten(monkeypatch):
    monkeypatch.setattr(main, "cache_get", _cached([declared(180)]))
    row = _row(at(14, 10), expected_departure=at(14, 11).isoformat(),
               delay_seconds=60, status="On time", live_source="prediction")
    out = main._apply_own_feed_estimates(_board([row]), FakeTimetable(), "STOP5", at(14, 5))
    assert out["departures"][0]["expected_departure"] == at(14, 11).isoformat()
    assert out["departures"][0]["live_source"] == "prediction"


def test_the_board_never_fetches_the_feed_itself(monkeypatch):
    # The board must not wait on BODS or add a call to it: no cached vehicles,
    # no estimate, and nothing else happens.
    monkeypatch.setattr(main, "cache_get", lambda key: None)
    out = main._apply_own_feed_estimates(_board([_row(at(14, 10))]),
                                         FakeTimetable(), "STOP5", at(14, 5))
    assert out["departures"][0]["expected_departure"] is None
    assert "live_source" not in out["departures"][0]


def test_an_inferred_bus_gives_the_board_no_estimate(monkeypatch):
    monkeypatch.setattr(main, "cache_get", _cached([declared(180, trip_source="inferred")]))
    out = main._apply_own_feed_estimates(_board([_row(at(14, 10))]),
                                         FakeTimetable(), "STOP5", at(14, 5))
    assert out["departures"][0]["expected_departure"] is None


def test_internal_trip_ids_do_not_reach_the_reader():
    payload = main._public_departures(_board([_row(at(14, 10))]))
    assert "_trip_id" not in payload["departures"][0]


def test_the_bus_tab_lists_the_journey_the_feed_named(monkeypatch):
    # Inference used to run first and win: a bus that said it was the 14:00
    # could be shown another departure's stops under a Journey row saying 14:00.
    import asyncio
    tt = FakeTimetable()
    bus = declared(120, service_ref="700", operator_ref="SCSO", journey_start="14:00",
                   report_age_secs=30)

    async def vehicles():
        return [bus]

    async def timetable():
        return tt

    async def inline(fn, *args):
        return fn(*args)

    monkeypatch.setattr(main, "_check_api_key", lambda: None)
    monkeypatch.setattr(main, "_get_vehicles_or_empty", vehicles)
    monkeypatch.setattr(main, "_get_timetable", timetable)
    monkeypatch.setattr(main, "off_loop", inline)
    monkeypatch.setattr(main, "_best_trip_for_vehicle", lambda *_: "VJ_SOMETHING_ELSE")
    out = asyncio.run(main.get_vehicle(vehicleRef="BUS-1"))
    assert out["source"] == "trip"
    assert out["vehicle"]["trip_source"] == "feed"
    assert out["vehicle"]["lateness_secs"] == 120
    assert any(r["expected"] for r in out["upcoming_stops"]), "the named journey was replaced"
