"""The live map, using the journey the feed names rather than guessing it.

SIRI-VM says where a bus is. GTFS-RT says which scheduled journey it is
running — 256 of 259 vehicles state one, and 184 name a journey we hold,
against 0 of 256 for SIRI-VM's own journey reference.

That changes what the map can honestly say. "The 14:22 from Brighton, four
minutes late" is a different claim from "probably a 700", and these tests pin
the difference: the feed's answer wins, inference fills the rest, and every
vehicle records which of the two it got.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import api.main as main                                          # noqa: E402
from api import gtfs_rt                                          # noqa: E402
from test_gtfs_rt import a_feed, a_vehicle                       # noqa: E402


class FakeTimetable:
    """Enough timetable for one journey: the 14:22, calling every two minutes."""

    def __init__(self, trip_id="VJ_1422", start=14 * 3600 + 22 * 60):
        self.trips = {trip_id: {"headsign": "Worthing", "route_id": "R700"}}
        self.routes = {"R700": {"short_name": "700"}}
        self.stops = {
            f"STOP{i}": {"lat": 50.83 + i * 0.001, "lon": -0.27, "name": f"Stop {i}"}
            for i in range(5)
        }
        self.calendar, self.calendar_dates = {}, {}
        self.stop_times = {}
        self._calls = [(start + i * 120, f"STOP{i}") for i in range(5)]

    def ok(self):
        return True

    def trip_stops_for(self, trip_id):
        return self._calls if trip_id in self.trips else []

    def noc_for_route(self, _route_id):
        return "SCSO"

    def service_endpoints(self, _service_key):
        """The inference path walks these. The declared path does not, which
        is rather the point: it needs the trip id and nothing else."""
        return []


def a_bus(**over):
    # Heading north, the way the fake journey's stops run: a bus pointing
    # against its declared journey is now grounds to doubt the declaration.
    bus = {"vehicle_ref": "BUS-1", "service_ref": "700", "operator_ref": "SCSO",
           "latitude": 50.832, "longitude": -0.27, "bearing": 0.5,
           "destination": "Worthing"}
    bus.update(over)
    return bus


def _freeze(monkeypatch, hour, minute):
    """Pin the clock the enrichment reads."""
    import datetime as dt

    class Frozen(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 9, 18, hour, minute, tzinfo=tz)

    monkeypatch.setattr(main, "datetime", Frozen)


# ── Reading the second feed ─────────────────────────────────

def test_a_declared_journey_is_read_from_the_feed():
    payload = a_feed([a_vehicle(trip="VJ_1422", vehicle="BUS-1"),
                      a_vehicle(trip="VJ_OTHER", vehicle="BUS-2")])
    _header, vehicles = gtfs_rt.parse_feed(payload)
    assert {v["vehicle_id"]: v["trip_id"] for v in vehicles} == {
        "BUS-1": "VJ_1422", "BUS-2": "VJ_OTHER"}


# ── What the map may then say ───────────────────────────────

def test_a_bus_that_names_its_journey_is_taken_at_its_word():
    tt = FakeTimetable()
    bus = a_bus(declared_trip_id="VJ_1422")
    main._attach_declared_journeys([bus], tt)
    assert bus["trip_id"] == "VJ_1422"
    assert bus["trip_headsign"] == "Worthing"
    assert bus["trip_source"] == "feed", "the map cannot tell a fact from a guess"
    assert bus["journey_start"] == "14:22", "the journey has no name a reader knows"


def test_lateness_is_measured_against_the_journey_the_bus_declared(monkeypatch):
    # The thing no amount of inference gives honestly: how late this bus is
    # against its own timetable. It sits at the third stop, due 14:26, at 14:30.
    tt = FakeTimetable()
    _freeze(monkeypatch, 14, 30)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.832, longitude=-0.27)
    main._attach_declared_journeys([bus], tt)
    assert bus["nearest_stop_name"] == "Stop 2"
    assert bus["lateness_secs"] == 4 * 60, f'{bus["lateness_secs"]} seconds'


def test_lateness_is_measured_on_the_bus_clock_not_ours(monkeypatch):
    # The feed runs a median 186 seconds behind: three quarters of reports are
    # over a minute stale and half over three. Judging a bus by the moment we
    # happened to ask therefore adds our own latency to its lateness, against
    # DfT bands one and six minutes wide — an operator shown late for our
    # network round trip.
    #
    # This bus is at its third stop, due 14:26, and it said so at 14:26. We are
    # reading the feed at 14:30. It is on time, and four minutes late only if
    # the clock we use is the wrong one.
    tt = FakeTimetable()
    _freeze(monkeypatch, 14, 30)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.832, longitude=-0.27,
                recorded_at="2026-09-18T14:26:00+01:00")
    main._attach_declared_journeys([bus], tt)
    assert bus["lateness_secs"] == 0, \
        f'{bus["lateness_secs"]}s: the feed\'s latency was charged to the bus'


def test_the_age_of_the_report_is_published_beside_it(monkeypatch):
    # A dot on a map reads as "now". Often it is three minutes old, and on a
    # stale report that is the difference between on time and late, so the map
    # is given what it needs to hedge instead of quietly asserting.
    tt = FakeTimetable()
    _freeze(monkeypatch, 14, 30)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.832, longitude=-0.27,
                recorded_at="2026-09-18T14:26:00+01:00")
    main._attach_declared_journeys([bus], tt)
    assert bus["report_age_secs"] == 4 * 60


def test_a_bus_that_never_said_when_falls_back_to_our_clock(monkeypatch):
    # Some reports carry no RecordedAtTime. Dropping those buses would empty
    # the map for a missing field; the honest answer is our clock and an
    # unstated age, so the map knows not to claim freshness it cannot check.
    tt = FakeTimetable()
    _freeze(monkeypatch, 14, 30)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.832, longitude=-0.27)
    main._attach_declared_journeys([bus], tt)
    assert bus["lateness_secs"] == 4 * 60
    assert bus["report_age_secs"] is None, "an unknown age was reported as fresh"


def test_a_night_bus_is_not_reported_a_day_late(monkeypatch):
    # GTFS writes half past midnight as 24:30. Comparing that against a clock
    # reading 00:28 without wrapping makes an on-time bus almost a day out.
    tt = FakeTimetable(trip_id="VJ_NIGHT", start=24 * 3600 + 20 * 60)
    _freeze(monkeypatch, 0, 28)
    bus = a_bus(declared_trip_id="VJ_NIGHT", latitude=50.832)
    main._attach_declared_journeys([bus], tt)
    assert abs(bus["lateness_secs"]) < 15 * 60, \
        f'a night bus came out {bus["lateness_secs"] / 3600:.1f} hours out'


def test_a_journey_we_do_not_hold_is_left_to_inference():
    # The feed names city routes our timetable filters out. Those buses must
    # still appear on the map, matched the old way.
    tt = FakeTimetable()
    bus = a_bus(declared_trip_id="VJ_NOT_OURS")
    main._attach_declared_journeys([bus], tt)
    assert "trip_id" not in bus
    assert bus.get("trip_source") is None


def test_inference_does_not_overwrite_what_the_feed_stated():
    # The two fallback strategies are guesses — about 80% right on this feed.
    # They may fill gaps; they may not argue with the operator. Enrichment is
    # called on its own here, as the live path calls it, so that dropping the
    # declared step entirely shows up as a failure rather than a silent
    # return to guessing.
    tt = FakeTimetable()
    stated = a_bus(declared_trip_id="VJ_1422")
    main._enrich_vehicles_with_trip_match([stated], tt)
    assert stated["trip_id"] == "VJ_1422", "the live path stopped reading the feed"
    assert stated["trip_source"] == "feed"


def test_a_bus_just_before_midnight_is_not_a_day_late(monkeypatch):
    # The clock reads 23:58 and the bus is due at 00:05 — 86,700 seconds in
    # GTFS, which is seven minutes away and not twenty-three hours ago. The
    # modulo alone does not fix this; the wrap does.
    # At its second stop, due 00:07: early at the first stop is a bus
    # waiting on the stand, which is a different claim (tested below).
    tt = FakeTimetable(trip_id="VJ_LATE_NIGHT", start=24 * 3600 + 5 * 60)
    _freeze(monkeypatch, 23, 58)
    bus = a_bus(declared_trip_id="VJ_LATE_NIGHT", latitude=50.831, longitude=-0.27)
    main._attach_declared_journeys([bus], tt)
    assert bus["lateness_secs"] == -9 * 60, \
        f'{bus["lateness_secs"] / 3600:.1f} hours out at the midnight boundary'


def test_a_bus_running_late_past_midnight_is_not_a_day_early(monkeypatch):
    # The mirror of the case above: due 23:55, still going at 00:05. Without
    # the wrap in the other direction it reads as 23 hours and 50 minutes
    # early, which on the map is a bus from tomorrow.
    tt = FakeTimetable(trip_id="VJ_OVERRUN", start=23 * 3600 + 55 * 60)
    _freeze(monkeypatch, 0, 5)
    bus = a_bus(declared_trip_id="VJ_OVERRUN", latitude=50.830, longitude=-0.27)
    main._attach_declared_journeys([bus], tt)
    assert bus["lateness_secs"] == 10 * 60, \
        f'{bus["lateness_secs"] / 3600:.1f} hours out at the midnight boundary'


def test_a_declaration_the_bus_is_driving_against_is_not_believed(monkeypatch):
    # The N48, 27 September: the ticket machine still named the 01:45 out
    # while the bus drove the 02:02 back, past the same stop the other way.
    # Measured against the journey it named, an on-time bus was 44 minutes
    # late. Here: the 14:22 runs north, and at 15:10 a bus pointing south sits
    # at its third stop, 44 minutes after that stop's time.
    tt = FakeTimetable()
    _freeze(monkeypatch, 15, 10)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.832, bearing=180.0)
    main._attach_declared_journeys([bus], tt)
    assert bus.get("trip_source") != "feed", "a contradicted declaration was taken as fact"
    assert "lateness_secs" not in bus, "the bus was still given the declared journey's lateness"
    assert "declared_journey_contradicted_by_heading" in bus["identity_flags"]
    assert 'service_origin_epoch' not in bus


def test_service_origin_stays_internal_in_vehicle_response(monkeypatch):
    import asyncio
    async def vehicles():
        return [{'vehicle_ref': 'bus', 'service_origin_epoch': 1234, 'trip_id': 'private'}]
    monkeypatch.setattr(main, '_check_api_key', lambda: None)
    monkeypatch.setattr(main, '_live_vehicles', vehicles)
    result = asyncio.run(main.get_vehicles())
    assert result['vehicles'] == [{'vehicle_ref': 'bus'}]


def test_a_diverted_bus_off_its_route_keeps_its_journey(monkeypatch):
    # The Western Road closure, 28 Sep 2026: a 5B 650 m off its route on the
    # diversion, 21 minutes late, pointing the "wrong" way along a side
    # street. Its heading says nothing about the journey there, and the rule
    # meant for stale declarations threw a true one away.
    tt = FakeTimetable()
    _freeze(monkeypatch, 15, 10)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.832, longitude=-0.261, bearing=180.0)
    main._attach_declared_journeys([bus], tt)
    assert bus["trip_source"] == "feed", "a diverted bus lost the journey it named"
    assert bus["lateness_secs"] == 44 * 60


def test_a_bus_nearest_a_closed_stop_is_on_diversion(tmp_path, monkeypatch):
    # "near Brunswick Place" of a bus two streets away on Lansdowne Road
    # sends a reader to the one stop it will not call at.
    import json
    from api import local_disruptions as ld
    path = tmp_path / "disruptions.json"
    path.write_text(json.dumps({"disruptions": [{
        "id": "x-closure", "starts": "2026-09-01T00:00:00+01:00",
        "ends": "2026-12-01T00:00:00+00:00",
        "stops_not_served": [{"atco": "STOP2"}]}]}))
    monkeypatch.setattr(ld, "PATH", path)
    tt = FakeTimetable()
    _freeze(monkeypatch, 14, 27)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.832)
    main._attach_declared_journeys([bus], tt)
    assert bus.get("on_diversion") is True
    assert bus["nearest_stop_name"] == ""


def test_a_very_late_bus_going_the_right_way_is_still_very_late(monkeypatch):
    # The same 44 minutes, heading north with its journey: a bus that really
    # is that late must not be explained away.
    tt = FakeTimetable()
    _freeze(monkeypatch, 15, 10)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.832, bearing=2.0)
    main._attach_declared_journeys([bus], tt)
    assert bus["trip_source"] == "feed"
    assert bus["lateness_secs"] == 44 * 60


def test_a_slightly_off_heading_does_not_overturn_an_on_time_bus(monkeypatch):
    # Headings are noisy at a stop. On time, pointing the wrong way, the bus
    # keeps its journey: only a claim of a long delay invites the doubt.
    tt = FakeTimetable()
    _freeze(monkeypatch, 14, 26)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.832, bearing=180.0)
    main._attach_declared_journeys([bus], tt)
    assert bus["trip_source"] == "feed"
    assert bus["lateness_secs"] == 0


def test_a_bus_on_the_stand_before_its_start_is_waiting_not_early(monkeypatch):
    # The N48 sat at Old Steine "10 minutes early" for its 02:45, and the
    # N25 "22 minutes early" at the Royal Pavilion. Both were on the stand.
    tt = FakeTimetable()
    _freeze(monkeypatch, 14, 12)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.8299, bearing=None)
    main._attach_declared_journeys([bus], tt)
    assert bus["waiting_to_start"] is True
    assert bus["lateness_secs"] == 0, "a bus waiting for its time was called early"


def test_a_bus_that_has_left_its_first_stop_early_is_early(monkeypatch):
    # Past the pole and towards the second stop: it went early, and says so.
    tt = FakeTimetable()
    _freeze(monkeypatch, 14, 18)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.8304)
    main._attach_declared_journeys([bus], tt)
    assert bus["waiting_to_start"] is False
    # Two fifths of the way to the 14:24 stop, where it was due at 14:22:48.
    assert bus["lateness_secs"] == -(4 * 60 + 48)


# ── Between stops ───────────────────────────────────────────

def test_a_bus_between_stops_is_judged_where_it_is_not_at_the_nearest_stop(monkeypatch):
    # Three fifths of the way from the 14:24 stop to the 14:26 one, at 14:25:12:
    # exactly on time. Judged at the nearest stop, due 14:26, it was 48 seconds
    # early; a bus just short of a stop was always early and one just past it
    # always late, by up to half the time between them.
    tt = FakeTimetable()
    _freeze(monkeypatch, 14, 30)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.8316,
                recorded_at="2026-09-18T14:25:12+01:00")
    main._attach_declared_journeys([bus], tt)
    assert bus["lateness_secs"] == 0, f'{bus["lateness_secs"]}s'
    assert bus["nearest_stop_name"] == "Stop 2", "the stop named is still the nearest"


class TimedTimetable(FakeTimetable):
    """The same journey, with its third stop a timing point."""
    def timepoints_by_call(self, _trip_id):
        return [1, 0, 1, 0, 1]


def test_a_bus_waiting_at_a_timing_point_before_its_time_is_on_time(monkeypatch):
    # Drivers must not leave a timing point early, and wait there. A bus sat
    # at the 14:26 stop at 14:25 will leave at 14:26.
    tt = TimedTimetable()
    _freeze(monkeypatch, 14, 30)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.832,
                recorded_at="2026-09-18T14:25:00+01:00")
    main._attach_declared_journeys([bus], tt)
    assert bus["lateness_secs"] == 0, "a bus waiting for its time was called early"


def test_a_bus_early_at_an_ordinary_stop_is_early(monkeypatch):
    # Nothing holds a bus at a stop that is not a timing point.
    tt = FakeTimetable()
    _freeze(monkeypatch, 14, 30)
    bus = a_bus(declared_trip_id="VJ_1422", latitude=50.832,
                recorded_at="2026-09-18T14:25:00+01:00")
    main._attach_declared_journeys([bus], tt)
    assert bus["lateness_secs"] == -60


def test_the_two_live_feeds_are_joined_on_the_vehicle(monkeypatch):
    """The live fetch asks both feeds and joins them on the vehicle reference.

    Both carry identical vehicle ids — checked vehicle by vehicle against the
    same minute, agreeing to a median of 0 metres. Testing the pieces
    separately never exercises the join, and a join that silently does nothing
    looks exactly like a feed with no trip ids in it.
    """
    import asyncio

    tt = FakeTimetable()
    monkeypatch.setattr(main, "_fetch_siri_vm",
                        _async(lambda: [a_bus(vehicle_ref="BUS-1"),
                                        a_bus(vehicle_ref="BUS-2")]))
    monkeypatch.setattr(main, "_fetch_declared_journeys",
                        _async(lambda: {"BUS-1": "VJ_1422"}))
    monkeypatch.setattr(main, "_get_timetable", _async(lambda: tt))
    monkeypatch.setattr(main, "off_loop", _passthrough)

    result = asyncio.run(main._fetch_and_match_vehicles())
    buses = {v["vehicle_ref"]: v for v in result["vehicles"]}
    assert buses["BUS-1"].get("trip_id") == "VJ_1422", \
        "the journey the feed named never reached the vehicle"
    assert buses["BUS-1"].get("trip_source") == "feed"
    assert buses["BUS-2"].get("trip_source") != "feed", \
        "a bus that declared nothing was marked as having declared"


def _async(fn):
    async def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)
    return wrapper


async def _passthrough(fn, *args, **kwargs):
    """Stand in for off_loop, which would otherwise need a thread pool."""
    return fn(*args, **kwargs)


# ── Where a bus has been ────────────────────────────────────

def test_a_trail_keeps_this_journey_once_each_and_forgets_the_old(monkeypatch):
    from datetime import datetime, timedelta, timezone
    monkeypatch.setattr(main, "_trails", {})
    t0 = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)

    def poll(minute, lat, trip):
        bus = {"vehicle_ref": "BUS-1", "latitude": lat, "longitude": -0.27,
               "recorded_at": (t0 + timedelta(minutes=minute)).isoformat(), "trip_id": trip}
        main._record_trails([bus], t0 + timedelta(minutes=minute))

    poll(0, 50.830, "VJ_EARLIER")
    poll(5, 50.831, "VJ_1422")
    poll(5, 50.831, "VJ_1422")          # the same report read twice
    poll(6, 50.832, "VJ_1422")
    trail = main._trail_for("BUS-1", "VJ_1422")
    assert [p[0] for p in trail] == [50.831, 50.832], \
        "the earlier journey, or a repeated report, was drawn into this one"
    assert len(main._trail_for("BUS-1", None)) == 3, "an undeclared bus shows all it has"

    poll(60, 50.840, "VJ_1422")          # 45 minutes on, the old points go
    assert [p[0] for p in main._trail_for("BUS-1", "VJ_1422")] == [50.840]
