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
    bus = {"vehicle_ref": "BUS-1", "service_ref": "700", "operator_ref": "SCSO",
           "latitude": 50.832, "longitude": -0.27, "bearing": 256.0,
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
    tt = FakeTimetable(trip_id="VJ_LATE_NIGHT", start=24 * 3600 + 5 * 60)
    _freeze(monkeypatch, 23, 58)
    bus = a_bus(declared_trip_id="VJ_LATE_NIGHT", latitude=50.830, longitude=-0.27)
    main._attach_declared_journeys([bus], tt)
    assert bus["lateness_secs"] == -7 * 60, \
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
