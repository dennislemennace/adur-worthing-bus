"""FlixBus's live feeds: where its coaches are, and how late FlixBus says.

FlixBus publishes no locations to BODS. Its own GTFS-RT feeds, published by
FlixBus France on transport.data.gouv.fr under the ODbL, carry both: trip
updates with a delay at every stop, and vehicle positions. As surveyed on
29 October 2026, they leave out a good deal a BODS feed carries, and each gap
is a test here:

  * no vehicle id at all, so a coach is known by its journey;
  * no heading, and no start date on a position;
  * delays only, never a time, and stops named by FlixBus's own ids.

The messages are built by an encoder written from the GTFS-RT specification,
not from the decoder, as tests/test_gtfs_rt.py does.

Run with:  pytest
"""

import asyncio
import copy
import struct
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import build_timetable as bt              # noqa: E402
import flixbus_fixture as fx              # noqa: E402
import flixbus_gtfs as fg                 # noqa: E402
import json_to_sqlite as j2s              # noqa: E402
import api.main as main                   # noqa: E402
from api import flixbus                   # noqa: E402
from api.timetable_db import Timetable    # noqa: E402

BRIGHTON = "9000F22A3795"
PRESTON = "9100PRSPBUS"
GATWICK_BAY = "4400CY0375"
BOX = (main.BBOX_MIN_LAT, main.BBOX_MAX_LAT, main.BBOX_MIN_LON, main.BBOX_MAX_LON)
NOW = datetime(2026, 11, 3, 7, 10, tzinfo=timezone.utc)


# ── A protobuf encoder, from the specification ───────────────

def varint(value):
    value &= (1 << 64) - 1          # int32 negatives are sign-extended to 64 bits
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def tag(number, wire):
    return varint((number << 3) | wire)


def f_varint(number, value):
    return tag(number, 0) + varint(value)


def f_bytes(number, raw):
    raw = raw.encode() if isinstance(raw, str) else raw
    return tag(number, 2) + varint(len(raw)) + raw


def f_float(number, value):
    return tag(number, 5) + struct.pack("<f", value)


def feed(*entities, stamp=None):
    stamp = stamp if stamp is not None else int(NOW.timestamp())
    header = f_bytes(1, "2.0") + f_varint(2, 0) + f_varint(3, stamp)
    return f_bytes(1, header) + b"".join(entities)


def position(trip, lat, lon, stamp):
    """A VehiclePosition as FlixBus sends one: no heading, an empty vehicle."""
    descriptor = f_bytes(1, trip) + f_varint(4, 0)
    where = f_float(1, lat) + f_float(2, lon)
    body = f_bytes(1, descriptor) + f_bytes(2, where) + f_varint(5, stamp) + f_bytes(8, b"")
    return f_bytes(2, f_bytes(1, "vp-" + trip) + f_bytes(4, body))


def call(seq, stop, arrival=None, departure=None):
    body = f_varint(1, seq)
    if arrival is not None:
        body += f_bytes(2, f_varint(1, arrival))
    if departure is not None:
        body += f_bytes(3, f_varint(1, departure))
    return body + f_bytes(4, stop) + f_varint(5, 0)


def update(trip, start_date, calls, stamp=None):
    stamp = stamp if stamp is not None else int(NOW.timestamp())
    descriptor = f_bytes(1, trip) + f_bytes(3, start_date) + f_varint(4, 0)
    body = (f_bytes(1, descriptor) + b"".join(f_bytes(2, c) for c in calls)
            + f_bytes(3, b"") + f_varint(4, stamp))
    return f_bytes(2, f_bytes(1, "tu-" + trip) + f_bytes(3, body))


# ── The timetable they are read against ──────────────────────

@pytest.fixture(scope="module")
def tt(tmp_path_factory):
    mp = pytest.MonkeyPatch()
    mp.setenv("SKIP_OSRM", "1")
    d = tmp_path_factory.mktemp("flixbus-live")
    fx.write_bods_zip(d / "bods.zip")
    fx.write_flixbus_zip(d / "flixbus.zip")
    built = bt.parse_gtfs(str(d / "bods.zip"))
    fg.merge(built, str(d / "flixbus.zip"), shape_box=bt.SHAPE_BOX)
    j2s.convert(copy.deepcopy(built), d / "timetable.sqlite")
    mp.undo()
    return Timetable(d / "timetable.sqlite", allow_fetch=False)


def _stamp(seconds_ago):
    return int((NOW - timedelta(seconds=seconds_ago)).timestamp())


WINTER_UPDATE = update(fx.T_WINTER, "20261103", [
    call(1, fx.SRBG, departure=120),
    call(2, fx.PRESTON, arrival=300, departure=360),
    call(3, fx.GATWICK_N, arrival=-90),
    call(4, fx.VICTORIA, arrival=-60),
])


# ── Positions ────────────────────────────────────────────────

def test_a_coach_in_the_box_is_kept_and_known_by_its_journey(tt):
    data = feed(position(fx.T_WINTER, 50.8440, -0.1505, _stamp(20)))
    [coach] = flixbus.parse_vehicles(data, tt, NOW, BOX, main.VEHICLE_STALE_SECONDS)
    assert coach["vehicle_ref"] == "FLIX-" + fx.T_WINTER
    assert coach["operator_ref"] == "FLIX"
    assert coach["service_ref"] == "UK066"
    assert coach["destination"] == "London Victoria Coach Station"
    assert coach["declared_trip_id"] == fx.T_WINTER
    assert coach["recorded_at"] == (NOW - timedelta(seconds=20)).isoformat()
    assert coach["bearing"] is None, "FlixBus sends no heading; none is invented"
    assert coach["fleet_known"] is False, "the ref is a journey, not a fleet number"


def test_coaches_outside_the_box_or_gone_quiet_are_dropped(tt):
    data = feed(
        position(fx.T_GATWICK, 51.1599, -0.1750, _stamp(10)),          # Gatwick
        position(fx.T_INBOUND, 50.8330, -0.1420, _stamp(main.VEHICLE_STALE_SECONDS + 60)),
    )
    assert flixbus.parse_vehicles(data, tt, NOW, BOX, main.VEHICLE_STALE_SECONDS) == []


def test_a_coach_on_a_journey_we_do_not_hold_still_shows_its_line(tt):
    data = feed(position("UK998-9-0800102026-MKCS#SRBG-00", 50.8400, -0.1500, _stamp(5)))
    [coach] = flixbus.parse_vehicles(data, tt, NOW, BOX, main.VEHICLE_STALE_SECONDS)
    assert coach["service_ref"] == "UK998"
    assert "declared_trip_id" not in coach


def test_a_position_on_a_split_journey_takes_the_half_running_that_day(tt):
    after_change = datetime(2027, 3, 29, 6, 10, tzinfo=timezone.utc)
    data = feed(position(fx.T_SPANS, 50.8440, -0.1505, int(after_change.timestamp()) - 10),
                stamp=int(after_change.timestamp()))
    [coach] = flixbus.parse_vehicles(data, tt, after_change, BOX, main.VEHICLE_STALE_SECONDS)
    assert coach["declared_trip_id"] == fx.T_SPANS + "@1"


def test_journey_for_follows_the_date(tt):
    assert flixbus.journey_for(tt, fx.T_SPANS, date(2027, 3, 27)) == fx.T_SPANS
    assert flixbus.journey_for(tt, fx.T_SPANS, date(2027, 3, 29)) == fx.T_SPANS + "@1"
    assert flixbus.journey_for(tt, fx.T_WINTER, date(2026, 11, 3)) == fx.T_WINTER
    assert flixbus.journey_for(tt, "UK000-unknown", date(2026, 11, 3)) is None


# ── Delays ───────────────────────────────────────────────────

def test_trip_updates_keep_held_journeys_and_name_our_stops(tt):
    data = feed(WINTER_UPDATE,
                update(fx.T_ELSEWHERE, "20261103", [call(1, fx.VICTORIA, departure=600)]),
                update("NOT-HELD", "20261103", [call(1, fx.SRBG, departure=600)]))
    updates = flixbus.parse_trip_updates(data, flixbus.held_trip_ids(tt), tt.stop_aliases)
    assert set(updates) == {fx.T_WINTER}
    assert updates[fx.T_WINTER]["start_date"] == "20261103"
    # Departure over arrival; Victoria is not a stop of ours; early is negative.
    assert updates[fx.T_WINTER]["delays"] == {BRIGHTON: 120, PRESTON: 360, GATWICK_BAY: -90}


def _row(trip, aimed, operator="FLIX", **extra):
    return {"service": "UK066" if operator == "FLIX" else "700", "operator": operator,
            "destination": "x", "aimed_departure": aimed, "expected_departure": None,
            "status": "Scheduled", "delay_seconds": None, "_trip_id": trip, **extra}


def _updates(tt, *entities):
    return flixbus.parse_trip_updates(feed(*entities), flixbus.held_trip_ids(tt), tt.stop_aliases)


def test_a_board_row_takes_the_delay_flixbus_gives(tt):
    payload = {"departures": [_row(fx.T_WINTER, "2026-11-03T07:05:00+00:00"),
                              _row("T_LOCAL", "2026-11-03T07:06:00+00:00", operator="SCSO")]}
    out = main._apply_flixbus_delays(payload, tt, PRESTON, _updates(tt, WINTER_UPDATE))
    coach, local = out["departures"]
    assert coach["expected_departure"] == "2026-11-03T07:11:00+00:00"
    assert coach["delay_seconds"] == 360
    assert coach["status"] == "Late"
    assert coach["live_source"] == "prediction", "FlixBus's own system, like any operator's"
    assert out["live"] is True
    assert local == payload["departures"][1], "a local bus was touched"


def test_a_delay_for_another_day_is_not_applied(tt):
    payload = {"departures": [_row(fx.T_WINTER, "2026-11-03T07:00:00+00:00")]}
    tomorrow = update(fx.T_WINTER, "20261104", [call(1, fx.SRBG, departure=600)])
    out = main._apply_flixbus_delays(payload, tt, BRIGHTON, _updates(tt, tomorrow))
    assert out["departures"][0]["expected_departure"] is None


def test_a_row_that_already_has_a_prediction_keeps_it(tt):
    row = _row(fx.T_WINTER, "2026-11-03T07:00:00+00:00",
               expected_departure="2026-11-03T07:01:00+00:00", live_source="prediction")
    out = main._apply_flixbus_delays({"departures": [row]}, tt, BRIGHTON,
                                     _updates(tt, WINTER_UPDATE))
    assert out["departures"][0]["expected_departure"] == "2026-11-03T07:01:00+00:00"


def test_a_coach_is_as_late_as_flixbus_says_at_its_next_stop(tt):
    # Between Brighton station and Preston Park, heading north: Preston Park
    # is next, where FlixBus has it six minutes down.
    coach = flixbus.parse_vehicles(feed(position(fx.T_WINTER, 50.8440, -0.1505, _stamp(20))),
                                   tt, NOW, BOX, main.VEHICLE_STALE_SECONDS)
    main._attach_declared_journeys(coach, tt, NOW.astimezone(main.UK_TZ))
    assert coach[0]["trip_source"] == "feed"
    flixbus.apply_operator_lateness(coach, tt, _updates(tt, WINTER_UPDATE))
    assert coach[0]["lateness_secs"] == 360
    assert coach[0]["waiting_to_start"] is False


# ── Wired into the API ───────────────────────────────────────

@pytest.fixture
def wired(tt, monkeypatch):
    """The API with this timetable, a frozen clock and canned FlixBus feeds."""
    class Frozen(main.datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is None else NOW.astimezone(tz)

    monkeypatch.setattr(main, "datetime", Frozen)
    monkeypatch.setattr(main, "_cache", {})
    monkeypatch.setattr(main, "NEXTBUSES_APP_ID", "")

    async def timetable():
        return tt
    monkeypatch.setattr(main, "_get_timetable", timetable)

    async def nothing(*_a, **_k):
        return None
    monkeypatch.setattr(main, "_diversions_doc", nothing)
    monkeypatch.setattr(main, "_recovery_doc", nothing)
    monkeypatch.setattr(main, "_disruptions_state", nothing)

    feeds = {flixbus.TRIP_UPDATES_URL: feed(WINTER_UPDATE),
             flixbus.VEHICLES_URL: feed(position(fx.T_WINTER, 50.8440, -0.1505, _stamp(20)))}
    asked = []

    async def fetch(url):
        asked.append(url)
        return feeds.get(url)
    monkeypatch.setattr(main, "_fetch_flixbus_feed", fetch)
    return {"feeds": feeds, "asked": asked}


def test_the_board_shows_flixbus_delays(wired):
    board = asyncio.run(main.get_departures(stopId=PRESTON, lat=None, lon=None))
    [coach] = [d for d in board["departures"] if d["operator"] == "FLIX"]
    assert coach["expected_departure"] == "2026-11-03T07:11:00+00:00"
    assert coach["live_source"] == "prediction"


def test_a_board_without_coaches_never_asks_flixbus(wired):
    asyncio.run(main.get_departures(stopId="4400SH01", lat=None, lon=None))
    assert flixbus.TRIP_UPDATES_URL not in wired["asked"]


def test_flixbus_coaches_join_the_live_buses(wired, monkeypatch):
    async def bods():
        return [{"vehicle_ref": "SCSO-1", "operator_ref": "SCSO", "service_ref": "700",
                 "latitude": 50.832, "longitude": -0.275, "recorded_at": NOW.isoformat(),
                 "calls": []}]

    async def declared():
        return {}
    monkeypatch.setattr(main, "_fetch_siri_vm", bods)
    monkeypatch.setattr(main, "_fetch_declared_journeys", declared)
    out = asyncio.run(main._fetch_and_match_vehicles())
    refs = {v["vehicle_ref"]: v for v in out["vehicles"]}
    assert "SCSO-1" in refs
    coach = refs["FLIX-" + fx.T_WINTER]
    assert coach["trip_source"] == "feed"
    assert coach["lateness_secs"] == 360, "the delay FlixBus gives, not a guess"


def test_a_flixbus_outage_leaves_the_buses_alone(wired, monkeypatch):
    wired["feeds"].clear()

    async def bods():
        return [{"vehicle_ref": "SCSO-1", "operator_ref": "SCSO", "service_ref": "700",
                 "latitude": 50.832, "longitude": -0.275, "recorded_at": NOW.isoformat(),
                 "calls": []}]

    async def declared():
        return {}
    monkeypatch.setattr(main, "_fetch_siri_vm", bods)
    monkeypatch.setattr(main, "_fetch_declared_journeys", declared)
    out = asyncio.run(main._fetch_and_match_vehicles())
    assert [v["vehicle_ref"] for v in out["vehicles"]] == ["SCSO-1"]
