"""Reading GTFS-RT without a protobuf library.

`api/gtfs_rt.py` exists because `gtfs-realtime-bindings` cannot be imported
on Python 3.14 while CI runs 3.12. Hand-reading a wire format is the kind of
decision that earns a sceptical test suite, so this builds messages with an
*encoder written independently of the decoder* — a decoder checked against its
own assumptions proves nothing.

The strongest evidence is not here, though: it is in
`docs/reliability/gtfs-rt-probe.md`, where all 259 decoded positions landed
inside the recorded bounding box and agreed with the SIRI-VM snapshot of the
same minute to a median of 0 metres. A wrong field number cannot survive that.
"""

import struct
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api.gtfs_rt import parse_feed, fields                       # noqa: E402


# ── A protobuf encoder, written from the spec, not from the decoder ──

def varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def tag(number, wire):
    return varint((number << 3) | wire)


def field_varint(number, value):
    return tag(number, 0) + varint(value)


def field_bytes(number, raw):
    raw = raw.encode() if isinstance(raw, str) else raw
    return tag(number, 2) + varint(len(raw)) + raw


def field_float(number, value):
    return tag(number, 5) + struct.pack("<f", value)


def a_vehicle(trip="VJ_abc", route="R1", lat=50.8312, lon=-0.2701, bearing=256.0,
              stamp=1_789_731_900, vehicle="BUS-1", sequence=None, status=None,
              stop=None):
    """One VehiclePosition, as the specification lays it out."""
    trip_msg = field_bytes(1, trip) + field_bytes(5, route)
    position = field_float(1, lat) + field_float(2, lon) + field_float(3, bearing)
    body = field_bytes(1, trip_msg) + field_bytes(2, position)
    if sequence is not None:
        body += field_varint(3, sequence)
    if status is not None:
        body += field_varint(4, status)
    body += field_varint(5, stamp)
    if stop is not None:
        body += field_bytes(7, stop)
    body += field_bytes(8, field_bytes(1, vehicle))
    return body


def a_feed(entities, stamp=1_789_731_920):
    header = field_bytes(1, "2.0") + field_varint(2, 0) + field_varint(3, stamp)
    out = field_bytes(1, header)
    for i, entity in enumerate(entities):
        out += field_bytes(2, field_bytes(1, f"entity-{i}") + field_bytes(4, entity))
    return out


# ── What it must read ───────────────────────────────────────

def test_a_vehicle_comes_back_whole():
    header, vehicles = parse_feed(a_feed([a_vehicle(sequence=12, status=1,
                                                    stop="4400AD0203")]))
    assert header == {"version": "2.0", "incrementality": 0, "timestamp": 1_789_731_920}
    assert len(vehicles) == 1
    v = vehicles[0]
    assert v["trip_id"] == "VJ_abc", "the journey identity is the whole point of this feed"
    assert v["route_id"] == "R1"
    assert v["vehicle_id"] == "BUS-1"
    assert v["current_stop_sequence"] == 12
    assert v["current_status"] == 1
    assert v["stop_id"] == "4400AD0203"
    assert v["timestamp"] == 1_789_731_900
    assert round(v["latitude"], 4) == 50.8312 and round(v["longitude"], 4) == -0.2701
    assert round(v["bearing"]) == 256


def test_positions_are_read_as_float32_not_as_something_else():
    # A field read at the wrong wire type gives plausible nonsense rather than
    # an error, which is the failure this decoder has to survive.
    _header, vehicles = parse_feed(a_feed([a_vehicle(lat=50.87, lon=-0.42)]))
    v = vehicles[0]
    assert 50.8 < v["latitude"] < 50.9, f"latitude decoded as {v['latitude']}"
    assert -0.5 < v["longitude"] < -0.3, f"longitude decoded as {v['longitude']}"


def test_several_vehicles_all_arrive():
    feed = a_feed([a_vehicle(trip="VJ_1", vehicle="A"),
                   a_vehicle(trip="VJ_2", vehicle="B"),
                   a_vehicle(trip="VJ_3", vehicle="C")])
    _header, vehicles = parse_feed(feed)
    assert [v["vehicle_id"] for v in vehicles] == ["A", "B", "C"]
    assert [v["trip_id"] for v in vehicles] == ["VJ_1", "VJ_2", "VJ_3"]


# ── What it must ignore ─────────────────────────────────────

def test_fields_we_do_not_know_are_skipped_not_guessed_at():
    # Feeds gain fields. An unknown one must be stepped over by length, not
    # read as whatever field happens to share its number in our subset.
    padded = a_vehicle() + field_bytes(99, "something new") + field_varint(50, 12345)
    _header, vehicles = parse_feed(a_feed([padded]))
    assert vehicles[0]["trip_id"] == "VJ_abc"
    assert vehicles[0]["vehicle_id"] == "BUS-1"


def test_an_entity_with_no_position_is_not_a_vehicle():
    # Trip updates and service alerts share the feed. This reads the vehicle
    # feed, and an entity without a position is not one.
    trip_update_only = field_bytes(1, field_bytes(1, "VJ_x"))
    feed = a_feed([a_vehicle(vehicle="REAL"), trip_update_only])
    _header, vehicles = parse_feed(feed)
    assert [v["vehicle_id"] for v in vehicles] == ["REAL"]


def test_an_empty_feed_is_not_an_error():
    header, vehicles = parse_feed(a_feed([]))
    assert vehicles == []
    assert header["version"] == "2.0"


def test_a_broken_wire_type_is_refused_rather_than_guessed():
    # Wire types 3 and 4 are the deprecated groups. Reading one as anything
    # would put invented values into published figures.
    try:
        list(fields(tag(1, 3) + b"\x01"))
    except ValueError as err:
        assert "wire type" in str(err)
    else:
        raise AssertionError("a group field was decoded as if it were understood")
