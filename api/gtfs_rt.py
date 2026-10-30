"""Read the fields we need from a GTFS-RT feed, without a protobuf dependency.

`gtfs-realtime-bindings` is the obvious way to do this, and it does not work
here: its protobuf build fails to import on Python 3.14 (`Metaclasses with
custom tp_new are not supported`) while CI runs 3.12. A dependency that works
in only one of the two places we run tests is worse than none.

The wire format is simple enough to read directly, and the subset we need is
small: which journey a bus is running, where it is, which stop it is at, and
when the operator says so. Everything else is skipped by length.

**Field numbers come from the GTFS-RT specification and are checked against
reality rather than trusted.** `scripts/probe_gtfs_rt.py` verifies that decoded
positions land inside the recorded bounding box and that vehicle ids match the
SIRI-VM snapshot of the same minute. A wrong field number fails those loudly
instead of producing plausible nonsense.
"""

import struct

# Wire types, from the protobuf encoding.
VARINT, FIXED64, LENGTH, FIXED32 = 0, 1, 2, 5


def _varint(buf, i):
    """A base-128 varint, and where it ends."""
    value = shift = 0
    while True:
        byte = buf[i]
        i += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, i
        shift += 7


def fields(buf, start=0, end=None):
    """Walk one message, yielding `(field number, wire type, value)`.

    Length-delimited values come back as raw bytes — a nested message, a string
    or a packed array, depending on the field. The caller decides which.
    """
    end = len(buf) if end is None else end
    i = start
    while i < end:
        key, i = _varint(buf, i)
        number, wire = key >> 3, key & 7
        if wire == VARINT:
            value, i = _varint(buf, i)
        elif wire == LENGTH:
            size, i = _varint(buf, i)
            value, i = buf[i:i + size], i + size
        elif wire == FIXED32:
            value, i = struct.unpack_from("<f", buf, i)[0], i + 4
        elif wire == FIXED64:
            value, i = struct.unpack_from("<d", buf, i)[0], i + 8
        else:
            raise ValueError(f"unsupported wire type {wire} for field {number}")
        yield number, wire, value


def _text(raw):
    return bytes(raw).decode("utf-8", "replace")


def _trip(raw):
    """TripDescriptor wire fields: 1 trip_id, 2 start_time, 3 start_date,
    4 schedule_relationship, 5 route_id (GTFS-realtime protobuf)."""
    out = {}
    for number, _wire, value in fields(raw):
        if number == 1:
            out["trip_id"] = _text(value)
        elif number == 5:
            out["route_id"] = _text(value)
        elif number == 2:
            out["start_time"] = _text(value)
        elif number == 3:
            out["start_date"] = _text(value)
        elif number == 4:
            out["schedule_relationship"] = value
    return out


def _position(raw):
    """Position: 1 latitude, 2 longitude, 3 bearing, 5 speed — float32."""
    out = {}
    for number, _wire, value in fields(raw):
        if number == 1:
            out["latitude"] = value
        elif number == 2:
            out["longitude"] = value
        elif number == 3:
            out["bearing"] = value
        elif number == 5:
            out["speed"] = value
    return out


def _vehicle_descriptor(raw):
    """VehicleDescriptor: 1 id, 2 label, 3 licence plate."""
    out = {}
    for number, _wire, value in fields(raw):
        if number == 1:
            out["vehicle_id"] = _text(value)
        elif number == 2:
            out["vehicle_label"] = _text(value)
    return out


def _vehicle_position(raw):
    """VehiclePosition: 1 trip, 2 position, 3 current_stop_sequence,
    4 current_status, 5 timestamp, 7 stop_id, 8 vehicle."""
    out = {}
    for number, _wire, value in fields(raw):
        if number == 1:
            out.update(_trip(value))
        elif number == 2:
            out.update(_position(value))
        elif number == 3:
            out["current_stop_sequence"] = value
        elif number == 4:
            out["current_status"] = value
        elif number == 5:
            out["timestamp"] = value
        elif number == 7:
            out["stop_id"] = _text(value)
        elif number == 8:
            out.update(_vehicle_descriptor(value))
    return out


def _header(raw):
    """FeedHeader: 1 version, 2 incrementality, 3 timestamp."""
    header = {}
    for number, _wire, value in fields(raw):
        if number == 1:
            header["version"] = _text(value)
        elif number == 2:
            header["incrementality"] = value
        elif number == 3:
            header["timestamp"] = value
    return header


def _signed(value):
    """An int32 or int64 read as a varint: a negative arrives as 2**64 less it."""
    return value - (1 << 64) if value >= 1 << 63 else value


def _stop_time_event(raw):
    """StopTimeEvent: 1 delay (seconds, signed), 2 time (epoch seconds)."""
    out = {}
    for number, _wire, value in fields(raw):
        if number == 1:
            out["delay"] = _signed(value)
        elif number == 2:
            out["time"] = _signed(value)
    return out


def _stop_time_update(raw):
    """StopTimeUpdate: 1 stop_sequence, 2 arrival, 3 departure, 4 stop_id,
    5 schedule_relationship (0 scheduled, 1 skipped, 2 no data)."""
    out = {"schedule_relationship": 0}
    for number, _wire, value in fields(raw):
        if number == 1:
            out["stop_sequence"] = value
        elif number == 2:
            out["arrival"] = _stop_time_event(value)
        elif number == 3:
            out["departure"] = _stop_time_event(value)
        elif number == 4:
            out["stop_id"] = _text(value)
        elif number == 5:
            out["schedule_relationship"] = value
    return out


def _trip_update(raw, wanted):
    """TripUpdate: 1 trip, 2 stop_time_update, 3 vehicle, 4 timestamp.

    A journey not in `wanted` is dropped as soon as its trip is read, before
    its stops, which is most of the work: serialisers write fields in number
    order, so the trip comes first.
    """
    trip, calls, stamp = None, [], None
    for number, _wire, value in fields(raw):
        if number == 1:
            trip = _trip(value)
            if wanted is not None and trip.get("trip_id") not in wanted:
                return None
        elif number == 2:
            calls.append(value)
        elif number == 4:
            stamp = value
    if trip is None or (wanted is not None and trip.get("trip_id") not in wanted):
        return None
    return {**trip, "timestamp": stamp,
            "stop_time_updates": [_stop_time_update(c) for c in calls]}


def parse_trip_updates(data, wanted=None):
    """`(header, updates)` from a GTFS-RT FeedMessage's trip updates.

    FeedEntity 3 is a TripUpdate. `wanted`, a set of trip ids, keeps only those
    journeys. FlixBus's European feed is 800 KB; reading only the British
    journeys we hold took 4 ms against 72 ms for all of it (29 October 2026).
    """
    header, updates = {}, []
    for number, _wire, value in fields(memoryview(data)):
        if number == 1:
            header = _header(value)
        elif number == 2:
            for e_number, _e_wire, e_value in fields(value):
                if e_number == 3:
                    update = _trip_update(e_value, wanted)
                    if update:
                        updates.append(update)
    return header, updates


def parse_feed(data):
    """`(header, vehicles)` from a GTFS-RT FeedMessage.

    FeedMessage: 1 header, 2 entity. FeedEntity: 1 id, 4 vehicle. Entities
    carrying anything else — trip updates, service alerts — are skipped,
    because this reads the vehicle feed.
    """
    header, vehicles = {}, []
    for number, _wire, value in fields(memoryview(data)):
        if number == 1:
            header = _header(value)
        elif number == 2:
            entity = {}
            for e_number, _e_wire, e_value in fields(value):
                if e_number == 1:
                    entity["id"] = _text(e_value)
                elif e_number == 4:
                    entity.update(_vehicle_position(e_value))
            if "latitude" in entity:
                vehicles.append(entity)
    return header, vehicles
