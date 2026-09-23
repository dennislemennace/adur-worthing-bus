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


def parse_feed(data):
    """`(header, vehicles)` from a GTFS-RT FeedMessage.

    FeedMessage: 1 header, 2 entity. FeedEntity: 1 id, 4 vehicle. Entities
    carrying anything else — trip updates, service alerts — are skipped,
    because this reads the vehicle feed.
    """
    header, vehicles = {}, []
    for number, _wire, value in fields(memoryview(data)):
        if number == 1:
            for h_number, _h_wire, h_value in fields(value):
                if h_number == 1:
                    header["version"] = _text(h_value)
                elif h_number == 2:
                    header["incrementality"] = h_value
                elif h_number == 3:
                    header["timestamp"] = h_value
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
