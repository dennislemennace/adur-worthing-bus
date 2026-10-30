"""FlixBus's live feeds: where its coaches are, and how late FlixBus says they are.

FlixBus publishes no locations to BODS, so until now its coaches were never on
the map and every FlixBus row on a board was timetable only: on 26 October
2026 all 244 FlixBus calls scored "no bus seen". FlixBus France publishes two
GTFS-RT feeds of its own on France's national access point,
transport.data.gouv.fr, under the Open Database Licence and with no key: trip
updates (the listed resource) and vehicle positions (the same API, one path
along). The site credits FlixBus and the licence wherever it lists its sources
(tests/test_data_credits.py).

What they carry, surveyed on 29 October 2026 (1,406 journeys and 1,677
coaches across Europe, about 160 and 230 of them British):

  * Trip updates: a delay in seconds at every stop, for arrival and departure,
    never a time; stops named by FlixBus's own ids; the date the journey
    started. Nothing cancelled or skipped was seen.
  * Positions: latitude, longitude, the moment, and the journey. No vehicle
    id, no heading, and no start date.

So a coach is known by its journey ("FLIX-" + trip id) and says so
(`fleet_known`), and FlixBus's stop ids become ours through the timetable's
`stop_aliases` (scripts/flixbus_gtfs.py). Both are read with api/gtfs_rt.py.
"""

from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from api import gtfs_rt, trip_match

FEED_BASE = ("https://global.api.flixbus.com/gis/v1/gtfs-rt/flavour/generic/"
             "region/eu/agency/bus_train")
TRIP_UPDATES_URL = f"{FEED_BASE}/trip-update"
VEHICLES_URL = f"{FEED_BASE}/vehicle-position"
NOC = "FLIX"
REF_PREFIX = "FLIX-"
# scripts/flixbus_gtfs.py names the half of a journey after a clock change
# "<trip id>@1"; the feeds only ever say "<trip id>".
SPLIT_MARK = "@"
LONDON = ZoneInfo("Europe/London")


def raw_trip_id(trip_id: str) -> str:
    """FlixBus's id for one of our journeys."""
    return trip_id.split(SPLIT_MARK, 1)[0]


def held_trip_ids(tt) -> frozenset:
    """FlixBus's ids for the journeys this timetable holds. Worked out once
    per loaded timetable: it walks every trip."""
    cached = getattr(tt, "_flixbus_trip_ids", None)
    if cached is not None and cached[0] is tt.trips:
        return cached[1]
    ids = frozenset(raw_trip_id(tid) for tid, trip in tt.trips.items()
                    if tt.noc_for_route(trip.get("route_id", "")) == NOC)
    tt._flixbus_trip_ids = (tt.trips, ids)
    return ids


def journey_for(tt, trip_id: str, day: date, *more_days: date) -> Optional[str]:
    """Our id for FlixBus's journey on `day` (or the first of `more_days` it
    runs on), or None if we do not hold it."""
    variants = [t for t in (trip_id, *(f"{trip_id}{SPLIT_MARK}{i}" for i in (1, 2)))
                if t in tt.trips]
    if len(variants) <= 1:
        return variants[0] if variants else None
    for d in (day, *more_days):
        for v in variants:
            if tt.runs_on(tt.trips[v]["service_id"], d):
                return v
    return variants[0]


def parse_vehicles(data: bytes, tt, now: datetime, box, stale_secs: int) -> list:
    """FlixBus's coaches inside `box` (min_lat, max_lat, min_lon, max_lon),
    shaped like the vehicles api/main.py reads from SIRI-VM.

    A position more than `stale_secs` old is dropped, as a bus's is: coaches
    parked at Gatwick went on reporting where they stood for over an hour.
    """
    _header, positions = gtfs_rt.parse_feed(data)
    today = now.astimezone(LONDON).date()
    out: dict = {}
    for p in positions:
        trip = p.get("trip_id") or ""
        lat, lon, stamp = p.get("latitude"), p.get("longitude"), p.get("timestamp")
        if not trip or lat is None or lon is None or not stamp:
            continue
        if not (box[0] <= lat <= box[1] and box[2] <= lon <= box[3]):
            continue
        at = datetime.fromtimestamp(stamp, timezone.utc)
        if (now - at).total_seconds() > stale_secs:
            continue
        # No start date on a position: today's journey, or yesterday's still
        # running past midnight.
        ours = journey_for(tt, trip, today, today - timedelta(days=1))
        held = tt.trips.get(ours) if ours else None
        route = tt.routes.get(held.get("route_id", "")) if held else None
        coach = {
            "vehicle_ref": REF_PREFIX + trip,
            # The line is the trip id's first part ("UK066-31-…"), for a
            # journey newer than this week's timetable.
            "service_ref": (route or {}).get("short_name") or trip.split("-", 1)[0],
            "operator_ref": NOC,
            "destination": (held or {}).get("headsign", ""),
            "origin_ref": "",
            "origin_aimed_departure": "",
            "destination_ref": "",
            "latitude": lat,
            "longitude": lon,
            "bearing": None,
            "delay_seconds": None,
            "recorded_at": at.isoformat(),
            "calls": [],
            "fleet_known": False,
        }
        if ours:
            coach["declared_trip_id"] = ours
        before = out.get(coach["vehicle_ref"])
        if before is None or before["recorded_at"] < coach["recorded_at"]:
            out[coach["vehicle_ref"]] = coach
    return list(out.values())


def parse_trip_updates(data: bytes, wanted, aliases: dict) -> dict:
    """`{FlixBus trip id: {"start_date", "delays": {our stop: seconds}}}`.

    Only journeys in `wanted`, and only stops we hold. The departure delay
    where there is one, since a board shows departures; the arrival's at a
    stop where the coach only arrives. A skipped stop, or one FlixBus has no
    data for, gives no delay rather than a wrong one.
    """
    _header, updates = gtfs_rt.parse_trip_updates(data, wanted)
    out = {}
    for update in updates:
        delays = {}
        for call in update["stop_time_updates"]:
            ours = aliases.get(call.get("stop_id"))
            if not ours or call.get("schedule_relationship", 0) != 0:
                continue
            for event in (call.get("departure"), call.get("arrival")):
                if event and "delay" in event:
                    delays[ours] = event["delay"]
                    break
        out.setdefault(update["trip_id"], {"start_date": update.get("start_date", ""),
                                           "delays": delays})
    return out


def apply_operator_lateness(vehicles: list, tt, updates: dict) -> None:
    """Each coach on a journey we hold is as late as FlixBus says it is at
    the next of its stops here, in place of our guess from its position.

    The guess is weak for a coach: our timetable has only the stops in our
    area, so one on the A23 is measured against Preston Park kilometres ahead,
    and an early one looks like a bus waiting at its first stop.
    """
    for v in vehicles:
        if v.get("operator_ref") != NOC or v.get("trip_source") not in trip_match.DECLARED_SOURCES:
            continue
        update = updates.get(raw_trip_id(v.get("trip_id") or ""))
        if not update or not update["delays"]:
            continue
        calls = [(secs, stop) for secs, stop in tt.trip_stops_for(v["trip_id"]) if secs is not None]
        idx = trip_match.nearest_call(tt, {"calls": calls}, v.get("latitude"), v.get("longitude"))
        if idx is None:
            continue
        if trip_match.past_pole(tt, v, calls, idx):
            idx += 1
        delay = update["delays"].get(calls[idx][1])
        if delay is None:
            continue
        v["lateness_secs"] = delay
        v["waiting_to_start"] = False
        v["lateness_source"] = "operator"
