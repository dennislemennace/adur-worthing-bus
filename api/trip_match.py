"""Deciding which scheduled journey a tracked bus is running.

The feed says where a bus is, which service it calls itself, and little else.
It does not say which departure it is: `DatedVehicleJourneyRef` carries the
operator's own journey number ("39"), which matches no GTFS trip id here — 0 of
256 in a live sample. So the journey has to be inferred from position and time,
and everything the site measures rests on that inference.

This module is the inference, shared by the two things that need it:

* `corridor_gaps.py`, live, for the next hour at three poles in each direction;
* the nightly processor, over a day of recorded snapshots, for punctuality.

They must agree. A live alert the published figures could not reproduce would be
the first thing an operator picks apart, so the matching lives in one place and
is tested once.

**How a bus is placed.** For each journey the bus could be running by service
number, find that journey's nearest scheduled stop to the bus. Reject it if the
bus is further than 400 m from every stop on it, or if the journey is due at
that stop more than 25 minutes ago or more than 5 minutes from now. The
asymmetry is deliberate: buses run late far more often than early, and a bus
placed on a departure that has not left yet invents a gap where none exists.
Where the feed sends a heading, a journey going the other way along the road is
rejected too — the two poles of a coast-road stop are metres apart.

Then each bus takes at most one journey and each journey at most one bus, best
fit first, so two buses reporting the same position cannot both claim it.
"""

import math
import re
from datetime import timedelta

# Late by up to 25 minutes; early by no more than 5. See the note above.
MATCH_TOLERANCE_SECS = 25 * 60
EARLY_TOLERANCE_SECS = 5 * 60
# A bus further than this from every stop on a journey is not running it.
MAX_OFF_ROUTE_KM = 0.4

# Coaches and airport shuttles call at these poles but are not local buses:
# they need their own ticket and are not what anyone at a bus stop is waiting for.
COACH_NOCS = frozenset({"NATX", "FLIX", "OXBC", "GHOP", "BMCS", "UNTM"})


def clock(secs: int) -> str:
    secs = int(secs) % 86400
    return f"{secs // 3600:02d}:{secs % 3600 // 60:02d}"


def km(lat1, lon1, lat2, lon2) -> float:
    dlat = (lat2 - lat1) * 111.0
    dlon = (lon2 - lon1) * 111.0 * math.cos(math.radians((lat1 + lat2) / 2))
    return math.hypot(dlat, dlon)


def bearing(lat1, lon1, lat2, lon2) -> float:
    dlon = (lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
    return math.degrees(math.atan2(dlon, lat2 - lat1)) % 360


def service_keys(name: str) -> set:
    """"025" and "25", "N700" and "700": the feed and the timetable disagree."""
    name = (name or "").strip().upper()
    if not name:
        return set()
    zero = name.lstrip("0") or name
    keys = {name, zero}
    for k in list(keys):
        if len(k) > 1 and k[0] == "N" and k[1:].isdigit():
            keys.add(k[1:])
    return keys


def service_days(today):
    """Today, and yesterday's service still running past midnight.

    GTFS writes half past midnight as 24:30 on the previous day's service, so a
    bus on the road at 00:30 belongs to yesterday. The shift converts such a
    time to seconds from today's midnight, where it can be compared with now.
    """
    return ((today, 0), (today - timedelta(days=1), 86400))


def build_instances(tt, today, atcos, window):
    """Every scheduled call at `atcos` inside `window`, and the journeys making them.

    Returns `(instances, scheduled)`. A journey instance is keyed by
    `(trip_id, service day)` because the same trip id can run on both days at
    once around midnight, and those are different buses.

    `window` is `(lo, hi)` in seconds from today's midnight; `atcos` the stops
    worth looking at. Coaches are left out entirely.
    """
    lo, hi = window
    days = service_days(today)
    runs: dict = {}

    def running(service_id, day):
        key = (service_id, day)
        if key not in runs:
            runs[key] = tt.runs_on(service_id, day)
        return runs[key]

    instances: dict = {}
    scheduled: dict = {atco: [] for atco in atcos}
    for atco in atcos:
        for secs, trip_id in tt.stop_times_for(atco):
            if secs is None:
                continue
            trip = tt.trips.get(trip_id) or {}
            if tt.noc_for_route(trip.get("route_id", "")) in COACH_NOCS:
                continue
            for day, shift in days:
                t = secs - shift
                if not (lo <= t <= hi):
                    continue
                if not running(trip.get("service_id", ""), day):
                    continue
                key = (trip_id, day)
                scheduled[atco].append((t, key))
                instances.setdefault(key, {"shift": shift, "trip": trip})
    for atco in scheduled:
        scheduled[atco].sort()

    for key, inst in instances.items():
        calls = tt.trip_stops_for(key[0])
        inst["calls"] = [(secs - inst["shift"], atco) for secs, atco in calls
                         if secs is not None]
        route = tt.routes.get(inst["trip"].get("route_id", "")) or {}
        inst["service"] = route.get("short_name", "")
        inst["keys"] = service_keys(inst["service"])
        inst["first"] = inst["calls"][0][0] if inst["calls"] else None
    return instances, scheduled


def place_declared(tt, vehicles, instances, now):
    """Place the buses that say which journey they are running.

    GTFS-RT carries a `trip_id`. Measured over our box, 256 of 259 vehicles
    carry one and 184 name a journey we hold — against 0 of 256 for SIRI-VM's
    own journey reference, which is why everything else here infers.

    A declared journey needs no tolerance window, so nothing it produces is
    censored: a bus 40 minutes late is 40 minutes late, not a bus running early
    on the next departure. Returns `(placed, claimed vehicle indices)`.

    The same trip id can exist on two service days at once around midnight, so
    the day whose scheduled span sits nearest `now` wins.
    """
    by_trip = {}
    for key in instances:
        by_trip.setdefault(key[0], []).append(key)

    placed, claimed = {}, set()
    for vi, v in enumerate(vehicles):
        trip = (v.get("trip_id") or "").strip()
        candidates = by_trip.get(trip)
        if not trip:
            continue
        if v.get("schedule_relationship", 0) not in (None, 0):
            claimed.add(vi)  # new/cancelled instance has no usable static schedule
            continue
        if not candidates:
            # Still usable for exploratory inference, never certified as declared.
            v.setdefault("identity_flags", []).append("unresolved_declared_trip")
            continue
        start_time = v.get("start_time")
        if start_time:
            valid = re.fullmatch(r"(\d+):([0-5]\d):([0-5]\d)", start_time)
            calls = tt.trip_stops_for(trip)
            start_secs = sum(int(x) * unit for x, unit in zip(valid.groups(), (3600, 60, 1))) if valid else None
            if not calls or start_secs != calls[0][0]:
                claimed.add(vi)  # frequency/replacement instance is not in this static timetable
                continue
        start_date = (v.get("start_date") or "").replace("-", "")
        if start_date:
            candidates = [k for k in candidates if k[1].strftime("%Y%m%d") == start_date]
            if not candidates:
                claimed.add(vi)  # named a different instance: do not silently infer one
                continue
        report_now = v.get("recorded_secs")
        report_now = now if report_now is None else report_now
        key = min(candidates, key=lambda k: _span_distance(instances[k], report_now))
        if key in placed:
            claimed.add(vi)
            other = vehicles[placed[key][0]]
            if v.get("vehicle_ref") != other.get("vehicle_ref"):
                v.setdefault("identity_flags", []).append("conflicting_declared_vehicles")
                other.setdefault("identity_flags", []).append("conflicting_declared_vehicles")
            continue                      # two buses claiming one journey
        idx = nearest_call(tt, instances[key], v.get("latitude"), v.get("longitude"))
        if idx is None:
            continue
        placed[key] = (vi, idx)
        claimed.add(vi)
    return placed, claimed


def _span_distance(inst, now):
    """How far `now` sits outside a journey's scheduled span. Zero if inside."""
    calls = inst.get("calls") or []
    if not calls:
        return 86_400
    first, last = calls[0][0], calls[-1][0]
    if now < first:
        return first - now
    if now > last:
        return now - last
    return 0


def nearest_call(tt, inst, lat, lon):
    """Which of a journey's calls the bus is closest to, or None."""
    if lat is None or lon is None:
        return None
    best_i, best_d = None, None
    for i, (_secs, atco) in enumerate(inst.get("calls") or []):
        stop = tt.stops.get(atco) or {}
        if stop.get("lat") is None:
            continue
        d = km(lat, lon, stop["lat"], stop["lon"])
        if best_d is None or d < best_d:
            best_i, best_d = i, d
    return best_i


def place_vehicles(tt, vehicles, instances, now, times=None,
                   skip_vehicles=(), skip_journeys=()) -> dict:
    """Which journey each tracked bus is running, at the moment `now`.

    Returns `{(trip_id, day): (vehicle index, index of its nearest call)}`. Both
    sides are exclusive: one bus, one journey.

    `times` optionally gives a per-vehicle moment — the time each position was
    *recorded*, rather than when it was read. The live monitor has no use for
    it: it acts on the newest positions it has, and treating a stale one as
    current is the conservative error there. The nightly processor must pass it.
    The feed runs a median 186 seconds behind, measured, and judging a
    three-minute-old position against the current clock does not merely blur
    the match — it moves the bus a consistent three minutes down the timetable,
    onto the following journey. That misattribution makes late buses look early.
    """
    options = []
    for vi, v in enumerate(vehicles):
        # Buses that already declared their journey, and the journeys they
        # took, are out of the running: inference exists to fill the gaps the
        # feed leaves, not to argue with what it states.
        if vi in skip_vehicles:
            continue
        if (v.get("operator_ref") or "").upper() in COACH_NOCS:
            continue
        vkeys = service_keys(v.get("service_ref"))
        vlat, vlon = v.get("latitude"), v.get("longitude")
        if not vkeys or vlat is None or vlon is None:
            continue
        dest = (v.get("destination") or "").replace("_", " ").strip().lower()
        heading = v.get("bearing")
        # Feeds send 0 for "no heading". Trusting it would rule out every
        # bus on an east-west road and invent the gap this reports.
        if not heading:
            heading = None
        per_vehicle = []
        for key, inst in instances.items():
            if key in skip_journeys:
                continue
            if not (vkeys & inst["keys"]) or not inst["calls"]:
                continue
            best_i, best_d = None, None
            for i, (_secs, atco) in enumerate(inst["calls"]):
                s = tt.stops.get(atco) or {}
                if s.get("lat") is None:
                    continue
                d = km(vlat, vlon, s["lat"], s["lon"])
                if best_d is None or d < best_d:
                    best_i, best_d = i, d
            if best_i is None or best_d > MAX_OFF_ROUTE_KM:
                continue
            lateness = (times[vi] if times else now) - inst["calls"][best_i][0]
            if not (-EARLY_TOLERANCE_SECS <= lateness <= MATCH_TOLERANCE_SECS):
                continue
            # A bus on the other side of the road sits within metres of this
            # trip's poles. Its heading, when the feed sends one, rules it out.
            if heading is not None and not heading_agrees(tt, inst["calls"], best_i, heading):
                continue
            headsign = (inst["trip"].get("headsign") or "").lower()
            last = (tt.stops.get(inst["calls"][-1][1]) or {}).get("name", "").lower()
            dir_match = bool(dest) and (dest in headsign or dest in last
                                        or (headsign and headsign in dest)
                                        or (last and last in dest))
            per_vehicle.append((abs(lateness), key, best_i, dir_match))
        # Where the destination names a trip, only trips it names are in the
        # running; otherwise a fresher trip the other way could win on time.
        if any(m for *_, m in per_vehicle):
            per_vehicle = [p for p in per_vehicle if p[3]]
        for score, key, idx, _m in per_vehicle:
            options.append((score, vi, key, idx))

    placed: dict = {}
    used = set()
    for score, vi, key, idx in sorted(options, key=lambda o: o[0]):
        if vi in used or key in placed:
            continue
        placed[key] = (vi, idx)
        used.add(vi)
    return placed


def heading_agrees(tt, calls, i, heading) -> bool:
    j, k = (i, i + 1) if i + 1 < len(calls) else (i - 1, i)
    if j < 0:
        return True
    a = tt.stops.get(calls[j][1]) or {}
    b = tt.stops.get(calls[k][1]) or {}
    if a.get("lat") is None or b.get("lat") is None:
        return True
    if km(a["lat"], a["lon"], b["lat"], b["lon"]) < 0.02:
        return True
    diff = abs((bearing(a["lat"], a["lon"], b["lat"], b["lon"]) - float(heading) + 180) % 360 - 180)
    return diff <= 90


def past_pole(tt, vehicle, calls, p) -> bool:
    """Nearest stop is the pole itself: has the bus gone by it yet?"""
    if p + 1 >= len(calls):
        return False
    pole = tt.stops.get(calls[p][1]) or {}
    nxt = tt.stops.get(calls[p + 1][1]) or {}
    if pole.get("lat") is None or nxt.get("lat") is None:
        return False
    return (km(vehicle["latitude"], vehicle["longitude"], nxt["lat"], nxt["lon"])
            < km(pole["lat"], pole["lon"], nxt["lat"], nxt["lon"]))
