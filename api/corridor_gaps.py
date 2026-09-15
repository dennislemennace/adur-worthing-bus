"""Long gaps between buses on the A259 coast road through Shoreham, right now.

The coast road through Shoreham is the corridor this project is about, and the
complaint people make about it is rarely the timetable on paper. It is standing
at a stop while the bus that should have come does not. This measures that, in
both directions, from the live bus positions the map already fetches, and says
so only when a gap is longer than the timetable itself would explain.

**Where the times come from.** Not the prediction feed. TransportAPI is capped
at 300 calls a day across the whole site, and six stops polled every minute
would spend that by mid-morning and take live times away from every stop
panel. BODS vehicle positions are already fetched once for the whole area and
cached, so this costs no upstream calls at all.

**How a bus becomes an arrival time.** Each tracked bus is placed on a
scheduled trip through the corridor by where it is and when: the trip whose
nearest stop to the bus is scheduled closest to now. Its arrival at a
watchpoint is now plus the *scheduled running time* from that stop to the
watchpoint. That is deliberately independent of lateness, which this feed
barely publishes: a bus twenty minutes late is still twenty minutes' running
time away.

**Three ways it raised a false alarm on live data, and what stops each.**

* A bus was placed on a trip that had not started. Two buses reported the same
  position, and the second was matched to the *next* departure, thirteen
  minutes ahead of it. Buses run late far more than early, so a bus may now be
  at most five minutes early for a trip, though still up to 25 minutes late.
* A trip a few minutes past its first stop, with no bus on it yet, was counted
  as not reporting. That is usually a bus leaving late. For fifteen minutes
  such a trip is taken at its timetable time, or later if it cannot now make
  that, before it counts as missing.
* A gap sitting on the threshold appeared for one refresh and went again. An
  alert is now only reported when a second look, at least 45 seconds after the
  first, still finds it.

**What it cannot see, and which way that cuts.** A bus that is not reporting
its position looks exactly like a bus that is not running. That bias makes gaps
look *longer*, which is the direction that suits this campaign, so a gap
containing a scheduled bus with no tracked vehicle says so in as many words
rather than calling it a wait.

Quiet hours, 23:30 to 04:30, are not measured at all. The timetable leaves
hour-long gaps then by design, and the question this answers is a daytime one.
"""

import math
from datetime import timedelta

# In the order a bus in that direction reaches them. Each pole serves one side
# of the road, so naming the pole is what makes a direction; there is no
# direction field in the feed to get wrong.
WEST_WATCHPOINTS = (
    ("4400AD0330", "Shoreham Port"),
    ("4400AD0203", "Shoreham High Street"),
    ("4400AD0063", "Beach Green Hotel, Lancing"),
)
EAST_WATCHPOINTS = (
    ("4400AD0064", "Beach Green Hotel, Lancing"),
    ("4400AD0204", "Shoreham High Street"),
    ("4400AD0329", "Shoreham Port"),
)

DIRECTIONS = (
    {"id": "worthing", "label": "A259 Coast Rd towards Worthing",
     "towards": "towards Worthing", "watchpoints": WEST_WATCHPOINTS},
    {"id": "brighton", "label": "A259 Coast Rd towards Brighton",
     "towards": "towards Brighton", "watchpoints": EAST_WATCHPOINTS},
)

QUIET_FROM_SECS = 23 * 3600 + 30 * 60
QUIET_TO_SECS = 4 * 3600 + 30 * 60

HORIZON_SECS = 60 * 60
# A gap has to be both long in itself and clearly longer than the timetable's
# own gap at that time. The second condition is what keeps an evening
# half-hourly service from reading as a failure every single evening.
ALERT_GAP_SECS = 20 * 60
ALERT_OVER_SCHEDULE_SECS = 10 * 60
# Seen once is not enough. The client asks once a minute and the answer is
# cached for 30 s, so a second look is 30 to 60 s after the first.
CONFIRM_SECS = 45

# Late by up to 25 minutes, the same bound the single-vehicle trip match uses.
# Early by no more than 5: a bus mid-route cannot be running a departure that
# has not left yet.
MATCH_TOLERANCE_SECS = 25 * 60
EARLY_TOLERANCE_SECS = 5 * 60
# A bus further than this from every stop on a trip is not running that trip.
MAX_OFF_ROUTE_KM = 0.4
# A scheduled bus this far past its watchpoint time with nothing tracked has
# probably gone by, or never came; either way it is no longer a future arrival.
PASSED_GRACE_SECS = 5 * 60
# How long a departure with no bus on it is presumed to be leaving late.
LATE_START_GRACE_SECS = 15 * 60
# Below this share of on-road buses reporting, a gap says more about the feed
# than about the service, and nothing is shown.
MIN_COVERAGE = 0.5
MIN_EXPECTED_FOR_COVERAGE = 3

# Coaches and airport shuttles call at these poles but are not local buses:
# they need their own ticket and are not what anyone at a bus stop is waiting for.
COACH_NOCS = frozenset({"NATX", "FLIX", "OXBC", "GHOP", "BMCS", "UNTM"})

NEXT_SHOWN = 3

METHOD = (
    "Arrival times are estimated from live bus positions (Bus Open Data Service) "
    "and scheduled running times: each tracked bus is matched to the scheduled "
    "trip that best fits where it is now, at most 5 minutes early or 25 minutes "
    "late, and its arrival is now plus the timetable's running time from there. "
    "Buses that have not started their journey are taken from the timetable, as "
    "are those up to 15 minutes past their first stop with no bus tracked yet. "
    "A gap is flagged when it is over 20 minutes and at least 10 minutes longer "
    "than the timetable's own gap at that time, and is still there when checked "
    "again at least 45 seconds later. Coaches are not counted. Not measured "
    "between 23:30 and 04:30."
)

CAVEATS = [
    "A bus that is not sending its position cannot be told apart from one that "
    "is not running, which makes gaps look longer. Gaps containing a scheduled "
    "bus with no tracked vehicle say so.",
    "A departure with no bus tracked is assumed to be leaving late for its first "
    "15 minutes, which can hide a cancellation for that long.",
    "Estimates assume the rest of the journey takes as long as the timetable "
    "says, so traffic ahead of a bus is not accounted for.",
    "Only the next hour is looked at, so a gap running past it is measured only "
    "up to that point.",
]


def is_quiet(now_local) -> bool:
    secs = now_local.hour * 3600 + now_local.minute * 60
    # Written as "outside the day", not "inside the night": the night crosses
    # midnight, and a from < now < to test across midnight is never true.
    return not (QUIET_TO_SECS <= secs < QUIET_FROM_SECS)


def _clock(secs: int) -> str:
    secs = int(secs) % 86400
    return f"{secs // 3600:02d}:{secs % 3600 // 60:02d}"


def _km(lat1, lon1, lat2, lon2) -> float:
    dlat = (lat2 - lat1) * 111.0
    dlon = (lon2 - lon1) * 111.0 * math.cos(math.radians((lat1 + lat2) / 2))
    return math.hypot(dlat, dlon)


def _bearing(lat1, lon1, lat2, lon2) -> float:
    dlon = (lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
    return math.degrees(math.atan2(dlon, lat2 - lat1)) % 360


def _service_keys(name: str) -> set:
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


def corridor_report(tt, vehicles, now_local, memory=None, directions=DIRECTIONS) -> dict:
    """Both directions, for one moment. `now_local` is Europe/London.

    `memory` carries first sightings of each alert between calls, and is what
    makes the second-look confirmation work. The API keeps one for the life of
    the process.
    """
    base = {
        "corridor": "A259",
        "quiet_hours": {"from": _clock(QUIET_FROM_SECS), "to": _clock(QUIET_TO_SECS)},
        "as_of": now_local.replace(microsecond=0).isoformat(),
        "method": METHOD,
        "caveats": CAVEATS,
    }
    if is_quiet(now_local):
        if memory is not None:
            memory.clear()
        return {**base, "active": False, "reason": "quiet_hours", "directions": []}
    out = []
    for d in directions:
        answer = direction_gaps(tt, vehicles, now_local, d["watchpoints"],
                                memory=memory, memory_key=d["id"])
        out.append({"id": d["id"], "label": d["label"], "towards": d["towards"], **answer})
    return {**base, "active": True, "directions": out}


def direction_gaps(tt, vehicles, now_local, watchpoints, memory=None, memory_key="") -> dict:
    """One direction's answer.

    With no `memory`, an alert is reported the first time it is seen. That is
    for tests of everything else; the API always passes one.
    """
    if tt is None or not tt.ok():
        return {"status": "unknown", "reason": "no_timetable"}
    if not vehicles:
        return {"status": "unknown", "reason": "no_live_data"}

    now = now_local.hour * 3600 + now_local.minute * 60 + now_local.second
    today = now_local.date()
    service_days = ((today, 0), (today - timedelta(days=1), 86400))
    runs: dict = {}

    def running(service_id, day):
        key = (service_id, day)
        if key not in runs:
            runs[key] = tt.runs_on(service_id, day)
        return runs[key]

    # ── Every scheduled call at a watchpoint worth knowing about ──
    # Keyed by (trip, service day): a trip written as 28:40 on yesterday's
    # service is a bus this morning, and the same trip_id also runs today.
    window_lo, window_hi = now - 2 * HORIZON_SECS, now + 2 * HORIZON_SECS
    instances: dict = {}          # (trip_id, day) -> {"shift", "trip", ...}
    scheduled: dict = {atco: [] for atco, _ in watchpoints}
    for atco, _name in watchpoints:
        for secs, trip_id in tt.stop_times_for(atco):
            if secs is None:
                continue
            trip = tt.trips.get(trip_id) or {}
            if tt.noc_for_route(trip.get("route_id", "")) in COACH_NOCS:
                continue
            for day, shift in service_days:
                t = secs - shift
                if not (window_lo <= t <= window_hi):
                    continue
                if not running(trip.get("service_id", ""), day):
                    continue
                key = (trip_id, day)
                scheduled[atco].append((t, key))
                instances.setdefault(key, {"shift": shift, "trip": trip})
    for atco in scheduled:
        scheduled[atco].sort()

    if not instances:
        return {"status": "unknown", "reason": "no_timetable"}

    for key, inst in instances.items():
        calls = tt.trip_stops_for(key[0])
        inst["calls"] = [(secs - inst["shift"], atco) for secs, atco in calls
                         if secs is not None]
        route = tt.routes.get(inst["trip"].get("route_id", "")) or {}
        inst["service"] = route.get("short_name", "")
        inst["keys"] = _service_keys(inst["service"])
        inst["first"] = inst["calls"][0][0] if inst["calls"] else None
        inst["started"] = inst["first"] is not None and inst["first"] <= now

    # ── Place each tracked bus on at most one trip, and each trip on one bus ──
    options = []
    for vi, v in enumerate(vehicles):
        if (v.get("operator_ref") or "").upper() in COACH_NOCS:
            continue
        vkeys = _service_keys(v.get("service_ref"))
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
            if not (vkeys & inst["keys"]) or not inst["calls"]:
                continue
            best_i, best_d = None, None
            for i, (_secs, atco) in enumerate(inst["calls"]):
                s = tt.stops.get(atco) or {}
                if s.get("lat") is None:
                    continue
                d = _km(vlat, vlon, s["lat"], s["lon"])
                if best_d is None or d < best_d:
                    best_i, best_d = i, d
            if best_i is None or best_d > MAX_OFF_ROUTE_KM:
                continue
            lateness = now - inst["calls"][best_i][0]
            if not (-EARLY_TOLERANCE_SECS <= lateness <= MATCH_TOLERANCE_SECS):
                continue
            # A bus on the other side of the road sits within metres of this
            # trip's poles. Its heading, when the feed sends one, rules it out.
            if heading is not None and not _heading_agrees(tt, inst["calls"], best_i, heading):
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

    placed: dict = {}             # (trip_id, day) -> (vehicle index, nearest call index)
    used = set()
    for score, vi, key, idx in sorted(options, key=lambda o: o[0]):
        if vi in used or key in placed:
            continue
        placed[key] = (vi, idx)
        used.add(vi)

    def leaving_late(inst):
        return inst["started"] and now - inst["first"] <= LATE_START_GRACE_SECS

    # ── Coverage: of the buses that should be on the road, how many are seen ──
    # A departure still inside its late-leaving grace is not yet expected to
    # be tracked, so it counts for neither side until it is.
    still_due = {k for atco, _ in watchpoints for t, k in scheduled[atco]
                 if t >= now - PASSED_GRACE_SECS}
    expected = [key for key, inst in instances.items()
                if inst["started"] and key in still_due
                and (key in placed or not leaving_late(inst))]
    reporting = sum(1 for key in expected if key in placed)
    coverage = {"on_road": len(expected), "reporting": reporting}
    if (len(expected) >= MIN_EXPECTED_FOR_COVERAGE
            and reporting / len(expected) < MIN_COVERAGE):
        return {"status": "unknown", "reason": "low_coverage", "coverage": coverage}

    end = now + HORIZON_SECS
    now_ts = now_local.timestamp()
    stops_out = []
    for atco, name in watchpoints:
        arrivals, missing = [], []
        for t, key in scheduled[atco]:
            inst = instances[key]
            if key in placed:
                vi, idx = placed[key]
                p = next((i for i, (_s, a) in enumerate(inst["calls"]) if a == atco), None)
                if p is None:
                    continue
                if idx < p or (idx == p and not _past_pole(tt, vehicles[vi], inst["calls"], p)):
                    eta = now + max(0, inst["calls"][p][0] - inst["calls"][idx][0])
                    arrivals.append((eta, inst["service"], "live"))
            elif not inst["started"]:
                if t >= now:
                    arrivals.append((t, inst["service"], "scheduled"))
            elif leaving_late(inst):
                # If it left this minute, it would reach the stop this long
                # from now; it cannot be any earlier than the timetable says.
                arrivals.append((max(t, now + (t - inst["first"])), inst["service"], "scheduled"))
            elif t >= now - PASSED_GRACE_SECS:
                missing.append(t)
        arrivals.sort()
        in_view = [a for a in arrivals if now <= a[0] <= end]
        sched_times = [t for t, _ in scheduled[atco]]

        points = [now] + [a[0] for a in in_view] + [end]
        worst = None
        for x, y in zip(points, points[1:]):
            length = y - x
            timetable_gap = _timetable_gap(sched_times, x, y)
            qualifies = (length > ALERT_GAP_SECS
                         and length - timetable_gap >= ALERT_OVER_SCHEDULE_SECS)
            gap = {
                "minutes": length // 60,
                "from": _clock(x),
                "to": _clock(y),
                "from_now": x == now,
                "to_horizon": y == end,
                "timetable_minutes": timetable_gap // 60,
                "not_reporting": sum(1 for m in missing if x < m < y),
                "alert": qualifies,
            }
            if worst is None or (qualifies, length) > (worst["alert"], worst["_secs"]):
                worst = {**gap, "_secs": length}
        if worst is not None:
            worst.pop("_secs")
            _confirm(worst, memory, f"{memory_key}:{atco}", now_ts)
        stops_out.append({
            "atco": atco,
            "name": name,
            "next": [{"service": svc, "due": _clock(t),
                      "minutes": max(0, (t - now) // 60), "source": src}
                     for t, svc, src in in_view[:NEXT_SHOWN]],
            "longest_gap": worst,
        })

    alerts = [s for s in stops_out if s["longest_gap"] and s["longest_gap"]["alert"]]
    alert = None
    if alerts:
        top = max(alerts, key=lambda s: s["longest_gap"]["minutes"])
        alert = {"atco": top["atco"], "name": top["name"], **top["longest_gap"]}
    return {
        "status": "alert" if alert else "normal",
        "reason": None,
        "stops": stops_out,
        "alert": alert,
        "coverage": coverage,
    }


def _confirm(gap, memory, key, now_ts) -> None:
    """Hold an alert back until a second look still finds it."""
    if memory is None:
        return
    if not gap["alert"]:
        memory.pop(key, None)
        return
    first_seen = memory.setdefault(key, now_ts)
    if now_ts - first_seen < CONFIRM_SECS:
        gap["alert"] = False
        gap["pending"] = True


def _timetable_gap(sched_times, x, y) -> int:
    """The longest gap the timetable itself has around [x, y].

    Scheduled headways that overlap the interval, not the stretch clipped to
    it: when a bus due in two minutes does not come, the timetable's gap there
    is the ten minutes either side of it, not the two minutes left before it.
    """
    worst = 0
    for a, b in zip(sched_times, sched_times[1:]):
        if a < y and b > x:
            worst = max(worst, b - a)
    if not any(x < t < y for t in sched_times):
        # Nothing scheduled inside at all: the timetable's own gap spans the
        # whole interval, and if the timetable runs out either side, so be it.
        before = [t for t in sched_times if t <= x]
        after = [t for t in sched_times if t >= y]
        if before and after:
            worst = max(worst, after[0] - before[-1])
        else:
            worst = max(worst, y - x)
    return worst


def _heading_agrees(tt, calls, i, heading) -> bool:
    j, k = (i, i + 1) if i + 1 < len(calls) else (i - 1, i)
    if j < 0:
        return True
    a = tt.stops.get(calls[j][1]) or {}
    b = tt.stops.get(calls[k][1]) or {}
    if a.get("lat") is None or b.get("lat") is None:
        return True
    if _km(a["lat"], a["lon"], b["lat"], b["lon"]) < 0.02:
        return True
    diff = abs((_bearing(a["lat"], a["lon"], b["lat"], b["lon"]) - float(heading) + 180) % 360 - 180)
    return diff <= 90


def _past_pole(tt, vehicle, calls, p) -> bool:
    """Nearest stop is the watchpoint itself: has the bus gone by it yet?"""
    if p + 1 >= len(calls):
        return False
    pole = tt.stops.get(calls[p][1]) or {}
    nxt = tt.stops.get(calls[p + 1][1]) or {}
    if pole.get("lat") is None or nxt.get("lat") is None:
        return False
    return (_km(vehicle["latitude"], vehicle["longitude"], nxt["lat"], nxt["lon"])
            < _km(pole["lat"], pole["lon"], nxt["lat"], nxt["lon"]))
