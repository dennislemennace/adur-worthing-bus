"""Rail replacement buses on the live map, matched to the journeys they run.

Asked for on 7 November 2026, when Coastway West was closed for the evening
and Southern ran buses between Brighton, Worthing and Littlehampton. The bus
reports to BODS with its route as "Rail" and nothing else: Metrobus 6989 that
evening was "Rail" to "Destination", with no journey. Realtime Trains lists
the replacement journeys themselves, under 0B headcodes, with each station's
scheduled time but no live ones (the buses are not tracked there).

So the journey a bus is running is matched by where it is. Each candidate
journey is drawn as the line of its stations; the bus is placed on that line,
and the timetable's time at that point is set against now. The closest in
place and in time, moving the right way along it, is the match. Nothing more
than MAX_OFF_LINE_M from a journey's line, or further out than MAX_LATE_S /
MAX_EARLY_S, matches at all: an unmatched bus is better than a wrong journey.

Everything here is pure; api/main.py fetches the boards and journeys.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

UK = ZoneInfo("Europe/London")

# A replacement bus keeps to the roads, which here run within a kilometre or
# so of the railway; 2.5 km allows for the A27 and the station approaches.
MAX_OFF_LINE_M = 2500
MAX_LATE_S = 45 * 60
MAX_EARLY_S = 15 * 60
# Less movement than this between two reports says nothing about direction.
MIN_MOVE_M = 80


def is_rail_replacement(vehicle: dict) -> bool:
    """The way these buses report themselves (the page uses the same rule,
    app.js isRailReplacementVehicle)."""
    return str((vehicle or {}).get("service_ref") or "").strip().lower() == "rail"


def _when(text):
    """An RTT time: ISO, local to the station when it carries no offset."""
    if not text:
        return None
    try:
        t = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=UK)


def _xy(lat, lon, lat0):
    """Metres on a flat plane around latitude lat0: plenty at this scale."""
    r = 6_371_000
    return (math.radians(lon) * r * math.cos(math.radians(lat0)), math.radians(lat) * r)


def calls_of(service: dict, stations: dict) -> list:
    """The journey's calls in order: name, CRS, scheduled time, and where the
    station is when it is one of ours (stations: {crs: {lat, lon, name}})."""
    out = []
    for loc in service.get("locations") or []:
        sched = ((loc.get("departure") or {}).get("scheduled")
                 or (loc.get("arrival") or {}).get("scheduled"))
        t = _when(sched)
        if t is None:
            continue
        st = stations.get(loc.get("crs") or "")
        out.append({"crs": loc.get("crs") or "", "name": loc.get("description") or (st or {}).get("name") or "",
                    "t": t, "lat": st and st.get("lat"), "lon": st and st.get("lon")})
    return out


def _place(lat, lon, calls):
    """Where a point sits on the line of the calls we can place: the index of
    the call before it, the share of the way to the next, metres along the
    line and metres off it. None when fewer than two calls have a place."""
    geo = [(i, c) for i, c in enumerate(calls) if c["lat"] is not None and c["lon"] is not None]
    if len(geo) < 2:
        return None
    lat0 = lat
    px, py = _xy(lat, lon, lat0)
    best, along = None, 0.0
    for (ia, a), (ib, b) in zip(geo, geo[1:]):
        ax, ay = _xy(a["lat"], a["lon"], lat0)
        bx, by = _xy(b["lat"], b["lon"], lat0)
        dx, dy = bx - ax, by - ay
        seg = math.hypot(dx, dy)
        f = 0.0 if seg == 0 else max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (seg * seg)))
        off = math.hypot(px - (ax + f * dx), py - (ay + f * dy))
        if best is None or off < best["off"]:
            best = {"a": ia, "b": ib, "f": f, "off": off, "along": along + f * seg}
        along += seg
    return best


def match(vehicle: dict, services: list, stations: dict, now: datetime, previous=None):
    """The replacement journey this bus is running, or None.

    previous: an earlier (lat, lon) of the same bus, for its direction.
    """
    lat, lon = vehicle.get("latitude"), vehicle.get("longitude")
    if lat is None or lon is None:
        return None
    best = None
    for svc in services:
        calls = calls_of(svc, stations)
        here = _place(lat, lon, calls)
        if here is None or here["off"] > MAX_OFF_LINE_M:
            continue
        a, b = calls[here["a"]], calls[here["b"]]
        at = a["t"] + (b["t"] - a["t"]) * here["f"]
        late = (now - at).total_seconds()
        if late > MAX_LATE_S or late < -MAX_EARLY_S:
            continue
        if previous:
            before = _place(previous[0], previous[1], calls)
            if before is not None and abs(here["along"] - before["along"]) >= MIN_MOVE_M \
                    and here["along"] < before["along"]:
                continue                                        # moving the other way
        # A minute late counts the same as 200 m off the line.
        score = here["off"] / 200 + abs(late) / 60
        if best is None or score < best[0]:
            best = (score, svc, calls, here, late)
    if best is None:
        return None
    _score, svc, calls, here, late = best
    return describe(svc, calls, here, late)


def _clock(t: datetime) -> str:
    return t.astimezone(UK).isoformat()


def describe(svc: dict, calls: list, here: dict, late: float) -> dict:
    """The match, with its calls as the Bus tab's upcoming stops: the shape
    api/live_eta.py gives a timetabled bus, so the page draws it the same way.
    Times ahead are the timetable's plus how late the bus is now, never
    earlier than the timetable (a replacement bus waits for its time)."""
    lateness = int(round(late))
    rows = []
    nxt = None
    for i, c in enumerate(calls):
        passed = i <= here["a"] and not (i == here["a"] and here["f"] == 0)
        expected = None if passed else c["t"] + timedelta(seconds=max(0, lateness))
        row = {"seq": i, "stop_id": "", "crs": c["crs"], "stop_name": c["name"],
               "scheduled": _clock(c["t"]), "expected": _clock(expected) if expected else None,
               "lateness_secs": None if passed else lateness, "passed": passed,
               "is_next": False, "is_terminus": i == len(calls) - 1, "timing_point": False}
        if not passed and nxt is None:
            row["is_next"] = True
            nxt = row
        rows.append(row)
    origin = (svc.get("origin") or [{}])[0].get("description") or (calls[0]["name"] if calls else "")
    destination = (svc.get("destination") or [{}])[-1].get("description") or (calls[-1]["name"] if calls else "")
    return {
        "uid": svc.get("uid"), "date": svc.get("departureDate"), "headcode": svc.get("headcode"),
        "operator": svc.get("atocName") or svc.get("atocCode") or "",
        "origin": origin, "destination": destination,
        "departs": _clock(calls[0]["t"]) if calls else None,
        "lateness_secs": lateness, "off_line_m": int(round(here["off"])),
        "upcoming_stops": rows,
    }


def candidates(boards: list, now: datetime, window_s: int = 75 * 60, limit: int = 8) -> list:
    """(uid, date) of the replacement journeys due near now at these stations'
    boards, soonest first: the ones worth fetching to match against."""
    seen = {}
    for board in boards:
        for s in (board or {}).get("services") or []:
            if s.get("mode") != "bus" or not s.get("uid"):
                continue
            t = _when((s.get("departure") or {}).get("scheduled") or (s.get("arrival") or {}).get("scheduled"))
            if t is None:
                continue
            gap = abs((t - now).total_seconds())
            key = (s["uid"], s.get("departureDate") or now.astimezone(UK).date().isoformat())
            if gap <= window_s and (key not in seen or gap < seen[key]):
                seen[key] = gap
    return [k for k, _ in sorted(seen.items(), key=lambda kv: kv[1])[:limit]]
