"""Disruptions we record ourselves, in data/disruptions.json.

The BODS disruptions feed (api/disruptions.py) carries nothing for this area:
no Sussex council or operator publishes to it. Operators do publish, on their
own websites, and the notices that matter most here are the long ones: a road
closed for six weeks, stops not served, buses on a diversion. Those are copied
in by hand from the operator's page, with the page and the date it was checked,
and served through the same path as the feed's notices so every board, row and
Bus tab shows them the same way.

Two things the feed's format cannot say, and these entries can:

- `stops_not_served`: stops that no bus will call at. Their boards say so
  instead of listing departures that will not come, and live estimates for
  them are withdrawn.
- `diversions`: which way the buses go instead, in the operator's words.

For each stop not served, the nearest stops on either side that *are* still
served are worked out from the timetable, walking each route through the stop
outwards to the first stop not on the list. That is where the operator's own
wording says buses "resume full route", and it needs no knowledge of the
diversion itself.

See docs/RUNBOOK.md, "Adding a disruption", and scripts/add_disruption.py.
"""

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
PATH = ROOT / "data" / "disruptions.json"

# How many still-served stops to offer for each stop not served.
ALTERNATIVES = 4
# Trips sampled per (route, headsign) when walking a stop's routes. The
# patterns inside one pair barely differ, and every trip would be thousands.
TRIPS_PER_PATTERN = 3

_cache: dict = {"mtime": None, "path": None, "entries": []}


def _parse(value: str) -> Optional[datetime]:
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def load(path: Optional[Path] = None) -> list:
    """The entries in the file, re-read only when it changes."""
    path = path or PATH
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return []
    if _cache["mtime"] != mtime or _cache["path"] != path:
        data = json.loads(path.read_text(encoding="utf-8"))
        _cache.update(mtime=mtime, path=path, entries=data.get("disruptions", []))
    return _cache["entries"]


def as_situations(entries: list) -> list:
    """Entries in the shape api/disruptions.py uses for the feed's situations,
    so `current`, `affecting` and `row_matches` treat both alike."""
    out = []
    for e in entries:
        closed = [s["atco"] for s in e.get("stops_not_served", [])]
        out.append({
            "id": e["id"],
            "progress": "open",
            "periods": [(_parse(e.get("starts")), _parse(e.get("ends")))],
            "publication": [],
            "planned": True,
            "reason": e.get("reason", ""),
            "severity": e.get("severity", ""),
            "summary": e.get("summary", ""),
            "description": e.get("description", ""),
            "advice": e.get("advice", ""),
            "link": e.get("source_url", ""),
            "publisher": e.get("publisher", ""),
            "lines": [{"operator": l["operator"], "line": l["line"], "line_ref": l["line"]}
                      for l in e.get("lines", [])],
            # A notice about a stop shows on that stop's board: the stops not
            # served and the stops on the diversion both count.
            "stops": closed + list(e.get("diversion_stops", [])),
            "operators": [],
            # Carried through to the published notice.
            "extra": {
                "source": "curated",
                "checked_on": e.get("checked_on", ""),
                "stops_not_served": closed,
                "diversions": e.get("diversions", []),
            },
        })
    return out


def in_force(entry: dict, now: datetime) -> bool:
    start, end = _parse(entry.get("starts")), _parse(entry.get("ends"))
    return (start is None or start <= now) and (end is None or end >= now)


def closures_now(now: datetime, path: Optional[Path] = None) -> dict:
    """{stop atco: entry} for every stop not served at this moment."""
    out = {}
    for e in load(path):
        if in_force(e, now):
            for s in e.get("stops_not_served", []):
                out[s["atco"]] = e
    return out


def _metres(a: dict, b: dict) -> float:
    dlat = math.radians(b["lat"] - a["lat"])
    dlon = math.radians(b["lon"] - a["lon"]) * math.cos(math.radians((a["lat"] + b["lat"]) / 2))
    return 6371000 * math.hypot(dlat, dlon)


COMPASS = ("north", "north-east", "east", "south-east",
           "south", "south-west", "west", "north-west")


def _direction(a: dict, b: dict) -> str:
    """Which way to walk from a to b, as a person at a stop would say it."""
    y = math.radians(b["lat"] - a["lat"])
    x = math.radians(b["lon"] - a["lon"]) * math.cos(math.radians((a["lat"] + b["lat"]) / 2))
    bearing = (math.degrees(math.atan2(x, y)) + 360) % 360
    return COMPASS[int((bearing + 22.5) // 45) % 8]


def _route_order(name: str):
    digits = "".join(ch for ch in name if ch.isdigit())
    return (int(digits) if digits else 10 ** 6, name)


def still_served_nearby(tt, stop_id: str, closed: set) -> list:
    """The nearest stops, on each side, that buses through `stop_id` still call at.

    Every route through the stop is walked backwards and forwards to the first
    call not in `closed`. Returned nearest first, each with the routes that
    reach it that way and whether buses get there before or after this stop.
    """
    here = tt.stops.get(stop_id)
    if not here or here.get("lat") is None:
        return []
    per_pattern: dict = {}
    for _secs, trip_id in tt.stop_times_for(stop_id):
        trip = tt.trips.get(trip_id) or {}
        route = (tt.routes.get(trip.get("route_id")) or {}).get("short_name", "")
        key = (route, trip.get("headsign", ""))
        if len(per_pattern.setdefault(key, [])) < TRIPS_PER_PATTERN:
            per_pattern[key].append(trip_id)

    found: dict = {}
    for (route, _headsign), trip_ids in per_pattern.items():
        for trip_id in trip_ids:
            atcos = [a for _s, a in tt.trip_stops_for(trip_id)]
            if stop_id not in atcos:
                continue
            i = atcos.index(stop_id)
            for side, walk in (("before", range(i - 1, -1, -1)),
                               ("after", range(i + 1, len(atcos)))):
                for j in walk:
                    if atcos[j] not in closed:
                        slot = found.setdefault(atcos[j], {"routes": set(), "sides": set()})
                        if route:
                            slot["routes"].add(route)
                        slot["sides"].add(side)
                        break

    out = []
    for atco, slot in found.items():
        s = tt.stops.get(atco) or {}
        if s.get("lat") is None:
            continue
        out.append({
            "atco": atco,
            "name": s.get("name", ""),
            "metres": round(_metres(here, s) / 10) * 10,
            "direction": _direction(here, s),
            "routes": sorted(slot["routes"], key=_route_order),
            # "before": buses reach it on the way to this stop; "after": once
            # past it. Both are places to board; which suits depends on where
            # the rider is going.
            "side": ("before" if slot["sides"] == {"before"} else
                     "after" if slot["sides"] == {"after"} else "both"),
        })
    out.sort(key=lambda a: a["metres"])
    return out[:ALTERNATIVES]


def closure_for(tt, stop_id: str, now: datetime, path: Optional[Path] = None) -> Optional[dict]:
    """What a board at `stop_id` should say if no bus calls there now."""
    closures = closures_now(now, path)
    entry = closures.get(stop_id)
    if not entry:
        return None
    return {
        "id": entry["id"],
        "summary": entry.get("summary", ""),
        "until": entry.get("ends"),
        "reason": entry.get("reason", ""),
        "publisher": entry.get("publisher", ""),
        "source_url": entry.get("source_url", ""),
        "checked_on": entry.get("checked_on", ""),
        "diversions": entry.get("diversions", []),
        "still_served": still_served_nearby(tt, stop_id, set(closures)),
    }
