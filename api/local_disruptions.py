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

One notice often says different things for different routes: the Old Steine
works of October 2026 kept every southbound bus off one stop, routes towards
Imperial Arcade off several, and the 25, 25X and 78 off the Old Steine only
from 07:00 to 18:00. So an entry may name a `parent`, the notice it is part
of. A sub-entry is about its own `lines` only:

- its `stops_not_served` are not served *by those lines*; other buses still
  call, and their rows on the same board stay live;
- `only_journeys_via` narrows it to journeys that call at one of those stops,
  which is how "routes towards Imperial Arcade" is told apart from the same
  route going the other way;
- `daily` (`{"from": "07:00", "to": "18:00"}`, UK time) puts it in force only
  in those hours of each day, and it is not shown outside them.

A top-level entry keeps the old meaning: its stops are served by no bus.
`stops_affected` are the stops a notice lists as affected, whose boards show
it, without saying anything about which buses call. `observed` marks an entry
we saw on the road rather than read in a notice, as when Stagecoach published
nothing for the 700.

See docs/RUNBOOK.md, "Adding a disruption", and scripts/add_disruption.py.
"""

import json
import math
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional
from zoneinfo import ZoneInfo

UK = ZoneInfo("Europe/London")

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


def windows(entry: dict) -> list:
    """`[(start, end)]` when the entry is in force: its dates, or with `daily`
    one window a day inside them, in UK time so a clock change moves nothing."""
    start, end = _parse(entry.get("starts")), _parse(entry.get("ends"))
    daily = entry.get("daily")
    if not daily or start is None or end is None:
        return [(start, end)]
    opens, shuts = time.fromisoformat(daily["from"]), time.fromisoformat(daily["to"])
    out = []
    day, last = start.astimezone(UK).date(), end.astimezone(UK).date()
    while day <= last:
        s = max(start, datetime.combine(day, opens, UK))
        e = min(end, datetime.combine(day, shuts, UK))
        if s < e:
            out.append((s, e))
        day += timedelta(days=1)
    return out


def _line(name: str) -> str:
    # Exact, unlike disruptions.service_key: a notice about the 700 says
    # nothing about which stops the N700 calls at.
    return "".join(str(name or "").split()).upper()


# Operator codes that publish the same buses (as in api/main.py).
_FAMILIES = {"SCSC": "SCSO", "CMPA": "COMT"}


def _op(code: str) -> str:
    return _FAMILIES.get(code or "", code or "")


def _names_line(entry: dict, operator: str, line: str) -> bool:
    return any(_op(l["operator"]) == _op(operator) and _line(l["line"]) == _line(line)
               for l in entry.get("lines", []))


def as_situations(entries: list) -> list:
    """Entries in the shape api/disruptions.py uses for the feed's situations,
    so `current`, `affecting` and `row_matches` treat both alike."""
    out = []
    for e in entries:
        closed = [s["atco"] for s in e.get("stops_not_served", [])]
        affected = [s["atco"] for s in e.get("stops_affected", [])]
        sub = bool(e.get("parent"))
        out.append({
            "id": e["id"],
            "progress": "open",
            "periods": windows(e),
            # Outside its hours a daily sub-entry is not news: the buses run
            # as normal, and "from 07:00" on every row all evening is noise.
            "only_while_live": bool(e.get("daily")),
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
            # served, the stops on the diversion and the stops it lists as
            # affected all count. A sub-entry is about its own routes, so it
            # reaches a board through their rows, not through the stop.
            "stops": [] if sub else closed + affected + list(e.get("diversion_stops", [])),
            "operators": [],
            # Carried through to the published notice.
            "extra": {
                "source": "curated",
                "checked_on": e.get("checked_on", ""),
                "stops_not_served": closed,
                "diversions": e.get("diversions", []),
                **({"parent": e["parent"]} if sub else {}),
                # Another publisher's notice of the same works: one notice on
                # the board, each author's words inside it.
                **({"same_event_as": e["same_event_as"]} if e.get("same_event_as") else {}),
                **({"only_journeys_via": list(e["only_journeys_via"])} if e.get("only_journeys_via") else {}),
                **({"daily": e["daily"], "final_ends": e.get("ends")} if e.get("daily") else {}),
                **({"observed": True} if e.get("observed") else {}),
            },
        })
    return out


def in_force(entry: dict, now: datetime) -> bool:
    return any((s is None or s <= now) and (e is None or e >= now) for s, e in windows(entry))


def closures_now(now: datetime, path: Optional[Path] = None) -> dict:
    """{stop atco: entry} for every stop no bus calls at, at this moment.

    Only top-level entries close a stop outright; a sub-entry's stops are
    closed to its own routes (`closed_for`).
    """
    out = {}
    for e in load(path):
        if not e.get("parent") and in_force(e, now):
            for s in e.get("stops_not_served", []):
                out[s["atco"]] = e
    return out


def sub_entry_applies(notice: dict, operator: str, line: str,
                      calls: Optional[Callable[[], Iterable[str]]] = None) -> bool:
    """Whether a published notice belongs on one journey's row or Bus tab.

    Any notice that is not a sub-entry does, as before. A sub-entry only on
    its own routes, named exactly (the 700's notice is not the N700's), and,
    narrowed by `only_journeys_via`, only on a journey known to call there.
    """
    if not notice.get("parent"):
        return True
    if not _names_line(notice, operator, line):
        return False
    via = set(notice.get("only_journeys_via") or [])
    return not via or (calls is not None and bool(via & set(calls())))


def line_closures_at(now: datetime, stop_id: str, path: Optional[Path] = None) -> list:
    """Sub-entries in force now that keep some routes off `stop_id`."""
    return [e for e in load(path)
            if e.get("parent") and in_force(e, now)
            and any(s["atco"] == stop_id for s in e.get("stops_not_served", []))]


def closed_for(now: datetime, operator: str, line: str,
               calls: Optional[Callable[[], Iterable[str]]] = None,
               path: Optional[Path] = None) -> dict:
    """{stop atco: entry} for the stops one journey will not call at now.

    The stops closed to every bus, and those a sub-entry closes to this
    journey's route. `calls` gives the journey's stops, asked for only when a
    sub-entry is narrowed to journeys via a stop; without it such a sub-entry
    is left out, because we cannot tell which way the bus is going.
    """
    out = dict(closures_now(now, path))
    stops = None
    for e in load(path):
        if not e.get("parent") or not in_force(e, now) or not _names_line(e, operator, line):
            continue
        via = set(e.get("only_journeys_via") or [])
        if via:
            if calls is None:
                continue
            if stops is None:
                stops = set(calls())
            if not via & stops:
                continue
        for s in e.get("stops_not_served", []):
            out.setdefault(s["atco"], e)
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


def _patterns(tt, stop_id: str, entry: Optional[dict] = None) -> dict:
    """`{(route, headsign): [trip ids]}` through `stop_id`, a few trips each.
    With a sub-entry, only the journeys it covers: its routes, and with
    `only_journeys_via` only those calling at one of those stops."""
    names = {_line(l["line"]) for l in entry.get("lines", [])} if entry else None
    via = set(entry.get("only_journeys_via") or []) if entry else set()
    per_pattern: dict = {}
    for _secs, trip_id in tt.stop_times_for(stop_id):
        trip = tt.trips.get(trip_id) or {}
        route = (tt.routes.get(trip.get("route_id")) or {}).get("short_name", "")
        if names is not None and _line(route) not in names:
            continue
        key = (route, trip.get("headsign", ""))
        if len(per_pattern.setdefault(key, [])) < TRIPS_PER_PATTERN:
            per_pattern[key].append(trip_id)
    if via:
        per_pattern = {k: kept for k, ids in per_pattern.items()
                       if (kept := [t for t in ids if via & {a for _s, a in tt.trip_stops_for(t)}])}
    return per_pattern


def still_served_nearby(tt, stop_id: str, closed: set, entry: Optional[dict] = None) -> list:
    """The nearest stops, on each side, that buses through `stop_id` still call at.

    Every route through the stop (or only the journeys a sub-entry `entry`
    covers) is walked backwards and forwards to the first call not in
    `closed`. Returned nearest first, each with the routes that reach it that
    way and whether buses get there before or after this stop.
    """
    here = tt.stops.get(stop_id)
    if not here or here.get("lat") is None:
        return []
    per_pattern = _patterns(tt, stop_id, entry)

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
    return {**_closure_fields(entry, now),
            "still_served": still_served_nearby(tt, stop_id, set(closures))}


def _closure_fields(entry: dict, now: datetime) -> dict:
    live = [e for s, e in windows(entry) if (s is None or s <= now) and (e is None or e >= now)]
    return {
        "id": entry["id"],
        "summary": entry.get("summary", ""),
        # Until when *now*: with `daily`, the end of today's hours.
        "until": live[0].isoformat() if live and live[0] else entry.get("ends"),
        "reason": entry.get("reason", ""),
        "publisher": entry.get("publisher", ""),
        "source_url": entry.get("source_url", ""),
        "checked_on": entry.get("checked_on", ""),
        "diversions": entry.get("diversions", []),
        # Only when the notice says so ("All stops on the diversion routes
        # will be served"); the board used to claim it of every closure.
        **({"diversion_stops_served": True} if entry.get("diversion_stops_served") else {}),
        **({"daily": entry["daily"], "final_ends": entry.get("ends")} if entry.get("daily") else {}),
        **({"observed": True} if entry.get("observed") else {}),
    }


def line_closures_for(tt, stop_id: str, now: datetime, path: Optional[Path] = None) -> list:
    """What a board at `stop_id` should say about routes that skip it now:
    one per sub-entry, with the routes it names and where they still call."""
    out = []
    whole = set(closures_now(now, path))
    for e in line_closures_at(now, stop_id, path):
        # Only the routes whose covered journeys really call here: a notice
        # for the 79 on a board the 79 never reaches, or for routes towards
        # Imperial Arcade at a stop they only pass going the other way, is
        # a claim about nothing.
        here = {_line(route) for route, _h in _patterns(tt, stop_id, e)}
        lines = [l["line"] for l in e.get("lines", []) if _line(l["line"]) in here]
        if not lines:
            continue
        closed = whole | {s["atco"] for s in e.get("stops_not_served", [])}
        out.append({**_closure_fields(e, now), "lines": lines,
                    "still_served": still_served_nearby(tt, stop_id, closed, e)})
    return out
