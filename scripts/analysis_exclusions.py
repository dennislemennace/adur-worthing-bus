"""The permanent register of events that distort what the site measures.

data/analysis_exclusions.json records diversions, closures and the like with
the routes, dates and streets they touched. It is kept for good, unlike the
notices in data/disruptions.json, which are pruned once they end, so a figure
built next year can still set the Western Road diversion apart.

`affecting` answers, for one journey or one stop-to-stop traversal, which
entries apply: the same operator and route, inside the dates, and either
calling at a stop the event closed or passing through its area (a stop inside
it, or the straight line between two stops crossing it). The builders use it
to *mark* what an event touched; dropping it is a separate, explicit choice
(`--exclude`), so recording an event never changes a published figure.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "analysis_exclusions.json"

# Families of operator codes that publish the same buses (as in api/main.py).
_FAMILIES = {"SCSC": "SCSO", "CMPA": "COMT", "METR": "BHBC"}


def load(path: Path = DATA) -> list:
    """The entries, with times parsed. An absent file means none."""
    try:
        doc = json.loads(Path(path).read_text())
    except FileNotFoundError:
        return []
    out = []
    for e in doc.get("exclusions", []):
        out.append({**e,
                    "_from": datetime.fromisoformat(e["from"]),
                    "_to": datetime.fromisoformat(e["to"]) if e.get("to") else None,
                    "_routes": {str(r).upper() for r in e.get("routes") or []},
                    "_stops": set(e.get("stops_not_served") or [])})
    return out


def stop_coords(path: Path = ROOT / "data" / "stops.json") -> dict:
    """`{atco: (lat, lon)}` for the map's stops, which is where any event we
    record happens. Observation rows name stops but carry no position."""
    try:
        stops = json.loads(Path(path).read_text())["stops"]
    except (FileNotFoundError, KeyError):
        return {}
    return {s["atco_code"]: (s["latitude"], s["longitude"]) for s in stops
            if s.get("latitude") is not None and s.get("longitude") is not None}


def _inside(point, polygon) -> bool:
    """Ray casting on (lat, lon); fine at street scale."""
    lat, lon = point
    inside = False
    j = len(polygon) - 1
    for i in range(len(polygon)):
        (ai, bi), (aj, bj) = polygon[i], polygon[j]
        if (bi > lon) != (bj > lon) and lat < (aj - ai) * (lon - bi) / (bj - bi) + ai:
            inside = not inside
        j = i
    return inside


def _cross(p, q, r, s) -> bool:
    def orient(a, b, c):
        v = (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
        return (v > 0) - (v < 0)
    return orient(p, q, r) != orient(p, q, s) and orient(r, s, p) != orient(r, s, q)


def path_touches(path: Sequence, polygon: Sequence) -> bool:
    """Any point inside, or any leg of the path crossing an edge."""
    if not polygon or not path:
        return False
    if any(_inside(p, polygon) for p in path):
        return True
    edges = list(zip(polygon, list(polygon[1:]) + [polygon[0]]))
    return any(_cross(a, b, c, d) for a, b in zip(path, path[1:]) for c, d in edges)


def affecting(entries: list, operator: str, route: str, when: datetime,
              stop_ids: Iterable[str] = (), path: Optional[Sequence] = None) -> list:
    """Ids of the entries that apply to a journey or traversal at `when`.

    `path` is the (lat, lon) of its stops in order, where known.
    """
    op = _FAMILIES.get(operator or "", operator or "")
    stops = set(stop_ids)
    out = []
    for e in entries:
        if _FAMILIES.get(e.get("operator", ""), e.get("operator", "")) != op:
            continue
        if e["_routes"] and str(route or "").upper() not in e["_routes"]:
            continue
        if when < e["_from"] or (e["_to"] is not None and when > e["_to"]):
            continue
        if (stops & e["_stops"]) or (path and path_touches(path, e.get("area") or [])):
            out.append(e["id"])
    return out
