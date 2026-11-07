#!/usr/bin/env python3
"""Fill in where each place in data/destinations.json is, and its nearest stops.

The places are curated by hand (name, a line in our words, its own page). This
adds what OpenStreetMap knows: the point, the object it came from, and its
wheelchair tag where one is mapped; then the stops within a short walk, from
data/stops.json, nearest first. Run after adding a place; a place already
located is left alone unless --refresh.

Nominatim's usage policy: one request a second, a real User-Agent, no bulk.
Twenty places is one short run.

    python scripts/locate_destinations.py [--refresh]
"""

import argparse
import json
import math
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEST = ROOT / "data" / "destinations.json"
STOPS = ROOT / "data" / "stops.json"
NOMINATIM = "https://nominatim.openstreetmap.org/search"
UA = {"User-Agent": "adur-worthing-bus/1.0 (destinations, by hand; +https://github.com/dennislemennace/adur-worthing-bus)"}
# The coast this site covers, a little wider for the Downs.
VIEWBOX = "-0.50,50.95,0.00,50.75"
WALK_M = 1500         # the furthest a stop may be: the Downs are a walk from any bus
STOPS_KEPT = 3


def metres(a, b):
    x = math.radians(b[1] - a[1]) * math.cos(math.radians((a[0] + b[0]) / 2))
    y = math.radians(b[0] - a[0])
    return math.hypot(x, y) * 6_371_000


def nearest_stops(at, stops):
    near = sorted(((metres(at, (s["latitude"], s["longitude"])), s) for s in stops), key=lambda x: x[0])
    return [{"atco": s["atco_code"], "name": s["name"], "metres": round(m)} for m, s in near[:STOPS_KEPT] if m <= WALK_M]


def timetable_stops(atcos):
    """Stops the site's map leaves out, from the timetable it runs on: Devil's
    Dyke's terminus is beyond the area the map shows, but the 77 runs there
    and /api/direct-from reads the same timetable."""
    import sqlite3
    db = ROOT / "data" / "timetable.sqlite"
    if not atcos or not db.exists():
        return {}
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    q = f"SELECT stop_id, name, lat, lon FROM stops WHERE stop_id IN ({','.join('?' * len(atcos))})"
    return {r[0]: {"atco_code": r[0], "name": r[1], "latitude": r[2], "longitude": r[3]}
            for r in con.execute(q, list(atcos))}


def hand_stops(at, chosen, stops):
    """Stops chosen by hand (`stops_by_hand`), for a place whose nearest stop
    is not its best: past the walk the map allows, or outside the map. A
    `walk_mins` given by hand is kept, for a walk a straight line misjudges."""
    by_id = {s["atco_code"]: s for s in stops}
    missing = [c["atco"] for c in chosen if c["atco"] not in by_id]
    by_id.update(timetable_stops(missing))
    out = []
    for c in chosen:
        s = by_id.get(c["atco"])
        if not s:
            raise SystemExit(f"stop {c['atco']} chosen by hand is in neither data/stops.json nor the timetable")
        entry = {"atco": s["atco_code"], "name": s["name"],
                 "metres": round(metres(at, (s["latitude"], s["longitude"])))}
        if c["atco"] not in {x["atco_code"] for x in stops}:
            entry["outside_map"] = True
        if c.get("walk_mins"):
            entry["walk_mins"] = c["walk_mins"]
        out.append(entry)
    return out


def locate(query):
    q = urllib.parse.urlencode({"q": query, "format": "jsonv2", "limit": 1, "extratags": 1,
                                "countrycodes": "gb", "viewbox": VIEWBOX, "bounded": 1})
    with urllib.request.urlopen(urllib.request.Request(f"{NOMINATIM}?{q}", headers=UA), timeout=30) as resp:
        found = json.loads(resp.read().decode("utf-8"))
    return found[0] if found else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--refresh", action="store_true")
    args = ap.parse_args()
    doc = json.loads(DEST.read_text(encoding="utf-8"))
    stops = json.loads(STOPS.read_text(encoding="utf-8"))["stops"]
    missing = []
    for p in doc["places"]:
        if args.refresh or "lat" not in p:
            hit = locate(p["query"])
            time.sleep(1.1)
            if not hit:
                missing.append(p["id"])
                continue
            p["lat"], p["lon"] = round(float(hit["lat"]), 5), round(float(hit["lon"]), 5)
            p["osm"] = f'{hit.get("osm_type", "")}/{hit.get("osm_id", "")}'
            wc = ((hit.get("extratags") or {}).get("wheelchair") or "").lower()
            if wc in ("yes", "limited", "no"):
                p["wheelchair"] = wc
            else:
                p.pop("wheelchair", None)
        p["stops"] = (hand_stops((p["lat"], p["lon"]), p["stops_by_hand"], stops) if p.get("stops_by_hand")
                      else nearest_stops((p["lat"], p["lon"]), stops))
    DEST.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Located {len(doc['places']) - len(missing)} places; not found: {missing or 'none'}")
    for p in doc["places"]:
        print(f"  {p['id']}: {len(p.get('stops') or [])} stops within {WALK_M} m"
              f"{', wheelchair ' + p['wheelchair'] if p.get('wheelchair') else ''}")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
