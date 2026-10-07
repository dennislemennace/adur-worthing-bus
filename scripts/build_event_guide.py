"""Work out the derived parts of an event travel guide (data/conference.json).

The guide's words are written by hand. Three things in it are not, and come
from here so they can be checked and rebuilt:

  stops   data/conference_stops.json: every stop with a bus that later calls at
          one of the venue's stops, for "Find my nearest stop".
  walks   walk_minutes on every place in the guide, and each hotel area's
          quickest way (walk if 15 minutes or less, otherwise bus).
  direct  the direct routes from near a point, ranked, to paste into a hotel
          area's or arrival point's `buses`.

Settings live in the guide's `derive` block: the venue's stops, the sample day
and hours, and the trip floors. Walking minutes are straight-line metres from
`venue.walk_point`, times 1.3 for real streets, at 80 metres a minute;
tests/test_conference.py holds the guide to that formula.

  python scripts/build_event_guide.py stops
  python scripts/build_event_guide.py walks
  python scripts/build_event_guide.py direct 50.8297 -0.1412 --radius 250
"""

import argparse
import collections
import json
import math
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

GUIDE = ROOT / "data" / "conference.json"
STOPS_OUT = ROOT / "data" / "conference_stops.json"


def metres(lat, lon, origin):
    return math.hypot((lat - origin[0]) * 111_195,
                      (lon - origin[1]) * 111_195 * math.cos(math.radians(origin[0])))


def walk_minutes(lat, lon, origin):
    return max(1, round(metres(lat, lon, origin) * 1.3 / 80))


def route_order(route):
    """Lowest number first, as the page lists them: 1, 1X, 5B, 12, 700, N700."""
    m = re.match(r"^([A-Z]*)(\d+)(.*)$", route, re.I)
    return (int(m.group(2)), m.group(1), m.group(3)) if m else (9999, "", route)


def to_venue(db, derive):
    """stop -> route -> trips that call there and later at a venue stop,
    counted at the boarding stop inside the sample hours."""
    from api.timetable_db import Timetable

    tt = Timetable(Path(db), allow_fetch=False)
    venue = set(derive["venue_stops"])
    day = date.fromisoformat(derive["day"])
    lo, hi = (int(h) * 3600 + int(m) * 60 for h, m in
              (t.split(":") for t in (derive["from"], derive["to"])))
    out = collections.defaultdict(collections.Counter)
    for tid, trip in tt.trips.items():
        if not tt.runs_on(trip["service_id"], day):
            continue
        calls = [c for c in tt.trip_stops_for(tid) if c[0] is not None]
        at_venue = [i for i, (_, atco) in enumerate(calls) if atco in venue]
        if not at_venue:
            continue
        route = (tt.routes.get(trip["route_id"]) or {}).get("short_name", "")
        for secs, atco in calls[:max(at_venue)]:
            if atco not in venue and lo <= secs <= hi:
                out[atco][route] += 1
    return out, tt


def cmd_stops(args, guide):
    derive = guide["derive"]
    counts, _ = to_venue(args.db, derive)
    stops = {s["atco_code"]: s for s in json.loads((ROOT / "data" / "stops.json").read_text())["stops"]}
    rows = []
    for atco, routes in counts.items():
        s = stops.get(atco)
        if not s:
            continue
        kept = sorted([r for r, n in routes.items() if n >= derive["locator_min_trips"]], key=lambda r: -routes[r])
        if kept:
            rows.append({"atco": atco, "name": s["name"], "lat": s["latitude"], "lon": s["longitude"],
                         "towards": s.get("towards") or "", "routes": sorted(kept[:8], key=route_order)})
    rows.sort(key=lambda r: r["atco"])
    comment = (f"Stops in the map area with a bus that later calls at one of the venue's stops "
               f"(at least {derive['locator_min_trips']} trips, {derive['from']} to {derive['to']}, "
               f"{derive['day']}, from the timetable). Built by scripts/build_event_guide.py stops; "
               f"read by the event guide's Find my nearest stop.")
    STOPS_OUT.write_text(json.dumps({"_comment": comment, "stops": rows}, separators=(",", ":")) + "\n")
    print(f"{len(rows)} stops -> {STOPS_OUT.relative_to(ROOT)}")


def places(guide):
    yield guide["station"]
    for key in ("coaches", "social_venues", "taxi_ranks", "hotel_areas"):
        yield from guide.get(key, [])
    yield from guide.get("driving", {}).get("car_parks", [])


def cmd_walks(args, guide):
    origin = guide["venue"]["walk_point"]
    changed = 0
    for p in places(guide):
        w = walk_minutes(p["lat"], p["lon"], origin)
        changed += p.get("walk_minutes") != w
        p["walk_minutes"] = w
    for h in guide.get("hotel_areas", []):
        h["quickest"] = "walk" if h["walk_minutes"] <= 15 or not h.get("buses") else "bus"
    GUIDE.write_text(json.dumps(guide, indent=2, ensure_ascii=False) + "\n")
    print(f"walk_minutes refreshed, {changed} changed")


def cmd_direct(args, guide):
    derive = guide["derive"]
    counts, tt = to_venue(args.db, derive)
    agg = collections.Counter()
    for atco, routes in counts.items():
        s = tt.stops.get(atco) or {}
        if s.get("lat") is not None and metres(s["lat"], s["lon"], (args.lat, args.lon)) <= args.radius:
            for r, n in routes.items():
                agg[r] = max(agg[r], n)
    ranked = [r for r, n in agg.most_common() if n >= derive["direct_min_trips"]]
    print(json.dumps(ranked[:args.limit]))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(ROOT / "data" / "timetable.sqlite"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("stops")
    sub.add_parser("walks")
    d = sub.add_parser("direct")
    d.add_argument("lat", type=float)
    d.add_argument("lon", type=float)
    d.add_argument("--radius", type=float, default=300)
    d.add_argument("--limit", type=int, default=8)
    args = ap.parse_args()
    guide = json.loads(GUIDE.read_text())
    {"stops": cmd_stops, "walks": cmd_walks, "direct": cmd_direct}[args.cmd](args, guide)


if __name__ == "__main__":
    main()
