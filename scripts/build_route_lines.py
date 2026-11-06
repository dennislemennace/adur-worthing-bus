#!/usr/bin/env python3
"""Write data/route_lines.json: every route's line, for Route view.

The same routes /api/route-lines serves (`Timetable.representative_polylines`),
built once a week with the timetable instead of on the API. On Render's free
tier the API took 14 seconds to build them and 3 to 4 seconds to send them even
from its cache, and the build held the one CPU while a visitor's departure
board waited behind it (measured 6 November 2026). As a file on GitHub Pages,
like data/stops.json, Route view draws without the API at all.

Each line is thinned with Douglas-Peucker at a few metres, which drops the
road shape's closely spaced points without moving the line off the road at any
zoom the map allows. Route frequency is measured on a typical Tuesday
(`service_frequency`), not today, so the file holds for the week.

    python scripts/build_route_lines.py [--db data/timetable.sqlite]
                                        [--out data/route_lines.json]
"""

import argparse
import json
import math
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api.main import (                                 # noqa: E402
    BBOX_MIN_LAT, BBOX_MAX_LAT, BBOX_MIN_LON, BBOX_MAX_LON,
)
from api.timetable_db import Timetable                 # noqa: E402

# Metres a thinned line may stray from the original. Under the width of the
# drawn line (4 px) at zoom 17, where a pixel is about 1.2 m here.
TOLERANCE_M = 4.0
# Fewer routes than this means the database or the box is wrong, not that
# the network shrank: refuse rather than publish an empty Route view.
MIN_ROUTES = 40


def _xy(point, lat0):
    """Metres east and north of the origin, near enough at this scale."""
    lat, lon = point
    return (lon * 111_320 * math.cos(math.radians(lat0)), lat * 110_540)


def _offset(p, a, b):
    """How far p is from the segment a-b, in the units of the points."""
    (px, py), (ax, ay), (bx, by) = p, a, b
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return math.hypot(px - ax, py - ay)
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    return math.hypot(px - (ax + t * dx), py - (ay + t * dy))


def simplify(coords, tolerance_m=TOLERANCE_M):
    """Douglas-Peucker on [lat, lon] pairs; the first and last points stay."""
    if len(coords) < 3:
        return [[round(c[0], 5), round(c[1], 5)] for c in coords]
    lat0 = coords[0][0]
    xy = [_xy(c, lat0) for c in coords]
    keep = [False] * len(coords)
    keep[0] = keep[-1] = True
    stack = [(0, len(coords) - 1)]
    while stack:
        i, j = stack.pop()
        worst, at = 0.0, None
        for k in range(i + 1, j):
            d = _offset(xy[k], xy[i], xy[j])
            if d > worst:
                worst, at = d, k
        if at is not None and worst > tolerance_m:
            keep[at] = True
            stack.extend(((i, at), (at, j)))
    return [[round(c[0], 5), round(c[1], 5)] for c, k in zip(coords, keep) if k]


def build(routes, generated_on):
    out = [{**r, "polylines": [simplify(line) for line in r.get("polylines") or []]} for r in routes]
    return {
        "generated_on": generated_on,
        "source": "The week's published timetable (Bus Open Data Service GTFS), as /api/route-lines",
        "tolerance_m": TOLERANCE_M,
        "count": len(out),
        "routes": out,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default=str(ROOT / "data" / "timetable.sqlite"))
    ap.add_argument("--out", default=str(ROOT / "data" / "route_lines.json"))
    args = ap.parse_args()

    tt = Timetable(Path(args.db), allow_fetch=False)
    if not tt.ok():
        print(f"ERROR: no timetable at {args.db}", file=sys.stderr)
        return 1
    routes = tt.representative_polylines(bbox=(BBOX_MIN_LAT, BBOX_MAX_LAT, BBOX_MIN_LON, BBOX_MAX_LON))
    if len(routes) < MIN_ROUTES:
        print(f"ERROR: only {len(routes)} routes in the box; refusing to publish", file=sys.stderr)
        return 1
    result = build(routes, date.today().isoformat())
    out = Path(args.out)
    out.write_text(json.dumps(result, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    before = sum(len(line) for r in routes for line in r.get("polylines") or [])
    after = sum(len(line) for r in result["routes"] for line in r["polylines"])
    print(f"Wrote {out}: {result['count']} routes, {after:,} of {before:,} points, "
          f"{out.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
