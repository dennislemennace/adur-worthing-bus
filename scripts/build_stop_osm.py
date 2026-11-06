#!/usr/bin/env python3
"""Write data/stop_osm.json: what OpenStreetMap records about each stop.

Whether a stop has a shelter, a bench, a raised kerb or tactile paving, and
whether a wheelchair user can board there, is not in NaPTAN or the timetable.
OpenStreetMap carries it, mapped by volunteers — many of them using OpenStop
(https://openstop.app), which asks exactly these questions at the stop. Each
OSM stop names its NaPTAN code (`naptan:AtcoCode`), so the facts join to ours
without guessing by position.

Only the facts the stop panel shows are kept, and only values whose meaning is
settled: a free-text or misspelt value is left out rather than shown as if it
said something. A stop OSM says nothing about simply has no entry.

The output is a derived database of OpenStreetMap, so it stays under the Open
Database Licence (ODbL 1.0), not this site's CC BY, and every place it is shown
credits "© OpenStreetMap contributors". See LICENSE-content.md.

Kept apart from stops.json, like stop_details.json: it needs only Overpass and
the stop list, and if Overpass is down the weekly build keeps last week's file.

    python scripts/build_stop_osm.py [--stops data/stops.json]
                                     [--out data/stop_osm.json]
                                     [--overpass-json saved-response.json]
"""

import argparse
import json
import math
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api.main import (                                 # noqa: E402
    BBOX_MIN_LAT, BBOX_MAX_LAT, BBOX_MIN_LON, BBOX_MAX_LON,
)

# Tried in order. The main server timed out twice in a week while the site was
# being built; the mirror answers the same query from the same data.
OVERPASS_URLS = ("https://overpass-api.de/api/interpreter",
                 "https://overpass.private.coffee/api/interpreter")

# How near a separately mapped kerb must be to count as a stop's own.
KERB_RADIUS_M = 12

_BOX = f"({BBOX_MIN_LAT},{BBOX_MIN_LON},{BBOX_MAX_LAT},{BBOX_MAX_LON})"
# The stops, as nodes and as platform ways, with where each is; then the kerbs
# mapped as their own nodes beside them. A kerb is often drawn on the footway
# at the boarding point rather than tagged on the stop (6 November 2026: a
# kerb was recorded on 10 of 1,496 stops). `out body center` gives a node its
# position and a way its centre, with the tags.
QUERY = (f"[out:json][timeout:120];"
         f"(node[\"highway\"=\"bus_stop\"]{_BOX};"
         f"node[\"public_transport\"=\"platform\"][\"bus\"=\"yes\"]{_BOX};"
         f"way[\"public_transport\"=\"platform\"][\"bus\"=\"yes\"]{_BOX};)->.stops;"
         f".stops out body center;"
         f"node(around.stops:{KERB_RADIUS_M})[\"barrier\"=\"kerb\"][\"kerb\"];out body;")

# The facts the stop panel shows, and the values each may take. These are the
# answers OpenStop's own questions write.
KEEP = {
    "wheelchair": {"yes", "limited", "no"},
    "kerb": {"raised", "regular", "lowered", "flush", "rolled", "no"},
    "tactile_paving": {"yes", "no", "partial", "incorrect"},
    "shelter": {"yes", "no"},
    "bench": {"yes", "no"},
    "lit": {"yes", "no"},
    "departures_board": {"realtime", "timetable", "yes", "no"},
    "passenger_information_display": {"yes", "no"},
    "bin": {"yes", "no"},
    "covered": {"yes", "no"},
}

# Fewer stops than this matched means Overpass answered with something other
# than our area; 87% matched when this was written (October 2026).
MIN_MATCHED = 0.7

LICENCE_URL = "https://opendatacommons.org/licenses/odbl/1-0/"


def kerb_height_cm(value) -> "int | None":
    """`kerb:height` in whole centimetres: "0.16", "0.16 m", "16 cm", "160 mm".

    A bare number is metres, as the tag documents. Anything else, or a height
    no bus kerb has (over 60 cm), is not guessed at.
    """
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(m|cm|mm)?\s*", str(value or ""))
    if not m:
        return None
    n, unit = float(m.group(1)), m.group(2) or "m"
    cm = n * 100 if unit == "m" else n if unit == "cm" else n / 10
    return int(round(cm)) if 0 <= cm <= 60 else None


def _facts(tags: dict) -> dict:
    facts = {}
    for key, allowed in KEEP.items():
        value = (tags.get(key) or "").strip().lower()
        if value in allowed:
            facts[key] = value
    height = kerb_height_cm(tags.get("kerb:height"))
    if height is not None:
        facts["kerb_height_cm"] = height
    check = (tags.get("check_date") or "").strip()
    if len(check) in (7, 10) and check[:4].isdigit():
        facts["check_date"] = check
    return facts


def _where(el: dict):
    """A node's position, or a way's centre; None for neither."""
    if "lat" in el and "lon" in el:
        return el["lat"], el["lon"]
    c = el.get("center") or {}
    return (c["lat"], c["lon"]) if "lat" in c and "lon" in c else None


def _metres(a, b) -> float:
    x = math.radians(b[1] - a[1]) * math.cos(math.radians((a[0] + b[0]) / 2))
    y = math.radians(b[0] - a[0])
    return math.hypot(x, y) * 6_371_000


def parse_overpass(payload: dict) -> dict:
    """`{ATCO code: facts}` from the Overpass response.

    One stop is often mapped twice or three times: a pole node, a platform
    node, a platform way. Their facts are combined; where they disagree, the
    better-mapped object (the one with more facts) wins, since it is the one
    mappers maintain, and its id is the one linked to.

    A stop with no kerb of its own takes one from a kerb node mapped within
    KERB_RADIUS_M of it, if every such kerb says the same; it is marked
    `kerb_nearby` so the panel can say where the fact came from.
    """
    claims, kerbs = {}, []
    for el in payload.get("elements") or []:
        tags = el.get("tags") or {}
        atco = (tags.get("naptan:AtcoCode") or "").strip()
        if atco:
            facts = _facts(tags)
            claims.setdefault(atco, []).append((len(facts), el.get("id"), facts, _where(el), el.get("type")))
        elif tags.get("barrier") == "kerb":
            kind = (tags.get("kerb") or "").strip().lower()
            where = _where(el)
            if kind in KEEP["kerb"] and where:
                kerbs.append((where, kind, kerb_height_cm(tags.get("kerb:height"))))
    out = {}
    for atco, objects in claims.items():
        # Best-mapped first; on a tie, the first one seen, as before.
        objects = sorted(objects, key=lambda o: -o[0])
        merged = {"id": objects[0][1]}
        for _n, _id, facts, _w, _t in reversed(objects):
            merged.update(facts)
        merged.update(objects[0][2])
        merged["id"] = objects[0][1]
        # Linked to by id, so a platform drawn as a way says it is one.
        if objects[0][4] == "way":
            merged["way"] = True
        if "kerb" not in merged:
            places = [o[3] for o in objects if o[3]]
            near = [(kind, h) for where, kind, h in kerbs
                    if any(_metres(where, p) <= KERB_RADIUS_M for p in places)]
            if near and len({k for k, _ in near}) == 1:
                merged["kerb"] = near[0][0]
                merged["kerb_nearby"] = True
                heights = {h for _, h in near if h is not None}
                if len(heights) == 1 and "kerb_height_cm" not in merged:
                    merged["kerb_height_cm"] = heights.pop()
        out[atco] = merged
    return out


def build(stops: list, osm: dict, osm_base: str = "") -> dict:
    """Our stops' entries, plus the provenance and licence they ship with."""
    matched = {s["atco_code"]: osm[s["atco_code"]] for s in stops if s.get("atco_code") in osm}
    return {
        "source": "© OpenStreetMap contributors",
        "source_url": "https://www.openstreetmap.org/copyright",
        "licence": "ODbL-1.0",
        "licence_url": LICENCE_URL,
        "generated_on": date.today().isoformat(),
        "osm_base": osm_base,
        "fields": sorted(KEEP) + ["kerb_height_cm", "kerb_nearby", "way"],
        "count": len(matched),
        "stops": matched,
    }


def fetch() -> dict:
    data = urllib.parse.urlencode({"data": QUERY}).encode()
    last = None
    for url in OVERPASS_URLS:
        for attempt in range(2):
            try:
                req = urllib.request.Request(url, data=data, headers={
                    "User-Agent": "adur-worthing-bus build (stop accessibility, weekly)"})
                with urllib.request.urlopen(req, timeout=150) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except Exception as err:                    # noqa: BLE001 — try the next
                last = err
                time.sleep(10 * (attempt + 1))
    raise RuntimeError(f"Overpass unavailable: {last}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--stops", default=str(ROOT / "data" / "stops.json"))
    ap.add_argument("--out", default=str(ROOT / "data" / "stop_osm.json"))
    ap.add_argument("--overpass-json", help="read a saved Overpass response instead of asking")
    args = ap.parse_args()

    stops = json.loads(Path(args.stops).read_text())["stops"]
    try:
        payload = (json.loads(Path(args.overpass_json).read_text()) if args.overpass_json
                   else fetch())
    except Exception as err:                            # noqa: BLE001
        print(f"ERROR: {err}", file=sys.stderr)
        return 1
    result = build(stops, parse_overpass(payload),
                   (payload.get("osm3s") or {}).get("timestamp_osm_base", ""))
    if result["count"] < MIN_MATCHED * len(stops):
        print(f"ERROR: OpenStreetMap matched {result['count']} of {len(stops)} stops; "
              "refusing to publish", file=sys.stderr)
        return 1
    out = Path(args.out)
    out.write_text(json.dumps(result, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {out}: {result['count']} of {len(stops)} stops, {out.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
