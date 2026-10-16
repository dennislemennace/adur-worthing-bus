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

QUERY = (f"[out:json][timeout:120];"
         f"(node[\"highway\"=\"bus_stop\"]({BBOX_MIN_LAT},{BBOX_MIN_LON},{BBOX_MAX_LAT},{BBOX_MAX_LON});"
         f"node[\"public_transport\"=\"platform\"][\"bus\"=\"yes\"]"
         f"({BBOX_MIN_LAT},{BBOX_MIN_LON},{BBOX_MAX_LAT},{BBOX_MAX_LON}););out tags;")

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
}

# Fewer stops than this matched means Overpass answered with something other
# than our area; 87% matched when this was written (October 2026).
MIN_MATCHED = 0.7

LICENCE_URL = "https://opendatacommons.org/licenses/odbl/1-0/"


def parse_overpass(payload: dict) -> dict:
    """`{ATCO code: facts}` from an Overpass `out tags` response.

    Where two OSM objects claim one code (a stop node and its platform), the
    one with more of the kept facts wins: it is the one mappers maintain.
    """
    out = {}
    for el in payload.get("elements") or []:
        tags = el.get("tags") or {}
        atco = (tags.get("naptan:AtcoCode") or "").strip()
        if not atco:
            continue
        facts = {"id": el.get("id")}
        for key, allowed in KEEP.items():
            value = (tags.get(key) or "").strip().lower()
            if value in allowed:
                facts[key] = value
        check = (tags.get("check_date") or "").strip()
        if len(check) in (7, 10) and check[:4].isdigit():
            facts["check_date"] = check
        held = out.get(atco)
        if held is None or len(facts) > len(held):
            out[atco] = facts
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
        "fields": sorted(KEEP),
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
