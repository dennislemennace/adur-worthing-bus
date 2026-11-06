#!/usr/bin/env python3
"""Write data/localities.json: every ward on the coast, with its bus service.

Route view's Areas layer draws the electoral wards of Adur, Worthing and
Brighton & Hove, shaded by how many buses a stop there gets on a weekday, and
opens a card for each: its stops, departures and routes by day, first and last
bus, late buses, how many of its stops have a shelter or step-free access
recorded, and who represents it. Asked for on 6 November 2026 ("locality stats
browser").

Wards, not NaPTAN localities, because a ward has a boundary and councillors to
write to: data/representatives.json is keyed by the same ward codes. The
boundaries are the ONS's "Wards (May 2025) UK BGC V2" (generalised to 20 m,
clipped to the coast; Open Government Licence), the edition
data/comparison_areas.json and the representatives already use.

The service figures follow scripts/build_evidence.py, for the same dated week
(data/boundary_evidence.json `measured_week`), so a ward's figure and the
boundary comparison never describe different weeks:

* departures are scheduled stop calls on that date, by each trip's calendar
  and its exceptions, not buses seen;
* a ward's stops are the stops inside it with at least one departure that
  week; departures per stop divide by those.

The timetable holds the stops in the area the site covers, so only wards
centred in that area are included (Rottingdean and Woodingdean, east of it,
would show no buses at all). A ward that reaches beyond it can be
under-counted, and says so (`inside` is false).

A day runs from 04:00 to 04:00, as a passenger lives it: "first" is the first
departure from 04:00 and "last" the last before 04:00 the next morning, so a
night bus at 02:30 is the end of the evening, not the start of the day. Buses
between half past midnight and half past four are counted on their own
(`night`).

    python scripts/build_localities.py [--db data/timetable.sqlite]
                                       [--wards-geojson saved.geojson]
                                       [--out data/localities.json]
"""

import argparse
import json
import sqlite3
import sys
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from api.main import (                                 # noqa: E402
    BBOX_MIN_LAT, BBOX_MAX_LAT, BBOX_MIN_LON, BBOX_MAX_LON,
)
from build_evidence import in_rings, service_runner     # noqa: E402

WARDS_URL = ("https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
             "WD_MAY_2025_UK_BGC_V2/FeatureServer/0/query")
DISTRICTS = ("Adur", "Worthing", "Brighton and Hove")
DAYS = ("monday", "saturday", "sunday")
LATE_FROM = 23 * 3600
DAY_STARTS = 4 * 3600
NIGHT = (int(0.5 * 3600), int(4.5 * 3600))
# Fewer wards than this means the ONS answered with something else.
MIN_WARDS = 40
SOURCE = {
    "name": "ONS Open Geography Portal",
    "dataset": "Wards (May 2025) UK BGC V2",
    "url": "https://geoportal.statistics.gov.uk/",
    "licence": "Open Government Licence v3.0",
    "attribution": "Contains OS data © Crown copyright and database right 2025. "
                   "Contains National Statistics data © Crown copyright and database right 2025.",
}
CAVEATS = [
    "Scheduled departures from the published timetable for one dated week, not buses that ran.",
    "A ward's stops are those inside it with at least one departure that week; a stop on the line "
    "between two wards counts in the one its point falls in.",
    "Only wards centred in the area this site covers are shown. Its timetable holds the stops in "
    "that area, so a ward reaching beyond it can be under-counted; such a ward is marked.",
    "A day runs from 04:00 to 04:00: the first bus is the first from 04:00, the last the last "
    "before 04:00 the next morning.",
    "Shelter and step-free access are as recorded in OpenStreetMap, which does not yet describe "
    "every stop: a low share can mean unrecorded, not missing.",
]


def fetch_wards() -> dict:
    where = "LAD25NM IN ({})".format(", ".join(f"'{d}'" for d in DISTRICTS))
    query = urllib.parse.urlencode({
        "where": where, "outFields": "WD25CD,WD25NM,LAD25CD,LAD25NM",
        "outSR": 4326, "geometryPrecision": 5, "f": "geojson"})
    req = urllib.request.Request(f"{WARDS_URL}?{query}", headers={
        "User-Agent": "adur-worthing-bus build (ward boundaries, weekly)"})
    with urllib.request.urlopen(req, timeout=90) as resp:
        return json.loads(resp.read().decode("utf-8"))


def previous_wards(path: Path) -> dict:
    """Last week's boundaries, back as GeoJSON features, for a week the ONS
    does not answer: the counts are this week's either way, so a ward's
    figure never falls a week behind the boundary comparison's."""
    doc = json.loads(path.read_text(encoding="utf-8"))
    return {"features": [{
        "properties": {"WD25CD": w["code"], "WD25NM": w["name"], "LAD25NM": w["district"]},
        "geometry": {"type": "Polygon", "coordinates": [[[x, y] for y, x in ring] for ring in w["rings"]]},
    } for w in doc.get("wards") or []]}


def ward_rings(geometry: dict) -> list:
    """Every ring, as [lon, lat], for `in_rings`' even-odd test."""
    if geometry["type"] == "Polygon":
        return geometry["coordinates"]
    return [ring for poly in geometry["coordinates"] for ring in poly]


def centred_inside(rings: list) -> bool:
    """Whether the middle of the ward's bounding box is in the site's area."""
    xs = [x for ring in rings for x, _ in ring]
    ys = [y for ring in rings for _, y in ring]
    x, y = (min(xs) + max(xs)) / 2, (min(ys) + max(ys)) / 2
    return BBOX_MIN_LON <= x <= BBOX_MAX_LON and BBOX_MIN_LAT <= y <= BBOX_MAX_LAT


def clock(secs: int) -> str:
    secs %= 86400
    return f"{secs // 3600:02d}:{secs % 3600 // 60:02d}"


def measure(con: sqlite3.Connection, wards: list, week: dict) -> tuple:
    """(`{ward code: {day: figures}}`, `{ward code: [served stop ids]}`)."""
    boxes = {}
    for w in wards:
        xs = [x for ring in w["rings"] for x, _ in ring]
        ys = [y for ring in w["rings"] for _, y in ring]
        boxes[w["code"]] = (min(xs), min(ys), max(xs), max(ys))
    ward_of, stop_id = {}, {}
    for sid, atco, lat, lon in con.execute("SELECT sid, stop_id, lat, lon FROM stops"):
        if lat is None or lon is None:
            continue
        for w in wards:
            x0, y0, x1, y1 = boxes[w["code"]]
            if x0 <= lon <= x1 and y0 <= lat <= y1 and in_rings(w["rings"], lon, lat):
                ward_of[sid] = w["code"]
                stop_id[sid] = atco
                break
    runs = service_runner(con, week)
    trip = {tid: (rid, service_id) for tid, rid, service_id in con.execute("SELECT tid, rid, service_id FROM trips")}
    short = {rid: s for rid, s in con.execute("SELECT rid, short_name FROM routes")}
    acc = {w["code"]: {d: {"departures": 0, "late": 0, "night": 0, "routes": set(), "stops": set(),
                           "first": None, "last": None} for d in DAYS} for w in wards}
    for sid, tid, dep in con.execute("SELECT sid, tid, dep_secs FROM stop_times"):
        code = ward_of.get(sid)
        if code is None or dep is None:
            continue
        rid, service_id = trip.get(tid, (None, ""))
        for day in DAYS:
            if not runs(service_id, day):
                continue
            a = acc[code][day]
            a["departures"] += 1
            a["stops"].add(sid)
            if short.get(rid):
                a["routes"].add(short[rid])
            if dep >= LATE_FROM:
                a["late"] += 1
            if NIGHT[0] <= dep % 86400 < NIGHT[1]:
                a["night"] += 1
            # On a 04:00-to-04:00 day: GTFS writes 00:30 tonight as 24:30, and
            # an 03:55 on the service day itself belongs to the night before.
            day_secs = (dep - DAY_STARTS) % 86400
            a["first"] = day_secs if a["first"] is None else min(a["first"], day_secs)
            a["last"] = day_secs if a["last"] is None else max(a["last"], day_secs)
    served = {code: sorted({stop_id[s] for d in DAYS for s in acc[code][d]["stops"]}) for code in acc}
    out = {}
    for code, days in acc.items():
        n = len(served[code])
        out[code] = {day: {
            "departures": a["departures"],
            "departures_per_stop": round(a["departures"] / n, 1) if n else None,
            "routes": len(a["routes"]),
            "route_list": sorted(a["routes"]),
            "first": clock(a["first"] + DAY_STARTS) if a["first"] is not None else None,
            "last": clock(a["last"] + DAY_STARTS) if a["last"] is not None else None,
            "departures_after_2300": a["late"],
            "night": a["night"],
        } for day, a in days.items()}
    return out, served


def access_shares(stops: list, osm: dict) -> dict:
    """How many of these stops have a shelter, and step-free access, recorded."""
    rows = [osm.get(s) or {} for s in stops]
    return {
        "stops_in_osm": sum(1 for s in stops if s in osm),
        "shelter_yes": sum(1 for r in rows if r.get("shelter") == "yes"),
        "step_free_recorded": sum(1 for r in rows if r.get("wheelchair")),
        "step_free_yes": sum(1 for r in rows if r.get("wheelchair") == "yes"),
    }


def build(wards_geojson: dict, con: sqlite3.Connection, week: dict, osm: dict, reps: dict,
          generated_on: str) -> dict:
    wards = []
    for f in wards_geojson.get("features") or []:
        p = f.get("properties") or {}
        if p.get("LAD25NM") not in DISTRICTS or not f.get("geometry"):
            continue
        rings = ward_rings(f["geometry"])
        if not centred_inside(rings):
            continue
        wards.append({"code": p["WD25CD"], "name": p["WD25NM"], "district": p["LAD25NM"],
                      "rings": rings})
    wards.sort(key=lambda w: (w["district"], w["name"]))
    stats, served = measure(con, wards, week)
    out = []
    for w in wards:
        xs = [x for ring in w["rings"] for x, _ in ring]
        ys = [y for ring in w["rings"] for _, y in ring]
        inside = (BBOX_MIN_LON <= min(xs) and max(xs) <= BBOX_MAX_LON
                  and BBOX_MIN_LAT <= min(ys) and max(ys) <= BBOX_MAX_LAT)
        rep = (reps.get("areas") or {}).get(w["code"]) or {}
        out.append({
            "code": w["code"], "name": w["name"], "district": w["district"],
            "inside": inside,
            "stops": len(served[w["code"]]),
            "days": stats[w["code"]],
            "access": access_shares(served[w["code"]], osm),
            "councillors": {"council": rep.get("council", ""),
                            "members": [{"name": m.get("name", ""), "party": m.get("party", ""),
                                         "url": m.get("url", "")} for m in rep.get("members") or []]},
            # [lat, lon], as the site's other shapes (ticket_zones.json).
            "rings": [[[round(y, 5), round(x, 5)] for x, y in ring] for ring in w["rings"]],
        })
    return {
        "_comment": ("Every ward of Adur, Worthing and Brighton & Hove with its scheduled bus service, "
                     "for Route view's Areas layer. Built weekly by scripts/build_localities.py."),
        "generated_on": generated_on,
        "measured_week": {d: week[d].isoformat() for d in DAYS},
        "source": SOURCE,
        "councillors_source": {"generated_on": reps.get("generated_on", ""),
                               "file": "data/representatives.json"},
        "caveats": CAVEATS,
        "count": len(out),
        "wards": out,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default=str(ROOT / "data" / "timetable.sqlite"))
    ap.add_argument("--evidence", default=str(ROOT / "data" / "boundary_evidence.json"))
    ap.add_argument("--osm", default=str(ROOT / "data" / "stop_osm.json"))
    ap.add_argument("--representatives", default=str(ROOT / "data" / "representatives.json"))
    ap.add_argument("--wards-geojson", help="read the ward boundaries from a file instead of the ONS")
    ap.add_argument("--out", default=str(ROOT / "data" / "localities.json"))
    args = ap.parse_args()

    try:
        wards = (json.loads(Path(args.wards_geojson).read_text()) if args.wards_geojson else fetch_wards())
    except Exception as err:                            # noqa: BLE001
        if not Path(args.out).exists():
            print(f"ERROR: ward boundaries unavailable: {err}", file=sys.stderr)
            return 1
        print(f"WARNING: ward boundaries unavailable ({err}); using last week's", file=sys.stderr)
        wards = previous_wards(Path(args.out))
    evidence = json.loads(Path(args.evidence).read_text())
    week = {d: date.fromisoformat(v) for d, v in evidence["measured_week"].items()}
    osm = json.loads(Path(args.osm).read_text()).get("stops") or {}
    reps = json.loads(Path(args.representatives).read_text())
    con = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    result = build(wards, con, week, osm, reps, date.today().isoformat())
    if result["count"] < MIN_WARDS:
        print(f"ERROR: only {result['count']} wards; refusing to publish", file=sys.stderr)
        return 1
    out = Path(args.out)
    out.write_text(json.dumps(result, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {out}: {result['count']} wards, {out.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
