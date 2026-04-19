"""
scripts/build_zones.py
======================
Generates GeoJSON ticket-zone boundaries from GTFS stop data.

For each zone in scripts/zone_definitions.json:
  1. Find route_ids whose short_name matches the zone's route list
  2. Collect stops served by those routes, filtered by ATCO prefix
  3. Buffer each stop point by BUFFER_M metres (in British National Grid),
     union all buffers, simplify, reproject back to WGS84
  4. Write data/zones/<id>.geojson and data/zones/index.json

Run locally:
    pip install shapely pyproj
    python scripts/build_zones.py
"""

import json
import logging
import math
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("build_zones")

try:
    from shapely.geometry import Point, mapping
    from shapely.ops import unary_union, transform as shp_transform
    from pyproj import Transformer
except ImportError as exc:
    log.error("Missing dependency: %s\nRun: pip install shapely pyproj", exc)
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────
TIMETABLE_PATH = Path(__file__).parent.parent / "data" / "timetable.json"
ZONE_DEFS_PATH = Path(__file__).parent / "zone_definitions.json"
ZONES_DIR      = Path(__file__).parent.parent / "data" / "zones"

BUFFER_M   = 600   # metres — joins closely-spaced stops into a contiguous shape
SIMPLIFY_M = 80    # metres — smooths the polygon without losing meaningful detail


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# EPSG:27700 = British National Grid (metric, accurate for UK geometry)
_to_bng   = Transformer.from_crs("EPSG:4326", "EPSG:27700", always_xy=True)
_to_wgs84 = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)


def main() -> None:
    log.info("Loading timetable from %s", TIMETABLE_PATH)
    if not TIMETABLE_PATH.exists():
        log.error("timetable.json not found — run build_timetable.py first")
        sys.exit(1)
    timetable = json.loads(TIMETABLE_PATH.read_text())

    log.info("Loading zone definitions from %s", ZONE_DEFS_PATH)
    zone_defs = json.loads(ZONE_DEFS_PATH.read_text())["zones"]

    ZONES_DIR.mkdir(parents=True, exist_ok=True)

    # ── Build lookup indices ──────────────────────────────────
    route_short_names: dict[str, str] = {
        rid: r["short_name"]
        for rid, r in timetable["routes"].items()
    }

    trip_to_route: dict[str, str] = {
        tid: t["route_id"]
        for tid, t in timetable["trips"].items()
    }

    log.info("Building trip→stops index from stop_times…")
    trip_stops: dict[str, set[str]] = {}
    for stop_id, times in timetable["stop_times"].items():
        for _dep_secs, trip_id in times:
            trip_stops.setdefault(trip_id, set()).add(stop_id)
    log.info("  %d trips indexed", len(trip_stops))

    stop_coords: dict[str, tuple[float, float]] = {
        sid: (s["lat"], s["lon"])
        for sid, s in timetable["stops"].items()
    }

    # ── Generate each zone ────────────────────────────────────
    manifest: list[dict] = []

    for zone in zone_defs:
        zone_id     = zone["id"]
        route_names = set(zone["routes"])
        prefix      = zone.get("atco_prefix", "")

        log.info("Zone: %s", zone_id)

        matching_route_ids = {
            rid for rid, sn in route_short_names.items()
            if sn in route_names
        }
        if not matching_route_ids:
            log.warning("  No route_ids matched routes %s — skipping", sorted(route_names))
            continue
        log.info("  %d route_ids matched", len(matching_route_ids))

        matching_trip_ids = {
            tid for tid, rid in trip_to_route.items()
            if rid in matching_route_ids
        }

        zone_stop_ids: set[str] = set()
        for tid in matching_trip_ids:
            zone_stop_ids.update(trip_stops.get(tid, set()))

        if prefix:
            zone_stop_ids = {sid for sid in zone_stop_ids if sid.startswith(prefix)}

        log.info("  %d stops (prefix filter: %r)", len(zone_stop_ids), prefix or "none")

        if len(zone_stop_ids) < 5:
            log.warning("  Too few stops — skipping %s", zone_id)
            continue

        coords = [stop_coords[sid] for sid in zone_stop_ids if sid in stop_coords]

        # Optional radius filter — excludes stops on distant route branches
        max_km = zone.get("max_km")
        if max_km:
            c_lat = zone["center_lat"]
            c_lon = zone["center_lon"]
            coords = [
                (lat, lon) for lat, lon in coords
                if _haversine_km(c_lat, c_lon, lat, lon) <= max_km
            ]
            log.info("  %d stops after %g km radius filter", len(coords), max_km)

        if len(coords) < 5:
            log.warning("  Too few coords — skipping %s", zone_id)
            continue

        # Project to BNG, buffer, union, simplify, reproject
        bng_points = [
            shp_transform(_to_bng.transform, Point(lon, lat))
            for lat, lon in coords
        ]
        union_bng  = unary_union([p.buffer(BUFFER_M) for p in bng_points])
        simple_bng = union_bng.simplify(SIMPLIFY_M, preserve_topology=True)
        wgs84_geom = shp_transform(_to_wgs84.transform, simple_bng)

        feature = {
            "type": "Feature",
            "properties": {
                "id":           zone_id,
                "name":         zone["name"],
                "operator_ref": zone["operator_ref"],
                "color":        zone["color"],
                "route_count":  len(matching_route_ids),
                "stop_count":   len(coords),
            },
            "geometry": mapping(wgs84_geom),
        }

        out_path = ZONES_DIR / f"{zone_id}.geojson"
        out_path.write_text(json.dumps(feature, separators=(",", ":")))
        log.info("  → %s (%d KB, %d stops)", out_path.name,
                 out_path.stat().st_size // 1024, len(coords))

        manifest.append({
            "id":           zone_id,
            "name":         zone["name"],
            "operator_ref": zone["operator_ref"],
            "color":        zone["color"],
            "path":         f"data/zones/{zone_id}.geojson",
        })

    index_path = ZONES_DIR / "index.json"
    index_path.write_text(json.dumps({"zones": manifest}, separators=(",", ":")))
    log.info("Done — %d zones written to %s", len(manifest), ZONES_DIR)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        import traceback
        log.error("FATAL: %s", exc)
        traceback.print_exc()
        sys.exit(1)
