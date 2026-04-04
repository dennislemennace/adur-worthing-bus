"""
api/main.py — Adur & Worthing Bus Tracker Backend v1.2
=======================================================

Changes from v1.1:
  - Fixed NOC codes for Stagecoach South (SCSO not SCSC)
  - GTFS zip parser now handles subdirectories inside the zip
  - Fixed BODS download authentication (query param not header)
  - /api/stops now served from GTFS stops.txt so ATCO codes
    always match the timetable lookup
  - Added fallback to Overpass if GTFS stops are unavailable

Environment variables required:
  BODS_API_KEY    — from https://data.bus-data.dft.gov.uk/
  ALLOWED_ORIGIN  — your GitHub Pages URL
"""

import csv
import io
import os
import time
import logging
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, date, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ────────────────────────────────────────────────────────────
# LOGGING
# ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")
log = logging.getLogger("bus_api")

# ────────────────────────────────────────────────────────────
# CONFIGURATION
# ────────────────────────────────────────────────────────────
BODS_API_KEY   = os.environ.get("BODS_API_KEY", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")

BODS_BASE = "https://data.bus-data.dft.gov.uk/api/v1"
SIRI_NS   = "http://www.siri.org.uk/siri"

# Bounding box for Adur & Worthing
BBOX_MIN_LAT, BBOX_MAX_LAT =  50.78,  50.87
BBOX_MIN_LON, BBOX_MAX_LON = -0.42,  -0.10
BBOX_STR = f"{BBOX_MIN_LON},{BBOX_MIN_LAT},{BBOX_MAX_LON},{BBOX_MAX_LAT}"

# National Operator Codes for operators serving Adur & Worthing.
# To find more: check operator_ref in /api/vehicles, or search
# https://data.bus-data.dft.gov.uk/operators/
AREA_OPERATOR_NOCS = [
    "SCSO",   # Stagecoach South (was incorrectly SCSC before)
    "BHBC",   # Brighton & Hove Bus and Coach Company
    "CMPA",   # Compass Travel (Sussex)
    "METR",   # Metrobus
]

GTFS_CACHE_TTL = 86_400  # 24 hours

# ────────────────────────────────────────────────────────────
# IN-MEMORY CACHE
# ────────────────────────────────────────────────────────────
_cache: dict = {}

def cache_get(key: str):
    entry = _cache.get(key)
    if not entry:
        return None
    data, expires_at = entry
    if time.time() > expires_at:
        del _cache[key]
        return None
    return data

def cache_set(key: str, data, ttl_seconds: int) -> None:
    _cache[key] = (data, time.time() + ttl_seconds)

# ────────────────────────────────────────────────────────────
# GTFS DATA STORE
# ────────────────────────────────────────────────────────────
_gtfs_data:      Optional[dict] = None
_gtfs_cached_at: float          = 0.0

# ────────────────────────────────────────────────────────────
# APP
# ────────────────────────────────────────────────────────────
app = FastAPI(title="Adur & Worthing Bus API", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN] if ALLOWED_ORIGIN != "*" else ["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ────────────────────────────────────────────────────────────
# HEALTH CHECK
# ────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    gtfs = _gtfs_data or {}
    return {
        "status":             "ok",
        "bods_key_configured": bool(BODS_API_KEY),
        "gtfs_loaded":         _gtfs_data is not None,
        "gtfs_stops_indexed":  len(gtfs.get("stop_times", {})),
        "gtfs_stops_available":len(gtfs.get("stops", {})),
        "gtfs_routes":         len(gtfs.get("routes", {})),
    }

# ────────────────────────────────────────────────────────────
# ENDPOINT: /api/stops
# Served from GTFS stops.txt so ATCO codes always match the
# timetable. Falls back to Overpass if GTFS not yet loaded.
# ────────────────────────────────────────────────────────────
@app.get("/api/stops")
async def get_stops():
    """Bus stops filtered to the Adur & Worthing bounding box."""
    cached = cache_get("stops")
    if cached:
        return cached

    # Try GTFS first (best ATCO code accuracy)
    try:
        gtfs = await _get_gtfs_data()
        stops = _gtfs_stops_in_bbox(gtfs)
        if stops:
            log.info("Serving %d stops from GTFS stops.txt", len(stops))
            result = {"stops": stops, "count": len(stops), "source": "gtfs"}
            cache_set("stops", result, GTFS_CACHE_TTL)
            return result
    except Exception as exc:
        log.warning("GTFS stop load failed, falling back to Overpass: %s", exc)

    # Fallback: Overpass (OSM) — ATCO codes may not match timetable
    stops = await _fetch_overpass_stops()
    result = {"stops": stops, "count": len(stops), "source": "overpass"}
    cache_set("stops", result, GTFS_CACHE_TTL)
    log.info("Serving %d stops from Overpass (fallback)", len(stops))
    return result


def _gtfs_stops_in_bbox(gtfs: dict) -> list[dict]:
    """Filter GTFS stops to the Adur & Worthing bounding box."""
    stops = []
    for stop_id, stop in gtfs.get("stops", {}).items():
        lat = stop.get("lat")
        lon = stop.get("lon")
        if lat is None or lon is None:
            continue
        if not (BBOX_MIN_LAT <= lat <= BBOX_MAX_LAT and
                BBOX_MIN_LON <= lon <= BBOX_MAX_LON):
            continue
        stops.append({
            "atco_code": stop_id,
            "name":      stop.get("name", "Bus Stop"),
            "latitude":  lat,
            "longitude": lon,
        })
    return stops


async def _fetch_overpass_stops() -> list[dict]:
    """Fallback stop source using OpenStreetMap Overpass API."""
    query = f"""
    [out:json][timeout:30];
    node["highway"="bus_stop"]
      ({BBOX_MIN_LAT},{BBOX_MIN_LON},{BBOX_MAX_LAT},{BBOX_MAX_LON});
    out body;
    """
    async with httpx.AsyncClient(timeout=40) as client:
        try:
            resp = await client.post(
                "https://overpass-api.de/api/interpreter",
                data={"data": query},
            )
            resp.raise_for_status()
            raw = resp.json()
        except Exception as exc:
            raise HTTPException(status_code=502,
                                detail=f"Overpass API error: {exc}")

    stops = []
    for el in raw.get("elements", []):
        if el.get("type") != "node":
            continue
        lat, lon = el.get("lat"), el.get("lon")
        if lat is None or lon is None:
            continue
        tags = el.get("tags", {})
        atco = (tags.get("naptan:AtcoCode") or tags.get("ref")
                or str(el["id"]))
        name = (tags.get("name") or tags.get("naptan:CommonName")
                or "Bus Stop")
        stops.append({
            "atco_code": atco,
            "name":      name,
            "latitude":  lat,
            "longitude": lon,
        })
    return stops

# ────────────────────────────────────────────────────────────
# ENDPOINT: /api/vehicles
# ────────────────────────────────────────────────────────────
@app.get("/api/vehicles")
async def get_vehicles():
    """Live bus positions from BODS SIRI-VM. Cached 15 s."""
    _check_api_key()
    cached = cache_get("vehicles")
    if cached:
        return cached

    vehicles = await _fetch_siri_vm()
    result = {"vehicles": vehicles, "count": len(vehicles)}
    cache_set("vehicles", result, 15)
    log.info("Fetched %d vehicles from BODS SIRI-VM", len(vehicles))
    return result


async def _fetch_siri_vm() -> list[dict]:
    url    = f"{BODS_BASE}/datafeed/"
    params = {"api_key": BODS_API_KEY, "boundingBox": BBOX_STR}
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=502,
                detail=f"BODS SIRI-VM error: {exc.response.status_code}")
        except httpx.RequestError:
            raise HTTPException(status_code=502,
                detail="Could not reach BODS API.")
    return _parse_siri_vm(resp.text)


def _parse_siri_vm(xml_text: str) -> list[dict]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    ns       = {"s": SIRI_NS}
    vehicles = []

    for activity in root.findall(".//s:VehicleActivity", ns):
        journey = activity.find("s:MonitoredVehicleJourney", ns)
        if journey is None:
            continue

        def jtext(tag):
            el = journey.find(f"s:{tag}", ns)
            return el.text.strip() if el is not None and el.text else ""

        loc = journey.find("s:VehicleLocation", ns)
        if loc is None:
            continue
        try:
            lat = float(loc.find("s:Latitude",  ns).text)
            lon = float(loc.find("s:Longitude", ns).text)
        except (TypeError, ValueError, AttributeError):
            continue

        delay_el = activity.find(".//s:Delay", ns)
        delay_s  = _parse_iso_duration(delay_el.text) if delay_el is not None else None
        rec_el   = activity.find("s:RecordedAtTime", ns)

        vehicles.append({
            "vehicle_ref":   jtext("VehicleRef"),
            "service_ref":   jtext("PublishedLineName") or jtext("LineRef"),
            "operator_ref":  jtext("OperatorRef"),
            "destination":   jtext("DestinationName") or jtext("DirectionRef"),
            "latitude":      lat,
            "longitude":     lon,
            "bearing":       _safe_float(jtext("Bearing")),
            "delay_seconds": delay_s,
            "recorded_at":   rec_el.text.strip() if rec_el is not None and rec_el.text else None,
        })
    return vehicles

# ────────────────────────────────────────────────────────────
# ENDPOINT: /api/departures
# ────────────────────────────────────────────────────────────
@app.get("/api/departures")
async def get_departures(stopId: str = Query(...)):
    """Scheduled departures for a stop from BODS GTFS. Cached 60 s."""
    _check_api_key()

    if not stopId or len(stopId) > 20:
        raise HTTPException(status_code=400, detail="Invalid stopId.")

    cached = cache_get(f"dep:{stopId}")
    if cached:
        return cached

    gtfs   = await _get_gtfs_data()
    result = _get_stop_departures(gtfs, stopId)
    cache_set(f"dep:{stopId}", result, 60)
    return result

# ────────────────────────────────────────────────────────────
# GTFS LOADER
# ────────────────────────────────────────────────────────────
async def _get_gtfs_data() -> dict:
    global _gtfs_data, _gtfs_cached_at
    if _gtfs_data and (time.time() - _gtfs_cached_at) < GTFS_CACHE_TTL:
        return _gtfs_data

    log.info("Downloading fresh GTFS timetable data…")
    _gtfs_data      = await _fetch_and_merge_gtfs()
    _gtfs_cached_at = time.time()
    log.info(
        "GTFS ready — %d stops, %d stop_time entries, %d routes",
        len(_gtfs_data.get("stops", {})),
        len(_gtfs_data.get("stop_times", {})),
        len(_gtfs_data.get("routes", {})),
    )
    return _gtfs_data


async def _fetch_and_merge_gtfs() -> dict:
    merged: dict = {
        "stops":          {},
        "routes":         {},
        "trips":          {},
        "stop_times":     {},
        "calendar":       {},
        "calendar_dates": {},
    }

    urls = await _discover_gtfs_urls()
    if not urls:
        log.warning("No GTFS dataset URLs found — check NOC codes and API key")
        return merged

    for url in urls:
        try:
            parsed = await _download_and_parse_gtfs(url)
            # Merge stops
            merged["stops"].update(parsed.get("stops", {}))
            merged["routes"].update(parsed.get("routes", {}))
            merged["trips"].update(parsed.get("trips", {}))
            merged["calendar"].update(parsed.get("calendar", {}))
            # Merge calendar_dates
            for sid, dates in parsed.get("calendar_dates", {}).items():
                if sid not in merged["calendar_dates"]:
                    merged["calendar_dates"][sid] = {}
                merged["calendar_dates"][sid].update(dates)
            # Append stop_times
            for stop_id, times in parsed.get("stop_times", {}).items():
                if stop_id not in merged["stop_times"]:
                    merged["stop_times"][stop_id] = []
                merged["stop_times"][stop_id].extend(times)
        except Exception as exc:
            log.warning("Skipping dataset %s — %s", url, exc)

    return merged


async def _discover_gtfs_urls() -> list[str]:
    """
    Query BODS dataset API for each operator NOC and collect
    the GTFS zip download URLs.
    """
    urls = []
    seen = set()

    async with httpx.AsyncClient(timeout=30) as client:
        for noc in AREA_OPERATOR_NOCS:
            try:
                resp = await client.get(
                    f"{BODS_BASE}/dataset/",
                    params={
                        "api_key": BODS_API_KEY,
                        "noc":     noc,
                        "status":  "published",
                        "limit":   20,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                log.warning("BODS dataset query failed for NOC %s: %s", noc, exc)
                continue

            results = data.get("results", [])
            log.info("NOC %s — %d dataset(s) found", noc, len(results))

            for dataset in results:
                # The download URL field name varies — try both
                dl_url = dataset.get("url") or dataset.get("download_url", "")
                if not dl_url:
                    continue
                # Ensure it is a zip download URL
                if "/download/" not in dl_url and not dl_url.endswith(".zip"):
                    dl_url = dl_url.rstrip("/") + "/download/"
                if dl_url and dl_url not in seen:
                    seen.add(dl_url)
                    urls.append(dl_url)
                    log.info("  Dataset URL: %s", dl_url)

    log.info("Total GTFS datasets to download: %d", len(urls))
    return urls


async def _download_and_parse_gtfs(download_url: str) -> dict:
    """
    Download a GTFS zip from BODS.
    Authentication uses api_key query param (not header).
    """
    log.info("Downloading %s", download_url)
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        resp = await client.get(
            download_url,
            params={"api_key": BODS_API_KEY},
        )
        resp.raise_for_status()
        zip_bytes = resp.content

    log.info("Downloaded %.1f MB — parsing…", len(zip_bytes) / 1_048_576)
    return _parse_gtfs_zip(zip_bytes)


def _parse_gtfs_zip(zip_bytes: bytes) -> dict:
    """
    Parse a GTFS zip into our internal structure.

    Handles zips where files are at the root OR inside a subdirectory
    (both are common with BODS-provided zips).
    """
    result: dict = {
        "stops":          {},
        "routes":         {},
        "trips":          {},
        "stop_times":     {},
        "calendar":       {},
        "calendar_dates": {},
    }

    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            all_names = zf.namelist()
            log.info("Zip contains %d files: %s…", len(all_names),
                     all_names[:8])

            def find_file(filename: str) -> Optional[str]:
                """
                Find a file in the zip regardless of subdirectory.
                Looks for exact match first, then suffix match.
                """
                # Exact match (file at root)
                if filename in all_names:
                    return filename
                # File inside a subdirectory
                for name in all_names:
                    if name.endswith("/" + filename):
                        return name
                return None

            def read_csv(filename: str):
                """Read a CSV from the zip with UTF-8 BOM handling."""
                path = find_file(filename)
                if not path:
                    log.warning("  %s not found in zip", filename)
                    return []
                with zf.open(path) as f:
                    return list(csv.DictReader(
                        io.TextIOWrapper(f, encoding="utf-8-sig")
                    ))

            # ── stops.txt ──────────────────────────────────────
            stop_count = 0
            for row in read_csv("stops.txt"):
                stop_id = row.get("stop_id", "").strip()
                if not stop_id:
                    continue
                try:
                    lat = float(row.get("stop_lat", 0))
                    lon = float(row.get("stop_lon", 0))
                except (ValueError, TypeError):
                    continue
                result["stops"][stop_id] = {
                    "name": row.get("stop_name", "Bus Stop").strip(),
                    "lat":  lat,
                    "lon":  lon,
                }
                stop_count += 1
            log.info("  Parsed %d stops", stop_count)

            # ── routes.txt ─────────────────────────────────────
            route_count = 0
            for row in read_csv("routes.txt"):
                route_id = row.get("route_id", "").strip()
                if not route_id:
                    continue
                result["routes"][route_id] = {
                    "short_name": row.get("route_short_name", "").strip(),
                    "long_name":  row.get("route_long_name",  "").strip(),
                    "agency_id":  row.get("agency_id",        "").strip(),
                }
                route_count += 1
            log.info("  Parsed %d routes", route_count)

            # ── trips.txt ──────────────────────────────────────
            trip_count = 0
            for row in read_csv("trips.txt"):
                trip_id = row.get("trip_id", "").strip()
                if not trip_id:
                    continue
                result["trips"][trip_id] = {
                    "route_id":   row.get("route_id",      "").strip(),
                    "service_id": row.get("service_id",    "").strip(),
                    "headsign":   row.get("trip_headsign", "").strip(),
                }
                trip_count += 1
            log.info("  Parsed %d trips", trip_count)

            # ── calendar.txt ───────────────────────────────────
            for row in read_csv("calendar.txt"):
                sid = row.get("service_id", "").strip()
                if not sid:
                    continue
                result["calendar"][sid] = {
                    "monday":     row.get("monday",     "0"),
                    "tuesday":    row.get("tuesday",    "0"),
                    "wednesday":  row.get("wednesday",  "0"),
                    "thursday":   row.get("thursday",   "0"),
                    "friday":     row.get("friday",     "0"),
                    "saturday":   row.get("saturday",   "0"),
                    "sunday":     row.get("sunday",     "0"),
                    "start_date": row.get("start_date", ""),
                    "end_date":   row.get("end_date",   ""),
                }

            # ── calendar_dates.txt ─────────────────────────────
            for row in read_csv("calendar_dates.txt"):
                sid = row.get("service_id", "").strip()
                if not sid:
                    continue
                if sid not in result["calendar_dates"]:
                    result["calendar_dates"][sid] = {}
                result["calendar_dates"][sid][row.get("date", "")] = \
                    row.get("exception_type", "")

            # ── stop_times.txt ─────────────────────────────────
            # This is the largest file — parse carefully
            st_count = 0
            for row in read_csv("stop_times.txt"):
                stop_id  = row.get("stop_id",  "").strip()
                trip_id  = row.get("trip_id",  "").strip()
                dep_time = (row.get("departure_time") or
                            row.get("arrival_time")   or "").strip()
                if not stop_id or not trip_id or not dep_time:
                    continue
                dep_secs = _gtfs_time_to_secs(dep_time)
                if dep_secs < 0:
                    continue
                if stop_id not in result["stop_times"]:
                    result["stop_times"][stop_id] = []
                result["stop_times"][stop_id].append((dep_secs, trip_id))
                st_count += 1
            log.info("  Parsed %d stop_time entries across %d stops",
                     st_count, len(result["stop_times"]))

    except zipfile.BadZipFile as exc:
        log.error("Bad zip file: %s", exc)

    return result

# ────────────────────────────────────────────────────────────
# DEPARTURE CALCULATION
# ────────────────────────────────────────────────────────────
def _get_stop_departures(gtfs: dict, stop_id: str) -> dict:
    now_local = datetime.now()
    today     = now_local.date()
    dow       = today.weekday()
    today_str = today.strftime("%Y%m%d")
    now_secs  = (now_local.hour * 3600 +
                 now_local.minute * 60 +
                 now_local.second)

    stop_info  = gtfs.get("stops", {}).get(stop_id, {})
    stop_name  = stop_info.get("name", stop_id)
    raw_times  = gtfs.get("stop_times", {}).get(stop_id, [])

    if not raw_times:
        return {
            "stop_name":  stop_name,
            "departures": [],
            "note": (
                f"No timetable data found for stop {stop_id}. "
                "This stop may not be served by the operators in our timetable, "
                "or the GTFS data may still be loading."
            ),
        }

    lookahead  = 7200  # 2 hours
    departures = []

    for (dep_secs, trip_id) in raw_times:
        if dep_secs < now_secs or dep_secs > now_secs + lookahead:
            continue

        trip  = gtfs["trips"].get(trip_id, {})
        route = gtfs["routes"].get(trip.get("route_id", ""), {})
        sid   = trip.get("service_id", "")

        if not _service_runs_today(sid, today, today_str, dow,
                                   gtfs["calendar"],
                                   gtfs["calendar_dates"]):
            continue

        dep_h   = dep_secs // 3600
        dep_m   = (dep_secs % 3600) // 60
        dep_dt  = now_local.replace(
            hour=dep_h % 24, minute=dep_m, second=0, microsecond=0
        )

        departures.append({
            "service":            route.get("short_name", "?"),
            "destination":        trip.get("headsign",    "Unknown"),
            "aimed_departure":    dep_dt.isoformat(),
            "expected_departure": None,
            "status":             "Scheduled",
            "delay_seconds":      None,
            "_trip_id":           trip_id,  # kept for Phase 2 GTFS-RT merge
        })

    departures.sort(key=lambda d: d["aimed_departure"])

    return {"stop_name": stop_name, "departures": departures[:15]}


def _service_runs_today(service_id: str, today: date, today_str: str,
                         dow: int, calendar: dict,
                         calendar_dates: dict) -> bool:
    exceptions = calendar_dates.get(service_id, {})
    if today_str in exceptions:
        return exceptions[today_str] == "1"
    cal = calendar.get(service_id)
    if not cal:
        return False
    if today_str < cal["start_date"] or today_str > cal["end_date"]:
        return False
    days = ["monday","tuesday","wednesday","thursday",
            "friday","saturday","sunday"]
    return cal.get(days[dow], "0") == "1"

# ────────────────────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────────────────────
def _check_api_key():
    if not BODS_API_KEY:
        raise HTTPException(status_code=503,
            detail="BODS_API_KEY not set. Add it as a Vercel environment variable.")

def _safe_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _gtfs_time_to_secs(t: str) -> int:
    parts = t.strip().split(":")
    if len(parts) != 3:
        return -1
    try:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        return -1

def _parse_iso_duration(duration: str) -> Optional[int]:
    if not duration:
        return None
    duration = duration.strip()
    negative = duration.startswith("-")
    duration = duration.lstrip("-P")
    parts    = duration.split("T")
    t        = parts[1] if len(parts) > 1 else parts[0]
    secs     = 0
    for token, mult in [("H", 3600), ("M", 60), ("S", 1)]:
        idx = t.find(token)
        if idx >= 0:
            try:
                secs += int(float(t[:idx])) * mult
            except ValueError:
                pass
            t = t[idx + 1:]
    return -secs if negative else secs
