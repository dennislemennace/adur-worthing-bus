"""
api/main.py — Adur & Worthing Bus Tracker Backend
===================================================

Endpoints:
  GET /                              Health check
  GET /api/stops                     Bus stops (from Overpass/OpenStreetMap)
  GET /api/vehicles                  Live bus positions (BODS SIRI-VM)
  GET /api/departures?stopId=XXXX    Scheduled departures (BODS GTFS timetable)

Phase 2 (GTFS-RT real-time) is stubbed and ready to layer on top of Phase 1.

Environment variables required:
  BODS_API_KEY    — from https://data.bus-data.dft.gov.uk/
  ALLOWED_ORIGIN  — your GitHub Pages URL, e.g. https://username.github.io
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

# National Operator Codes (NOCs) for operators serving Adur & Worthing.
# These are used to find the right GTFS timetable files on BODS.
# To discover more: check the operator_ref field in /api/vehicles JSON,
# or search https://data.bus-data.dft.gov.uk/operators/
AREA_OPERATOR_NOCS = [
    "SCSC",   # Stagecoach South Coast
    "BHBC",   # Brighton & Hove Bus and Coach Company
    "CMPA",   # Compass Travel (Sussex)
    "METR",   # Metrobus
]

# How long to cache GTFS data (timetables change weekly at most)
GTFS_CACHE_TTL = 86_400   # 24 hours

# ────────────────────────────────────────────────────────────
# SIMPLE IN-MEMORY CACHE (key → (data, expiry_timestamp))
# ────────────────────────────────────────────────────────────
_cache: dict = {}

def cache_get(key: str) -> Optional[dict]:
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
# Stored separately from the general cache because it's large
# and needs special handling.
# ────────────────────────────────────────────────────────────
_gtfs_data:      Optional[dict] = None
_gtfs_cached_at: float          = 0.0


# ────────────────────────────────────────────────────────────
# FASTAPI APP
# ────────────────────────────────────────────────────────────
app = FastAPI(
    title="Adur & Worthing Bus API",
    version="1.1.0",
)

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
    return {
        "status": "ok",
        "bods_key_configured": bool(BODS_API_KEY),
        "gtfs_loaded": _gtfs_data is not None,
        "gtfs_stops_indexed": len(_gtfs_data["stop_times"]) if _gtfs_data else 0,
    }


# ────────────────────────────────────────────────────────────
# ENDPOINT: /api/stops
# Source: Overpass API (OpenStreetMap) — free, no key needed
# ────────────────────────────────────────────────────────────
@app.get("/api/stops")
async def get_stops():
    """All bus stops in the Adur & Worthing bounding box. Cached 24 h."""
    cached = cache_get("stops")
    if cached:
        return cached

    stops = await _fetch_overpass_stops()
    result = {"stops": stops, "count": len(stops)}
    cache_set("stops", result, GTFS_CACHE_TTL)
    log.info("Fetched %d stops from Overpass", len(stops))
    return result


async def _fetch_overpass_stops() -> list[dict]:
    query = f"""
    [out:json][timeout:30];
    node["highway"="bus_stop"]
      ({BBOX_MIN_LAT},{BBOX_MIN_LON},{BBOX_MAX_LAT},{BBOX_MAX_LON});
    out body;
    """
    async with httpx.AsyncClient(timeout=40) as client:
        try:
            resp = await client.post("https://overpass-api.de/api/interpreter",
                                     data={"data": query})
            resp.raise_for_status()
            raw = resp.json()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=502,
                                detail=f"Overpass API error: {exc.response.status_code}")
        except httpx.RequestError:
            raise HTTPException(status_code=502, detail="Could not reach Overpass API.")

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
                or tags.get("description") or "Bus Stop")
        stops.append({"atco_code": atco, "name": name,
                      "latitude": lat, "longitude": lon})
    return stops


# ────────────────────────────────────────────────────────────
# ENDPOINT: /api/vehicles
# Source: BODS SIRI-VM — live bus positions
# ────────────────────────────────────────────────────────────
@app.get("/api/vehicles")
async def get_vehicles():
    """Live bus positions. Cached 15 s."""
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
    url = f"{BODS_BASE}/datafeed/"
    params = {"api_key": BODS_API_KEY, "boundingBox": BBOX_STR}

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=502,
                                detail=f"BODS SIRI-VM error: {exc.response.status_code}")
        except httpx.RequestError:
            raise HTTPException(status_code=502, detail="Could not reach BODS API.")

    return _parse_siri_vm(resp.text)


def _parse_siri_vm(xml_text: str) -> list[dict]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    ns = {"s": SIRI_NS}
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
        lat_el = loc.find("s:Latitude",  ns)
        lon_el = loc.find("s:Longitude", ns)
        try:
            lat = float(lat_el.text)
            lon = float(lon_el.text)
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
# Source: BODS GTFS static timetable — scheduled times
#
# PHASE 2 UPGRADE PATH:
#   Once this is working, add a call to _fetch_gtfs_rt() here.
#   That returns a dict of {trip_id: delay_seconds}.
#   Merge those delays into the departures list before returning.
#   See the stub at the bottom of this file.
# ────────────────────────────────────────────────────────────
@app.get("/api/departures")
async def get_departures(stopId: str = Query(...)):
    """Scheduled departures for a stop from BODS GTFS timetable. Cached 60 s."""
    _check_api_key()

    if not stopId or len(stopId) > 20:
        raise HTTPException(status_code=400, detail="Invalid stopId.")

    cached = cache_get(f"dep:{stopId}")
    if cached:
        return cached

    # Load (or reuse cached) GTFS data
    gtfs = await _get_gtfs_data()

    # Get departures from timetable
    result = _get_stop_departures(gtfs, stopId)

    cache_set(f"dep:{stopId}", result, 60)
    return result


# ────────────────────────────────────────────────────────────
# GTFS DATA LOADER
# Downloads operator GTFS zips from BODS and parses them
# into an in-memory index, cached for 24 hours.
# ────────────────────────────────────────────────────────────
async def _get_gtfs_data() -> dict:
    """Return cached GTFS data, downloading fresh if cache is stale."""
    global _gtfs_data, _gtfs_cached_at

    if _gtfs_data and (time.time() - _gtfs_cached_at) < GTFS_CACHE_TTL:
        return _gtfs_data

    log.info("GTFS cache stale — downloading fresh timetable data")
    _gtfs_data = await _fetch_and_merge_gtfs()
    _gtfs_cached_at = time.time()
    log.info("GTFS loaded: %d stops indexed, %d routes",
             len(_gtfs_data["stop_times"]), len(_gtfs_data["routes"]))
    return _gtfs_data


async def _fetch_and_merge_gtfs() -> dict:
    """
    Query the BODS dataset API for each operator NOC, download their
    GTFS zip, parse it, and merge everything into one combined dataset.
    """
    merged: dict = {
        "agencies":       {},
        "routes":         {},
        "trips":          {},
        "stop_times":     {},   # stop_id → [(dep_secs, trip_id)]
        "calendar":       {},
        "calendar_dates": {},
    }

    download_urls = await _discover_gtfs_urls()

    if not download_urls:
        log.warning("No GTFS datasets found — departures will be empty")
        return merged

    for url in download_urls:
        try:
            parsed = await _download_and_parse_gtfs(url)
            # Merge — later datasets overwrite earlier ones for same IDs,
            # which is fine because IDs are unique per operator.
            for key in merged:
                if key == "stop_times":
                    # Append rather than overwrite for stop_times
                    for stop_id, times in parsed["stop_times"].items():
                        if stop_id not in merged["stop_times"]:
                            merged["stop_times"][stop_id] = []
                        merged["stop_times"][stop_id].extend(times)
                elif key == "calendar_dates":
                    for sid, dates in parsed["calendar_dates"].items():
                        if sid not in merged["calendar_dates"]:
                            merged["calendar_dates"][sid] = {}
                        merged["calendar_dates"][sid].update(dates)
                else:
                    merged[key].update(parsed.get(key, {}))
        except Exception as exc:
            log.warning("Skipping dataset %s — %s", url, exc)

    return merged


async def _discover_gtfs_urls() -> list[str]:
    """
    Query the BODS timetable dataset API for each operator NOC
    and collect the GTFS download URLs.
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
                        "limit":   10,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                log.warning("Could not query BODS datasets for NOC %s: %s", noc, exc)
                continue

            for dataset in data.get("results", []):
                # The BODS API returns the GTFS download URL in the `url` field
                dl_url = dataset.get("url") or dataset.get("download_url")
                if dl_url and dl_url not in seen:
                    seen.add(dl_url)
                    urls.append(dl_url)
                    log.info("Found GTFS dataset for %s: %s", noc, dl_url)

    log.info("Discovered %d unique GTFS dataset(s) to download", len(urls))
    return urls


async def _download_and_parse_gtfs(download_url: str) -> dict:
    """Download a GTFS zip from BODS and parse it."""
    log.info("Downloading GTFS from %s", download_url)

    async with httpx.AsyncClient(
        timeout=120,
        follow_redirects=True,
        headers={"Authorization": f"Token {BODS_API_KEY}"},
    ) as client:
        resp = await client.get(download_url)
        resp.raise_for_status()
        zip_bytes = resp.content

    log.info("Downloaded %.1f MB — parsing…", len(zip_bytes) / 1_048_576)
    return _parse_gtfs_zip(zip_bytes)


def _parse_gtfs_zip(zip_bytes: bytes) -> dict:
    """
    Parse a GTFS zip file into our internal data structure.

    Only the files needed for departure boards are parsed:
    agency.txt, routes.txt, trips.txt, calendar.txt,
    calendar_dates.txt, stop_times.txt
    """
    result: dict = {
        "agencies":       {},
        "routes":         {},
        "trips":          {},
        "stop_times":     {},
        "calendar":       {},
        "calendar_dates": {},
    }

    def read_csv(zf, filename):
        """Read a CSV file from the zip, handling UTF-8 BOM."""
        with zf.open(filename) as f:
            return csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))

    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()

            # agency.txt
            if "agency.txt" in names:
                for row in read_csv(zf, "agency.txt"):
                    aid = row.get("agency_id", "default")
                    result["agencies"][aid] = row.get("agency_name", "")

            # routes.txt
            if "routes.txt" in names:
                for row in read_csv(zf, "routes.txt"):
                    result["routes"][row["route_id"]] = {
                        "short_name": row.get("route_short_name", ""),
                        "long_name":  row.get("route_long_name",  ""),
                        "agency_id":  row.get("agency_id", ""),
                    }

            # trips.txt
            if "trips.txt" in names:
                for row in read_csv(zf, "trips.txt"):
                    result["trips"][row["trip_id"]] = {
                        "route_id":   row.get("route_id",      ""),
                        "service_id": row.get("service_id",    ""),
                        "headsign":   row.get("trip_headsign", ""),
                    }

            # calendar.txt
            if "calendar.txt" in names:
                for row in read_csv(zf, "calendar.txt"):
                    result["calendar"][row["service_id"]] = {
                        "monday":    row.get("monday",    "0"),
                        "tuesday":   row.get("tuesday",   "0"),
                        "wednesday": row.get("wednesday", "0"),
                        "thursday":  row.get("thursday",  "0"),
                        "friday":    row.get("friday",    "0"),
                        "saturday":  row.get("saturday",  "0"),
                        "sunday":    row.get("sunday",    "0"),
                        "start_date":row.get("start_date",""),
                        "end_date":  row.get("end_date",  ""),
                    }

            # calendar_dates.txt — service exceptions (bank holidays etc.)
            if "calendar_dates.txt" in names:
                for row in read_csv(zf, "calendar_dates.txt"):
                    sid = row["service_id"]
                    if sid not in result["calendar_dates"]:
                        result["calendar_dates"][sid] = {}
                    result["calendar_dates"][sid][row["date"]] = row["exception_type"]

            # stop_times.txt — potentially the largest file
            # We build an index: stop_id → [(departure_seconds, trip_id)]
            if "stop_times.txt" in names:
                for row in read_csv(zf, "stop_times.txt"):
                    stop_id  = row.get("stop_id",  "")
                    dep_time = (row.get("departure_time") or
                                row.get("arrival_time")  or "")
                    trip_id  = row.get("trip_id", "")

                    if not stop_id or not dep_time or not trip_id:
                        continue

                    dep_secs = _gtfs_time_to_secs(dep_time)
                    if dep_secs < 0:
                        continue

                    if stop_id not in result["stop_times"]:
                        result["stop_times"][stop_id] = []
                    result["stop_times"][stop_id].append((dep_secs, trip_id))

    except zipfile.BadZipFile as exc:
        log.error("Bad zip file: %s", exc)

    return result


# ────────────────────────────────────────────────────────────
# DEPARTURE CALCULATION FROM GTFS
# ────────────────────────────────────────────────────────────
def _get_stop_departures(gtfs: dict, stop_id: str) -> dict:
    """
    Given the parsed GTFS data and a stop ATCO code, return the next
    departures from that stop for today, sorted by time.
    """
    now_utc  = datetime.now(timezone.utc)
    # BODS timetables use UK local time — adjust if needed.
    # For simplicity we use wall-clock time (correct for BST/GMT in practice).
    now_local = datetime.now()
    today     = now_local.date()
    dow       = today.weekday()          # 0 = Monday … 6 = Sunday
    today_str = today.strftime("%Y%m%d")

    # Seconds since midnight right now
    now_secs = (now_local.hour * 3600 +
                now_local.minute * 60 +
                now_local.second)

    # Look up stop_times for this stop
    raw_times = gtfs["stop_times"].get(stop_id, [])

    if not raw_times:
        # The Overpass stop might use an OSM node ID rather than an ATCO code.
        # Try a prefix match in case the GTFS uses a slightly different code.
        # (BODS GTFS ATCO codes are usually exact matches.)
        log.info("No stop_times for stop_id '%s' — stop may use OSM node ID "
                 "instead of ATCO code", stop_id)
        return {"stop_name": "Unknown", "departures": [],
                "note": "No timetable data found for this stop. "
                        "The stop marker may be using an OSM ID rather than "
                        "an ATCO code. Try clicking a different nearby stop."}

    departures = []
    lookahead  = 7200  # Show up to 2 hours ahead

    for (dep_secs, trip_id) in raw_times:
        # Filter to upcoming departures in the next 2 hours
        if dep_secs < now_secs or dep_secs > now_secs + lookahead:
            continue

        # Look up trip → route
        trip  = gtfs["trips"].get(trip_id, {})
        route = gtfs["routes"].get(trip.get("route_id", ""), {})
        sid   = trip.get("service_id", "")

        # Check if this service runs today
        if not _service_runs_today(sid, today, today_str, dow,
                                   gtfs["calendar"],
                                   gtfs["calendar_dates"]):
            continue

        # Build ISO datetime for the departure
        dep_hour   = dep_secs // 3600
        dep_minute = (dep_secs % 3600) // 60
        dep_dt     = now_local.replace(
            hour=dep_hour % 24, minute=dep_minute, second=0, microsecond=0
        )
        dep_iso = dep_dt.isoformat()

        departures.append({
            "service":            route.get("short_name", "?"),
            "destination":        trip.get("headsign",    "Unknown"),
            "aimed_departure":    dep_iso,
            "expected_departure": None,   # Filled in by Phase 2 GTFS-RT
            "status":             "Scheduled",
            "delay_seconds":      None,   # Filled in by Phase 2 GTFS-RT
        })

    # Sort by departure time
    departures.sort(key=lambda d: d["aimed_departure"])

    return {"stop_name": stop_id, "departures": departures[:15]}


def _service_runs_today(service_id: str, today: date, today_str: str,
                         dow: int, calendar: dict,
                         calendar_dates: dict) -> bool:
    """
    Check GTFS calendar and calendar_dates to see if a service
    runs on today's date.
    """
    # calendar_dates exceptions take priority over calendar
    exceptions = calendar_dates.get(service_id, {})
    if today_str in exceptions:
        # exception_type 1 = service added, 2 = service removed
        return exceptions[today_str] == "1"

    cal = calendar.get(service_id)
    if not cal:
        return False

    # Check date range
    if today_str < cal["start_date"] or today_str > cal["end_date"]:
        return False

    # Check day of week
    days = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
    return cal.get(days[dow], "0") == "1"


# ────────────────────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────────────────────
def _check_api_key():
    if not BODS_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="BODS_API_KEY not configured. "
                   "Add it as an environment variable on Vercel/Render.",
        )

def _safe_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _gtfs_time_to_secs(t: str) -> int:
    """
    Convert GTFS time string to seconds since midnight.
    GTFS allows times > 24:00 for services running past midnight.
    Returns -1 if the string cannot be parsed.
    """
    parts = t.strip().split(":")
    if len(parts) != 3:
        return -1
    try:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        return -1

def _parse_iso_duration(duration: str) -> Optional[int]:
    """Parse ISO 8601 duration (e.g. PT2M30S) to total seconds."""
    if not duration:
        return None
    duration  = duration.strip()
    negative  = duration.startswith("-")
    duration  = duration.lstrip("-P")
    parts     = duration.split("T")
    time_part = parts[1] if len(parts) > 1 else parts[0]
    seconds   = 0
    for token, mult in [("H", 3600), ("M", 60), ("S", 1)]:
        idx = time_part.find(token)
        if idx >= 0:
            try:
                seconds += int(float(time_part[:idx])) * mult
            except ValueError:
                pass
            time_part = time_part[idx + 1:]
    return -seconds if negative else seconds

def _derive_status(delay_seconds: Optional[int]) -> str:
    if delay_seconds is None:
        return "Scheduled"
    mins = delay_seconds / 60
    if mins <= -1.5: return "Early"
    if mins <=  1.5: return "On time"
    return "Late"


# ════════════════════════════════════════════════════════════
# PHASE 2 STUB — GTFS-RT REAL-TIME UPDATES
# ════════════════════════════════════════════════════════════
# When you're ready to add real-time delays on top of the
# static timetable, follow these steps:
#
# 1. Find the BODS GTFS-RT feed URL in your BODS dashboard
#    (it will look like https://data.bus-data.dft.gov.uk/api/v1/gtfsrtdatafeeds/NNN/)
#
# 2. Add this function:
#
#   async def _fetch_gtfs_rt() -> dict:
#       """
#       Fetch GTFS-RT TripUpdates feed and return a dict of
#       {trip_id: delay_seconds} for active trips.
#       """
#       from google.transit import gtfs_realtime_pb2
#       url = "https://data.bus-data.dft.gov.uk/api/v1/gtfsrtdatafeeds/YOUR_FEED_ID/"
#       async with httpx.AsyncClient(timeout=20) as client:
#           resp = await client.get(url, params={"api_key": BODS_API_KEY})
#           resp.raise_for_status()
#       feed = gtfs_realtime_pb2.FeedMessage()
#       feed.ParseFromString(resp.content)
#       delays = {}
#       for entity in feed.entity:
#           if entity.HasField("trip_update"):
#               tu = entity.trip_update
#               trip_id = tu.trip.trip_id
#               for stu in tu.stop_time_update:
#                   if stu.HasField("departure"):
#                       delays[trip_id] = stu.departure.delay
#                       break
#       return delays
#
# 3. In get_departures(), after calling _get_stop_departures():
#
#       rt_delays = await _fetch_gtfs_rt()
#       for dep in result["departures"]:
#           trip_id = dep.get("_trip_id")   # add _trip_id to _get_stop_departures output
#           if trip_id in rt_delays:
#               delay = rt_delays[trip_id]
#               dep["delay_seconds"]      = delay
#               dep["expected_departure"] = _add_delay_to_iso(dep["aimed_departure"], delay)
#               dep["status"]             = _derive_status(delay)
#
# ════════════════════════════════════════════════════════════
