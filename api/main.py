"""
api/main.py — Adur & Worthing Bus Tracker Backend
===================================================

A lightweight FastAPI application that:
  1. Proxies requests to the UK Bus Open Data Service (BODS) API,
     keeping the API key securely server-side.
  2. Caches responses to stay within free-tier rate limits.
  3. Returns clean, normalised JSON to the frontend.

Endpoints
---------
  GET /api/stops         — All bus stops in the Adur & Worthing bounding box
  GET /api/vehicles      — Live bus positions (SIRI-VM feed)
  GET /api/departures    — Live departures for a stop (SIRI-SM feed)
                           Query param: stopId (ATCO code, e.g. 1400A0001)

Environment Variables
----------------------
  BODS_API_KEY   — Required. Get one free at https://data.bus-data.dft.gov.uk/
  ALLOWED_ORIGIN — Optional. Your GitHub Pages URL, e.g. https://username.github.io
                   Defaults to "*" (allow all) if not set.

Running locally
---------------
  pip install -r requirements.txt
  BODS_API_KEY=your_key_here uvicorn api.main:app --reload --port 8000

Deployment
----------
  See vercel.json and README.md.
"""

import os
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
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
BODS_API_KEY  = os.environ.get("BODS_API_KEY", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")

BODS_BASE = "https://data.bus-data.dft.gov.uk/api/v1"
NAPTAN_BASE = "https://naptan.api.dft.gov.uk/v1"

# Bounding box for Adur & Worthing, West Sussex
# Format used by BODS: minLon,minLat,maxLon,maxLat
BBOX_MIN_LON = -0.42
BBOX_MIN_LAT =  50.78
BBOX_MAX_LON = -0.10
BBOX_MAX_LAT =  50.87

BBOX_STR = f"{BBOX_MIN_LON},{BBOX_MIN_LAT},{BBOX_MAX_LON},{BBOX_MAX_LAT}"

# SIRI XML namespace (used across SIRI-VM and SIRI-SM responses)
SIRI_NS = "http://www.siri.org.uk/siri"

# ────────────────────────────────────────────────────────────
# SIMPLE IN-MEMORY CACHE
# ────────────────────────────────────────────────────────────
# This is a plain dict for simplicity on a single-process server.
# On Vercel (multiple invocations / serverless), each cold start
# gets a fresh cache — that's fine; we keep TTLs short for vehicles.
# For a persistent cache, switch to Redis or Upstash (both have free tiers).
_cache: dict = {}


def cache_get(key: str) -> Optional[dict]:
    """Return cached value if present and not expired, else None."""
    entry = _cache.get(key)
    if entry is None:
        return None
    data, expires_at = entry
    if time.time() > expires_at:
        del _cache[key]
        return None
    return data


def cache_set(key: str, data: dict, ttl_seconds: int) -> None:
    """Store data in cache with an expiry time."""
    _cache[key] = (data, time.time() + ttl_seconds)


# ────────────────────────────────────────────────────────────
# FASTAPI APP
# ────────────────────────────────────────────────────────────
app = FastAPI(
    title="Adur & Worthing Bus API",
    description="Proxy for BODS (Bus Open Data Service) — UK DfT",
    version="1.0.0",
)

# CORS — allow the frontend (GitHub Pages) to call this backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN] if ALLOWED_ORIGIN != "*" else ["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ────────────────────────────────────────────────────────────
# HEALTH CHECK
# ────────────────────────────────────────────────────────────
@app.get("/", tags=["health"])
async def root():
    """Health check — confirms the API is running."""
    key_ok = bool(BODS_API_KEY)
    return {
        "status": "ok",
        "bods_key_configured": key_ok,
        "message": "Adur & Worthing Bus API is running." if key_ok
                   else "WARNING: BODS_API_KEY environment variable is not set.",
    }


# ────────────────────────────────────────────────────────────
# ENDPOINT: /api/stops
# ────────────────────────────────────────────────────────────
@app.get("/api/stops", tags=["stops"])
async def get_stops():
    """
    Return all bus stops in the Adur & Worthing area.

    Data source: DfT NaPTAN API (no API key required).
    Cached for 24 hours — stop data changes very rarely.

    Response format:
        { "stops": [ { "atco_code", "name", "latitude", "longitude" }, … ] }
    """
    cache_key = "stops"
    cached = cache_get(cache_key)
    if cached:
        log.info("Returning cached stops (%d stops)", len(cached["stops"]))
        return cached

    stops = await _fetch_naptan_stops()
    result = {"stops": stops, "count": len(stops)}
    cache_set(cache_key, result, ttl_seconds=86_400)   # 24 hours
    log.info("Fetched %d stops from NaPTAN", len(stops))
    return result


async def _fetch_naptan_stops() -> list[dict]:
    """
    Fetch bus stops in the Adur & Worthing bounding box using the
    Overpass API (OpenStreetMap data). Free, no API key required.

    Falls back to an empty list on error so the rest of the app still works.
    """
    # Overpass QL query — finds all bus_stop nodes in the bounding box
    # Bounding box order for Overpass: south,west,north,east
    query = f"""
    [out:json][timeout:30];
    node["highway"="bus_stop"]
      ({BBOX_MIN_LAT},{BBOX_MIN_LON},{BBOX_MAX_LAT},{BBOX_MAX_LON});
    out body;
    """

    url = "https://overpass-api.de/api/interpreter"

    async with httpx.AsyncClient(timeout=40) as client:
        try:
            resp = await client.post(url, data={"data": query})
            resp.raise_for_status()
            raw = resp.json()
        except httpx.HTTPStatusError as exc:
            log.error("Overpass API error %s: %s", exc.response.status_code, exc.response.text[:200])
            raise HTTPException(status_code=502, detail="Overpass (stops) API returned an error.")
        except httpx.RequestError as exc:
            log.error("Overpass request error: %s", exc)
            raise HTTPException(status_code=502, detail="Could not reach Overpass API.")

    stops = []
    for element in raw.get("elements", []):
        if element.get("type") != "node":
            continue

        lat = element.get("lat")
        lon = element.get("lon")
        if lat is None or lon is None:
            continue

        tags = element.get("tags", {})

        # Use the NaPTAN ATCO code if OSM has it, otherwise use the OSM node ID
        atco = tags.get("naptan:AtcoCode") or tags.get("ref") or str(element["id"])
        name = (tags.get("name")
                or tags.get("naptan:CommonName")
                or tags.get("description")
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
@app.get("/api/vehicles", tags=["vehicles"])
async def get_vehicles():
    """
    Return live bus positions from BODS SIRI-VM feed.

    Filtered to the Adur & Worthing bounding box.
    Cached for 15 seconds to avoid hammering the BODS API.

    Response format:
        { "vehicles": [ { "vehicle_ref", "service_ref", "operator_ref",
                          "destination", "latitude", "longitude",
                          "bearing", "delay_seconds", "recorded_at" }, … ] }
    """
    _check_api_key()

    cache_key = "vehicles"
    cached = cache_get(cache_key)
    if cached:
        return cached

    vehicles = await _fetch_siri_vm()
    result = {"vehicles": vehicles, "count": len(vehicles)}
    cache_set(cache_key, result, ttl_seconds=15)   # 15 seconds
    log.info("Fetched %d vehicles from BODS SIRI-VM", len(vehicles))
    return result


async def _fetch_siri_vm() -> list[dict]:
    """
    Call the BODS SIRI-VM (Vehicle Monitoring) endpoint and parse the XML.

    BODS docs: https://data.bus-data.dft.gov.uk/guidance/developer-documentation/
    """
    url = f"{BODS_BASE}/datafeed/"
    params = {
        "api_key":     BODS_API_KEY,
        "boundingBox": BBOX_STR,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            xml_text = resp.text
        except httpx.HTTPStatusError as exc:
            log.error("BODS SIRI-VM error %s", exc.response.status_code)
            raise HTTPException(status_code=502, detail=f"BODS API error: {exc.response.status_code}")
        except httpx.RequestError as exc:
            log.error("BODS request error: %s", exc)
            raise HTTPException(status_code=502, detail="Could not reach BODS API.")

    return _parse_siri_vm(xml_text)


def _parse_siri_vm(xml_text: str) -> list[dict]:
    """
    Parse a SIRI-VM XML response into a list of vehicle dicts.

    SIRI-VM XPath (simplified):
      Siri/ServiceDelivery/VehicleMonitoringDelivery/VehicleActivity/
        MonitoredVehicleJourney/...
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        log.error("XML parse error in SIRI-VM: %s", exc)
        return []

    ns = {"s": SIRI_NS}
    vehicles = []

    activities = root.findall(".//s:VehicleActivity", ns)
    log.debug("Found %d VehicleActivity elements", len(activities))

    for activity in activities:
        journey = activity.find("s:MonitoredVehicleJourney", ns)
        if journey is None:
            continue

        def text(tag: str) -> str:
            el = journey.find(f"s:{tag}", ns)
            return el.text.strip() if el is not None and el.text else ""

        lat_str = text("VehicleLocation/Latitude")  if journey.find("s:VehicleLocation", ns) is not None else ""
        lon_str = text("VehicleLocation/Longitude") if journey.find("s:VehicleLocation", ns) is not None else ""

        # Latitude and Longitude are inside VehicleLocation
        loc = journey.find("s:VehicleLocation", ns)
        if loc is not None:
            lat_el = loc.find("s:Latitude", ns)
            lon_el = loc.find("s:Longitude", ns)
            lat_str = lat_el.text.strip() if lat_el is not None and lat_el.text else ""
            lon_str = lon_el.text.strip() if lon_el is not None and lon_el.text else ""

        try:
            lat = float(lat_str)
            lon = float(lon_str)
        except (ValueError, TypeError):
            continue

        # Calculate delay from Delay element (ISO 8601 duration, e.g. PT2M30S = +150s, -PT1M = -60s)
        delay_el = journey.find("s:Delay", ns)
        delay_seconds = _parse_iso_duration(delay_el.text) if delay_el is not None and delay_el.text else None

        recorded_el = activity.find("s:RecordedAtTime", ns)
        recorded_at = recorded_el.text.strip() if recorded_el is not None and recorded_el.text else None

        vehicles.append({
            "vehicle_ref":  text("VehicleRef"),
            "service_ref":  text("PublishedLineName") or text("LineRef"),
            "operator_ref": text("OperatorRef"),
            "destination":  text("DestinationName") or text("DirectionRef"),
            "latitude":     lat,
            "longitude":    lon,
            "bearing":      _safe_float(text("Bearing")),
            "delay_seconds": delay_seconds,
            "recorded_at":  recorded_at,
        })

    return vehicles


# ────────────────────────────────────────────────────────────
# ENDPOINT: /api/departures
# ────────────────────────────────────────────────────────────
@app.get("/api/departures", tags=["departures"])
async def get_departures(stopId: str = Query(..., description="ATCO stop code, e.g. 1400A0001")):
    """
    Return live departures for a single bus stop from BODS SIRI-SM feed.

    Cached for 30 seconds per stop.

    Response format:
        { "stop_name": "...", "departures": [
            { "service", "destination", "aimed_departure",
              "expected_departure", "status", "delay_seconds" }, … ] }
    """
    _check_api_key()

    if not stopId or len(stopId) > 20:
        raise HTTPException(status_code=400, detail="Invalid stopId parameter.")

    cache_key = f"departures:{stopId}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    result = await _fetch_siri_sm(stopId)
    cache_set(cache_key, result, ttl_seconds=30)   # 30 seconds
    return result


async def _fetch_siri_sm(stop_id: str) -> dict:
    """
    Call the BODS SIRI-SM (Stop Monitoring) endpoint for a single stop.
    """
    url = f"{BODS_BASE}/siri-sm/"
    params = {
        "api_key":       BODS_API_KEY,
        "MonitoringRef": stop_id,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            xml_text = resp.text
        except httpx.HTTPStatusError as exc:
            log.error("BODS SIRI-SM error %s for stop %s", exc.response.status_code, stop_id)
            raise HTTPException(status_code=502, detail=f"BODS API error: {exc.response.status_code}")
        except httpx.RequestError as exc:
            log.error("BODS SIRI-SM request error: %s", exc)
            raise HTTPException(status_code=502, detail="Could not reach BODS API.")

    return _parse_siri_sm(xml_text)


def _parse_siri_sm(xml_text: str) -> dict:
    """
    Parse a SIRI-SM XML response into a departure board dict.

    SIRI-SM XPath (simplified):
      Siri/ServiceDelivery/StopMonitoringDelivery/MonitoredStopVisit/
        MonitoredVehicleJourney/...
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        log.error("XML parse error in SIRI-SM: %s", exc)
        return {"stop_name": "Unknown", "departures": []}

    ns = {"s": SIRI_NS}
    departures = []
    stop_name = "Unknown"

    visits = root.findall(".//s:MonitoredStopVisit", ns)
    log.debug("Found %d MonitoredStopVisit elements", len(visits))

    for visit in visits:
        journey = visit.find("s:MonitoredVehicleJourney", ns)
        if journey is None:
            continue

        def text(tag: str) -> str:
            el = journey.find(f"s:{tag}", ns)
            return el.text.strip() if el is not None and el.text else ""

        # Stop name (only need it once)
        if stop_name == "Unknown":
            call = journey.find("s:MonitoredCall", ns)
            if call is not None:
                sn_el = call.find("s:StopPointName", ns)
                if sn_el is not None and sn_el.text:
                    stop_name = sn_el.text.strip()

        call = journey.find("s:MonitoredCall", ns)

        def call_text(tag: str) -> str:
            if call is None:
                return ""
            el = call.find(f"s:{tag}", ns)
            return el.text.strip() if el is not None and el.text else ""

        aimed    = call_text("AimedDepartureTime") or call_text("AimedArrivalTime")
        expected = call_text("ExpectedDepartureTime") or call_text("ExpectedArrivalTime")

        # Determine status
        delay_el = journey.find("s:Delay", ns)
        delay_seconds = _parse_iso_duration(delay_el.text) if delay_el is not None and delay_el.text else None

        departure_status_el = call.find("s:DepartureStatus", ns) if call is not None else None
        raw_status = departure_status_el.text.strip() if departure_status_el is not None and departure_status_el.text else ""

        status = _derive_status(raw_status, delay_seconds)

        departures.append({
            "service":           text("PublishedLineName") or text("LineRef"),
            "destination":       text("DestinationName") or text("DirectionRef"),
            "operator_ref":      text("OperatorRef"),
            "aimed_departure":   aimed    if aimed    else None,
            "expected_departure":expected if expected else None,
            "status":            status,
            "delay_seconds":     delay_seconds,
        })

    # Sort departures by aimed/expected time ascending
    def sort_key(d):
        t = d["expected_departure"] or d["aimed_departure"] or ""
        try:
            return datetime.fromisoformat(t.replace("Z", "+00:00"))
        except Exception:
            return datetime.max.replace(tzinfo=timezone.utc)

    departures.sort(key=sort_key)

    return {"stop_name": stop_name, "departures": departures}


# ────────────────────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────────────────────

def _check_api_key():
    """Raise a 503 if no BODS API key is configured."""
    if not BODS_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="BODS_API_KEY environment variable is not set. "
                   "Get a free key at https://data.bus-data.dft.gov.uk/",
        )


def _safe_float(value: str) -> Optional[float]:
    """Convert a string to float, returning None if conversion fails."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_duration(duration: str) -> Optional[int]:
    """
    Parse an ISO 8601 duration string to total seconds.
    Handles the subset used by SIRI: e.g. PT2M30S, -PT1M, P0Y0M0DT0H2M30.000S

    Returns positive seconds for late buses, negative for early.
    Returns None if parsing fails.
    """
    if not duration:
        return None

    duration = duration.strip()
    negative = duration.startswith("-")
    duration = duration.lstrip("-")   # remove leading minus

    # Strip leading 'P' and split on 'T'
    duration = duration.lstrip("P")
    parts = duration.split("T")
    date_part = parts[0] if parts else ""
    time_part = parts[1] if len(parts) > 1 else ""

    seconds = 0

    # Parse date part (D only; years/months ignored for delay purposes)
    for token, multiplier in [("D", 86400)]:
        idx = date_part.find(token)
        if idx > 0:
            try:
                seconds += int(float(date_part[:idx])) * multiplier
            except ValueError:
                pass
            date_part = date_part[idx + 1:]

    # Parse time part
    for token, multiplier in [("H", 3600), ("M", 60), ("S", 1)]:
        idx = time_part.find(token)
        if idx >= 0:
            try:
                seconds += int(float(time_part[:idx])) * multiplier
            except ValueError:
                pass
            time_part = time_part[idx + 1:]

    return -seconds if negative else seconds


def _derive_status(raw_status: str, delay_seconds: Optional[int]) -> str:
    """Derive a human-readable status string from SIRI fields."""
    rs = (raw_status or "").lower()
    if rs in ("cancelled", "missed"):
        return "Cancelled"
    if delay_seconds is not None:
        mins = delay_seconds / 60
        if mins <= -1.5:
            return "Early"
        if mins <= 1.5:
            return "On time"
        return "Late"
    if rs == "ontime":
        return "On time"
    if rs:
        return raw_status.title()
    return "Scheduled"


#
# FUTURE EXTENSION POINTS
# ═══════════════════════
#
# SERVICE ALERTS
#   Add a new endpoint:
#     @app.get("/api/alerts")
#     async def get_alerts(stopId: Optional[str] = None):
#         # Fetch from BODS SIRI-SX (Situation Exchange) feed
#         # URL: f"{BODS_BASE}/siri-sx/?api_key={BODS_API_KEY}"
#         ...
#
# ROUTE / TIMETABLE DATA
#   Add an endpoint that queries BODS GTFS timetable data:
#     @app.get("/api/route")
#     async def get_route(serviceRef: str):
#         # Download and cache GTFS zip, parse with the `gtfs-kit` or `partridge` library
#         ...
#
# OPERATOR INFO
#   @app.get("/api/operators")
#   Fetch from BODS operator registry endpoint.
#
