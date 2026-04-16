"""
api/main.py — Adur & Worthing Bus Tracker Backend v2.0
=======================================================

Timetable data is loaded from data/timetable.json which is built
weekly by GitHub Actions (scripts/build_timetable.py).

Only live vehicle positions are fetched from BODS at runtime —
no large downloads, no timeouts.

Environment variables:
  BODS_API_KEY    — https://data.bus-data.dft.gov.uk/
  ALLOWED_ORIGIN  — your GitHub Pages URL
"""

import json
import os
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s  %(name)s  %(message)s")
log = logging.getLogger("bus_api")

# ── Config ───────────────────────────────────────────────────
BODS_API_KEY   = os.environ.get("BODS_API_KEY", "")
BODS_BASE = "https://data.bus-data.dft.gov.uk/api/v1"
SIRI_NS   = "http://www.siri.org.uk/siri"

BBOX_MIN_LAT, BBOX_MAX_LAT =  50.78,  50.87
BBOX_MIN_LON, BBOX_MAX_LON = -0.42,  -0.10
BBOX_STR = f"{BBOX_MIN_LON},{BBOX_MIN_LAT},{BBOX_MAX_LON},{BBOX_MAX_LAT}"

WEST_SUSSEX_ATCO_PREFIX = "4400"
TIMETABLE_CACHE_TTL     = 3600   # Re-read file from disk every hour
TIMETABLE_PATH = Path(__file__).parent.parent / "data" / "timetable.json"

# GTFS stop_times are in local UK time; Render runs in UTC. Using an
# explicit zone makes the departure filter correct across BST/GMT.
UK_TZ = ZoneInfo("Europe/London")

# Drop vehicles whose last RecordedAtTime is older than this. Clears out
# depot-parked buses that stop reporting (and BODS "zombie" vehicles).
VEHICLE_STALE_SECONDS = 300

# ── Traveline NextBuses (SIRI-SM real-time predictions) ───────
# Transport API (transportapi.com) — REST JSON live bus departures.
# Set NEXTBUSES_APP_ID and NEXTBUSES_APP_KEY in Render env vars.
# Endpoint: GET {NEXTBUSES_BASE_URL}/{atcocode}/live.json?app_id=...&app_key=...
NEXTBUSES_APP_ID             = os.environ.get("NEXTBUSES_APP_ID", "")
NEXTBUSES_APP_KEY            = os.environ.get("NEXTBUSES_APP_KEY", "")
NEXTBUSES_BASE_URL           = os.environ.get("NEXTBUSES_BASE_URL",
                               "https://transportapi.com/v3/uk/bus/stop")
NEXTBUSES_CACHE_TTL          = 90   # seconds to cache per-stop predictions
NEXTBUSES_SKIP_THRESHOLD_SEC = (
    int(os.getenv("NEXTBUSES_SKIP_THRESHOLD_MINUTES", "30")) * 60
)
NEXTBUSES_DAILY_LIMIT        = int(os.getenv("NEXTBUSES_DAILY_LIMIT", "300"))

# ── In-memory cache ───────────────────────────────────────────
_cache: dict = {}

# ── NextBuses daily quota counter ─────────────────────────────
# Single-process (Render free tier); no Redis needed.
_nb_quota: dict = {"date": None, "count": 0}

def cache_get(key: str):
    entry = _cache.get(key)
    if not entry:
        return None
    data, expires_at = entry
    if time.time() > expires_at:
        del _cache[key]
        return None
    return data

def cache_set(key: str, data, ttl: int) -> None:
    _cache[key] = (data, time.time() + ttl)

# ── Timetable store ───────────────────────────────────────────
_timetable:    Optional[dict] = None
_timetable_at: float          = 0.0

# ── App ───────────────────────────────────────────────────────
app = FastAPI(title="Adur & Worthing Bus API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)
# ── Health ────────────────────────────────────────────────────
@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    t = _timetable or {}
    return {
        "status":              "ok",
        "bods_key_configured": bool(BODS_API_KEY),
        "timetable_loaded":    _timetable is not None,
        "timetable_file":      str(TIMETABLE_PATH),
        "timetable_exists":    TIMETABLE_PATH.exists(),
        "stops_in_timetable":  len(t.get("stops",      {})),
        "stop_times_indexed":  len(t.get("stop_times", {})),
        "routes_loaded":       len(t.get("routes",     {})),
        "trips_loaded":        len(t.get("trips",      {})),
    }

# ── Debug endpoint (remove once departures are confirmed working)
@app.get("/api/debug/stop")
async def debug_stop(stopId: str = Query(...)):
    tt        = await _get_timetable()
    variants  = _normalise_atco(stopId)
    now_local = datetime.now(UK_TZ)
    today     = now_local.date()
    dow       = today.weekday()
    today_str = today.strftime("%Y%m%d")
    now_secs  = (now_local.hour * 3600
                 + now_local.minute * 60
                 + now_local.second)

    result = {
        "stop_id_received":          stopId,
        "variants_tried":            variants,
        "now_local":                 now_local.isoformat(),
        "now_secs":                  now_secs,
        "today_str":                 today_str,
        "day_of_week":               dow,
        "sample_timetable_stop_ids": list(tt.get("stop_times", {}).keys())[:10],
        "variants_detail":           [],
    }

    for variant in variants:
        raw_times = tt.get("stop_times", {}).get(variant, [])
        samples   = []
        for (dep_secs, trip_id) in raw_times[:50]:
            trip      = tt["trips"].get(trip_id, {})
            route     = tt["routes"].get(trip.get("route_id", ""), {})
            sid       = trip.get("service_id", "")
            cal       = tt["calendar"].get(sid, {})
            runs      = _runs_today(sid, today, today_str, dow,
                                    tt["calendar"], tt["calendar_dates"])
            in_window = now_secs <= dep_secs <= now_secs + 7200
            reason    = None
            if not in_window:
                reason = f"outside window (dep={dep_secs}, now={now_secs})"
            elif not runs:
                reason = f"not running today (cal={cal})"
            if len(samples) < 5:
                samples.append({
                    "dep_secs":    dep_secs,
                    "dep_time":    f"{dep_secs//3600:02d}:{(dep_secs%3600)//60:02d}",
                    "trip_id":     trip_id,
                    "service":     route.get("short_name", "?"),
                    "headsign":    trip.get("headsign", "?"),
                    "runs_today":  runs,
                    "in_window":   in_window,
                    "skip_reason": reason,
                    "calendar":    cal,
                })
        result["variants_detail"].append({
            "variant":         variant,
            "raw_times_count": len(raw_times),
            "samples":         samples,
        })

    return result

# ── /api/stops ────────────────────────────────────────────────
@app.get("/api/stops")
async def get_stops():
    """Bus stops in Adur & Worthing bounding box. Cached 24 h."""
    cached = cache_get("stops")
    if cached:
        return cached
    try:
        stops = await _fetch_overpass_stops()
    except Exception as exc:
        log.warning("Overpass fetch failed: %s", exc)
        return {"stops": [], "count": 0}
    result = {"stops": stops, "count": len(stops)}
    if stops:
        cache_set("stops", result, 86_400)
    log.info("Serving %d stops from Overpass", len(stops))
    return result


async def _fetch_overpass_stops() -> list:
    query = f"""
    [out:json][timeout:30];
    node["highway"="bus_stop"]
      ({BBOX_MIN_LAT},{BBOX_MIN_LON},{BBOX_MAX_LAT},{BBOX_MAX_LON});
    out body;
    """
    async with httpx.AsyncClient(timeout=40) as client:
        resp = await client.post(
            "https://overpass-api.de/api/interpreter",
            data={"data": query},
        )
        resp.raise_for_status()
        raw = resp.json()

    stops = []
    for el in raw.get("elements", []):
        if el.get("type") != "node":
            continue
        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            continue
        tags = el.get("tags", {})
        atco = (tags.get("naptan:AtcoCode")
                or tags.get("ref")
                or str(el["id"]))
        name = tags.get("name") or tags.get("naptan:CommonName") or "Bus Stop"
        stops.append({
            "atco_code": atco,
            "name":      name,
            "latitude":  lat,
            "longitude": lon,
        })
    return stops

# ── Debug: raw SIRI-VM dump with no stale filter
@app.get("/api/debug/vehicles-raw")
async def debug_vehicles_raw(q: str = Query("")):
    """
    Fetches raw BODS SIRI-VM and returns every VehicleActivity — including
    stale ones — with all the fields we care about for diagnostics.
    Use ?q=N700 to filter down to matches containing that substring.
    """
    _check_api_key()
    params = {"api_key": BODS_API_KEY, "boundingBox": BBOX_STR}
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(f"{BODS_BASE}/datafeed/", params=params)
        resp.raise_for_status()

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as exc:
        return {"error": f"parse_error: {exc}"}

    ns      = {"s": SIRI_NS}
    now_utc = datetime.now(timezone.utc)
    q_lower = q.lower()
    out     = []

    for activity in root.findall(".//s:VehicleActivity", ns):
        journey = activity.find("s:MonitoredVehicleJourney", ns)
        if journey is None:
            continue

        def jtext(tag):
            el = journey.find(f"s:{tag}", ns)
            return el.text.strip() if el is not None and el.text else ""

        rec_el = activity.find("s:RecordedAtTime", ns)
        recorded_at = rec_el.text.strip() if rec_el is not None and rec_el.text else None
        recorded_dt = _parse_iso_datetime(recorded_at)
        age = int((now_utc - recorded_dt).total_seconds()) if recorded_dt else None

        info = {
            "vehicle_ref":         jtext("VehicleRef"),
            "line_ref":            jtext("LineRef"),
            "published_line_name": jtext("PublishedLineName"),
            "operator_ref":        jtext("OperatorRef"),
            "origin_ref":          jtext("OriginRef"),
            "destination_ref":     jtext("DestinationRef"),
            "destination_name":    jtext("DestinationName"),
            "direction_ref":       jtext("DirectionRef"),
            "recorded_at":         recorded_at,
            "age_seconds":         age,
            "is_stale":            age is not None and age > VEHICLE_STALE_SECONDS,
        }

        if q_lower:
            blob = " ".join(str(v) for v in info.values() if v is not None).lower()
            if q_lower not in blob:
                continue

        out.append(info)

    return {
        "count":           len(out),
        "stale_threshold": VEHICLE_STALE_SECONDS,
        "vehicles":        out,
    }

# ── /api/vehicles ─────────────────────────────────────────────
@app.get("/api/vehicles")
async def get_vehicles():
    """Live bus positions from BODS SIRI-VM. Cached 15 s."""
    _check_api_key()
    cached = cache_get("vehicles")
    if cached is None:
        vehicles = await _fetch_siri_vm()
        tt       = await _get_timetable()
        _enrich_vehicles_with_trip_match(vehicles, tt)
        cached   = {"vehicles": vehicles, "count": len(vehicles)}
        cache_set("vehicles", cached, 15)
    # 'calls' and 'trip_id' are internal; strip from the public payload
    # to keep responses small. 'trip_headsign' is what the client needs.
    hidden = {"calls", "trip_id", "origin_ref", "destination_ref"}
    public = [{k: val for k, val in v.items() if k not in hidden}
              for v in cached["vehicles"]]
    return {"vehicles": public, "count": len(public)}


@app.get("/api/debug/siri-sample")
async def debug_siri_sample():
    """Dump raw XML structure of first 2 VehicleActivity elements."""
    _check_api_key()
    params = {"api_key": BODS_API_KEY, "boundingBox": BBOX_STR}
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(f"{BODS_BASE}/datafeed/", params=params)
        resp.raise_for_status()
    ns = {"s": SIRI_NS}
    root = ET.fromstring(resp.text)
    samples = []
    for act in root.findall(".//s:VehicleActivity", ns):
        j = act.find("s:MonitoredVehicleJourney", ns)
        if j is None:
            continue
        vref_el = j.find("s:VehicleRef", ns)
        vref = vref_el.text.strip() if vref_el is not None and vref_el.text else ""
        has_mc = j.find("s:MonitoredCall", ns) is not None
        has_oc = j.find("s:OnwardCalls", ns) is not None
        op_el = j.find("s:OperatorRef", ns)
        op = op_el.text.strip() if op_el is not None and op_el.text else ""
        if has_mc or has_oc or len(samples) < 4:
            samples.append({
                "vehicle_ref": vref,
                "operator": op,
                "has_monitored_call": has_mc,
                "has_onward_calls": has_oc,
                "xml": ET.tostring(act, encoding="unicode")[:800],
            })
        if len(samples) >= 10:
            break
    all_acts = root.findall(".//s:VehicleActivity", ns)
    mc_count = sum(1 for a in all_acts
                   if a.find("s:MonitoredVehicleJourney", ns) is not None
                   and a.find("s:MonitoredVehicleJourney/s:MonitoredCall", ns) is not None)

    tt = await _get_timetable()
    trip_ids = set(tt.get("trips", {}).keys())
    journey_refs = []
    for a in all_acts:
        j = a.find("s:MonitoredVehicleJourney", ns)
        if j is None:
            continue
        fvjr = j.find("s:FramedVehicleJourneyRef", ns)
        dvjr = fvjr.find("s:DatedVehicleJourneyRef", ns) if fvjr is not None else None
        ref = dvjr.text.strip() if dvjr is not None and dvjr.text else ""
        vref_el = j.find("s:VehicleRef", ns)
        vref = vref_el.text.strip() if vref_el is not None and vref_el.text else ""
        op_el = j.find("s:OperatorRef", ns)
        op = op_el.text.strip() if op_el is not None and op_el.text else ""
        svc_el = j.find("s:PublishedLineName", ns)
        svc = svc_el.text.strip() if svc_el is not None and svc_el.text else ""
        matched = ref in trip_ids if ref else False
        journey_refs.append({
            "vehicle_ref": vref, "operator": op, "service": svc,
            "dated_vehicle_journey_ref": ref, "matches_gtfs_trip": matched,
        })

    matched_count = sum(1 for j in journey_refs if j["matches_gtfs_trip"])
    return {"total_activities": len(all_acts),
            "with_monitored_call": mc_count,
            "journey_ref_matched": matched_count,
            "journey_ref_total": len(journey_refs),
            "samples": journey_refs[:15]}

@app.get("/api/debug/match-stats")
async def debug_match_stats():
    """Diagnostics: how many vehicles have calls, trip matches, etc."""
    _check_api_key()
    cached = cache_get("vehicles")
    if cached is None:
        vehicles = await _fetch_siri_vm()
        tt = await _get_timetable()
        _enrich_vehicles_with_trip_match(vehicles, tt)
        cached = {"vehicles": vehicles, "count": len(vehicles)}
        cache_set("vehicles", cached, 15)
    vehicles = cached.get("vehicles", [])
    with_calls = sum(1 for v in vehicles if v.get("calls"))
    with_trip = sum(1 for v in vehicles if v.get("trip_id"))
    with_headsign = sum(1 for v in vehicles if v.get("trip_headsign"))
    unmatched = []
    matched_samples = []
    for v in vehicles:
        info = {
            "ref": v["vehicle_ref"],
            "svc": v.get("service_ref"),
            "op": v.get("operator_ref"),
            "origin_ref": v.get("origin_ref"),
            "dest_ref": v.get("destination_ref"),
            "trip_id": v.get("trip_id"),
            "headsign": v.get("trip_headsign"),
        }
        if v.get("trip_id"):
            if len(matched_samples) < 5:
                matched_samples.append(info)
        else:
            if len(unmatched) < 5:
                unmatched.append(info)
    return {
        "total": len(vehicles),
        "with_calls": with_calls,
        "with_trip_id": with_trip,
        "with_headsign": with_headsign,
        "matched_samples": matched_samples,
        "unmatched_samples": unmatched,
    }

@app.get("/api/debug/nb-quota")
async def debug_nb_quota():
    """Diagnostics: current NextBuses daily quota usage."""
    return {
        "date":            _nb_quota.get("date"),
        "count":           _nb_quota.get("count", 0),
        "limit":           NEXTBUSES_DAILY_LIMIT,
        "remaining":       max(0, NEXTBUSES_DAILY_LIMIT - _nb_quota.get("count", 0)),
        "app_id_configured":  bool(NEXTBUSES_APP_ID),
        "app_key_configured": bool(NEXTBUSES_APP_KEY),
    }


@app.get("/api/debug/live-raw")
async def debug_live_raw(stopId: str = Query(...)):
    """Diagnostics: raw Transport API response + parsed predictions for a stop."""
    if not NEXTBUSES_APP_ID or not NEXTBUSES_APP_KEY:
        return {"error": "Transport API credentials not configured"}
    url = f"{NEXTBUSES_BASE_URL}/{stopId}/live.json"
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            resp = await client.get(url, params={
                "app_id": NEXTBUSES_APP_ID, "app_key": NEXTBUSES_APP_KEY,
                "group": "no", "nextbuses": "yes",
            })
        resp.raise_for_status()
        raw = resp.json()
    except Exception as exc:
        return {"error": str(exc)}
    predictions = _parse_transportapi_json(raw)
    return {
        "raw_keys":   list(raw.keys()),
        "departures_keys": list(raw.get("departures", {}).keys()),
        "raw_all_sample": raw.get("departures", {}).get("all", [])[:3],
        "parsed_predictions": predictions[:10],
    }


# ── /api/vehicle ──────────────────────────────────────────────
@app.get("/api/vehicle")
async def get_vehicle(vehicleRef: str = Query(...)):
    """
    Detailed info for a single live vehicle: its matched GTFS trip (so
    we can show the friendly headsign) and an upcoming-stops list.

    Preferred source for upcoming stops is the matched trip's full
    scheduled remainder; SIRI-VM OnwardCalls are used as a fallback
    when no trip match is found.
    """
    _check_api_key()
    if not vehicleRef or len(vehicleRef) > 80:
        raise HTTPException(status_code=400, detail="Invalid vehicleRef.")

    vehicles = await _get_vehicles_or_empty()
    vehicle  = next((v for v in vehicles
                     if v.get("vehicle_ref") == vehicleRef), None)
    if vehicle is None:
        raise HTTPException(status_code=404,
                            detail="Vehicle not currently tracked.")

    tt = await _get_timetable()

    trip_id  = vehicle.get("trip_id")
    headsign = vehicle.get("trip_headsign")
    upcoming = _upcoming_stops_from_trip(vehicle, tt, trip_id)
    source   = "trip"
    if not upcoming:
        upcoming = _upcoming_stops_from_calls(vehicle, tt)
        source   = "siri_onward_calls" if upcoming else "none"

    return {
        "vehicle": {
            "vehicle_ref":   vehicle.get("vehicle_ref"),
            "service_ref":   vehicle.get("service_ref"),
            "operator_ref":  vehicle.get("operator_ref"),
            "destination":   vehicle.get("destination"),
            "trip_headsign": headsign,
            "latitude":      vehicle.get("latitude"),
            "longitude":     vehicle.get("longitude"),
            "recorded_at":   vehicle.get("recorded_at"),
        },
        "upcoming_stops": upcoming,
        "source":         source,
    }


async def _fetch_siri_vm() -> list:
    params = {"api_key": BODS_API_KEY, "boundingBox": BBOX_STR}
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(f"{BODS_BASE}/datafeed/", params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=502,
                detail=f"BODS error: {exc.response.status_code}")
        except httpx.RequestError:
            raise HTTPException(status_code=502,
                detail="Could not reach BODS API.")
    return _parse_siri_vm(resp.text)


def _parse_siri_vm(xml_text: str) -> list:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    ns       = {"s": SIRI_NS}
    vehicles = []
    now_utc  = datetime.now(timezone.utc)
    dropped_stale = 0

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
        recorded_at = rec_el.text.strip() if rec_el is not None and rec_el.text else None

        # Drop vehicles whose last report is older than VEHICLE_STALE_SECONDS.
        # If we can't parse the timestamp, keep the vehicle (safer default).
        recorded_dt = _parse_iso_datetime(recorded_at)
        if recorded_dt is not None:
            age = (now_utc - recorded_dt).total_seconds()
            if age > VEHICLE_STALE_SECONDS:
                dropped_stale += 1
                continue

        calls = _extract_calls(journey, ns)

        vehicles.append({
            "vehicle_ref":   jtext("VehicleRef"),
            "service_ref":   jtext("PublishedLineName") or jtext("LineRef"),
            "operator_ref":  jtext("OperatorRef"),
            "destination":   jtext("DestinationName") or jtext("DirectionRef"),
            "origin_ref":    jtext("OriginRef"),
            "destination_ref": jtext("DestinationRef"),
            "latitude":      lat,
            "longitude":     lon,
            "bearing":       _safe_float(jtext("Bearing")),
            "delay_seconds": delay_s,
            "recorded_at":   recorded_at,
            "calls":         calls,
        })

    if dropped_stale:
        log.info("Dropped %d stale vehicle(s) (>%ds old)",
                 dropped_stale, VEHICLE_STALE_SECONDS)
    with_calls = sum(1 for v in vehicles if v.get("calls"))
    log.info("SIRI-VM parsed: %d vehicles (%d with onward calls)",
             len(vehicles), with_calls)
    return vehicles


def _extract_calls(journey, ns) -> list:
    """
    Extract MonitoredCall + OnwardCall blocks from a MonitoredVehicleJourney.

    Each entry is the vehicle's prediction for a specific downstream stop.
    MonitoredCall is the *next* stop; OnwardCall entries are the stops after.
    Returns a list of {stop_id, aimed_arrival, expected_arrival,
    aimed_departure, expected_departure} dicts (fields may be None).
    """
    def parse_call(el):
        if el is None:
            return None
        def txt(tag):
            e = el.find(f"s:{tag}", ns)
            return e.text.strip() if e is not None and e.text else None
        stop_id = txt("StopPointRef")
        if not stop_id:
            return None
        return {
            "stop_id":            stop_id,
            "aimed_arrival":      txt("AimedArrivalTime"),
            "expected_arrival":   txt("ExpectedArrivalTime"),
            "aimed_departure":    txt("AimedDepartureTime"),
            "expected_departure": txt("ExpectedDepartureTime"),
        }

    calls = []
    mon = parse_call(journey.find("s:MonitoredCall", ns))
    if mon:
        calls.append(mon)
    onward_container = journey.find("s:OnwardCalls", ns)
    if onward_container is not None:
        for oc in onward_container.findall("s:OnwardCall", ns):
            c = parse_call(oc)
            if c:
                calls.append(c)
    return calls

# ── /api/departures ───────────────────────────────────────────
@app.get("/api/departures")
async def get_departures(stopId: str = Query(...)):
    """
    Scheduled departures for a stop from the pre-built timetable, with
    a real-time overlay from Traveline NextBuses SIRI-SM when configured.

    The schedule itself is cached for 60 s. NextBuses predictions are
    cached separately per stop for 90 s and subject to a daily quota cap.
    Adds `live` (bool) and `live_reason` (str) fields to the response so
    the frontend can show a notice when live data is unavailable.
    """
    _check_api_key()
    if not stopId or len(stopId) > 20:
        raise HTTPException(status_code=400, detail="Invalid stopId.")

    cache_key = f"dep:{stopId}"
    base = cache_get(cache_key)
    if base is None:
        tt   = await _get_timetable()
        base = _departures_for_stop(tt, stopId)
        cache_set(cache_key, base, 60)

    return await _apply_live_overlay(base, stopId)


async def _get_vehicles_or_empty() -> list:
    """
    Return live vehicles from the 15s cache, fetching if necessary.
    Never raises — if BODS is unreachable we just return [] so the
    departures endpoint can still serve scheduled times.
    """
    cached = cache_get("vehicles")
    if cached:
        return cached.get("vehicles", [])
    try:
        vehicles = await _fetch_siri_vm()
    except HTTPException:
        return []
    except Exception as exc:
        log.warning("Live vehicle fetch failed during overlay: %s", exc)
        return []
    cache_set("vehicles", {"vehicles": vehicles, "count": len(vehicles)}, 15)
    return vehicles


async def _fetch_nextbuses(stop_id: str) -> Optional[list]:
    """
    GET live departures from Transport API for a single stop.

    Returns:
      list  — parsed predictions, possibly empty (no live data for this stop)
      None  — network / HTTP / JSON error; caller should show "upstream" notice

    Does NOT handle caching or quota — that is the caller's responsibility.
    """
    url = f"{NEXTBUSES_BASE_URL}/{stop_id}/live.json"
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            resp = await client.get(
                url,
                params={
                    "app_id":    NEXTBUSES_APP_ID,
                    "app_key":   NEXTBUSES_APP_KEY,
                    "group":     "no",
                    "nextbuses": "yes",
                },
            )
        resp.raise_for_status()
        return _parse_transportapi_json(resp.json())
    except Exception as exc:
        log.warning("Transport API request failed for stop %s: %s", stop_id, exc)
        return None


def _parse_transportapi_json(data: dict) -> list:
    """
    Parse a Transport API /live.json response into prediction dicts.
    Each dict: {service, aimed (ISO str), expected (ISO str | None)}.
    Times in the response are "HH:MM" local UK time (possibly "24:xx"
    for past-midnight services); these are converted to ISO 8601.
    """
    predictions = []
    for dep in data.get("departures", {}).get("all", []):
        service = (dep.get("line_name") or dep.get("line") or "").strip()
        if not service:
            continue
        aimed_iso = _hhmm_to_iso(dep.get("date"), dep.get("aimed_departure_time"))
        if aimed_iso is None:
            continue
        exp_iso = _hhmm_to_iso(dep.get("date"), dep.get("expected_departure_time"))
        predictions.append({
            "service":  service,
            "aimed":    aimed_iso,
            "expected": exp_iso,
        })
    return predictions


def _hhmm_to_iso(date_str: Optional[str], time_str: Optional[str]) -> Optional[str]:
    """
    Convert a Transport API date "YYYY-MM-DD" and time "HH:MM" (which may
    use hours >= 24 for past-midnight services) into a timezone-aware ISO
    8601 string using the UK local timezone.
    """
    if not date_str or not time_str:
        return None
    try:
        h, m    = time_str.split(":")
        hours   = int(h)
        mins    = int(m)
        base    = date.fromisoformat(date_str) + timedelta(days=hours // 24)
        dt      = datetime(base.year, base.month, base.day,
                           hours % 24, mins, tzinfo=UK_TZ)
        return dt.isoformat()
    except (ValueError, IndexError, TypeError, AttributeError):
        return None


async def _apply_live_overlay(base: dict, stop_id: str) -> dict:
    """
    Overlay real-time departure predictions from Traveline NextBuses onto
    a scheduled departures payload.

    Returns the base payload (scheduled only) when:
    - No NextBuses API key is configured (feature not yet enabled)
    - Next departure is more than NEXTBUSES_SKIP_THRESHOLD_SEC away
    - The daily quota is exhausted
    - NextBuses returns an upstream error or no predictions

    A `live` bool and `live_reason` string are added when live data is
    expected but unavailable, so the frontend can show a subtle notice.
    No flag is added when the feature is simply not configured yet.
    """
    departures = base.get("departures") or []
    if not departures or not NEXTBUSES_APP_ID or not NEXTBUSES_APP_KEY:
        return base

    # Skip-if-far: don't call NextBuses when next departure is > threshold away.
    # No banner — this is normal expected behaviour, not a degradation.
    try:
        first_aimed = _parse_iso_datetime(departures[0].get("aimed_departure"))
        if first_aimed:
            gap = (first_aimed - datetime.now(timezone.utc)).total_seconds()
            if gap > NEXTBUSES_SKIP_THRESHOLD_SEC:
                return {**base, "live": False, "live_reason": "too_far"}
    except Exception:
        pass

    # Cache hits never count against quota — check before the quota gate.
    cached_preds = cache_get(f"nb:{stop_id}")
    if cached_preds is None:
        # About to make a real network call — gate on quota first.
        today = date.today().isoformat()
        if _nb_quota["date"] != today:
            _nb_quota["date"] = today
            _nb_quota["count"] = 0
        if _nb_quota["count"] >= NEXTBUSES_DAILY_LIMIT:
            log.info("NextBuses quota exhausted (%d/%d)", _nb_quota["count"], NEXTBUSES_DAILY_LIMIT)
            return {**base, "live": False, "live_reason": "quota"}

        _nb_quota["count"] += 1
        log.info("NextBuses hit %d/%d for stop %s", _nb_quota["count"], NEXTBUSES_DAILY_LIMIT, stop_id)

        result = await _fetch_nextbuses(stop_id)
        if result is None:
            _nb_quota["count"] -= 1  # don't burn quota on a broken/unreachable API
            return {**base, "live": False, "live_reason": "upstream"}

        cache_set(f"nb:{stop_id}", result, NEXTBUSES_CACHE_TTL)
        cached_preds = result

    predictions = cached_preds
    if not predictions:
        return {**base, "live": False, "live_reason": "no_coverage"}

    # Merge predictions onto scheduled departures by service + aimed time.
    overlaid = []
    matched  = 0
    for dep in departures:
        new      = dict(dep)
        svc      = dep.get("service") or ""
        aimed_dt = _parse_iso_datetime(dep.get("aimed_departure"))
        if aimed_dt is None:
            overlaid.append(new)
            continue

        svc_keys = {svc, _strip_night_prefix(svc)}

        best       = None
        best_delta = None
        for pred in predictions:
            if pred.get("service") not in svc_keys:
                continue
            pred_aimed = _parse_iso_datetime(pred.get("aimed"))
            if pred_aimed is None:
                continue
            delta = abs((pred_aimed - aimed_dt).total_seconds())
            if best_delta is None or delta < best_delta:
                best       = pred
                best_delta = delta

        if best is None or best_delta is None or best_delta > 300:
            overlaid.append(new)
            continue

        expected_dt = _parse_iso_datetime(best.get("expected"))
        if expected_dt is None:
            overlaid.append(new)
            continue

        delay                    = int((expected_dt - aimed_dt).total_seconds())
        new["expected_departure"] = expected_dt.isoformat()
        new["delay_seconds"]      = delay
        new["status"]             = _delay_to_status(delay)
        matched += 1
        overlaid.append(new)

    log.info("NextBuses overlay %s: matched %d/%d departures", stop_id, matched, len(departures))
    return {**base, "departures": overlaid, "live": True}


def _build_prediction_index(vehicles: list) -> dict:
    """
    Build a lookup keyed on (stop_id, service_ref) → list of calls,
    each call being {aimed: datetime|None, expected: datetime|None}.
    Prefers *departure* times; falls back to *arrival* times if the
    feed only publishes those.
    """
    index: dict = {}
    for v in vehicles:
        svc = v.get("service_ref")
        if not svc:
            continue
        for c in v.get("calls") or []:
            stop_id = c.get("stop_id")
            if not stop_id:
                continue
            aimed_dt    = _parse_iso_datetime(
                c.get("aimed_departure") or c.get("aimed_arrival"))
            expected_dt = _parse_iso_datetime(
                c.get("expected_departure") or c.get("expected_arrival"))
            if aimed_dt is None and expected_dt is None:
                continue
            index.setdefault((stop_id, svc), []).append({
                "aimed":    aimed_dt,
                "expected": expected_dt,
            })
    return index


def _delay_to_status(delay_secs: int) -> str:
    """Map a delay in seconds to a human-readable status string."""
    if abs(delay_secs) <= 60:
        return "On time"
    if delay_secs < 0:
        return "Early"
    return "Late"


# ── Trip matcher & enrichment ─────────────────────────────────
def _enrich_vehicles_with_trip_match(vehicles: list, tt: dict) -> None:
    """
    For each live vehicle, attempt to find the scheduled GTFS trip it
    is currently running. When a match is found, attach `trip_id` and
    `trip_headsign` to the vehicle dict in place.

    Strategy 1 (preferred): if the vehicle has MonitoredCall data, match
    on (service, next_stop_id, aimed_time).

    Strategy 2 (fallback): match on (service, origin_ref, destination_ref)
    from the SIRI-VM journey, picking the trip whose scheduled start
    is closest to the current time. This works even when operators don't
    publish MonitoredCall/OnwardCall blocks.
    """
    if not vehicles or not tt.get("trip_stops"):
        return

    now_local = datetime.now(UK_TZ)
    now_secs  = (now_local.hour * 3600
                 + now_local.minute * 60
                 + now_local.second)
    today     = now_local.date()
    dow       = today.weekday()
    today_str = today.strftime("%Y%m%d")
    calendar       = tt.get("calendar", {})
    calendar_dates = tt.get("calendar_dates", {})
    trips          = tt.get("trips", {})
    routes         = tt.get("routes", {})
    stop_times     = tt.get("stop_times", {})
    svc_endpoints  = tt.get("svc_trip_endpoints", {})

    matched_n = 0
    for v in vehicles:
        svc = v.get("service_ref") or ""
        if not svc:
            continue

        # ── Strategy 1: MonitoredCall-based matching ──
        calls = v.get("calls") or []
        if calls:
            next_call = calls[0]
            next_stop = next_call.get("stop_id")
            aimed_iso = (next_call.get("aimed_departure")
                         or next_call.get("aimed_arrival"))
            aimed_dt  = _parse_iso_datetime(aimed_iso)
            if next_stop and aimed_dt is not None:
                aimed_local = aimed_dt.astimezone(UK_TZ)
                aimed_secs  = (aimed_local.hour * 3600
                               + aimed_local.minute * 60
                               + aimed_local.second)
                match = _match_by_stop(
                    svc, next_stop, aimed_secs, stop_times, trips, routes,
                    today, today_str, dow, calendar, calendar_dates, 120)
                if match:
                    v["trip_id"]       = match
                    v["trip_headsign"] = trips.get(match, {}).get("headsign") or None
                    matched_n += 1
                    continue

        # ── Strategy 2: service + time-of-day matching ──
        # Without MonitoredCall data, match on service name and pick the
        # trip whose first departure is closest to now. The destination
        # name from SIRI-VM is used as a tiebreaker when multiple trips
        # start at similar times.
        svc_set = {svc, _strip_night_prefix(svc)}
        dest_name = (v.get("destination") or "").replace("_", " ").lower()
        best_trip  = None
        best_delta = None
        best_name_match = False

        for svc_key in svc_set:
            candidates = svc_endpoints.get(svc_key, [])
            for trip_id, first_stop, last_stop, first_secs in candidates:
                trip = trips.get(trip_id, {})
                if not _runs_today(trip.get("service_id", ""), today,
                                   today_str, dow, calendar, calendar_dates):
                    continue
                delta = abs(first_secs - now_secs)
                if delta > 43200:
                    delta = 86400 - delta
                headsign = (trip.get("headsign") or "").lower()
                name_match = (dest_name and headsign
                              and dest_name in headsign)
                if best_delta is None or delta < best_delta:
                    best_trip  = trip_id
                    best_delta = delta
                    best_name_match = name_match
                elif (delta == best_delta and name_match
                      and not best_name_match):
                    best_trip  = trip_id
                    best_name_match = name_match

        if best_trip is not None and best_delta is not None and best_delta <= 7200:
            v["trip_id"]       = best_trip
            v["trip_headsign"] = trips.get(best_trip, {}).get("headsign") or None
            matched_n += 1

    if vehicles:
        log.info("Trip match: %d/%d vehicles linked to a GTFS trip",
                 matched_n, len(vehicles))


def _match_by_stop(svc, stop_id, aimed_secs, stop_times, trips, routes,
                   today, today_str, dow, calendar, calendar_dates,
                   max_delta):
    """Try to match a vehicle to a trip via a stop_id and aimed time."""
    svc_set = {svc, _strip_night_prefix(svc)}
    entries = stop_times.get(stop_id, [])
    best_trip  = None
    best_delta = None
    for dep_secs, trip_id in entries:
        trip  = trips.get(trip_id, {})
        route = routes.get(trip.get("route_id", ""), {})
        short_name = route.get("short_name", "")
        if (short_name not in svc_set
                and _strip_night_prefix(short_name) not in svc_set):
            continue
        if not _runs_today(trip.get("service_id", ""), today, today_str,
                           dow, calendar, calendar_dates):
            continue
        delta = abs(dep_secs - aimed_secs)
        if delta > 43200:
            delta = 86400 - delta
        if best_delta is None or delta < best_delta:
            best_trip  = trip_id
            best_delta = delta
    if best_trip and best_delta is not None and best_delta <= max_delta:
        return best_trip
    return None


def _atco_match(ref: str, stop_id: str) -> bool:
    """Check if a SIRI-VM stop ref matches a GTFS stop_id, allowing
    for ATCO prefix differences (4400 vs 1490 vs 1400)."""
    if not ref or not stop_id:
        return False
    if ref == stop_id:
        return True
    if len(ref) >= 4 and len(stop_id) >= 4 and ref[4:] == stop_id[4:]:
        return True
    return False


def _upcoming_stops_from_trip(vehicle: dict, tt: dict, trip_id) -> list:
    """
    Return the list of upcoming scheduled stops for a vehicle whose
    trip has been matched. Slices the trip's stop sequence from the
    vehicle's next-stop onward; returns up to 12 entries.
    """
    if not trip_id:
        return []
    trip_stops = tt.get("trip_stops", {}).get(trip_id) or []
    if not trip_stops:
        return []

    calls = vehicle.get("calls") or []
    next_stop = calls[0].get("stop_id") if calls else None

    start_idx = 0

    if next_stop:
        for i, (_secs, sid) in enumerate(trip_stops):
            if sid == next_stop or _atco_match(next_stop, sid):
                start_idx = i
                break
    else:
        # No MonitoredCall — estimate position using current time.
        # Find the first stop whose scheduled departure is still ahead.
        now_local_tmp = datetime.now(UK_TZ)
        now_secs = (now_local_tmp.hour * 3600
                    + now_local_tmp.minute * 60
                    + now_local_tmp.second)
        for i, (dep_secs, _sid) in enumerate(trip_stops):
            if dep_secs >= now_secs:
                start_idx = i
                break
        else:
            start_idx = len(trip_stops)

    # Build a lookup from the vehicle's own SIRI-VM calls so we can
    # attach live predicted times when the operator provides them.
    call_pred: dict = {}
    for c in calls:
        sid = c.get("stop_id")
        if not sid:
            continue
        call_pred[sid] = (c.get("expected_departure")
                          or c.get("expected_arrival")
                          or c.get("aimed_departure")
                          or c.get("aimed_arrival"))

    stops_meta = tt.get("stops", {})
    now_local  = datetime.now(UK_TZ)
    out = []
    for dep_secs, sid in trip_stops[start_idx:start_idx + 12]:
        dep_h = (dep_secs // 3600) % 24
        dep_m = (dep_secs % 3600) // 60
        aimed_dt = now_local.replace(
            hour=dep_h, minute=dep_m, second=0, microsecond=0)
        out.append({
            "stop_id":          sid,
            "stop_name":        stops_meta.get(sid, {}).get("name", sid),
            "aimed_departure":  aimed_dt.isoformat(),
            "expected_departure": call_pred.get(sid),
        })
    return out


def _upcoming_stops_from_calls(vehicle: dict, tt: dict) -> list:
    """
    Fallback when no GTFS trip was matched: build the upcoming-stops
    list directly from the vehicle's SIRI-VM MonitoredCall + OnwardCall
    entries. Only as rich as what the operator chose to publish.
    """
    calls = vehicle.get("calls") or []
    if not calls:
        return []
    stops_meta = tt.get("stops", {})
    out = []
    for c in calls[:12]:
        sid = c.get("stop_id")
        if not sid:
            continue
        out.append({
            "stop_id":            sid,
            "stop_name":          stops_meta.get(sid, {}).get("name", sid),
            "aimed_departure":    (c.get("aimed_departure")
                                   or c.get("aimed_arrival")),
            "expected_departure": (c.get("expected_departure")
                                   or c.get("expected_arrival")),
        })
    return out

# ── Timetable loader ──────────────────────────────────────────
async def _get_timetable() -> dict:
    """
    Load timetable from data/timetable.json (built by GitHub Actions).
    Cached in memory for 1 hour, then re-read from disk in case
    the file was updated by a new Action run.
    """
    global _timetable, _timetable_at

    if _timetable and (time.time() - _timetable_at) < TIMETABLE_CACHE_TTL:
        return _timetable

    log.info("Loading timetable from %s…", TIMETABLE_PATH)

    try:
        with open(TIMETABLE_PATH, encoding="utf-8") as f:
            raw = json.load(f)

        # stop_times are stored as [[dep_secs, trip_id], ...] in JSON
        # Convert back to list of tuples for internal use
        raw["stop_times"] = {
            k: [(int(entry[0]), entry[1]) for entry in v]
            for k, v in raw.get("stop_times", {}).items()
        }

        _build_trip_indices(raw)

        _timetable    = raw
        _timetable_at = time.time()

        log.info(
            "Timetable loaded: %d stops, %d stop_time entries, "
            "%d routes, %d trips, %d route→trips, %d trip→stops",
            len(_timetable.get("stops",       {})),
            len(_timetable.get("stop_times",  {})),
            len(_timetable.get("routes",      {})),
            len(_timetable.get("trips",       {})),
            len(_timetable.get("route_trips", {})),
            len(_timetable.get("trip_stops",  {})),
        )

    except FileNotFoundError:
        log.error(
            "data/timetable.json not found. "
            "Run the GitHub Action or: "
            "BODS_API_KEY=your_key python scripts/build_timetable.py"
        )
        _timetable = {
            "stops": {}, "routes": {}, "trips": {},
            "stop_times": {}, "calendar": {}, "calendar_dates": {},
        }
        _timetable_at = time.time()

    except json.JSONDecodeError as exc:
        log.error("Corrupt timetable.json: %s", exc)
        _timetable = {
            "stops": {}, "routes": {}, "trips": {},
            "stop_times": {}, "calendar": {}, "calendar_dates": {},
        }
        _timetable_at = time.time()

    return _timetable

# ── Trip index builder ────────────────────────────────────────
def _build_trip_indices(tt: dict) -> None:
    """
    Build two in-memory indices used by the trip matcher and the
    upcoming-stops endpoint:

      trip_stops  : trip_id → [(dep_secs, stop_id), ...] ordered by time
      route_trips : route_id → [trip_id, ...]

    Runs once per timetable reload (cost ≈ 0.2 s on the current file).
    """
    trip_stops: dict = {}
    for stop_id, entries in tt.get("stop_times", {}).items():
        for dep_secs, trip_id in entries:
            trip_stops.setdefault(trip_id, []).append((int(dep_secs), stop_id))
    for entries in trip_stops.values():
        entries.sort()
    tt["trip_stops"] = trip_stops

    route_trips: dict = {}
    for trip_id, trip in tt.get("trips", {}).items():
        rid = trip.get("route_id")
        if rid:
            route_trips.setdefault(rid, []).append(trip_id)
    tt["route_trips"] = route_trips

    svc_trip_endpoints: dict = {}
    routes = tt.get("routes", {})
    trips = tt.get("trips", {})
    for trip_id, stops in trip_stops.items():
        if not stops:
            continue
        trip = trips.get(trip_id, {})
        route = routes.get(trip.get("route_id", ""), {})
        svc = route.get("short_name", "")
        if not svc:
            continue
        first_stop = stops[0][1]
        first_secs = stops[0][0]
        last_stop  = stops[-1][1]
        svc_trip_endpoints.setdefault(svc, []).append(
            (trip_id, first_stop, last_stop, first_secs))
    tt["svc_trip_endpoints"] = svc_trip_endpoints

# ── Departure calculation ─────────────────────────────────────
def _departures_for_stop(tt: dict, stop_id: str) -> dict:
    now_local = datetime.now(UK_TZ)
    today     = now_local.date()
    dow       = today.weekday()
    today_str = today.strftime("%Y%m%d")
    now_secs  = (now_local.hour * 3600
                 + now_local.minute * 60
                 + now_local.second)
    lookahead = 7200   # 2 hours

    variants  = _normalise_atco(stop_id)
    matched   = stop_id
    raw_times = []
    for v in variants:
        times = tt.get("stop_times", {}).get(v, [])
        if times:
            matched   = v
            raw_times = times
            break

    stop_name = tt.get("stops", {}).get(matched, {}).get("name", stop_id)

    if not raw_times:
        return {
            "stop_name":  stop_name,
            "departures": [],
            "note": (
                f"No timetable entry for stop {stop_id}. "
                "This stop may be served by an operator not yet in the "
                "timetable, or the timetable may need rebuilding via "
                "the GitHub Action."
            ),
        }

    departures = []
    for (dep_secs, trip_id) in raw_times:
        if dep_secs < now_secs or dep_secs > now_secs + lookahead:
            continue
        trip  = tt["trips"].get(trip_id, {})
        route = tt["routes"].get(trip.get("route_id", ""), {})
        sid   = trip.get("service_id", "")
        if not _runs_today(sid, today, today_str, dow,
                           tt["calendar"], tt["calendar_dates"]):
            continue
        dep_h  = (dep_secs // 3600) % 24
        dep_m  = (dep_secs % 3600) // 60
        dep_dt = now_local.replace(
            hour=dep_h, minute=dep_m, second=0, microsecond=0
        )
        departures.append({
            "service":            route.get("short_name", "?"),
            "destination":        trip.get("headsign", "Unknown"),
            "aimed_departure":    dep_dt.isoformat(),
            "expected_departure": None,   # Phase 2: filled from GTFS-RT
            "status":             "Scheduled",
            "delay_seconds":      None,   # Phase 2: filled from GTFS-RT
            "_trip_id":           trip_id,
        })

    departures.sort(key=lambda d: d["aimed_departure"])
    return {"stop_name": stop_name, "departures": departures[:15]}


def _runs_today(service_id: str, today: date, today_str: str,
                dow: int, calendar: dict, calendar_dates: dict) -> bool:
    exceptions = calendar_dates.get(service_id, {})
    if today_str in exceptions:
        return exceptions[today_str] == "1"
    cal = calendar.get(service_id)
    if not cal:
        return True   # no calendar info — assume it runs
    if (today_str < cal.get("start_date", "")
            or today_str > cal.get("end_date", "99991231")):
        return False
    days = ["monday","tuesday","wednesday","thursday",
            "friday","saturday","sunday"]
    return cal.get(days[dow], "0") == "1"

# ── Helpers ───────────────────────────────────────────────────
def _check_api_key():
    if not BODS_API_KEY:
        raise HTTPException(status_code=503,
            detail="BODS_API_KEY not configured.")

def _strip_night_prefix(svc: str) -> str:
    """
    Return the service label with a leading "N" removed when the rest is
    purely digits, so "N700" and "700" can be treated as the same line.
    Used to reconcile operators that publish night variants without the
    N prefix (Stagecoach SCSO) against timetables that keep it.
    """
    if svc and len(svc) > 1 and svc[0] in ("N", "n") and svc[1:].isdigit():
        return svc[1:]
    return svc

def _normalise_atco(stop_id: str) -> list:
    """
    Return lookup variants for a NaPTAN stop ID. West Sussex uses
    prefix 4400, Brighton & Hove uses 1490. SIRI-VM and timetable
    data may reference the same physical stop with either prefix,
    so try both forms.
    """
    variants = [stop_id]
    if stop_id.startswith("1400"):
        variants.append("4400" + stop_id[4:])
    elif stop_id.startswith("4400"):
        variants.append("1400" + stop_id[4:])
    return variants

def _safe_float(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

def _parse_iso_datetime(s: Optional[str]) -> Optional[datetime]:
    """
    Parse an ISO 8601 timestamp (as emitted by BODS SIRI-VM) into a
    timezone-aware UTC datetime. Returns None if it can't be parsed.
    """
    if not s:
        return None
    try:
        # Python's fromisoformat only gained 'Z' support in 3.11;
        # normalise for older runtimes just in case.
        normalised = s.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalised)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


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
