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

# ── In-memory cache ───────────────────────────────────────────
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

# ── /api/vehicles ─────────────────────────────────────────────
@app.get("/api/vehicles")
async def get_vehicles():
    """Live bus positions from BODS SIRI-VM. Cached 15 s."""
    _check_api_key()
    cached = cache_get("vehicles")
    if cached:
        return cached
    vehicles = await _fetch_siri_vm()
    result   = {"vehicles": vehicles, "count": len(vehicles)}
    cache_set("vehicles", result, 15)
    log.info("Fetched %d vehicles", len(vehicles))
    return result


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

        vehicles.append({
            "vehicle_ref":   jtext("VehicleRef"),
            "service_ref":   jtext("PublishedLineName") or jtext("LineRef"),
            "operator_ref":  jtext("OperatorRef"),
            "destination":   jtext("DestinationName") or jtext("DirectionRef"),
            "latitude":      lat,
            "longitude":     lon,
            "bearing":       _safe_float(jtext("Bearing")),
            "delay_seconds": delay_s,
            "recorded_at":   recorded_at,
        })

    if dropped_stale:
        log.info("Dropped %d stale vehicle(s) (>%ds old)",
                 dropped_stale, VEHICLE_STALE_SECONDS)
    return vehicles

# ── /api/departures ───────────────────────────────────────────
@app.get("/api/departures")
async def get_departures(stopId: str = Query(...)):
    """
    Scheduled departures for a stop from the pre-built timetable, with
    a live "fleet delay" overlay applied from current SIRI-VM vehicles.

    The schedule itself is cached for 60 s; the live overlay is
    re-applied on every request so users always see the freshest delay.
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

    vehicles = await _get_vehicles_or_empty()
    return _apply_live_overlay(base, vehicles)


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


def _apply_live_overlay(base: dict, vehicles: list) -> dict:
    """
    Overlay current vehicle delays onto a scheduled departures payload.

    Strategy (Phase 2a — fleet-delay heuristic):
      • Group all live vehicles by service_ref.
      • For each service with at least one delay reading, take the
        median delay_seconds across its vehicles.
      • Apply that delta to every scheduled departure of the same
        service to compute expected_departure / status / delay_seconds.

    The base payload is never mutated; a new dict is returned with
    fresh departure entries.
    """
    departures = base.get("departures") or []
    if not departures or not vehicles:
        return base

    delays_by_service: dict = {}
    for v in vehicles:
        svc = v.get("service_ref")
        delay = v.get("delay_seconds")
        if svc and delay is not None:
            delays_by_service.setdefault(svc, []).append(int(delay))

    if not delays_by_service:
        return base

    overlaid = []
    for dep in departures:
        new = dict(dep)
        svc = dep.get("service")
        delays = delays_by_service.get(svc)
        if delays:
            ordered = sorted(delays)
            median = ordered[len(ordered) // 2]
            try:
                aimed_dt = datetime.fromisoformat(dep["aimed_departure"])
                expected_dt = aimed_dt + timedelta(seconds=median)
                new["expected_departure"] = expected_dt.isoformat()
                new["delay_seconds"]      = median
                new["status"]             = _delay_to_status(median)
            except (ValueError, KeyError, TypeError):
                pass
        overlaid.append(new)

    return {**base, "departures": overlaid}


def _delay_to_status(delay_secs: int) -> str:
    """Map a delay in seconds to a human-readable status string."""
    if abs(delay_secs) <= 60:
        return "On time"
    if delay_secs < 0:
        return "Early"
    return "Late"

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

        _timetable    = raw
        _timetable_at = time.time()

        log.info(
            "Timetable loaded: %d stops, %d stop_time entries, "
            "%d routes, %d trips",
            len(_timetable.get("stops",      {})),
            len(_timetable.get("stop_times", {})),
            len(_timetable.get("routes",     {})),
            len(_timetable.get("trips",      {})),
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

def _normalise_atco(stop_id: str) -> list:
    """
    Return lookup variants for a NaPTAN stop ID. Historically the
    backend mangled 4400 (West Sussex) into 1400 (East Sussex); we
    now keep the original code and also try the mangled form so old
    clients/cached references still resolve.
    """
    variants = [stop_id]
    if stop_id.startswith("1400"):
        variants.append("4400" + stop_id[4:])
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
