"""
api/main.py — Adur & Worthing Bus Tracker Backend v1.5
"""

import io
import os
import time
import logging
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, date
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s  %(name)s  %(message)s")
log = logging.getLogger("bus_api")

# ── Config ───────────────────────────────────────────────────
BODS_API_KEY   = os.environ.get("BODS_API_KEY", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")

BODS_BASE = "https://data.bus-data.dft.gov.uk/api/v1"
SIRI_NS   = "http://www.siri.org.uk/siri"
TXC_NS    = "http://www.transxchange.org.uk/"

BBOX_MIN_LAT, BBOX_MAX_LAT =  50.78,  50.87
BBOX_MIN_LON, BBOX_MAX_LON = -0.42,  -0.10
BBOX_STR = f"{BBOX_MIN_LON},{BBOX_MIN_LAT},{BBOX_MAX_LON},{BBOX_MAX_LAT}"

# West Sussex ATCO area prefix
WEST_SUSSEX_ATCO_PREFIX = "1400"

AREA_OPERATOR_NOCS = [
    "SCSO",
    "BHBC",
    "CMPA",
    "METR",
]

# Increased to 8 so we reach the West Sussex datasets
# (earlier datasets for each operator may cover other regions)
MAX_DATASETS_PER_OPERATOR = 4
TIMETABLE_CACHE_TTL = 86_400

# ── Cache ─────────────────────────────────────────────────────
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
app = FastAPI(title="Adur & Worthing Bus API", version="1.5.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN] if ALLOWED_ORIGIN != "*" else ["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Health ────────────────────────────────────────────────────
@app.get("/")
async def root():
    t = _timetable or {}
    return {
        "status":              "ok",
        "bods_key_configured": bool(BODS_API_KEY),
        "timetable_loaded":    _timetable is not None,
        "stops_in_timetable":  len(t.get("stops",      {})),
        "stop_times_indexed":  len(t.get("stop_times", {})),
        "routes_loaded":       len(t.get("routes",     {})),
        "trips_loaded":        len(t.get("trips",      {})),
    }

# ── Debug endpoint (remove once departures are working) ───────
@app.get("/api/debug/stop")
async def debug_stop(stopId: str = Query(...)):
    tt        = _timetable or {}
    variants  = _normalise_atco(stopId)
    now_local = datetime.now()
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
    cached = cache_get("stops")
    if cached:
        return cached
    try:
        stops = await _fetch_overpass_stops()
    except Exception as exc:
        log.warning("Overpass fetch failed: %s", exc)
        return {"stops": [], "count": 0}
    result = {"stops": stops, "count": len(stops)}
    cache_set("stops", result, TIMETABLE_CACHE_TTL)
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
        # Correct OSM 4400 prefix to proper West Sussex 1400 prefix
        if atco.startswith("4400"):
            atco = "1400" + atco[4:]
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

# ── /api/departures ───────────────────────────────────────────
@app.get("/api/departures")
async def get_departures(stopId: str = Query(...)):
    _check_api_key()
    if not stopId or len(stopId) > 20:
        raise HTTPException(status_code=400, detail="Invalid stopId.")
    cached = cache_get(f"dep:{stopId}")
    if cached:
        return cached
    tt     = await _get_timetable()
    result = _departures_for_stop(tt, stopId)
    cache_set(f"dep:{stopId}", result, 60)
    return result

# ── Timetable loader ──────────────────────────────────────────
async def _get_timetable() -> dict:
    global _timetable, _timetable_at
    if _timetable and (time.time() - _timetable_at) < TIMETABLE_CACHE_TTL:
        return _timetable
    log.info("Loading timetable from BODS…")
    _timetable    = await _download_and_merge_timetables()
    _timetable_at = time.time()
    log.info(
        "Timetable ready: %d stops, %d stop_time entries, %d routes, %d trips",
        len(_timetable.get("stops",      {})),
        len(_timetable.get("stop_times", {})),
        len(_timetable.get("routes",     {})),
        len(_timetable.get("trips",      {})),
    )
    return _timetable


async def _download_and_merge_timetables() -> dict:
    merged: dict = {
        "stops": {}, "routes": {}, "trips": {},
        "stop_times": {}, "calendar": {}, "calendar_dates": {},
    }
    urls = await _discover_dataset_urls()
    if not urls:
        log.warning("No dataset URLs found")
        return merged
    for url in urls:
        try:
            parsed = await _download_and_parse_dataset(url)
            _merge_into(merged, parsed)
        except Exception as exc:
            log.warning("Skipping %s — %s", url, exc)
    return merged


async def _discover_dataset_urls() -> list:
    """
    Query BODS timetable datasets by bounding box only.
    This returns all operators covering Adur & Worthing in one call,
    avoiding the 400 error caused by combining noc + boundingBox.
    """
    urls = []
    seen = set()
    page = 1

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            try:
                resp = await client.get(
                    f"{BODS_BASE}/dataset/",
                    params={
                        "api_key":    BODS_API_KEY,
                        "status":     "published",
                        "limit":      25,
                        "offset":     (page - 1) * 25,
                        "adminArea":  "014",  # West Sussex admin area code
                    },
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                log.warning("Dataset query failed (page %d): %s", page, exc)
                break

            results = data.get("results", [])
            log.info("Page %d — %d dataset(s) returned", page, len(results))

            for dataset in results:
                dl_url = dataset.get("url", "")
                if not dl_url:
                    continue
                if "/download/" not in dl_url:
                    dl_url = dl_url.rstrip("/") + "/download/"
                if dl_url in seen:
                    continue
                seen.add(dl_url)
                urls.append(dl_url)
                log.info("  Queued: %s", dl_url)

            # Stop if we have enough or there are no more pages
            if len(urls) >= MAX_DATASETS_PER_OPERATOR or not data.get("next"):
                break
            page += 1

    log.info("Total datasets to download: %d", len(urls))
    return urls


async def _download_and_parse_dataset(url: str) -> dict:
    log.info("Downloading %s", url)
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        resp = await client.get(url, params={"api_key": BODS_API_KEY})
        resp.raise_for_status()
        zip_bytes = resp.content
    log.info("Downloaded %.1f MB", len(zip_bytes) / 1_048_576)
    return _parse_transxchange_zip(zip_bytes)

# ── TransXChange parser ───────────────────────────────────────
def _parse_transxchange_zip(zip_bytes: bytes) -> dict:
    result: dict = {
        "stops": {}, "routes": {}, "trips": {},
        "stop_times": {}, "calendar": {}, "calendar_dates": {},
    }
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            xml_files = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            log.info("  Zip has %d XML file(s)", len(xml_files))
            for xml_name in xml_files:
                with zf.open(xml_name) as f:
                    _parse_txc_xml(f.read(), result)
    except zipfile.BadZipFile as exc:
        log.error("Bad zip: %s", exc)
    return result


def _parse_txc_xml(xml_bytes: bytes, out: dict) -> None:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        log.warning("XML parse error: %s", exc)
        return

    ns = {"t": TXC_NS}

    def tx(el, path, default=""):
        return el.findtext(f"t:{path}", default, ns)

    def tf(el, path):
        return el.find(f"t:{path}", ns)

    # ── Stop names ────────────────────────────────────────────
    for s in root.findall(".//t:AnnotatedStopPointRef", ns):
        ref  = tx(s, "StopPointRef")
        name = tx(s, "CommonName", "Bus Stop")
        if ref and ref not in out["stops"]:
            out["stops"][ref] = {"name": name}

    for s in root.findall(".//t:StopPoint", ns):
        ref  = tx(s, "AtcoCode") or tx(s, "StopPointRef")
        name = (tx(s, "Descriptor/CommonName")
                or tx(s, "CommonName", "Bus Stop"))
        if ref and ref not in out["stops"]:
            out["stops"][ref] = {"name": name}

    # ── Routes ────────────────────────────────────────────────
    for svc in root.findall(".//t:Service", ns):
        code = tx(svc, "ServiceCode")
        if not code:
            continue
        line_el   = svc.find(".//t:Line", ns)
        line_name = tx(line_el, "LineName") if line_el is not None else ""
        out["routes"][code] = {
            "short_name": line_name,
            "long_name":  tx(svc, "Description"),
        }

    # ── Journey Pattern Sections ──────────────────────────────
    jps_map: dict = {}
    for jps in root.findall(".//t:JourneyPatternSection", ns):
        jps_id = jps.get("id", "")
        links  = jps.findall("t:JourneyPatternTimingLink", ns)
        cumul  = 0
        seq    = []
        for i, link in enumerate(links):
            from_el = tf(link, "From")
            to_el   = tf(link, "To")
            if from_el is None or to_el is None:
                continue
            from_ref  = tx(from_el, "StopPointRef")
            to_ref    = tx(to_el,   "StopPointRef")
            wait_from = _dur(tx(from_el, "WaitTime"))
            run_time  = _dur(tx(link,    "RunTime"))
            wait_to   = _dur(tx(to_el,   "WaitTime"))
            if i == 0 and from_ref:
                seq.append((from_ref, cumul + wait_from))
            cumul += wait_from + run_time
            if to_ref:
                seq.append((to_ref, cumul + wait_to))
        jps_map[jps_id] = seq

    # ── Journey Patterns ──────────────────────────────────────
    jp_map: dict = {}
    for jp in root.findall(".//t:JourneyPattern", ns):
        jp_id    = jp.get("id", "")
        sec_refs = [
            el.text for el in
            jp.findall("t:JourneyPatternSectionRefs", ns)
            if el.text
        ]
        combined = []
        for sr in sec_refs:
            combined.extend(jps_map.get(sr, []))
        jp_map[jp_id] = combined

    # ── Vehicle Journeys ──────────────────────────────────────
    for vj in root.findall(".//t:VehicleJourney", ns):
        vj_code  = tx(vj, "VehicleJourneyCode")
        svc_ref  = tx(vj, "ServiceRef")
        jp_ref   = tx(vj, "JourneyPatternRef")
        dep_str  = tx(vj, "DepartureTime")
        if not dep_str or not jp_ref:
            continue
        base_secs = _hms_to_secs(dep_str)
        if base_secs < 0:
            continue

        trip_id = vj_code or f"{svc_ref}_{jp_ref}_{dep_str}"
        op_el   = tf(vj, "OperatingProfile")
        out["calendar"][trip_id] = _parse_operating_profile(op_el)

        headsign = tx(vj, "DestinationDisplay") or ""
        if not headsign:
            route    = out["routes"].get(svc_ref, {})
            headsign = route.get("long_name") or route.get("short_name") or ""

        out["trips"][trip_id] = {
            "route_id":   svc_ref,
            "service_id": trip_id,
            "headsign":   headsign,
        }

        # Only index West Sussex stops (ATCO prefix 1400)
        # so Surrey/London stops from multi-area operators are excluded
        for (stop_ref, offset) in jp_map.get(jp_ref, []):
            if not stop_ref:
                continue
            if not stop_ref.startswith(WEST_SUSSEX_ATCO_PREFIX):
                continue
            dep_secs = base_secs + offset
            if stop_ref not in out["stop_times"]:
                out["stop_times"][stop_ref] = []
            out["stop_times"][stop_ref].append((dep_secs, trip_id))


def _parse_operating_profile(el) -> dict:
    cal = {
        "monday": "0", "tuesday": "0", "wednesday": "0",
        "thursday": "0", "friday": "0", "saturday": "0", "sunday": "0",
        "start_date": "20240101",
        "end_date":   "20991231",
    }
    if el is None:
        for d in list(cal.keys()):
            if d not in ("start_date", "end_date"):
                cal[d] = "1"
        return cal

    ns_t    = {"t": TXC_NS}
    days_el = el.find(".//t:DaysOfWeek", ns_t)
    if days_el is None:
        for d in list(cal.keys()):
            if d not in ("start_date", "end_date"):
                cal[d] = "1"
        return cal

    day_map = {
        "Monday":           ["monday"],
        "Tuesday":          ["tuesday"],
        "Wednesday":        ["wednesday"],
        "Thursday":         ["thursday"],
        "Friday":           ["friday"],
        "Saturday":         ["saturday"],
        "Sunday":           ["sunday"],
        "MondayToFriday":   ["monday","tuesday","wednesday","thursday","friday"],
        "MondayToSaturday": ["monday","tuesday","wednesday","thursday",
                             "friday","saturday"],
        "MondayToSunday":   ["monday","tuesday","wednesday","thursday",
                             "friday","saturday","sunday"],
        "Weekend":          ["saturday","sunday"],
        "NotSaturday":      ["monday","tuesday","wednesday","thursday",
                             "friday","sunday"],
        "HolidaysOnly":     [],
    }
    for tag, days in day_map.items():
        if days_el.find(f"t:{tag}", ns_t) is not None:
            for d in days:
                cal[d] = "1"
    return cal

# ── Departure calculation ─────────────────────────────────────
def _departures_for_stop(tt: dict, stop_id: str) -> dict:
    now_local = datetime.now()
    today     = now_local.date()
    dow       = today.weekday()
    today_str = today.strftime("%Y%m%d")
    now_secs  = (now_local.hour * 3600
                 + now_local.minute * 60
                 + now_local.second)
    lookahead = 7200

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
                "This stop may be served by an operator not yet in our "
                "timetable, or the ATCO code may not match. "
                "Try a nearby stop."
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
            "expected_departure": None,
            "status":             "Scheduled",
            "delay_seconds":      None,
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
        return True
    if (today_str < cal.get("start_date", "")
            or today_str > cal.get("end_date", "99991231")):
        return False
    days = ["monday","tuesday","wednesday","thursday",
            "friday","saturday","sunday"]
    return cal.get(days[dow], "0") == "1"

# ── Merge ─────────────────────────────────────────────────────
def _merge_into(merged: dict, parsed: dict) -> None:
    merged["stops"].update(parsed.get("stops", {}))
    merged["routes"].update(parsed.get("routes", {}))
    merged["trips"].update(parsed.get("trips", {}))
    merged["calendar"].update(parsed.get("calendar", {}))
    for sid, dates in parsed.get("calendar_dates", {}).items():
        if sid not in merged["calendar_dates"]:
            merged["calendar_dates"][sid] = {}
        merged["calendar_dates"][sid].update(dates)
    for stop_id, times in parsed.get("stop_times", {}).items():
        if stop_id not in merged["stop_times"]:
            merged["stop_times"][stop_id] = []
        merged["stop_times"][stop_id].extend(times)

# ── Helpers ───────────────────────────────────────────────────
def _check_api_key():
    if not BODS_API_KEY:
        raise HTTPException(status_code=503,
            detail="BODS_API_KEY not configured.")

def _normalise_atco(stop_id: str) -> list:
    if stop_id.startswith("4400"):
        return ["1400" + stop_id[4:], stop_id]
    return [stop_id]

def _safe_float(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

def _hms_to_secs(t: str) -> int:
    try:
        parts = t.strip().split(":")
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except (ValueError, IndexError):
        return -1

def _dur(s: str) -> int:
    if not s:
        return 0
    return abs(_parse_iso_duration(s) or 0)

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
