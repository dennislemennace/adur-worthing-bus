"""
api/main.py — Adur & Worthing Bus Tracker Backend v1.3
=======================================================
Changes from v1.2:
  - Replaced GTFS CSV parser with TransXChange XML parser
    (BODS timetable zips contain .xml files, not .txt files)
  - Limited to 2 most-recent datasets per operator to avoid timeout
  - Overpass used for stop coordinates; TransXChange for timetables
  - ATCO codes from TransXChange matched against Overpass stop markers

Environment variables:
  BODS_API_KEY    — https://data.bus-data.dft.gov.uk/
  ALLOWED_ORIGIN  — your GitHub Pages URL
"""

import io
import os
import re
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
logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s  %(name)s  %(message)s")
log = logging.getLogger("bus_api")

# ────────────────────────────────────────────────────────────
# CONFIGURATION
# ────────────────────────────────────────────────────────────
BODS_API_KEY   = os.environ.get("BODS_API_KEY", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")

BODS_BASE = "https://data.bus-data.dft.gov.uk/api/v1"
SIRI_NS   = "http://www.siri.org.uk/siri"
TXC_NS    = "http://www.transxchange.org.uk/"

# Bounding box for Adur & Worthing
BBOX_MIN_LAT, BBOX_MAX_LAT =  50.78,  50.87
BBOX_MIN_LON, BBOX_MAX_LON = -0.42,  -0.10
BBOX_STR = f"{BBOX_MIN_LON},{BBOX_MIN_LAT},{BBOX_MAX_LON},{BBOX_MAX_LAT}"

# Operators serving Adur & Worthing — add more as you discover them
# in the operator_ref field of /api/vehicles
AREA_OPERATOR_NOCS = [
    "SCSO",   # Stagecoach South
    "BHBC",   # Brighton & Hove Bus and Coach Company
    "CMPA",   # Compass Travel (Sussex)
    "METR",   # Metrobus
]

# Maximum datasets to download per operator.
# Keeps total download time within Vercel's 60-second function limit.
MAX_DATASETS_PER_OPERATOR = 2

GTFS_CACHE_TTL = 86_400   # Cache timetable data for 24 hours

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

def cache_set(key: str, data, ttl: int) -> None:
    _cache[key] = (data, time.time() + ttl)

# ────────────────────────────────────────────────────────────
# TIMETABLE DATA STORE
# ────────────────────────────────────────────────────────────
_timetable:      Optional[dict] = None
_timetable_at:   float          = 0.0

# ────────────────────────────────────────────────────────────
# APP
# ────────────────────────────────────────────────────────────
app = FastAPI(title="Adur & Worthing Bus API", version="1.3.0")

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
    t = _timetable or {}
    return {
        "status":               "ok",
        "bods_key_configured":  bool(BODS_API_KEY),
        "timetable_loaded":     _timetable is not None,
        "stops_in_timetable":   len(t.get("stops",      {})),
        "stop_times_indexed":   len(t.get("stop_times", {})),
        "routes_loaded":        len(t.get("routes",     {})),
        "trips_loaded":         len(t.get("trips",      {})),
    }

# ────────────────────────────────────────────────────────────
# ENDPOINT: /api/stops
# Coordinates from Overpass (OSM) — stop IDs from Overpass
# are matched against TransXChange ATCO codes at departure time.
# ────────────────────────────────────────────────────────────
@app.get("/api/stops")
async def get_stops():
    """
    Bus stops in the Adur & Worthing bounding box.
    Source: Overpass (OpenStreetMap) — no dependency on timetable load.
    Cached 24 h.
    """
    cached = cache_get("stops")
    if cached:
        return cached

    try:
        stops = await _fetch_overpass_stops()
    except HTTPException:
        # Overpass occasionally has outages — return empty rather than crashing
        log.warning("Overpass fetch failed — returning empty stop list")
        return {"stops": [], "count": 0}

    result = {"stops": stops, "count": len(stops)}
    cache_set("stops", result, GTFS_CACHE_TTL)
    log.info("Serving %d stops from Overpass", len(stops))
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
        name = tags.get("name") or tags.get("naptan:CommonName") or "Bus Stop"
        stops.append({"atco_code": atco, "name": name,
                      "latitude": lat, "longitude": lon})
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
    result   = {"vehicles": vehicles, "count": len(vehicles)}
    cache_set("vehicles", result, 15)
    log.info("Fetched %d vehicles", len(vehicles))
    return result


async def _fetch_siri_vm() -> list[dict]:
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


def _parse_siri_vm(xml_text: str) -> list[dict]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    ns, vehicles = {"s": SIRI_NS}, []

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
    """Scheduled departures for a stop. Cached 60 s."""
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

# ────────────────────────────────────────────────────────────
# TIMETABLE LOADER
# ────────────────────────────────────────────────────────────
async def _get_timetable() -> dict:
    global _timetable, _timetable_at
    if _timetable and (time.time() - _timetable_at) < GTFS_CACHE_TTL:
        return _timetable

    log.info("Loading timetable from BODS TransXChange datasets…")
    _timetable   = await _download_and_merge_timetables()
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
        "stops":          {},
        "routes":         {},
        "trips":          {},
        "stop_times":     {},
        "calendar":       {},
        "calendar_dates": {},
    }

    urls = await _discover_dataset_urls()
    if not urls:
        log.warning("No dataset URLs found — check NOC codes and API key")
        return merged

    for url in urls:
        try:
            parsed = await _download_and_parse_dataset(url)
            _merge_into(merged, parsed)
        except Exception as exc:
            log.warning("Skipping %s — %s", url, exc)

    return merged


async def _discover_dataset_urls() -> list[str]:
    """
    Query the BODS dataset API for each operator NOC.
    Returns at most MAX_DATASETS_PER_OPERATOR URLs per operator,
    preferring the most recently modified datasets.
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

            count = 0
            for dataset in results:
                if count >= MAX_DATASETS_PER_OPERATOR:
                    break

                # Build the download URL
                dl_url = dataset.get("url", "")
                if not dl_url:
                    continue
                if "/download/" not in dl_url:
                    dl_url = dl_url.rstrip("/") + "/download/"

                if dl_url in seen:
                    continue

                seen.add(dl_url)
                urls.append(dl_url)
                count += 1
                log.info("  Queued: %s", dl_url)

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

# ────────────────────────────────────────────────────────────
# TRANSXCHANGE XML PARSER
# ────────────────────────────────────────────────────────────
def _parse_transxchange_zip(zip_bytes: bytes) -> dict:
    """
    A BODS timetable zip contains one or more TransXChange .xml files.
    Parse each one and merge the results.
    """
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
    """
    Parse a single TransXChange XML file and merge data into `out`.

    TransXChange structure (simplified):
      TransXChange
        ├── StopPoints / AnnotatedStopPointRefs  ← stop names & ATCO codes
        ├── Services                              ← routes / line names
        ├── JourneyPatternSections               ← timing links between stops
        └── VehicleJourneys                      ← individual timed trips
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        log.warning("XML parse error: %s", exc)
        return

    ns = {"t": TXC_NS}

    def tx(el, path, default=""):
        """Shortcut: findtext with TransXChange namespace."""
        return el.findtext(f"t:{path}", default, ns)

    def tf(el, path):
        """Shortcut: find with TransXChange namespace."""
        return el.find(f"t:{path}", ns)

    # ── 1. Stop names (no coordinates in TransXChange) ───────
    for s in root.findall(".//t:AnnotatedStopPointRef", ns):
        ref  = tx(s, "StopPointRef")
        name = tx(s, "CommonName", "Bus Stop")
        if ref and ref not in out["stops"]:
            out["stops"][ref] = {"name": name}

    for s in root.findall(".//t:StopPoint", ns):
        ref  = tx(s, "AtcoCode") or tx(s, "StopPointRef")
        name = tx(s, "Descriptor/CommonName") or tx(s, "CommonName")
        if ref and ref not in out["stops"]:
            out["stops"][ref] = {"name": name or "Bus Stop"}

    # ── 2. Services → Routes ──────────────────────────────────
    for svc in root.findall(".//t:Service", ns):
        code = tx(svc, "ServiceCode")
        if not code:
            continue
        line_el   = svc.find(".//t:Line", ns)
        line_name = tx(line_el, "LineName") if line_el is not None else ""
        desc      = tx(svc, "Description")
        out["routes"][code] = {
            "short_name": line_name,
            "long_name":  desc,
        }

    # ── 3. Journey Pattern Sections ───────────────────────────
    # jps_id → [(stop_atco, cumulative_offset_seconds_at_departure)]
    jps_map: dict[str, list] = {}

    for jps in root.findall(".//t:JourneyPatternSection", ns):
        jps_id = jps.get("id", "")
        links  = jps.findall("t:JourneyPatternTimingLink", ns)
        cumul  = 0
        seq    = []

        for i, link in enumerate(links):
            from_el  = tf(link, "From")
            to_el    = tf(link, "To")
            if from_el is None or to_el is None:
                continue

            from_ref  = tx(from_el, "StopPointRef")
            to_ref    = tx(to_el,   "StopPointRef")
            wait_from = _dur(tx(from_el, "WaitTime"))
            run_time  = _dur(tx(link,    "RunTime"))
            wait_to   = _dur(tx(to_el,   "WaitTime"))

            # First stop: departs after its own wait time
            if i == 0 and from_ref:
                seq.append((from_ref, cumul + wait_from))
            cumul += wait_from + run_time
            if to_ref:
                seq.append((to_ref, cumul + wait_to))

        jps_map[jps_id] = seq

    # ── 4. Journey Patterns ───────────────────────────────────
    # jp_id → combined stop sequence [(stop_atco, offset_secs)]
    jp_map: dict[str, list] = {}

    for jp in root.findall(".//t:JourneyPattern", ns):
        jp_id    = jp.get("id", "")
        sec_refs = [el.text for el in jp.findall("t:JourneyPatternSectionRefs", ns) if el.text]
        combined = []
        for sr in sec_refs:
            combined.extend(jps_map.get(sr, []))
        jp_map[jp_id] = combined

    # ── 5. Vehicle Journeys → Trips + Stop Times ──────────────
    for vj in root.findall(".//t:VehicleJourney", ns):
        vj_code  = tx(vj, "VehicleJourneyCode")
        svc_ref  = tx(vj, "ServiceRef")
        jp_ref   = tx(vj, "JourneyPatternRef")
        dep_str  = tx(vj, "DepartureTime")

        if not dep_str or not jp_ref:
            continue

        # Parse HH:MM:SS departure time
        base_secs = _hms_to_secs(dep_str)
        if base_secs < 0:
            continue

        # Operating calendar
        trip_id    = vj_code or f"{svc_ref}_{jp_ref}_{dep_str}"
        op_prof_el = tf(vj, "OperatingProfile")
        cal        = _parse_operating_profile(op_prof_el, ns)
        out["calendar"][trip_id] = cal

        # Destination headsign — try several locations in the XML
        headsign = (tx(vj, "DestinationDisplay")
                    or tx(vj, "Operational/TicketMachine/JourneyCode")
                    or "")

        # Look up route name for headsign fallback
        if not headsign:
            route = out["routes"].get(svc_ref, {})
            headsign = route.get("long_name") or route.get("short_name") or ""

        out["trips"][trip_id] = {
            "route_id":   svc_ref,
            "service_id": trip_id,
            "headsign":   headsign,
        }

        # Build stop times from journey pattern
        stop_seq = jp_map.get(jp_ref, [])
        for (stop_ref, offset) in stop_seq:
            if not stop_ref:
                continue
            dep_secs = base_secs + offset
            if stop_ref not in out["stop_times"]:
                out["stop_times"][stop_ref] = []
            out["stop_times"][stop_ref].append((dep_secs, trip_id))


def _parse_operating_profile(el, ns) -> dict:
    """
    Convert a TransXChange OperatingProfile element to a simple
    {monday..sunday, start_date, end_date} dict.
    Defaults to running every day if the element is missing.
    """
    cal = {
        "monday": "0", "tuesday": "0", "wednesday": "0",
        "thursday": "0", "friday": "0", "saturday": "0", "sunday": "0",
        "start_date": "20240101",
        "end_date":   "20991231",
    }

    if el is None:
        for d in cal:
            if d not in ("start_date", "end_date"):
                cal[d] = "1"
        return cal

    days_el = el.find(".//t:DaysOfWeek", {"t": TXC_NS})
    if days_el is None:
        for d in cal:
            if d not in ("start_date", "end_date"):
                cal[d] = "1"
        return cal

    # Map TransXChange day element names to our dict keys
    day_map = {
        "Monday":           ["monday"],
        "Tuesday":          ["tuesday"],
        "Wednesday":        ["wednesday"],
        "Thursday":         ["thursday"],
        "Friday":           ["friday"],
        "Saturday":         ["saturday"],
        "Sunday":           ["sunday"],
        "MondayToFriday":   ["monday","tuesday","wednesday","thursday","friday"],
        "MondayToSaturday": ["monday","tuesday","wednesday","thursday","friday","saturday"],
        "MondayToSunday":   ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"],
        "Weekend":          ["saturday","sunday"],
        "HolidaysOnly":     [],
        "NotSaturday":      ["monday","tuesday","wednesday","thursday","friday","sunday"],
    }

    ns_t = {"t": TXC_NS}
    for tag, days in day_map.items():
        if days_el.find(f"t:{tag}", ns_t) is not None:
            for d in days:
                cal[d] = "1"

    return cal

# ────────────────────────────────────────────────────────────
# DEPARTURE CALCULATION
# ────────────────────────────────────────────────────────────
def _departures_for_stop(tt: dict, stop_id: str) -> dict:
    now_local = datetime.now()
    today     = now_local.date()
    dow       = today.weekday()           # 0 = Mon … 6 = Sun
    today_str = today.strftime("%Y%m%d")
    now_secs  = (now_local.hour * 3600
                 + now_local.minute * 60
                 + now_local.second)
    lookahead = 7200                      # 2 hours

    stop_name = tt.get("stops", {}).get(stop_id, {}).get("name", stop_id)
    raw_times = tt.get("stop_times", {}).get(stop_id, [])

    if not raw_times:
        return {
            "stop_name":  stop_name,
            "departures": [],
            "note": (
                f"No timetable entry found for stop {stop_id}. "
                "This stop may be served by an operator not yet in our "
                "timetable, or the ATCO code in OpenStreetMap may not "
                "match the timetable. Try a nearby stop."
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
            "expected_departure": None,    # Phase 2: filled from GTFS-RT
            "status":             "Scheduled",
            "delay_seconds":      None,    # Phase 2: filled from GTFS-RT
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
        return True   # default to showing if no calendar info
    if today_str < cal.get("start_date","") or today_str > cal.get("end_date","99991231"):
        return False
    days = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
    return cal.get(days[dow], "0") == "1"

# ────────────────────────────────────────────────────────────
# MERGE HELPER
# ────────────────────────────────────────────────────────────
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
      
@app.on_event("startup")
async def warmup():
    """
    Pre-load the stops cache when the server starts.
    This means the first map load is fast even on a cold Vercel instance.
    Timetable loading is intentionally NOT done here — it's too slow for
    startup and is loaded lazily on the first /api/departures request.
    """
    try:
        log.info("Startup: warming stops cache…")
        stops = await _fetch_overpass_stops()
        cache_set("stops", {"stops": stops, "count": len(stops)}, GTFS_CACHE_TTL)
        log.info("Startup: cached %d stops", len(stops))
    except Exception as exc:
        log.warning("Startup warmup failed (non-fatal): %s", exc)
# ────────────────────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────────────────────
def _check_api_key():
    if not BODS_API_KEY:
        raise HTTPException(status_code=503,
            detail="BODS_API_KEY not configured.")

def _safe_float(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

def _hms_to_secs(t: str) -> int:
    """Convert HH:MM:SS to seconds since midnight. Returns -1 on error."""
    try:
        parts = t.strip().split(":")
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except (ValueError, IndexError):
        return -1

def _dur(s: str) -> int:
    """Parse ISO 8601 duration to seconds. e.g. PT5M30S → 330. Returns 0 on error."""
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
