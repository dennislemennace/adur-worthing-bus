"""
scripts/build_timetable.py
==========================
Run by GitHub Actions weekly to download and pre-process
timetable data from BODS, saving the result to data/timetable.json

Run locally:
    BODS_API_KEY=your_key python scripts/build_timetable.py
"""

import io
import os
import sys
import json
import logging
import zipfile
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

# Flush output immediately so GitHub Actions shows logs in real time
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("build_timetable")

try:
    import httpx
except ImportError:
    print("FATAL: httpx is not installed. Run: pip install httpx", flush=True)
    sys.exit(1)

# ── Config ───────────────────────────────────────────────────
BODS_API_KEY = os.environ.get("BODS_API_KEY", "")
BODS_BASE    = "https://data.bus-data.dft.gov.uk/api/v1"
TXC_NS       = "http://www.transxchange.org.uk/"

WEST_SUSSEX_ATCO_PREFIX = "1400"

AREA_OPERATOR_NOCS = [
    "SCSO",
    "BHBC",
    "CMPA",
    "METR",
]

MAX_DATASETS_PER_OPERATOR = 10
OUTPUT_PATH = Path(__file__).parent.parent / "data" / "timetable.json"


# ── Main ─────────────────────────────────────────────────────
def main():
    print("Script started", flush=True)
    print(f"Output path: {OUTPUT_PATH}", flush=True)
    print(f"API key present: {bool(BODS_API_KEY)}", flush=True)
    print(f"API key length: {len(BODS_API_KEY)}", flush=True)

    if not BODS_API_KEY:
        print("ERROR: BODS_API_KEY is not set — aborting", flush=True)
        sys.exit(1)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"Output directory ready: {OUTPUT_PATH.parent}", flush=True)

    timetable: dict = {
        "stops":          {},
        "routes":         {},
        "trips":          {},
        "stop_times":     {},
        "calendar":       {},
        "calendar_dates": {},
    }

    urls = discover_dataset_urls()
    if not urls:
        log.error("No dataset URLs found — writing empty timetable")
        # Write empty file so the backend doesn't crash on startup
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(timetable, f)
        return

    for url in urls:
        try:
            parsed = download_and_parse(url)
            merge_into(timetable, parsed)
            log.info(
                "Running totals: %d WS stops, %d trips",
                len(timetable["stop_times"]),
                len(timetable["trips"]),
            )
        except Exception as exc:
            log.warning("Skipping %s — %s", url, exc)

    # Convert stop_times tuples to lists for JSON serialisation
    timetable["stop_times"] = {
        k: [[dep_secs, trip_id] for dep_secs, trip_id in v]
        for k, v in timetable["stop_times"].items()
    }

    log.info("Writing timetable to %s", OUTPUT_PATH)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(timetable, f, separators=(",", ":"))

    size_mb = OUTPUT_PATH.stat().st_size / 1_048_576
    log.info("Done — %.1f MB written", size_mb)
    log.info(
        "Final counts: %d stops, %d routes, %d trips, %d stop_time entries",
        len(timetable["stops"]),
        len(timetable["routes"]),
        len(timetable["trips"]),
        len(timetable["stop_times"]),
    )


# ── HTTP retry helper ─────────────────────────────────────────
def _http_get_with_retry(client, url, retries=3, **kwargs):
    """GET request with exponential backoff retry."""
    import time as _time
    last_exc = None
    for attempt in range(retries):
        try:
            resp = client.get(url, **kwargs)
            resp.raise_for_status()
            return resp
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            last_exc = exc
            wait = 2 ** attempt
            log.warning("  Attempt %d/%d failed: %s — retrying in %ds",
                        attempt + 1, retries, exc, wait)
            _time.sleep(wait)
    raise last_exc


def _http_stream_with_retry(client, url, retries=3, **kwargs):
    """Streaming GET with exponential backoff retry."""
    import time as _time
    last_exc = None
    for attempt in range(retries):
        try:
            return client.stream("GET", url, **kwargs)
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            last_exc = exc
            wait = 2 ** attempt
            log.warning("  Stream attempt %d/%d failed: %s — retrying in %ds",
                        attempt + 1, retries, exc, wait)
            _time.sleep(wait)
    raise last_exc


# ── Dataset discovery ─────────────────────────────────────────
def discover_dataset_urls() -> list:
    urls = []
    seen = set()

    WEST_SUSSEX_MARKERS = {
        "west sussex", "worthing", "adur", "horsham",
        "crawley", "chichester", "arun", "mid sussex"
    }

    with httpx.Client(timeout=30) as client:
        for noc in AREA_OPERATOR_NOCS:
            log.info("Querying BODS for NOC: %s", noc)
            try:
                resp = _http_get_with_retry(
                    client,
                    f"{BODS_BASE}/dataset/",
                    params={
                        "api_key": BODS_API_KEY,
                        "noc":     noc,
                        "status":  "published",
                        "limit":   25,
                    },
                )
                data = resp.json()
            except Exception as exc:
                log.warning("  Query failed for NOC %s after retries: %s", noc, exc)
                continue

            results = data.get("results", [])
            total   = data.get("count", "?")
            next_pg = data.get("next", None)
            log.info("  Found %d dataset(s) (total=%s, has_next=%s)",
                     len(results), total, bool(next_pg))

            # Log what we found for debugging
            for i, ds in enumerate(results[:5]):
                log.info("  Dataset %d: %s | adminAreas: %s | localities: %s",
                         i,
                         ds.get("name", "?"),
                         [a.get("name","?") for a in ds.get("adminAreas", [])[:3]],
                         [l.get("name","?") for l in ds.get("localities", [])[:3]])

            count = 0
            for dataset in results:
                if count >= MAX_DATASETS_PER_OPERATOR:
                    break

                covers = _covers_west_sussex(dataset, WEST_SUSSEX_MARKERS)
                log.info("  '%s' covers West Sussex: %s",
                         dataset.get("name", "?"), covers)

                if not covers:
                    continue

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

            # Fallback — if nothing matched West Sussex filter,
            # queue all datasets for this operator up to the limit
            if count == 0 and results:
                log.info(
                    "  No WS match for NOC %s — queuing all %d as fallback",
                    noc, min(len(results), MAX_DATASETS_PER_OPERATOR)
                )
                for dataset in results[:MAX_DATASETS_PER_OPERATOR]:
                    dl_url = dataset.get("url", "")
                    if not dl_url:
                        continue
                    if "/download/" not in dl_url:
                        dl_url = dl_url.rstrip("/") + "/download/"
                    if dl_url in seen:
                        continue
                    seen.add(dl_url)
                    urls.append(dl_url)
                    log.info("  Fallback queued: %s", dl_url)

    log.info("=== Discovery summary ===")
    log.info("Total datasets to download: %d", len(urls))
    for i, u in enumerate(urls):
        log.info("  [%d] %s", i + 1, u)
    if not urls:
        log.warning("No dataset URLs found! Check BODS API key and operator NOCs.")
    return urls


def _covers_west_sussex(dataset: dict, markers: set) -> bool:
    for area in dataset.get("adminAreas", []):
        name = (area.get("name") or "").lower()
        code = str(area.get("atcoAreaCode") or "")
        if any(m in name for m in markers) or code in ("14", "140"):
            return True
    for locality in dataset.get("localities", []):
        if any(m in (locality.get("name") or "").lower() for m in markers):
            return True
    name = (dataset.get("name") or "").lower()
    desc = (dataset.get("description") or "").lower()
    return any(m in name or m in desc for m in markers)


# ── Download and parse ────────────────────────────────────────
def download_and_parse(url: str) -> dict:
    log.info("Downloading %s", url)
    result: dict = {
        "stops": {}, "routes": {}, "trips": {},
        "stop_times": {}, "calendar": {}, "calendar_dates": {},
    }

    import time as _time
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = tmp.name
        last_exc = None
        for attempt in range(3):
            try:
                with httpx.Client(timeout=600, follow_redirects=True) as client:
                    with client.stream(
                        "GET", url, params={"api_key": BODS_API_KEY}
                    ) as resp:
                        resp.raise_for_status()
                        total = 0
                        for chunk in resp.iter_bytes(chunk_size=65536):
                            tmp.write(chunk)
                            total += len(chunk)
                log.info("Downloaded %.1f MB to disk", total / 1_048_576)
                last_exc = None
                break
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                last_exc = exc
                wait = 2 ** attempt
                log.warning("  Download attempt %d/3 failed: %s — retrying in %ds",
                            attempt + 1, exc, wait)
                tmp.seek(0)
                tmp.truncate()
                _time.sleep(wait)
        if last_exc:
            raise last_exc

    try:
        with zipfile.ZipFile(tmp_path) as zf:
            all_xml = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            log.info("Zip has %d XML file(s)", len(all_xml))

            for i, xml_name in enumerate(all_xml):
                try:
                    with zf.open(xml_name) as f:
                        xml_bytes = f.read()

                    temp: dict = {
                        "stops": {}, "routes": {}, "trips": {},
                        "stop_times": {}, "calendar": {}, "calendar_dates": {},
                    }
                    parse_txc_xml(xml_bytes, temp)
                    del xml_bytes

                    # Filter to West Sussex stops only
                    ws_stop_times = {
                        k: v for k, v in temp["stop_times"].items()
                        if k.startswith(WEST_SUSSEX_ATCO_PREFIX)
                    }
                    ws_trip_ids = {
                        trip_id
                        for times in ws_stop_times.values()
                        for (_, trip_id) in times
                    }
                    ws_trips     = {k: v for k, v in temp["trips"].items()
                                    if k in ws_trip_ids}
                    ws_route_ids = {t["route_id"] for t in ws_trips.values()}
                    ws_routes    = {k: v for k, v in temp["routes"].items()
                                    if k in ws_route_ids}
                    ws_calendar  = {k: v for k, v in temp["calendar"].items()
                                    if k in ws_trip_ids}

                    result["stops"].update(temp["stops"])
                    result["routes"].update(ws_routes)
                    result["trips"].update(ws_trips)
                    result["calendar"].update(ws_calendar)
                    for stop_id, times in ws_stop_times.items():
                        if stop_id not in result["stop_times"]:
                            result["stop_times"][stop_id] = []
                        result["stop_times"][stop_id].extend(times)
                    del temp

                    if (i + 1) % 50 == 0 or (i + 1) == len(all_xml):
                        log.info(
                            "  %d/%d files — %d WS stops, %d WS trips",
                            i + 1, len(all_xml),
                            len(result["stop_times"]),
                            len(result["trips"]),
                        )

                except Exception as exc:
                    log.warning("  Skipping %s — %s", xml_name, exc)
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass

    return result


# ── TransXChange parser ───────────────────────────────────────
def parse_txc_xml(xml_bytes: bytes, out: dict) -> None:
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

    # Stop names
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

    # Routes
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

    # Journey Pattern Sections
    jps_map: dict = {}
    for jps in root.findall(".//t:JourneyPatternSection", ns):
        jps_id = jps.get("id", "")
        cumul, seq = 0, []
        for i, link in enumerate(
            jps.findall("t:JourneyPatternTimingLink", ns)
        ):
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

    # Journey Patterns
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

    # Vehicle Journeys
    for vj in root.findall(".//t:VehicleJourney", ns):
        vj_code   = tx(vj, "VehicleJourneyCode")
        svc_ref   = tx(vj, "ServiceRef")
        jp_ref    = tx(vj, "JourneyPatternRef")
        dep_str   = tx(vj, "DepartureTime")
        if not dep_str or not jp_ref:
            continue
        base_secs = _hms_to_secs(dep_str)
        if base_secs < 0:
            continue

        trip_id  = vj_code or f"{svc_ref}_{jp_ref}_{dep_str}"
        op_el    = tf(vj, "OperatingProfile")
        out["calendar"][trip_id] = _parse_operating_profile(op_el)

        headsign = tx(vj, "DestinationDisplay") or ""
        if not headsign:
            route    = out["routes"].get(svc_ref, {})
            headsign = (route.get("long_name")
                        or route.get("short_name") or "")

        out["trips"][trip_id] = {
            "route_id":   svc_ref,
            "service_id": trip_id,
            "headsign":   headsign,
        }

        for (stop_ref, offset) in jp_map.get(jp_ref, []):
            if not stop_ref:
                continue
            dep_secs = base_secs + offset
            if stop_ref not in out["stop_times"]:
                out["stop_times"][stop_ref] = []
            out["stop_times"][stop_ref].append((dep_secs, trip_id))


def _parse_operating_profile(el) -> dict:
    cal = {
        "monday": "0", "tuesday": "0", "wednesday": "0",
        "thursday": "0", "friday": "0", "saturday": "0", "sunday": "0",
        "start_date": "20240101", "end_date": "20991231",
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
        "MondayToFriday":   ["monday","tuesday","wednesday",
                             "thursday","friday"],
        "MondayToSaturday": ["monday","tuesday","wednesday",
                             "thursday","friday","saturday"],
        "MondayToSunday":   ["monday","tuesday","wednesday","thursday",
                             "friday","saturday","sunday"],
        "Weekend":          ["saturday","sunday"],
        "NotSaturday":      ["monday","tuesday","wednesday",
                             "thursday","friday","sunday"],
        "HolidaysOnly":     [],
    }
    for tag, days in day_map.items():
        if days_el.find(f"t:{tag}", ns_t) is not None:
            for d in days:
                cal[d] = "1"
    return cal


# ── Merge ─────────────────────────────────────────────────────
def merge_into(merged: dict, parsed: dict) -> None:
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
def _hms_to_secs(t: str) -> int:
    try:
        parts = t.strip().split(":")
        return (int(parts[0]) * 3600
                + int(parts[1]) * 60
                + int(parts[2]))
    except (ValueError, IndexError):
        return -1


def _dur(s: str) -> int:
    if not s:
        return 0
    try:
        s = s.strip().lstrip("-P")
        parts = s.split("T")
        t    = parts[1] if len(parts) > 1 else parts[0]
        secs = 0
        for token, mult in [("H", 3600), ("M", 60), ("S", 1)]:
            idx = t.find(token)
            if idx >= 0:
                try:
                    secs += int(float(t[:idx])) * mult
                except ValueError:
                    pass
                t = t[idx + 1:]
        return secs
    except Exception:
        return 0


# ── Entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        import traceback
        print("FATAL ERROR:", exc, flush=True)
        traceback.print_exc()
        sys.exit(1)
