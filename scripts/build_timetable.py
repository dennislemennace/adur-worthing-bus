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
import json
import logging
import zipfile
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s  %(message)s")
log = logging.getLogger("build_timetable")

# ── Config ───────────────────────────────────────────────────
BODS_API_KEY = os.environ["BODS_API_KEY"]
BODS_BASE    = "https://data.bus-data.dft.gov.uk/api/v1"
TXC_NS       = "http://www.transxchange.org.uk/"

WEST_SUSSEX_ATCO_PREFIX = "1400"

AREA_OPERATOR_NOCS = [
    "SCSO",
    "BHBC",
    "CMPA",
    "METR",
]

MAX_DATASETS_PER_OPERATOR = 6
OUTPUT_PATH = Path(__file__).parent.parent / "data" / "timetable.json"


# ── Main ─────────────────────────────────────────────────────
def main():
    OUTPUT_PATH.parent.mkdir(exist_ok=True)

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
        log.error("No dataset URLs found — aborting")
        return

    for url in urls:
        try:
            parsed = download_and_parse(url)
            merge_into(timetable, parsed)
            log.info(
                "Running totals: %d stops, %d stop_times, %d trips",
                len(timetable["stop_times"]),
                sum(len(v) for v in timetable["stop_times"].values()),
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
        json.dump(timetable, f, separators=(",", ":"))  # compact JSON

    size_mb = OUTPUT_PATH.stat().st_size / 1_048_576
    log.info("Done — %.1f MB written", size_mb)
    log.info("Final counts: %d stops, %d routes, %d trips",
             len(timetable["stops"]),
             len(timetable["routes"]),
             len(timetable["trips"]))


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
            try:
                resp = client.get(
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
                log.warning("Query failed for NOC %s: %s", noc, exc)
                continue

            results = data.get("results", [])
            log.info("NOC %s — %d dataset(s)", noc, len(results))

            count = 0
            for dataset in results:
                if count >= MAX_DATASETS_PER_OPERATOR:
                    break

                if not _covers_west_sussex(dataset, WEST_SUSSEX_MARKERS):
                    log.info("  Skipping '%s' — not West Sussex",
                             dataset.get("name", ""))
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
                log.info("  Queued: %s", dataset.get("name", dl_url))

            # Fallback — queue first dataset if none matched West Sussex
            if count == 0 and results:
                dl_url = results[0].get("url", "")
                if dl_url and dl_url not in seen:
                    if "/download/" not in dl_url:
                        dl_url = dl_url.rstrip("/") + "/download/"
                    seen.add(dl_url)
                    urls.append(dl_url)
                    log.info("  Fallback: %s", dl_url)

    log.info("Total datasets to download: %d", len(urls))
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

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = tmp.name
        with httpx.Client(timeout=300, follow_redirects=True) as client:
            with client.stream("GET", url,
                               params={"api_key": BODS_API_KEY}) as resp:
                resp.raise_for_status()
                total = 0
                for chunk in resp.iter_bytes(chunk_size=65536):
                    tmp.write(chunk)
                    total += len(chunk)
        log.info("Downloaded %.1f MB", total / 1_048_576)

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

                    # Filter to West Sussex only
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
                        log.info("  %d/%d — %d WS stops so far",
                                 i + 1, len(all_xml),
                                 len(result["stop_times"]))

                except Exception as exc:
                    log.warning("  Skipping %s — %s", xml_name, exc)
    finally:
        os.unlink(tmp_path)

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
        name = tx(s, "Descriptor/CommonName") or tx(s, "CommonName", "Bus Stop")
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
        for i, link in enumerate(jps.findall("t:JourneyPatternTimingLink", ns)):
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
        jp_id
