"""
scripts/build_timetable.py
==========================
Run by GitHub Actions weekly to download and pre-process
timetable data from BODS, saving the result to data/timetable.json

This uses the BODS regional GTFS feed for South East England, which
covers all operators serving West Sussex in a single standardised
download (rather than hunting operator-by-operator through
TransXChange files).

Run locally:
    python scripts/build_timetable.py
"""

import csv
import io
import json
import logging
import os
import sys
import tempfile
import zipfile
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
# BODS publishes regional GTFS bundles; South East covers West Sussex
GTFS_URL = "https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/south_east/"

# Keep stops whose ATCO code begins with this prefix (West Sussex = 4400).
# NaPTAN admin area 440 = West Sussex, giving stop IDs like "4400AD0316"
# for Adur, "4400WO..." for Worthing, etc.
WEST_SUSSEX_ATCO_PREFIX = "4400"

OUTPUT_PATH = Path(__file__).parent.parent / "data" / "timetable.json"


# ── Main ─────────────────────────────────────────────────────
def main():
    log.info("Script started")
    log.info("Output path: %s", OUTPUT_PATH)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        download_gtfs(GTFS_URL, tmp_path)
        timetable = parse_gtfs(tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    # Convert stop_times tuples to lists for JSON serialisation
    timetable["stop_times"] = {
        k: [[dep_secs, trip_id] for (dep_secs, trip_id) in v]
        for k, v in timetable["stop_times"].items()
    }

    log.info("Writing timetable to %s", OUTPUT_PATH)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(timetable, f, separators=(",", ":"))

    size_mb = OUTPUT_PATH.stat().st_size / 1_048_576
    total_stop_time_entries = sum(
        len(v) for v in timetable["stop_times"].values()
    )
    log.info("Done — %.1f MB written", size_mb)
    log.info(
        "Final counts: %d stops, %d routes, %d trips, "
        "%d stops with departures, %d total stop_time entries",
        len(timetable["stops"]),
        len(timetable["routes"]),
        len(timetable["trips"]),
        len(timetable["stop_times"]),
        total_stop_time_entries,
    )


# ── Download ─────────────────────────────────────────────────
def download_gtfs(url: str, dest_path: str) -> None:
    log.info("Downloading GTFS from %s", url)
    last_exc = None
    for attempt in range(3):
        try:
            with open(dest_path, "wb") as out, \
                 httpx.Client(timeout=600, follow_redirects=True) as client:
                with client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    total = 0
                    for chunk in resp.iter_bytes(chunk_size=262144):
                        out.write(chunk)
                        total += len(chunk)
            log.info("Downloaded %.1f MB", total / 1_048_576)
            return
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            last_exc = exc
            wait = 2 ** attempt
            log.warning(
                "  Download attempt %d/3 failed: %s — retrying in %ds",
                attempt + 1, exc, wait,
            )
            import time as _time
            _time.sleep(wait)
    raise RuntimeError(f"GTFS download failed after 3 attempts: {last_exc}")


# ── GTFS parser ──────────────────────────────────────────────
def parse_gtfs(zip_path: str) -> dict:
    """
    Extract West Sussex timetable data from a GTFS zip.
    Returns a dict matching the format the backend expects.
    """
    timetable = {
        "stops":          {},
        "routes":         {},
        "trips":          {},
        "stop_times":     {},
        "calendar":       {},
        "calendar_dates": {},
    }

    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        log.info("GTFS zip contains: %s", sorted(names))

        _require(names, "stops.txt")
        _require(names, "stop_times.txt")
        _require(names, "trips.txt")
        _require(names, "routes.txt")

        # 1. stops.txt — keep West Sussex stops only
        log.info("Parsing stops.txt…")
        ws_stop_ids = set()
        with zf.open("stops.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                stop_id = row.get("stop_id", "")
                if not stop_id.startswith(WEST_SUSSEX_ATCO_PREFIX):
                    continue
                ws_stop_ids.add(stop_id)
                timetable["stops"][stop_id] = {
                    "name": row.get("stop_name") or "Bus Stop",
                }
        log.info("  %d West Sussex stops", len(ws_stop_ids))

        if not ws_stop_ids:
            log.error("No West Sussex stops found — aborting")
            return timetable

        # 2. stop_times.txt — keep only rows for WS stops.
        # This is the biggest file; stream through it.
        log.info("Parsing stop_times.txt (this is the big one)…")
        needed_trip_ids = set()
        row_count = 0
        kept_count = 0
        with zf.open("stop_times.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                row_count += 1
                if row_count % 1_000_000 == 0:
                    log.info(
                        "  processed %s rows, kept %s entries",
                        f"{row_count:,}", f"{kept_count:,}",
                    )
                stop_id = row.get("stop_id", "")
                if stop_id not in ws_stop_ids:
                    continue
                trip_id = row.get("trip_id", "")
                dep_time = (row.get("departure_time")
                            or row.get("arrival_time", ""))
                dep_secs = _hms_to_secs(dep_time)
                if dep_secs < 0 or not trip_id:
                    continue
                if stop_id not in timetable["stop_times"]:
                    timetable["stop_times"][stop_id] = []
                timetable["stop_times"][stop_id].append((dep_secs, trip_id))
                needed_trip_ids.add(trip_id)
                kept_count += 1
        log.info(
            "  processed %s rows total; kept %s entries across %d trips",
            f"{row_count:,}", f"{kept_count:,}", len(needed_trip_ids),
        )

        # 3. trips.txt — keep only trips that serve a WS stop
        log.info("Parsing trips.txt…")
        needed_route_ids = set()
        needed_service_ids = set()
        with zf.open("trips.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                trip_id = row.get("trip_id", "")
                if trip_id not in needed_trip_ids:
                    continue
                route_id = row.get("route_id", "")
                service_id = row.get("service_id", "")
                timetable["trips"][trip_id] = {
                    "route_id":   route_id,
                    "service_id": service_id,
                    "headsign":   row.get("trip_headsign") or "",
                }
                needed_route_ids.add(route_id)
                needed_service_ids.add(service_id)
        log.info(
            "  %d trips, %d routes, %d services needed",
            len(timetable["trips"]),
            len(needed_route_ids),
            len(needed_service_ids),
        )

        # 4. routes.txt
        log.info("Parsing routes.txt…")
        with zf.open("routes.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                route_id = row.get("route_id", "")
                if route_id not in needed_route_ids:
                    continue
                timetable["routes"][route_id] = {
                    "short_name": row.get("route_short_name") or "",
                    "long_name":  row.get("route_long_name") or "",
                }
        log.info("  %d routes kept", len(timetable["routes"]))

        # 5. calendar.txt
        if "calendar.txt" in names:
            log.info("Parsing calendar.txt…")
            with zf.open("calendar.txt") as f:
                reader = csv.DictReader(
                    io.TextIOWrapper(f, encoding="utf-8-sig")
                )
                for row in reader:
                    service_id = row.get("service_id", "")
                    if service_id not in needed_service_ids:
                        continue
                    timetable["calendar"][service_id] = {
                        "monday":     row.get("monday",    "0"),
                        "tuesday":    row.get("tuesday",   "0"),
                        "wednesday":  row.get("wednesday", "0"),
                        "thursday":   row.get("thursday",  "0"),
                        "friday":     row.get("friday",    "0"),
                        "saturday":   row.get("saturday",  "0"),
                        "sunday":     row.get("sunday",    "0"),
                        "start_date": row.get("start_date") or "20240101",
                        "end_date":   row.get("end_date")   or "20991231",
                    }
            log.info("  %d calendar entries kept", len(timetable["calendar"]))

        # 6. calendar_dates.txt — exception days (added/removed services)
        if "calendar_dates.txt" in names:
            log.info("Parsing calendar_dates.txt…")
            entry_count = 0
            with zf.open("calendar_dates.txt") as f:
                reader = csv.DictReader(
                    io.TextIOWrapper(f, encoding="utf-8-sig")
                )
                for row in reader:
                    service_id = row.get("service_id", "")
                    if service_id not in needed_service_ids:
                        continue
                    date = row.get("date", "")
                    # GTFS exception_type: 1 = service added, 2 = removed
                    value = "1" if row.get("exception_type") == "1" else "0"
                    if service_id not in timetable["calendar_dates"]:
                        timetable["calendar_dates"][service_id] = {}
                    timetable["calendar_dates"][service_id][date] = value
                    entry_count += 1
            log.info(
                "  %d calendar_dates entries across %d services",
                entry_count, len(timetable["calendar_dates"]),
            )

    # Sort stop_times per stop so the backend can slice them in order
    for stop_id, times in timetable["stop_times"].items():
        times.sort(key=lambda t: t[0])

    return timetable


# ── Helpers ───────────────────────────────────────────────────
def _require(names: set, filename: str) -> None:
    if filename not in names:
        raise RuntimeError(f"GTFS zip missing required file: {filename}")


def _hms_to_secs(t: str) -> int:
    """
    Convert a GTFS HH:MM:SS time to seconds since midnight.
    GTFS allows hours > 24 for overnight services — we mod by 86400
    so they land in the next-day slot.
    """
    if not t:
        return -1
    try:
        parts = t.strip().split(":")
        secs = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        return secs % 86400
    except (ValueError, IndexError):
        return -1


# ── Entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        import traceback
        print("FATAL ERROR:", exc, flush=True)
        traceback.print_exc()
        sys.exit(1)
