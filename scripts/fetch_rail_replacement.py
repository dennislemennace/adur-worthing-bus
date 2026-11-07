#!/usr/bin/env python3
"""Where rail replacement buses stop at each station, from the operator's posters.

Southern publishes a "Rail Replacement Bus Services" poster for each station,
which National Rail links from the station's page: where the buses stop, how
far it is to walk, and a what3words address, over a map. The site draws each
stop on the map and names it on the station's board when a replacement bus is
due (app.js). Brighton's was read by hand on 1 November 2026; this reads every
station's (asked for 7 November 2026, the evening Coastway West ran as buses).

For each station in data/rail_stations.json: its National Rail page, the
poster PDF it links (..._RRB_<CRS>_...pdf), the poster's code, its "Display
until" date, its SHA-256 and the what3words addresses on it (pdftotext).

Each stop's words and position are curated by hand in
data/rail_replacement_positions.json, by what3words address: the posters'
text comes out of the PDF in map-label order, too tangled to read reliably,
and a what3words address cannot be turned into a position without
what3words' own service. Positions are placed from the poster's map against
OpenStreetMap, each saying how. A curated stop whose address has gone from
its poster, or an address on a poster with no curated stop, is reported and
left out, never guessed.

    python scripts/fetch_rail_replacement.py [--posters DIR] [--only CRS,CRS]

About thirty requests a run (a page and a poster a station), a second apart,
with a named User-Agent (LIMITS.md).
"""

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATIONS = ROOT / "data" / "rail_stations.json"
OUT = ROOT / "data" / "rail_replacement_stops.json"
POSITIONS = ROOT / "data" / "rail_replacement_positions.json"
UA = {"User-Agent": "adur-worthing-bus/1.0 (rail replacement posters, run by hand; "
                    "+https://github.com/dennislemennace/adur-worthing-bus)"}
POSTER = re.compile(r'https://assets\.nationalrail\.co\.uk/[^"\s]*?_RRB_([A-Z]{3})_[^"\s]*?\.pdf')


def station_page(name: str) -> str:
    """National Rail's address for a station: "London Road (Brighton)" is
    london-road-brighton, "Shoreham-By-Sea" shoreham-by-sea."""
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return f"https://www.nationalrail.co.uk/stations/{slug}/"


def get(url: str) -> bytes:
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60) as resp:
        return resp.read()


def poster_meta(text: str) -> dict:
    code = re.search(r"\b(\d{4}-\d{2}_[A-Z0-9_]*RRB_[A-Z]{3}_[A-Z]+)\b", text)
    until = re.search(r"Display until:\s*(\d{2})/(\d{2})/(\d{4})", text)
    return {"poster": code.group(1) if code else "",
            "display_until": f"{until.group(3)}-{until.group(2)}-{until.group(1)}" if until else None}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--posters", default=str(Path.home() / ".cache" / "adur-worthing-bus" / "rrb-posters"),
                    help="where the PDFs are kept, for placing the stops by hand")
    ap.add_argument("--only", help="comma-separated CRS codes")
    args = ap.parse_args()
    folder = Path(args.posters)
    folder.mkdir(parents=True, exist_ok=True)
    stations = json.loads(STATIONS.read_text())["stations"]
    only = set(args.only.split(",")) if args.only else None
    curated = (json.loads(POSITIONS.read_text()) if POSITIONS.exists() else {}).get("stops", {})
    old = json.loads(OUT.read_text()) if OUT.exists() else {"stations": []}
    kept = {s["crs"]: s for s in old.get("stations", [])}
    today = date.today().isoformat()
    unplaced, missing, changed = [], [], []
    for st in stations:
        crs = st["crs"]
        if only and crs not in only:
            continue
        page = station_page(st["name"])
        try:
            html = get(page).decode("utf-8", "replace")
        except Exception as err:                                  # noqa: BLE001
            missing.append(f"{crs}: {page} could not be read ({err})")
            continue
        time.sleep(1)
        found = [m for m in POSTER.finditer(html) if m.group(1) == crs]
        if not found:
            missing.append(f"{crs}: no rail replacement poster on {page}")
            continue
        url = found[0].group(0)
        pdf = get(url)
        time.sleep(1)
        path = folder / f"{crs}.pdf"
        path.write_bytes(pdf)
        text = subprocess.run(["pdftotext", str(path), "-"], capture_output=True, text=True, check=True).stdout
        meta = poster_meta(text)
        sha = hashlib.sha256(pdf).hexdigest()
        on_poster = set(re.findall(r"///([a-z]+\.[a-z]+\.[a-z]+)", text))
        out = []
        for w3w, stop in curated.items():
            if stop["crs"] != crs:
                continue
            if w3w not in on_poster:
                changed.append(f"{crs}: ///{w3w} ({stop['name']}) is no longer on the poster; check {path}")
                continue
            out.append({
                "name": stop["name"], "for": stop.get("for"), "access": stop.get("access"), "walk": stop.get("walk"),
                "what3words": w3w, "lat": stop["lat"], "lon": stop["lon"], "position_basis": stop["position_basis"],
                "source": f"Southern, Rail Replacement Bus Services: {st['name']} (poster {meta['poster']})",
                "source_url": url, "station_page": page, "source_sha256": sha,
                "poster_date": meta["poster"][:7] if meta["poster"] else None,
                "display_until": meta["display_until"], "checked_on": today,
            })
        for w3w in sorted(on_poster - set(curated)):
            unplaced.append(f"{crs}: ///{w3w} is on the poster but not in {POSITIONS.name}; poster in {path}")
        if out:
            kept[crs] = {"crs": crs, "station": st["name"], "stops": out}
        print(f"{crs}: {len(out)} stop{'s' if len(out) != 1 else ''} placed from {meta['poster'] or url}")
    doc = {**{k: v for k, v in old.items() if k != "stations"},
           "stations": [kept[s["crs"]] for s in stations if s["crs"] in kept]}
    OUT.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    for line in missing + changed + unplaced:
        print("  " + line, file=sys.stderr)
    if unplaced:
        print(f"{len(unplaced)} stop(s) need a position in {POSITIONS.relative_to(ROOT)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
