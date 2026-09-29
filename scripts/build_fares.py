"""Write data/fare_tables.json: each local route's published adult single fares.

Operators publish their fares to the Bus Open Data Service as NeTEx. For each
route and direction there is a "fare triangle": the route's fare stages, each a
group of stops, and the adult single between every pair of them. It is the
same table printed in the timetable leaflet, and the only source that says what
a short hop actually costs rather than what the national cap allows.

Two dialects are read, because the operators here use both:

* Brighton & Hove, Compass and Metrobus: each stage pair names a price band
  (`PriceGroupRef`), and the band has the amount.
* Stagecoach (Fare Exchange): each stage pair has its own
  `DistanceMatrixElementPrice` with the amount.

Only adult singles are kept, and only tables with a stop in the map area
(data/stops.json), so the file stays small. Where an operator publishes the
same route and direction more than once, the one valid latest is kept.

**What it can't tell you.** A published fare is the operator's statement, not
a measurement. It is the on-bus price; app and contactless fares can be lower.
England's national cap applies on top, so the site never quotes more than the
cap (data/ticket_zones.json, `fares_meta.single_fare`).

Usage:
    python scripts/build_fares.py [--zip-dir DIR] [--out data/fare_tables.json]

Without --zip-dir the four datasets are downloaded from BODS, which needs no key.
"""

import argparse
import io
import json
import re
import sys
import urllib.request
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent

# BODS fares dataset ids, one per operator serving the area. Found on the BODS
# fares search; each replaces the operator's earlier upload in place.
DATASETS = {"BHBC": 14265, "SCSO": 5354, "COMT": 6229, "METR": 14264}
DOWNLOAD = "https://data.bus-data.dft.gov.uk/fares/dataset/{id}/download/"
PAGE = "https://data.bus-data.dft.gov.uk/fares/dataset/{id}/"
# The download is refused to clients that do not look like a browser.
USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/130 Safari/537.36")

# Adult singles only: "AdultSingle" (Go-Ahead, Compass) or "ADULT-SINGLE"
# written with a Unicode hyphen (Stagecoach).
ADULT_SINGLE = re.compile(r"adult[\W_]*single", re.I)
# Young people's and children's fares share the words in some file names.
NOT_ADULT = re.compile(r"16-?20|child|student|young", re.I)


def local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def children(el, name):
    return [c for c in el.iter() if local(c.tag) == name]


def text(el, name):
    for c in el:
        if local(c.tag) == name:
            return (c.text or "").strip()
    return ""


def stage_key(zone_id: str) -> tuple:
    """`fs@38@boarding` -> ("fs@38", "boarding"). Unsuffixed ids board and alight."""
    for side in ("boarding", "alighting"):
        if zone_id.endswith("@" + side):
            return zone_id[: -len(side) - 1], side
    return zone_id, None


def parse_table(xml: bytes) -> list:
    """The fare triangle in one NeTEx file, as plain dicts, or [] if none."""
    root = ET.fromstring(xml)

    lines = []
    for ln in children(root, "Line"):
        op = ""
        for c in ln:
            if local(c.tag) == "OperatorRef":
                op = (c.get("ref") or "").replace("noc:", "")
        lines.append({"line": text(ln, "PublicCode"), "operator": op,
                      "description": text(ln, "Description") or text(ln, "Name")})
    if not lines:
        return []

    stop_names = {}
    for sp in children(root, "ScheduledStopPoint"):
        sid = (sp.get("id") or "").replace("atco:", "")
        if sid:
            name = text(sp, "Name")
            suffix = text(sp, "NameSuffix")
            stop_names[sid] = f"{name} {suffix}".strip() if suffix else name

    # Stages in the order published, which is the order along the route.
    stages, index = [], {}
    for fz in children(root, "FareZone"):
        key, side = stage_key(fz.get("id") or "")
        if not key:
            continue
        members = [(r.get("ref") or "").replace("atco:", "")
                   for r in fz.iter() if local(r.tag) == "ScheduledStopPointRef"]
        if key not in index:
            index[key] = len(stages)
            stages.append({"name": text(fz, "Name"), "board": [], "alight": []})
        st = stages[index[key]]
        for s in members:
            if side in (None, "boarding") and s not in st["board"]:
                st["board"].append(s)
            if side in (None, "alighting") and s not in st["alight"]:
                st["alight"].append(s)

    bands = {}
    for pg in children(root, "PriceGroup"):
        amount = next((a.text for a in pg.iter() if local(a.tag) == "Amount"), None)
        if amount:
            bands[pg.get("id")] = amount

    elements = {}
    for dme in children(root, "DistanceMatrixElement"):
        start = end = band = None
        for c in dme.iter():
            n = local(c.tag)
            if n == "StartTariffZoneRef":
                start = stage_key(c.get("ref") or "")[0]
            elif n == "EndTariffZoneRef":
                end = stage_key(c.get("ref") or "")[0]
            elif n == "PriceGroupRef":
                band = c.get("ref")
        elements[dme.get("id")] = (start, end, bands.get(band))
    for price in children(root, "DistanceMatrixElementPrice"):
        amount = next((a.text for a in price.iter() if local(a.tag) == "Amount"), None)
        ref = next((r.get("ref") for r in price.iter()
                    if local(r.tag) == "DistanceMatrixElementRef"), None)
        if ref in elements and amount:
            start, end, _ = elements[ref]
            elements[ref] = (start, end, amount)

    prices = []
    for start, end, amount in elements.values():
        if start in index and end in index and amount:
            pence = round(float(amount) * 100)
            if pence > 0:
                prices.append([index[start], index[end], pence])
    if not prices:
        return []

    # A file carries several validity periods (the frame, the tariff, the
    # product), some decades old. The fares took effect on the latest start
    # that has already come.
    today = date.today().isoformat()
    valid_from = max((d.text[:10] for d in root.iter()
                      if local(d.tag) == "FromDate" and d.text and d.text[:10] <= today),
                     default=None)
    used = {s for st in stages for s in st["board"] + st["alight"]}
    return [{**lines[0], "valid_from": valid_from, "stages": stages,
             "prices": sorted(prices),
             "stop_names": {s: stop_names[s] for s in sorted(used) if s in stop_names}}]


def build(zips: dict, area: set) -> dict:
    kept = {}
    counts = {}
    for noc, (dataset_id, data) in zips.items():
        z = zipfile.ZipFile(io.BytesIO(data))
        seen = 0
        for name in z.namelist():
            if (not name.lower().endswith(".xml") or not ADULT_SINGLE.search(name)
                    or NOT_ADULT.search(name)):
                continue
            seen += 1
            try:
                tables = parse_table(z.read(name))
            except ET.ParseError:
                continue
            for t in tables:
                stops = {s for st in t["stages"] for s in st["board"] + st["alight"]}
                if not stops & area:
                    continue
                t["operator"] = t["operator"] or noc
                t["dataset_id"] = dataset_id
                first, last = t["stages"][0]["name"], t["stages"][-1]["name"]
                t["direction"] = f"{first} to {last}"
                # The same route and direction published twice: keep the one
                # valid latest.
                key = (t["operator"], t["line"], first, last, len(t["stages"]))
                if key not in kept or (t["valid_from"] or "") > (kept[key]["valid_from"] or ""):
                    kept[key] = t
        counts[noc] = {"adult_single_files": seen}
    tables = sorted(kept.values(),
                    key=lambda t: (t["operator"], _line_order(t["line"]), t["direction"]))
    for noc, (dataset_id, _) in zips.items():
        counts[noc]["tables_kept"] = sum(1 for t in tables if t["dataset_id"] == dataset_id)
    return {"tables": tables, "counts": counts}


def _line_order(line: str):
    m = re.match(r"([A-Za-z]*)(\d+)(.*)", line or "")
    return (m.group(1), int(m.group(2)), m.group(3)) if m else (line or "", 0, "")


def fetch(dataset_id: int) -> bytes:
    req = urllib.request.Request(DOWNLOAD.format(id=dataset_id),
                                 headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=300) as resp:
        return resp.read()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--zip-dir", type=Path,
                    help="read <noc lowercase>.zip from here instead of downloading")
    ap.add_argument("--stops", type=Path, default=ROOT / "data" / "stops.json")
    ap.add_argument("--out", type=Path, default=ROOT / "data" / "fare_tables.json")
    args = ap.parse_args(argv)

    stops = json.loads(args.stops.read_text())["stops"]
    area = {s["atco_code"] for s in stops}
    area_services = {svc for s in stops for svc in s.get("services") or []}
    zips = {}
    for noc, dataset_id in DATASETS.items():
        if args.zip_dir:
            data = (args.zip_dir / f"{noc.lower()}.zip").read_bytes()
        else:
            print(f"Downloading {noc} fares (dataset {dataset_id})…", flush=True)
            data = fetch(dataset_id)
        zips[noc] = (dataset_id, data)

    result = build(zips, area)
    out = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "as_of": date.today().isoformat(),
        "method": ("Adult single fares from each operator's NeTEx fares data on the "
                   "Bus Open Data Service: for each route and direction, the fare "
                   "stages (groups of stops) and the price between every pair. Only "
                   "tables with a stop in the map area are kept; where a route and "
                   "direction is published more than once, the one valid latest."),
        "caveats": [
            "These are the operators' published on-bus prices. App, contactless "
            "and m-ticket fares can be lower, and are not published here.",
            "England's national fare cap applies on top: the site never quotes a "
            "single above the cap in data/ticket_zones.json.",
            "A fare applies between fare stages, not stops. A stop the operator "
            "has not placed in a stage has no published fare, and the site falls "
            "back to the cap for it.",
        ],
        "sources": [{"operator": noc, "dataset_id": d,
                     "source_url": PAGE.format(id=d)} for noc, (d, _) in zips.items()],
        "counts": result["counts"],
        # Routes calling in the map area that no operator publishes a fare
        # table for. The 700, the corridor's busiest route, was one when this
        # was written: Stagecoach publishes 43 Sussex routes but not it.
        "services_without_tables": sorted(
            area_services - {t["line"] for t in result["tables"]}, key=_line_order),
        "tables": result["tables"],
    }
    n = len(result["tables"])
    if not n:
        # A format change or an empty upload: keep last week's tables rather
        # than publish none.
        print(f"No fare tables found; {args.out} left as it was. {result['counts']}")
        return 1
    args.out.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")) + "\n")
    print(f"Wrote {n} fare tables to {args.out} "
          f"({args.out.stat().st_size // 1024} KB); {result['counts']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
