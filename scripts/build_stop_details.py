#!/usr/bin/env python3
"""Write data/stop_details.json: where each stop actually is, from NaPTAN.

A name is not enough to find a stop. Two poles called "Grand Avenue" stand
either side of the road, a town centre has a Stop A to Stop P, and a visitor
does not know which way "towards Marina Cinema" is. NaPTAN, the national stop
register, carries what the flag and the street say:

* the indicator: "Stop D", "opp", "adj", "o/s", "W-bound";
* the street, and a landmark the indicator refers to ("opp The Cuthbert");
* the bearing buses travel in when they call (N, NE, ...).

Kept apart from stops.json on purpose: that file is built from the timetable
database in the weekly workflow, and this one only needs NaPTAN and the stop
list, so it can be refreshed without a timetable rebuild. The site loads it
when a stop is opened or searched, not on first paint.

    python scripts/build_stop_details.py [--stops data/stops.json]
                                         [--out data/stop_details.json]

Open Government Licence, like the BODS feeds.
"""

import argparse
import csv
import io
import json
import sys
import urllib.request
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# The same filtered NaPTAN query as scripts/build_timetable.py uses for
# localities: our two ATCO areas, about 2 MB against 60 MB for the country.
NAPTAN_URL = ("https://naptan.api.dft.gov.uk/v1/access-nodes"
              "?dataFormat=csv&atcoAreaCodes=149,440")

FIELDS = ["indicator", "street", "landmark", "bearing"]

# Below this, NaPTAN answered with something other than our stops, and
# publishing it would quietly blank the detail for most of the map.
MIN_COVERAGE = 0.9


def parse_naptan(csv_text: str) -> dict:
    """`{ATCO code: [indicator, street, landmark, bearing]}` from NaPTAN CSV."""
    out = {}
    for row in csv.DictReader(io.StringIO(csv_text)):
        atco = (row.get("ATCOCode") or "").strip()
        if not atco:
            continue
        out[atco] = [(row.get(k) or "").strip()
                     for k in ("Indicator", "Street", "Landmark", "Bearing")]
    return out


def build(stops: list, naptan: dict) -> dict:
    details = {s["atco_code"]: naptan[s["atco_code"]]
               for s in stops if s.get("atco_code") in naptan}
    return {
        "source": "NaPTAN, Department for Transport (Open Government Licence)",
        "source_url": NAPTAN_URL,
        "checked_on": date.today().isoformat(),
        "fields": FIELDS,
        "count": len(details),
        "stops": details,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--stops", default=str(ROOT / "data" / "stops.json"))
    ap.add_argument("--out", default=str(ROOT / "data" / "stop_details.json"))
    ap.add_argument("--naptan-csv", help="read NaPTAN from a file instead of the API")
    args = ap.parse_args()

    stops = json.loads(Path(args.stops).read_text())["stops"]
    if args.naptan_csv:
        text = Path(args.naptan_csv).read_text(encoding="utf-8-sig")
    else:
        req = urllib.request.Request(NAPTAN_URL, headers={"User-Agent": "adur-worthing-bus build"})
        with urllib.request.urlopen(req, timeout=90) as resp:
            text = resp.read().decode("utf-8-sig", errors="replace")

    payload = build(stops, parse_naptan(text))
    if payload["count"] < MIN_COVERAGE * len(stops):
        print(f"ERROR: NaPTAN described {payload['count']} of {len(stops)} stops; "
              "refusing to publish", file=sys.stderr)
        return 1
    out = Path(args.out)
    out.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {out}: {payload['count']} of {len(stops)} stops, {out.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
