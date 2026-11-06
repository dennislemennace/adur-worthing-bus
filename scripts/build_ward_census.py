#!/usr/bin/env python3
"""Write data/ward_census.json: who lives in each ward, from Census 2021.

Route view's Areas card sets a ward's buses beside the people who depend on
them: households with no car or van, and residents aged 65 and over (asked for
on 6 November 2026, Areas phase B). A ward with few buses and many households
without a car is the case the site exists to make.

Census 2021 is published on older ward boundaries, and Brighton & Hove's wards
were redrawn in 2023, so the counts are taken for output areas (the smallest
Census geography) and summed into the May 2025 wards the rest of the card uses,
through the ONS's best-fit lookup of 2021 output areas to 2025 wards. Best fit
means each output area goes wholly into one ward.

Sources, both Open Government Licence:
* Nomis, Census 2021 tables TS045 (car or van availability, NM_2063_1) and
  TS007A (age by five-year bands, NM_2020_1), by 2021 output area;
* ONS Open Geography, OA21_WD25_LAD25_EW_LU_v3.

A one-off: Census figures do not change. Run again only if the wards do.

    python scripts/build_ward_census.py [--out data/ward_census.json]
        [--lookup-json f] [--cars-csv f] [--age-csv f]   (saved answers)
"""

import argparse
import csv
import io
import json
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DISTRICTS = {"E07000223": "Adur", "E07000229": "Worthing", "E06000043": "Brighton and Hove"}
LOOKUP_URL = ("https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
              "OA21_WD25_LAD25_EW_LU_v3/FeatureServer/0/query")
NOMIS = "https://www.nomisweb.co.uk/api/v01/dataset/{}.data.csv"
CARS, AGE = "NM_2063_1", "NM_2020_1"
UA = {"User-Agent": "adur-worthing-bus build (ward census, one-off)"}
OLDER = {"Aged 65 to 69 years", "Aged 70 to 74 years", "Aged 75 to 79 years",
         "Aged 80 to 84 years", "Aged 85 years and over"}


def get(url: str) -> str:
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=120) as resp:
        return resp.read().decode("utf-8-sig")


def fetch_lookup() -> dict:
    """`{OA21 code: WD25 code}` for the three districts, a page at a time."""
    out, offset = {}, 0
    where = "LAD25CD IN ({})".format(", ".join(f"'{c}'" for c in DISTRICTS))
    while True:
        q = urllib.parse.urlencode({"where": where, "outFields": "OA21CD,WD25CD", "returnGeometry": "false",
                                    "resultOffset": offset, "resultRecordCount": 1000, "f": "json"})
        page = json.loads(get(f"{LOOKUP_URL}?{q}"))
        rows = [f["attributes"] for f in page.get("features") or []]
        out.update({r["OA21CD"]: r["WD25CD"] for r in rows})
        if not page.get("exceededTransferLimit") and len(rows) < 1000:
            return out
        offset += len(rows)


# Nomis answers at most this many rows a request, silently: the age table for
# the three districts is 28,842 rows, and the first build lost 51,000 people
# to it before this paged.
NOMIS_PAGE = 25000


def fetch_table(dataset: str, column: str) -> str:
    """The whole table as one CSV, a page at a time."""
    geography = ",".join(f"{code}TYPE150" for code in DISTRICTS)
    header, rows, offset = None, [], 0
    while True:
        q = urllib.parse.urlencode({"geography": geography, "measures": 20100, "recordoffset": offset,
                                    "select": f"geography_code,{column}_name,obs_value"})
        lines = get(f"{NOMIS.format(dataset)}?{q}").splitlines()
        if not lines:
            break
        header = header or lines[0]
        page = [line for line in lines[1:] if line.strip()]
        rows.extend(page)
        if len(page) < NOMIS_PAGE:
            break
        offset += len(page)
    return "\n".join([header or ""] + rows) + "\n"


def build(lookup: dict, cars_csv: str, age_csv: str, generated_on: str) -> dict:
    wards = defaultdict(lambda: {"households": 0, "households_no_car": 0, "residents": 0, "residents_65_plus": 0})
    for row in csv.DictReader(io.StringIO(cars_csv)):
        ward = lookup.get(row["GEOGRAPHY_CODE"])
        if not ward:
            continue
        name, n = row["C2021_CARS_5_NAME"], int(row["OBS_VALUE"])
        if name.startswith("Total"):
            wards[ward]["households"] += n
        elif name.startswith("No cars"):
            wards[ward]["households_no_car"] += n
    for row in csv.DictReader(io.StringIO(age_csv)):
        ward = lookup.get(row["GEOGRAPHY_CODE"])
        if not ward:
            continue
        name, n = row["C2021_AGE_19_NAME"], int(row["OBS_VALUE"])
        if name == "Total":
            wards[ward]["residents"] += n
        elif name in OLDER:
            wards[ward]["residents_65_plus"] += n
    return {
        "_comment": ("Census 2021 by May 2025 ward, for Route view's Areas card: households with no car or van, "
                     "and residents aged 65 and over. Output areas summed by the ONS best-fit lookup. Built once by "
                     "scripts/build_ward_census.py."),
        "generated_on": generated_on,
        "census_day": "2021-03-21",
        "sources": [
            {"name": "Census 2021, TS045 Car or van availability (Nomis NM_2063_1)",
             "url": "https://www.nomisweb.co.uk/datasets/c2021ts045"},
            {"name": "Census 2021, TS007A Age by five-year age bands (Nomis NM_2020_1)",
             "url": "https://www.nomisweb.co.uk/datasets/c2021ts007a"},
            {"name": "ONS, Output Area (2021) to Ward (May 2025) best-fit lookup",
             "url": "https://geoportal.statistics.gov.uk/"},
        ],
        "licence": "Open Government Licence v3.0",
        "attribution": ("Office for National Statistics, Census 2021. Source: ONS licensed under the "
                        "Open Government Licence v3.0."),
        "caveats": ["Each output area is counted wholly in one ward (best fit), so a ward's figures can differ "
                    "slightly from any published for it directly."],
        "wards": dict(sorted(wards.items())),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", default=str(ROOT / "data" / "ward_census.json"))
    ap.add_argument("--lookup-json")
    ap.add_argument("--cars-csv")
    ap.add_argument("--age-csv")
    args = ap.parse_args()
    lookup = json.loads(Path(args.lookup_json).read_text()) if args.lookup_json else fetch_lookup()
    cars = Path(args.cars_csv).read_text() if args.cars_csv else fetch_table(CARS, "c2021_cars_5")
    age = Path(args.age_csv).read_text() if args.age_csv else fetch_table(AGE, "c2021_age_19")
    result = build(lookup, cars, age, date.today().isoformat())
    # Every output area in the lookup must have both tables, or a page was lost.
    for name, text in (("cars", cars), ("age", age)):
        seen = {row["GEOGRAPHY_CODE"] for row in csv.DictReader(io.StringIO(text))}
        if seen != set(lookup):
            print(f"ERROR: the {name} table covers {len(seen)} of {len(lookup)} output areas; refusing to publish",
                  file=sys.stderr)
            return 1
    if len(result["wards"]) < 40:
        print(f"ERROR: only {len(result['wards'])} wards; refusing to publish", file=sys.stderr)
        return 1
    Path(args.out).write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {args.out}: {len(result['wards'])} wards from {len(lookup)} output areas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
