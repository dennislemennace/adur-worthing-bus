"""Write data/fleet.json: each bus's livery, model and deck, so the map can
draw every bus in its own colours (asked for 2 November 2026; shown only
with ?preview=1 until it has been looked over).

bustimes.org keeps a community-maintained list of every operator's vehicles,
with each one's livery, branding, model and garage, and serves it as JSON
with no key. It is used here with bustimes.org's permission, and credited on
the About page. One request per operator, once a week in the timetable
workflow (LIMITS.md); the map never calls bustimes.org itself.

A bus is matched to the live feed the way each operator names its buses
there: Brighton & Hove, Metrobus and Stagecoach by fleet number (Stagecoach's
as "SCSO-10941"), Compass by registration ("YW25_DFC"). The map reduces both
to capitals and digits before looking them up, so the keys here are the same.

Which icon a livery gets is LIVERY_ICONS below: a drawn livery in
icons/liveries/ (scripts/livery_designs.py), an existing master, or none, in
which case the map falls back to the route's or the operator's icon as
before. Brighton & Hove's red and cream gets none on purpose: its route icons
are red and cream already, in each route's colour, which bustimes.org's list
does not record.

**What it can't tell you.** The list is kept by enthusiasts and is as current
as their last edit: a bus repainted this week may still show its old colours.

Usage:
    python scripts/build_fleet.py [--json-dir DIR] [--out data/fleet.json]
"""

import argparse
import json
import re
import sys
import time
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
API = "https://bustimes.org/api/vehicles/"
SITE = "https://bustimes.org/"
USER_AGENT = "worthingbrightonbus.co.uk fleet list (privacy@worthingbrightonbus.co.uk)"

# The operators whose buses reach the map, and how the live feed names them.
OPERATORS = {"BHBC": "fleet", "METR": "fleet", "SCSO": "fleet", "COMT": "reg"}

# bustimes.org livery id -> icon by deck ("dd", "sd"), relative to icons/.
# A deck missing from an entry has no icon of its own.
LIVERY_ICONS = {
    # Brighton & Hove
    3057: {"dd": "liveries/bh-teal-dd.png", "sd": "liveries/bh-teal-sd.png"},
    1200: {"dd": "liveries/bh-coaster-local-dd.png"},
    3243: {"dd": "BHBC-12.png"},                       # Coaster
    3780: {"dd": "BHBC-28.png"},                       # Regency
    236: {"dd": "BHBC-CSS.png"},                       # City Sightseeing
    365: {"dd": "liveries/bh-live-breathe-dd.png"},
    824: {"dd": "liveries/bh-above-beyond-dd.png"},
    3759: {"dd": "liveries/bh-beach-dd.png"},
    3844: {"dd": "liveries/bh-albion-dd.png"},
    3899: {"dd": "liveries/bh-albion-foundation-dd.png"},
    3811: {"dd": "liveries/bh-pride-dd.png"},
    3807: {"dd": "liveries/bh-spirit-of-sussex-dd.png", "sd": "liveries/bh-spirit-of-sussex-sd.png"},
    3532: {"dd": "liveries/bh-red-dd.png", "sd": "liveries/mb-red-sd.png"},
    # Metrobus
    188: {"dd": "liveries/mb-dd.png", "sd": "liveries/mb-sd.png"},
    3789: {"dd": "liveries/mb-new-dd.png", "sd": "liveries/mb-new-sd.png"},
    3481: {"dd": "liveries/mb-hydrogen-dd.png", "sd": "liveries/mb-hydrogen-sd.png"},
    2924: {"sd": "liveries/mb-fastway-blue-sd.png"},
    187: {"sd": "liveries/mb-fastway-silver-sd.png"},
    4026: {"sd": "liveries/mb-hiring-sd.png"},
    3105: {"sd": "liveries/mb-classic-sd.png"},
    882: {"sd": "liveries/mb-southdown-sd.png"},
    3905: {"dd": "liveries/mb-st-catherines-dd.png"},
    22: {"sd": "liveries/mb-morebus-sd.png"},
    1591: {"sd": "liveries/mb-carousel-sd.png"},
    3407: {"sd": "liveries/mb-archway-sd.png"},
    782: {"sd": "liveries/mb-warrington-sd.png"},
    # Stagecoach
    3459: {"dd": "liveries/sc-wegotyou-dd.png", "sd": "SCSO-17.png"},
    600: {"dd": "liveries/sc-local-dd.png", "sd": "liveries/sc-local-sd.png"},
    8: {"dd": "liveries/sc-beachball-dd.png", "sd": "liveries/sc-beachball-sd.png"},
    2996: {"dd": "liveries/sc-coastliner-dd.png"},
    43: {"dd": "SCSO.png"},                            # Coastliner 700
    3333: {"dd": "liveries/sc-think-brain-health-dd.png"},
    2135: {"dd": "liveries/sc-pride-dd.png"},
    3850: {"dd": "liveries/sc-cant-miss-us-dd.png"},
    # Compass: one livery under three ids
    2959: {"dd": "liveries/compass-dd.png", "sd": "liveries/compass-sd.png"},
    16: {"dd": "liveries/compass-dd.png", "sd": "liveries/compass-sd.png"},
    3812: {"dd": "liveries/compass-dd.png", "sd": "liveries/compass-sd.png"},
    # Plain white, any operator
    164: {"dd": "liveries/white-dd.png", "sd": "liveries/white-sd.png"},
}
# Brighton & Hove's red and cream (id 36) falls through to the route icons;
# on a Metrobus, which has no red route icons, it is drawn as itself.
OPERATOR_LIVERY_ICONS = {("METR", 36): {"dd": "BHBC.png"}}
# Branding that changes the picture: the 1X and 3X teal buses, and buses whose
# livery bustimes.org leaves empty because they are all-over adverts.
BRANDING_ICONS = {
    (3057, "1X"): {"dd": "liveries/bh-teal-1x-dd.png"},
    (3057, "3X"): {"dd": "liveries/bh-teal-3x-dd.png"},
    (164, "GET BUS(Y) Get Fitter"): {"dd": "liveries/bh-getbusy-fitter-dd.png"},
    (None, "Martin Fisher Foundation"): {"dd": "liveries/bh-martin-fisher-dd.png"},
    (None, "Brighton Housing Trust"): {"dd": "liveries/bh-housing-trust-dd.png"},
    (None, "Breeze up to the Downs"): {"dd": "liveries/bh-breeze-downs-dd.png"},
    (None, "GET BUS(Y) Arts"): {"dd": "liveries/bh-getbusy-arts-dd.png"},
    (None, "Amex"): {"dd": "liveries/bh-amex-dd.png"},
    (None, "Fun Brighton"): {"dd": "liveries/bh-fun-brighton-dd.png"},
    (None, "GET BUS(Y) Beach"): {"dd": "liveries/mb-getbusy-beach-dd.png"},
    (None, "Pride in our Community"): {"dd": "liveries/mb-pride-dd.png"},
}


def key(text) -> str:
    """Capitals and digits only, as the map reduces a vehicle ref."""
    return re.sub(r"[^A-Z0-9]", "", str(text or "").upper())


def deck(vehicle) -> str:
    t = vehicle.get("vehicle_type") or {}
    if t.get("double_decker"):
        return "dd"
    return "coach" if t.get("coach") else "sd"


def icon_for(operator, vehicle) -> str:
    livery = vehicle.get("livery") or {}
    lid, branding, d = livery.get("id"), (vehicle.get("branding") or "").strip(), deck(vehicle)
    for table, k in ((BRANDING_ICONS, (lid, branding)), (OPERATOR_LIVERY_ICONS, (operator, lid)),
                     (LIVERY_ICONS, lid)):
        if k in table:
            return table[k].get(d, "")
    return ""


def build(lists):
    """{operator: [bustimes.org vehicle, ...]} -> the operators block of
    fleet.json. Spare ticket machines, which bustimes.org lists as vehicles
    with no type or registration, are left out."""
    out = {}
    for operator, vehicles in lists.items():
        by = OPERATORS[operator]
        rows = {}
        for v in vehicles:
            if v.get("withdrawn") or "ticket machine" in (v.get("notes") or "").lower():
                continue
            ident = key(v.get("fleet_code") or v.get("fleet_number")) if by == "fleet" else key(v.get("reg"))
            if not ident:
                continue
            livery = v.get("livery") or {}
            row = {"l": livery.get("name") or (v.get("branding") or "").strip(),
                   "t": (v.get("vehicle_type") or {}).get("name") or "",
                   "d": deck(v)}
            icon = icon_for(operator, v)
            if icon:
                row["i"] = icon
            row["r" if by == "fleet" else "f"] = v.get("reg") if by == "fleet" else v.get("fleet_code")
            rows[ident] = {k: val for k, val in row.items() if val}
        out[operator] = {"by": by, "vehicles": dict(sorted(rows.items()))}
    return out


def fetch(operator, pause=1.0):
    vehicles, url = [], f"{API}?operator={operator}&withdrawn=False&limit=1000"
    while url:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=60) as res:
            page = json.load(res)
        vehicles += page.get("results") or []
        url = page.get("next")
        time.sleep(pause)
    return vehicles


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--json-dir", type=Path, help="read OPERATOR.json files saved from the API here")
    ap.add_argument("--out", type=Path, default=ROOT / "data" / "fleet.json")
    args = ap.parse_args(argv)

    lists = {}
    for op in OPERATORS:
        if args.json_dir:
            lists[op] = json.loads((args.json_dir / f"{op}.json").read_text())["results"]
        else:
            lists[op] = fetch(op)
    operators = build(lists)
    total = sum(len(o["vehicles"]) for o in operators.values())
    if total < 200:
        print(f"Only {total} vehicles; {args.out} left as it was.")
        return 1
    missing = sorted({r["i"] for o in operators.values() for r in o["vehicles"].values()
                      if r.get("i") and not (ROOT / "icons" / r["i"]).exists()})
    if missing:
        print("Icons named but not built: " + ", ".join(missing), file=sys.stderr)
        return 1
    out = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "checked_on": date.today().isoformat(),
        "source_url": SITE,
        "api": API,
        "permission": "Used with bustimes.org's permission (2 November 2026).",
        "method": ("bustimes.org's vehicle list for Brighton & Hove, Metrobus, Stagecoach South "
                   "and Compass, withdrawn vehicles left out: each bus's livery, branding, model "
                   "and deck, and the icon drawn for that livery (scripts/build_fleet.py)."),
        "caveats": [
            "Kept by enthusiasts, so as current as their last edit: a bus repainted this "
            "week may still show its old colours.",
            "Brighton & Hove's red and cream buses show their route's icon, as before.",
        ],
        "operators": operators,
    }
    args.out.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")) + "\n")
    with_icon = sum(1 for o in operators.values() for r in o["vehicles"].values() if r.get("i"))
    print(f"Wrote {total} vehicles ({with_icon} with a livery icon) to {args.out} "
          f"({args.out.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
