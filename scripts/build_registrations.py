"""Write data/registrations.json: each local route's registration with the
Traffic Commissioner, whether a council pays towards it, and how it has changed.

Every local bus service in England outside London must be registered with the
Traffic Commissioner, and every change to it registered again as a
"variation", with 70 days' notice unless short notice is granted. DVSA
publishes the register for each traffic area as CSV, updated daily, with no
key. Sussex is traffic area K (London and the South East).

Two files are read:

* `Bus_RegisteredOnly_K.csv`: every registration in force, with where it
  runs, its type, and whether it is subsidised, and by whom.
* `Bus_Variation_K.csv`: every variation ever registered, which is the
  route's history of timetable changes, withdrawals of sections and the like.

Only the three licences running the area's buses are kept (Brighton & Hove,
which includes Metrobus; Stagecoach South; Compass), and only the service
numbers that call at our stops (data/stops.json). A variant with no
registration of its own (5A, 12X) is matched to its base number (5, 12) and
says so.

**What it can't tell you.** Subsidy is as the operator declared it when it
last registered, and "in part" does not say which journeys or how much. The
register is the legal record, not the timetable: a registration can outlive
the service on the road by weeks.

Usage:
    python scripts/build_registrations.py [--csv-dir DIR] [--out data/registrations.json]
"""

import argparse
import csv
import io
import json
import re
import sys
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

BASE = "https://content.mgmt.dvsacloud.uk/olcs.app.prod.dvsa.aws/data-gov-uk-export/"
REGISTERED = "Bus_RegisteredOnly_K.csv"
VARIATIONS = "Bus_Variation_K.csv"
# Where the files are listed, for a reader following the link.
DATASET_PAGE = "https://www.data.gov.uk/dataset/bus-registrations-data-set"

# Operator licence -> the operator code the rest of the site uses.
LICENCES = {"PK0001213": "BHBC", "PK0002571": "SCSO", "PK0003556": "COMT"}

# How far back the history goes: long enough to show a route's recent
# changes, short enough to keep the file small.
HISTORY_FROM = "2019-01-01"

COUNCILS = (
    ("west sussex", "West Sussex County Council"),
    ("east sussex", "East Sussex County Council"),
    ("brighton", "Brighton & Hove City Council"),
    ("surrey", "Surrey County Council"),
    ("hampshire", "Hampshire County Council"),
)


def iso(d: str):
    """DVSA writes dd/mm/yy. Two-digit years before 70 are this century."""
    try:
        return datetime.strptime((d or "").strip(), "%d/%m/%y").date().isoformat()
    except ValueError:
        return None


def councils(details: str) -> list:
    text = (details or "").lower()
    return [name for key, name in COUNCILS if key in text]


def service_numbers(field: str) -> list:
    """ "22, 22A" / "21 / 21A" -> ["22", "22A"]."""
    return [p.strip().upper() for p in re.split(r"[/,&]| and ", field or "") if p.strip()]


def published_numbers(pub_text: str) -> list:
    """The numbers a registration covers, as its notice gives them.

    Brighton & Hove register several routes together and name only one in the
    service number field: PK0001213/13 is "1", and its notice reads "given
    service number 1 / 1X / 6 / 71 / 73 / N1 / 71A".
    """
    m = re.search(r"service numbers? (.+?) effective", pub_text or "", re.I)
    return service_numbers(m.group(1)) if m else []


def base_number(svc: str) -> str:
    """"5A" -> "5", "N12" -> "12", "12X" -> "12"."""
    m = re.match(r"N?(\d+)[A-Z]*$", svc)
    return m.group(1) if m else svc


# Words in a locality's name that say nothing about where it is.
GENERIC = {"east", "west", "north", "south", "upper", "lower", "village", "beach",
           "valley", "estate", "high", "town", "green", "park"}


def place_words(localities) -> set:
    """"Shoreham-by-Sea" -> {"shoreham"}, "East Worthing" -> {"worthing"}."""
    words = set()
    for name in localities:
        for w in re.split(r"[\s\-/,]+", (name or "").lower()):
            if len(w) >= 4 and w not in GENERIC and w not in {"by", "sea"}:
                words.add(w)
    return words


# Towns these licences also serve, well away from this map.
ELSEWHERE = {"guildford", "woking", "farnham", "godalming", "knaphill", "rushmoor",
             "crawley", "horley", "horsham", "haywards", "burgess", "grinstead",
             "eastbourne", "hailsham", "polegate", "chichester", "midhurst",
             "bognor", "petersfield", "haslemere", "redhill", "reigate",
             "pease", "dorking", "epsom", "tunbridge", "uckfield", "heathfield",
             "pulborough", "alford", "alfold", "billingshurst"}


def runs_here(r: dict, places: set) -> bool:
    """Is this registration for a route on this map? A licence covers more than
    it: Stagecoach South's 1 and 2 also run in Guildford, and Brighton & Hove's
    licence carries Metrobus's Crawley routes. Naming one of our places settles
    it; naming only a town elsewhere rules it out; a school or works route named
    by its streets ("Coombe Road to Cardinal Newman School") is kept."""
    text = " ".join((r.get(k) or "") for k in ("start_point", "finish_point", "via")).lower()
    return names_ours(r, places) or not _names(text, ELSEWHERE)


def _names(text: str, words) -> bool:
    return any(re.search(rf"\b{re.escape(w)}\b", text) for w in words)


def names_ours(r: dict, places: set) -> bool:
    text = " ".join((r.get(k) or "") for k in ("start_point", "finish_point", "via")).lower()
    return _names(text, places)


def read_csv(data: bytes) -> list:
    return list(csv.DictReader(io.StringIO(data.decode("utf-8", errors="replace"))))


def build(registered: list, variations: list, area_services: set, places: set) -> dict:
    regs = {}
    for r in registered:
        if (r.get("Lic_No") in LICENCES and r.get("Registration Status") == "Registered"
                and runs_here(r, places)):
            regs[r["Reg_No"]] = r    # a licence with two trading names lists each twice

    by_number = {}
    for r in regs.values():
        for n in dict.fromkeys(service_numbers(r["Service Number"])
                               + published_numbers(r.get("Pub_Text"))):
            by_number.setdefault((LICENCES[r["Lic_No"]], n), []).append(r)

    history = {}
    for v in variations:
        if v.get("Reg_No") not in regs:
            continue
        eff = iso(v.get("effective_date"))
        if not eff or eff < HISTORY_FROM:
            continue
        history.setdefault(v["Reg_No"], []).append({
            "variation": int(v["Variation Number"]) if (v.get("Variation Number") or "").isdigit() else None,
            "effective": eff,
            "received": iso(v.get("received_date")),
            "change": (v.get("Service_Type_Other_Details") or "").strip(),
            "short_notice": (v.get("Short Notice") or "").strip().lower() == "yes",
        })

    services, unmatched = [], []
    for svc in sorted(area_services, key=_order):
        found = False
        for noc in LICENCES.values():
            rows = by_number.get((noc, svc.upper()))
            via_base = False
            if not rows and base_number(svc.upper()) != svc.upper():
                rows = by_number.get((noc, base_number(svc.upper())))
                via_base = bool(rows)
            # One number, two registrations: a Brighton route and a school
            # run in Crawley. Where some name a place on this map, only those.
            if rows and any(names_ours(r, places) for r in rows):
                rows = [r for r in rows if names_ours(r, places)]
            for r in rows or []:
                found = True
                changes = sorted(history.get(r["Reg_No"], []),
                                 key=lambda c: (c["effective"], c["variation"] or 0), reverse=True)
                # One row per variation, whatever the file repeats.
                seen, unique = set(), []
                for c in changes:
                    if c["variation"] in seen:
                        continue
                    seen.add(c["variation"])
                    unique.append(c)
                services.append({
                    "operator": noc,
                    "service": svc,
                    # Said when the route is registered under another number,
                    # its base (5A under 5) or its bundle (6 under 1).
                    "registered_as": (r["Service Number"].strip()
                                      if via_base or svc.upper() not in service_numbers(r["Service Number"])
                                      else None),
                    "reg_no": r["Reg_No"],
                    "start": (r.get("start_point") or "").strip(),
                    "finish": (r.get("finish_point") or "").strip(),
                    "via": (r.get("via") or "").strip(),
                    "service_type": (r.get("Service_Type_Description") or "").strip(),
                    "subsidy": (r.get("Subsidies_Description") or "").strip() or None,
                    "subsidised_by": councils(r.get("Subsidies_Details")),
                    "in_force_from": iso(r.get("effective_date")),
                    "last_change": (r.get("Service_Type_Other_Details") or "").strip() or None,
                    "changes": unique,
                })
        if not found:
            unmatched.append(svc)
    return {"services": services, "unmatched": unmatched}


def _order(svc: str):
    m = re.match(r"([A-Za-z]*)(\d+)(.*)", svc or "")
    return (m.group(1), int(m.group(2)), m.group(3)) if m else (svc or "", 0, "")


def fetch(name: str) -> bytes:
    with urllib.request.urlopen(BASE + name, timeout=300) as resp:
        return resp.read()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--csv-dir", type=Path, help="read the two CSVs from here")
    ap.add_argument("--stops", type=Path, default=ROOT / "data" / "stops.json")
    ap.add_argument("--out", type=Path, default=ROOT / "data" / "registrations.json")
    args = ap.parse_args(argv)

    stops = json.loads(args.stops.read_text())["stops"]
    area_services = {s for st in stops for s in st.get("services") or []}
    places = place_words({st.get("locality") for st in stops})
    raw = {}
    for name in (REGISTERED, VARIATIONS):
        raw[name] = ((args.csv_dir / name).read_bytes() if args.csv_dir else fetch(name))

    result = build(read_csv(raw[REGISTERED]), read_csv(raw[VARIATIONS]), area_services, places)
    if not result["services"]:
        print(f"No registrations matched; {args.out} left as it was.")
        return 1
    out = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "as_of": date.today().isoformat(),
        "source_url": DATASET_PAGE,
        "files": [BASE + REGISTERED, BASE + VARIATIONS],
        "method": ("The Traffic Commissioner's register of local bus services for "
                   "traffic area K, as DVSA publishes it: registrations in force "
                   "for the Brighton & Hove, Stagecoach South and Compass licences "
                   "whose service number calls at a stop on this map and whose start, "
                   "finish or route names a place on it, with their "
                   f"variations effective since {HISTORY_FROM}."),
        "caveats": [
            "Subsidy is as the operator declared it at registration. \"In part\" "
            "does not say which journeys, or how much a council pays.",
            "The register is the legal record, not the timetable, and can lag "
            "what runs on the road by some weeks.",
            "A variant with no registration of its own (5A, 12X) is shown with "
            "its base number's registration, and says so.",
        ],
        "unmatched": result["unmatched"],
        "services": result["services"],
    }
    args.out.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")) + "\n")
    print(f"Wrote {len(result['services'])} registrations to {args.out} "
          f"({args.out.stat().st_size // 1024} KB); unmatched {result['unmatched']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
