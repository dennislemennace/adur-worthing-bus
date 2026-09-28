#!/usr/bin/env python3
"""
scripts/add_disruption.py
=========================
Record a disruption from an operator's or council's own notice in
data/disruptions.json, so it shows on the boards, rows and Bus tab it affects.

Nobody in this area publishes to the BODS disruptions feed, so the notices that
matter (a road closed for weeks, stops not served) are copied in by hand. This
script keeps that routine and checks what it writes: every stop must exist,
every date must parse with its offset, and the source must be linked.

Find the stops a notice names (by name or street, with direction and routes):

    python scripts/add_disruption.py --find "Brunswick Place"

Add an entry:

    python scripts/add_disruption.py \\
      --id bhbc-western-road-hove-2026-09-28 \\
      --summary "Western Road closed between Holland Road and Montpelier Road" \\
      --publisher "Brighton & Hove Buses" \\
      --source-url https://www.buses.co.uk/service-updates \\
      --starts 2026-09-28T07:00+01:00 --ends 2026-11-06T23:59+00:00 \\
      --lines BHBC:1,BHBC:2,BHBC:5 \\
      --not-served 149000006101,149000007101 \\
      --diversion "Hove|All routes except 1X, 2 and 46|Montpelier Road;Davigdor Road;Holland Road|Palmeira Square" \\
      --description-file notice.txt --advice "Please allow extra time."

List what is recorded, or drop what has ended:

    python scripts/add_disruption.py --list
    python scripts/add_disruption.py --prune

The description should be the author's own words, pasted, not ours.
"""
import argparse
import json
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "disruptions.json"
STOPS = ROOT / "data" / "stops.json"
DETAILS = ROOT / "data" / "stop_details.json"

REQUIRED = ("id", "summary", "description", "publisher", "source_url",
            "checked_on", "starts", "ends")
ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{3,80}$")


def _stamp(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"{value!r} has no UTC offset; write +01:00 in summer, +00:00 in winter")
    return dt


def stop_index() -> dict:
    return {s["atco_code"]: s for s in json.loads(STOPS.read_text())["stops"]}


def validate(entry: dict, stops: dict) -> list:
    """Every problem with one entry, as sentences. Empty when it is sound."""
    problems = []
    for field in REQUIRED:
        if not str(entry.get(field) or "").strip():
            problems.append(f"'{field}' is missing")
    if entry.get("id") and not ID_RE.match(entry["id"]):
        problems.append(f"id {entry['id']!r} should be lower-case words and dashes")
    if entry.get("source_url") and not str(entry["source_url"]).startswith("https://"):
        problems.append("source_url must be the https:// page the notice came from")
    try:
        date.fromisoformat(entry.get("checked_on") or "")
    except ValueError:
        problems.append("checked_on must be YYYY-MM-DD")
    try:
        if _stamp(entry["ends"]) <= _stamp(entry["starts"]):
            problems.append("ends is not after starts")
    except (KeyError, TypeError, ValueError) as exc:
        problems.append(f"dates: {exc}")
    for line in entry.get("lines", []):
        if not (line.get("operator") and line.get("line")):
            problems.append(f"line {line!r} needs an operator code and a line name")
    for s in entry.get("stops_not_served", []):
        if s.get("atco") not in stops:
            problems.append(f"stop {s.get('atco')!r} is not in data/stops.json")
    for atco in entry.get("diversion_stops", []):
        if atco not in stops:
            problems.append(f"diversion stop {atco!r} is not in data/stops.json")
    for d in entry.get("diversions", []):
        if not d.get("via"):
            problems.append(f"diversion {d!r} needs 'via', the streets it takes")
    if not (entry.get("lines") or entry.get("stops_not_served")):
        problems.append("name the lines affected, the stops not served, or both")
    return problems


def load() -> dict:
    if DATA.exists():
        return json.loads(DATA.read_text(encoding="utf-8"))
    return {"_comment": "", "disruptions": []}


def save(doc: dict) -> None:
    DATA.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def find(query: str) -> None:
    """Stops whose name or NaPTAN street contains `query`, with enough to tell
    the two sides of the road apart."""
    q = query.lower()
    details = json.loads(DETAILS.read_text()) if DETAILS.exists() else {"fields": [], "stops": {}}
    fields = details["fields"]
    for atco, s in sorted(stop_index().items(), key=lambda kv: kv[1]["name"]):
        d = dict(zip(fields, details["stops"].get(atco, [])))
        if q in s["name"].lower() or q in (d.get("street") or "").lower():
            print(f"{atco}  {s['name']:<28} {d.get('street') or '':<22} "
                  f"{d.get('indicator') or '':<10} {d.get('bearing') or '':<3} "
                  f"towards {s.get('towards') or '?':<22} {', '.join(s.get('services', []))}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--find")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--prune", action="store_true")
    ap.add_argument("--id")
    ap.add_argument("--summary")
    ap.add_argument("--description")
    ap.add_argument("--description-file")
    ap.add_argument("--advice", default="")
    ap.add_argument("--reason", default="roadworks")
    ap.add_argument("--severity", default="normal")
    ap.add_argument("--publisher")
    ap.add_argument("--source-url")
    ap.add_argument("--checked-on", default=date.today().isoformat())
    ap.add_argument("--starts")
    ap.add_argument("--ends")
    ap.add_argument("--lines", default="", help="OPERATOR:LINE,... e.g. BHBC:1,SCSO:700")
    ap.add_argument("--not-served", default="", help="stop ATCO codes, comma-separated")
    ap.add_argument("--diversion-stops", default="", help="stop ATCO codes served on the diversion")
    ap.add_argument("--diversion", action="append", default=[],
                    help="towards|routes|street;street;...|rejoins (repeatable)")
    ap.add_argument("--notes", default="")
    args = ap.parse_args()

    if args.find:
        find(args.find)
        return

    doc = load()
    now = datetime.now(timezone.utc)
    if args.list:
        for e in doc["disruptions"]:
            state = ("ended" if _stamp(e["ends"]) < now
                     else "in force" if _stamp(e["starts"]) <= now else "upcoming")
            print(f"{e['id']}  [{state}]  {e['starts']} to {e['ends']}  {e['summary']}")
        return
    if args.prune:
        keep = [e for e in doc["disruptions"] if _stamp(e["ends"]) >= now]
        gone = len(doc["disruptions"]) - len(keep)
        doc["disruptions"] = keep
        save(doc)
        print(f"Removed {gone} ended disruption(s).")
        return

    description = args.description or (
        Path(args.description_file).read_text(encoding="utf-8").strip()
        if args.description_file else "")
    stops = stop_index()
    entry = {
        "id": args.id,
        "summary": args.summary,
        "description": " ".join(description.split()),
        "advice": args.advice,
        "reason": args.reason,
        "severity": args.severity,
        "starts": args.starts,
        "ends": args.ends,
        "publisher": args.publisher,
        "source_url": args.source_url,
        "checked_on": args.checked_on,
        "lines": [{"operator": op, "line": line}
                  for op, line in (x.split(":", 1) for x in args.lines.split(",") if x)],
        "stops_not_served": [{"atco": a, "as_named": stops.get(a, {}).get("name", "")}
                             for a in args.not_served.split(",") if a],
        "diversion_stops": [a for a in args.diversion_stops.split(",") if a],
        "diversions": [dict(zip(("towards", "routes", "via", "rejoins"),
                                (p[0], p[1], p[2].split(";"), p[3] if len(p) > 3 else "")))
                       for p in (d.split("|") for d in args.diversion)],
    }
    if args.notes:
        entry["notes"] = args.notes
    problems = validate(entry, stops)
    if any(e["id"] == entry["id"] for e in doc["disruptions"]):
        problems.append(f"id {entry['id']!r} is already recorded")
    if problems:
        sys.exit("Not written:\n  " + "\n  ".join(problems))
    doc["disruptions"].append(entry)
    save(doc)
    print(f"Added {entry['id']}: {len(entry['stops_not_served'])} stop(s) not served, "
          f"{len(entry['lines'])} line(s). Commit data/disruptions.json and push; the "
          f"API picks it up on redeploy.")


if __name__ == "__main__":
    main()
