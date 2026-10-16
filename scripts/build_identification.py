"""How each operator's buses were matched to their journeys, over the window.

Every figure on the site rests on knowing which timetabled journey a bus was
running. There are three ways we know it, from strongest to weakest:

- **journey ID**: the operator's live feed named a journey our timetable holds;
- **start time**: the feed named the line, first stop and scheduled start, and
  exactly one journey in our timetable has all three (method 7, for operators
  whose live journey IDs our timetable does not hold: Stagecoach South since
  7 October 2026, because BODS leaves it out of its own GTFS);
- **position**: neither, so the journey was worked out from where the bus was
  and when (inferred). This can confuse a very late bus with the next one.

This counts journeys, not stop timings, per operator and per service, against
the timetabled journeys running while the recorder was running. Counts only:
the site divides, with the denominators beside it.

    python scripts/build_identification.py --observations inputs/window --out identification.json
"""

import argparse
import glob
import gzip
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from observation_contract import digest_file                      # noqa: E402

SCHEMA_VERSION = 1
# Raise whenever a change would move a published number.
METHOD_VERSION = 1

# One operator under two codes: Stagecoach's Chichester depot and Compass's
# second licence. The site names both the same (OPERATOR_NAMES in app.js).
FAMILIES = {"SCSC": "SCSO", "CMPA": "COMT"}
WAYS = ("journey_id", "start", "position")

METHOD = (
    "Journeys are counted once per recorded day: a timetabled journey of one service day is one journey "
    "however many of its calls were timed. A journey counts as tracked when at least one of its calls was "
    "measured rather than interpolated. It is identified by journey ID when any of its timings was declared "
    "by a journey ID our timetable holds; otherwise by start time when any was declared by the feed's line, "
    "first stop and scheduled start; otherwise by position. Timetabled journeys are those running while the "
    "recorder was running (coverage.cohorts, scheduled_in_recording_span)."
)

CAVEATS = [
    "A journey running across midnight belongs to two recorded days and can be counted on both, as both a "
    "timetabled and a tracked journey.",
    "Identified by start time rests on our timetable matching the operator's to the second. If an operator "
    "retimes a journey without publishing the new timetable, its bus falls back to position or is not "
    "matched at all; it is never declared to the wrong journey.",
    "Identified by position can confuse a bus running more than half a headway late with the next journey, "
    "so very late running can be understated where it is common.",
    "A timetabled journey with no tracked bus may have run without reporting its position. It is missing "
    "evidence, not a cancellation.",
]


def operator_of(code):
    return FAMILIES.get(code, code or "?")


def way_of(rows):
    """How one journey was identified, from all its timings."""
    if any(r.get("match") == "declared" and r.get("declared_by") != "start" for r in rows):
        return "journey_id"
    if any(r.get("match") == "declared" and r.get("declared_by") == "start" for r in rows):
        return "start"
    return "position"


def _empty():
    return {"scheduled": 0, "tracked": 0, "by": dict.fromkeys(WAYS, 0)}


def count_day(doc):
    """`{(operator, service): counts}` for one recorded day's observations file."""
    journeys = defaultdict(list)
    for row in doc.get("observations") or []:
        journeys[(row.get("day"), row.get("trip_id"))].append(row)
    out = defaultdict(_empty)
    for rows in journeys.values():
        if all(r.get("estimated") for r in rows):
            continue
        key = (operator_of(rows[0].get("operator")), str(rows[0].get("service") or "?"))
        out[key]["tracked"] += 1
        out[key]["by"][way_of(rows)] += 1
    for cohort in (doc.get("coverage") or {}).get("cohorts") or []:
        key = (operator_of(cohort.get("operator")), str(cohort.get("service") or "?"))
        out[key]["scheduled"] += int(cohort.get("scheduled_in_recording_span") or 0)
    return out


def _add(into, counts):
    into["scheduled"] += counts["scheduled"]
    into["tracked"] += counts["tracked"]
    for way in WAYS:
        into["by"][way] += counts["by"][way]


def build(days, as_of=None):
    """`days`: `[(day, sha256, {(operator, service): counts})]`, one per recorded day."""
    operators, services = defaultdict(_empty), defaultdict(_empty)
    by_day = defaultdict(dict)
    for day, _, counts in sorted(days, key=lambda d: d[0]):
        daily = defaultdict(_empty)
        for (op, svc), c in counts.items():
            _add(operators[op], c)
            _add(services[f"{svc}|{op}"], c)
            _add(daily[op], c)
        for op, c in daily.items():
            by_day[op][day] = c
    return {
        "schema_version": SCHEMA_VERSION,
        "method_version": METHOD_VERSION,
        "as_of": as_of or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "days": sorted(d[0] for d in days),
        "sources": [{"day": d, "sha256": h} for d, h, _ in sorted(days, key=lambda d: d[0])],
        "method": METHOD,
        "caveats": CAVEATS,
        "operators": dict(sorted(operators.items())),
        "services": {key: {"operator": key.split("|")[1], "service": key.split("|")[0], **c}
                     for key, c in sorted(services.items())},
        "by_day": {op: dict(sorted(days_.items())) for op, days_ in sorted(by_day.items())},
    }


def _paths(patterns):
    paths = []
    for pattern in patterns:
        candidate = Path(pattern)
        if candidate.is_dir():
            paths += sorted(candidate.glob("observations-*.json*"))
        else:
            paths += [Path(p) for p in sorted(glob.glob(pattern))]
    return sorted(set(paths))


def load_days(patterns):
    """`[(day, sha256, counts)]` for each distinct observations file."""
    days, seen = [], set()
    for path in _paths(patterns):
        digest = digest_file(path)
        if digest in seen:
            continue
        seen.add(digest)
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as handle:
            doc = json.load(handle)
        days.append((doc.get("day") or "?", digest, count_day(doc)))
    return days


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--observations", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    days = load_days(args.observations)
    doc = build(days)
    Path(args.out).write_text(json.dumps(doc, separators=(",", ":"), sort_keys=True) + "\n")
    for op, c in doc["operators"].items():
        by = c["by"]
        print(f"{op}: {c['tracked']} of {c['scheduled']} timetabled journeys tracked; "
              f"by journey ID {by['journey_id']}, by start {by['start']}, by position {by['position']}")
    print(f"{len(days)} days → {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
