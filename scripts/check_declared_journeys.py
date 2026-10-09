"""Has an operator's timetable moved on without ours?

Operators publish new timetables mid-week, and BODS gives their journeys new
ids. Our timetable is rebuilt weekly, so until then the live feed names
journeys we do not hold. On 7 October 2026 Stagecoach did this: 606 of their
619 journeys named a journey we did not hold, against about 4% for Brighton &
Hove on the same day — and BODS had left Stagecoach South out of its own GTFS,
so no rebuild could have fixed it.

Since method 7 such a bus may still be declared by SIRI-VM's line, first stop
and scheduled start (`declared_by: "start"`); otherwise it is inferred and
flagged `unresolved_declared_trip`. Either way GTFS-RT gave no id we hold, and
that is what this counts: any operator with at least MIN_JOURNEYS journeys of
which SHARE or more were not named by a held id.

    python scripts/check_declared_journeys.py observations-2026-10-07.json.gz

Prints a GitHub `::warning::` for each such operator. Exit status is always 0:
the night's data is good, and the cure is the operator's BODS dataset or our
next timetable build, which a person should look at.
"""

import argparse
import gzip
import json
import sys
from collections import defaultdict
from pathlib import Path

FLAG = "unresolved_declared_trip"
MIN_JOURNEYS = 20
SHARE = 0.3


def unresolved_by_operator(doc):
    """`{operator: (journeys, not named by a held id)}`, a journey counted once."""
    journeys = defaultdict(set)
    unresolved = defaultdict(set)
    for row in doc.get("observations") or []:
        key = (row.get("day"), row.get("trip_id"))
        op = row.get("operator") or "?"
        journeys[op].add(key)
        if FLAG in (row.get("quality_flags") or []) or row.get("declared_by") == "start":
            unresolved[op].add(key)
    return {op: (len(keys), len(unresolved[op])) for op, keys in journeys.items()}


def stale_operators(doc, min_journeys=MIN_JOURNEYS, share=SHARE):
    return sorted(op for op, (n, bad) in unresolved_by_operator(doc).items()
                  if n >= min_journeys and bad >= share * n)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("observations")
    args = ap.parse_args(argv)
    path = Path(args.observations)
    raw = path.read_bytes()
    doc = json.loads(gzip.decompress(raw) if path.suffix == ".gz" else raw)
    counts = unresolved_by_operator(doc)
    for op, (n, bad) in sorted(counts.items()):
        print(f"{op}: {bad} of {n} journeys were not named by a journey id our timetable holds")
    for op in stale_operators(doc):
        n, bad = counts[op]
        print(f"::warning::{op}: {bad} of {n} journeys on {doc.get('day')} were not named by a "
              "journey id our timetable holds. Check the operator's BODS timetable dataset "
              "and, if BODS has the new timetable, rebuild ours.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
