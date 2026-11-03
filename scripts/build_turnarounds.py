#!/usr/bin/env python3
"""Which journey buses go on to run, measured from recorded days, for the
board to estimate a journey before its bus sets off (api/turnarounds.py).

For every bus identified exactly (declared, not estimated) on two journeys, the
second is its next journey if it starts no more than five minutes before the
first ends, by the timetable, and within the hour after; otherwise the bus did
not carry on in service (the garage, a break, a driver change). Each journey
ending is counted under its operator family, service, last stop and day type,
once with its time band and once without, as api/turnarounds.py keys it.

A cell names the service most of those buses went on to and a low percentile
of the gap, so the journey the API picks is the first one a bus could
plausibly take. Erring early is deliberate: a bus shown sooner than it comes
costs a wait, a bus shown later than it comes costs the bus. Published only
where at least MIN_JOURNEYS endings were seen on at least MIN_DAYS days and
more than half carried on to that service.

    python scripts/build_turnarounds.py --observations 'obs/*.json.gz' \\
        --timetable data/timetable.sqlite [--until 2026-10-28] --out turnarounds.json
"""
import argparse
import glob
import gzip
import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api import turnarounds                                        # noqa: E402

MIN_JOURNEYS = 10
MIN_DAYS = 3
MIN_SHARE = 0.5
PERCENTILE = 10
EARLY_SECS = 5 * 60


def sequences(rows):
    """{(operator, vehicle): [trip_id, ...]} in the order each bus was first
    seen on them, from declared rows only."""
    first = defaultdict(dict)
    for row in rows:
        if row.get("match") != "declared" or row.get("estimated") or not row.get("vehicle"):
            continue
        if row.get("observed_epoch") is None or not row.get("trip_id"):
            continue
        seen = first[(row.get("operator") or "", row["vehicle"])]
        if row["trip_id"] not in seen or row["observed_epoch"] < seen[row["trip_id"]]:
            seen[row["trip_id"]] = row["observed_epoch"]
    return {k: [t for t, _ in sorted(v.items(), key=lambda x: x[1])] for k, v in first.items()}


class Journeys:
    """What the timetable says about each journey: operator, service, first
    and last call. Memoised; None for a journey this timetable does not hold."""

    def __init__(self, tt):
        self.tt, self.memo = tt, {}

    def __call__(self, trip_id):
        if trip_id not in self.memo:
            trip = self.tt.trips.get(trip_id)
            calls = [c for c in self.tt.trip_stops_for(trip_id) if c[0] is not None] if trip else []
            if not calls:
                self.memo[trip_id] = None
            else:
                route = trip.get("route_id", "")
                self.memo[trip_id] = {
                    "operator": self.tt.noc_for_route(route),
                    "service": (self.tt.routes.get(route) or {}).get("short_name", ""),
                    "start": calls[0][0], "end": calls[-1][0], "terminus": calls[-1][1]}
        return self.memo[trip_id]


def endings(rows, day, journey):
    """(key, next service or None, gap secs or None) for each journey a bus
    finished, keyed both ways."""
    for seq in sequences(rows).values():
        for i, a in enumerate(seq):
            A = journey(a)
            if not A or not A["operator"] or not A["service"]:
                continue
            B = journey(seq[i + 1]) if i + 1 < len(seq) else None
            carried = B is not None and A["end"] - EARLY_SECS <= B["start"] <= A["end"] + turnarounds.WITHIN_SECS
            for key in turnarounds.keys(A["operator"], A["service"], A["terminus"], day, A["end"]):
                yield key, (B["service"] if carried else None), (B["start"] - A["end"] if carried else None)


def build(paths, tt, until=None):
    journey = Journeys(tt)
    seen = defaultdict(lambda: {"next": Counter(), "gaps": defaultdict(list), "days": set()})
    days = []
    for path in paths:
        opener = gzip.open if str(path).endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as handle:
            doc = json.load(handle)
        day = date.fromisoformat(doc["day"])
        if until and day > until:
            continue
        days.append(doc["day"])
        for key, nxt, gap in endings(doc.get("observations") or [], day, journey):
            cell = seen[key]
            cell["next"][nxt] += 1
            cell["days"].add(doc["day"])
            if nxt is not None:
                cell["gaps"][nxt].append(gap)
    cells = {}
    for key, cell in seen.items():
        n = sum(cell["next"].values())
        nxt, count = max(((s, c) for s, c in cell["next"].items() if s is not None),
                         key=lambda x: x[1], default=(None, 0))
        if nxt is None or n < MIN_JOURNEYS or len(cell["days"]) < MIN_DAYS or count / n <= MIN_SHARE:
            continue
        gaps = sorted(cell["gaps"][nxt])
        cells[key] = {"next": nxt, "after": max(0, gaps[(len(gaps) - 1) * PERCENTILE // 100]),
                      "journeys": n, "share": round(count / n, 3)}
    return {
        "schema_version": 1,
        "as_of": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "percentile": PERCENTILE,
        "method": ("For each journey a bus was identified on exactly, the journey the same bus started "
                   f"next, if within {turnarounds.WITHIN_SECS // 60} minutes of the first ending by the "
                   "timetable. Grouped by operator family, service, last stop, day type and time band (and "
                   f"without the band); the most common next service where more than {MIN_SHARE:.0%} of "
                   f"buses carried on to it, from at least {MIN_JOURNEYS} journeys on {MIN_DAYS} days, and "
                   f"the {PERCENTILE}th percentile of the gap between the timetabled end and next start."),
        "days": sorted(days),
        "cells": dict(sorted(cells.items())),
    }


def main(argv=None):
    from api.timetable_db import Timetable

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--observations", required=True, help="glob of observations-*.json(.gz)")
    ap.add_argument("--timetable", required=True, help="timetable.sqlite in force")
    ap.add_argument("--until", type=date.fromisoformat, help="ignore days after this one")
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    paths = sorted(glob.glob(args.observations))
    if not paths:
        sys.exit(f"no observations match {args.observations}")
    doc = build(paths, Timetable(Path(args.timetable), allow_fetch=False), args.until)
    Path(args.out).write_text(json.dumps(doc, separators=(",", ":")) + "\n")
    print(f"{len(doc['cells'])} cells from {len(doc['days'])} days -> {args.out}")


if __name__ == "__main__":
    main()
