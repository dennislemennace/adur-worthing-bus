#!/usr/bin/env python3
"""How a bus's lateness changes between timing points, measured from recorded
journeys, for the live estimate to allow (api/recovery.py).

For every journey identified exactly (declared, measured, no quality flags)
and outside any recorded disruption (data/analysis_exclusions.json, which the
diversion allowance covers on its own), and for every pair of its timing
points, the change in lateness from the first to the second. Grouped by
operator, service, the two timing points, day type, the time band the bus
left the first, and how late it was there (api/recovery.py), and published as
one percentile per group.

Which percentile is a choice about which way to be wrong. A late bus that makes
up time arrives before an estimate that assumed it would not: the error that
loses someone their bus. A low percentile of the change assumes more recovery
(and less time lost), so the estimate comes earlier and the bus, if anything,
later, which is the safer way round; a high percentile does the opposite (a
first test at the 70th and 85th raised "bus came early" from 8% to 11% and
15%). The choice is tested against days the figures never saw
(scripts/backtest_live_eta.py --model), never assumed.

A group with fewer than 30 journeys, or from fewer than 5 days, is left out;
so is a change under 30 seconds either way, which is noise at one report a
minute. Left out means no change, which is today's estimate.

    python scripts/build_running_recovery.py --observations 'obs/*.json.gz' \\
        --percentile 70 [--until 2026-10-15] --out running-recovery.json
"""
import argparse
import glob
import gzip
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import analysis_exclusions as ax                                  # noqa: E402
from api import recovery                                           # noqa: E402

MIN_JOURNEYS = 30
MIN_DAYS = 5
MIN_EFFECT_SECS = 30


def usable(row):
    return (row.get("match") == "declared" and not row.get("estimated")
            and not row.get("quality_flags") and row.get("lateness_secs") is not None
            and row.get("timepoint") == 1 and row.get("scheduled_secs") is not None)


def journeys(paths, until=None):
    """Each journey's timing-point rows in stop order, from every file."""
    for path in paths:
        opener = gzip.open if str(path).endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as handle:
            doc = json.load(handle)
        if until and doc.get("day", "") >= until:
            continue
        by = defaultdict(list)
        for row in doc.get("observations") or []:
            if usable(row):
                by[(row["day"], row["trip_id"])].append(row)
        for rows in by.values():
            rows.sort(key=lambda r: r["stop_index"])
            yield rows


def changes(rows):
    """`[(cell, change in seconds)]` for every pair of a journey's timing points."""
    first = rows[0]
    day = date.fromisoformat(first["day"])
    out = []
    for i, a in enumerate(rows):
        for b in rows[i + 1:]:
            cell = recovery.key(first.get("operator"), first.get("service"), a["atco"], b["atco"], day,
                                a["scheduled_secs"], a["lateness_secs"])
            if cell:
                out.append((cell, int(b["lateness_secs"]) - int(a["lateness_secs"])))
    return out


def build(paths, percentile, until=None, events=None, coords=None):
    events = ax.load() if events is None else events
    coords = ax.stop_coords() if coords is None else coords
    cells = defaultdict(list)
    days = set()
    for rows in journeys(paths, until):
        first = rows[0]
        when = datetime.fromtimestamp(first.get("observed_epoch") or 0, timezone.utc)
        atcos = [r["atco"] for r in rows]
        if events and ax.affecting(events, first.get("operator"), first.get("service"), when, atcos,
                                   [coords[a] for a in atcos if a in coords]):
            continue
        days.add(first["day"])
        for cell, change in changes(rows):
            cells[cell].append((change, first["day"]))
    out = {}
    for cell, values in cells.items():
        if len(values) < MIN_JOURNEYS or len({d for _c, d in values}) < MIN_DAYS:
            continue
        ordered = sorted(c for c, _d in values)
        value = ordered[round(percentile / 100 * (len(ordered) - 1))]
        if abs(value) >= MIN_EFFECT_SECS:
            out[cell] = value
    return {
        "schema_version": 1, "percentile": percentile, "days": sorted(days),
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "method": (f"For each pair of timing points on each route, the change in lateness of journeys identified "
                   f"exactly, by day type, time band and how late the bus was at the first; the {percentile}th "
                   f"percentile, from at least {MIN_JOURNEYS} journeys on {MIN_DAYS} days."),
        "caveats": ["Journeys through recorded disruptions are left out; diversion allowances cover those.",
                    "A missing cell means no change: the estimate carries the bus's lateness forward, as before."],
        "cells": dict(sorted(out.items())),
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--observations", nargs="+", required=True)
    ap.add_argument("--percentile", type=int, default=70)
    ap.add_argument("--until", help="first day NOT used, YYYY-MM-DD (to hold days back for testing)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    paths = sorted({p for pattern in args.observations for p in glob.glob(pattern)})
    doc = build(paths, args.percentile, args.until)
    Path(args.out).write_text(json.dumps(doc, separators=(",", ":")) + "\n")
    print(f"{len(doc['cells'])} cells from {len(doc['days'])} days, {args.percentile}th percentile")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
