#!/usr/bin/env python3
"""How much later buses run past a diversion, for the live estimates to allow.

Live estimates (api/live_eta.py) carry a bus's lateness forward unchanged, so a
bus that has not yet reached a diversion is promised at the stops beyond it as
if the diversion were not there. The Western Road closure of autumn 2026 sent
the 700 towards Worthing round Seven Dials, and buses reached King Alfred a
typical five minutes later than they had the week before — five minutes the
board did not know about.

For each event in data/analysis_exclusions.json marked `adjust_predictions`,
this measures, from the buses' own recorded journeys:

* the **entry point**: the last timing point before the closed area, where the
  route was still normal (Churchill Square, for the 700 westbound);
* for each timing point beyond it, the lateness a journey **gained since the
  entry point**, during the event and in the weeks before it;
* the **15th percentile** of that gain during the event, by day type and time
  of day: the amount the live estimate, which assumes none, adds.

Gained since the entry point, not lateness itself, so a bus's lateness before
the diversion is not counted twice; and measured at every point beyond, not one
section's delay, because buses make some of it back further on (the 700 made
back about two and a half minutes between King Alfred and Shoreham).

Not the difference from the weeks before. That was the first design, and the
week before the closure turned out to hold an event of its own: from 21 to 25
September the 700 was already losing 3 to 10 minutes by Beach Green, which made
the closure look like a gain there. The before figure is kept beside each cell
as context; whether the during figure improves the estimates is tested against
recorded journeys (docs/reliability/), not assumed.

Why the 15th percentile, not the median. A median is right half the time and
early the other half, and an estimate that is early is the one that loses
someone their bus: tested on 10–14 October against what the buses then did,
the median cut the typical error from 2.7 to 1.9 minutes but had buses come 2
or more minutes before the estimate 17% of the time, against 2% without it.
The 15th percentile kept almost all the gain (2.0 minutes, 50% within two
minutes against 41%) for 3%. An allowance only ever adds: a point where the
effect has worn off is kept at zero, so a larger figure either side cannot
stand in for it. docs/reliability/diversion-adjustments-backtest-2026-10-15.md
has the full table.

Floor: 30 journeys over 5 days during the event, else the day type's all-day
figure, else nothing. Figures under 30 seconds count as zero.

    python scripts/build_diversion_adjustments.py --observations inputs/window \\
        --out diversion-adjustments.json [--today YYYY-MM-DD]
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import analysis_exclusions as ax                                  # noqa: E402
from build_delay_hotspots import entry_period, quality_reason     # noqa: E402
from observation_contract import row_identity                     # noqa: E402

MIN_JOURNEYS, MIN_DAYS = 30, 5
MIN_EFFECT_SECS = 30
QUANTILE = 0.15
# How far before an event its baseline reaches: long enough to clear the floor,
# short enough that it is the same timetable and season.
BASELINE_DAYS = 28
MAX_INTERVAL_SECS = 180

METHOD = ("For each journey, the lateness gained between the last timing point before "
          "the event's area (the entry point) and each timing point after it; the 15th "
          "percentile during the event, by day type and the entry's time of day, so the "
          "estimate is rarely later than the bus; never below zero. Measured timing points "
          "of declared journeys only, as the delay map uses. A cell needs 30 journeys over "
          "5 days, else the day type's all-day figure is used, else nothing. The same "
          "figure from the 28 days before the event is given as context.")
CAVEATS = [
    "A low figure on purpose: most buses lose more than this, so estimates stay on the side of the bus being later than said.",
    "Times are departures from a radius around each stop, so dwell is included.",
    "Everything that slowed or sped buses past the entry point during the event is counted, not only the diversion.",
]


def _family(op):
    return ax._FAMILIES.get(op or "", op or "")


def _journeys(rows):
    """Each journey's measured timing points, in route order."""
    by = defaultdict(list)
    for r in rows:
        if r.get("timepoint") == 1 and not quality_reason(r, MAX_INTERVAL_SECS):
            by[row_identity(r)[:3]].append(r)
    for calls in by.values():
        calls.sort(key=lambda r: r.get("stop_index", -1))
    return by


def entry_index(calls, area, coords):
    """Index into `calls` of the last timing point before the area: the start
    of the first stretch whose straight line enters it. None if none does."""
    for i in range(len(calls) - 1):
        a, b = coords.get(calls[i]["atco"]), coords.get(calls[i + 1]["atco"])
        if a and b and ax.path_touches([a, b], area):
            # Only where the two are adjacent timing points: with the real
            # entry point unmeasured, an earlier one would stand in for it
            # and file its gains under a point the live estimate never uses.
            adjacent = calls[i + 1].get("timing_point_order") == calls[i].get("timing_point_order", -2) + 1
            return i if adjacent and not ax.path_touches([a], area) else None
    return None


def measure(rows, event, coords):
    """`{(service, direction, entry_atco): {...}}` gains since the entry point,
    split before / during the event, per downstream point and cell."""
    start = event["_from"]
    end = event["_to"]
    baseline_from = start - timedelta(days=BASELINE_DAYS)
    out = {}
    for calls in _journeys(rows).values():
        first = calls[0]
        if _family(first.get("operator")) != _family(event.get("operator")):
            continue
        if event["_routes"] and str(first.get("service") or "").upper() not in event["_routes"]:
            continue
        i = entry_index(calls, event.get("area") or [], coords)
        if i is None:
            continue
        entry = calls[i]
        when = datetime.fromtimestamp(entry["observed_epoch"], timezone.utc)
        if start <= when and (end is None or when <= end):
            phase = "during"
        elif baseline_from <= when < start:
            phase = "before"
        else:
            continue
        day_type = "weekday" if date.fromisoformat(entry["day"]).weekday() < 5 else "weekend"
        period = entry_period(entry["observed_epoch"])
        key = (first.get("service"), first.get("direction"), entry["atco"])
        held = out.setdefault(key, {"entry_name": entry.get("stop_name", ""), "points": {}})
        for later in calls[i + 1:]:
            gained = later["lateness_secs"] - entry["lateness_secs"]
            point = held["points"].setdefault(later["atco"], {
                "name": later.get("stop_name", ""), "order": later["stop_index"] - entry["stop_index"],
                "samples": defaultdict(list)})
            for cell in (f"{day_type}|{period}", f"{day_type}|all"):
                point["samples"][(phase, cell)].append((gained, entry["day"]))
    return out


def _side(samples, quantile=0.5):
    days = {d for _g, d in samples}
    if len(samples) < MIN_JOURNEYS or len(days) < MIN_DAYS:
        return None
    gains = sorted(g for g, _d in samples)
    return gains[min(len(gains) - 1, int(len(gains) * quantile))], len(samples), len(days)


def build(rows, events, coords, as_of=None):
    adjustments = []
    for event in events:
        if not event.get("adjust_predictions"):
            continue
        services = []
        for (service, direction, entry_atco), held in sorted(measure(rows, event, coords).items(),
                                                             key=lambda kv: tuple(map(str, kv[0]))):
            points = []
            for atco, point in sorted(held["points"].items(), key=lambda kv: kv[1]["order"]):
                cells = {}
                for (phase, cell) in point["samples"]:
                    if phase != "during" or cell in cells:
                        continue
                    during = _side(point["samples"][("during", cell)], QUANTILE)
                    if not during:
                        continue
                    # Kept at zero where the effect is gone, not left out: a
                    # missing point would let a larger figure either side of it
                    # stand in, promising buses later than they come.
                    extra = during[0] if during[0] >= MIN_EFFECT_SECS else 0
                    before = _side(point["samples"].get(("before", cell), []))
                    cells[cell] = {"extra_secs": int(round(extra / 10.0) * 10),
                                   "journeys": during[1], "days": during[2],
                                   "before_secs": int(round(before[0] / 10.0) * 10) if before else None}
                if cells:
                    points.append({"atco": atco, "name": point["name"], "cells": dict(sorted(cells.items()))})
            if any(c["extra_secs"] for p in points for c in p["cells"].values()):
                services.append({"service": service, "direction": direction, "entry_atco": entry_atco,
                                 "entry_name": held["entry_name"], "points": points})
        if services:
            adjustments.append({
                "disruption_id": event.get("disruption_id") or event["id"], "operator": event.get("operator"),
                "routes": event.get("routes") or [], "from": event["from"], "to": event.get("to"),
                "stops_not_served": event.get("stops_not_served") or [],
                # The words a board shows: "allows 3 min for the Western Road diversion".
                "reason": event.get("label") or event.get("summary", ""), "services": services})
    return {"schema_version": 1, "as_of": as_of or datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "method": METHOD, "caveats": CAVEATS,
            "floor": {"journeys": MIN_JOURNEYS, "distinct_days": MIN_DAYS, "min_effect_secs": MIN_EFFECT_SECS},
            "adjustments": adjustments}


def main(argv=None):
    from query_reliability import load_observations
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--observations", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--exclusions", default=str(ax.DATA))
    ap.add_argument("--today", help="leave out events that ended before this day (default: today)")
    args = ap.parse_args(argv)
    today = date.fromisoformat(args.today) if args.today else datetime.now(timezone.utc).date()
    events = [e for e in ax.load(Path(args.exclusions)) if e.get("adjust_predictions")
              and (e["_to"] is None or e["_to"].date() >= today)]
    if not events:
        rows = []
    else:
        rows, _meta = load_observations(args.observations, row_filter=lambda r: r.get("timepoint") == 1)
    result = build(rows, events, ax.stop_coords())
    out = Path(args.out)
    out.write_text(json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n")
    n = sum(len(s["points"]) for a in result["adjustments"] for s in a["services"])
    print(f"{len(events)} events to adjust for; {len(result['adjustments'])} measured; "
          f"{n} downstream points → {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
