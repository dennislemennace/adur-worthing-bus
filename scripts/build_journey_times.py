"""Publish what each journey actually did, so the site can answer stop to stop.

Open Innovations' tool asks the question a passenger actually has: pick two
stops on a route, and see every observed journey time between them against the
timetable. That is far more legible than a punctuality percentage — "your 08:15
takes 52 minutes, not the 48 advertised" needs no explanation of DfT bands, and
nobody can argue about the definition of on time.

**The shape is theirs, and deliberately so.** Rather than precompute every stop
pair — n² per route, most never looked at — each journey is published as the
list of calls it made, and the browser subtracts one call from another. Files
stay small and every pair stays available.

Written per service, because that is how a reader thinks about a bus, and
because it keeps each file to something a phone can fetch.

    python scripts/build_journey_times.py --observations obs/ --out data/journey-times
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from process_snapshots import CAVEATS, METHOD, METHOD_VERSION    # noqa: E402
from query_reliability import load_observations                  # noqa: E402

# A journey with fewer calls than this says nothing about running time between
# two places, and would only add noise to a chart.
MIN_CALLS = 3

# Every stop, not just the operator's timing points.
#
# Publishing timing points only was tried first, to keep the files small enough
# to commit — and it cost the feature. A passenger wants the stop outside their
# house, and barely a fifth of stops are timing points. Since these files are
# served from R2 rather than committed, size is no longer the constraint, so
# the restriction went with it.
#
# What remains true is that a *scheduled* time at an interpolated stop is
# GTFS's estimate. Each call says which it is, and the chart draws the
# timetable line only between stops the operator commits to.
TIMING_POINTS_ONLY = False


def route_document(service, rows, meta, timing_points_only=TIMING_POINTS_ONLY,
                   operator=""):
    """One service: its stops, and every journey observed along them."""
    if timing_points_only:
        rows = [r for r in rows if r.get("timepoint") == 1]
    stops, order = {}, {}
    for row in rows:
        atco = row["atco"]
        if atco not in stops:
            stops[atco] = {"atco": atco, "name": row.get("stop_name", ""),
                           "direction": row.get("direction", "unknown")}
        # A stop's place in the route differs between journeys; the median
        # position is good enough to order a picker, and the browser reads the
        # real order from each journey's own calls.
        order.setdefault(atco, []).append(row.get("stop_index", 0))

    listed = sorted(stops.values(),
                    key=lambda s: (s["direction"],
                                   sorted(order[s["atco"]])[len(order[s["atco"]]) // 2]))
    index = {s["atco"]: i for i, s in enumerate(listed)}

    journeys = {}
    for row in rows:
        key = (row["day"], row["trip_id"])
        journey = journeys.setdefault(key, {
            "day": row["day"],
            "start": row.get("journey_start", ""),
            "direction": row.get("direction", "unknown"),
            "headsign": row.get("headsign", ""),
            "match": row.get("match", "inferred"),
            "calls": [],
        })
        journey["calls"].append([
            index[row["atco"]],
            row["observed_secs"],
            row["scheduled_secs"],
            # Two flags, because they mean different things to a chart: an
            # estimated *observation* was interpolated between two sightings,
            # while a non-timing-point *schedule* is GTFS's guess at when the
            # bus was due. The first affects the dot, the second the line.
            (1 if row.get("estimated") else 0) | (0 if row.get("timepoint") == 1 else 2),
        ])

    kept = []
    for journey in journeys.values():
        journey["calls"].sort(key=lambda call: call[2])
        if len(journey["calls"]) >= MIN_CALLS:
            kept.append(journey)
    kept.sort(key=lambda j: (j["day"], j["start"]))

    return {
        "service": service,
        # A service number belongs to an operator. The 1, the 5 and the 7 are
        # each run by both Brighton & Hove and Stagecoach in this area, and
        # merging them made one document out of two different routes: a stop
        # list that is the union of both, directions from both, and journey
        # times between two stops no single bus has ever run in sequence.
        "operator": operator,
        "as_of": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "days": meta["days"],
        "data_versions": meta["data_versions"],
        "method": METHOD,
        "method_version": METHOD_VERSION,
        "caveats": CAVEATS,
        # Calls are [stop, observed, scheduled, estimated] — seconds from
        # midnight, and estimated meaning the time was interpolated between two
        # sightings rather than observed. Said here because the browser reads
        # these arrays and a reader may open the file directly.
        # flags: 1 the observation was interpolated, 2 the scheduled time is
        # GTFS's estimate rather than a timing point.
        "call_format": ["stop_index", "observed_secs", "scheduled_secs", "flags"],
        "series": "timing_point" if timing_points_only else "all_stops",
        "stops": listed,
        "journeys": kept,
    }


def document_name(service, operator):
    """The file one service-and-operator is published as.

    The operator is always in the name, even where only one runs the number.
    Naming it only when there is a clash means the name changes the day a
    second operator appears, which breaks every link to it — and that day is
    exactly when someone is looking.
    """
    safe = "".join(c for c in service if c.isalnum() or c in "-_") or "unknown"
    noc = "".join(c for c in (operator or "") if c.isalnum()) or "unknown"
    return f"{safe}-{noc}"


def build(rows, meta, timing_points_only=TIMING_POINTS_ONLY):
    """`{(service, operator): document}` for everything with something to show."""
    grouped = {}
    for row in rows:
        key = (row.get("service", "?"), (row.get("operator") or "").strip())
        grouped.setdefault(key, []).append(row)
    return {key: route_document(key[0], rows_here, meta, timing_points_only,
                                operator=key[1])
            for key, rows_here in grouped.items()}


def main(argv=None):
    ap = argparse.ArgumentParser(description="Publish observed journey times.")
    ap.add_argument("--observations", nargs="+", required=True,
                    help="files, directories or globs of observation JSON")
    ap.add_argument("--out", default=str(ROOT / "data" / "journey-times"))
    ap.add_argument("--timing-points-only", action="store_true",
                    help="publish only stops the operator commits to a time for")
    ap.add_argument("--min-journeys", type=int, default=3,
                    help="services with fewer observed journeys are not written")
    args = ap.parse_args(argv)

    rows, meta = load_observations(args.observations)
    if not rows:
        print(f"no observations found in {args.observations}", file=sys.stderr)
        return 1

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    written, skipped, index = [], [], []
    for (service, operator), doc in sorted(
            build(rows, meta, args.timing_points_only).items()):
        label = f"{service} ({operator})" if operator else service
        if len(doc["journeys"]) < args.min_journeys:
            skipped.append(label)
            continue
        # One file a service *as one operator runs it*: a reader wants one
        # route, not the county, and not two companies' routes overlaid.
        path = out / f"{document_name(service, operator)}.json"
        path.write_text(json.dumps(doc, separators=(",", ":"), sort_keys=True) + "\n",
                        encoding="utf-8")
        written.append((label, len(doc["journeys"]), path.stat().st_size))
        index.append({"service": service, "operator": operator, "file": path.name,
                      "journeys": len(doc["journeys"]), "stops": len(doc["stops"])})

    (out / "index.json").write_text(
        json.dumps({"as_of": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    "days": meta["days"], "services": index},
                   indent=1, sort_keys=True) + "\n", encoding="utf-8")

    total = sum(size for _s, _j, size in written)
    for label, journeys, size in written:
        print(f"  {label:<14} {journeys:>4} journeys  {size / 1024:>6.0f} KB")
    print(f"{len(written)} services, {total / 1024:.0f} KB total → {out}")
    if skipped:
        print(f"too thin to publish ({args.min_journeys} journeys needed): "
              f"{', '.join(skipped)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
