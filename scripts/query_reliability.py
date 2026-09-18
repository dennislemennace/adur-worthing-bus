"""Ask the record which services and which hours are worst.

Reads the arrival observations the nightly processor publishes and prints
tables — by service, by hour, by stop, by journey, and the one that actually
locates a problem, by segment.

    # which service runs worst, at the operator's own timing points
    python scripts/query_reliability.py --by service

    # when: bucketed on the hour each bus was DUE, not the hour it turned up
    python scripts/query_reliability.py --by hour --service 700

    # where the time goes, worst first
    python scripts/query_reliability.py --by segment --direction westbound

    # a findings document, and the monthly aggregate the site will read
    python scripts/query_reliability.py --by service --report docs/reliability/2026-09.md
    python scripts/query_reliability.py --rollup data/reliability/2026-09.json

Observations come from the `reliability-YYYY-MM` releases:

    gh release download reliability-2026-09 -D /tmp/obs

**What every table says, and why.** Arrivals are counted only at the operator's
own timing points unless `--all-stops` is given, because lateness at a stop
GTFS interpolated is partly a measure of the interpolation. Each row carries
both denominators — arrivals and journeys — since one badly delayed journey can
otherwise supply thirty "very late" arrivals and look like thirty problems.
Medians, not means: the matching window censors both tails. And a cell thinner
than the floor prints `—` rather than a number, with the count of what was
suppressed, because a table that hides its thin cells hides how little it rests
on.
"""

import argparse
import glob
import gzip
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import reliability_stats as rs                                   # noqa: E402
from process_snapshots import CAVEATS, METHOD                    # noqa: E402


def load_observations(patterns):
    """Observations from files, directories or globs. Gzipped or not.

    Returns `(rows, meta)`, where meta records which days and which timetable
    builds produced them — a figure that cannot name its data cannot be
    checked, and a run may span two builds.
    """
    paths = []
    for pattern in patterns:
        candidate = Path(pattern)
        if candidate.is_dir():
            paths += sorted(candidate.glob("observations-*.json*"))
        else:
            paths += [Path(p) for p in sorted(glob.glob(pattern))]
    rows, days, versions = [], set(), set()
    for path in paths:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as handle:
            doc = json.load(handle)
        rows += doc.get("observations", [])
        days.add(doc.get("day", "?"))
        versions.add(doc.get("data_version", "unknown"))
    return rows, {"files": len(paths), "days": sorted(days),
                  "data_versions": sorted(versions)}


def filtered(rows, args):
    """The rows a run is about."""
    kept = rows if args.all_stops else rs.timing_points(rows)
    if args.service:
        kept = [r for r in kept if r.get("service") in args.service]
    if args.direction:
        kept = [r for r in kept if r.get("direction") == args.direction]
    if args.day:
        kept = [r for r in kept if r.get("day") in args.day]
    if args.from_hour is not None:
        kept = [r for r in kept if r["scheduled_secs"] // 3600 % 24 >= args.from_hour]
    if args.to_hour is not None:
        kept = [r for r in kept if r["scheduled_secs"] // 3600 % 24 <= args.to_hour]
    return kept


KEYS = {
    "service": (lambda r: r.get("service", "?"), "service"),
    "operator": (lambda r: r.get("operator", "?"), "operator"),
    "direction": (lambda r: r.get("direction", "unknown"), "direction"),
    "day": (lambda r: r.get("day", "?"), "day"),
    "vehicle": (lambda r: r.get("vehicle", "?"), "vehicle"),
    "stop": (lambda r: r.get("stop_name", "?"), "stop"),
    "hour": (rs.scheduled_hour, "hour due"),
    "service-hour": (lambda r: f'{r.get("service", "?")} {rs.scheduled_hour(r)}',
                     "service, hour due"),
    # For comparing the same hour across days: "did Thursday's 17:00 look like
    # Wednesday's?" Sort it with --sort key, or the rows arrive worst-first and
    # the two days interleave.
    "day-hour": (lambda r: f'{r.get("day", "?")} {rs.scheduled_hour(r)}',
                 "day, hour due"),
    "journey": (lambda r: f'{r.get("service", "?")} {r.get("journey_start", "?")}'
                          f' {r.get("direction", "?")[:4]}', "service, departure"),
}


def mins(secs):
    return f"{secs / 60:+.1f}"


def render_table(rows, args):
    """One table, worst first, with its denominators and its floor."""
    if args.by == "segment":
        return render_segments(rows, args)
    key, label = KEYS[args.by]
    cells = rs.group_stats(rows, key, mean=args.mean)
    kept, thin = rs.suppress(cells, args.min, args.min_journeys)
    if not kept and not thin:
        return ["No arrivals match."]

    order = (sorted(kept.items()) if args.sort == "key"
             else sorted(kept.items(), key=lambda kv: -kv[1]["not_on_time_share"]))
    head = (f"  {label:<26} {'arrivals':>8} {'journeys':>8} {'not on time':>12} "
            f"{'median':>7} {'p90':>7} {'per-journey':>11}")
    out = [head, "  " + "-" * (len(head) - 2)]
    for name, cell in order[:args.limit]:
        out.append(
            f"  {str(name):<26} {cell['observations']:>8} {cell['journeys']:>8} "
            f"{100 * cell['not_on_time_share']:>11.1f}% {mins(cell['median_secs']):>7} "
            f"{mins(cell['p90_secs']):>7} {mins(cell['per_journey_median_secs']):>11}")
        if args.mean:
            out.append(f"  {'':<26} mean {mins(cell['mean_secs'])} — {cell['mean_caveat']}")
        if cell["at_censoring_bound"]:
            out.append(f"  {'':<26} {cell['at_censoring_bound']} arrival(s) sit on the "
                       "matching window, so this row is a floor")
    if thin:
        out.append(f"  {'—':<26} {sum(c['observations'] for c in thin.values()):>8} "
                   f"{'':>8} {len(thin)} cell(s) suppressed under "
                   f"{args.min} arrivals / {args.min_journeys} journeys")
    return out


def render_segments(rows, args):
    """Where time is lost, worst first.

    Lateness *gained* between two timing points, which is the only form of this
    question that locates anything: rank stops by lateness instead and the
    answer is always the end of the route, because a stop inherits every minute
    lost before it.
    """
    cells = rs.segment_stats(rows, hour=args.segment_hours)
    kept, thin = rs.suppress(cells, args.min, args.min_journeys)
    if not kept and not thin:
        return ["No segments match."]
    order = sorted(kept.items(), key=lambda kv: -kv[1]["median_gained_secs"])
    head = (f"  {'segment (direction)':<56} {'legs':>5} {'journeys':>8} "
            f"{'gained':>7} {'p90':>7} {'worst':>7} {'sched':>6} {'vs sched':>9}")
    out = [head, "  " + "-" * (len(head) - 2)]
    for key, cell in order[:args.limit]:
        tail = f" [{key[2]}" + (f" {key[3]}:00]" if args.segment_hours else "]")
        # Trim the stop names, never the direction tag: a segment without its
        # direction is two different roads averaged together.
        name = f"{key[0]} → {key[1]}"[:56 - len(tail)]
        out.append(
            f"  {name + tail:<56} {cell['observations']:>5} "
            f"{cell['journeys']:>8} {mins(cell['median_gained_secs']):>7} "
            f"{mins(cell['p90_gained_secs']):>7} {mins(cell['worst_gained_secs']):>7} "
            f"{cell['scheduled_secs'] / 60:>6.0f} "
            f"{100 * cell['over_scheduled_share']:>+8.0f}%")
    if thin:
        out.append(f"  {'—':<56} {len(thin)} segment(s) suppressed under "
                   f"{args.min} legs / {args.min_journeys} journeys")
    return out


def provenance(rows, meta, args):
    """What a reader needs before believing any of the numbers above."""
    journeys = len({rs.journey_key(r) for r in rows})
    series = ("all stops (timing points and GTFS-interpolated)" if args.all_stops
              else "the operator's own timing points only")
    buckets = ("hour the bus was due" if args.by in ("hour", "service-hour")
               else "hour traversed" if args.by == "segment" else "n/a")
    return [
        "",
        f"  arrivals: {len(rows)} across {journeys} journeys, "
        f"{len(meta['days'])} day(s): {', '.join(meta['days'])}",
        f"  series:   {series}",
        f"  buckets:  {buckets}",
        f"  data:     {', '.join(meta['data_versions'])}",
        "  on time:  1 min early to 5 min 59 late (DfT BUS09), per arrival",
        f"  floor:    {args.min} arrivals and {args.min_journeys} journeys a cell",
        "  caveat:   lateness is censored at 5 min early / 25 min late by the",
        "            matcher, so early and very-late counts are floors.",
    ]


def write_report(lines, meta, args):
    """A dated findings document — the form a councillor or an FOI needs."""
    path = Path(args.report)
    path.parent.mkdir(parents=True, exist_ok=True)
    body = [
        f"# Measured reliability — {', '.join(meta['days'])}",
        "",
        f"Generated {datetime.now(timezone.utc).replace(microsecond=0).isoformat()} "
        f"by `scripts/query_reliability.py --by {args.by}`, from "
        f"{', '.join(meta['data_versions'])}.",
        "",
        "## Method",
        "",
        METHOD,
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in CAVEATS],
        "",
        f"## By {args.by}",
        "",
        "```",
        *lines,
        "```",
    ]
    path.write_text("\n".join(body) + "\n", encoding="utf-8")
    return f"report written to {path}"


def write_rollup(rows, meta, args):
    """The monthly aggregate the site will read.

    Counts and medians, with the denominators and the floor beside them. No
    percentages are precomputed beyond the band shares the cells already carry:
    a share travels away from its denominator far too easily.
    """
    month = (meta["days"][0] or "")[:7] if meta["days"] else ""
    kept_service, thin_service = rs.suppress(
        rs.group_stats(rows, KEYS["service"][0]), args.min, args.min_journeys)
    kept_hour, thin_hour = rs.suppress(
        rs.group_stats(rows, rs.scheduled_hour), args.min, args.min_journeys)
    segments, thin_segments = rs.suppress(
        rs.segment_stats(rows), args.min, args.min_journeys)
    payload = {
        "month": month,
        "as_of": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "data_versions": meta["data_versions"],
        "days": meta["days"],
        "method": METHOD,
        "caveats": CAVEATS,
        "series": "all_stops" if args.all_stops else "timing_point",
        "hour_basis": "scheduled departure hour",
        "floor": {"observations": args.min, "journeys": args.min_journeys},
        "suppressed": {"services": len(thin_service), "hours": len(thin_hour),
                       "segments": len(thin_segments)},
        "by_service": kept_service,
        "by_hour": kept_hour,
        "segments": {f"{k[0]} → {k[1]} [{k[2]}]": v for k, v in segments.items()},
    }
    path = Path(args.rollup)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n",
                    encoding="utf-8")
    return f"rollup written to {path}"


def main(argv=None):
    ap = argparse.ArgumentParser(description="Query measured bus reliability.")
    ap.add_argument("--observations", nargs="+",
                    default=[str(ROOT / "data" / "observations")],
                    help="files, directories or globs of observation JSON")
    ap.add_argument("--by", default="service", choices=sorted(KEYS) + ["segment"])
    ap.add_argument("--service", nargs="+", help="only these service numbers")
    ap.add_argument("--direction", choices=["westbound", "eastbound", "unknown"])
    ap.add_argument("--day", nargs="+", help="only these service days")
    ap.add_argument("--from-hour", type=int, help="earliest scheduled hour")
    ap.add_argument("--to-hour", type=int, help="latest scheduled hour")
    ap.add_argument("--all-stops", action="store_true",
                    help="include stops whose times GTFS interpolated")
    ap.add_argument("--segment-hours", action="store_true",
                    help="split segments by the hour the bus traversed them")
    ap.add_argument("--min", type=int, default=rs.MIN_OBS,
                    help="arrivals a cell needs to be shown")
    ap.add_argument("--min-journeys", type=int, default=rs.MIN_JOURNEYS,
                    help="journeys a cell needs to be shown")
    ap.add_argument("--limit", type=int, default=25, help="rows to print")
    ap.add_argument("--sort", choices=["worst", "key"], default="worst",
                    help="worst first, or in key order for comparing like with like")
    ap.add_argument("--mean", action="store_true",
                    help="also print the mean, with its censoring caveat")
    ap.add_argument("--json", action="store_true", help="print the cells as JSON")
    ap.add_argument("--report", help="write a findings document here")
    ap.add_argument("--rollup", help="write the monthly aggregate here")
    args = ap.parse_args(argv)

    rows, meta = load_observations(args.observations)
    if not rows:
        print(f"no observations found in {args.observations}", file=sys.stderr)
        return 1
    before = len(rows)
    # Observations processed before the timing-point flag reached the pipeline
    # carry no flag at all, and the default series then empties — which looks
    # like a broken tool rather than data that cannot answer the question.
    has_timepoints = any(r.get("timepoint") == 1 for r in rows)
    rows = filtered(rows, args)
    if not rows:
        if not args.all_stops and not has_timepoints:
            print(f"none of the {before} arrivals are at a timing point: this file was "
                  "processed before the timing-point flag reached the pipeline. "
                  "Reprocess the day, or pass --all-stops and read the figures knowing "
                  "they mix the operator's own times with GTFS estimates.",
                  file=sys.stderr)
        else:
            print("no arrivals match those filters", file=sys.stderr)
        return 1

    if args.json:
        cells = (rs.segment_stats(rows, hour=args.segment_hours) if args.by == "segment"
                 else rs.group_stats(rows, KEYS[args.by][0], mean=args.mean))
        print(json.dumps({" | ".join(k) if isinstance(k, tuple) else k: v
                          for k, v in cells.items()}, indent=1, sort_keys=True))
    else:
        lines = render_table(rows, args)
        print("\n".join(lines))
        print("\n".join(provenance(rows, meta, args)))
        if args.report:
            print(write_report(lines, meta, args))
    if args.rollup:
        print(write_rollup(rows, meta, args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
