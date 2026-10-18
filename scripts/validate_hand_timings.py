#!/usr/bin/env python3
"""How close are our recorded stop times to times seen by hand?

Every stop time on the site is inferred: the last location report within
150 m of the stop, bounded by the next report about a minute later. Nothing
had checked one against a bus someone watched leave. This takes the departures
logged by hand on timing.html (route, fleet number or registration, and the
moment the doors opened and the bus pulled away) and matches each to the
observation row for the same bus at the same stop on the same day.

**Error is our recorded time minus the hand-timed departure, in seconds.**
Positive means we record the bus as leaving later than it did. Reported:

- the share of hand-timed buses we recorded at all, and why the rest were not;
- the median, 90th-percentile and signed bias of the error;
- how often the true departure fell inside our recorded bounds (the last
  report in the stop area and the next report after it);
- how long buses stood with their doors open;
- all of it by kind of stop (timing point, end of route, other) and operator.

A bus matched by its fleet number or registration is a strong match. One
logged without either is matched on route and the nearest time within three
minutes, and counted apart as a weak match.

    python scripts/validate_hand_timings.py --timings timings-2026-10-20.csv \\
        --observations obs/ --out hand-check.json
    python scripts/validate_hand_timings.py --timings 'timings-*.csv' --fetch obs-cache/
"""

import argparse
import csv
import glob
import gzip
import json
import statistics
import sys
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PUBLISHED = "https://adur-worthing-submissions.dennislemennace.workers.dev/journey-times"

# A bus's recorded departure this far from the hand-timed one is not the same
# departure: the next pass of a looping route, or the bus logged at the wrong
# pole.
STRONG_WINDOW_SECS = 15 * 60
WEAK_WINDOW_SECS = 3 * 60
# Poles of one stop name within this distance are the same place for a
# passenger, and a person can log the wrong side of the road.
SIBLING_METRES = 120


def vehicle_key(text):
    """As timing.js's timingVehicleKey: "SCSO-10950" -> "SCSO10950"."""
    return "".join(ch for ch in str(text or "").upper() if ch.isalnum())


def same_vehicle(recorded, typed):
    """The fleet number on the bus is the tail of what the feed carries for
    Stagecoach ("SCSO-10950") and all of it for the others ("832", "YX12_FRC")."""
    r, t = vehicle_key(recorded), vehicle_key(typed)
    return bool(r and t and len(t) >= 3 and (r == t or r.endswith(t)))


def _epoch(iso):
    if not iso:
        return None
    return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).timestamp()


def read_timings(paths):
    """Logged buses, one dict each, with `departed` and `arrived` as epochs
    corrected by the phone's measured clock offset (positive: phone fast)."""
    out = []
    for path in paths:
        with open(path, newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                offset = float(row.get("clock_offset_ms") or 0) / 1000
                dep, arr = _epoch(row.get("departed_at")), _epoch(row.get("arrived_at"))
                out.append({**row, "source": Path(path).name,
                            "departed": dep - offset if dep is not None else None,
                            "arrived": arr - offset if arr is not None else None})
    return out


def siblings(stops, atco):
    """The pole itself and same-named poles within SIBLING_METRES."""
    here = stops.get(atco)
    if not here:
        return {atco}
    out = {atco}
    for other, s in stops.items():
        if s["name"] == here["name"]:
            dy = (s["latitude"] - here["latitude"]) * 111_320
            dx = (s["longitude"] - here["longitude"]) * 111_320 * 0.63
            if (dx * dx + dy * dy) ** 0.5 <= SIBLING_METRES:
                out.add(other)
    return out


def stop_kind(row):
    if row.get("stop_index") == 0 or (row.get("calls_total") and row.get("stop_index") == row["calls_total"] - 1):
        return "end of route"
    return "timing point" if row.get("timepoint") == 1 else "other stop"


def match(entry, by_atco, stops):
    """`(observation row or None, how it matched or why not)` for one logged bus."""
    dep = entry.get("departed")
    if dep is None:
        return None, "no departure logged"
    poles = siblings(stops, entry.get("atco"))
    rows = [r for a in poles for r in by_atco.get(a, ())]
    if entry.get("vehicle"):
        near = [r for r in rows if same_vehicle(r.get("vehicle"), entry["vehicle"])
                and abs(r["observed_epoch"] - dep) <= STRONG_WINDOW_SECS]
        if near:
            return min(near, key=lambda r: abs(r["observed_epoch"] - dep)), "fleet number"
        if not any(same_vehicle(r.get("vehicle"), entry["vehicle"]) for r in rows):
            return None, "bus not recorded at this stop"
        return None, "bus recorded here, but not near this time"
    route = str(entry.get("route") or "").upper()
    near = [r for r in rows if str(r.get("service") or "").upper() == route
            and abs(r["observed_epoch"] - dep) <= WEAK_WINDOW_SECS]
    if near:
        return min(near, key=lambda r: abs(r["observed_epoch"] - dep)), "route and time"
    return None, "no fleet number, and no bus of that route within three minutes"


def compare(entries, observation_rows, stops):
    """One result per logged bus."""
    by_atco = defaultdict(list)
    for r in observation_rows:
        if not r.get("estimated") and r.get("observed_epoch") is not None:
            by_atco[r.get("atco")].append(r)
    results = []
    for e in entries:
        row, how = match(e, by_atco, stops)
        result = {"id": e.get("id"), "day": e.get("day"), "atco": e.get("atco"), "stop": e.get("stop_name"),
                  "route": e.get("route"), "vehicle": e.get("vehicle"), "matched": how if row else None,
                  "reason": None if row else how,
                  "dwell_secs": (round(e["departed"] - e["arrived"])
                                 if e.get("departed") is not None and e.get("arrived") is not None else None)}
        if row:
            lo, hi = (row.get("observed_interval_epoch") or [row["observed_epoch"], row["observed_epoch"]])[:2]
            result.update({
                "error_secs": round(row["observed_epoch"] - e["departed"]),
                "inside_bounds": lo - 1 <= e["departed"] <= hi + 1,
                "bounds_secs": round(hi - lo),
                "operator": row.get("operator"), "kind": stop_kind(row), "identity": row.get("match"),
                "flagged": bool(row.get("quality_flags")),
            })
        results.append(result)
    return results


def _quantile(values, q):
    s = sorted(values)
    return s[min(len(s) - 1, int(q * len(s)))] if s else None


def _block(rs):
    errs = [r["error_secs"] for r in rs if r.get("error_secs") is not None]
    dwell = [r["dwell_secs"] for r in rs if r.get("dwell_secs") is not None]
    inside = [r["inside_bounds"] for r in rs if r.get("inside_bounds") is not None]
    return {"matched": len(errs),
            "median_abs_secs": statistics.median(abs(x) for x in errs) if errs else None,
            "p90_abs_secs": _quantile([abs(x) for x in errs], 0.9),
            "bias_secs": statistics.median(errs) if errs else None,
            "within_30_secs": sum(abs(x) <= 30 for x in errs),
            "within_60_secs": sum(abs(x) <= 60 for x in errs),
            "inside_bounds": sum(inside), "bounds_checked": len(inside),
            "median_dwell_secs": statistics.median(dwell) if dwell else None}


def summarise(results):
    strong = [r for r in results if r["matched"] == "fleet number"]
    reasons = defaultdict(int)
    for r in results:
        if r["reason"]:
            reasons[r["reason"]] += 1

    def by(key):
        return {k: _block([r for r in strong if r.get(key) == k])
                for k in sorted({r.get(key) for r in strong if r.get(key)})}

    return {"logged": len(results), "strong": _block(strong),
            "weak": _block([r for r in results if r["matched"] == "route and time"]),
            "unmatched": dict(sorted(reasons.items())), "by_kind": by("kind"), "by_operator": by("operator"),
            "days": sorted({r["day"] for r in results if r.get("day")})}


def load_observations(days, folder):
    rows = []
    for day in days:
        for path in sorted(Path(folder).glob(f"observations-{day}.json*")):
            opener = gzip.open if path.suffix == ".gz" else open
            with opener(path, "rt", encoding="utf-8") as handle:
                rows += json.load(handle).get("observations") or []
    return rows


def fetch(days, folder, base=PUBLISHED):
    """Download the published observation files for `days` into `folder`."""
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(f"{base}/index.json", timeout=60) as res:
        index = json.load(res)
    for day in days:
        name = f"observations-{day}.json.gz"
        entry = (index.get("artifacts") or {}).get(name)
        if not entry:
            print(f"{name}: not published yet")
            continue
        if not (folder / name).exists():
            urllib.request.urlretrieve(f"{base}/{entry['file']}", folder / name)


def markdown(summary):
    def secs(v, signed=False):
        if v is None:
            return "–"
        return f"{v:+.0f} s" if signed else f"{v:.0f} s"

    def share(a, b):
        return f"{a} of {b}" + (f" ({round(100 * a / b)}%)" if b else "")

    s = summary["strong"]
    lines = [f"Days: {', '.join(summary['days'])}. Buses logged: {summary['logged']}.", "",
             f"- Matched by fleet number or registration: **{s['matched']}**; by route and time only: "
             f"{summary['weak']['matched']}.",
             f"- Typical error: **{secs(s['median_abs_secs'])}**; 9 in 10 within **{secs(s['p90_abs_secs'])}**; "
             f"bias **{secs(s['bias_secs'], True)}** (positive: we record buses leaving later than they did).",
             f"- Within 30 s: {share(s['within_30_secs'], s['matched'])}; within 60 s: "
             f"{share(s['within_60_secs'], s['matched'])}.",
             f"- The true departure fell inside our recorded bounds: "
             f"{share(s['inside_bounds'], s['bounds_checked'])}.",
             f"- Doors open for a median {secs(s['median_dwell_secs'])}.", ""]
    if summary["unmatched"]:
        lines += ["Not matched: " + "; ".join(f"{n} {why}" for why, n in summary["unmatched"].items()), ""]
    for title, key in (("By kind of stop", "by_kind"), ("By operator", "by_operator")):
        lines += [f"### {title}", "",
                  "| | Matched | Typical error | 9 in 10 within | Bias | Inside bounds |",
                  "|---|---|---|---|---|---|"]
        for name, b in summary[key].items():
            lines.append(f"| {name} | {b['matched']} | {secs(b['median_abs_secs'])} | {secs(b['p90_abs_secs'])} "
                         f"| {secs(b['bias_secs'], True)} | {share(b['inside_bounds'], b['bounds_checked'])} |")
        lines.append("")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--timings", nargs="+", required=True, help="CSV files exported from timing.html")
    ap.add_argument("--observations", help="folder holding observations-YYYY-MM-DD.json.gz")
    ap.add_argument("--fetch", help="download the published observations into this folder first")
    ap.add_argument("--out", help="write the per-bus results and summary as JSON")
    args = ap.parse_args(argv)
    paths = sorted({p for pattern in args.timings for p in glob.glob(pattern)})
    entries = read_timings(paths)
    days = sorted({e["day"] for e in entries if e.get("day")})
    # A bus after midnight belongs to the service day before.
    wanted = sorted(set(days) | {(date.fromisoformat(d) - timedelta(days=1)).isoformat() for d in days})
    folder = args.fetch or args.observations
    if not folder:
        ap.error("give --observations or --fetch")
    if args.fetch:
        fetch(wanted, args.fetch)
    stops = {s["atco_code"]: s for s in json.loads((ROOT / "data/stops.json").read_text())["stops"]}
    results = compare(entries, load_observations(wanted, folder), stops)
    summary = summarise(results)
    if args.out:
        Path(args.out).write_text(json.dumps({"summary": summary, "results": results}, indent=1) + "\n")
    print(markdown(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
