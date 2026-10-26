#!/usr/bin/env python3
"""How close were our live arrival estimates? A replay of recorded days.

The stop board and the Bus tab estimate when a tracked bus will reach each
stop ahead (api/live_eta.py): its current lateness carried forward, with an
early bus taken as on time from its next timing point, where it must wait.
Nothing measured how good that is. This replays the same function
(`live_eta.projected_lateness`) from every timing point a bus was recorded
leaving, and compares each estimate with when that bus really left each later
stop on its journey.

Error is estimate minus actual, in seconds. **Positive means the bus came
earlier than the estimate**: someone who trusted it and walked to the stop on
its time has missed that bus. That share is the one to watch.

Biases, both stated in the report:

- **Fresher than live.** The replay starts from the moment the bus left a
  timing point. A live estimate starts from the bus's last report, which the
  feed delivers a median three minutes late, so live errors are larger,
  most of all a few minutes ahead.
- **Departures, not arrivals.** Every recorded time is the last report within
  150 m of the stop, so a bus that waits at a stop is timed leaving it. That
  affects the estimate and the actual alike.

Only journeys identified exactly (`match: declared`) are replayed, because only
they get a live estimate. Interpolated and quality-flagged timings are neither
anchors nor targets. Journeys through a recorded disruption
(data/analysis_exclusions.json) are scored separately: the live estimate adds
diversion allowances there (api/diversions.py) that this replay leaves out.

    python scripts/backtest_live_eta.py --observations 'obs/*.json.gz' --out report.json
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
sys.path.insert(0, str(ROOT / "scripts"))

import analysis_exclusions as ax                                  # noqa: E402
import rtig_windows as rtig                                       # noqa: E402
from api import recovery                                           # noqa: E402
from api.live_eta import projected_lateness                       # noqa: E402

BIN_SECS = 10
# Ranges in report order: RTIG's own, then the predictions it sets no window for.
RANGE_ORDER = rtig.LABELS + ["over 30 min", "already due"]


def usable(row):
    return (row.get("match") == "declared" and not row.get("estimated")
            and not row.get("quality_flags") and row.get("lateness_secs") is not None
            and row.get("observed_epoch") is not None)


def replay(calls, model=None):
    """`[(horizon_secs, estimate_error, timetable_error)]` for one journey.

    `calls` are its rows in stop order, interpolated ones included: they say
    where the timing points are, which the early-bus rule needs. The
    timetable's own error (estimate of no lateness at all) is the yardstick.
    With `model` (scripts/build_running_recovery.py), the estimate also allows
    for how lateness changed between these timing points on other days.
    """
    tps = [1 if c.get("timepoint") == 1 else 0 for c in calls]
    stops = [(c.get("scheduled_secs") or 0, c.get("atco")) for c in calls]
    first = calls[0] if calls else {}
    day = date.fromisoformat(first["day"]) if model and first.get("day") else None
    out = []
    for i, anchor in enumerate(calls):
        if anchor.get("timepoint") != 1 or not usable(anchor):
            continue
        ahead = projected_lateness(int(anchor["lateness_secs"]), tps, i + 1, len(calls))
        if model:
            ahead = recovery.apply(ahead, recovery.adjustments(
                model, first.get("operator"), first.get("service"), stops, tps, i, i + 1,
                int(anchor["lateness_secs"]), day))
        for k in range(i + 1, len(calls)):
            target = calls[k]
            if not usable(target):
                continue
            horizon = target["observed_epoch"] - anchor["observed_epoch"]
            if horizon <= 0:
                continue
            actual = int(target["lateness_secs"])
            out.append((horizon, ahead[k - i - 1] - actual, -actual))
    return out


def journeys(doc):
    by = defaultdict(list)
    for row in doc.get("observations") or []:
        by[(row.get("day"), row.get("trip_id"))].append(row)
    for rows in by.values():
        rows.sort(key=lambda r: (r.get("stop_index") is None, r.get("stop_index") or 0))
        yield rows


class Tally:
    """Histograms of error, per group, in BIN_SECS bins: twenty days of pairs
    would not fit in memory as a list. Alongside, each prediction judged by
    RTIG's window for how far ahead it was (scripts/rtig_windows.py)."""

    def __init__(self):
        self.groups = defaultdict(lambda: {"estimate": Counter(), "timetable": Counter(),
                                           "rtig_estimate": Counter(), "rtig_timetable": Counter(),
                                           "journeys": set()})

    def _one(self, key, which, journey, err, horizon):
        # How far ahead the prediction put the bus: when it said, less now.
        # The bus left `horizon` seconds after the estimate was made, and the
        # estimate said `err` seconds later than that.
        g = self.groups[key]
        g[which][round(err / BIN_SECS)] += 1
        g["rtig_" + which][rtig.classify(horizon + err, err)] += 1
        g["journeys"].add(journey)

    def add(self, keys, journey, err, base, horizon, scope=None):
        """One estimate/outcome pair in each group of `keys`. With `scope`, it
        is also filed under RTIG's range for how far ahead it was; the
        timetable's time for the same stop under its own range."""
        for key in keys:
            self._one(key, "estimate", journey, err, horizon)
            self._one(key, "timetable", journey, base, horizon)
        if scope is not None:
            self._one(("range", scope, rtig.label(horizon + err)), "estimate", journey, err, horizon)
            self._one(("range", scope, rtig.label(horizon + base)), "timetable", journey, base, horizon)

    @staticmethod
    def score(hist):
        n = sum(hist.values())
        if not n:
            return None

        def quantile_abs(q):
            absolute = Counter()
            for b, c in hist.items():
                absolute[abs(b)] += c
            seen = 0
            for b in sorted(absolute):
                seen += absolute[b]
                if seen >= q * n:
                    return b * BIN_SECS / 60
            return None

        def share(test):
            return sum(c for b, c in hist.items() if test(b * BIN_SECS)) / n

        seen, median = 0, 0
        for b, c in sorted(hist.items()):
            seen += c
            if seen >= n / 2:
                median = b * BIN_SECS / 60
                break
        return {"pairs": n, "median_abs_min": quantile_abs(0.5), "p90_abs_min": quantile_abs(0.9),
                "median_signed_min": median}

    def report(self):
        return {key: {"journeys": len(g["journeys"]), "estimate": self.score(g["estimate"]),
                      "timetable": self.score(g["timetable"]),
                      "rtig": {"estimate": rtig.shares(g["rtig_estimate"]),
                               "timetable": rtig.shares(g["rtig_timetable"])}}
                for key, g in sorted(self.groups.items(), key=lambda kv: [str(x) for x in kv[0]])}


def run(paths, events=None, coords=None, model=None):
    events = ax.load() if events is None else events
    coords = ax.stop_coords() if coords is None else coords
    tally, days = Tally(), []
    for path in paths:
        opener = gzip.open if str(path).endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as handle:
            doc = json.load(handle)
        days.append(doc.get("day"))
        for calls in journeys(doc):
            first = calls[0]
            pairs = replay(calls, model)
            if not pairs:
                continue
            seen = [c for c in calls if c.get("observed_epoch")]
            when = datetime.fromtimestamp(seen[0]["observed_epoch"], timezone.utc) if seen else None
            atcos = [c.get("atco") for c in calls]
            path_pts = [coords[a] for a in atcos if a in coords]
            hit = bool(when and ax.affecting(events, first.get("operator"), first.get("service"), when,
                                             atcos, path_pts))
            scope = "disrupted" if hit else "clear"
            op = first.get("operator") or "?"
            svc = f"{first.get('service')}|{op}"
            journey = (first.get("day"), first.get("trip_id"))
            for horizon, err, base in pairs:
                tally.add([("all", scope), ("operator", scope, op), ("service", scope, svc)],
                          journey, err, base, horizon, scope=scope)
    return sorted(d for d in days if d), tally


def markdown(days, report, min_pairs=2000):
    pct = lambda x: "–" if x is None else f"{x * 100:.0f}%"
    mins = lambda x: "–" if x is None else f"{x:.1f}"

    def field(r, which, name):
        return (r["rtig"][which] or {}).get(name)

    def table(rows, label):
        out = [f"| {label} | Pairs | Journeys | Typical error (min) | 9 in 10 within (min) | Bias (min) "
               "| Acceptable (RTIG) | **Bus came early** | Bus came late | Rating | Timetable alone: acceptable |",
               "|---|---|---|---|---|---|---|---|---|---|---|"]
        for name, r in rows:
            e = r["estimate"]
            out.append(f"| {name} | {e['pairs']:,} | {r['journeys']:,} | {mins(e['median_abs_min'])} "
                       f"| {mins(e['p90_abs_min'])} | {e['median_signed_min']:+.1f} "
                       f"| {pct(field(r, 'estimate', 'acceptable'))} | **{pct(field(r, 'estimate', 'early'))}** "
                       f"| {pct(field(r, 'estimate', 'late'))} | {field(r, 'estimate', 'rating') or '–'} "
                       f"| {pct(field(r, 'timetable', 'acceptable'))} |")
        return "\n".join(out)

    def pick(kind, scope="clear", floor=0):
        return [(k[2], v) for k, v in report.items()
                if k[0] == kind and k[1] == scope and v["estimate"] and v["estimate"]["pairs"] >= floor]

    def ranges_table(rows):
        out = ["| Ahead, as the estimate said | Allowed early / late | Estimates | Acceptable | **Bus came early** "
               "| Bus came late | Rating | Timetable times this far ahead | Timetable alone: acceptable |",
               "|---|---|---|---|---|---|---|---|---|"]
        windows = {f"{lo}-{hi} min": f"{early} s / {late} s" for lo, hi, early, late in rtig.RANGES}
        for name, r in rows:
            e, t = r["estimate"], r["timetable"]
            out.append(f"| {name} | {windows.get(name, 'none set')} | {e['pairs'] if e else 0:,} "
                       f"| {pct(field(r, 'estimate', 'acceptable'))} | **{pct(field(r, 'estimate', 'early'))}** "
                       f"| {pct(field(r, 'estimate', 'late'))} | {field(r, 'estimate', 'rating') or '–'} "
                       f"| {t['pairs'] if t else 0:,} | {pct(field(r, 'timetable', 'acceptable'))} |")
        return "\n".join(out)

    ranges = sorted([(k[2], v) for k, v in report.items() if k[0] == "range" and k[1] == "clear"],
                    key=lambda kv: RANGE_ORDER.index(kv[0]))
    services = sorted([kv for kv in pick("service", floor=min_pairs) if kv[1]["rtig"]["estimate"]],
                      key=lambda kv: -kv[1]["rtig"]["estimate"]["early"])[:15]
    lines = [
        f"Days replayed: {days[0]} to {days[-1]} ({len(days)} days).", "",
        "Judged by RTIG's accuracy windows (RTIGT041-1.0, section 6.9): each estimate by "
        "how far ahead the time it gave was. Estimates more than 30 minutes ahead, or "
        "already due when made, have no RTIG window and are left out of the shares.", "",
        "### All journeys outside recorded disruptions", "",
        table([("All", report[("all", "clear")])], "Scope"), "",
        "### By how far ahead the estimate said the bus was", "",
        ranges_table(ranges), "",
        "### By operator", "",
        table(sorted(pick("operator")), "Operator"), "",
        f"### The services where buses most often came earlier than RTIG allows "
        f"(at least {min_pairs:,} pairs)", "",
        table([(k.replace("|", " "), v) for k, v in services], "Service"), "",
    ]
    if ("all", "disrupted") in report:
        lines += ["### Journeys through a recorded disruption (no allowance applied in this replay)", "",
                  table([("All", report[("all", "disrupted")])], "Scope"), ""]
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--observations", nargs="+", required=True)
    ap.add_argument("--out", help="write the full report as JSON")
    ap.add_argument("--model", help="running-recovery.json to allow for (scripts/build_running_recovery.py)")
    ap.add_argument("--from-day", help="first day to replay, YYYY-MM-DD (days a model never saw)")
    args = ap.parse_args(argv)
    paths = sorted({p for pattern in args.observations for p in glob.glob(pattern)})
    if args.from_day:
        paths = [p for p in paths if Path(p).name.split('-', 1)[-1][:10] >= args.from_day]
    model = json.loads(Path(args.model).read_text()) if args.model else None
    days, tally = run(paths, model=model)
    report = tally.report()
    if args.out:
        Path(args.out).write_text(json.dumps({"days": days, "groups": [
            {"key": list(k), **v} for k, v in report.items()]}, indent=1) + "\n")
    print(markdown(days, report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
