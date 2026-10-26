#!/usr/bin/env python3
"""Would the diversion allowance have made the live estimates better?

Calibrates scripts/build_diversion_adjustments.py on the event's early days,
then, on later days it never saw, compares two estimates for every recorded
journey past the diversion, made as the bus left the entry point:

* **today's**: the bus's lateness there, carried forward (api/live_eta.py);
* **adjusted**: the same, plus the allowance (api/diversions.py).

Each is scored against when the bus really left each timing point beyond, by
RTIG's accuracy windows (scripts/rtig_windows.py). "Bus came early", earlier
than the window allows, is the count that matters most: someone who trusts
the estimate and arrives at the stop on its time has missed that bus.

    python scripts/backtest_diversion_adjustments.py --observations 'obs/*.json.gz' \\
        --split 2026-10-06
"""

import argparse
import statistics
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import analysis_exclusions as ax                                  # noqa: E402
import build_diversion_adjustments as bd                          # noqa: E402
import rtig_windows as rtig                                       # noqa: E402
from api import diversions                                        # noqa: E402
from build_delay_hotspots import entry_period                     # noqa: E402


def score(pairs):
    """`pairs` are (seconds until the bus left, estimate minus actual)."""
    n = len(pairs)
    judged = Counter(rtig.classify(ahead + err, err) for ahead, err in pairs)
    return {"n": n,
            "median_abs_min": statistics.median(abs(e) for _a, e in pairs) / 60 if n else None,
            "rtig": rtig.shares(judged)}


def run(rows, events, coords, split):
    calib = [r for r in rows if r["day"] < split]
    test = [r for r in rows if r["day"] >= split]
    doc = bd.build(calib, events, coords)
    by = defaultdict(lambda: {"today": [], "adjusted": []})
    for event in events:
        for calls in bd._journeys(test).values():
            first = calls[0]
            if bd._family(first.get("operator")) != bd._family(event.get("operator")):
                continue
            if event["_routes"] and str(first.get("service") or "").upper() not in event["_routes"]:
                continue
            i = bd.entry_index(calls, event.get("area") or [], coords)
            if i is None:
                continue
            entry = calls[i]
            when = datetime.fromtimestamp(entry["observed_epoch"], timezone.utc)
            if not (event["_from"] <= when and (event["_to"] is None or when <= event["_to"])):
                continue
            atcos = [c["atco"] for c in calls]
            secs = [c["scheduled_secs"] for c in calls]
            plan = diversions.plan_for_trip(doc, first.get("operator"), first.get("service"), atcos, when)
            day_type = "weekday" if date.fromisoformat(entry["day"]).weekday() < 5 else "weekend"
            period = entry_period(entry["observed_epoch"])
            late = entry["lateness_secs"]
            for k in range(i + 1, len(calls)):
                today = late if late >= 0 or k == i + 1 else 0
                adjusted = today + diversions.extra_at(plan, k, day_type, period, secs)
                actual = calls[k]["lateness_secs"]
                ahead = calls[k]["observed_epoch"] - entry["observed_epoch"]
                key = (first.get("service"), first.get("direction"))
                for group in (key, "ALL"):
                    by[group]["today"].append((ahead, today - actual))
                    by[group]["adjusted"].append((ahead, adjusted - actual))
    return doc, by


def report(by, split, days_calib, days_test):
    lines = [f"Calibrated on days before {split} ({days_calib}); tested on {days_test}.", "",
             "| Service, direction | Timing points scored | Median error, today → adjusted (min) | "
             "Acceptable (RTIG) | Bus came early | Bus came late |",
             "|---|---|---|---|---|---|"]
    keys = ["ALL"] + sorted(k for k in by if k != "ALL")
    for key in keys:
        t, a = score(by[key]["today"]), score(by[key]["adjusted"])
        if not t["n"]:
            continue
        label = "**All**" if key == "ALL" else f"{key[0]} {key[1]}"

        def change(name):
            if not (t["rtig"] and a["rtig"]):
                return "–"
            return f"{t['rtig'][name]:.0%} → {a['rtig'][name]:.0%}"

        lines.append(f"| {label} | {t['n']} | {t['median_abs_min']:.1f} → {a['median_abs_min']:.1f} | "
                     f"{change('acceptable')} | {change('early')} | {change('late')} |")
    return "\n".join(lines)


def main(argv=None):
    from query_reliability import load_observations
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--observations", nargs="+", required=True)
    ap.add_argument("--split", required=True, help="first day used for testing, YYYY-MM-DD")
    args = ap.parse_args(argv)
    events = [e for e in ax.load() if e.get("adjust_predictions")]
    rows, _meta = load_observations(args.observations, row_filter=lambda r: r.get("timepoint") == 1)
    days = sorted({r["day"] for r in rows})
    calib = [d for d in days if d < args.split]
    test = [d for d in days if d >= args.split]
    _doc, by = run(rows, events, ax.stop_coords(), args.split)
    print(report(by, args.split, f"{calib[0]} to {calib[-1]}" if calib else "none",
                 f"{test[0]} to {test[-1]}" if test else "none"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
