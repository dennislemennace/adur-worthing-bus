#!/usr/bin/env python3
"""Write data/ramp_use.json: how often the wheelchair ramp goes out at a stop.

No open data says when a bus's ramp is used: the location feeds carry no ramp
or door status, and a position a minute cannot time a dwell. So this counts
what riders log by hand on ride.html ("Ramp out" and "Ramp in", 6 November
2026), from the CSVs they export. Earlier rides noted it as "Wheelchair ramp
used", without times; those count as a deployment with no duration.

A stop's figures are published only past a floor (MIN_DEPLOYMENTS over
MIN_DAYS), and always as hand-logged: it says where riders saw the ramp used,
not how often it is used. Below the floor only the totals are kept.

    python scripts/build_ramp_log.py --rides path/to/exported-csvs [--out data/ramp_use.json]
"""

import argparse
import csv
import json
import statistics
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MIN_DEPLOYMENTS = 10
MIN_DAYS = 5
NOTE = "wheelchair ramp used"
# Longer than this is a tap forgotten, not a ramp: left out of the timing.
MAX_SECS = 15 * 60


def _instant(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def read_rows(folder: Path):
    """Every logged stop row from the exported CSVs, each once (rows carry an id)."""
    seen, out = set(), []
    for path in sorted(folder.glob("*.csv")):
        with path.open(encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                key = row.get("id") or (row.get("ride_id"), row.get("seq"))
                if key in seen:
                    continue
                seen.add(key)
                out.append(row)
    return out


def build(rows, generated_on):
    stops, days_all, rides = {}, set(), set()
    total = 0
    for r in rows:
        timed = bool(r.get("ramp_out_at"))
        noted = NOTE in (r.get("note") or "").lower()
        if not (timed or noted) or (r.get("event") or "stop") != "stop" or not r.get("atco"):
            continue
        total += 1
        days_all.add(r.get("day", ""))
        rides.add(r.get("ride_id", ""))
        s = stops.setdefault(r["atco"], {"name": r.get("stop_name", ""), "deployments": 0, "days": set(), "secs": []})
        s["deployments"] += 1
        s["days"].add(r.get("day", ""))
        start, end = _instant(r.get("ramp_out_at")), _instant(r.get("ramp_in_at"))
        if start and end and 0 < (end - start).total_seconds() <= MAX_SECS:
            s["secs"].append((end - start).total_seconds())
    published = {}
    for atco, s in sorted(stops.items()):
        if s["deployments"] < MIN_DEPLOYMENTS or len(s["days"]) < MIN_DAYS:
            continue
        days = sorted(d for d in s["days"] if d)
        published[atco] = {"name": s["name"], "deployments": s["deployments"], "days": len(days),
                           "from": days[0], "to": days[-1], "timed": len(s["secs"]),
                           "median_secs": round(statistics.median(s["secs"])) if s["secs"] else None}
    days = sorted(d for d in days_all if d)
    return {
        "_comment": ("Wheelchair ramp deployments logged by hand on ride.html, by stop, built by "
                     "scripts/build_ramp_log.py. Where riders saw the ramp used, not how often it is used."),
        "generated_on": generated_on,
        "floor": {"deployments": MIN_DEPLOYMENTS, "days": MIN_DAYS},
        "totals": {"deployments": total, "stops": len(stops), "rides": len(rides),
                   "from": days[0] if days else None, "to": days[-1] if days else None},
        "caveats": ["Logged by hand by riders on the buses they rode, so it shows where they saw the ramp used, "
                    "not how often it is used at each stop.",
                    f"A stop is shown only with {MIN_DEPLOYMENTS} or more deployments over {MIN_DAYS} or more days."],
        "stops": published,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rides", required=True, help="a folder of CSVs exported from ride.html")
    ap.add_argument("--out", default=str(ROOT / "data" / "ramp_use.json"))
    args = ap.parse_args()
    result = build(read_rows(Path(args.rides)), date.today().isoformat())
    Path(args.out).write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    t = result["totals"]
    print(f"Wrote {args.out}: {t['deployments']} deployments at {t['stops']} stops; "
          f"{len(result['stops'])} past the floor")
    return 0


if __name__ == "__main__":
    sys.exit(main())
