#!/usr/bin/env python3
"""Write detected-diversions.json: stops the buses have stopped passing.

Diversions are recorded by hand from the operators' notices
(data/disruptions.json). Some are never announced, or are announced late. This
finds them in the buses' own recorded positions, nightly, and says so plainly
as detected, not confirmed (asked for on 6 November 2026; design in
docs/POST_EXPO_ROADMAP.md).

The measure, for each operator, route, direction and stop: the share of the
day's journeys actually seen at the stop (a position within the stop's radius),
against those only filled in between sightings (`estimated`, flagged
interpolated). A bus sampled once a minute misses some stops on an ordinary
day, so each stop is compared with its own usual share, not with 100%.

A stop is a candidate when, over the earlier days of the window, at least
USUAL_SHARE of at least USUAL_JOURNEYS journeys were seen there, and on each of
the last RECENT_DAYS days at most RECENT_SHARE of at least RECENT_JOURNEYS
were. Neighbouring candidates on a route make one stretch. Nothing here is a
recorded diversion until a person confirms it: each new detection becomes a
notice for the maintainer (notify_warnings.py), and
`scripts/add_disruption.py --from-detected <id>` drafts the entry.

    python scripts/build_detected_diversions.py --observations inputs/window
        [--out detected-diversions.json] [--notices notices.tsv] [--previous-index previous-index.json]
"""

import argparse
import hashlib
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

USUAL_SHARE = 0.6
USUAL_JOURNEYS = 20
RECENT_SHARE = 0.15
RECENT_JOURNEYS = 6
RECENT_DAYS = 2
METHOD = ("For each operator, route, direction and stop, the share of the day's journeys seen at the stop in their "
          f"recorded positions. A stop where at least {round(USUAL_SHARE * 100)}% of {USUAL_JOURNEYS} or more journeys "
          f"were seen on the earlier days of the window, and at most {round(RECENT_SHARE * 100)}% of {RECENT_JOURNEYS} or "
          f"more on each of the last {RECENT_DAYS} days, is a candidate; neighbouring candidates make one stretch.")
CAVEATS = [
    "Detected from the buses' own positions, not confirmed by the operator.",
    "Positions are recorded once a minute, so a stop passed quickly can be missed on an ordinary day; each stop is "
    "compared with its own usual share.",
    "A stretch at the very end of a route can be missed, since nothing is filled in after the last sighting.",
]


def _seen(row) -> bool:
    return not row.get("estimated") and "interpolated" not in (row.get("quality_flags") or [])


def detect(rows):
    """(The detected stretches, the days the window holds.)"""
    days = sorted({r["day"] for r in rows if r.get("day")})
    if len(days) <= RECENT_DAYS:
        return [], days
    recent, earlier = days[-RECENT_DAYS:], days[:-RECENT_DAYS]
    stats = {}
    for r in rows:
        if not r.get("atco") or not r.get("service") or not r.get("day"):
            continue
        key = (r.get("operator", ""), r["service"], r.get("direction") or "", r["atco"])
        d = stats.setdefault(key, {"days": {}, "name": r.get("stop_name", ""), "index": []})
        x = d["days"].setdefault(r["day"], [0, 0])
        x[0] += _seen(r)
        x[1] += 1
        d["index"].append(r.get("stop_index") or 0)
    flagged = {}
    for key, d in stats.items():
        base_seen = sum(d["days"].get(day, [0, 0])[0] for day in earlier)
        base_total = sum(d["days"].get(day, [0, 0])[1] for day in earlier)
        if base_total < USUAL_JOURNEYS or base_seen / base_total < USUAL_SHARE:
            continue
        shares = []
        for day in recent:
            seen, total = d["days"].get(day, [0, 0])
            if total < RECENT_JOURNEYS or seen / total > RECENT_SHARE:
                break
            shares.append((seen, total))
        else:
            # When it began: walking back, the first day that was already low.
            first = recent[0]
            for day in reversed(earlier):
                seen, total = d["days"].get(day, [0, 0])
                if total >= RECENT_JOURNEYS and seen / total <= RECENT_SHARE:
                    first = day
                else:
                    break
            flagged[key] = {"usual": base_seen / base_total,
                            "recent": sum(s for s, _ in shares) / sum(t for _, t in shares),
                            "journeys": sum(t for _, t in shares), "first_seen": first}
    # Neighbouring flagged stops, in the route's own order, make one stretch.
    out = []
    routes = {}
    for key, d in stats.items():
        routes.setdefault(key[:3], []).append((statistics.median(d["index"]), key))
    for route, stops in sorted(routes.items()):
        stops.sort()
        run = []
        for _, key in stops + [(None, None)]:
            if key in flagged:
                run.append(key)
                continue
            if run:
                out.append(_stretch(route, run, stats, flagged, recent[-1]))
                run = []
    return out, days


def _stretch(route, keys, stats, flagged, last_day):
    operator, service, direction = route
    f = [flagged[k] for k in keys]
    first = min(x["first_seen"] for x in f)
    ident = hashlib.sha1(f"{operator}|{service}|{direction}|{keys[0][3]}|{first}".encode()).hexdigest()[:12]
    usual = min(x["usual"] for x in f)
    recent = max(x["recent"] for x in f)
    return {
        "id": ident,
        "operator": operator, "service": service, "direction": direction,
        "stops": [{"atco": k[3], "name": stats[k]["name"]} for k in keys],
        "first_seen": first, "last_seen": last_day,
        "journeys": min(x["journeys"] for x in f),
        "usual_share": round(usual, 2), "recent_share": round(recent, 2),
        # Two or more stops in a row, none seen at all, against a usual share
        # of four in five: as clear as this measure gets.
        "confidence": "high" if len(keys) >= 2 and recent == 0 and usual >= 0.8 else "medium",
    }


def notice_lines(found, previous_ids):
    """Lines for notify_warnings.py: a warning per detection (the same key each
    night, so its issue is kept up to date, not opened again), and a clear for
    each of last night's that has gone, so its issue closes itself."""
    current = {d["id"] for d in found}
    lines = [f"detected-diversion:{i}\tclear\n" for i in sorted(set(previous_ids) - current)]
    for d in found:
        names = ", ".join(s["name"] for s in d["stops"][:4]) + ("…" if len(d["stops"]) > 4 else "")
        lines.append(f"detected-diversion:{d['id']}\twarn\tThe {d['service']} ({d['operator']}, "
                     f"{d['direction'] or 'one way'}) has passed {len(d['stops'])} stop"
                     f"{'s' if len(d['stops']) != 1 else ''} it usually serves ({names}) on almost no journeys "
                     f"since {d['first_seen']}. Draft it with `scripts/add_disruption.py --from-detected {d['id']}`.\n")
    return lines


def previous_ids(index_path: str, base: str) -> set:
    """Last night's detections, from the published build the previous index
    names. Best effort: without them nothing is cleared tonight, only later."""
    try:
        index = json.loads(Path(index_path).read_text(encoding="utf-8"))
        art = (index.get("artifacts") or {}).get("detected-diversions.json")
        if not art:
            return set()
        from published_fetch import read_published
        doc = json.loads(read_published(f"{base}/{art['file']}").decode("utf-8"))
        return {d["id"] for d in doc.get("detections", [])}
    except Exception:                                   # noqa: BLE001
        return set()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--observations", nargs="+", required=True)
    ap.add_argument("--out", default="detected-diversions.json")
    ap.add_argument("--notices", help="append a line per new detection for notify_warnings.py")
    ap.add_argument("--previous-index", help="the previous published index, to clear detections that have gone")
    ap.add_argument("--published", default="https://adur-worthing-submissions.dennislemennace.workers.dev/journey-times")
    args = ap.parse_args()
    from query_reliability import load_observations
    rows, _meta = load_observations(args.observations)
    found, days = detect(rows)
    doc = {"as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "window": {"from": days[0] if days else None, "to": days[-1] if days else None, "days": len(days)},
           "method": METHOD, "caveats": CAVEATS, "detections": found}
    Path(args.out).write_text(json.dumps(doc, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    if args.notices:
        before = previous_ids(args.previous_index, args.published) if args.previous_index else set()
        with open(args.notices, "a", encoding="utf-8") as fh:
            fh.writelines(notice_lines(found, before))
    print(f"Wrote {args.out}: {len(found)} detected stretch{'es' if len(found) != 1 else ''} over {len(days)} days")
    return 0


if __name__ == "__main__":
    sys.exit(main())
