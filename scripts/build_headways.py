"""Waiting, bunching and punctuality at timing points, from the recorded evidence.

A preview of the measures the Traffic Commissioners use for local buses
(Senior Traffic Commissioner, Statutory Document No. 14, v6.0, January 2024):

- **Frequent services** (a scheduled interval of 10 minutes or less): Excess
  Wait Time, the average wait passengers actually had against the one the
  timetable implies; the hourly tests (at least six departures an hour, no gap
  over 15 minutes); and bunching, buses arriving together.
- **Other services**: departures on time, one minute early to five late, with
  the DfT BUS09 window this repository already uses (up to 5 min 59 s late).

And, for both, whether buses late here were already late near the start of
their journey or lost the time on the way, and how the day's first and last
buses compare with the rest.

Every figure is a count over recorded evidence, with its denominators and the
direction its biases run. Nothing here says a bus was cancelled, or that an
operator is "non-compliant": a journey with no tracked bus may have run without
reporting, and our timing points are GTFS timepoints, not necessarily the
registered principal timing points the Commissioners judge.

    python scripts/build_headways.py --observations inputs/window --out-dir headways
"""

import argparse
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import analysis_exclusions as ax                                  # noqa: E402
import reliability_stats as rs                                    # noqa: E402
from observation_contract import service_origin                   # noqa: E402

# Raise whenever a change would move a published number.
METHOD_VERSION = 1
SCHEMA_VERSION = 1

# The Commissioners' line between frequent and other services: a scheduled
# interval of ten minutes or less.
FREQUENT_MAX_GAP_SECS = 10 * 60
# Their frequent-service tests: six or more departures an hour, no gap over 15 min.
TC_MIN_PER_HOUR = 6
TC_MAX_GAP_SECS = 15 * 60
# One untracked bus turns two gaps into one long one, which inflates waiting
# and fails the gap test. A day's period counts towards those only when nearly
# every scheduled bus is accounted for.
COMPLETE_SHARE = 0.95
# Reports arrive about a minute apart, so "within 60 seconds" cannot be told
# from measurement error. Two minutes, and only where the timetable kept the
# two buses at least six minutes apart.
BUNCH_SECS = 2 * 60
BUNCH_MIN_SCHEDULED_GAP_SECS = 6 * 60
# How late an unbounded or interpolated time might really be, for "possibly
# bunched": about two report intervals.
UNBOUNDED_SECS = 2 * 60
# Late, as Simple view counts it, for the "late from the start" split.
LATE_SECS = 5 * 60
# "Near the start": the journey's first three calls.
START_CALLS = 3
# A cell is offered once it rests on this much.
MIN_DAYS = 5
MIN_JUDGED = 30

# Simple view's times of day (JT_PERIODS in app.js), with its "early & late"
# split in two: one block across the night would hold an eleven-hour gap.
# "any" is the whole service day.
PERIODS = (("early", 0, 8), ("08-10", 8, 10), ("10-16", 10, 16), ("16-18", 16, 18), ("late", 18, 99))

METHOD = (
    "Scheduled passages at GTFS timing points come from each day's recorded timetable; observed "
    "passages from the recorded evidence, measured or, where a bus was seen before and after a stop, "
    "interpolated. Observed passages fall in the period in which they were seen, so a bus matched to "
    "the wrong journey moves no gap. Waiting and the hourly tests are measured over runs of clock hours "
    "with six or more buses due, where the scheduled interval is 10 minutes or less; a period without "
    "them is judged on punctuality. Excess Wait Time pools the eligible days: the mean wait of a passenger "
    "turning up at a random moment in those hours until the next bus, recorded less timetabled, a bus after "
    "the window still ending the waits it ends, using only "
    "day-periods with at least 95% of scheduled passages accounted for, both by journey and by the "
    "buses seen in those hours. The 15-minute gap test skips gaps the timetable itself has. Each visit to "
    "a stop is matched by its place in the route, so a loop's two calls are two passages. Bunching counts "
    "consecutive observed buses no more than 2 minutes apart whichever left first, by their report bounds, "
    "where the timetable had "
    "them at least 6 minutes apart; a pair 'left together' if it was already within 2 minutes at its "
    "first shared earlier timing point. Punctuality counts measured, declared departures without "
    "quality flags, 1 minute early to 5 minutes 59 seconds late."
)

CAVEATS = [
    "A journey with no tracked bus may have run without reporting its position. It is counted as "
    "missing, never as cancelled; incomplete day-periods are left out of waiting and the hourly tests.",
    "Missing buses push Excess Wait Time up and make the 15-minute gap test fail more often, so both "
    "can overstate a problem where coverage is only just complete.",
    "Times are departures from a 150 m stop area, bounded by the next report about a minute later. "
    "Bunching within two minutes is counted as definite only when both bounds agree.",
    "Our timing points are GTFS timepoints, which may differ from the registered principal timing "
    "points the Traffic Commissioners judge. These figures are measured against their published "
    "standard; they are not a compliance finding.",
    "The on-time window follows DfT BUS09 (up to 5 minutes 59 seconds late); the Commissioners' text "
    "says 'more than 5 minutes late'.",
    "Time lost 'on the way' includes stop dwell and holding as well as traffic. 'Late from the start' "
    "needs the journey seen at one of its first three calls; otherwise the start is unknown.",
    "Day types do not separate school holidays; recorded disruptions are marked, not removed.",
]


def period_of(secs):
    """The time of day of a service-day time. Past midnight is still "late"."""
    hour = int(secs) // 3600
    for key, start, end in PERIODS:
        if start <= hour < end:
            return key
    return "late"


def day_type(day):
    """Simple view's day chips: weekdays, or the weekend."""
    return "weekend" if date.fromisoformat(day).weekday() >= 5 else "weekday"


def wait_integral(departures, t0, t1):
    """Waiting summed over everyone turning up at a steady rate in [t0, t1),
    each until the next departure: seconds squared, so divide by the window
    for the mean wait.

    A departure after t1 still ends the waits of those who came before it,
    which is what a late last bus costs them; leaving it out hid exactly the
    gaps this is for. None where no departure at or after t1 ends the window.
    """
    total, t = 0.0, t0
    for d in sorted(departures):
        if d < t0:
            continue
        if t >= t1:
            break
        end = min(d, t1)
        if end > t:
            total += ((d - t) + (d - end)) / 2 * (end - t)
        t = max(t, d)
    return total if t >= t1 else None


def _bounds(row):
    """The departure's earliest and latest possible time, in service-day seconds,
    and whether the latest is measured.

    The recorded time is the last report inside the stop area; the bus left
    before the next report. Interpolated times have no bounds of their own.
    """
    secs = row["observed_secs"]
    interval = row.get("observed_interval_epoch")
    if row.get("estimated") or not interval or row.get("observed_epoch") is None \
            or len(interval) < 2 or interval[1] is None:
        return secs, secs + UNBOUNDED_SECS, False
    return secs, interval[1] + secs - row["observed_epoch"], True


def _scheduled(schedules):
    """Scheduled timing-point passages, `{(day, op, svc, dir, atco): [(secs, trip, call)]}`,
    `call` being the stop's place in the route, so a loop's two visits stay two,
    and each day's first and last journey of every service and direction."""
    passages = defaultdict(list)
    group = {}
    for day, sched in schedules.items():
        patterns, profiles = sched.get("patterns") or {}, sched.get("profiles") or []
        starts = defaultdict(list)
        for trip_id, pattern_id, profile_ref, start, _headsign in sched.get("trips") or []:
            pattern = patterns.get(pattern_id)
            profile = (profiles[profile_ref] if isinstance(profiles, list)
                       else profiles.get(str(profile_ref)))
            if not pattern or not profile:
                continue
            route = (pattern.get("operator") or "", str(pattern.get("service") or ""),
                     pattern.get("direction") or "unknown")
            starts[route].append((start, trip_id))
            for call, (atco, offset, timepoint) in enumerate(
                    zip(pattern["atcos"], profile["offsets"], profile["timepoints"])):
                if timepoint == 1:
                    passages[(day, *route, atco)].append((start + offset, trip_id, call))
        for route, trips in starts.items():
            trips.sort()
            if len(trips) >= 2:
                group[(day, route[0], trips[0][1])] = "first"
                group[(day, route[0], trips[-1][1])] = "last"
    for key in passages:
        passages[key].sort()
    return passages, group


def _observed(rows):
    """Observed timing-point passages and each journey's calls, legacy rows left out."""
    seen = {}
    for row in rows:
        if row.get("observed_secs") is None or row.get("timepoint") != 1:
            continue
        if "legacy_matching_unverified" in (row.get("quality_flags") or []):
            continue
        route = (row.get("operator") or "", str(row.get("service") or ""), row.get("direction") or "unknown")
        lower, upper, bounded = _bounds(row)
        passage = {"secs": row["observed_secs"], "lower": lower, "upper": upper, "bounded": bounded,
                   "estimated": bool(row.get("estimated")), "index": row.get("stop_index", 0),
                   "atco": row["atco"], "lateness": row.get("lateness_secs"),
                   "judged": (not row.get("estimated") and row.get("match") == "declared"
                              and not row.get("quality_flags") and row.get("lateness_secs") is not None)}
        # stop_index is the call's place in the route, as the recorded
        # timetable numbers it: a loop's second visit is its own passage.
        key = (row["day"], *route, row["atco"], row["trip_id"], row.get("stop_index", 0))
        if key not in seen or (seen[key]["estimated"] and not passage["estimated"]):
            seen[key] = passage
    calls = defaultdict(list)
    for (day, op, svc, direction, _atco, trip, _call), passage in seen.items():
        calls[(day, op, svc, direction, trip)].append(passage)
    for journey in calls.values():
        journey.sort(key=lambda p: p["index"])
    return seen, calls


def _start_lateness(journey):
    """Lateness near the start of a journey, or None where it was not seen there."""
    judged = [p for p in journey if p["judged"]]
    return judged[0]["lateness"] if judged and judged[0]["index"] < START_CALLS else None


def _origin(calls, a, b, here_index):
    """Whether two bunched buses were already together earlier on the route."""
    at_a = {p["atco"]: p for p in calls.get(a, []) if p["index"] < here_index}
    shared = [(p["index"], at_a[p["atco"]], p) for p in calls.get(b, [])
              if p["atco"] in at_a and p["index"] < here_index]
    if not shared:
        return "unknown"
    _, pa, pb = min(shared, key=lambda s: s[0])
    if _definitely_within(pa, pb):
        return "left_together"
    if max(pb["lower"] - pa["upper"], pa["lower"] - pb["upper"]) > BUNCH_SECS:
        return "closed_up"
    return "unknown"


def _definitely_within(pa, pb):
    """Both bounded, and within BUNCH_SECS whichever of them really left first."""
    return (pa["bounded"] and pb["bounded"]
            and max(pb["upper"] - pa["lower"], pa["upper"] - pb["lower"]) <= BUNCH_SECS)


def _possibly_within(pa, pb):
    return max(pb["lower"] - pa["upper"], pa["lower"] - pb["upper"]) <= BUNCH_SECS


def _new_cell():
    return {"days": set(), "days_eligible": set(), "days_incomplete": set(), "days_judged": set(),
            "scheduled": 0, "accounted": 0, "exposure": 0, "s_int": 0.0, "o_int": 0.0, "daily_ewt": [],
            "hours_tested": 0, "hours_six_plus": 0, "gaps_tested": 0, "gaps_over_15": 0,
            "bunched_definite": 0, "bunched_possible": 0, "left_together": 0, "closed_up": 0,
            "origin_unknown": 0, "judged": 0, "on_time": 0, "early": 0, "late": 0,
            "late_here": 0, "late_from_start": 0, "late_on_the_way": 0, "late_start_unknown": 0,
            "excluded_by": set(), "days_marked": set()}


def _judge(cell, passage, journey_calls):
    if not passage or not passage["judged"]:
        return
    lateness = passage["lateness"]
    cell["judged"] += 1
    cell[{"early": "early", "on_time": "on_time"}.get(rs.band(lateness), "late")] += 1
    if lateness > LATE_SECS:
        cell["late_here"] += 1
        start = _start_lateness(journey_calls)
        if start is None:
            cell["late_start_unknown"] += 1
        elif start > LATE_SECS:
            cell["late_from_start"] += 1
        else:
            cell["late_on_the_way"] += 1


def frequent_runs(block):
    """The stretches of consecutive clock hours with six or more buses due,
    as `[(first hour, hour after the last)]`.

    Waiting is measured only inside them. Taken over a whole day, the
    timetable's five-hour gap between a midnight bus and the first morning one
    swamps everything, and a passenger is not waiting through it.
    """
    due = Counter(int(s) // 3600 for s, *_ in block)
    runs = []
    for hour in sorted(h for h, n in due.items() if n >= TC_MIN_PER_HOUR):
        if runs and runs[-1][1] == hour:
            runs[-1][1] = hour + 1
        else:
            runs.append([hour, hour + 1])
    return [tuple(r) for r in runs], due


def _in_runs(secs, runs):
    return any(a * 3600 <= secs < b * 3600 for a, b in runs)


def _timetable_gap(sched_day, sa, sb):
    """The longest gap the timetable itself has across [sa, sb]."""
    before = [x for x in sched_day if x <= sa]
    after = [x for x in sched_day if x >= sb]
    points = before[-1:] + [x for x in sched_day if sa < x < sb] + after[:1]
    return max((b - a for a, b in zip(points, points[1:])), default=0)


def _frequent(cell, day, runs, sched_day, obs_day, calls, route, due_at):
    """Waiting, the hourly tests and bunching for one frequent day-period.

    `sched_day` is every scheduled time at the stop that day, `obs_day` every
    passage seen there, by when it was seen: the departures either side of a
    window bound the waits inside it. Returns False, leaving the day out, if
    a window cannot be bounded.
    """
    obs_times = [o[0] for o in obs_day]
    windows = []
    for a, b in runs:
        t0, t1 = a * 3600, b * 3600
        if not any(x >= t1 for x in sched_day):        # the timetable stops inside the run
            t1 = max((x for x in sched_day if x < t1), default=t0)
        if t1 <= t0:
            continue
        s_int, o_int = wait_integral(sched_day, t0, t1), wait_integral(obs_times, t0, t1)
        if s_int is None or o_int is None:
            return False
        windows.append((a, b, t0, t1, s_int, o_int))
    if not windows:
        return False
    cell["days_eligible"].add(day)
    exposure = sum(t1 - t0 for _a, _b, t0, t1, *_ in windows)
    s_total = sum(w[4] for w in windows)
    o_total = sum(w[5] for w in windows)
    cell["exposure"] += exposure
    cell["s_int"] += s_total
    cell["o_int"] += o_total
    cell["daily_ewt"].append((o_total - s_total) / exposure)
    for a, b, t0, t1, *_ in windows:
        for (sa, ta, ca, pa), (sb, tb, cb, pb) in zip(obs_day, obs_day[1:]):
            if not t0 <= sa < t1:
                continue
            if _timetable_gap(sched_day, sa, sb) <= TC_MAX_GAP_SECS:
                cell["gaps_tested"] += 1
                cell["gaps_over_15"] += sb - sa > TC_MAX_GAP_SECS
            due_a, due_b = due_at.get((ta, ca)), due_at.get((tb, cb))
            if due_a is None or due_b is None or abs(due_b - due_a) < BUNCH_MIN_SCHEDULED_GAP_SECS:
                continue
            if not _possibly_within(pa, pb):
                continue
            cell["bunched_possible"] += 1
            if _definitely_within(pa, pb):
                cell["bunched_definite"] += 1
                kind = _origin(calls, (day, *route, ta), (day, *route, tb), min(pa["index"], pb["index"]))
                cell[{"left_together": "left_together", "closed_up": "closed_up"}.get(kind, "origin_unknown")] += 1
        for hour in range(a, b):
            cell["hours_tested"] += 1
            cell["hours_six_plus"] += sum(1 for x in obs_times if int(x) // 3600 == hour) >= TC_MIN_PER_HOUR
    return True


def _cell_out(key, c):
    _op, _svc, direction, atco, dtype, period, kind = key
    out = {"atco": atco, "direction": direction, "day_type": dtype, "period": period, "kind": kind,
           "days": len(c["days"]), "days_eligible": len(c["days_eligible"]),
           "days_incomplete": len(c["days_incomplete"]),
           "scheduled_passages": c["scheduled"], "accounted_passages": c["accounted"]}
    if kind == "frequent":
        swt = c["s_int"] / c["exposure"] if c["exposure"] else None
        awt = c["o_int"] / c["exposure"] if c["exposure"] else None
        out.update({
            "swt_secs": round(swt) if swt is not None else None,
            "awt_secs": round(awt) if awt is not None else None,
            "ewt_secs": round(awt - swt) if swt is not None and awt is not None else None,
            "ewt_daily_median_secs": round(statistics.median(c["daily_ewt"])) if c["daily_ewt"] else None,
            **{k: c[k] for k in ("hours_tested", "hours_six_plus", "gaps_tested", "gaps_over_15",
                                 "bunched_definite", "bunched_possible", "left_together", "closed_up",
                                 "origin_unknown")}})
    out.update({k: c[k] for k in ("judged", "on_time", "early", "late", "late_here", "late_from_start",
                                  "late_on_the_way", "late_start_unknown")})
    out["sample_sufficient"] = (len(c["days_eligible"]) >= MIN_DAYS if kind == "frequent"
                                else c["judged"] >= MIN_JUDGED and len(c["days_judged"]) >= MIN_DAYS)
    if c["excluded_by"]:
        out["excluded_by"] = sorted(c["excluded_by"])
        out["days_marked"] = len(c["days_marked"])
    return out


def build(rows, meta, exclusions=(), as_of=None):
    """`{(operator, service): document}` for every service with timing-point cells."""
    passages, group = _scheduled(meta.get("schedules") or {})
    seen, calls = _observed(rows)
    cells = defaultdict(_new_cell)
    names = defaultdict(dict)
    for row in rows:
        names[(row.get("operator") or "", str(row.get("service") or ""))].setdefault(row["atco"], row.get("stop_name", ""))

    at_stop = defaultdict(list)
    for (day, op, svc, direction, atco, trip, call), passage in seen.items():
        at_stop[(day, op, svc, direction, atco)].append((passage["secs"], trip, call, passage))

    for key, scheduled in passages.items():
        day, op, svc, direction, atco = key
        due_at = {(trip, call): secs for secs, trip, call in scheduled}
        sched_day = [secs for secs, *_ in scheduled]
        obs_day = sorted(at_stop.get(key, []), key=lambda x: x[0])
        by_period, seen_in = defaultdict(list), defaultdict(list)
        for passage in scheduled:
            by_period[period_of(passage[0])].append(passage)
            by_period["any"].append(passage)
        for passage in obs_day:
            seen_in[period_of(passage[0])].append(passage)
            seen_in["any"].append(passage)
        for period, block in by_period.items():
            found = [(secs, trip, seen.get((*key, trip, call))) for secs, trip, call in block]
            runs, due = frequent_runs(block)
            in_runs = [x for x in block if _in_runs(x[0], runs)]
            run_gaps = [b[0] - a[0] for a, b in zip(in_runs, in_runs[1:])
                        if any(x * 3600 <= a[0] and b[0] < y * 3600 for x, y in runs)]
            frequent = bool(run_gaps) and statistics.median(run_gaps) <= FREQUENT_MAX_GAP_SECS
            cell = cells[(op, svc, direction, atco, day_type(day), period,
                          "frequent" if frequent else "non_frequent")]
            accounted = sum(1 for *_, p in found if p)
            cell["days"].add(day)
            cell["scheduled"] += len(block)
            cell["accounted"] += accounted
            if exclusions:
                origin = service_origin(day)
                marks = ax.affecting(exclusions, op, svc,
                                     datetime.fromtimestamp(origin + block[0][0], timezone.utc),
                                     stop_ids=[atco],
                                     until=datetime.fromtimestamp(origin + block[-1][0], timezone.utc))
                if marks:
                    cell["excluded_by"].update(marks)
                    cell["days_marked"].add(day)
            for _secs, trip, passage in found:
                _judge(cell, passage, calls.get((day, op, svc, direction, trip), []))
                if passage and passage["judged"]:
                    cell["days_judged"].add(day)
            if not frequent:
                continue
            observed = [o for o in seen_in.get(period, []) if _in_runs(o[0], runs)]
            tracked = sum(1 for _secs, trip, call in in_runs if seen.get((*key, trip, call)))
            if min(tracked, len(observed)) < COMPLETE_SHARE * len(in_runs) or \
                    not _frequent(cell, day, runs, sched_day, obs_day, calls, (op, svc, direction), due_at):
                cell["days_incomplete"].add(day)

    # The day's first and last buses against the rest: punctuality over all
    # their timing points, and how many were tracked at all.
    groups = defaultdict(lambda: {"scheduled": 0, "tracked": 0, "judged": 0, "on_time": 0, "early": 0, "late": 0})
    trips = {}
    for (day, op, svc, direction, _atco), scheduled in passages.items():
        for _secs, trip, _call in scheduled:
            trips[(day, op, trip)] = (svc, direction)
    for (day, op, trip), (svc, direction) in trips.items():
        g = groups[(op, svc, direction, day_type(day), group.get((day, op, trip), "day"))]
        g["scheduled"] += 1
        journey = calls.get((day, op, svc, direction, trip), [])
        g["tracked"] += bool(journey)
        for passage in journey:
            if passage["judged"]:
                g["judged"] += 1
                g[{"early": "early", "on_time": "on_time"}.get(rs.band(passage["lateness"]), "late")] += 1

    documents = {}
    for key, c in sorted(cells.items()):
        documents.setdefault(key[:2], {"cells": [], "groups": []})["cells"].append(_cell_out(key, c))
    for (op, svc, direction, dtype, kind), g in sorted(groups.items()):
        if (op, svc) in documents:
            documents[(op, svc)]["groups"].append({"direction": direction, "day_type": dtype, "group": kind, **g})

    days = sorted({key[0] for key in passages})
    for (op, svc), doc in documents.items():
        doc.update({
            "schema_version": SCHEMA_VERSION, "service": svc, "operator": op,
            "method": METHOD, "method_version": METHOD_VERSION, "caveats": CAVEATS,
            "as_of": as_of or datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "data_versions": meta.get("data_versions", []),
            "source_method_versions": [str(v) for v in meta.get("method_versions", [])],
            "days": days, "days_without_timetable": sorted(set(meta.get("days", [])) - set(days)),
            "floors": {"days": MIN_DAYS, "judged": MIN_JUDGED, "complete_share": COMPLETE_SHARE},
            "thresholds": {"frequent_max_gap_secs": FREQUENT_MAX_GAP_SECS, "bunch_secs": BUNCH_SECS,
                           "bunch_min_scheduled_gap_secs": BUNCH_MIN_SCHEDULED_GAP_SECS,
                           "tc_min_per_hour": TC_MIN_PER_HOUR, "tc_max_gap_secs": TC_MAX_GAP_SECS,
                           "late_secs": LATE_SECS, "on_time_from_secs": rs.ON_TIME_FROM_SECS,
                           "on_time_to_secs": rs.ON_TIME_TO_SECS, "start_calls": START_CALLS},
            "stops": {atco: names[(op, svc)].get(atco, "") for atco in sorted({c["atco"] for c in doc["cells"]})},
        })
    return documents


def file_name(service, operator):
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in f"{service}-{operator}")
    return f"headways-{safe}.json"


def main(argv=None):
    from query_reliability import load_observations
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--observations", nargs="+", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args(argv)
    # Compact: the source reports each row carries are the bulk of a day, and
    # nothing here reads them. A 35-day window would otherwise need gigabytes.
    rows, meta = load_observations(args.observations, compact=True,
                                   row_filter=lambda row: row.get("timepoint") == 1, with_schedules=True)
    documents = build(rows, meta, exclusions=ax.load())
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for (op, svc), doc in sorted(documents.items()):
        (out / file_name(svc, op)).write_text(json.dumps(doc, separators=(",", ":"), sort_keys=True) + "\n")
    cells = sum(len(d["cells"]) for d in documents.values())
    ready = sum(c["sample_sufficient"] for d in documents.values() for c in d["cells"])
    print(f"{len(documents)} services, {cells} cells, {ready} at the floor → {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
