"""One route's performance, in one file a resident, councillor or officer can read.

Brings together, for each service and operator over the reliability window,
what the site measures in separate places, each with its denominator:

- the period, and the timetabled journeys against those tracked and how each
  was identified (identification.json);
- punctuality at timing points, by direction and day type, against the Traffic
  Commissioners' 95% expectation, with the spread from day to day so one bad
  day cannot pass for the usual;
- the same by time of day, and week by week;
- waiting at the main timing point of frequent services (headways files);
- where buses lose most time (delay-map files);
- the recorded disruptions it ran through;
- the method, versions, caveats and the observation files behind it.

Counts, not shares: the page divides, with the denominator beside each figure.
A row below the floor says how thin it is rather than giving a figure.

    python scripts/build_route_reports.py --observations inputs/window \\
        --identification identification.json --headways-dir headways \\
        --map-dir hotspot-map --out-dir route-reports
"""

import argparse
import json
import statistics
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import analysis_exclusions as ax                                  # noqa: E402
import reliability_stats as rs                                    # noqa: E402
from build_headways import file_name as headways_file             # noqa: E402
from build_identification import FAMILIES                         # noqa: E402
from build_journey_times import document_name                     # noqa: E402

SCHEMA_VERSION = 1
# Raise whenever a change would move a published number.
METHOD_VERSION = 1
MIN_JUDGED, MIN_DAYS = 30, 5
# A day's on-time share joins the day-to-day spread only on this many timings.
MIN_DAILY = 10
STANDARD = 0.95
PERIODS = (("08-10", 8, 10), ("10-16", 10, 16), ("16-18", 16, 18))
BOTTLENECKS_PER_DIRECTION = 3

METHOD = (
    "Punctuality counts departures from GTFS timing points by journeys identified exactly (declared by "
    "journey ID or by line, first stop and start time), measured rather than interpolated and without "
    "quality flags. On time is 1 minute early to 5 minutes 59 seconds late (DfT BUS09), very late is "
    "more than 15 minutes. Day type and time of day are the scheduled departure's. The day-to-day spread "
    "is the on-time share of each day with at least 10 timings. Waiting is the headways builder's Excess "
    "Wait Time at the direction's busiest timing point; where buses lose time is the delay map's median "
    "gain over each timing-point stretch, from cells that pass its floor, latest timetable only."
)

CAVEATS = [
    "Stop times are inferred from the buses' own location reports: the last report within 150 m of a "
    "stop, about a minute apart. They have not yet been checked against times someone saw.",
    "A bus that waits at a timing point is timed leaving it, so departures can read later than arrivals.",
    "A timetabled journey with no tracked bus may have run without reporting. It is missing evidence, "
    "never counted as cancelled.",
    "Our timing points are GTFS timepoints, which may differ from the registered points the Traffic "
    "Commissioners judge. Figures are measured against their published standard, not a compliance finding.",
    "Recorded disruptions are marked, not removed: the share of timings on disrupted journeys is given.",
    "Timings of one journey are not independent: a few bad journeys move a row more than its count suggests.",
]


def operator_of(code):
    return FAMILIES.get(code, code or "?")


def judged(row):
    return (row.get("timepoint") == 1 and not row.get("estimated") and row.get("match") == "declared"
            and not row.get("quality_flags") and row.get("lateness_secs") is not None)


def day_type(day):
    return "weekday" if date.fromisoformat(day).weekday() < 5 else "weekend"


def period_of(row):
    hour = int(rs.scheduled_hour(row)) % 24
    for key, lo, hi in PERIODS:
        if lo <= hour < hi:
            return key
    return "other"


def _counts():
    return {"judged": 0, "on_time": 0, "early": 0, "late": 0, "very_late": 0, "days": set()}


def _add(c, row):
    c["judged"] += 1
    c[rs.band(int(row["lateness_secs"]))] += 1
    c["days"].add(row["day"])


def _out(c):
    days = len(c["days"])
    return {**{k: c[k] for k in ("judged", "on_time", "early", "late", "very_late")}, "days": days,
            "sample_sufficient": c["judged"] >= MIN_JUDGED and days >= MIN_DAYS}


def _week(day):
    y, w, _ = date.fromisoformat(day).isocalendar()
    return f"{y}-W{w:02d}"


def punctuality(rows, disrupted):
    """Punctuality blocks, and the weekly trend, for one service's timing-point rows."""
    main, period, weekly = defaultdict(_counts), defaultdict(_counts), defaultdict(_counts)
    clear = defaultdict(_counts)
    daily = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    late_by_week = defaultdict(list)
    marked = 0
    for row in rows:
        if not judged(row):
            continue
        dtype = day_type(row["day"])
        key = (row.get("direction") or "unknown", dtype)
        _add(main[key], row)
        if dtype == "weekday":
            _add(period[(key[0], period_of(row))], row)
        _add(weekly[_week(row["day"])], row)
        late_by_week[_week(row["day"])].append(int(row["lateness_secs"]))
        cell = daily[key][row["day"]]
        cell[0] += 1
        cell[1] += rs.band(int(row["lateness_secs"])) == "on_time"
        if (row["day"], row.get("trip_id")) in disrupted:
            marked += 1
        else:
            _add(clear[key], row)
    rows_out = []
    for (direction, dtype), c in sorted(main.items()):
        shares = sorted(on / n for n, on in daily[(direction, dtype)].values() if n >= MIN_DAILY)
        rows_out.append({"direction": direction, "day_type": dtype, **_out(c),
                         "excluding_disruptions": _out(clear[(direction, dtype)]),
                         "daily": ({"days": len(shares), "min": round(shares[0], 3),
                                    "median": round(statistics.median(shares), 3),
                                    "max": round(shares[-1], 3)} if shares else None)})
    total = sum(c["judged"] for c in main.values())
    return {
        "definition": {"on_time_from_secs": rs.ON_TIME_FROM_SECS, "on_time_to_secs": rs.ON_TIME_TO_SECS,
                       "very_late_secs": rs.VERY_LATE_SECS, "standard": STANDARD},
        "rows": rows_out,
        "by_period": [{"direction": d, "day_type": "weekday", "period": p, **_out(c)}
                      for (d, p), c in sorted(period.items())],
        "disrupted_timings": marked, "timings": total,
    }, [{"week": wk, **_out(c), "median_lateness_secs": round(statistics.median(late_by_week[wk]))}
        for wk, c in sorted(weekly.items())]


def waits(headways, names):
    """Excess waiting at each direction's busiest frequent timing point."""
    if not headways:
        return []
    best = {}
    for cell in headways.get("cells") or []:
        if cell.get("kind") != "frequent" or cell.get("period") != "any":
            continue
        key = (cell.get("direction"), cell.get("day_type"))
        if key not in best or cell.get("scheduled_passages", 0) > best[key].get("scheduled_passages", 0):
            best[key] = cell
    out = []
    for (direction, dtype), cell in sorted(best.items(), key=lambda kv: [str(x) for x in kv[0]]):
        out.append({"direction": direction, "day_type": dtype, "atco": cell.get("atco"),
                    "stop_name": names.get(cell.get("atco"), ""),
                    **{k: cell.get(k) for k in ("swt_secs", "awt_secs", "ewt_secs", "days_eligible",
                                                "days", "hours_tested", "hours_six_plus", "gaps_tested",
                                                "gaps_over_15", "bunched_definite", "sample_sufficient")}})
    return out


def bottlenecks(delay_map):
    """The stretches where buses lose most time, per direction, from cells at
    the map's floor in its latest timetable."""
    if not delay_map:
        return []
    stretches = {s["id"]: s for s in delay_map.get("stretches") or []}
    best = {}
    for cell in delay_map.get("cells") or []:
        if not cell.get("sample_sufficient") or not cell.get("latest") or cell.get("median_gained_secs") is None:
            continue
        s = stretches.get(cell.get("stretch"))
        if not s:
            continue
        if cell["stretch"] not in best or cell["median_gained_secs"] > best[cell["stretch"]][1]["median_gained_secs"]:
            best[cell["stretch"]] = (s, cell)
    by_direction = defaultdict(list)
    for s, cell in best.values():
        by_direction[s.get("direction", "unknown")].append({
            "direction": s.get("direction", "unknown"), "from_name": s.get("from_name", ""),
            "to_name": s.get("to_name", ""), "day_type": cell.get("day_type"), "period": cell.get("period"),
            "hour": cell.get("hour"),
            "median_gained_secs": cell["median_gained_secs"], "journeys": cell.get("journeys"),
            "distinct_days": cell.get("distinct_days")})
    out = []
    for direction in sorted(by_direction):
        out += sorted(by_direction[direction], key=lambda b: -b["median_gained_secs"])[:BOTTLENECKS_PER_DIRECTION]
    return out


def build(rows, meta, identification=None, headways_dir=None, map_dir=None, exclusions=(), coords=None,
          as_of=None):
    """`{(operator, service): document}`."""
    coords = coords or {}
    exclusions = list(exclusions)
    by_service = defaultdict(list)
    names = {}
    for row in rows:
        by_service[(operator_of(row.get("operator")), str(row.get("service") or "?"))].append(row)
        if row.get("atco") and row.get("stop_name"):
            names[row["atco"]] = row["stop_name"]
    as_of = as_of or datetime.now(timezone.utc).isoformat(timespec="seconds")
    sources = [{"day": s.get("day"), "sha256": s.get("sha256")} for s in meta.get("sources") or []]
    by_id = {e["id"]: e for e in exclusions}
    out = {}
    for (op, svc), svc_rows in sorted(by_service.items()):
        journeys = defaultdict(list)
        for row in svc_rows:
            journeys[(row["day"], row.get("trip_id"))].append(row)
        disrupted, events = set(), defaultdict(set)
        for key, calls in journeys.items():
            seen = [c["observed_epoch"] for c in calls if c.get("observed_epoch")]
            if not seen:
                continue
            when = datetime.fromtimestamp(min(seen), timezone.utc)
            atcos = [c.get("atco") for c in calls]
            hits = ax.affecting(exclusions, op, svc, when, atcos, [coords[a] for a in atcos if a in coords])
            if hits:
                disrupted.add(key)
                for h in hits:
                    events[h].add(key)
        punct, trend = punctuality(svc_rows, disrupted)
        if not punct["timings"]:
            continue
        hw = _read(Path(headways_dir) / headways_file(svc, op)) if headways_dir else None
        dm = _read(Path(map_dir) / f"hotspot-map-{document_name(svc, op)}.json") if map_dir else None
        days = sorted({r["day"] for r in svc_rows})
        # The window is the recorded days; a file's small-hours rows belong to
        # the day before it, which is not a day the window recorded.
        window = meta.get("days") or days
        out[(op, svc)] = {
            "schema_version": SCHEMA_VERSION, "method_version": METHOD_VERSION, "as_of": as_of,
            "service": svc, "operator": op, "days": days, "window": [window[0], window[-1]],
            "identification": ((identification or {}).get("services") or {}).get(f"{svc}|{op}"),
            "punctuality": punct, "trend": trend,
            "waits": waits(hw, names), "bottlenecks": bottlenecks(dm),
            "disruptions": [{"id": eid,
                             "label": by_id.get(eid, {}).get("label") or by_id.get(eid, {}).get("reason") or eid,
                             "from": by_id.get(eid, {}).get("from"), "to": by_id.get(eid, {}).get("to"),
                             "journeys_marked": len(keys)} for eid, keys in sorted(events.items())],
            "floor": {"judged": MIN_JUDGED, "distinct_days": MIN_DAYS, "daily_timings": MIN_DAILY},
            "method": METHOD, "caveats": CAVEATS,
            "data_versions": meta.get("data_versions") or [], "method_versions": meta.get("method_versions") or [],
            "sources": sources,
            "inputs": {"identification_as_of": (identification or {}).get("as_of"),
                       "headways_as_of": (hw or {}).get("as_of"), "delay_map_as_of": (dm or {}).get("as_of")},
        }
    return out


def _read(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None


def file_name(service, operator):
    return f"route-report-{document_name(service, operator)}.json"


def main(argv=None):
    from query_reliability import load_observations
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--observations", nargs="+", required=True)
    ap.add_argument("--identification")
    ap.add_argument("--headways-dir")
    ap.add_argument("--map-dir")
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args(argv)
    rows, meta = load_observations(args.observations, compact=True,
                                   row_filter=lambda row: row.get("timepoint") == 1)
    ident = _read(args.identification) if args.identification else None
    docs = build(rows, meta, ident, args.headways_dir, args.map_dir, ax.load(), ax.stop_coords())
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for (op, svc), doc in docs.items():
        (out / file_name(svc, op)).write_text(json.dumps(doc, separators=(",", ":"), sort_keys=True) + "\n")
    print(f"{len(docs)} route reports → {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
