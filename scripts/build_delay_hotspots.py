"""Prepare auditable section traversals; do not infer traffic causation.

Only adjacent, measured timing points with bounded report evidence enter the
default declared-identity cohort. Legacy data remains explicitly excluded.
"""
import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import statistics

import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from observation_contract import LONDON, row_identity


def entry_period(epoch):
    """The time-of-day group a traversal falls in: the journey-time view's own
    chips, the rush hours 08-10 and 16-18 (narrowed from 07-10 and 16-19 on
    24 September 2026), daytime between them, and early and late the rest.
    tests/test_delay_hotspots.py holds the two to the same hours."""
    hour = datetime.fromtimestamp(epoch, LONDON).hour
    return "08-10" if 8 <= hour < 10 else "10-16" if 10 <= hour < 16 else "16-18" if 16 <= hour < 18 else "other"


def quality_reason(row, max_interval_secs):
    if not isinstance(row.get("method_version"), int) or row["method_version"] < 4:
        return "legacy_or_unknown_method"
    if row.get("estimated") or row.get("timepoint") != 1:
        return "not_measured_timing_point"
    if row.get("quality_flags"):
        return "flagged_measurement"
    if row.get("match") != "declared":
        return "inferred_or_mixed_identity"
    if not row.get("source_reports") or not row.get("source_file_sha256"):
        return "missing_source_evidence"
    if not all(row.get(k) is not None for k in ("scheduled_epoch", "observed_epoch", "route_pattern", "timing_point_order")):
        return "missing_time_or_pattern_identity"
    interval = row.get("observed_interval_epoch")
    if not interval or len(interval) != 2 or any(x is None for x in interval):
        return "unbounded_report_interval"
    if not 0 <= interval[1] - interval[0] <= max_interval_secs or interval[0] != row["observed_epoch"]:
        return "invalid_or_wide_report_interval"
    return None


def entry_hour(epoch):
    return datetime.fromtimestamp(epoch, LONDON).hour


# One stretch of one route pattern: which operator's bus, which way, between
# which two adjacent timing points.
STRETCH = ("operator", "service", "route_pattern", "direction", "from_sequence", "to_sequence")

_CELL_BASE = ("operator", "service", "route_pattern", "direction", "from_atco", "to_atco",
              "from_sequence", "to_sequence", "schedule_era", "method_version", "match", "day_type")
PERIOD_DIMENSIONS = _CELL_BASE + ("period",)
HOUR_DIMENSIONS = _CELL_BASE + ("hour",)


def schedule_eras(traversals):
    """Which weekly timetable builds may be pooled, stretch by stretch.

    Grouping by build kept like with like, but the timetable is rebuilt every
    week, so no stretch could ever pool more than one week and most would never
    reach the floor. Builds are pooled where the promise is demonstrably the
    same: two builds share an era for a stretch only if, for every departure
    they both schedule, they schedule the same section time. A departure only
    one of them has — a Friday extra, a new journey — neither joins nor splits
    them; a changed running time does split them, which is exactly the case
    where pooling would compare two different promises.

    A build joins an era only if it agrees with every member, not just one:
    agreement is not transitive, and chaining A~B~C would pool A with a C it
    contradicts. Builds are taken oldest first, so the answer is deterministic.
    Two builds with no departure in common cannot be shown to agree, and stay
    apart.
    """
    promises = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    first_day = {}
    for leg in traversals:
        stretch = tuple(leg[k] for k in STRETCH)
        version = leg["data_version"]
        promises[stretch][version][(leg["day_type"], leg["scheduled_depart_secs"])].add(leg["scheduled_secs"])
        first_day[version] = min(first_day.get(version, leg["day"]), leg["day"])

    def agree(a, b):
        shared = a.keys() & b.keys()
        return bool(shared) and all(a[k] == b[k] for k in shared)

    eras = {}
    for stretch, versions in promises.items():
        groups = []
        for version in sorted(versions, key=lambda v: (first_day[v], str(v))):
            home = next((g for g in groups
                         if all(agree(versions[version], versions[m]) for m in g)), None)
            if home is None:
                groups.append([version])
            else:
                home.append(version)
        for members in groups:
            era = hashlib.sha256(json.dumps(sorted(members, key=str)).encode()).hexdigest()[:16]
            for version in members:
                eras[stretch + (version,)] = era
    return eras


def summarise_cells(traversals, dimensions, min_journeys, min_days, resolution):
    groups = defaultdict(list)
    for leg in traversals:
        groups[tuple(leg[k] for k in dimensions)].append(leg)
    cells = []
    for key, legs in sorted(groups.items(), key=lambda item: str(item[0])):
        gains = sorted(leg["gained_secs"] for leg in legs)
        days = sorted({leg["day"] for leg in legs})
        count = len({tuple(leg["journey_id"]) for leg in legs})
        sufficient = count >= min_journeys and len(days) >= min_days
        cells.append({**dict(zip(dimensions, key)), "resolution": resolution,
                      "id": hashlib.sha256(json.dumps([resolution, *key]).encode()).hexdigest(),
                      "from_name": legs[0]["from_name"], "to_name": legs[0]["to_name"],
                      "data_versions": sorted({leg["data_version"] for leg in legs}, key=str),
                      "traversals": len(legs), "journeys": count, "distinct_days": len(days),
                      "days": days, "median_gained_secs": statistics.median(gains),
                      "p90_gained_secs": gains[min(len(gains)-1, int(len(gains)*.9))] if sufficient else None,
                      "at_least_600s": sum(g >= 600 for g in gains),
                      "at_least_600s_even_at_interval_lower_bound": sum(l["gain_interval_secs"][0] >= 600 for l in legs),
                      "daily": {d: {"traversals": sum(l["day"] == d for l in legs),
                                    "at_least_600s": sum(l["day"] == d and l["gained_secs"] >= 600 for l in legs)} for d in days},
                      "publishable": False, "sample_sufficient": sufficient,
                      "status": "sample_sufficient_preview" if sufficient else "insufficient_data",
                      "scheduled_traversals": None, "coverage_status": "not_established_for_observed_entry_cohort"})
    return cells


def build_hotspots(rows, meta, min_journeys=30, min_days=5, max_interval_secs=180):
    journeys, excluded, traversals = defaultdict(list), Counter(), []
    for row in rows:
        if row.get("timepoint") == 1:
            # Keep invalid endpoints in order, so excluding B never invents A→C.
            journeys[row_identity(row)[:3]].append(row)
    for identity, calls in journeys.items():
        calls.sort(key=lambda r: r.get("stop_index", -1))
        if len({r.get("stop_index") for r in calls}) != len(calls):
            excluded["journey_has_duplicate_call_identity"] += max(1, len(calls)-1)
            continue
        for first, second in zip(calls, calls[1:]):
            reason = quality_reason(first, max_interval_secs) or quality_reason(second, max_interval_secs)
            if reason:
                excluded[reason] += 1
                continue
            if second["timing_point_order"] != first["timing_point_order"] + 1:
                excluded["missing_intermediate_timing_point"] += 1
                continue
            if any(first.get(k) != second.get(k) for k in ("operator", "service", "route_pattern", "vehicle", "direction", "data_version", "method_version")):
                excluded["inconsistent_traversal_identity"] += 1
                continue
            observed = second["observed_epoch"] - first["observed_epoch"]
            scheduled = second["scheduled_epoch"] - first["scheduled_epoch"]
            if observed <= 0 or scheduled <= 0 or observed > 12 * 3600:
                excluded["invalid_elapsed_time"] += 1
                continue
            if any(r["observed_epoch"] - r["scheduled_epoch"] != r["lateness_secs"] for r in (first, second)):
                excluded["inconsistent_time_origin"] += 1
                continue
            a, b = first["observed_interval_epoch"], second["observed_interval_epoch"]
            traversals.append({
                "journey_id": list(identity), "trip_id": first["trip_id"], "day": first["day"],
                **{k: first.get(k) for k in ("operator", "service", "route_pattern", "direction", "data_version", "method_version", "match")},
                "from_atco": first["atco"], "to_atco": second["atco"],
                "from_name": first.get("stop_name", ""), "to_name": second.get("stop_name", ""),
                "from_sequence": first["stop_index"], "to_sequence": second["stop_index"],
                "entry_epoch": first["observed_epoch"], "exit_epoch": second["observed_epoch"],
                "period": entry_period(first["observed_epoch"]),
                "day_type": "weekday" if date.fromisoformat(first["day"]).weekday() < 5 else "weekend",
                "observed_secs": observed, "scheduled_secs": scheduled, "gained_secs": observed-scheduled,
                "scheduled_depart_secs": first["scheduled_secs"], "hour": entry_hour(first["observed_epoch"]),
                "inherited_lateness_secs": first["lateness_secs"],
                "gain_interval_secs": [b[0]-a[1]-scheduled, b[1]-a[0]-scheduled],
                "source_files": sorted({first["source_file_sha256"], second["source_file_sha256"]}),
                "source_reports": first["source_reports"] + second["source_reports"],
            })
    eras = schedule_eras(traversals)
    for leg in traversals:
        leg["schedule_era"] = eras[tuple(leg[k] for k in STRETCH) + (leg["data_version"],)]
    cells = summarise_cells(traversals, PERIOD_DIMENSIONS, min_journeys, min_days, "period")
    hour_cells = summarise_cells(traversals, HOUR_DIMENSIONS, min_journeys, min_days, "hour")
    return {"schema_version": 1, "as_of": datetime.now(timezone.utc).isoformat(),
            "method": "Difference in endpoint lateness between adjacent measured timing points; "
                      "declared identities, no quality flags, bounded report intervals; grouped by operator, "
                      "pattern, call pair, schedule era (weekly builds pooled only where their promises agree), "
                      "method, day type, and observed London entry period (cells) or hour (hour_cells).",
            "data_versions": meta.get("data_versions", []), "method_versions": meta.get("method_versions", []),
            "input_manifest": [{k: s.get(k) for k in ("file", "sha256", "day", "data_version", "method_version")}
                               for s in meta.get("sources", [])],
            "floor": {"journeys": min_journeys, "distinct_days": min_days, "max_endpoint_interval_secs": max_interval_secs},
            "caveats": ["Preview: sample sufficiency is not independent validation or proof of congestion.",
                        "Report intervals bound departures from a radius, not exact bus-stop departures.",
                        "Section time includes dwell and holding. No precise road geometry or traffic cause is inferred.",
                        "Scheduled coverage of observed-entry cohorts is not established. Missing trips may bias delay downwards.",
                        "Weekday/weekend grouping does not control holidays, diversions or school calendars."],
            "pooling": "Weekly timetable builds are pooled for a stretch only where they agree on the "
                       "scheduled section time of every departure they share; a changed promise starts "
                       "a new schedule_era. Each cell names the builds it pools in data_versions.",
            "days_collected": sorted({leg["day"] for leg in traversals}),
            "excluded": dict(excluded), "cells": cells, "hour_cells": hour_cells,
            "traversals": traversals}


# ── What the browser draws ───────────────────────────────────

# Fields a map cell keeps. The rest — per-day breakdowns, every traversal and
# its source reports — stays in hotspot-preview.json, the evidence file, so the
# map a phone downloads stays small.
MAP_CELL_FIELDS = ("resolution", "period", "hour", "day_type", "schedule_era", "data_versions",
                   "median_gained_secs", "p90_gained_secs", "at_least_600s", "traversals",
                   "journeys", "distinct_days", "sample_sufficient", "status")

# A stretch whose road-following path is this much longer than the chain of
# its stops has been projected onto the wrong pass of a looping shape.
DETOUR_LIMIT = 2.5
SHAPE_SNAP_M = 150         # a stop further than this from the shape is off it
DECIMATE_M = 20            # keep a point only if it is this far from the last


def _metres(a, b):
    import math
    lat = math.radians((a[0] + b[0]) / 2)
    return math.hypot((b[0] - a[0]) * 111_320, (b[1] - a[1]) * 111_320 * math.cos(lat))


def _path_length(points):
    return sum(_metres(a, b) for a, b in zip(points, points[1:]))


def _decimate(points):
    if len(points) <= 2:
        return [[round(p[0], 5), round(p[1], 5)] for p in points]
    kept = [points[0]]
    for point in points[1:-1]:
        if _metres(kept[-1], point) >= DECIMATE_M:
            kept.append(point)
    kept.append(points[-1])
    return [[round(p[0], 5), round(p[1], 5)] for p in kept]


def _project(shape, stops):
    """Each stop's position along the shape, never moving backwards."""
    positions, start = [], 0
    for lat, lon in stops:
        best, best_i = None, start
        for i in range(start, len(shape)):
            d = _metres((lat, lon), shape[i])
            if best is None or d < best:
                best, best_i = d, i
        positions.append((best_i, best if best is not None else float("inf")))
        start = best_i
    return positions


def pattern_geometry(tt, pattern, trip_id):
    """Road-following points for each call of a pattern, projected once.

    Returns `(shape, positions)` or `(None, None)` when the trip has no shape.
    """
    trip = tt.trips.get(trip_id) or {}
    tid = trip.get("_tid")
    shape = tt._shape_points_for_trip(tid) if tid is not None else None
    stops = [tt.stops.get(a) or {} for a in pattern["atcos"]]
    if not shape or any(s.get("lat") is None for s in stops):
        return None, None
    return shape, _project(shape, [(s["lat"], s["lon"]) for s in stops])


def stretch_geometry(tt, pattern, i, j, projected):
    """The line for one stretch, and whether it is only approximate.

    The trip's own road-following shape between the two stops where it can be
    trusted; otherwise the chain of stops between them, flagged approximate so
    the map can say it is drawn roughly. Never a guess dressed as the road.
    """
    chain = [(tt.stops[a]["lat"], tt.stops[a]["lon"]) for a in pattern["atcos"][i:j + 1]
             if (tt.stops.get(a) or {}).get("lat") is not None]
    shape, positions = projected
    if shape and positions and j < len(positions):
        (a, da), (b, db) = positions[i], positions[j]
        piece = shape[a:b + 1]
        if (b > a and da <= SHAPE_SNAP_M and db <= SHAPE_SNAP_M and len(chain) >= 2
                and _path_length(piece) <= DETOUR_LIMIT * max(_path_length(chain), 1)):
            return _decimate(piece), False
    return _decimate(chain), True


def build_map(result, tt, day, out_dir):
    """Per-service map files: every timing-point stretch, and what was measured on it.

    Every stretch of the day's timetable is drawn, measured or not, so that an
    unmeasured one shows as "not enough journeys yet" rather than vanishing —
    a missing line reads as "no delay", which is the one thing it cannot mean.
    Stretches are keyed by the stops they run through, so the several route
    variants of one service that share a piece of road draw one line, not a
    stack of them; every variant's figures are kept.
    """
    from process_snapshots import record_schedule
    from build_journey_times import document_name
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    schedule = record_schedule(tt, date.fromisoformat(day) if isinstance(day, str) else day)

    shaped_cache = {}

    def has_shape(trip_id):
        """Whether the feed gives this trip a road shape — asked, not loaded."""
        if trip_id not in shaped_cache:
            tid = (tt.trips.get(trip_id) or {}).get("_tid")
            try:
                row = tt._conn().execute(
                    "SELECT EXISTS(SELECT 1 FROM shapes s JOIN trips t ON s.shape_id = t.shape_id "
                    "WHERE t.tid = ? AND t.shape_id != '')", (tid,)).fetchone() if tid is not None else None
            except Exception:                      # noqa: BLE001 — an older blob has no shapes
                row = None
            shaped_cache[trip_id] = bool(row and row[0])
        return shaped_cache[trip_id]

    services = defaultdict(lambda: {"patterns": {}, "trip": {}, "timepoints": {}})
    for trip_id, pattern_id, profile_ref, _start, headsign in schedule["trips"]:
        pattern = schedule["patterns"][pattern_id]
        entry = services[(pattern["service"], pattern.get("operator") or "")]
        entry["patterns"][pattern_id] = pattern
        # The trip whose road shape draws the pattern: the first that has one.
        # About half the feed's trips carry no shape, and simply taking the
        # first trip drew eleven patterns as straight lines between stops when
        # another journey on the identical route could have drawn the road.
        held = entry["trip"].get(pattern_id)
        if held is None:
            entry["trip"][pattern_id] = (trip_id, headsign, has_shape(trip_id))
        elif not held[2] and has_shape(trip_id):
            entry["trip"][pattern_id] = (trip_id, headsign, True)
        flags = tuple(schedule["profiles"][profile_ref]["timepoints"])
        counts = entry["timepoints"].setdefault(pattern_id, Counter())
        counts[flags] += 1

    cells_by_service = defaultdict(list)
    for cell in result["cells"] + result["hour_cells"]:
        cells_by_service[(cell["service"], cell.get("operator") or "")].append(cell)

    index = []
    for (service, operator) in sorted(set(services) | set(cells_by_service)):
        entry = services.get((service, operator), {"patterns": {}, "trip": {}, "timepoints": {}})
        stretches, sections = {}, {}

        def add_section(pattern_id, pattern, i, j, headsign, projected):
            key = (pattern_id, i, j)
            if key in sections:
                return
            atcos = pattern["atcos"][i:j + 1]
            stretch_key = hashlib.sha256(json.dumps(atcos).encode()).hexdigest()[:16]
            if stretch_key not in stretches:
                geometry, approximate = stretch_geometry(tt, pattern, i, j, projected)
                stretches[stretch_key] = {
                    "id": stretch_key, "direction": pattern.get("direction", "unknown"),
                    "from_atco": atcos[0], "to_atco": atcos[-1],
                    "from_name": (tt.stops.get(atcos[0]) or {}).get("name", ""),
                    "to_name": (tt.stops.get(atcos[-1]) or {}).get("name", ""),
                    "headsign": headsign, "geometry": geometry, "approximate": approximate}
            sections[key] = stretch_key

        for pattern_id, pattern in entry["patterns"].items():
            trip_id, headsign, _shaped = entry["trip"][pattern_id]
            projected = pattern_geometry(tt, pattern, trip_id)
            flags = entry["timepoints"][pattern_id].most_common(1)[0][0]
            points = [i for i, f in enumerate(flags) if f == 1]
            for i, j in zip(points, points[1:]):
                add_section(pattern_id, pattern, i, j, headsign, projected)

        cells = []
        for cell in cells_by_service.get((service, operator), []):
            key = (cell["route_pattern"], cell["from_sequence"], cell["to_sequence"])
            if key not in sections:
                pattern = entry["patterns"].get(cell["route_pattern"])
                if pattern and cell["to_sequence"] < len(pattern["atcos"]):
                    trip_id, headsign, _shaped = entry["trip"][cell["route_pattern"]]
                    add_section(cell["route_pattern"], pattern, cell["from_sequence"],
                                cell["to_sequence"], headsign,
                                pattern_geometry(tt, pattern, trip_id))
                else:
                    # A pattern today's timetable no longer runs. Its road is
                    # usually still drawn by a current stretch with the same two
                    # ends — reuse that line rather than lay a second one over it.
                    same_ends = next((s["id"] for s in stretches.values()
                                      if (s["from_atco"], s["to_atco"])
                                      == (cell["from_atco"], cell["to_atco"])), None)
                    if same_ends:
                        sections[key] = same_ends
                        cells.append({"stretch": same_ends, "route_pattern": cell["route_pattern"],
                                      **{k: cell.get(k) for k in MAP_CELL_FIELDS if k in cell}})
                        continue
                    # Otherwise draw the two ends, roughly, rather than drop
                    # measured evidence.
                    ends = [tt.stops.get(cell["from_atco"]) or {}, tt.stops.get(cell["to_atco"]) or {}]
                    if any(s.get("lat") is None for s in ends):
                        continue
                    stretch_key = hashlib.sha256(json.dumps(
                        [cell["from_atco"], cell["to_atco"]]).encode()).hexdigest()[:16]
                    stretches.setdefault(stretch_key, {
                        "id": stretch_key, "direction": cell.get("direction", "unknown"),
                        "from_atco": cell["from_atco"], "to_atco": cell["to_atco"],
                        "from_name": cell.get("from_name", ""), "to_name": cell.get("to_name", ""),
                        "headsign": "", "geometry": _decimate([(e["lat"], e["lon"]) for e in ends]),
                        "approximate": True})
                    sections[key] = stretch_key
            cells.append({"stretch": sections[key], "route_pattern": cell["route_pattern"],
                          **{k: cell.get(k) for k in MAP_CELL_FIELDS if k in cell}})
        if not stretches:
            continue
        # The era a reader should see first: the one holding the latest day.
        latest = {}
        for cell in cells_by_service.get((service, operator), []):
            key = (cell["route_pattern"], cell["from_sequence"], cell["to_sequence"])
            if key in sections:
                newest = max(cell.get("days") or [""])
                held = latest.get(sections[key])
                if held is None or newest > held[0]:
                    latest[sections[key]] = (newest, cell["schedule_era"])
        for cell in cells:
            cell["latest"] = latest.get(cell["stretch"], (None, None))[1] == cell.get("schedule_era")

        name = f"hotspot-map-{document_name(service, operator)}.json"
        doc = {"schema_version": 1, "service": service, "operator": operator,
               "as_of": result["as_of"], "timetable_day": schedule["day"],
               "floor": result["floor"], "method": result["method"], "pooling": result.get("pooling"),
               "caveats": result["caveats"],
               "days_collected": sorted({d for c in cells_by_service.get((service, operator), [])
                                         for d in c.get("days", [])}),
               "stretches": sorted(stretches.values(), key=lambda s: (s["direction"], s["id"])),
               "cells": cells}
        (out_dir / name).write_text(json.dumps(doc, separators=(",", ":"), sort_keys=True) + "\n")
        index.append({"service": service, "operator": operator, "file": name,
                      "stretches": len(stretches),
                      "sufficient_cells": sum(1 for c in cells if c.get("sample_sufficient")),
                      "days_collected": len(doc["days_collected"])})
    (out_dir / "hotspot-map-index.json").write_text(json.dumps({
        "schema_version": 1, "as_of": result["as_of"], "floor": result["floor"],
        "days_collected": result.get("days_collected", []), "timetable_day": schedule["day"],
        "services": index}, separators=(",", ":"), sort_keys=True) + "\n")
    return index


def main(argv=None):
    from query_reliability import load_observations
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--observations", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--map-out", help="directory for the compact per-service map files")
    ap.add_argument("--timetable", help="timetable.sqlite, for stretch geometry (needed with --map-out)")
    ap.add_argument("--map-day", help="the day whose timetable defines the stretches drawn "
                                      "(default: the newest observed day)")
    args = ap.parse_args(argv)
    rows, meta = load_observations(args.observations, row_filter=lambda row: row.get("timepoint") == 1)
    result = build_hotspots(rows, meta)
    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
    print(f'{len(result["traversals"])} eligible traversals; {len(result["cells"])} cohorts; '
          f'{sum(result["excluded"].values())} excluded candidate pairs → {out}')
    if args.map_out:
        if not args.timetable:
            ap.error("--map-out needs --timetable for stretch geometry")
        from api.timetable_db import Timetable
        tt = Timetable(Path(args.timetable), allow_fetch=False)
        day = args.map_day or max(meta.get("days") or [date.today().isoformat()])
        index = build_map(result, tt, day, args.map_out)
        print(f"{len(index)} service map files, "
              f"{sum(s['sufficient_cells'] for s in index)} cells at the floor → {args.map_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
