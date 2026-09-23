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

from observation_contract import LONDON, row_identity


def entry_period(epoch):
    hour = datetime.fromtimestamp(epoch, LONDON).hour
    return "07-10" if 7 <= hour < 10 else "10-16" if 10 <= hour < 16 else "16-19" if 16 <= hour < 19 else "other"


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
                "inherited_lateness_secs": first["lateness_secs"],
                "gain_interval_secs": [b[0]-a[1]-scheduled, b[1]-a[0]-scheduled],
                "source_files": sorted({first["source_file_sha256"], second["source_file_sha256"]}),
                "source_reports": first["source_reports"] + second["source_reports"],
            })
    groups = defaultdict(list)
    dimensions = ("operator", "service", "route_pattern", "direction", "from_atco", "to_atco",
                  "from_sequence", "to_sequence", "data_version", "method_version", "match", "day_type", "period")
    for leg in traversals:
        groups[tuple(leg[k] for k in dimensions)].append(leg)
    cells = []
    for key, legs in sorted(groups.items(), key=lambda item: str(item[0])):
        gains = sorted(leg["gained_secs"] for leg in legs)
        days = sorted({leg["day"] for leg in legs})
        count = len({tuple(leg["journey_id"]) for leg in legs})
        sufficient = count >= min_journeys and len(days) >= min_days
        cells.append({**dict(zip(dimensions, key)), "id": hashlib.sha256(json.dumps(key).encode()).hexdigest(),
                      "from_name": legs[0]["from_name"], "to_name": legs[0]["to_name"],
                      "traversals": len(legs), "journeys": count, "distinct_days": len(days),
                      "days": days, "median_gained_secs": statistics.median(gains),
                      "p90_gained_secs": gains[min(len(gains)-1, int(len(gains)*.9))] if sufficient else None,
                      "at_least_600s": sum(g >= 600 for g in gains),
                      "at_least_600s_even_at_interval_lower_bound": sum(l["gain_interval_secs"][0] >= 600 for l in legs),
                      "daily": {d: {"traversals": sum(l["day"] == d for l in legs),
                                    "at_least_600s": sum(l["day"] == d and l["gained_secs"] >= 600 for l in legs)} for d in days},
                      "publishable": False, "sample_sufficient": sufficient, "status": "sample_sufficient_preview" if sufficient else "insufficient_data",
                      "scheduled_traversals": None, "coverage_status": "not_established_for_observed_entry_cohort"})
    return {"schema_version": 1, "as_of": datetime.now(timezone.utc).isoformat(),
            "method": "Difference in endpoint lateness between adjacent measured timing points; "
                      "declared identities, no quality flags, bounded report intervals; grouped by operator, "
                      "pattern, call pair, method/timetable, day type and observed London entry period.",
            "data_versions": meta.get("data_versions", []), "method_versions": meta.get("method_versions", []),
            "input_manifest": [{k: s.get(k) for k in ("file", "sha256", "day", "data_version", "method_version")}
                               for s in meta.get("sources", [])],
            "floor": {"journeys": min_journeys, "distinct_days": min_days, "max_endpoint_interval_secs": max_interval_secs},
            "caveats": ["Preview: sample sufficiency is not independent validation or proof of congestion.",
                        "Report intervals bound departures from a radius, not exact bus-stop departures.",
                        "Section time includes dwell and holding. No precise road geometry or traffic cause is inferred.",
                        "Scheduled coverage of observed-entry cohorts is not established. Missing trips may bias delay downwards.",
                        "Weekday/weekend grouping does not control holidays, diversions or school calendars."],
            "excluded": dict(excluded), "cells": cells, "traversals": traversals}


def main(argv=None):
    from query_reliability import load_observations
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--observations", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    rows, meta = load_observations(args.observations, row_filter=lambda row: row.get("timepoint") == 1)
    result = build_hotspots(rows, meta)
    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
    print(f'{len(result["traversals"])} eligible traversals; {len(result["cells"])} cohorts; '
          f'{sum(result["excluded"].values())} excluded candidate pairs → {out}')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
