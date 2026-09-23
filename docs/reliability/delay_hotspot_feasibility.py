"""Offline diagnostic for the 22 September review; not a publication pipeline.

Requires the five observation assets identified by the adjacent review manifest.
No network access or application writes. Run from any directory with Python 3.
"""

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import reliability_stats as rs  # noqa: E402


def extra_seconds(first, second):
    return (second["observed_secs"] - first["observed_secs"]) - (
        second["scheduled_secs"] - first["scheduled_secs"])


def period(seconds):
    hour = seconds / 3600
    if 7 <= hour < 10:
        return "07-10"
    if 10 <= hour < 16:
        return "10-16"
    if 16 <= hour < 19:
        return "16-19"
    return "other daytime"


def summarize(legs):
    gains = sorted(leg["gain"] for leg in legs)
    days = sorted({leg["day"] for leg in legs})
    return {
        "traversals": len(legs),
        "distinct_journeys": len({leg["journey"] for leg in legs}),
        "distinct_days": len(days),
        "by_day": {day: {
            "traversals": sum(leg["day"] == day for leg in legs),
            "gained_at_least_600s": sum(leg["day"] == day and leg["gain"] >= 600
                                       for leg in legs),
        } for day in days},
        "median_gained_secs": statistics.median(gains),
        # Match the repository's existing p90 convention, not a fitted model.
        "p90_gained_secs": gains[min(len(gains) - 1, int(len(gains) * .9))],
        "gained_at_least_600s": sum(gain >= 600 for gain in gains),
        "median_stops_apart": statistics.median(leg["stops_apart"] for leg in legs),
        "meets_exploratory_floor_30_journeys_5_days": (
            len({leg["journey"] for leg in legs}) >= 30 and len(days) >= 5),
    }


def audit(observations):
    manifest = Path(__file__).with_name("review-2026-09-22-manifest.sha256")
    inputs, all_rows, cells = [], [], defaultdict(list)
    exclusions = Counter()
    measured_by_day = []
    for line in manifest.read_text().splitlines():
        expected, name = line.split()
        if not name.startswith("observations-"):
            continue
        path = observations / name
        raw = path.read_bytes()
        actual = hashlib.sha256(raw).hexdigest()
        if actual != expected:
            raise ValueError(f"Reviewed input has changed: {name}")
        payload = json.loads(gzip.decompress(raw))
        rows = payload["observations"]
        all_rows.extend(rows)
        inputs.append({"file": name, "sha256": actual, "rows": len(rows),
                       **{k: payload[k] for k in ("day", "as_of", "data_version", "method_version")}})
        measured = [r for r in rows if not r.get("estimated") and r.get("timepoint") == 1]
        measured_by_day.append({"day": payload["day"], "measured_timing_points": len(measured),
                                "declared_tagged": sum(r.get("match") == "declared" for r in measured)})
        journeys = defaultdict(list)
        for row in measured:
            key = (row["day"], row["operator"], row["trip_id"], row["vehicle"])
            journeys[key].append(row)
        for identity, journey in journeys.items():
            indices = [r["stop_index"] for r in journey]
            if len(indices) != len(set(indices)):
                exclusions["journeys_with_duplicate_call_index"] += 1
                continue
            journey.sort(key=lambda r: r["stop_index"])
            for first, second in zip(journey, journey[1:]):
                # Form pairs before filtering times. No splicing across removed points.
                if first["day"] != payload["day"] or second["day"] != payload["day"]:
                    exclusions["pairs_service_day_differs_from_file_day"] += 1
                    continue
                if not all(6 * 3600 <= row[field] < 22 * 3600
                           for row in (first, second) for field in ("scheduled_secs", "observed_secs")):
                    exclusions["pairs_outside_daytime_scope"] += 1
                    continue
                fields = ("operator", "service", "direction", "headsign", "match", "calls_total")
                if any(first.get(k) != second.get(k) for k in fields):
                    exclusions["pairs_with_inconsistent_identity_labels"] += 1
                    continue
                if any(second[k] <= first[k] for k in ("scheduled_secs", "observed_secs", "stop_index")):
                    exclusions["pairs_with_nonpositive_time_or_sequence"] += 1
                    continue
                if any(r["observed_secs"] - r["scheduled_secs"] != r["lateness_secs"]
                       for r in (first, second)):
                    exclusions["pairs_with_inconsistent_lateness"] += 1
                    continue
                weekday = date.fromisoformat(first["day"]).weekday() < 5
                key = (payload["data_version"], payload["method_version"],
                       *(first.get(k) for k in fields),
                       first["atco"], second["atco"], first["stop_name"], second["stop_name"],
                       first["stop_index"], second["stop_index"],
                       "weekday" if weekday else "weekend", period(first["observed_secs"]))
                cells[key].append({"journey": identity, "day": first["day"],
                                   "gain": extra_seconds(first, second),
                                   "stops_apart": second["stop_index"] - first["stop_index"]})

    legacy = rs.segment_stats([r for r in all_rows if r.get("service") == "700"
                               and r.get("direction") == "westbound"], hour=True)
    kept, thin = rs.suppress(legacy)
    key_fields = ("data_version", "method_version", "operator", "service", "direction", "headsign",
                  "match_label", "calls_total", "from_atco", "to_atco", "from_name", "to_name",
                  "from_stop_index", "to_stop_index", "day_type", "observed_entry_period")
    results = [{**dict(zip(key_fields, key)), **summarize(legs)} for key, legs in sorted(cells.items())]
    example = [r for r in results if r["operator"] == "BHBC" and r["service"] == "46"
               and r["direction"] == "westbound" and r["from_name"] == "Holmbush Centre"
               and r["to_name"] == "Southwick Square"]
    return {
        "status": "diagnostic only; upstream review findings unresolved",
        "as_of": datetime.now(timezone.utc).isoformat(),
        "data_version": "review manifest sha256:" + hashlib.sha256(manifest.read_bytes()).hexdigest(),
        "diagnostic_method_version": 1,
        "reviewed_application_commit": "ec9f12c7940ca0ba4bd452cb33ebf67ba8e0c28a",
        "diagnostic_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "reliability_stats_sha256": hashlib.sha256(Path(rs.__file__).read_bytes()).hexdigest(),
        "method": "Measured timing-point pairs, same-file same-day, scheduled/observed 06:00–22:00; "
                  "partition by timetable, method, operator, service, direction, headsign, match label, "
                  "call count, ordered ATCO/call indices, weekday/weekend and observed entry period. "
                  "Consecutive observed timing points need not be adjacent scheduled timing points.",
        "caveats": ["Headsign/call count/indices are a route-pattern proxy, not a verified shape.",
                    "Declared is the current journey-level label, not proof for each observation.",
                    "Endpoint report intervals and raw matching evidence are absent.",
                    "No scheduled-traversal denominator or congestion causation is established.",
                    "The 30-journey/5-day floor is an exploratory design choice, not statistical proof.",
                    "Filters bound the diagnostic; they do not repair or validate upstream measurements."],
        "inputs": inputs,
        "input_rows": len(all_rows),
        "measured_timing_points_by_day": measured_by_day,
        "existing_700_westbound_hourly": {"cells": len(legacy), "shown": len(kept), "suppressed": len(thin),
                                          "floor": {"traversals": rs.MIN_OBS, "journeys": rs.MIN_JOURNEYS}},
        "diagnostic": {"cells": len(results), "traversals": sum(r["traversals"] for r in results),
                       "cells_meeting_exploratory_floor": sum(r["meets_exploratory_floor_30_journeys_5_days"]
                                                              for r in results),
                       "exclusions": dict(exclusions)},
        "example_46_westbound_holmbush_to_southwick": example,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(audit(args.observations), indent=2, ensure_ascii=False))
