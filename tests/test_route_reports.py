"""scripts/build_route_reports.py and report.html: one route's performance,
with every figure's denominator, floor, method and sources.

A report a councillor prints has to survive being checked: the counts must
add up, a thin row must say it is thin, one bad day must not pass for the
usual, and the evidence behind it must be named.
"""

import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import build_route_reports as br                                   # noqa: E402
import check_published as checks                                   # noqa: E402

DAYS = ["2026-10-05", "2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09", "2026-10-10"]  # Mon–Sat


def row(day, trip, late, direction="westbound", hour=9, **over):
    return {"day": day, "trip_id": trip, "operator": "SCSO", "service": "700", "direction": direction,
            "atco": "A", "stop_name": "Stop A", "timepoint": 1, "estimated": False, "match": "declared",
            "quality_flags": [], "lateness_secs": late, "scheduled_secs": hour * 3600,
            "observed_epoch": 1_790_000_000, **over}


def rows():
    out = []
    for d in DAYS[:5]:                       # five weekdays, ten timings each
        for i in range(10):
            # Monday is a bad day: every bus 10 minutes late.
            out.append(row(d, f"T{i}", 600 if d == DAYS[0] else 60))
    out.append(row(DAYS[5], "S1", 60))       # one Saturday timing
    # Left out: interpolated, flagged, worked out from position, not a timing point.
    out += [row(DAYS[1], "X1", 0, estimated=True), row(DAYS[1], "X2", 0, quality_flags=["unbounded_departure"]),
            row(DAYS[1], "X3", 0, match="inferred"), row(DAYS[1], "X4", 0, timepoint=0)]
    return out


def report(**kw):
    meta = {"days": DAYS, "sources": [{"day": d, "sha256": f"h{d}"} for d in DAYS],
            "data_versions": ["v1"], "method_versions": [7]}
    return br.build(rows(), meta, as_of="2026-10-11T03:30:00+00:00", **kw)[("SCSO", "700")]


def test_counts_add_up_and_only_judged_timings_count():
    doc = report()
    weekday = next(r for r in doc["punctuality"]["rows"] if r["day_type"] == "weekday")
    assert weekday["judged"] == 50, "an interpolated, flagged, inferred or non-timing-point row was counted"
    assert (weekday["on_time"], weekday["late"]) == (40, 10)
    assert weekday["sample_sufficient"] is True
    weekend = next(r for r in doc["punctuality"]["rows"] if r["day_type"] == "weekend")
    assert (weekend["judged"], weekend["sample_sufficient"]) == (1, False), "a thin row claimed a figure"


def test_one_bad_day_shows_in_the_spread_not_hidden_in_the_average():
    weekday = next(r for r in report()["punctuality"]["rows"] if r["day_type"] == "weekday")
    assert weekday["daily"] == {"days": 5, "min": 0.0, "median": 1.0, "max": 1.0}


def test_disrupted_journeys_are_marked_not_removed():
    event = {"id": "works", "operator": "SCSO", "_routes": {"700"}, "_stops": {"A"}, "area": [],
             "_from": __import__("datetime").datetime(2026, 1, 1, tzinfo=__import__("datetime").timezone.utc),
             "_to": None, "label": "the works"}
    doc = report(exclusions=[event])
    p = doc["punctuality"]
    assert p["disrupted_timings"] == p["timings"] == 51
    weekday = next(r for r in p["rows"] if r["day_type"] == "weekday")
    assert weekday["judged"] == 50 and weekday["excluding_disruptions"]["judged"] == 0
    assert doc["disruptions"][0]["label"] == "the works"


def test_the_report_names_its_window_method_and_sources_and_passes_its_check():
    doc = json.loads(json.dumps(report()))
    assert doc["window"] == [DAYS[0], DAYS[-1]]
    assert doc["sources"][0] == {"day": DAYS[0], "sha256": f"h{DAYS[0]}"}
    fails = checks.Failures()
    checks.check_route_report(doc, "route-report-700-SCSO.json", fails)
    assert not fails.items, fails.items


def test_the_check_refuses_a_report_that_does_not_add_up():
    doc = json.loads(json.dumps(report()))
    weekday = doc["punctuality"]["rows"][0]
    weekday["on_time"] += 5
    doc["trend"][0]["sample_sufficient"] = True
    doc["trend"][0]["days"] = 1
    del doc["sources"]
    fails = checks.Failures()
    checks.check_route_report(doc, "route-report-700-SCSO.json", fails)
    found = {check for check, _ in fails.items}
    assert "route report bands do not add up to the timings" in found
    assert "route report claims sufficiency below the floor" in found
    assert "route report is missing its provenance" in found


def test_waits_and_bottlenecks_take_the_busiest_point_and_the_worst_stretches():
    hw = {"cells": [
        {"kind": "frequent", "period": "any", "direction": "westbound", "day_type": "weekday", "atco": "A",
         "scheduled_passages": 100, "ewt_secs": 60, "sample_sufficient": True},
        {"kind": "frequent", "period": "any", "direction": "westbound", "day_type": "weekday", "atco": "B",
         "scheduled_passages": 50, "ewt_secs": 300, "sample_sufficient": True},
        {"kind": "non_frequent", "period": "any", "direction": "eastbound", "day_type": "weekday", "atco": "C"}]}
    got = br.waits(hw, {"A": "Stop A"})
    assert [(w["atco"], w["stop_name"]) for w in got] == [("A", "Stop A")]
    dm = {"stretches": [{"id": f"s{i}", "direction": "westbound", "from_name": f"F{i}", "to_name": f"T{i}"}
                        for i in range(5)],
          "cells": [{"stretch": f"s{i}", "sample_sufficient": True, "latest": True, "median_gained_secs": i * 60,
                     "day_type": "weekday", "period": "16-18", "journeys": 40, "distinct_days": 6}
                    for i in range(5)]
                   + [{"stretch": "s0", "sample_sufficient": False, "latest": True, "median_gained_secs": 9999},
                      {"stretch": "s1", "sample_sufficient": True, "latest": False, "median_gained_secs": 9999}]}
    worst = br.bottlenecks(dm)
    assert [b["from_name"] for b in worst] == ["F4", "F3", "F2"], "a thin or out-of-date cell was ranked"


def test_the_nightly_run_builds_and_publishes_them():
    workflow = yaml.safe_load((ROOT / ".github/workflows/process-snapshots.yml").read_text())
    runs = "\n".join(s.get("run") or "" for s in workflow["jobs"]["process"]["steps"])
    assert "build_route_reports.py --observations inputs/window" in runs
    assert "route-reports/*.json" in runs
    assert 'name.startswith("route-report-")' in (ROOT / "scripts/publication_bundle.py").read_text()
    assert br.file_name("700", "SCSO") == "route-report-700-SCSO.json"
    assert br.file_name("N700", "SCSO") == "route-report-N700-SCSO.json"


def test_overtaking_totals_by_direction_and_names_the_worst_timing_points():
    hw = {"cells": [
        {"period": "16-18", "direction": "eastbound", "day_type": "weekday", "atco": "A", "order_checked": 97,
         "overtaken": 7, "days": 14},
        {"period": "10-16", "direction": "eastbound", "day_type": "weekday", "atco": "B", "order_checked": 20,
         "overtaken": 5, "days": 14},           # under the floor: totalled, never named
        {"period": "any", "direction": "eastbound", "day_type": "weekday", "atco": "A", "order_checked": 117,
         "overtaken": 12, "days": 14},          # the all-day cell repeats the others: not added again
    ]}
    got = br.overtaking(hw, {"A": "High Street"})
    assert got["directions"] == [{"direction": "eastbound", "day_type": "weekday", "checked": 117, "overtaken": 12}]
    assert [(w["stop_name"], w["overtaken"], w["checked"]) for w in got["worst"]] == [("High Street", 7, 97)]
    doc = json.loads(json.dumps(report()))
    doc["overtaking"] = {"directions": [{"direction": "x", "checked": 3, "overtaken": 4}], "worst": []}
    fails = checks.Failures()
    checks.check_route_report(doc, "route-report-700-SCSO.json", fails)
    assert "route report's overtaking counts are not a share of the buses checked" in {c for c, _ in fails.items}
