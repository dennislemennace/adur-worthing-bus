"""Evidence gates for location claims, not just arithmetic snapshots."""
from datetime import date, timedelta
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))


def pair(day="2026-09-21", operator="SCSO", first_late=120, second_late=720):
    from observation_contract import service_origin
    base = service_origin(day)
    rows = []
    for i, late in enumerate((first_late, second_late)):
        scheduled = 8 * 3600 + i * 600
        epoch = base + scheduled + late
        rows.append({"day": day, "trip_id": "T", "operator": operator, "service": "700",
                     "route_pattern": "pattern", "direction": "westbound", "atco": f"S{i}",
                     "stop_name": f"Stop {i}", "stop_index": i, "timing_point_order": i,
                     "timepoint": 1, "estimated": False, "match": "declared", "vehicle": "V",
                     "scheduled_secs": scheduled, "observed_secs": scheduled + late,
                     "scheduled_epoch": base + scheduled, "observed_epoch": epoch,
                     "lateness_secs": late, "observed_interval_epoch": [epoch, epoch + 30],
                     "quality_flags": [], "source_reports": [{"object": "rt/day/report.pb", "sha256": "a"*64}],
                     "data_version": "tt", "method_version": 4, "source_file_sha256": "b"*64})
    return rows


def test_delay_gained_and_uncertainty_are_carried_to_the_traversal():
    from build_delay_hotspots import build_hotspots
    result = build_hotspots(pair(), {})
    leg = result["traversals"][0]
    assert leg["gained_secs"] == 600
    assert leg["gain_interval_secs"] == [570, 630]
    assert leg["inherited_lateness_secs"] == 120
    assert not result["cells"][0]["sample_sufficient"]
    assert result["cells"][0]["at_least_600s"] == 1


def test_carried_lateness_does_not_become_a_hotspot():
    from build_delay_hotspots import build_hotspots
    result = build_hotspots(pair(first_late=720, second_late=720), {})
    assert result["traversals"][0]["gained_secs"] == 0


def test_operator_and_method_cohorts_do_not_merge():
    from build_delay_hotspots import build_hotspots
    result = build_hotspots(pair() + pair(operator="BHBC"), {})
    assert len(result["cells"]) == 2


def test_legacy_interpolated_ambiguous_and_unbounded_calls_cannot_support_location_claims():
    from build_delay_hotspots import build_hotspots
    for changes in ({"method_version": 3}, {"estimated": True},
                    {"quality_flags": ["ambiguous_visit_alignment"]},
                    {"observed_interval_epoch": [10, None]}, {"source_reports": []},
                    {"match": "inferred"}):
        rows = pair(); rows[1].update(changes)
        result = build_hotspots(rows, {})
        assert result["traversals"] == []
        assert sum(result["excluded"].values()) == 1


def test_missing_timing_point_does_not_localise_a_long_gap_to_a_short_section():
    from build_delay_hotspots import build_hotspots
    rows = pair(); rows[1]["timing_point_order"] = 2
    assert not build_hotspots(rows, {})["traversals"]


def test_five_comparable_dates_are_required_as_well_as_thirty_journeys():
    from build_delay_hotspots import build_hotspots
    rows = []
    for j in range(30):
        for r in pair():
            rows.append(dict(r, trip_id=f"T{j}"))
    assert not build_hotspots(rows, {})["cells"][0]["sample_sufficient"]
    rows = []
    for d in range(5):
        for j in range(6):
            rows += [dict(r, trip_id=f"T{j}") for r in pair((date(2026, 9, 21)+timedelta(days=d)).isoformat())]
    result = build_hotspots(rows, {})
    assert result["cells"][0]["sample_sufficient"]
    assert result["cells"][0]["distinct_days"] == 5
