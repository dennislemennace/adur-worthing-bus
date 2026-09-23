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


# ── Pooling weekly timetable builds ──────────────────────────
#
# The timetable is rebuilt every week. Grouping by build kept like with like but
# meant no stretch could pool more than a week, so most would never reach the
# floor. Builds now pool only where they make the same promise.

def trip(day, version, trip_id, start, section=600, late=(120, 720)):
    """One journey over one stretch: departing `start`, scheduled `section`."""
    from observation_contract import service_origin
    base = service_origin(day)
    rows = []
    for i, (scheduled, lateness) in enumerate(((start, late[0]), (start + section, late[1]))):
        epoch = base + scheduled + lateness
        rows.append({"day": day, "trip_id": trip_id, "operator": "SCSO", "service": "700",
                     "route_pattern": "pattern", "direction": "westbound", "atco": f"S{i}",
                     "stop_name": f"Stop {i}", "stop_index": i, "timing_point_order": i,
                     "timepoint": 1, "estimated": False, "match": "declared", "vehicle": "V",
                     "scheduled_secs": scheduled, "observed_secs": scheduled + lateness,
                     "scheduled_epoch": base + scheduled, "observed_epoch": epoch,
                     "lateness_secs": lateness, "observed_interval_epoch": [epoch, epoch + 30],
                     "quality_flags": [], "source_reports": [{"object": "rt/x.pb", "sha256": "a"*64}],
                     "data_version": version, "method_version": 4, "source_file_sha256": "b"*64})
    return rows


def eras_of(result):
    return {leg["data_version"]: leg["schedule_era"] for leg in result["traversals"]}


def test_two_weekly_builds_making_the_same_promise_pool():
    from build_delay_hotspots import build_hotspots
    rows = trip("2026-09-21", "week1", "A", 8 * 3600) + trip("2026-09-28", "week2", "B", 8 * 3600)
    result = build_hotspots(rows, {})
    assert len(result["cells"]) == 1, "identical promises were kept apart"
    assert result["cells"][0]["data_versions"] == ["week1", "week2"]
    assert result["cells"][0]["journeys"] == 2


def test_a_changed_running_time_starts_a_new_era():
    # Four more minutes of running time is a different promise: pooling it
    # would read the operator's own change as the road getting faster.
    from build_delay_hotspots import build_hotspots
    rows = (trip("2026-09-21", "week1", "A", 8 * 3600, section=600)
            + trip("2026-09-28", "week2", "B", 8 * 3600, section=840))
    eras = eras_of(build_hotspots(rows, {}))
    assert eras["week1"] != eras["week2"]


def test_agreement_is_not_chained_into_contradiction():
    # A and B agree at 08:00; B and C agree at 09:00; A and C contradict each
    # other at 10:00. Chaining would pool A with a C it disagrees with.
    from build_delay_hotspots import build_hotspots
    rows = (trip("2026-09-21", "A", "a1", 8 * 3600) + trip("2026-09-21", "A", "a2", 10 * 3600)
            + trip("2026-09-22", "B", "b1", 8 * 3600) + trip("2026-09-22", "B", "b2", 9 * 3600)
            + trip("2026-09-23", "C", "c1", 9 * 3600) + trip("2026-09-23", "C", "c2", 10 * 3600, section=660))
    eras = eras_of(build_hotspots(rows, {}))
    assert eras["A"] == eras["B"]
    assert eras["C"] != eras["A"]


def test_builds_with_no_departure_in_common_cannot_be_shown_to_agree():
    from build_delay_hotspots import build_hotspots
    rows = trip("2026-09-21", "week1", "A", 8 * 3600) + trip("2026-09-28", "week2", "B", 9 * 3600)
    eras = eras_of(build_hotspots(rows, {}))
    assert eras["week1"] != eras["week2"]


def test_hour_cells_are_published_alongside_the_periods():
    from build_delay_hotspots import build_hotspots
    result = build_hotspots(trip("2026-09-21", "week1", "A", 8 * 3600), {})
    assert [c["hour"] for c in result["hour_cells"]] == [8]
    assert result["hour_cells"][0]["resolution"] == "hour"
    assert result["cells"][0]["resolution"] == "period"
    assert result["days_collected"] == ["2026-09-21"]


# ── The map a phone downloads ────────────────────────────────

import json as _json
import pytest


class _Stub:
    """Just enough Timetable for the geometry: three stops in a line, and
    optionally a road shape for the one trip."""
    def __init__(self, shape=None):
        self.stops = {"A": {"lat": 50.83, "lon": -0.30, "name": "A"},
                      "B": {"lat": 50.83, "lon": -0.29, "name": "B"},
                      "C": {"lat": 50.83, "lon": -0.28, "name": "C"}}
        self.trips = {"T": {"_tid": 1}}
        self._shape = shape

    def _shape_points_for_trip(self, tid):
        return self._shape


PATTERN = {"atcos": ["A", "B", "C"], "direction": "eastbound"}


def test_a_stretch_follows_the_road_where_the_feed_gives_one():
    from build_delay_hotspots import pattern_geometry, stretch_geometry
    road = [(50.83, -0.30 + i * 0.001) for i in range(21)]        # A to C along the road
    tt = _Stub(shape=road)
    line, approximate = stretch_geometry(tt, PATTERN, 0, 2, pattern_geometry(tt, PATTERN, "T"))
    assert approximate is False
    assert len(line) > 3, "the road shape was not used"


def test_a_stretch_with_no_road_shape_says_it_is_approximate():
    from build_delay_hotspots import pattern_geometry, stretch_geometry
    tt = _Stub(shape=None)
    line, approximate = stretch_geometry(tt, PATTERN, 0, 2, pattern_geometry(tt, PATTERN, "T"))
    assert approximate is True
    assert line == [[50.83, -0.3], [50.83, -0.29], [50.83, -0.28]]


def test_a_shape_that_wanders_far_from_the_stops_is_not_trusted():
    # A loop that detours five kilometres north between A and C: projected
    # naively it would draw the detour as the road between two adjacent stops.
    from build_delay_hotspots import pattern_geometry, stretch_geometry
    detour = ([(50.83, -0.30)] + [(50.83 + i * 0.005, -0.295) for i in range(1, 10)]
              + [(50.83, -0.29), (50.83, -0.28)])
    tt = _Stub(shape=detour)
    _line, approximate = stretch_geometry(tt, PATTERN, 0, 1, pattern_geometry(tt, PATTERN, "T"))
    assert approximate is True


def test_projection_never_runs_backwards_along_a_loop():
    from build_delay_hotspots import _project
    loop = [(0.0, 0.0), (0.0, 0.001), (0.001, 0.001), (0.001, 0.0), (0.0, 0.0), (0.0, -0.001)]
    positions = [i for i, _d in _project(loop, [(0.0, 0.0), (0.001, 0.001), (0.0, 0.0)])]
    assert positions == sorted(positions)
    assert positions[-1] == 4, "the second visit was matched to the first pass"


from test_process_snapshots import DAY, monkeypatch_module, tt  # noqa: E402,F401


def _empty_result(**extra):
    return {"as_of": "2026-09-23T00:00:00+00:00", "floor": {"journeys": 30, "distinct_days": 5},
            "method": "m", "caveats": [], "pooling": "p", "days_collected": [],
            "cells": [], "hour_cells": [], **extra}


def test_every_timetabled_stretch_is_drawn_even_before_anything_is_measured(tt, tmp_path):
    # An unmeasured stretch must show as "not enough journeys yet", not vanish:
    # a missing line reads as "no delay", the one thing it cannot mean.
    from build_delay_hotspots import build_map
    index = build_map(_empty_result(), tt, DAY, tmp_path)
    files = {s["file"] for s in index}
    assert "hotspot-map-700-SCSO.json" in files
    assert not any("NATX" in f for f in files), "a coach was drawn as a local bus"
    doc = _json.loads((tmp_path / "hotspot-map-700-SCSO.json").read_text())
    assert {s["direction"] for s in doc["stretches"]} == {"eastbound", "westbound"}
    assert all(len(s["geometry"]) >= 2 for s in doc["stretches"])
    assert doc["cells"] == []
    assert (tmp_path / "hotspot-map-index.json").exists()


def _real_stretch(tt):
    """A real timing-point stretch of the fixture's westbound 700."""
    import process_snapshots as ps
    schedule = ps.record_schedule(tt, DAY)
    pattern_id, pattern = next((k, p) for k, p in schedule["patterns"].items()
                               if p["direction"] == "westbound")
    return pattern_id, pattern


def _cell(pattern_id, pattern, i, j, **extra):
    return {"service": "700", "operator": "SCSO", "route_pattern": pattern_id,
            "direction": "westbound", "from_sequence": i, "to_sequence": j,
            "from_atco": pattern["atcos"][i], "to_atco": pattern["atcos"][j],
            "from_name": "x", "to_name": "y", "resolution": "period", "period": "07-10",
            "day_type": "weekday", "schedule_era": "e1", "data_versions": ["tt"],
            "median_gained_secs": 150, "p90_gained_secs": None, "at_least_600s": 1,
            "traversals": 12, "journeys": 12, "distinct_days": 2, "days": ["2026-09-16"],
            "daily": {"2026-09-16": {"traversals": 12, "at_least_600s": 1}},
            "sample_sufficient": False, "status": "insufficient_data", **extra}


def test_a_measured_cell_lands_on_its_stretch_without_its_raw_evidence(tt, tmp_path):
    from build_delay_hotspots import build_map
    pattern_id, pattern = _real_stretch(tt)
    build_map(_empty_result(cells=[_cell(pattern_id, pattern, 10, 20)]), tt, DAY, tmp_path)
    text = (tmp_path / "hotspot-map-700-SCSO.json").read_text()
    doc = _json.loads(text)
    cell = doc["cells"][0]
    stretch = next(s for s in doc["stretches"] if s["id"] == cell["stretch"])
    assert (stretch["from_atco"], stretch["to_atco"]) == (pattern["atcos"][10], pattern["atcos"][20])
    assert "daily" not in cell and "source_reports" not in text
    assert cell["latest"] is True


def test_a_retired_route_variant_reuses_the_line_already_drawn(tt, tmp_path):
    # Measured evidence from a variant today's timetable no longer runs, over a
    # road a current stretch already draws: one line, not two laid on top.
    from build_delay_hotspots import build_map
    pattern_id, pattern = _real_stretch(tt)
    retired = _cell("retired-pattern", pattern, 10, 20)
    build_map(_empty_result(cells=[retired]), tt, DAY, tmp_path)
    doc = _json.loads((tmp_path / "hotspot-map-700-SCSO.json").read_text())
    ends = [(s["from_atco"], s["to_atco"]) for s in doc["stretches"]]
    assert ends.count((pattern["atcos"][10], pattern["atcos"][20])) == 1
    assert doc["cells"][0]["stretch"] in {s["id"] for s in doc["stretches"]}


def test_the_published_map_check_refuses_an_overclaim_and_raw_evidence():
    import check_published as checks
    doc = {"floor": {"journeys": 30, "distinct_days": 5},
           "stretches": [{"id": "s1", "geometry": [[0, 0], [0, 1]]}],
           "cells": [{"stretch": "s1", "journeys": 12, "distinct_days": 2, "sample_sufficient": True},
                     {"stretch": "ghost", "journeys": 40, "distinct_days": 6, "sample_sufficient": True}],
           "traversals": []}
    failures = checks.Failures()
    checks.check_hotspot_map(doc, "m.json", failures, size=600_000)
    found = " ".join(str(i) for i in failures.items)
    for message in ("below the floor", "does not draw", "raw traversals", "too large"):
        assert message in found, message
