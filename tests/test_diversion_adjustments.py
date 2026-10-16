"""Allowing for a diversion in live estimates.

scripts/build_diversion_adjustments.py measures how much later buses reach the
stops past a recorded diversion; api/diversions.py and api/live_eta.py add it
to a bus that has not yet gone round. Each test pins a way it could tell a
passenger a time the bus beats — the error that loses them it.
"""
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))

import build_diversion_adjustments as bd                         # noqa: E402
from api import diversions, live_eta                             # noqa: E402
from api.trip_match import LONDON                                # noqa: E402
from build_delay_hotspots import entry_period                    # noqa: E402
from test_live_eta import FakeTimetable, at, declared            # noqa: E402

AREA = [[50.82, -0.19], [50.84, -0.19], [50.84, -0.18], [50.82, -0.18]]
COORDS = {"W": (50.83, -0.22), "E": (50.83, -0.20), "K1": (50.83, -0.17), "K2": (50.83, -0.16)}
EVENT = {"id": "x-closure", "operator": "SCSO", "routes": ["700"], "area": AREA,
         "from": "2026-10-01T00:00:00+01:00", "to": "2026-11-01T00:00:00+00:00",
         "adjust_predictions": True, "summary": "Road closed", "stops_not_served": []}


def event(**over):
    e = {**EVENT, **over}
    return {**e, "_from": datetime.fromisoformat(e["from"]), "_to": datetime.fromisoformat(e["to"]),
            "_routes": {r.upper() for r in e["routes"]}, "_stops": set()}


def journey(day, n, gains, stops=("W", "E", "K1", "K2"), late_at_entry=60):
    """Measured timing points for one journey at 11:00, with lateness at each
    stop after E being `late_at_entry` plus its gain."""
    base = int(datetime(day.year, day.month, day.day, 11, n, tzinfo=LONDON).timestamp())
    rows = []
    for order, atco in enumerate(("W", "E", "K1", "K2")):
        if atco not in stops:
            continue
        late = late_at_entry + gains.get(atco, 0)
        sched = base + order * 300
        rows.append({"day": day.isoformat(), "operator": "SCSO", "service": "700", "direction": "westbound",
                     "trip_id": f"T{day}{n}", "data_version": "v", "method_version": 7, "timepoint": 1,
                     "estimated": False, "quality_flags": [], "match": "declared", "source_reports": [1],
                     "source_file_sha256": "s", "route_pattern": "P", "timing_point_order": order,
                     "stop_index": order * 3, "atco": atco, "stop_name": atco,
                     "scheduled_epoch": sched, "observed_epoch": sched + late, "lateness_secs": late,
                     "scheduled_secs": 39600 + order * 300,
                     "observed_interval_epoch": [sched + late, sched + late + 60]})
    return rows


def window(first, days, per_day, gains_for, **kw):
    rows = []
    for d in range(days):
        for n in range(per_day):
            rows += journey(first + timedelta(days=d), n, gains_for(d, n), **kw)
    return rows


def weekdays_from(day):
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return day


def during(gains_for, days=7, per_day=6, **kw):
    out, d = [], date(2026, 10, 5)                 # Monday 5 Oct to Friday 9 Oct, then on
    for k in range(days):
        day = weekdays_from(d)
        for n in range(per_day):
            out += journey(day, n, gains_for(k, n), **kw)
        d = day + timedelta(days=1)
    return out


def test_the_allowance_is_a_low_figure_and_kept_at_zero_where_it_wears_off():
    # 42 journeys losing 240 to 450 seconds by K1, and nothing by K2.
    rows = during(lambda k, n: {"K1": 240 + 5 * (k * 6 + n), "K2": 10})
    doc = bd.build(rows, [event()], COORDS)
    (svc,) = doc["adjustments"][0]["services"]
    assert svc["entry_atco"] == "E", "the entry point is the last stop before the closed area"
    points = {p["atco"]: p["cells"]["weekday|all"]["extra_secs"] for p in svc["points"]}
    gains = sorted(240 + 5 * i for i in range(42))
    assert points["K1"] == round(gains[int(42 * 0.15)] / 10) * 10, "the 15th percentile, not the median"
    assert points["K1"] < sorted(gains)[21], "a median would put the bus ahead of the estimate half the time"
    assert points["K2"] == 0, "where the loss has gone, zero, so a larger figure cannot stand in"


def test_an_unmeasured_entry_point_is_not_stood_in_for():
    rows = during(lambda k, n: {"K1": 300}, stops=("W", "K1", "K2"))
    assert bd.build(rows, [event()], COORDS)["adjustments"] == []


def test_too_few_days_and_unflagged_events_add_nothing():
    rows = during(lambda k, n: {"K1": 300}, days=4, per_day=10)
    assert bd.build(rows, [event()], COORDS)["adjustments"] == [], "under the five-day floor"
    rows = during(lambda k, n: {"K1": 300})
    assert bd.build(rows, [event(adjust_predictions=False)], COORDS)["adjustments"] == []


def test_the_periods_are_the_delay_maps_hours():
    for hour in range(24):
        epoch = datetime(2026, 10, 7, hour, 30, tzinfo=LONDON).timestamp()
        assert diversions.period_for(epoch) == entry_period(epoch), hour


# ── Applying it (api/diversions.py, api/live_eta.py) ────────────────

DOC = {"adjustments": [{
    "disruption_id": "x-closure", "operator": "SCSO", "from": "2026-09-01T00:00:00+01:00",
    "to": "2026-12-01T00:00:00+00:00", "reason": "Road closed", "stops_not_served": ["STOP3"],
    "services": [{"service": "700", "direction": "westbound", "entry_atco": "STOP2",
                  "points": [{"atco": "STOP4", "cells": {"weekday|all": {"extra_secs": 300}}},
                             {"atco": "STOP6", "cells": {"weekday|all": {"extra_secs": 120}}}]}]}]}


class OperatedTimetable(FakeTimetable):
    def noc_for_route(self, route_id):
        return "SCSO"


def test_only_events_in_force_apply_and_the_journey_is_matched_by_its_stops():
    now = datetime(2026, 10, 7, 12, tzinfo=timezone.utc)
    atcos = [f"STOP{i}" for i in range(8)]
    assert diversions.plan_for_trip(DOC, "SCSO", "700", atcos, now)["entry"] == 2
    assert diversions.plan_for_trip(DOC, "SCSC", "700", atcos, now), "Stagecoach's other code"
    assert diversions.plan_for_trip(DOC, "SCSO", "700", atcos, datetime(2026, 12, 2, tzinfo=timezone.utc)) is None
    assert diversions.plan_for_trip(DOC, "SCSO", "700", list(reversed(atcos)), now) is None, \
        "the other direction calls at the points before the entry, so it is not matched"
    assert diversions.plan_for_trip(DOC, "BHBC", "700", atcos, now) is None


def test_the_allowance_holds_to_the_first_point_then_follows_the_timetable():
    plan = diversions.plan_for_trip(DOC, "SCSO", "700", [f"STOP{i}" for i in range(8)],
                                    datetime(2026, 10, 7, 12, tzinfo=timezone.utc))
    secs = [i * 120 for i in range(8)]
    got = [diversions.extra_at(plan, j, "weekday", "10-16", secs) for j in range(8)]
    assert got == [0, 0, 0, 300, 300, 210, 120, 120], got
    assert diversions.extra_at(plan, 5, "weekend", "10-16", secs) == 0, "no figure for weekends"


def rows_for(bus, now, doc=DOC):
    return {r["stop_id"]: r for r in live_eta.project_trip(OperatedTimetable(), "VJ_1400", bus, now, doc)}


def late_min(r):
    return None if r["expected"] is None else round(
        (datetime.fromisoformat(r["expected"]) - datetime.fromisoformat(r["scheduled"])).total_seconds() / 60, 1)


def test_a_bus_short_of_the_diversion_is_expected_later_beyond_it():
    now = at(14, 2)                                # a Friday
    got = rows_for(declared(60, lat=50.831), now)
    assert late_min(got["STOP2"]) == 1, "nothing before the entry point"
    assert got["STOP3"]["not_served"] and got["STOP3"]["expected"] is None
    assert late_min(got["STOP4"]) == 6 and got["STOP4"]["allowance_secs"] == 300
    assert got["STOP4"]["allowance_reason"] == "Road closed"
    assert late_min(got["STOP7"]) == 3
    without = rows_for(declared(60, lat=50.831), now, doc=None)
    assert late_min(without["STOP4"]) == 1 and "allowance_secs" not in without["STOP4"]


def test_a_bus_on_the_diversion_counts_its_own_lateness_or_the_allowance_whichever_is_more():
    got = rows_for(declared(240, lat=50.833), at(14, 8))     # past the entry, short of STOP4
    assert late_min(got["STOP4"]) == 5, "4 min late already; the allowance says 5"
    got = rows_for(declared(420, lat=50.833), at(14, 8))
    assert late_min(got["STOP4"]) == 7 and "allowance_secs" not in got["STOP4"]


def test_a_bus_past_the_diversion_carries_only_its_own_lateness():
    got = rows_for(declared(360, lat=50.8355), at(14, 13))
    assert all("allowance_secs" not in r for r in got.values())


def test_the_published_file_is_refused_if_a_figure_could_mislead():
    import check_published as checks

    def failures(**cell):
        doc = {"schema_version": 1, "floor": {"journeys": 30, "distinct_days": 5}, "adjustments": [
            {"disruption_id": "x", "from": "2026-10-01T00:00:00+01:00", "to": "2026-11-01T00:00:00+00:00",
             "services": [{"entry_atco": "E", "points": [{"atco": "K", "cells": {"weekday|all": {
                 "extra_secs": 300, "journeys": 40, "days": 6, **cell}}}]}]}]}
        f = checks.Failures()
        checks.check_diversions(doc, "diversion-adjustments.json", f)
        return [c for c, _d in f.items]

    assert failures() == []
    assert failures(extra_secs=-60), "an allowance only ever adds"
    assert failures(extra_secs=7200), "two hours is not an allowance"
    assert failures(journeys=12), "below its floor"
    assert failures(extra_secs=300.5), "whole seconds"
