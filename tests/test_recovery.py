"""api/recovery.py and scripts/build_running_recovery.py: lateness that changes
between timing points, measured, and allowed for only where it was measured."""

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from api import recovery                                           # noqa: E402
import build_running_recovery as br                               # noqa: E402

MONDAY = date(2026, 10, 26)
CALLS = [(36000, "A"), (36300, "x"), (36600, "B"), (36900, "y"), (37200, "C")]
TPS = [1, 0, 1, 0, 1]


def cell(frm, to, late="3-6 min"):
    return f"COMT|37|{frm}|{to}|weekday|midday|{late}"


def test_the_change_applies_from_the_timing_point_each_call_has_reached():
    doc = {"cells": {cell("A", "B"): -120, cell("A", "C"): -200}}
    adj = recovery.adjustments(doc, "CMPA", "37", CALLS, TPS, 0, 1, 240, MONDAY)
    # x is before the next timing point: no change; B and y take A->B; C takes A->C.
    assert adj == [0, -120, -120, -200], "the operator family or the timing points were not matched"


def test_nothing_measured_means_todays_estimate():
    assert recovery.adjustments({"cells": {}}, "COMT", "37", CALLS, TPS, 0, 1, 240, MONDAY) == [0, 0, 0, 0]
    assert recovery.adjustments({"cells": {cell("A", "B"): -120}}, "COMT", "37", CALLS, TPS, 0, 1, -200, MONDAY) \
        == [0, 0, 0, 0], "an early bus is held at timing points; its lateness is not adjusted"


def test_a_late_bus_can_make_up_its_lateness_but_not_run_early_on_it():
    assert recovery.apply([240, 240, None], [-120, -300, 0]) == [120, 0, None]
    assert recovery.apply([30], [60]) == [90], "a bus losing time was not allowed for"


def test_the_build_takes_every_pair_of_timing_points():
    rows = [{"day": "2026-10-26", "operator": "COMT", "service": "37", "atco": a, "scheduled_secs": s,
             "lateness_secs": late} for (s, a), late in zip([CALLS[0], CALLS[2], CALLS[4]], [240, 120, 60])]
    changes = dict(br.changes(rows))
    assert changes == {cell("A", "B"): -120, cell("A", "C"): -180, cell("B", "C", "1-3 min"): -60}


def test_a_cell_needs_enough_journeys_on_enough_days(tmp_path):
    import gzip
    import json

    def day_file(day, n):
        obs = []
        for t in range(n):
            for (s, a), late in zip([CALLS[0], CALLS[2]], [240, 60]):
                obs.append({"day": day, "trip_id": f"T{t}", "operator": "COMT", "service": "37", "atco": a,
                            "stop_index": s, "scheduled_secs": s, "lateness_secs": late, "timepoint": 1,
                            "match": "declared", "estimated": False, "quality_flags": [], "observed_epoch": 0})
        path = tmp_path / f"observations-{day}.json.gz"
        path.write_bytes(gzip.compress(json.dumps({"day": day, "observations": obs}).encode()))
        return str(path)
    four_days = [day_file(f"2026-10-0{d}", 10) for d in range(5, 9)]
    assert br.build(four_days, 70, events=[], coords={})["cells"] == {}, "four days were enough"
    five_days = four_days + [day_file("2026-10-09", 10)]
    assert br.build(five_days, 70, events=[], coords={})["cells"] == {cell("A", "B"): -180}
