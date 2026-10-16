"""scripts/backtest_live_eta.py: replaying the live estimate against recorded days.

The replay must use the site's own estimate (api/live_eta.projected_lateness),
score the sign the right way round (positive: the bus came earlier than the
estimate, so someone trusting it missed it), and leave out what the live site
would not estimate from.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import backtest_live_eta as bl                                    # noqa: E402


def call(i, late, epoch, tp=1, **over):
    return {"stop_index": i, "timepoint": tp, "match": "declared", "estimated": False,
            "quality_flags": [], "lateness_secs": late, "observed_epoch": epoch, **over}


def test_a_late_bus_that_recovers_counts_as_earlier_than_estimated():
    # Five minutes late at the first timing point, on time ten minutes later:
    # the estimate said five minutes late, so the bus came five minutes early.
    pairs = bl.replay([call(0, 300, 1000), call(1, 0, 1600)])
    assert pairs == [(600, 300, 0)]


def test_an_early_bus_is_estimated_on_time_from_its_next_timing_point():
    # Two minutes early at a timing point; the next stop is not a timing
    # point, the one after is. The site says early until then, on time after.
    calls = [call(0, -120, 1000), call(1, -120, 1300, tp=0), call(2, 0, 1600)]
    assert [err for _, err, _ in bl.replay(calls)] == [0, 0]
    calls[2]["lateness_secs"] = -120
    assert [err for _, err, _ in bl.replay(calls)][-1] == 120, "an early bus past a timing point is the risky one"


def test_only_what_the_site_would_estimate_from_is_replayed():
    inferred = [call(0, 300, 1000, match="inferred"), call(1, 0, 1600, match="inferred")]
    assert bl.replay(inferred) == [], "a journey worked out from position gets no live estimate"
    # Not anchored at a stop that is not a timing point, or an interpolated or
    # flagged one; none of them is a target either.
    calls = [call(0, 300, 1000, tp=0), call(1, 300, 1300, estimated=True),
             call(2, 300, 1500, quality_flags=["unbounded_departure"]), call(3, 0, 1600)]
    assert bl.replay(calls) == []


def test_horizons_and_scores():
    assert bl.horizon_of(240) == "0-5 min"
    assert bl.horizon_of(1500) == "20-40 min"
    assert bl.horizon_of(3000) == "over 40 min"
    tally = bl.Tally()
    for err in (0, 30, 130, -150):
        tally.add([("all", "clear")], ("d", "t"), err, 0)
    score = tally.report()[("all", "clear")]
    assert score["journeys"] == 1
    est = score["estimate"]
    assert est["pairs"] == 4
    assert est["bus_2_min_earlier"] == 0.25 and est["bus_2_min_later"] == 0.25
    assert est["within_2_min"] == 0.5
    assert score["timetable"]["within_2_min"] == 1.0
