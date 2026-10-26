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
import rtig_windows as rw                                         # noqa: E402


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


def test_rtig_windows():
    # RTIGT041-1.0, 6.9: the range is how far ahead the predicted time was.
    assert [rw.label(s) for s in (0, 170, 180, 181, 1800, 1801)] == \
        ["already due", "0-3 min", "0-3 min", "3-6 min", "18-30 min", "over 30 min"]
    # Its worked example (7.3): at 09:00 the bus is due 09:07 and comes 09:09.
    # Seven minutes ahead allows 120 s early and 210 s late, so it is accurate.
    # Our error is estimate minus actual: that bus is -120.
    assert rw.classify(420, -120) == "ok"
    assert rw.classify(420, -211) == "late"
    assert rw.classify(420, 120) == "ok" and rw.classify(420, 121) == "early", \
        "a bus coming earlier than the allowance is the one someone misses"
    assert rw.classify(170, -61) == "late", "under 3 minutes ahead allows only 60 s either way"
    assert rw.classify(2400, 0) is None and rw.classify(-10, 0) is None, "RTIG sets no window there"
    assert [rw.rating(x) for x in (0.95, 0.85, 0.7, 0.69)] == \
        ["excellent", "good", "satisfactory", "unsatisfactory"]


def test_scores_use_rtig_windows_each_prediction_in_its_own_range():
    tally = bl.Tally()
    # Seven minutes ahead by the clock. The estimate said 7, 8.7, 9.2 and 2.8
    # minutes: three in 6-12, one in 0-3.
    for err in (0, 100, 130, -250):
        tally.add([("all", "clear")], ("d", "t"), err, 0, 420, scope="clear")
    report = tally.report()
    score = report[("all", "clear")]
    assert score["journeys"] == 1 and score["estimate"]["pairs"] == 4
    est = score["rtig"]["estimate"]
    assert (est["acceptable"], est["early"], est["late"]) == (0.5, 0.25, 0.25)
    assert score["rtig"]["timetable"]["acceptable"] == 1.0
    mid = report[("range", "clear", "6-12 min")]["rtig"]
    assert mid["estimate"]["scored"] == 3 and mid["estimate"]["early"] == 1 / 3
    assert mid["timetable"]["scored"] == 4, "the timetable's times sit in their own range"
    assert report[("range", "clear", "0-3 min")]["rtig"]["estimate"]["late"] == 1.0
    assert report[("range", "clear", "0-3 min")]["rtig"]["timetable"] is None
