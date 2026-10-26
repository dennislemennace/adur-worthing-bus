"""RTIG's accuracy windows for a real-time prediction.

From RTIG's *Accuracy and Quality of Real Time Predictions* (RTIGT041-1.0,
December 2020), section 6.9, its recommended starting point. A prediction is
put in a range by how far ahead the time it gave was when it was made ("time
to prediction", which the report recommends in 7.7 because it is what the
passenger saw), and is acceptable if the bus came no more than the allowance
earlier, nor more than the allowance later, than it said. Allowances widen
with the range, and a bus coming early is allowed less than one coming late,
because early is the error that loses someone their bus.

The report sets nothing beyond 30 minutes ahead, so those predictions are not
scored, nor is one whose time had already come when it was made.

Error here is estimate minus actual, the sign the backtests use: positive
means the bus came earlier than the estimate. RTIG writes it the other way
round (actual minus predicted), so its "early" is our positive.
"""
from typing import Optional

# (from, to] minutes ahead, then how many seconds early and late are allowed.
RANGES = ((0, 3, 60, 60), (3, 6, 90, 120), (6, 12, 120, 210), (12, 18, 150, 240), (18, 30, 180, 360))
LABELS = [f"{lo}-{hi} min" for lo, hi, _e, _l in RANGES]

# Section 8.8's suggested starting point for judging a share acceptable. It
# gives 70-80% as satisfactory and "70% and below" as unsatisfactory; exactly
# 70% is read here as satisfactory.
RATINGS = ((0.9, "excellent"), (0.8, "good"), (0.7, "satisfactory"), (0.0, "unsatisfactory"))


def _range(ahead_secs: float) -> Optional[tuple]:
    for r in RANGES:
        if r[0] * 60 < ahead_secs <= r[1] * 60:
            return r
    return None


def label(ahead_secs: float) -> str:
    """The range a prediction this many seconds ahead belongs to."""
    if ahead_secs <= 0:
        return "already due"
    r = _range(ahead_secs)
    return f"{r[0]}-{r[1]} min" if r else "over 30 min"


def classify(ahead_secs: float, error_secs: float) -> Optional[str]:
    """`"ok"`, `"early"` (the bus came before the allowance) or `"late"`, or
    None for a prediction RTIG's ranges do not cover."""
    r = _range(ahead_secs)
    if r is None:
        return None
    if error_secs > r[2]:
        return "early"
    if error_secs < -r[3]:
        return "late"
    return "ok"


def rating(share: Optional[float]) -> Optional[str]:
    if share is None:
        return None
    return next(name for floor, name in RATINGS if share >= floor)


def shares(counts) -> Optional[dict]:
    """Acceptable, early and late as shares of the predictions scored, from a
    Counter of `classify` results; None when nothing was scored."""
    n = counts.get("ok", 0) + counts.get("early", 0) + counts.get("late", 0)
    if not n:
        return None
    ok = counts.get("ok", 0) / n
    return {"scored": n, "unscored": counts.get(None, 0), "acceptable": ok,
            "early": counts.get("early", 0) / n, "late": counts.get("late", 0) / n,
            "rating": rating(ok)}
