# Diversion allowances in live estimates: backtest, 15 October 2026

## What was tested

The Western Road closure (Hove, from 28 September 2026) sends buses towards
Worthing round Seven Dials. Live estimates (`api/live_eta.py`) carry a bus's
lateness forward unchanged, so a bus still east of the closure was promised at
the stops beyond it as if the closure were not there.

`scripts/build_diversion_adjustments.py` measures, for each recorded event
marked `adjust_predictions`, how much lateness buses gained between the last
timing point before the closed area (the entry point) and each timing point
after it, during the event. `api/diversions.py` adds that to estimates for a
bus that has not yet reached the entry point.

`scripts/backtest_diversion_adjustments.py` calibrated the allowance on the
days before 10 October and scored it on 10 to 14 October, days it never saw.
For every recorded journey through the area, it compared two estimates made as
the bus left the entry point, today's and today's plus the allowance, against
when the bus really left each timing point beyond. Measured timing points of
declared journeys only, as the delay map uses.

## Choices the backtest made

- **The median was rejected.** It cut the typical error from 2.7 to 1.9
  minutes, but buses came 2 or more minutes *before* the estimate 17% of the
  time, against 2% without it. That is the error that loses someone their bus.
- **The 15th percentile was kept.** Nearly all the gain (typical error 2.0
  minutes; 50% of estimates within two minutes, against 41%) for 3% of buses
  coming 2 or more minutes before the estimate.
- **An allowance only adds.** Negative figures made eastbound estimates worse;
  they are now dropped.
- **Where the effect has worn off, zero is kept,** not left out. Leaving such
  points out let a larger figure either side stand in for them, which made the
  700 worse at the 10th percentile than the 15th.
- **Not a difference from the weeks before.** The week before the closure held
  an event of its own (21 to 25 September: the 700 was losing 3 to 10 minutes
  by Beach Green), which made the closure look like a gain there.

## Result (15th percentile, additions only)

Calibrated on days before 2026-10-10 (2026-09-20 to 2026-10-09); tested on 2026-10-10 to 2026-10-14.

| Service, direction | Timing points scored | Median error, today → adjusted (min) | Within 2 min | Bus 2+ min earlier than estimate | Bus 2+ min later than estimate |
|---|---|---|---|---|---|
| **All** | 19760 | 2.7 → 2.0 | 41% → 50% | 2% → 3% | 58% → 47% |
| 1 eastbound | 2079 | 1.1 → 1.1 | 69% → 69% | 4% → 4% | 27% → 27% |
| 1 westbound | 1270 | 4.3 → 2.8 | 19% → 38% | 0% → 4% | 81% → 59% |
| 1X eastbound | 1332 | 2.0 → 1.7 | 51% → 56% | 1% → 1% | 48% → 43% |
| 1X westbound | 901 | 2.9 → 1.9 | 31% → 52% | 0% → 2% | 69% → 46% |
| 2 eastbound | 609 | 1.7 → 1.5 | 57% → 61% | 1% → 1% | 42% → 38% |
| 2 westbound | 1489 | 4.5 → 3.6 | 20% → 31% | 1% → 2% | 80% → 67% |
| 21 eastbound | 595 | 1.4 → 1.4 | 60% → 61% | 8% → 8% | 32% → 32% |
| 21 westbound | 296 | 1.1 → 1.1 | 66% → 67% | 0% → 0% | 35% → 33% |
| 25 eastbound | 206 | 0.9 → 0.9 | 78% → 78% | 4% → 4% | 18% → 18% |
| 25 westbound | 49 | 4.0 → 2.0 | 14% → 51% | 0% → 0% | 86% → 49% |
| 46 eastbound | 868 | 1.7 → 1.6 | 56% → 59% | 3% → 3% | 41% → 38% |
| 46 westbound | 1013 | 4.0 → 3.0 | 29% → 35% | 4% → 4% | 68% → 61% |
| 49 eastbound | 1013 | 1.2 → 1.2 | 65% → 65% | 2% → 2% | 33% → 33% |
| 49 westbound | 601 | 5.3 → 2.6 | 6% → 40% | 0% → 2% | 95% → 57% |
| 5 eastbound | 809 | 1.8 → 1.7 | 55% → 57% | 2% → 2% | 44% → 41% |
| 5 westbound | 438 | 5.0 → 3.4 | 10% → 32% | 0% → 3% | 89% → 64% |
| 5A eastbound | 607 | 2.0 → 1.9 | 50% → 52% | 2% → 2% | 48% → 47% |
| 5A westbound | 542 | 5.0 → 3.0 | 15% → 36% | 0% → 7% | 86% → 57% |
| 5B eastbound | 1539 | 1.9 → 1.8 | 52% → 54% | 2% → 2% | 46% → 45% |
| 5B westbound | 1092 | 5.0 → 3.3 | 10% → 34% | 0% → 3% | 90% → 64% |
| 6 eastbound | 243 | 1.2 → 1.2 | 67% → 67% | 0% → 0% | 33% → 33% |
| 6 westbound | 630 | 4.6 → 3.1 | 15% → 36% | 0% → 2% | 85% → 62% |
| 60 eastbound | 3 | 0.9 → 0.9 | 100% → 100% | 0% → 0% | 0% → 0% |
| 700 westbound | 1422 | 3.6 → 2.4 | 36% → 45% | 1% → 3% | 63% → 53% |
| 71 eastbound | 15 | 1.3 → 1.3 | 67% → 67% | 0% → 0% | 33% → 33% |
| 71 westbound | 8 | 4.4 → 4.4 | 25% → 25% | 0% → 0% | 75% → 75% |
| 71A westbound | 7 | 3.8 → 3.8 | 14% → 14% | 0% → 0% | 86% → 86% |
| N1 eastbound | 40 | 0.6 → 0.6 | 98% → 98% | 0% → 0% | 2% → 2% |
| N1 westbound | 21 | 0.4 → 0.4 | 100% → 100% | 0% → 0% | 0% → 0% |
| N5 eastbound | 15 | 1.2 → 1.2 | 73% → 73% | 13% → 13% | 20% → 20% |
| N5 westbound | 8 | 0.8 → 0.8 | 62% → 62% | 0% → 0% | 38% → 38% |

"Bus 2+ min earlier than estimate" is the count that matters most: someone who
trusts the estimate and arrives on its time has missed that bus. The worst rise
is the 5A westbound, 0% to 7%. Rows unchanged between the two estimates had no
allowance: no figure cleared the floor, or there was no effect to allow for.

## Caveats

- Estimates here are made at the entry point with the bus's lateness there. On
  the road, an estimate is made from wherever the bus last reported, often
  before the entry point, so real errors are somewhat larger for both.
- A bus already on the diversion is given its own lateness or the allowance,
  whichever is more. That rule is not tested here: timing points do not show
  where a bus is mid-diversion.
- Everything that changed during the event is counted as the event's.
- Five test days, one event. The nightly build recomputes the figures; this
  test was not repeated automatically.

Reproduce with the observation files for 20 September to 14 October:

    python scripts/backtest_diversion_adjustments.py --observations 'obs/*.json.gz' --split 2026-10-10

## Rescored by RTIG's accuracy windows, 25 October 2026

The backtest now judges each estimate by the window RTIG recommends for how far
ahead it said the bus was (RTIGT041-1.0, December 2020, section 6.9;
`scripts/rtig_windows.py`; see `live-eta-accuracy-2026-10-25.md` for the
windows), instead of a single 2 minutes either way. Same test days and the
same estimates: the 19,760 timing points and the typical error, 2.7 → 2.0
minutes, are unchanged. Run from the observation files for 25 September to
14 October (the closure began on 28 September, so the calibration is the same).

| Service, direction | Timing points scored | Median error, today → adjusted (min) | Acceptable (RTIG) | Bus came early | Bus came late |
|---|---|---|---|---|---|
| **All** | 19760 | 2.7 → 2.0 | 66% → 78% | 0% → 1% | 33% → 21% |
| 1 westbound | 1270 | 4.3 → 2.8 | 53% → 74% | 0% → 2% | 47% → 24% |
| 1X westbound | 901 | 2.9 → 1.9 | 54% → 78% | 0% → 1% | 46% → 21% |
| 2 westbound | 1489 | 4.5 → 3.6 | 40% → 62% | 0% → 1% | 59% → 37% |
| 25 westbound | 49 | 4.0 → 2.0 | 20% → 78% | 0% → 0% | 80% → 22% |
| 46 westbound | 1013 | 4.0 → 3.0 | 39% → 59% | 0% → 0% | 60% → 40% |
| 49 westbound | 601 | 5.3 → 2.6 | 15% → 62% | 0% → 1% | 85% → 36% |
| 5 westbound | 438 | 5.0 → 3.4 | 37% → 64% | 0% → 1% | 62% → 35% |
| 5A westbound | 542 | 5.0 → 3.0 | 43% → 67% | 0% → 3% | 57% → 30% |
| 5B westbound | 1092 | 5.0 → 3.3 | 38% → 62% | 0% → 1% | 62% → 37% |
| 6 westbound | 630 | 4.6 → 3.1 | 52% → 73% | 0% → 1% | 47% → 27% |
| 700 westbound | 1422 | 3.6 → 2.4 | 45% → 68% | 0% → 1% | 55% → 31% |

Westbound rows, where the allowance applies; eastbound rows change by at most a
few points, as before. RTIG allows a bus far from the stop more than 2 minutes
early, so fewer estimates count as "bus came early" than under the old measure
(1%, not 3%, with the allowance); the worst rise is still the 5A westbound,
now 0% to 3%. The allowance's choice (the 15th percentile) was made on the old
measure and is not revisited here.
