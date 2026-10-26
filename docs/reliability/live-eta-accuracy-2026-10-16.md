# How close are our live arrival estimates? A replay, 16 October 2026

> **Superseded** by `live-eta-accuracy-2026-10-25.md`, which judges the same
> replay by RTIG's accuracy windows instead of a single "within 2 minutes",
> over 28 days. Kept as the record of what was published until then.

**Value.** Outside recorded disruptions, our live estimate of when a tracked bus
reaches a stop was within 2 minutes of when it left that stop **71% of the
time**, against 47% for the timetable alone. **12% of the time the bus came 2
or more minutes earlier than the estimate**, which is the case that makes
someone miss it: 6% when the estimate was under 5 minutes ahead, rising to 15%
at 20–40 minutes ahead. Counts are estimate/outcome pairs, not journeys; the
journeys behind each row are given.

**As of** 16 October 2026, from the published observation files for
25 September to 14 October 2026 (20 days, method 5 and later), code at
`7e42402` plus this change.

## Method

`scripts/backtest_live_eta.py` replays the live estimate's own function,
`api/live_eta.projected_lateness`, the one the stop board and the Bus tab use:

1. Every journey identified exactly (`match: declared`), because only those
   get a live estimate.
2. From every timing point the bus was recorded leaving (measured, no quality
   flags), take its lateness there as the bus's current lateness.
3. Estimate its lateness at every later stop on the journey the way the site
   does: late stays late; early is taken as on time from the next timing point.
4. Compare with the lateness actually recorded when the bus left each of those
   stops (measured, no quality flags). Error = estimate − actual; positive means
   the bus came earlier than estimated.
5. Group by how far ahead the estimate was made (time between the two
   departures), by operator and by service. The same pairs are scored for the
   timetable alone (an estimate of no lateness) as the yardstick.

Journeys through a recorded disruption (`data/analysis_exclusions.json`: the
Western Road works from 28 September) are scored separately, because the live
estimate now adds a diversion allowance there that this replay leaves out.

Reproduce:

```sh
python scripts/backtest_live_eta.py --observations 'obs/observations-*.json.gz' --out report.json
```

with the observation files listed in the published index
(`journey-times/index.json`, `observation_sources`).

## Caveats, and which way each one leans

- **Fresher than live, so these errors are a floor.** The replay starts the
  moment the bus left a timing point. A live estimate starts from the bus's
  last report, which arrives a median three minutes late, so live errors are
  larger, most of all for buses a few minutes away.
- **Departures, not arrivals.** Every recorded time is the last report within
  150 m of a stop. A bus that waits at a stop is timed leaving it. This affects
  the estimate and the outcome alike, and nothing here has been checked against
  a time someone saw (see the roadmap's hand-timed check).
- **Pairs are not independent.** One journey contributes up to a few hundred
  pairs, so a handful of unusual journeys can move a service's row. The
  journeys column says how many there are.
- **Identified journeys only.** A journey worked out from position gets no live
  estimate and is not replayed.

## Results

Days replayed: 2026-09-25 to 2026-10-14 (20 days).

### All journeys outside recorded disruptions

| Scope | Pairs | Journeys | Typical error (min) | 9 in 10 within (min) | Bias (min) | Within 2 min | **Bus 2+ min earlier** | Bus 2+ min later | Timetable alone: within 2 min |
|---|---|---|---|---|---|---|---|---|---|
| All | 2,803,361 | 48,728 | 1.2 | 4.5 | -0.2 | 71% | **12%** | 21% | 47% |

### By how far ahead the estimate was made

| Ahead | Pairs | Journeys | Typical error (min) | 9 in 10 within (min) | Bias (min) | Within 2 min | **Bus 2+ min earlier** | Bus 2+ min later | Timetable alone: within 2 min |
|---|---|---|---|---|---|---|---|---|---|
| 0-5 min | 580,175 | 48,623 | 0.7 | 1.7 | +0.2 | 95% | **6%** | 2% | 55% |
| 5-10 min | 548,521 | 47,102 | 1.0 | 2.7 | +0.0 | 84% | **11%** | 10% | 52% |
| 10-20 min | 783,466 | 43,091 | 1.3 | 4.0 | -0.2 | 69% | **13%** | 22% | 47% |
| 20-40 min | 689,642 | 30,764 | 2.0 | 6.3 | -0.8 | 52% | **15%** | 36% | 41% |
| over 40 min | 201,557 | 10,615 | 3.0 | 9.5 | -2.0 | 39% | **14%** | 50% | 32% |

### By operator

| Operator | Pairs | Journeys | Typical error (min) | 9 in 10 within (min) | Bias (min) | Within 2 min | **Bus 2+ min earlier** | Bus 2+ min later | Timetable alone: within 2 min |
|---|---|---|---|---|---|---|---|---|---|
| BHBC | 2,071,937 | 36,765 | 1.0 | 4.0 | -0.2 | 74% | **8%** | 21% | 49% |
| COMT | 302,703 | 2,777 | 2.0 | 7.3 | +0.0 | 54% | **28%** | 22% | 35% |
| METR | 37,722 | 1,759 | 1.5 | 5.3 | -1.2 | 61% | **4%** | 40% | 24% |
| SCSO | 390,999 | 7,427 | 1.2 | 4.7 | +0.0 | 69% | **19%** | 19% | 50% |

### The services where buses most often came 2+ min earlier than estimated (at least 2,000 pairs)

| Service | Pairs | Journeys | Typical error (min) | 9 in 10 within (min) | Bias (min) | Within 2 min | **Bus 2+ min earlier** | Bus 2+ min later | Timetable alone: within 2 min |
|---|---|---|---|---|---|---|---|---|---|
| 37B COMT | 51,515 | 292 | 3.7 | 9.7 | +1.3 | 34% | **47%** | 21% | 24% |
| 37 COMT | 66,971 | 820 | 3.0 | 11.0 | +1.3 | 41% | **46%** | 16% | 29% |
| 743 COMT | 2,259 | 34 | 2.3 | 7.2 | +0.2 | 46% | **35%** | 25% | 21% |
| 7 SCSO | 28,879 | 163 | 2.0 | 6.7 | +0.2 | 53% | **29%** | 24% | 36% |
| 11 SCSO | 7,988 | 1,012 | 1.0 | 3.0 | +1.0 | 80% | **28%** | 4% | 59% |
| 16 COMT | 46,031 | 527 | 2.0 | 6.7 | -0.3 | 53% | **25%** | 26% | 30% |
| 700 SCSO | 72,013 | 598 | 1.3 | 5.0 | +0.2 | 65% | **22%** | 19% | 54% |
| 5 SCSO | 38,773 | 1,465 | 1.0 | 3.0 | +0.8 | 80% | **20%** | 7% | 61% |
| 10 SCSO | 22,200 | 709 | 1.0 | 3.3 | +0.3 | 77% | **19%** | 12% | 67% |
| 9 SCSO | 57,870 | 357 | 1.7 | 5.7 | +0.0 | 61% | **18%** | 27% | 34% |
| 52 COMT | 51,875 | 202 | 1.2 | 4.3 | +0.0 | 69% | **18%** | 18% | 56% |
| N700 SCSO | 2,249 | 22 | 1.0 | 4.2 | +0.2 | 73% | **18%** | 14% | 64% |
| 1 SCSO | 10,242 | 480 | 1.0 | 3.8 | +0.0 | 76% | **17%** | 16% | 47% |
| 46 BHBC | 70,169 | 383 | 1.5 | 5.7 | -0.2 | 62% | **17%** | 25% | 47% |
| 701 SCSO | 138,434 | 2,083 | 1.2 | 4.2 | +0.0 | 70% | **15%** | 21% | 52% |

### Journeys through a recorded disruption (no allowance applied in this replay)

| Scope | Pairs | Journeys | Typical error (min) | 9 in 10 within (min) | Bias (min) | Within 2 min | **Bus 2+ min earlier** | Bus 2+ min later | Timetable alone: within 2 min |
|---|---|---|---|---|---|---|---|---|---|
| All | 3,468,227 | 20,557 | 1.8 | 7.8 | -0.7 | 55% | **12%** | 37% | 34% |


Inside the disruption (no allowance applied here), the 700 came 2+ min earlier
than estimated on 24% of pairs (1,877 journeys), Brighton & Hove's routes on 11%.
The allowance's own backtest is `diversion-adjustments-backtest-2026-10-15.md`.

## What it says

- The estimate is a large improvement on the timetable at every distance, and
  is best within 10 minutes of the bus (84–95% within 2 minutes).
- It errs both ways, more often late than early overall (bias −0.2 min), but the
  early side is what matters: **about one estimate in eight lets a bus arrive 2+
  minutes sooner than it said**. The board's advice to be at the stop by the
  timetabled time is doing real work.
- **Compass's 37 and 37B are the outliers**: the bus came 2+ min earlier than
  estimated on 46–47% of pairs. Late Compass buses recover time between timing
  points far more than the model assumes. Stagecoach's 7, 11 and 700 are next
  (22–29%).
- Further ahead, buses recover lateness the model carries forward unchanged,
  which is the roadmap's "per-stop estimates from measured running times" item.
  These figures give it a baseline to beat.

Not yet published on the site: these are a first measurement of our own
method, and the hand-timed check of stop times should come first.
