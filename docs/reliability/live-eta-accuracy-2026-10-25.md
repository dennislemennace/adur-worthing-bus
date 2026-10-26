# How close are our live arrival estimates? Judged by RTIG's windows, 25 October 2026

**Value.** Outside recorded disruptions, **83% of our live estimates** of when a
tracked bus reaches a stop were within the accuracy window RTIG recommends for
how far ahead the estimate was, against **64% for the timetable alone**.
**8% of the time the bus came earlier than the window allows**, which is the
case that makes someone miss it, and 9% later. Counts are estimate/outcome
pairs, not journeys; the journeys behind each row are given. This replaces the
single "within 2 minutes" measure of `live-eta-accuracy-2026-10-16.md`.

**As of** 25 October 2026, from the published observation files for
25 September to 24 October 2026 (28 days; 18 and 19 October were not recorded;
recording methods 5 to 7; 7 timetable builds, each named in its file), code at
`476e818` plus this change.

## The windows

RTIG (the Real Time Information Group), *Accuracy and Quality of Real Time
Predictions*, RTIGT041-1.0, December 2020, section 6.9, the recommended
starting point:

| How far ahead the estimate said the bus was | Bus may come this much earlier | or this much later |
|---|---|---|
| 0–3 minutes | 60 s | 60 s |
| 3–6 minutes | 90 s | 120 s |
| 6–12 minutes | 120 s | 210 s |
| 12–18 minutes | 150 s | 240 s |
| 18–30 minutes | 180 s | 360 s |

- **The range is set by the estimate, not by the outcome.** RTIG calls this
  "time to prediction" and recommends it (section 7.7) because it is what the
  passenger saw: an estimate that said 7 minutes is judged as a 7-minute
  estimate, whenever the bus really came. The timetable's own time for the
  same stop is put in its own range the same way.
- **Nothing beyond 30 minutes is scored**, because RTIG sets no window there,
  and nor is an estimate whose time had already come when it was made. Both
  are counted in the tables and left out of the shares: 500,006 estimates
  (14%) were more than 30 minutes ahead.
- **Ratings** are RTIG's suggested starting point (section 8.8): 90% and above
  excellent, 80–90% good, 70–80% satisfactory, below 70% unsatisfactory. The
  report gives 70% to both satisfactory and unsatisfactory; we read exactly
  70% as satisfactory.
- **Sign.** RTIG writes error as actual minus predicted. The tables keep this
  repository's sign, estimate minus actual, and say "bus came early" or "bus
  came late" in words, so no reader has to know either.

The windows are in `scripts/rtig_windows.py`, which both backtests use.

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
   stops (measured, no quality flags).
5. Judge each estimate by RTIG's window for how far ahead it said the bus was,
   and group by that range, by operator and by service. The same pairs are
   judged for the timetable alone (an estimate of no lateness) as the yardstick.

Journeys through a recorded disruption (`data/analysis_exclusions.json`) are
scored separately, because the live estimate adds a diversion allowance there
that this replay leaves out.

Reproduce:

```sh
python scripts/backtest_live_eta.py --observations 'obs/observations-*.json.gz' --out report.json
```

with the observation files for those days listed in the published index
(`journey-times/index.json`, `observation_sources`), each checked against its
published SHA-256 before use.

## Caveats, and which way each one leans

- **Fresher than live, so these shares are a ceiling.** The replay starts the
  moment the bus left a timing point. A live estimate starts from the bus's
  last report, which arrives a median three minutes late, so live estimates
  are less accurate, most of all for buses a few minutes away: exactly the
  0–3 minute range, where RTIG's window is tightest.
- **Departures, not arrivals.** Every recorded time is the last report within
  150 m of a stop, so a bus that waits at a stop is timed leaving it. RTIG's
  windows are written for arrival at the stop. A bus that dwells counts as
  later than it reached the stop, which leans towards "late" in the shortest
  ranges (18% late at 0–3 minutes), and nothing here has yet been checked
  against a time someone saw.
- **RTIG's windows are a starting point, not a pass mark.** The report says to
  adjust them to the service; we use them unchanged so the figures can be
  compared with anyone else who does the same.
- **Pairs are not independent.** One journey contributes up to a few hundred
  pairs, so a handful of unusual journeys can move a service's row. The
  journeys column says how many there are.
- **Identified journeys only.** A journey worked out from position gets no live
  estimate and is not replayed.

## Results

Days replayed: 2026-09-25 to 2026-10-24 (28 days).

### All journeys outside recorded disruptions

| Scope | Pairs | Journeys | Typical error (min) | 9 in 10 within (min) | Bias (min) | Acceptable (RTIG) | **Bus came early** | Bus came late | Rating | Timetable alone: acceptable |
|---|---|---|---|---|---|---|---|---|---|---|
| All | 3,648,077 | 66,153 | 1.2 | 4.5 | -0.2 | 83% | **8%** | 9% | good | 64% |

### By how far ahead the estimate said the bus was

| Ahead, as the estimate said | Allowed early / late | Estimates | Acceptable | **Bus came early** | Bus came late | Rating | Timetable times this far ahead | Timetable alone: acceptable |
|---|---|---|---|---|---|---|---|---|
| 0-3 min | 60 s / 60 s | 467,659 | 77% | **5%** | 18% | satisfactory | 451,842 | 36% |
| 3-6 min | 90 s / 120 s | 489,999 | 83% | **8%** | 9% | good | 451,867 | 56% |
| 6-12 min | 120 s / 210 s | 844,216 | 86% | **8%** | 6% | good | 745,311 | 69% |
| 12-18 min | 150 s / 240 s | 621,006 | 82% | **9%** | 9% | good | 526,017 | 72% |
| 18-30 min | 180 s / 360 s | 714,437 | 84% | **9%** | 7% | good | 616,738 | 79% |
| over 30 min | none set | 500,006 | – | **–** | – | – | 408,763 | – |
| already due | none set | 10,754 | – | **–** | – | – | 447,539 | – |

### By operator

| Operator | Pairs | Journeys | Typical error (min) | 9 in 10 within (min) | Bias (min) | Acceptable (RTIG) | **Bus came early** | Bus came late | Rating | Timetable alone: acceptable |
|---|---|---|---|---|---|---|---|---|---|---|
| BHBC | 2,623,447 | 49,113 | 1.0 | 4.0 | -0.2 | 86% | **6%** | 8% | good | 66% |
| COMT | 445,266 | 4,095 | 1.8 | 7.0 | +0.0 | 67% | **17%** | 16% | unsatisfactory | 52% |
| METR | 53,083 | 2,502 | 1.5 | 5.3 | -1.2 | 66% | **4%** | 29% | unsatisfactory | 41% |
| SCSO | 526,281 | 10,443 | 1.2 | 4.5 | +0.0 | 78% | **13%** | 9% | satisfactory | 68% |

### The services where buses most often came earlier than RTIG allows (at least 2,000 pairs)

| Service | Pairs | Journeys | Typical error (min) | 9 in 10 within (min) | Bias (min) | Acceptable (RTIG) | **Bus came early** | Bus came late | Rating | Timetable alone: acceptable |
|---|---|---|---|---|---|---|---|---|---|---|
| 743 COMT | 2,784 | 45 | 2.2 | 7.2 | +0.2 | 51% | **30%** | 19% | unsatisfactory | 43% |
| 37 COMT | 102,390 | 1,196 | 2.3 | 9.8 | +0.5 | 53% | **30%** | 17% | unsatisfactory | 45% |
| 11 SCSO | 11,426 | 1,457 | 1.0 | 3.0 | +1.0 | 68% | **26%** | 6% | unsatisfactory | 63% |
| 37B COMT | 74,100 | 405 | 3.2 | 9.2 | +0.3 | 52% | **24%** | 24% | unsatisfactory | 36% |
| 7 SCSO | 41,586 | 234 | 2.0 | 6.3 | +0.2 | 71% | **18%** | 11% | satisfactory | 61% |
| 16 COMT | 69,252 | 819 | 1.8 | 6.3 | -0.3 | 65% | **18%** | 17% | unsatisfactory | 54% |
| N48 BHBC | 2,196 | 144 | 0.8 | 2.7 | +0.5 | 80% | **17%** | 4% | satisfactory | 85% |
| 5 SCSO | 54,397 | 2,039 | 1.0 | 3.0 | +0.8 | 81% | **16%** | 4% | good | 73% |
| 10 SCSO | 31,954 | 1,017 | 1.0 | 3.7 | +0.3 | 76% | **15%** | 9% | satisfactory | 75% |
| 1 SCSO | 14,493 | 680 | 1.0 | 3.8 | +0.0 | 72% | **14%** | 14% | satisfactory | 63% |
| N25 BHBC | 14,696 | 705 | 0.8 | 2.3 | +0.3 | 82% | **14%** | 4% | good | 74% |
| 700 SCSO | 75,639 | 735 | 1.3 | 5.0 | +0.2 | 80% | **13%** | 6% | good | 71% |
| 27B BHBC | 2,892 | 34 | 1.2 | 3.0 | +0.7 | 84% | **13%** | 4% | good | 83% |
| 9 SCSO | 83,793 | 530 | 1.7 | 5.2 | +0.0 | 78% | **11%** | 11% | satisfactory | 56% |
| 701 SCSO | 192,373 | 2,911 | 1.2 | 4.2 | +0.0 | 80% | **11%** | 10% | satisfactory | 69% |

### Journeys through a recorded disruption (no allowance applied in this replay)

| Scope | Pairs | Journeys | Typical error (min) | 9 in 10 within (min) | Bias (min) | Acceptable (RTIG) | **Bus came early** | Bus came late | Rating | Timetable alone: acceptable |
|---|---|---|---|---|---|---|---|---|---|---|
| All | 5,195,660 | 33,546 | 1.8 | 8.0 | -0.7 | 78% | **9%** | 14% | satisfactory | 54% |

## What it says

- By RTIG's measure the estimate rates **good overall (83%)** and beats the
  timetable in every range, by most where the passenger is closest: 77%
  against 36% under 3 minutes away.
- The two measures disagree most at distance. The old "within 2 minutes"
  measure called estimates 20–40 minutes out poor (52%); RTIG allows a bus
  that far away 3 minutes early and 6 late, and by that measure they are as
  good as the rest. Near the stop it is the other way round: RTIG allows only
  a minute either way.
- **Brighton & Hove's routes rate good (86%); Stagecoach satisfactory (78%);
  Compass (67%) and Metrobus (66%) unsatisfactory.** Compass's 37, 37B and 743
  are still the outliers for buses coming early (24–30%): late Compass buses
  recover time between timing points far more than the model assumes.
  Metrobus errs the other way, late on 29% of estimates.
- The shortest range is the weakest (77%), mostly buses leaving later than
  estimated, which is partly the departures-not-arrivals caveat above.
- These shares are a ceiling for the live site (see the first caveat). The
  roadmap's "per-stop estimates from measured running times" item now has a
  baseline in the industry's terms to beat.
