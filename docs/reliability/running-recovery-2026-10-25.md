# Allowing for lateness that changes between timing points: test, 25 October 2026

## What was tested

Live estimates (`api/live_eta.py`) carry a bus's lateness forward unchanged.
Buses do not: late Compass and Stagecoach buses make up time between timing
points, others lose it. RTIG's guidance (RTIGT041-1.0, 3.3.7) lists historical
journey times by time of day as the first thing a prediction should use beyond
the timetable.

`scripts/build_running_recovery.py` measures, from journeys identified exactly
and outside recorded disruptions, the change in lateness from each timing point
to each later one, by route, day type, time band and how late the bus was at
the first (`api/recovery.py`). A group needs 30 journeys from 5 days, and a
change of at least 30 seconds, to be used. A late bus may make up its lateness
but not run early on it: timing points hold early buses.

## The gate, set beforehand

Built from 25 September to 14 October; replayed (`scripts/backtest_live_eta.py
--model --from-day`) on 15 to 24 October, days it never saw, judged by RTIG's
accuracy windows. Ship only if the acceptable share rises overall **and** "bus
came early" rises by no more than one point overall or for any operator.

## Result (journeys outside recorded disruptions)

Acceptable / bus came early / bus came late:

| Model | All | Brighton & Hove | Stagecoach | Compass | Metrobus |
|---|---|---|---|---|---|
| None (today) | 81.6% / 8.4% / 10.0% | 85.2% / 6.4% / 8.3% | 78.0% / 13.3% / 8.7% | 70.5% / 13.1% / 16.4% | 68.2% / 4.4% / 27.5% |
| 15th percentile | 81.4% / 7.4% / 11.2% | 85.0% / 5.2% / 9.8% | 78.0% / 12.2% / 9.8% | 70.5% / 12.9% / 16.6% | 68.0% / 4.3% / 27.7% |
| **30th percentile** | **82.1% / 7.7% / 10.1%** | 85.9% / 5.6% / 8.5% | 78.1% / 12.7% / 9.2% | 70.6% / 12.9% / 16.4% | 68.2% / 4.3% / 27.5% |
| 40th percentile | 82.2% / 8.1% / 9.7% | 86.1% / 6.1% / 7.8% | 78.1% / 13.0% / 8.9% | 70.7% / 13.0% / 16.4% | 68.2% / 4.3% / 27.5% |
| 50th percentile | 82.1% / 8.6% / 9.3% | 85.9% / 6.8% / 7.3% | 78.0% / 13.4% / 8.6% | 70.7% / 13.0% / 16.3% | 68.2% / 4.3% / 27.5% |
| 70th percentile | 80.6% / 10.7% / 8.7% | 84.1% / 9.4% / 6.5% | 76.6% / 15.2% / 8.2% | 70.7% / 13.1% / 16.2% | 68.6% / 4.4% / 27.0% |
| 85th percentile | 77.2% / 14.5% / 8.3% | 79.7% / 14.2% / 6.1% | 73.9% / 18.2% / 8.0% | 70.6% / 13.2% / 16.2% | 68.8% / 4.7% / 26.5% |

**The 30th percentile passes and is used**: acceptable 81.6% → 82.1%, and
fewer buses earlier than the estimate (8.4% → 7.7%), lower or level for every
operator. The 40th and 50th gain as much acceptable but give back the early
improvement; above the 50th, estimates come too late and buses arrive before
them. A low percentile assumes more recovery, so the estimate comes earlier and
the bus, if anything, later: the safer way round.

## What it does not do

- The gain is small: half a point. Compass and Metrobus barely move, because
  few of their timing-point pairs reach 30 journeys from 5 days in the window;
  their errors are where the next improvement is.
- Estimates are made from the bus's current position, not from the timing
  point the figures were measured from; the board's own accuracy with the model
  is checked separately (`scripts/score_served_estimates.py --recovery`).
- Rebuilt every night from the 35-day window at the 30th percentile; the choice
  of percentile is not re-tested automatically.
- **Weekends get little or nothing.** A group needs journeys from 5 different
  days, and a 35-day window holds five Saturdays and five Sundays at most, so
  weekend groups rarely qualify (the test model, from 20 days, had none). On
  the board rebuilt minute by minute for Saturday 24 October it therefore
  changed nothing. With weekday groups standing in for Saturday ones, as a check
  that the model reaches the live estimate at all, an hour of that afternoon
  went from 67.4% to 68.7% acceptable.

Reproduce:

    python scripts/build_running_recovery.py --observations 'obs/*.json.gz' --percentile 30 \
        --until 2026-10-15 --out running-recovery.json
    python scripts/backtest_live_eta.py --observations 'obs/*.json.gz' --from-day 2026-10-15 \
        --model running-recovery.json
