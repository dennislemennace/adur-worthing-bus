# A live time before the bus sets off, for every operator: 3 November 2026

**Value.** A board row only had a live time once a bus was on that journey,
except for Stagecoach and Compass, whose published blocks say which bus runs
which journeys. Brighton & Hove and Metrobus publish none, so their later
journeys showed "Timetable" until the bus left, most visibly on the night
buses. The journey each bus goes on to run is now learned from the recorded
days (`api/turnarounds.py`, `scripts/build_turnarounds.py`). On six replayed
days its estimates beat the timetable for Brighton & Hove, Compass and
Stagecoach, and they are switched on for those three. Metrobus is left off.

**As of** 3 November 2026. Identification: 13 recorded days, 20 October to
1 November 2026. Board replays: 22, 23 and 29 October to 1 November 2026, each
with a model built only from days before it. Code at `489b4b0` plus this change.

## Which journey a bus runs next

For every bus identified exactly on two journeys in a row, the guess was
compared with the journey it really ran next. Learned from up to 14 earlier
days of the same day type, by service, last stop and day type:

| Operator | Learned: right | Published block: right |
|---|---|---|
| Brighton & Hove | 86% | none published |
| Compass | 96% | 86% |
| Metrobus | 96% | none published |
| Stagecoach | 97% | 86% |

"Right" is of the guesses made. "Earliest journey of the same service from
that stop" was right only 56% of the time for Brighton & Hove: at a busy
terminus the bus takes a later departure than the first one after it arrives.
So each cell also learns how long after a journey ends the next one usually
starts, and takes a low (10th) percentile of it: the first journey a bus could
plausibly take. Where most buses ending there went out of service, no journey
is guessed.

## What the board would have shown

`scripts/score_served_estimates.py` rebuilds the board for every recorded
minute and judges each estimate by RTIG's window for how far ahead it was
(RTIGT041-1.0, 6.9), beside the timetable's own time for the same departures.
Journeys outside recorded disruptions. "Came before shown" is the bus leaving
earlier than the window allows: the error that loses someone the bus.

29 October to 1 November (half term, then a weekend), half the late start
passed on:

| Operator | Ours acceptable | Timetable acceptable | Ours: came before shown | Timetable: came before shown |
|---|---|---|---|---|
| Brighton & Hove | 68.7% | 64.8% | 1.8% | 0.5% |
| Compass | 69.9% | 65.9% | 0.5% | 0.2% |
| Metrobus | 63.0% | 62.5% | 0.2% | 0.2% |
| Stagecoach | 77.1% | 72.8% | 2.1% | 0.8% |

22 and 23 October (term-time weekdays), the same:

| Operator | Ours acceptable | Timetable acceptable | Ours: came before shown |
|---|---|---|---|
| Brighton & Hove | 77.9% | 76.7% | 0.2% |
| Compass | 69.0% | 63.8% | 0.5% |
| Metrobus | 48.5% | 49.4% | 0.3% |
| Stagecoach | 80.6% | 79.0% | 1.8% |

Metrobus did worse than the timetable on the term days, so it is not switched
on (`PREVIOUS_JOURNEY_OPERATORS`, `api/main.py`); the nightly scoring keeps
measuring it.

## Erring early

Asked for (3 November 2026): estimates should lean towards the reader arriving
early rather than late. The next journey is estimated to leave when this bus
reaches its last stop, with no break allowed for at the stand, which already
errs early. When the guess about the bus is wrong, though, it shows a late time
for a bus that comes on time. How much of the late start to pass on, 29 October
to 1 November, all operators:

| Late start passed on | Acceptable | Came before shown | Came after shown |
|---|---|---|---|
| All of it | 74.6% | 3.5% | 21.9% |
| Half | 69.9% | 1.5% | 28.6% |
| All, less 3 minutes | 70.3% | 1.9% | 27.8% |
| Half, less 2 minutes | 67.5% | 0.9% | 31.5% |
| None (the timetable) | 66.0% | 0.5% | 33.5% |

Half is used (`PREVIOUS_JOURNEY_SHARE`, `api/live_eta.py`): the bus comes
before the time shown on 1.5% of these estimates, under the 2.2% the
block-based estimates had before this change, and still within the window more
often than the timetable.

**Buses already on the road** now pass on four fifths of their lateness
(`ROAD_LATE_SHARE`, `api/live_eta.py`), chosen by the maintainer on 3 November
2026 after seeing what it costs (29 October to 1 November, all operators):

| Lateness passed on | Acceptable | Came before shown | Came after shown |
|---|---|---|---|
| All of it (before 3 November) | 77.3% | 5.4% | 17.3% |
| 80% (from 3 November) | 72.0% | 3.4% | 24.5% |
| All, less 1 minute | 70.9% | 3.7% | 25.4% |
| All, less 2 minutes | 66.3% | 3.0% | 30.6% |

The share is applied once, before a diversion allowance is weighed against it,
so a bus on a diversion still shows its shown lateness or the allowance,
whichever is more; the 80% row was re-run with that in place and did not move.
The next journey's estimate starts from the bus's whole arrival time, not four
fifths of it, because its own half was measured that way. The nightly figure on
the live data health page scores each new day as the board then showed it, so
its 30-day window moves over to the new share by 3 December 2026; each day
records which share it was scored at (`late_shares`).

## Caveats

- **Few test days.** Six, four of them half term or a weekend. Direction: unknown.
- **The 22 October timetable change.** Journeys recorded before it are not in
  the timetable that replaced it, so the models for 22 and 23 October were
  built from few days (181 and 509 cells, against about 600). The nightly build
  meets the same thing after every timetable change, for a few days. Direction:
  understates what the method does once it has history.
- **Models in force.** The replays used the diversion and recovery files
  published on 3 November, not the ones in force on each day. Both runs of each
  day used the same files, so the comparison stands; the absolute figures may
  differ slightly from the nightly ones.
- **What the board shows first.** A TransportAPI prediction, where there is
  one, still takes precedence, and the board's row cap is not modelled.

## Reproduce

    python scripts/build_turnarounds.py --observations 'obs/*.json.gz' \
        --timetable data/timetable.sqlite --until 2026-10-28 --out turn-2026-10-29.json
    python scripts/score_served_estimates.py day --day 2026-10-29 --timetable data/timetable.sqlite \
        --snapshots snapshots/2026-10-29 --rt gtfs-rt/2026-10-29 \
        --observations observations-2026-10-29.json.gz --diversions diversion-adjustments.json \
        --recovery running-recovery.json --turnarounds turn-2026-10-29.json --out served-2026-10-29.json

The recorded days come from each night's `reliability-run-*` release
(`scripts/restore_recorded_day.py`).
