# What the stop board actually showed, against when the buses came: 25 October 2026

**Value.** On ordinary days, **79% of the live times the board showed** were
within RTIG's accuracy window for how far ahead they were (5.5% the bus came
earlier than allowed, 15.3% later). **86% of timetabled departures** had a live
time on the board at some point in the 15 minutes before they were due.
Counts are of live times shown (one per bus, per stop ahead, per minute), and
of departures and journeys where stated.

**As of** 25 October 2026, from the recorded feeds of 25 September to
24 October 2026 (28 days; 18 and 19 October were not recorded), each replayed
with the timetable in force that day, code at `476e818` plus this change.

## Why this, and not only the replay

`live-eta-accuracy-2026-10-25.md` replays our estimate from the moment a bus
left a timing point: the best the method can do (83% acceptable). A passenger
sees less, because the feed reaches us late and a bus is usually between stops.
RTIG's guidance (RTIGT041-1.0, 4.2.4 and 7.4) is to judge what the system
actually said. So `scripts/score_served_estimates.py` rebuilds the board for
every recorded minute with the API's own code and judges every estimate
against when that bus left, by RTIG's window (RTIGT041 6.9) for how far ahead it
said the bus was.

## Results

### The board's live times, journeys outside recorded disruptions

Acceptable / bus came early / bus came late (live times scored):

| | |
|---|---|
| **All** | **79.2% / 5.5% / 15.3%** (22,696,125) |
| Brighton & Hove | 81.1% / 3.8% / 15.1% (16,155,505) |
| Stagecoach | 79.5% / 8.1% / 12.4% (4,099,727) |
| Compass | 66.7% / 15.7% / 17.6% (1,862,835) |
| Metrobus | 63.3% / 4.4% / 32.2% (578,058) |
| 0–3 min ahead | 66.9% / 3.1% / 30.0% |
| 3–6 min | 76.8% / 4.2% / 18.9% |
| 6–12 min | 82.4% / 5.7% / 11.9% |
| 12–18 min | 79.7% / 6.1% / 14.2% |
| 18–30 min | 83.4% / 6.9% / 9.7% |
| At the journey's first stop | 66.0% / 2.3% / 31.7% |
| Stops 2–3 | 68.2% / 1.7% / 30.1% |
| Later stops | 79.6% / 5.7% / 14.7% |
| Through a recorded disruption | 75.5% / 6.3% / 18.2% (29,269,109) |

### Live times on the board (RTIGT041 5.2)

Of 3,776,054 timetabled departures in the area in recording hours, **86.1%**
had a live time at some minute in the 15 minutes before they were due
(Stagecoach 90.5%, Compass 87.6%, Brighton & Hove 85.3%, Metrobus 81.3%). The
rest:

- 8.6%: the feed named the journey but there was no estimate for that stop
  then (mostly the first stops, before the bus has set off);
- 3.2%: no bus was seen on the journey at all;
- 2.2%: a bus was seen but its feed did not name the journey.

89.8% of 119,855 timetabled journeys had a live estimate at some stop.

### Buses still listed after they had gone (RTIGT041 3.6.8)

Of 2,335,245 timed departures, the board would still have listed 7.6% a minute
after the bus left, 3.1% three minutes after and 2.3% five minutes after.
19.5% left before their timetabled time; with no live time, the board keeps a
departure until its timetabled time.

### Estimates before a journey starts (RTIGT041 3.4)

For Stagecoach and Compass, whose timetables say which journey each bus works
next, the next journey's start estimated from the bus's current one
(`live_eta.from_previous_journey`), against the timetable's own time for the
same departures, 8 to 24 October (15 days), ordinary days:

| | Our estimate | The timetable |
|---|---|---|
| Compass | 78.7% / 2.4% / 18.9% | 68.2% / 0.2% / 31.6% |
| Stagecoach | 84.4% / 3.5% / 12.0% | 72.4% / 0.6% / 27.0% |

Both beat the timetable, so the board now shows them for both operators,
marked with a ring. The cost: buses come earlier than these estimates about 3%
of the time, against under 1% for the timetable.

## Caveats, and which way each leans

- **Our estimates only.** Where the board showed TransportAPI's prediction
  instead, that is not reproduced; those are now logged and compared
  separately.
- **Departures, not arrivals.** RTIG's windows are for arrivals; a bus that
  waits at a stop is timed leaving it, which leans towards "late" in the
  shortest range and at first stops, where buses wait.
- **Once a minute.** The recorder samples once a minute; the live API looks
  every 15 seconds while someone watches, so the board can be a little better
  than this.
- **Every stop ahead counts.** The board shows ten rows; this scores every stop
  ahead of every bus.
- **Diversion allowances** are included from 16 October, when the board
  started adding them.

Reproduce: restore a day's feeds and timetable from its `evidence.tar.gz`
(`scripts/restore_recorded_day.py`), then

    python scripts/score_served_estimates.py day --day 2026-10-24 --timetable timetable.sqlite \
        --snapshots snapshots/2026-10-24 --rt gtfs-rt/2026-10-24 \
        --observations observations-2026-10-24.json.gz --out served-2026-10-24.json
    python scripts/score_served_estimates.py merge served-*.json --out served-accuracy.json

The per-day counts are in `data/reliability/served/`; from tonight the nightly
build adds a day at a time and publishes `served-accuracy.json`, which
`health.html` reads.
