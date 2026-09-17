"""The rules that decide which journey a bus is running.

These were tested only through the gap monitor, and loosely: two of the bounds
survived being broken outright while all 33 corridor tests passed. That mattered
less while one caller used them. Now `api/trip_match.py` is shared between the
live monitor and the nightly processor that will produce published punctuality
figures, so a silent change here would move a published number.

Each bound below exists because it was got wrong first:

* **5 minutes early.** A duplicate bus was matched to the *next* departure,
  thirteen minutes ahead of itself, and the monitor reported a gap while a bus
  sat plainly on the map.
* **25 minutes late.** Wide, because buses run late; wider still and a bus
  starts claiming a journey that ran before it.
* **400 m off route.** Without it a bus on a parallel road joins the corridor.
* **One bus, one journey.** Two buses reporting the same position both claimed
  the same journey, which hid one of them from every count that followed.
* **The service day.** GTFS writes 04:40 as 28:40 on the previous day. A bus at
  04:45 belongs to yesterday's service; matched against today's, it vanishes.
"""

import sys
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT))

from api import trip_match                                   # noqa: E402
# The corridor fixture: a 60-stop coast road with poles on both sides, buses
# every ten minutes, a coach behind each, and one bus written as 28:40.
from test_corridor_gaps import (                             # noqa: E402,F401
    WATCH, at, bus_at, east_xy, monkeypatch_module, tt,
)

WATCH_ATCOS = [atco for atco, _ in WATCH]
HOURS_2 = 2 * 3600


def instances_at(tt, when, only=None):
    """The journeys in view at `when`, optionally narrowed to one trip id.

    Narrowing is what makes a bound testable. With every journey in play the
    matcher simply picks a better one, so a broken bound still looks like a
    pass — which is how these went untested for as long as they did.
    """
    now = when.hour * 3600 + when.minute * 60 + when.second
    insts, _sched = trip_match.build_instances(
        tt, when.date(), WATCH_ATCOS, (now - HOURS_2, now + HOURS_2))
    if only is not None:
        insts = {k: v for k, v in insts.items() if k[0] == only}
    return insts, now


def place(tt, when, vehicles, only=None):
    insts, now = instances_at(tt, when, only)
    return trip_match.place_vehicles(tt, vehicles, insts, now)


# ── How early a bus may be for its journey ──────────────────
# W600 leaves stop 0 at 10:00. A bus sitting there beforehand is either early
# or waiting for something else; five minutes is the line.

def test_a_bus_four_minutes_early_is_running_the_journey(tt):
    placed = place(tt, at(9, 56), [bus_at(0)], only="W600")
    assert [k[0] for k in placed] == ["W600"]


def test_a_bus_six_minutes_early_is_not_yet_running_it(tt):
    placed = place(tt, at(9, 54), [bus_at(0)], only="W600")
    assert placed == {}, "a bus took a departure six minutes before it leaves"


# ── How late ────────────────────────────────────────────────

def test_a_bus_twenty_four_minutes_late_still_counts(tt):
    placed = place(tt, at(10, 24), [bus_at(0)], only="W600")
    assert [k[0] for k in placed] == ["W600"]


def test_a_bus_twenty_six_minutes_late_does_not(tt):
    placed = place(tt, at(10, 26), [bus_at(0)], only="W600")
    assert placed == {}, "a bus claimed a journey 26 minutes after it was due"


# ── How far off route ───────────────────────────────────────

def test_a_bus_two_kilometres_away_is_not_on_the_corridor(tt):
    inland = bus_at(0)
    inland["latitude"] += 0.02          # ~2.2 km north: a parallel road
    assert place(tt, at(10, 0), [inland], only="W600") == {}, \
        "a bus 2 km off the route was placed on it"


def test_a_bus_at_the_stop_is(tt):
    assert place(tt, at(10, 0), [bus_at(0)], only="W600") != {}


# ── Which way it is pointing ────────────────────────────────

def test_a_bus_heading_the_other_way_is_not_this_journey(tt):
    # The eastbound pole is ~90 m across the road, well inside 400 m, so
    # position alone cannot tell the two apart. The heading can.
    # 10:40 is exactly when W620 is due at stop 20, so time, service and
    # distance all fit and the heading is the only thing left to reject it.
    lat, lon = east_xy(20)
    eastbound = {"service_ref": "700", "operator_ref": "SCSO", "latitude": lat,
                 "longitude": lon, "bearing": 76.0, "destination": "Brighton"}
    assert place(tt, at(10, 40), [eastbound], only="W620") == {}, \
        "a bus driving east was placed on a westbound journey"
    # Proof the rest of the match is sound: turn it round and it is placed.
    assert place(tt, at(10, 40), [{**eastbound, "bearing": 256.0,
                                   "destination": "Worthing"}], only="W620") != {}


# ── One bus, one journey ────────────────────────────────────

def test_two_buses_in_one_place_do_not_share_a_journey(tt):
    # At 10:16 stop 10 has two journeys in tolerance — the 10:00 six minutes
    # late and the 10:10 four minutes early — so both buses have somewhere to
    # go, and sharing one journey would be a choice rather than the only fit.
    twins = [bus_at(10), bus_at(10)]
    placed = place(tt, at(10, 16), twins)
    assert len(placed) == 2, f"two buses shared one journey: {placed}"
    assert len({vi for vi, _ in placed.values()}) == 2, "one bus took two journeys"


# ── The service day ─────────────────────────────────────────

def test_the_first_bus_of_the_morning_belongs_to_yesterday(tt):
    # Written 28:40 on Tuesday's service, which is 04:40 on Wednesday.
    when = at(4, 45)
    placed = place(tt, when, [bus_at(0)], only="MORNING")
    assert [k[0] for k in placed] == ["MORNING"]
    (_trip, day), = placed
    assert day == when.date() - timedelta(days=1), \
        "the 04:40 was filed under today rather than yesterday's service"


# ── Coaches ─────────────────────────────────────────────────

def test_a_coach_through_the_same_poles_is_not_a_bus(tt):
    # Coaches call at these stops but need their own ticket. Counting them
    # would fill a gap that someone waiting for a bus still experiences.
    insts, now = instances_at(tt, at(10, 5))
    assert not any(k[0].startswith("C") for k in insts), "a coach journey was built"
    coach = bus_at(0, service="025", operator="NATX", destination="London")
    assert trip_match.place_vehicles(tt, [coach], insts, now) == {}, \
        "a coach was placed on a bus journey"


# ── Matching a position at the time it was recorded ─────────

def test_a_stale_position_is_judged_at_its_own_time(tt):
    # The processor replays recorded positions, and the feed runs a median 186
    # seconds behind. Judged against the clock instead of its own timestamp, a
    # bus at stop 20 reported four minutes ago looks four minutes further down
    # the timetable — and lands on the following journey, which reads as a bus
    # running early rather than late.
    bus = bus_at(20)
    ten_twenty = 10 * 3600 + 20 * 60
    insts, _now = instances_at(tt, at(10, 24))          # read four minutes later
    only600 = {k: v for k, v in insts.items() if k[0] == "W600"}
    # Its own timestamp says it was at stop 20 exactly on time.
    assert trip_match.place_vehicles(tt, [bus], only600, 10 * 3600 + 24 * 60,
                                     times=[ten_twenty]), \
        "a position matched at its recorded time was rejected"
    # And the live monitor, which passes no times, still judges by the clock.
    placed_live = trip_match.place_vehicles(tt, [bus], only600, 10 * 3600 + 24 * 60)
    assert placed_live, "the live path changed behaviour"


def test_the_recorded_time_changes_which_journey_wins(tt):
    # This is the whole point, in one comparison. A bus sits at stop 0, its
    # position recorded at 10:10 — exactly on time for the 10:10 departure —
    # and read ten minutes later.
    bus = bus_at(0)
    insts, _ = instances_at(tt, at(10, 20))
    read_at = 10 * 3600 + 20 * 60
    recorded_at = 10 * 3600 + 10 * 60

    own_time = trip_match.place_vehicles(tt, [bus], insts, read_at, times=[recorded_at])
    assert [k[0] for k in own_time] == ["W610"], \
        f"judged at its own time it is the 10:10: {sorted(k[0] for k in own_time)}"

    # Judged by the clock instead, the same bus becomes the 10:20 departure
    # sitting at its first stop dead on time — a different journey, and the
    # 10:10's lateness disappears from the record entirely.
    by_clock = trip_match.place_vehicles(tt, [bus], insts, read_at)
    assert [k[0] for k in by_clock] == ["W620"], \
        f"expected the clock to mislead here: {sorted(k[0] for k in by_clock)}"
