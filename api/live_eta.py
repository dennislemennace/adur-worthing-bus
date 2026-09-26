"""When a tracked bus is likely to reach each stop still ahead of it.

One method, shared by the Bus tab's list of upcoming stops and the stop board's
estimates, so the two can never tell a passenger different things about the
same bus.

**The method.** A bus whose feed names the journey it is running carries its
lateness, measured where it last reported against that journey's timetable, on
the bus's own clock (`main._attach_declared_journeys`). Every stop still ahead
is estimated at its timetabled time plus that lateness:

* a late bus is assumed to stay exactly as late;
* an early bus is assumed to stay early only until its next timing point, and
  on time from there on, because UK buses must not leave a timing point early
  and drivers wait at them.

**Which way it is wrong.** Timetables carry slack, and late buses often make
some of it up, so an estimate further down the route tends to say *later* than
the bus will really come. That is the direction that can make someone miss a
bus, which is why every place the estimate is shown also tells the reader to be
at the stop by the timetabled time.

**Stale reports.** The feed runs a median three minutes behind. A stop whose
estimate is already more than a minute gone is marked `passed`: the bus has
probably been by, even though its last report put it short of the stop.

A bus whose journey is only inferred gets no estimate at all. Inference picks
the right departure about four times in five, which is good enough to show the
stops ahead but not to say how late the bus is running.
"""

from datetime import datetime, timedelta
from typing import Optional

from api import trip_match
from api.trip_match import LONDON

# How long after an estimated time a stop is treated as already passed.
PASSED_GRACE_SECS = 60


def place_on_day(secs: int, now: datetime) -> datetime:
    """A GTFS time (which may run past 24:00) as the London instant nearest now.

    Tried against yesterday's, today's and tomorrow's service days, because at
    00:10 a 24:05 belongs to yesterday and at 23:50 a 00:05 to tomorrow.
    """
    now_local = now.astimezone(LONDON)
    best = None
    for offset in (-1, 0, 1):
        day = now_local.date() + timedelta(days=offset)
        when = datetime.fromtimestamp(trip_match._service_origin(day) + secs, LONDON)
        if best is None or abs((when - now_local).total_seconds()) < abs((best - now_local).total_seconds()):
            best = when
    return best


def next_call(tt, calls: list, vehicle: dict) -> Optional[int]:
    """Index of the first call the bus has not yet reached, by position.

    The nearest stop, moved on by one if the bus is already beyond it towards
    the next. None when the bus has no position or no stop on the journey has
    one.
    """
    lat, lon = vehicle.get("latitude"), vehicle.get("longitude")
    near = trip_match.nearest_call(tt, {"calls": calls}, lat, lon)
    if near is None:
        return None
    if trip_match.past_pole(tt, vehicle, calls, near):
        return near + 1
    return near


def projected_lateness(lateness: int, timing_points: list, start: int, count: int) -> list:
    """Lateness expected at each call from `start` to the end of the journey.

    Late carries forward unchanged. Early carries forward only until the first
    timing point at or after `start`, and is zero from there on. Where the
    timetable does not say which stops are timing points, early carries all the
    way, which errs towards telling people to be there sooner.
    """
    out = []
    held = False
    for j in range(start, count):
        if lateness < 0 and not held and j < len(timing_points) and timing_points[j] == 1:
            held = True
        out.append(0 if (lateness < 0 and held) else lateness)
    return out


def project_trip(tt, trip_id: str, vehicle: dict, now: datetime) -> list:
    """The stops still ahead of `vehicle` on `trip_id`, with estimates where honest.

    Each row: `stop_id, stop_name, seq, scheduled, expected, lateness_secs,
    timing_point, passed, is_next, is_terminus`. The first row is the last stop
    the bus has passed, when there is one, so a reader can see where it is.
    `expected` and `lateness_secs` are None unless the feed named the journey
    and it carries a lateness.
    """
    calls = [(secs, atco) for secs, atco in (tt.trip_stops_for(trip_id) or [])
             if secs is not None]
    if not calls:
        return []
    start = next_call(tt, calls, vehicle)
    if start is None:
        return []
    start = min(start, len(calls) - 1)

    declared = vehicle.get("trip_source") == "feed" and vehicle.get("trip_id") == trip_id
    lateness = vehicle.get("lateness_secs") if declared else None
    tps = list(tt.timepoints_by_call(trip_id) or []) if hasattr(tt, "timepoints_by_call") else []
    if len(tps) != len(calls):
        tps = []
    projected = (projected_lateness(int(lateness), tps, start, len(calls))
                 if lateness is not None else [None] * (len(calls) - start))

    def row(j, late, passed):
        secs, atco = calls[j]
        scheduled = place_on_day(secs, now)
        expected = scheduled + timedelta(seconds=late) if late is not None else None
        return {
            "stop_id": atco,
            "stop_name": (tt.stops.get(atco) or {}).get("name", atco),
            "seq": j,
            "scheduled": scheduled.isoformat(),
            "expected": expected.isoformat() if expected else None,
            "lateness_secs": late,
            "timing_point": (tps[j] == 1) if tps and tps[j] is not None else None,
            "passed": passed,
            "is_next": False,
            "is_terminus": j == len(calls) - 1,
        }

    rows = []
    if start > 0:
        rows.append(row(start - 1, None, True))
    cutoff = now - timedelta(seconds=PASSED_GRACE_SECS)
    for k, j in enumerate(range(start, len(calls))):
        r = row(j, projected[k], False)
        if r["expected"] and datetime.fromisoformat(r["expected"]) < cutoff:
            r["passed"] = True
        rows.append(r)
    # Only the most recent passed stop is worth showing: it says where the bus is.
    last_passed = max((i for i, r in enumerate(rows) if r["passed"]), default=None)
    if last_passed is not None:
        rows = rows[last_passed:]
    for r in rows:
        if not r["passed"]:
            r["is_next"] = True
            break
    return rows


def estimate_at(tt, trip_id: str, vehicle: dict, stop_id: str,
                aimed: datetime, now: datetime) -> Optional[dict]:
    """The estimate for one call of one journey, if it is still ahead of the bus.

    Matched on the stop and its timetabled time, because a looping journey can
    call at the same stop twice. None when there is no honest estimate: the
    journey is not declared, the bus has passed the stop, or it isn't on it.
    """
    for r in project_trip(tt, trip_id, vehicle, now):
        if r["stop_id"] != stop_id or r["passed"] or r["expected"] is None:
            continue
        if abs((datetime.fromisoformat(r["scheduled"]) - aimed).total_seconds()) <= 60:
            return r
    return None
