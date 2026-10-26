"""When a tracked bus is likely to reach each stop still ahead of it.

One method, shared by the Bus tab's list of upcoming stops and the stop board's
estimates, so the two can never tell a passenger different things about the
same bus.

**The method.** A bus whose feed names the journey it is running carries its
lateness, measured where it last reported against that journey's timetable, on
the bus's own clock (`main._attach_declared_journeys`). Where it is between two
stops, the timetable's time for that point is taken in proportion along the
stretch between them (`trip_match.scheduled_at_position`); a bus stood at a
timing point before its time counts as on time, because it will wait there. Every stop still ahead
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

**Diversions.** Where a recorded diversion is in force (api/diversions.py), a
bus that has not yet reached it gets an allowance at the stops beyond: a low
figure measured from the buses that went round it, so the estimate stays on
the later side of the bus. A bus already on it counts for its own lateness or
the allowance, whichever is more. Stops it does not call at get no estimate.
"""

from datetime import datetime, timedelta
from typing import Optional

from api import diversions, recovery, trip_match
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
        if best is None or abs(when.timestamp() - now_local.timestamp()) < abs(best.timestamp() - now_local.timestamp()):
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


def project_trip(tt, trip_id: str, vehicle: dict, now: datetime,
                 diversions_doc: Optional[dict] = None,
                 recovery_doc: Optional[dict] = None) -> list:
    """The stops still ahead of `vehicle` on `trip_id`, with estimates where honest.

    Each row: `stop_id, stop_name, seq, scheduled, expected, lateness_secs,
    timing_point, passed, is_next, is_terminus`, plus `allowance_secs` and
    `allowance_reason` where a diversion allowance is included, and
    `not_served` for a stop the diversion skips. The first row is the last stop
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

    declared = (vehicle.get("trip_source") in trip_match.DECLARED_SOURCES
                and vehicle.get("trip_id") == trip_id)
    lateness = vehicle.get("lateness_secs") if declared else None
    tps = list(tt.timepoints_by_call(trip_id) or []) if hasattr(tt, "timepoints_by_call") else []
    if len(tps) != len(calls):
        tps = []
    projected = (projected_lateness(int(lateness), tps, start, len(calls))
                 if lateness is not None else [None] * (len(calls) - start))
    # Anchor the whole journey once; individual calls must never change day.
    origin = vehicle.get('service_origin_epoch')
    if origin is None:
        origin = place_on_day(calls[start][0], now).timestamp() - calls[start][0]
    # How lateness has changed beyond the last timing point the bus passed,
    # on other days (api/recovery.py): late buses make up time, some lose it.
    if recovery_doc and lateness is not None and tps:
        anchor = max((i for i in range(start) if tps[i] == 1), default=None)
        route_id = (tt.trips.get(trip_id) or {}).get("route_id", "")
        projected = recovery.apply(projected, recovery.adjustments(
            recovery_doc, tt.noc_for_route(route_id) if hasattr(tt, "noc_for_route") else "",
            (tt.routes.get(route_id) or {}).get("short_name", ""), calls, tps, anchor, start,
            int(lateness), recovery.service_day(origin)))

    allowance = _allowances(tt, trip_id, calls, start, origin, lateness, projected,
                            diversions_doc, now)
    closed = allowance.pop("not_served", set())
    reason = allowance.pop("reason", "")

    def row(j, late, passed):
        secs, atco = calls[j]
        if j in closed:
            late = None
        extra = allowance.get(j, 0) if late is not None else 0
        late = late + extra if late is not None else None
        scheduled = datetime.fromtimestamp(origin + secs, LONDON)
        expected = datetime.fromtimestamp(origin + secs + late, LONDON) if late is not None else None
        return {**({"allowance_secs": extra, "allowance_reason": reason} if extra else {}),
                **({"not_served": True} if j in closed else {}),
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
    cutoff = now.timestamp() - PASSED_GRACE_SECS
    for k, j in enumerate(range(start, len(calls))):
        r = row(j, projected[k], False)
        if r["expected"] and datetime.fromisoformat(r["expected"]).timestamp() < cutoff:
            r["passed"] = True
        rows.append(r)
    # Only the most recent passed stop is worth showing: it says where the bus is.
    last_passed = max((i for i, r in enumerate(rows) if r["passed"]), default=None)
    if last_passed is not None:
        rows = rows[last_passed:]
    for r in rows:
        if not r["passed"] and not r.get("not_served"):
            r["is_next"] = True
            break
    return rows


def _allowances(tt, trip_id, calls, start, origin, lateness, projected, doc, now) -> dict:
    """`{call index: seconds}` to add for a diversion in force, plus
    `not_served` (call indexes) and `reason`. Empty when none applies."""
    if not doc:
        return {}
    trip = tt.trips.get(trip_id) or {}
    route_id = trip.get("route_id", "")
    service = (tt.routes.get(route_id) or {}).get("short_name", "")
    operator = tt.noc_for_route(route_id) if hasattr(tt, "noc_for_route") else ""
    plan = diversions.plan_for_trip(doc, operator, service,
                                    [atco for _secs, atco in calls], now)
    if not plan:
        return {}
    out = {"not_served": plan["not_served"], "reason": plan["reason"]}
    if lateness is None or start > plan["points"][0][0]:
        # No lateness to add to, or the bus is past the diversion and its own
        # lateness already carries what it lost there.
        return out
    secs = [s for s, _atco in calls]
    entry_at = origin + secs[plan["entry"]] + max(int(lateness), 0)
    day = datetime.fromtimestamp(origin + 12 * 3600, LONDON).date()
    day_type = "weekday" if day.weekday() < 5 else "weekend"
    period = diversions.period_for(entry_at)
    on_it = start > plan["entry"]
    for k, j in enumerate(range(start, len(calls))):
        extra = diversions.extra_at(plan, j, day_type, period, secs)
        if extra <= 0:
            continue
        if on_it:
            # Already going round: what it has lost so far is in its lateness,
            # so it counts for that or the allowance, whichever is more.
            extra = max(0, extra - max(projected[k] or 0, 0))
        if extra:
            out[j] = extra
    return out


def from_previous_journey(tt, vehicle: dict, now: datetime, day,
                          diversions_doc: Optional[dict] = None,
                          recovery_doc: Optional[dict] = None) -> Optional[dict]:
    """The journey this bus works next, and how late it will start it.

    From the operator's block (`Timetable.next_in_block`, Stagecoach and
    Compass only): the next journey leaves its first stop at its timetabled
    time, or when this bus gets there from the end of the journey it is on now,
    whichever is later. RTIG's "cross journey prediction" (RTIGT041-1.0, 3.4):
    without it a bus station shows no live time until the bus has set off.
    It can be wrong in ways an estimate on the road cannot (a driver change, a
    swapped bus, a journey cut short: RTIGT041 3.6.6), so the board marks it.

    `{"trip_id", "lateness_secs"}`, or None when the bus names no journey,
    has no estimate for its last stop, or works nothing after it.
    """
    trip_id = vehicle.get("trip_id")
    if not trip_id or vehicle.get("trip_source") not in trip_match.DECLARED_SOURCES:
        return None
    if not hasattr(tt, "next_in_block"):
        return None
    nxt = tt.next_in_block(trip_id, day)
    if not nxt:
        return None
    rows = project_trip(tt, trip_id, vehicle, now, diversions_doc, recovery_doc)
    last = rows[-1] if rows else None
    if not last or not last["is_terminus"] or last["passed"] or not last["expected"]:
        return None
    arrives = datetime.fromisoformat(last["expected"])
    starts = place_on_day(nxt["depart_secs"], now)
    return {"trip_id": nxt["trip_id"],
            "lateness_secs": max(0, round((arrives - starts).total_seconds()))}


def estimate_at(tt, trip_id: str, vehicle: dict, stop_id: str,
                aimed: datetime, now: datetime,
                diversions_doc: Optional[dict] = None,
                recovery_doc: Optional[dict] = None) -> Optional[dict]:
    """The estimate for one call of one journey, if it is still ahead of the bus.

    Matched on the stop and its timetabled time, because a looping journey can
    call at the same stop twice. None when there is no honest estimate: the
    journey is not declared, the bus has passed the stop, or it isn't on it.
    """
    for r in project_trip(tt, trip_id, vehicle, now, diversions_doc, recovery_doc):
        if r["stop_id"] != stop_id or r["passed"] or r["expected"] is None:
            continue
        if abs(datetime.fromisoformat(r["scheduled"]).timestamp() - aimed.timestamp()) <= 60:
            return r
    return None
