"""How much a bus's lateness changes between timing points, from past journeys.

Live estimates (api/live_eta.py) carry a bus's lateness forward unchanged. Buses
do not: a late Compass 37 makes up minutes between timing points, a Metrobus
loses them. RTIG's guidance (RTIGT041-1.0, 3.3.7) lists historical journey
times, by time of day, as the first thing a prediction should use beyond the
timetable. scripts/build_running_recovery.py measures, from recorded journeys,
the change in lateness from one timing point to each later one, for each
route, day type, time of day and how late the bus already was; this looks the
figure up.

A cell is keyed `OP|SVC|from|to|day|band|late`: operator family, service, the
two timing points' ATCO codes, weekday/saturday/sunday, the time band the bus
left the first one, and how late it was there. A cell with too few journeys
is not published, and a missing cell means no change, which is today's
estimate.
"""
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

LONDON = ZoneInfo("Europe/London")
FAMILIES = {"SCSC": "SCSO", "CMPA": "COMT"}
# When the bus left the first timing point, by the clock.
BANDS = ((0, 7, "early"), (7, 10, "morning peak"), (10, 15, "midday"), (15, 19, "evening peak"), (19, 30, "evening"))
# How late it was there. Early buses are held at timing points (live_eta), so
# they are left alone.
LATE = ((-60, 60, "on time"), (60, 180, "1-3 min"), (180, 360, "3-6 min"), (360, 10 ** 6, "6+ min"))


def family(operator: str) -> str:
    return FAMILIES.get(operator or "", operator or "")


def day_type(day) -> str:
    return "weekday" if day.weekday() < 5 else "saturday" if day.weekday() == 5 else "sunday"


def band(secs: int) -> str:
    hour = (secs // 3600)
    return next((name for lo, hi, name in BANDS if lo <= hour < hi), "evening")


def late_band(lateness: int) -> Optional[str]:
    return next((name for lo, hi, name in LATE if lo <= lateness < hi), None)


def key(operator, service, frm, to, day, anchor_secs, lateness) -> Optional[str]:
    lb = late_band(int(lateness))
    if lb is None:
        return None
    return "|".join((family(operator), str(service), frm, to, day_type(day), band(int(anchor_secs)), lb))


def adjustments(doc: Optional[dict], operator: str, service: str, calls: list, timing_points: list,
                anchor: Optional[int], start: int, lateness: int, day) -> list:
    """Seconds to add at each call from `start` on: the measured change in
    lateness from the timing point `anchor` (the last one the bus passed) to the
    latest timing point at or before each call. Zeros where nothing is known."""
    out = [0] * max(0, len(calls) - start)
    cells = (doc or {}).get("cells") or {}
    if not cells or anchor is None or lateness is None or late_band(int(lateness)) is None:
        return out
    frm = calls[anchor][1]
    target = None
    for k, j in enumerate(range(start, len(calls))):
        if j < len(timing_points) and timing_points[j] == 1 and j > anchor:
            target = j
        if target is None:
            continue
        cell = key(operator, service, frm, calls[target][1], day, calls[anchor][0], lateness)
        out[k] = int(cells.get(cell, 0)) if cell else 0
    return out


def apply(projected: list, adjust: list) -> list:
    """Lateness at each call with the measured change added. A late bus can
    make up its lateness but not run early on the strength of it: timing
    points hold early buses (live_eta.projected_lateness), so the floor is on
    time, or the lateness itself where that was already early."""
    return [None if p is None else max(p + a, min(p, 0)) for p, a in zip(projected, adjust)]


def service_day(origin_epoch: float):
    """The service day a journey anchored at `origin_epoch` belongs to."""
    return datetime.fromtimestamp(origin_epoch + 12 * 3600, LONDON).date()
