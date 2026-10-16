"""Allowances for a diversion, added to live estimates beyond it.

`scripts/build_diversion_adjustments.py` measures, each night, how much later
than the timetable buses reach each timing point past a diversion than they
were at the last timing point before it (the entry point). Live estimates
otherwise carry a bus's lateness forward unchanged, which promises a bus still
short of the diversion at the stops beyond as if it were not there.

The same lookup serves the live estimate (api/live_eta.py) and the backtest
that decided whether to use it, so the two cannot drift apart.

A journey is matched by operator, route and the entry point's stop, not by
direction: stop poles are one-directional, so a journey that calls at the entry
pole and then at the measured points beyond it is going the measured way.
"""

from datetime import datetime
from typing import Optional

from api.trip_match import LONDON

# As api/main.py's _NOC_FAMILIES: codes that publish the same buses.
_FAMILIES = {"SCSC": "SCSO", "CMPA": "COMT"}


def _family(op) -> str:
    return _FAMILIES.get(op or "", op or "")


def period_for(epoch: float) -> str:
    """The time-of-day group the allowances are measured in: the same hours as
    scripts/build_delay_hotspots.py's `entry_period`, which a test holds."""
    hour = datetime.fromtimestamp(epoch, LONDON).hour
    return "08-10" if 8 <= hour < 10 else "10-16" if 10 <= hour < 16 else "16-18" if 16 <= hour < 18 else "other"


def active(doc: Optional[dict], now: datetime) -> list:
    """The adjustments in force at `now`: an ended event adds nothing, whatever
    the last night's file says."""
    out = []
    for adj in (doc or {}).get("adjustments") or []:
        try:
            start = datetime.fromisoformat(adj["from"])
            end = datetime.fromisoformat(adj["to"]) if adj.get("to") else None
        except (KeyError, TypeError, ValueError):
            continue
        if start <= now and (end is None or now <= end):
            out.append(adj)
    return out


def plan_for_trip(doc: Optional[dict], operator: str, service: str, atcos: list,
                  now: datetime) -> Optional[dict]:
    """What applies to one journey, given its calls' stops in order.

    `{"entry": i, "points": [(j, cells), ...], "not_served": {k, ...}, "reason",
    "disruption_id"}` with indexes into `atcos`, or None.
    """
    for adj in active(doc, now):
        if _family(adj.get("operator")) != _family(operator):
            continue
        for svc in adj.get("services") or []:
            if str(svc.get("service", "")).upper() != str(service or "").upper():
                continue
            try:
                entry = atcos.index(svc["entry_atco"])
            except ValueError:
                continue
            points = []
            for p in svc.get("points") or []:
                try:
                    j = atcos.index(p["atco"], entry + 1)
                except ValueError:
                    continue
                points.append((j, p.get("cells") or {}))
            if not points:
                continue
            closed = set(adj.get("stops_not_served") or [])
            return {"entry": entry, "points": sorted(points, key=lambda p: p[0]),
                    "not_served": {k for k, a in enumerate(atcos) if k > entry and a in closed},
                    "reason": adj.get("reason", ""), "disruption_id": adj.get("disruption_id", "")}
    return None


def _cell(cells: dict, day_type: str, period: str) -> Optional[int]:
    cell = cells.get(f"{day_type}|{period}") or cells.get(f"{day_type}|all")
    return int(cell["extra_secs"]) if cell and cell.get("extra_secs") is not None else None


def extra_at(plan: Optional[dict], j: int, day_type: str, period: str,
             secs: Optional[list] = None) -> int:
    """Seconds to add at call `j`.

    Nothing at or before the entry point. Up to the first measured point, that
    point's figure: the time is lost on the diversion itself, early in the
    stretch. Between two measured points, in proportion to the timetable's time
    between them (or the calls, without `secs`). After the last, the last.
    """
    if not plan or j <= plan["entry"]:
        return 0
    known = [(k, v) for k, v in ((k, _cell(c, day_type, period)) for k, c in plan["points"]) if v is not None]
    if not known:
        return 0
    if j <= known[0][0]:
        return known[0][1]
    for (a, va), (b, vb) in zip(known, known[1:]):
        if a < j <= b:
            if secs and secs[b] != secs[a]:
                frac = (secs[j] - secs[a]) / (secs[b] - secs[a])
            else:
                frac = (j - a) / (b - a)
            return int(round(va + (vb - va) * frac))
    return known[-1][1]
