"""Which journey a bus works next, learned from the journeys buses really ran.

A board can only time a journey once a bus is on it, unless it knows which bus
will be. Stagecoach and Compass publish blocks (`Timetable.next_in_block`);
Brighton & Hove and Metrobus do not, so their journeys stayed "Timetable" until
the bus set off, and the night buses an hour apart showed it most. Measured on
13 recorded days (22 October to 1 November 2026), the published blocks named
the journey a bus really ran next 86% of the time; this, learned from the days
before, named it 86% (Brighton & Hove) to 97% (Stagecoach) of the time.

scripts/build_turnarounds.py counts, for every journey a bus was identified on
exactly, the journey the same bus ran next: its service, and how long after the
first one ended (by the timetable) the second one started. Grouped by operator
family, service, the stop the journey ends at, day type and time band
(api/recovery.py's), and coarser without the band where the band has too few.

A cell is keyed `OP|SVC|terminus|day[|band]` and says
`{"next": service, "after": seconds, "journeys": n, "share": fraction}`: the
service most buses there went on to, and a low percentile of the gap, so the
journey picked is the first one a bus could plausibly take. A cell is published
only where most buses carried on in service, so a bus going back to the garage
is not handed a journey.
"""
from typing import Optional

from api import recovery

# Next journeys start near where the last one ended, but not always at the same
# pole: the stand across the road, or the first stop round the corner.
NEAR_M = 400
# Beyond this the bus has had its break, a driver change or a depot run, and a
# guess is a guess about the whole schedule.
WITHIN_SECS = 60 * 60


def keys(operator: str, service: str, terminus: str, day, end_secs: int) -> tuple:
    """The fine key (with the time band) and the coarse one (without)."""
    base = "|".join((recovery.family(operator), str(service), terminus, recovery.day_type(day)))
    return f"{base}|{recovery.band(int(end_secs))}", base


def lookup(doc: Optional[dict], operator: str, service: str, terminus: str, day,
           end_secs: int) -> Optional[dict]:
    """The cell for a journey ending at `terminus` at `end_secs`, or None."""
    cells = (doc or {}).get("cells") or {}
    if not cells or not terminus:
        return None
    fine, coarse = keys(operator, service, terminus, day, end_secs)
    return cells.get(fine) or cells.get(coarse)
