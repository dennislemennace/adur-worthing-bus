"""Names the site shows in place of the published ones.

Two kinds, both curated in data/place_names.json:

* a stop renamed for passengers: the coach stop behind Brighton station is
  published as "Brighton Rail Station Stroudley Road opposite" by National
  Express and "Brighton Railway Station - Stroudley Road" by FlixBus, and is
  shown as one place, "Brighton Station (Stroudley Road car park)";
* a coach's destination as a passenger says it, with the exact stop beneath:
  "Swansea City Bus Station (Bay B and C)" reads as Swansea, "City Bus Station,
  bays B and C". FlixBus publishes its headsign as the destination's whole
  name; National Express publishes the journey ("Brighton - Belgravia,
  Victoria"), so for it the far end is what is looked up.

A name with no entry is shown as published. A missing or unreadable file
changes nothing: the published names are always the fallback.
"""

import functools
import json
from pathlib import Path

PATH = Path(__file__).resolve().parent.parent / "data" / "place_names.json"

# National Express writes every headsign as "origin - destination" (all 439 of
# its trips in the feed, measured 3 November 2026), and none of its stop names
# holds " - ", so for it the far end is safe to take.
JOURNEY_HEADSIGN_NOCS = frozenset({"NATX"})


@functools.lru_cache(maxsize=1)
def _doc() -> dict:
    try:
        doc = json.loads(PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"stops": {}, "coach_destinations": {}}
    return {"stops": doc.get("stops") or {}, "coach_destinations": doc.get("coach_destinations") or {}}


def stop_name(atco: str, published: str) -> str:
    """The name to show for a stop: its curated name, else as published."""
    entry = _doc()["stops"].get(atco)
    return (entry or {}).get("name") or published


def coach_destination(name: str, operator: str = "") -> tuple:
    """`(place, stop)` for a destination; `(name, "")` when it has no entry.

    Looked up as written. For an operator that writes its headsign as the
    journey, the far end is taken first. A destination that merely contains a
    hyphen is otherwise left whole: FlixBus's names contain hyphens of their own.
    """
    name = (name or "").strip()
    if operator in JOURNEY_HEADSIGN_NOCS and " - " in name:
        name = name.rsplit(" - ", 1)[1].strip()
    entry = _doc()["coach_destinations"].get(name)
    if not entry:
        return name, ""
    return entry.get("place") or name, entry.get("stop") or ""
