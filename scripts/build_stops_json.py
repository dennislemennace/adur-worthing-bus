#!/usr/bin/env python3
"""Write data/stops.json — the stop list, served as a static file.

The API runs on a free Render instance that spins down after 15 minutes of
inactivity and takes roughly 20 seconds to come back. The first visitor after
a quiet spell therefore watched an empty map while the container woke up, for
data that changes when the timetable is rebuilt and not otherwise.

So the stop list is published to GitHub Pages alongside the site. The map is
drawn from the static file immediately, and the API is needed only for the
things that are genuinely live — departures, vehicle positions, journeys.

Run after scripts/json_to_sqlite.py, in the same workflow that publishes the
database, so the two can never describe different weeks.

    python scripts/build_stops_json.py [--db data/timetable.sqlite]
                                       [--out data/stops.json]
"""

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api.timetable_db import Timetable, db_is_usable   # noqa: E402
from api.main import (                                 # noqa: E402
    BBOX_MIN_LAT, BBOX_MAX_LAT, BBOX_MIN_LON, BBOX_MAX_LON,
)

# The bbox is imported rather than restated. A second copy of four numbers is
# a second thing to update, and the failure — a static file covering a
# different area than the API — would show up as stops quietly missing from
# the map rather than as an error.
BBOX = (BBOX_MIN_LAT, BBOX_MAX_LAT, BBOX_MIN_LON, BBOX_MAX_LON)

# Below this, something has gone wrong upstream: the bbox covers Adur and
# Worthing and there are on the order of a thousand stops in it. Publishing a
# near-empty file would give every visitor a blank map, and the API fallback
# would never fire because the fetch succeeded.
MIN_EXPECTED_STOPS = 800


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db",  default=str(ROOT / "data" / "timetable.sqlite"))
    ap.add_argument("--out", default=str(ROOT / "data" / "stops.json"))
    args = ap.parse_args()

    db = Path(args.db)
    if not db_is_usable(db):
        print(f"ERROR: {db} is not a usable timetable", file=sys.stderr)
        return 1

    tt = Timetable(db, allow_fetch=False)
    if not tt.ok():
        print(f"ERROR: could not open {db}", file=sys.stderr)
        return 1

    stops = tt.stop_list(BBOX)
    if len(stops) < MIN_EXPECTED_STOPS:
        print(f"ERROR: only {len(stops)} stops in the bbox, expected at least "
              f"{MIN_EXPECTED_STOPS} — refusing to publish", file=sys.stderr)
        return 1

    payload = {
        "stops": stops,
        "count": len(stops),
        # Shown in the UI when the live service is unreachable, so a reader
        # can tell whether they are looking at something current.
        "generated_on": date.today().isoformat(),
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    # Compact: this is downloaded on every first visit. Pretty-printing it
    # costs about a third again in bytes for nobody's benefit.
    out.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")

    with_dir = sum(1 for s in stops if "towards" in s)
    print(f"Wrote {out} — {len(stops)} stops "
          f"({with_dir} with a direction), {out.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
