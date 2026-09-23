"""Does this timetable build describe this day at all?

A BODS bundle looks forward. The build fetched on 20 September 2026 ran from
`20260920` to `20270621` and said nothing whatever about the 18th — so
measuring the 18th against it left every service inactive, nothing scheduled,
nothing matched, and a day published as zeroes that every later check found
perfectly self-consistent. Two days went out that way.

`scripts/process_snapshots.py` now refuses such a day outright. This is the
half that lets the nightly Action *recover* rather than merely stop: it answers
the coverage question about a file on disk, so the workflow can walk the dated
`timetable-YYYY-MM-DD-HHMM` releases and pick one that was actually in force.

Exit status is 0 when the day is covered and 1 when it is not, so it composes
with shell control flow:

    python scripts/timetable_covers.py --timetable data/timetable.sqlite \\
        --day 2026-09-18 || echo "fetch an older build"

With no `--day` it prints the window and exits 0, which is what you want when
reading a downloaded asset by hand.
"""

import argparse
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api.timetable_db import Timetable                            # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--timetable", default=str(ROOT / "data" / "timetable.sqlite"))
    ap.add_argument("--day", help="service day, YYYY-MM-DD")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)

    path = Path(args.timetable)
    if not path.exists():
        print(f"{path} does not exist", file=sys.stderr)
        return 1
    # allow_fetch=False: this is asked about a file we already have, and a
    # coverage question that silently downloaded a different file would be
    # answering about something other than what the caller is holding.
    tt = Timetable(path, allow_fetch=False)
    first, last = tt.service_window()
    window = f"{first}..{last}" if first else "no dated calendar at all"

    if not args.day:
        print(window)
        return 0

    # Use exactly the recording area's cohorts; a calendar elsewhere is insufficient.
    from process_snapshots import stops_in_box
    missing = tt.uncovered_cohorts(date.fromisoformat(args.day), stops_in_box(tt))
    if tt.covers_day(date.fromisoformat(args.day)) and not missing:
        if not args.quiet:
            print(f"{path.name} covers {args.day} (describes {window})")
        return 0
    print(f"{path.name} does NOT cover all local cohorts for {args.day} (describes {window}; uncovered={missing})",
          file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
