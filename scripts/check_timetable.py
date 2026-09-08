#!/usr/bin/env python3
"""Semantic checks on a built timetable, before it is published.

The release workflow used to count rows and stop. Non-empty is not the same as
correct: every defect the pre-release audit found in this data would have
sailed through a row count. These are the properties that, when they break,
break quietly — a service day folded at midnight, a trip's stops reordered by
the clock, a service with no calendar behind it at all.

Exits non-zero, loudly, with the counts that failed. Run:

    python scripts/check_timetable.py data/timetable.sqlite
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

# A feed this small means something went wrong upstream rather than that the
# network shrank. Set well below the ~5,400 stops the South East bundle
# currently yields, so ordinary variation does not trip it.
MIN_STOPS = 2_000
MIN_TRIPS = 5_000
MIN_STOP_TIMES = 100_000

# Trips whose stops are timed identically end to end are a symptom of a
# conversion that lost its times, not of a real service.
MAX_ZERO_LENGTH_TRIP_FRACTION = 0.02


def fail(message: str) -> None:
    print(f"FAIL  {message}", file=sys.stderr)


def main(path: Path, allow_small: bool = False) -> int:
    """`allow_small` skips the size floors only — every semantic check still
    runs. It exists so the tests can drive this over a fixture of a dozen
    rows; the release workflow never passes it."""
    if not path.exists():
        fail(f"{path} does not exist")
        return 1

    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    problems = 0

    # ── Size ────────────────────────────────────────────────
    counts = {}
    for table in ("stops", "routes", "trips", "stop_times",
                  "calendar", "calendar_dates"):
        counts[table] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"{table}: {counts[table]:,}")

    for table, floor in (() if allow_small else
                         (("stops", MIN_STOPS), ("trips", MIN_TRIPS),
                          ("stop_times", MIN_STOP_TIMES))):
        if counts[table] < floor:
            fail(f"{table} has {counts[table]:,} rows, below the floor of {floor:,} "
                 "— this looks like a partial build, not a smaller network")
            problems += 1

    # ── Service days run past midnight ──────────────────────
    #
    # GTFS times beyond 24:00 are how a feed says "still the same service day".
    # The ingest used to apply `% 86400`, which filed the tail of every
    # overnight trip under the wrong day. A feed for this region always has
    # some: if none survive, they were folded away.
    overnight = con.execute(
        "SELECT COUNT(*) FROM stop_times WHERE dep_secs >= 86400").fetchone()[0]
    print(f"stop_times past midnight (>= 24:00): {overnight:,}")
    if overnight == 0:
        fail("no departure is timed past 24:00 — overnight services have been "
             "folded into the following morning, and their trips reordered")
        problems += 1

    # ── Stop order is the feed's ────────────────────────────
    #
    # If order were re-derived by sorting on departure time, no trip could ever
    # have a later stop leaving earlier than an earlier one. Real overnight
    # trips do not have that property either — but a trip whose *sequence*
    # disagrees with its times in the wrong direction means the sequence was
    # thrown away and rebuilt.
    backwards = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT st.tid FROM stop_times st
            JOIN stop_times nxt
              ON nxt.tid = st.tid AND nxt.seq = st.seq + 1
            WHERE nxt.dep_secs < st.dep_secs
            GROUP BY st.tid
        )
    """).fetchone()[0]
    print(f"trips whose times decrease along their sequence: {backwards:,}")
    if backwards:
        fail(f"{backwards:,} trips call at stops in an order their departure "
             "times contradict — the service-day offset has been lost")
        problems += 1

    # ── Trips have duration ─────────────────────────────────
    zero_length = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT tid FROM stop_times
            GROUP BY tid
            HAVING MIN(dep_secs) = MAX(dep_secs) AND COUNT(*) > 1
        )
    """).fetchone()[0]
    trips = max(counts["trips"], 1)
    print(f"trips with no elapsed time: {zero_length:,} "
          f"({zero_length / trips:.2%})")
    if zero_length / trips > MAX_ZERO_LENGTH_TRIP_FRACTION:
        fail("too many trips begin and end at the same second")
        problems += 1

    # ── Every trip belongs to a service that is described ───
    #
    # A trip whose service_id appears in neither table can never be shown to
    # run or not run; the API's answer for it is whatever its default is, and
    # the default used to be "yes".
    undescribed = con.execute("""
        SELECT COUNT(DISTINCT service_id) FROM trips
        WHERE service_id NOT IN (SELECT service_id FROM calendar)
          AND service_id NOT IN (SELECT service_id FROM calendar_dates)
    """).fetchone()[0]
    print(f"service_ids with no calendar and no exceptions: {undescribed:,}")
    if undescribed:
        fail(f"{undescribed:,} services have no calendar entry of any kind — "
             "nothing can say which days they run")
        problems += 1

    # ── Coordinates are on this coast ───────────────────────
    off_map = con.execute("""
        SELECT COUNT(*) FROM stops
        WHERE lat NOT BETWEEN 50.0 AND 52.0
           OR lon NOT BETWEEN -2.0 AND 1.5
    """).fetchone()[0]
    print(f"stops outside the region: {off_map:,}")
    if off_map:
        fail(f"{off_map:,} stops are outside the South East — a transposed or "
             "unparsed coordinate")
        problems += 1

    con.close()
    if problems:
        print(f"\n{problems} check(s) failed — not publishing.", file=sys.stderr)
        return 1
    print("\nAll semantic checks passed.")
    return 0


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    target = Path(args[0]) if args else Path("data/timetable.sqlite")
    sys.exit(main(target, allow_small="--allow-small" in sys.argv))
