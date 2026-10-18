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
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api.timetable_db import service_runs_on  # noqa: E402

# A feed this small means something went wrong upstream rather than that the
# network shrank. Set well below the ~5,400 stops the South East bundle
# currently yields, so ordinary variation does not trip it.
MIN_STOPS = 2_000
MIN_TRIPS = 5_000
MIN_STOP_TIMES = 100_000

# Trips whose stops are timed identically end to end are a symptom of a
# conversion that lost its times, not of a real service.
MAX_ZERO_LENGTH_TRIP_FRACTION = 0.02


# The operators the site cannot do without, and the route it is built around:
# each must run on every day of the next COVERAGE_DAYS. A feed that drops one
# is a broken feed, not a quieter week. Stagecoach South vanished from BODS's
# GTFS from 7 October 2026, and the 4 October build kept serving only because
# later ones failed for other reasons; its Brighton & Hove calendars then ran
# out on 1 November, two days before Euro Bus Expo, and nothing said so.
REQUIRED = ("BHBC", "SCSO", "COMT", "METR", ("SCSO", "700"))
COVERAGE_DAYS = 14


def forward_coverage(con, start: date, days: int = COVERAGE_DAYS,
                     required=REQUIRED) -> dict:
    """`{operator or (operator, route): [dates with no trip]}` over `days`
    from `start`, by the same calendar rule the API uses (`service_runs_on`)."""
    calendar = {}
    for row in con.execute(
            "SELECT service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, "
            "start_date, end_date FROM calendar"):
        calendar[row[0]] = {**dict(zip(("monday", "tuesday", "wednesday", "thursday", "friday",
                                        "saturday", "sunday"), (str(v) for v in row[1:8]))),
                            "start_date": row[8], "end_date": row[9]}
    calendar_dates = {}
    for sid, stamp, exc in con.execute("SELECT service_id, date, exception FROM calendar_dates"):
        calendar_dates.setdefault(sid, {})[stamp] = str(exc)
    services = {}
    for noc, short, sid in con.execute(
            "SELECT DISTINCT r.noc, r.short_name, t.service_id FROM trips t JOIN routes r ON r.rid = t.rid"):
        services.setdefault(noc, set()).add(sid)
        services.setdefault((noc, short), set()).add(sid)
    window = [start + timedelta(days=i) for i in range(days)]
    return {key: [d for d in window
                  if not any(service_runs_on(calendar, calendar_dates, sid, d) for sid in services.get(key, ()))]
            for key in required}


def fail(message: str) -> None:
    print(f"FAIL  {message}", file=sys.stderr)


def main(path: Path, allow_small: bool = False, today: date | None = None) -> int:
    """`allow_small` skips the size floors and the operator coverage, which
    name this region's real operators; every semantic check still runs. It
    exists so the tests can drive this over a fixture of a dozen rows; the
    release workflow never passes it."""
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

    # ── Every main operator runs, every day, for a fortnight ──
    if not allow_small:
        start = today or date.today()
        for key, gaps in forward_coverage(con, start).items():
            name = key if isinstance(key, str) else f"{key[0]} route {key[1]}"
            print(f"{name}: runs on {COVERAGE_DAYS - len(gaps)} of the next {COVERAGE_DAYS} days")
            if gaps:
                fail(f"{name} has no trips on {len(gaps)} of the next {COVERAGE_DAYS} days "
                     f"(first {gaps[0].isoformat()}): an operator missing from the feed, or "
                     "calendars that have run out")
                problems += 1

    con.close()
    if problems:
        print(f"\n{problems} check(s) failed — not publishing.", file=sys.stderr)
        return 1
    print("\nAll semantic checks passed.")
    return 0


def coverage_notices(path: Path, notices: Path, today: date | None = None) -> int:
    """The nightly expiry watch: one notice line per operator and route in
    REQUIRED (scripts/notify_warnings.py), warning while any of the next
    COVERAGE_DAYS has no trips in the timetable being served. Never fails."""
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    lines = []
    for key, gaps in forward_coverage(con, today or date.today()).items():
        name = key if isinstance(key, str) else f"{key[0]}-{key[1]}"
        label = key if isinstance(key, str) else f"{key[0]} route {key[1]}"
        if gaps:
            message = (f"The timetable being served has no {label} trips from {gaps[0].isoformat()} "
                       f"({len(gaps)} of the next {COVERAGE_DAYS} days). Rebuild it: "
                       "gh workflow run update-timetable.yml")
            print(f"::warning::{message}")
            lines.append(f"timetable-expiry:{name.lower()}\twarn\t{message}")
        else:
            lines.append(f"timetable-expiry:{name.lower()}\tclear")
    con.close()
    with open(notices, "a", encoding="utf-8") as out:
        out.writelines(line + "\n" for line in lines)
    return 0


if __name__ == "__main__":
    flags_with_values = {"--notices"}
    args, skip = [], False
    for a in sys.argv[1:]:
        if skip:
            skip = False
        elif a in flags_with_values:
            skip = True
        elif not a.startswith("--"):
            args.append(a)
    target = Path(args[0]) if args else Path("data/timetable.sqlite")
    if "--notices" in sys.argv:
        # python scripts/check_timetable.py data/timetable.sqlite --notices notices.tsv
        sys.exit(coverage_notices(target, Path(sys.argv[sys.argv.index("--notices") + 1])))
    sys.exit(main(target, allow_small="--allow-small" in sys.argv))
