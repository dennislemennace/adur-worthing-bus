"""Correct BODS's GTFS calendars where its conversion is shown to be wrong.

The curated entries live in data/timetable_corrections.json, each with its
evidence, caveats, source and the feed version it was checked against. Two
kinds, both from the 2026 holiday season (docs/reliability/
bods-2026-holiday-conversion.md):

``move_exception``
    BODS's feed put TransXChange's NewYearsEve on 1 December 2026, so calendars
    that skip New Year's Eve skipped 1 December instead, and services that run
    only on New Year's Eve ran on 1 December. Each calendar's 1 December
    exception moves to 31 December. Where the calendar ends before then, or
    already says the same about 31 December, the misplaced date is dropped. A
    1 December exception beside one of the same kind on 30 November or 2
    December is a run of dates, not New Year's Eve, and is left alone. With
    ``fill_date_lists``, a service dated day by day (school days) that runs on
    30 November, 2 December and 8 December but has nothing on 1 December gets
    1 December back: the same removal, made inside a list of dates.

    Applied only while the feed still shows the error: enough calendars that
    treat Christmas Eve and 1 December alike, against few that treat Christmas
    Eve and 31 December alike outside school-holiday runs. A corrected feed
    turns this off by itself.

``restore_only_day``
    A calendar of a fortnight or less with no running day left, whose removals
    include 25 December or 1 January on a day its pattern runs: the services
    that run only on Christmas Day or New Year's Day. Those days are put back.

Nothing here guesses beyond the entries: a date not named in a correction is
never touched. Every change is returned for the timetable's corrections table,
by date and operator, so a board on that day can say the times were corrected.
"""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "data" / "timetable_corrections.json"
WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

log = logging.getLogger("timetable_corrections")


def load(path: Path = DATA) -> list:
    return json.loads(Path(path).read_text(encoding="utf-8"))["corrections"]


def _day(stamp: str) -> date:
    return date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:8]))


def _stamp(day: date) -> str:
    return day.strftime("%Y%m%d")


def _in_range(cal: dict, stamp: str) -> bool:
    return cal["start_date"] <= stamp <= cal["end_date"]


def _pattern_runs(cal: dict, day: date) -> bool:
    return _in_range(cal, _stamp(day)) and str(cal.get(WEEKDAYS[day.weekday()], "0")) == "1"


def _runs(cal, exc: dict, stamp: str) -> bool:
    """GTFS's rule: an exception decides the day; otherwise the weekly pattern."""
    if stamp in exc:
        return exc[stamp] == "1"
    return cal is not None and _pattern_runs(cal, _day(stamp))


def signature(calendar: dict, calendar_dates: dict, entry: dict) -> tuple:
    """(calendars treating the pair date and the wrong date alike,
    calendars treating the pair date and the right date alike outside school runs).
    Only calendars whose range covers both dates count."""
    sig = entry["signature"]
    wrong, right, pair = entry["from_date"], entry["to_date"], sig["pair_date"]
    school = sig.get("school_dates", [])
    with_wrong = with_right = 0
    for sid, exc in calendar_dates.items():
        cal = calendar.get(sid)
        if cal is None or not (_in_range(cal, wrong) and _in_range(cal, right)):
            continue
        kind = exc.get(pair)
        if kind is None:
            continue
        if exc.get(wrong) == kind:
            with_wrong += 1
        if exc.get(right) == kind and not any(exc.get(s) == kind for s in school):
            with_right += 1
    return with_wrong, with_right


def move_exception(calendar: dict, calendar_dates: dict, entry: dict) -> list:
    """[(service_id, "moved" | "dropped")], changing `calendar_dates` in place."""
    wrong, right = entry["from_date"], entry["to_date"]
    before, after = _stamp(_day(wrong) - timedelta(days=1)), _stamp(_day(wrong) + timedelta(days=1))
    changed = []
    for sid, exc in calendar_dates.items():
        kind = exc.get(wrong)
        if kind is None or exc.get(before) == kind or exc.get(after) == kind:
            continue
        there = exc.get(right)
        if there is not None and there != kind:
            continue        # the feed says the opposite on the right day: leave both
        cal = calendar.get(sid)
        del exc[wrong]
        if there is None and (cal is None or _in_range(cal, right)):
            exc[right] = kind
            changed.append((sid, "moved"))
        else:
            changed.append((sid, "dropped"))
    if entry.get("fill_date_lists"):
        # Services dated day by day (school days, mostly) had the same date
        # taken out of their list: an unbroken run either side of it and the
        # same weekday a week later, with nothing said about the day itself.
        week_on = _stamp(_day(wrong) + timedelta(days=7))
        for sid, exc in calendar_dates.items():
            if sid in calendar or wrong in exc:
                continue
            if exc.get(before) == exc.get(after) == exc.get(week_on) == "1":
                exc[wrong] = "1"
                changed.append((sid, "filled"))
    return changed


def restore_only_day(calendar: dict, calendar_dates: dict, entry: dict) -> list:
    """[(service_id, date restored)], changing `calendar_dates` in place."""
    span = int(entry.get("max_span_days", 14))
    changed = []
    for sid, exc in calendar_dates.items():
        cal = calendar.get(sid)
        if cal is None:
            continue
        start, end = _day(cal["start_date"]), _day(cal["end_date"])
        days = (end - start).days + 1
        if days < 1 or days > span:
            continue
        runs = False
        for i in range(days):
            d = start + timedelta(days=i)
            kind = exc.get(_stamp(d))
            if kind == "1" or (kind is None and _pattern_runs(cal, d)):
                runs = True
                break
        if runs:
            continue
        for stamp in entry["dates"]:
            if exc.get(stamp) == "0" and _pattern_runs(cal, _day(stamp)):
                del exc[stamp]
                changed.append((sid, stamp))
    return changed


def apply(timetable: dict, corrections: list | None = None) -> list:
    """Apply every correction to `timetable` (the build's dict) and return the
    rows for its corrections table: one per changed date, correction and operator."""
    corrections = load() if corrections is None else corrections
    calendar, calendar_dates = timetable["calendar"], timetable["calendar_dates"]
    noc_of_route = {rid: (r.get("noc") or "?") for rid, r in timetable["routes"].items()}
    trips = defaultdict(Counter)        # service_id -> {noc: trips}
    for trip in timetable["trips"].values():
        trips[trip.get("service_id", "")][noc_of_route.get(trip.get("route_id", ""), "?")] += 1
    tally = defaultdict(lambda: [set(), 0])     # (date, id, noc) -> [services, trips]
    for entry in corrections:
        before = {sid: dict(exc) for sid, exc in calendar_dates.items()}
        if entry["kind"] == "move_exception":
            with_wrong, with_right = signature(calendar, calendar_dates, entry)
            sig = entry["signature"]
            if with_wrong < sig["min_calendars"] or with_wrong < sig["min_ratio"] * with_right:
                log.info("Correction %s: not applied, the feed no longer shows the error "
                         "(%d calendars pair %s with %s, %d with %s)", entry["id"], with_wrong,
                         sig["pair_date"], entry["from_date"], with_right, entry["to_date"])
                continue
            move_exception(calendar, calendar_dates, entry)
            dates = [entry["from_date"], entry["to_date"]]
        elif entry["kind"] == "restore_only_day":
            restore_only_day(calendar, calendar_dates, entry)
            dates = list(entry["dates"])
        else:
            raise ValueError(f"unknown correction kind {entry['kind']!r} in {entry['id']}")
        # A change is recorded only where it changes whether the service runs
        # that day: a Saturday calendar's removal of a Tuesday mattered to no one.
        changes = [(sid, stamp) for sid, exc in calendar_dates.items() if exc != before.get(sid, {})
                   for stamp in dates
                   if _runs(calendar.get(sid), before.get(sid, {}), stamp) != _runs(calendar.get(sid), exc, stamp)]
        mine = defaultdict(set)
        for sid, stamp in changes:
            for noc, n in trips.get(sid, {}).items():
                row = tally[(stamp, entry["id"], noc)]
                row[0].add(sid)
                row[1] += n
                mine[noc].add(sid)
        log.info("Correction %s: %d calendar changes (%s)", entry["id"], len(changes),
                 ", ".join(f"{noc} {len(s)} calendars" for noc, s in sorted(mine.items())) or "none")
    summaries = {e["id"]: e for e in corrections}
    return [{"date": stamp, "id": cid, "noc": noc, "services": len(sids), "trips": n,
             "summary": summaries[cid]["summary"], "url": summaries[cid]["url"]}
            for (stamp, cid, noc), (sids, n) in sorted(tally.items())]
