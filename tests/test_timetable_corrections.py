"""BODS's 2026 holiday conversion errors are corrected in our build.

BODS's south-east GTFS (feed 20261124_044012, checked 26 November 2026) puts
TransXChange's NewYearsEve on 1 December 2026 instead of 31 December. Brighton &
Hove's route 7 file and Stagecoach's 700 files never mention 1 December, yet
their GTFS calendars remove it where 31 December should be, so Tuesday 1
December lost every B&H and 700 journey and New Year's Eve kept journeys the
operators exclude. The same feed removes 25 December and 1 January from the
calendars of B&H's Christmas Day and New Year's Day services, the only days
those calendars run. Evidence: docs/reliability/bods-2026-holiday-conversion.md.

Each calendar below is shaped like one in that feed.

Run with:  pytest tests/test_timetable_corrections.py
"""

import csv
import io
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import build_timetable as bt      # noqa: E402

HOLIDAYS = ["20261224", "20261225", "20261226", "20261228", "20270101"]

# service_id: (pattern Mon..Sun, start, end, {date: exception_type})
CALENDARS = {
    # The 700's Mon-Fri calendar: 31 December replaced by 1 December.
    "WKDY": ("1111100", "20261124", "20270824", {**{d: 2 for d in HOLIDAYS}, "20261201": 2}),
    "SAT":  ("0000010", "20261124", "20270824", {**{d: 2 for d in HOLIDAYS}, "20261201": 2}),
    # Sunday service: runs on the Monday holidays, not on 1 Jan or NYE.
    "SUN":  ("0000001", "20261124", "20270824", {"20261201": 2, "20261224": 2, "20261225": 2,
                                                 "20261226": 2, "20270101": 2, "20261228": 1}),
    # B&H's weekly window, 29 Nov to 20 Dec: NYE is outside it.
    "BHWK": ("1111100", "20261129", "20261220", {"20261201": 2}),
    # B&H's Christmas Day service: Friday only, in the week of 20-26 Dec.
    "XMAS": ("0000100", "20261220", "20261226", {"20261224": 2, "20261225": 2, "20261226": 2}),
    # B&H's New Year's Day service: Mon and Fri, 31 Dec to 2 Jan.
    "NYD":  ("1000100", "20261231", "20270102", {"20270101": 2}),
    # A service dated only by exception that runs on Christmas Eve and NYE.
    "EVE":  (None, None, None, {"20261201": 1, "20261224": 1}),
    # A run of ordinary days: 1 December is one of many, not New Year's Eve.
    "RUN":  ("1111100", "20261124", "20270824", {"20261130": 2, "20261201": 2, "20261202": 2,
                                                 **{d: 2 for d in HOLIDAYS}}),
    # A school-day calendar that already lists 31 December itself.
    "SCHOOL": ("1111100", "20261124", "20270824", {**{d: 2 for d in HOLIDAYS}, "20261201": 2,
                                                   "20261221": 2, "20261222": 2, "20261223": 2,
                                                   "20261229": 2, "20261230": 2, "20261231": 2}),
    # School days dated one by one: the same removal punches 1 December out
    # of an unbroken run of weekdays (B&H 18988, Metrobus 19926, Compass 5146).
    "TERM": (None, None, None, {"20261130": 1, "20261202": 1, "20261203": 1, "20261208": 1}),
    # Dated lists that are not unbroken around 1 December stay as they are.
    "ODD":  (None, None, None, {"20261127": 1, "20261204": 1, "20261208": 1}),
    "LATE": (None, None, None, {"20261202": 1, "20261203": 1, "20261208": 1}),
    # Controls: no 1 December exception; a Christmas week calendar with other days.
    "PLAIN": ("1111100", "20261124", "20270824", {d: 2 for d in HOLIDAYS}),
    "SUNXW": ("0000001", "20261220", "20261226", {"20261224": 2, "20261225": 2, "20261226": 2}),
}

STOPS = [("4400AAA", "First", 50.83, -0.40), ("4400BBB", "Second", 50.82, -0.38)]


def _csv(rows, header):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue()


@pytest.fixture(scope="module")
def timetable(tmp_path_factory):
    path = tmp_path_factory.mktemp("gtfs") / "feed.zip"
    services = sorted(CALENDARS)
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("agency.txt", _csv([("AG1", "Brighton & Hove", "BHBC")],
                                      ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv([("R7", "AG1", "7", "Hove - Marina", 3)],
                                      ["route_id", "agency_id", "route_short_name",
                                       "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv([(f"T{s}", "R7", s, "Marina", "") for s in services],
                                     ["trip_id", "route_id", "service_id", "trip_headsign",
                                      "shape_id"]))
        z.writestr("stop_times.txt", _csv(
            [row for s in services for row in ((f"T{s}", 0, "4400AAA", "09:00:00", "09:00:00", 1),
                                               (f"T{s}", 1, "4400BBB", "09:10:00", "09:10:00", 1))],
            ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time", "timepoint"]))
        z.writestr("calendar.txt", _csv(
            [(s, *pattern, start, end) for s, (pattern, start, end, _e) in CALENDARS.items() if pattern],
            ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday",
             "saturday", "sunday", "start_date", "end_date"]))
        z.writestr("calendar_dates.txt", _csv(
            [(s, d, e) for s, (_p, _s, _e, exc) in CALENDARS.items() for d, e in sorted(exc.items())],
            ["service_id", "date", "exception_type"]))
    mp = pytest.MonkeyPatch()
    mp.setenv("SKIP_OSRM", "1")
    try:
        yield bt.parse_gtfs(str(path))
    finally:
        mp.undo()


def _exc(tt, sid):
    """{date: "1" added / "0" removed}, as the build stores them."""
    return tt["calendar_dates"].get(sid, {})


def test_new_years_eve_goes_back_from_1_december_to_31_december(timetable):
    for sid in ("WKDY", "SAT", "SUN"):
        exc = _exc(timetable, sid)
        assert "20261201" not in exc, f"{sid} still skips Tuesday 1 December"
        assert exc.get("20261231") == "0", f"{sid} should not run its regular timetable on New Year's Eve"
    # Everything else the feed said is kept.
    assert _exc(timetable, "SUN")["20261228"] == "1"
    assert _exc(timetable, "WKDY")["20261225"] == "0"


def test_a_window_that_ends_before_new_years_eve_just_loses_the_misplaced_date(timetable):
    exc = _exc(timetable, "BHWK")
    assert "20261201" not in exc and "20261231" not in exc


def test_a_new_years_eve_special_runs_on_31_december_not_1_december(timetable):
    assert _exc(timetable, "EVE") == {"20261224": "1", "20261231": "1"}


def test_a_school_calendar_that_already_skips_31_december_runs_on_1_december(timetable):
    exc = _exc(timetable, "SCHOOL")
    assert "20261201" not in exc and exc["20261231"] == "0"


def test_school_days_dated_one_by_one_get_1_december_back(timetable):
    assert _exc(timetable, "TERM")["20261201"] == "1"
    assert "20261201" not in _exc(timetable, "ODD")
    assert "20261201" not in _exc(timetable, "LATE")


def test_a_run_of_ordinary_days_is_left_alone(timetable):
    exc = _exc(timetable, "RUN")
    assert exc["20261130"] == exc["20261201"] == exc["20261202"] == "0"
    assert "20261231" not in exc


def test_christmas_day_and_new_years_day_services_run_on_their_day(timetable):
    xmas = _exc(timetable, "XMAS")
    assert "20261225" not in xmas, "B&H's Christmas Day service would never run"
    assert xmas["20261224"] == xmas["20261226"] == "0"
    assert "20270101" not in _exc(timetable, "NYD"), "B&H's New Year's Day service would never run"


def test_a_calendar_with_other_days_keeps_its_christmas_removal(timetable):
    assert _exc(timetable, "SUNXW")["20261225"] == "0"
    assert _exc(timetable, "PLAIN") == {d: "0" for d in HOLIDAYS}


def test_every_change_is_recorded_by_date_and_operator(timetable):
    rows = {(r["date"], r["id"]): r for r in timetable["corrections"]}
    nye = rows[("20261201", "bods-2026-new-years-eve-as-1-december")]
    assert nye["noc"] == "BHBC"
    # Where it changes whether a bus runs on Tuesday 1 December: WKDY, BHWK,
    # EVE, SCHOOL and TERM. SAT and SUN never ran on a Tuesday; RUN, ODD and
    # LATE are left alone.
    assert nye["services"] == 5 and nye["trips"] == 5
    nye_31 = rows[("20261231", "bods-2026-new-years-eve-as-1-december")]
    assert nye_31["services"] == 2      # WKDY stops, EVE starts; nothing else changes on a Thursday
    assert ("20261225", "bods-2026-holiday-services-removed") in rows
    assert ("20270101", "bods-2026-holiday-services-removed") in rows
    for r in timetable["corrections"]:
        assert r["summary"] and r["url"].startswith("https://")


def test_the_record_reaches_the_database_and_the_board(timetable, tmp_path):
    """The board line on a corrected day: built, stored, loaded, and shown only
    where an operator it corrected runs."""
    import copy
    from datetime import date
    import json_to_sqlite
    from api import main
    from api.timetable_db import Timetable

    db = tmp_path / "timetable.sqlite"
    json_to_sqlite.convert(copy.deepcopy(timetable), db)
    tt = Timetable(db, allow_fetch=False)
    assert {r["id"] for r in tt.corrections["20261201"]} == {"bods-2026-new-years-eve-as-1-december"}
    note = main._correction_note(tt, date(2026, 12, 1), [{"operator": "BHBC"}])["timetable_correction"]
    assert note["url"].endswith("bods-2026-holiday-conversion.md") and note["summary"]
    assert main._correction_note(tt, date(2026, 12, 2), [{"operator": "BHBC"}]) == {}
    assert main._correction_note(tt, date(2026, 12, 1), [{"operator": "SCSO"}]) == {}, \
        "a board with none of the corrected operator's buses says nothing"


def test_a_feed_without_the_error_is_left_alone():
    """Once BODS fixes its conversion, 31 December carries New Year's Eve again
    beside Christmas Eve, and a stray 1 December exception is not moved."""
    import timetable_corrections as tc

    entry = next(e for e in tc.load() if e["kind"] == "move_exception")
    fixed = {f"S{i}": {"20261224": "0", "20261231": "0"} for i in range(5)}
    fixed["ONE"] = {"20261201": "0", "20261224": "0"}
    calendar = {sid: {"monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1",
                      "friday": "1", "saturday": "0", "sunday": "0",
                      "start_date": "20261124", "end_date": "20270824"} for sid in fixed}
    timetable = {"calendar": calendar, "calendar_dates": fixed,
                 "routes": {"R": {"noc": "BHBC"}}, "trips": {"T": {"route_id": "R", "service_id": "ONE"}}}
    assert tc.apply(timetable, [entry]) == []
    assert fixed["ONE"] == {"20261201": "0", "20261224": "0"}
