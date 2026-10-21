"""The boundary evidence measures a full week, not half term.

The timetable rebuilt on 21 October 2026 had the evidence measure the week of
26 October, which is half term on both sides of the line. The school-day
routes stopped, the weekday route count in the band fell from 20 and 39 to 13
and 27, Lancing's departures per stop from 32.8 to 32.3, and the objectives
page failed its check against the evidence, all with nothing about the
service changed. The week after measures exactly what the week before did.

Run with:  pytest tests/test_evidence_week.py
"""

import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import build_evidence as be      # noqa: E402


def test_half_term_is_passed_over_for_the_week_after():
    assert be.first_full_service([13, 20, 20, 20, 20]) == 1


def test_an_ordinary_week_is_measured_as_before():
    assert be.first_full_service([20, 20, 20, 20]) == 0


def test_the_two_short_weeks_of_christmas_are_both_passed_over():
    assert be.first_full_service([5, 5, 20, 20, 20]) == 2


def test_with_nothing_to_compare_the_coming_week_is_measured():
    # The timetable runs out after the coming week: there is no better one.
    assert be.first_full_service([13, 0, 0]) == 0
    assert be.first_full_service([0, 20]) == 0


def _inside(area_id):
    """A point inside one of the named places, which the comparison needs a stop in."""
    area = next(a for a in be.load_areas()["areas"] if a["id"] == area_id)
    x0, y0, x1, y1 = be.bbox(area["rings"])
    for i in range(1, 40):
        for j in range(1, 40):
            lon, lat = x0 + (x1 - x0) * i / 40, y0 + (y1 - y0) * j / 40
            if be.in_rings(area["rings"], lon, lat):
                return lat, lon
    raise AssertionError(f"no point found inside {area_id}")


def _timetable(half_term):
    con = sqlite3.connect(":memory:")
    con.executescript("""
        CREATE TABLE stops (sid INTEGER PRIMARY KEY, stop_id TEXT, name TEXT, lat REAL, lon REAL);
        CREATE TABLE routes (rid INTEGER PRIMARY KEY, route_id TEXT, short_name TEXT, long_name TEXT, noc TEXT);
        CREATE TABLE trips (tid INTEGER PRIMARY KEY, trip_id TEXT, rid INTEGER, service_id TEXT, headsign TEXT);
        CREATE TABLE stop_times (tid INTEGER, seq INTEGER, sid INTEGER, dep_secs INTEGER, timepoint INTEGER);
        CREATE TABLE calendar (service_id TEXT PRIMARY KEY, monday INT, tuesday INT, wednesday INT,
            thursday INT, friday INT, saturday INT, sunday INT, start_date TEXT, end_date TEXT);
        CREATE TABLE calendar_dates (service_id TEXT, date TEXT, exception INT);
    """)
    # One stop either side of the line, inside the band, and one in each place.
    con.executemany("INSERT INTO stops VALUES (?,?,?,?,?)",
                    [(1, "W", "West", 50.83, -0.24), (2, "E", "East", 50.83, -0.19),
                     (3, "L", "Lancing", *_inside("lancing")),
                     (4, "P", "South Portslade", *_inside("south-portslade"))])
    con.executemany("INSERT INTO routes VALUES (?,?,?,?,?)",
                    [(1, "R700", "700", "", "SCSO"), (2, "R602", "602", "", "SCSO")])
    # The 700 runs every day; the 602 is a school bus, off in half term.
    con.executemany("INSERT INTO trips VALUES (?,?,?,?,?)",
                    [(1, "T700", 1, "DAILY", ""), (2, "T602", 2, "SCHOOL", "")])
    con.executemany("INSERT INTO stop_times VALUES (?,?,?,?,?)",
                    [(1, 0, 1, 28800, 1), (1, 1, 2, 29400, 1), (2, 0, 1, 29000, 1)])
    con.executemany("INSERT INTO calendar VALUES (?,?,?,?,?,?,?,?,?,?)",
                    [("DAILY", 1, 1, 1, 1, 1, 1, 1, "20260101", "20271231"),
                     ("SCHOOL", 1, 1, 1, 1, 1, 0, 0, "20260101", "20271231")])
    con.executemany("INSERT INTO calendar_dates VALUES (?,?,?)",
                    [("SCHOOL", d.strftime("%Y%m%d"), 2) for d in half_term])
    return con


def test_a_timetable_with_the_school_buses_off_next_week_is_measured_the_week_after():
    half_term = [date(2026, 10, 26) + timedelta(days=i) for i in range(5)]
    con = _timetable(half_term)
    monday, passed = be.choose_week(con, be.load_areas(), today=date(2026, 10, 21))
    assert monday == date(2026, 11, 2)
    assert passed == [date(2026, 10, 26)]


def test_a_timetable_with_no_holiday_ahead_is_measured_the_coming_week():
    con = _timetable([])
    monday, passed = be.choose_week(con, be.load_areas(), today=date(2026, 10, 14))
    assert monday == date(2026, 10, 19) and passed == []
