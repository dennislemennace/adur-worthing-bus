"""Which journey a bus runs next, learned from what buses really ran
(api/turnarounds.py, scripts/build_turnarounds.py, Timetable.next_by_turnaround,
live_eta.from_previous_journey).

Brighton & Hove publish no blocks, so before this a journey of theirs showed
"Timetable" until its bus set off. The learned guess must pick the first
journey a bus could take, of the service buses there went on to, starting near
where the last one ended; and say nothing where buses mostly went home.

Run with:  pytest
"""
import csv
import gzip
import io
import json
import sys
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import build_timetable as bt              # noqa: E402
import build_turnarounds as builder       # noqa: E402
import check_published as cp              # noqa: E402
import json_to_sqlite as j2s              # noqa: E402
from api import live_eta                  # noqa: E402
from api.timetable_db import Timetable    # noqa: E402

STOPS = [("4400TERM", "Hangleton", 50.8500, -0.2000),
         ("4400TERMB", "Hangleton stand", 50.8505, -0.2005),     # about 65 m away
         ("4400STEINE", "Old Steine", 50.8230, -0.1370)]
# (trip, route, service, [(stop, time)])
TRIPS = [
    ("IN_1000", "R5", "WK", [("4400STEINE", "10:00:00"), ("4400TERM", "10:40:00")]),
    ("OUT_1045", "R5", "WK", [("4400TERM", "10:45:00"), ("4400STEINE", "11:25:00")]),
    ("FAR_1050", "R5", "WK", [("4400STEINE", "10:50:00"), ("4400TERM", "11:30:00")]),
    ("OUT_1055", "R5", "WK", [("4400TERMB", "10:55:00"), ("4400STEINE", "11:35:00")]),
    ("OUT_5A_1050", "R5A", "WK", [("4400TERM", "10:50:00"), ("4400STEINE", "11:30:00")]),
    ("SAT_1051", "R5", "SAT", [("4400TERM", "10:51:00"), ("4400STEINE", "11:31:00")]),
]
TUESDAY = date(2026, 9, 29)


def _csv(rows, header):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue()


@pytest.fixture(scope="module")
def tt(tmp_path_factory):
    mp = pytest.MonkeyPatch()
    mp.setenv("SKIP_OSRM", "1")
    feed = tmp_path_factory.mktemp("gtfs") / "feed.zip"
    with zipfile.ZipFile(feed, "w") as z:
        z.writestr("agency.txt", _csv([("AG1", "Brighton & Hove", "BHBC")],
                                      ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv([("R5", "AG1", "5", "", 3), ("R5A", "AG1", "5A", "", 3)],
                                      ["route_id", "agency_id", "route_short_name",
                                       "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv([(t, r, s, "Hangleton", "", "") for t, r, s, _ in TRIPS],
                                     ["trip_id", "route_id", "service_id", "trip_headsign",
                                      "shape_id", "block_id"]))
        z.writestr("stop_times.txt", _csv(
            [(t, i + 1, sid, hms, hms) for t, _, _, calls in TRIPS for i, (sid, hms) in enumerate(calls)],
            ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"]))
        z.writestr("calendar.txt", _csv(
            [("WK", 1, 1, 1, 1, 1, 0, 0, "20260101", "20271231"),
             ("SAT", 0, 0, 0, 0, 0, 1, 0, "20260101", "20271231")],
            ["service_id", "monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday", "start_date", "end_date"]))
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(bt.parse_gtfs(str(feed)), out)
    yield Timetable(out, allow_fetch=False)
    mp.undo()


def cells(next_service="5", after=600, band=True):
    key = "BHBC|5|4400TERM|weekday" + ("|midday" if band else "")
    return {"cells": {key: {"next": next_service, "after": after, "journeys": 20, "share": 0.8}}}


def test_the_first_journey_a_bus_could_take_after_its_usual_break(tt):
    # Ends 10:40, usually leaves ten minutes or more later: not the 10:45, not
    # the 10:50 from the far end of the route, but the 10:55 from the stand.
    nxt = tt.next_by_turnaround("IN_1000", TUESDAY, cells(after=600))
    assert nxt and nxt["trip_id"] == "OUT_1055", nxt
    assert (nxt["service"], nxt["from_name"]) == ("5", "Hangleton stand")
    assert tt.next_by_turnaround("IN_1000", TUESDAY, cells(after=0))["trip_id"] == "OUT_1045"


def test_the_service_buses_there_go_on_to(tt):
    assert tt.next_by_turnaround("IN_1000", TUESDAY, cells("5A", after=0))["trip_id"] == "OUT_5A_1050"


def test_without_the_band_the_coarse_cell_answers(tt):
    assert tt.next_by_turnaround("IN_1000", TUESDAY, cells(after=600, band=False))["trip_id"] == "OUT_1055"


def test_nothing_learned_or_nothing_within_the_hour_is_no_journey(tt):
    assert tt.next_by_turnaround("IN_1000", TUESDAY, {"cells": {}}) is None
    assert tt.next_by_turnaround("IN_1000", TUESDAY, None) is None
    assert tt.next_by_turnaround("IN_1000", TUESDAY, cells(after=3000)) is None, "past the hour"


def test_only_journeys_running_that_day(tt):
    saturday = {"cells": {"BHBC|5|4400TERM|saturday": {"next": "5", "after": 0, "journeys": 20, "share": 0.8}}}
    # The Saturday 10:51 is the only one running; a weekday journey is not.
    assert tt.next_by_turnaround("IN_1000", date(2026, 10, 3), saturday)["trip_id"] == "SAT_1051"


def _observations(path, day, sequences):
    rows = []
    for vehicle, trips in sequences.items():
        for i, trip in enumerate(trips):
            rows.append({"match": "declared", "estimated": False, "vehicle": vehicle, "operator": "BHBC",
                         "trip_id": trip, "observed_epoch": 1_000_000 + i * 3600})
    with gzip.open(path, "wt") as fh:
        json.dump({"day": day, "observations": rows}, fh)


def test_build_learns_the_next_service_and_a_low_gap(tt, tmp_path):
    for day in ("2026-09-29", "2026-09-30", "2026-10-01"):
        _observations(tmp_path / f"observations-{day}.json.gz", day, {
            "V1": ["IN_1000", "OUT_1055"], "V2": ["IN_1000", "OUT_1055"],
            "V3": ["IN_1000", "OUT_1045"], "V4": ["IN_1000"]})       # V4 went to the garage
    doc = builder.build(sorted(tmp_path.glob("*.json.gz")), tt)
    cell = doc["cells"]["BHBC|5|4400TERM|weekday|midday"]
    assert cell["next"] == "5" and cell["journeys"] == 12 and cell["share"] == 0.75
    assert cell["after"] == 300, "the low end of the gap, so the first plausible journey"
    assert doc["cells"]["BHBC|5|4400TERM|weekday"] == cell
    fails = cp.Failures()
    cp.check_turnarounds(doc, "turnarounds.json", fails)
    assert not fails.items, fails.items


def test_build_says_nothing_where_most_buses_went_home(tt, tmp_path):
    for day in ("2026-09-29", "2026-09-30", "2026-10-01"):
        _observations(tmp_path / f"observations-{day}.json.gz", day, {
            "V1": ["IN_1000", "OUT_1055"], "V2": ["IN_1000"], "V3": ["IN_1000"], "V4": ["IN_1000"]})
    assert builder.build(sorted(tmp_path.glob("*.json.gz")), tt)["cells"] == {}


def test_build_wants_several_days(tt, tmp_path):
    for day in ("2026-09-29", "2026-09-30"):
        _observations(tmp_path / f"observations-{day}.json.gz", day,
                      {f"V{i}": ["IN_1000", "OUT_1055"] for i in range(8)})
    assert builder.build(sorted(tmp_path.glob("*.json.gz")), tt)["cells"] == {}


def test_a_cell_naming_no_service_or_a_gap_past_the_hour_fails_the_check():
    good = {"schema_version": 1, "method": "m", "as_of": "2026-11-03T00:00:00+00:00", "days": ["2026-11-02"],
            "percentile": 10, "cells": {"BHBC|5|4400TERM|weekday": {"next": "5", "after": 300,
                                                                    "journeys": 12, "share": 0.75}}}
    for bad in ({"next": "", "after": 300, "journeys": 12, "share": 0.75},
                {"next": "5", "after": 7200, "journeys": 12, "share": 0.75},
                {"next": "5", "after": 300, "journeys": 12, "share": 0.4}):
        fails = cp.Failures()
        cp.check_turnarounds({**good, "cells": {"BHBC|5|4400TERM|weekday": bad}}, "t.json", fails)
        assert fails.items, bad


def test_the_learned_journey_comes_before_the_block(monkeypatch):
    class TT:
        def next_by_turnaround(self, *_a):
            return {"trip_id": "LEARNED", "depart_secs": 11 * 3600}

        def next_in_block(self, *_a):
            return {"trip_id": "BLOCK", "depart_secs": 11 * 3600}

    end = {"is_terminus": True, "passed": False, "expected": "2026-09-29T10:05:00+00:00"}
    monkeypatch.setattr(live_eta, "project_trip", lambda *_a, **_k: [end])
    bus = {"trip_id": "IN_1000", "trip_source": "feed"}
    now = datetime(2026, 9, 29, 9, 30, tzinfo=timezone.utc)
    assert live_eta.from_previous_journey(TT(), bus, now, TUESDAY, None, None, cells())["trip_id"] == "LEARNED"
    assert live_eta.from_previous_journey(TT(), bus, now, TUESDAY)["trip_id"] == "BLOCK", \
        "without the learned cells, the block as before"


def test_only_half_the_late_start_is_passed_on(monkeypatch):
    # Being wrong about which bus works a journey shows a late time for a bus
    # that comes on time, and the reader misses it. Arriving 10 minutes after
    # the next journey is due to leave, the board says 5 minutes late.
    class TT:
        def next_by_turnaround(self, *_a):
            return {"trip_id": "LEARNED", "depart_secs": 10 * 3600}

    end = {"is_terminus": True, "passed": False, "expected": "2026-09-29T09:10:00+00:00"}   # 10:10 London
    monkeypatch.setattr(live_eta, "project_trip", lambda *_a, **_k: [end])
    bus = {"trip_id": "IN_1000", "trip_source": "feed"}
    now = datetime(2026, 9, 29, 8, 30, tzinfo=timezone.utc)
    assert live_eta.from_previous_journey(TT(), bus, now, TUESDAY, None, None, cells())["lateness_secs"] == 300
