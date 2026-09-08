"""Release-path regressions, using synthetic GTFS and no upstream requests."""

import json
import shutil
import sqlite3
import sys
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import build_timetable
import check_timetable
import json_to_sqlite
from api import main
from api.timetable_db import Timetable


@pytest.fixture
def built_timetable(tmp_path, monkeypatch):
    archive_path = tmp_path / "feed.zip"
    files = {
        "agency.txt": "agency_id,agency_name,agency_noc\noperator,Test,TSTO\n",
        "stops.txt": (
            "stop_id,stop_name,stop_lat,stop_lon\n"
            "4400A,First,50.82,-0.30\n"
            "4400B,Second,50.82,-0.29\n"
            "4400C,Last,50.82,-0.28\n"
        ),
        "routes.txt": (
            "route_id,agency_id,route_short_name,route_long_name,route_type\n"
            "route,operator,700,Test coast,3\n"
        ),
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign\n"
            "route,service,overnight,Last\n"
            "route,service,tied,Last\n"
        ),
        "stop_times.txt": (
            "trip_id,departure_time,stop_id,stop_sequence\n"
            "overnight,24:10:00,4400C,30\n"
            "overnight,23:55:00,4400A,10\n"
            "overnight,24:05:00,4400B,20\n"
            "tied,09:00:00,4400B,20\n"
            "tied,09:00:00,4400A,10\n"
            "tied,09:01:00,4400C,30\n"
        ),
        "calendar_dates.txt": (
            "service_id,date,exception_type\nservice,20260908,1\n"
        ),
    }
    with zipfile.ZipFile(archive_path, "w") as archive:
        for filename, content in files.items():
            archive.writestr(filename, content)

    json_path = tmp_path / "timetable.json"
    database_path = tmp_path / "timetable.sqlite"
    monkeypatch.setenv("SKIP_OSRM", "1")
    monkeypatch.setattr(build_timetable, "OUTPUT_PATH", json_path)
    monkeypatch.setattr(
        build_timetable, "download_gtfs",
        lambda url, destination: shutil.copyfile(archive_path, destination),
    )
    monkeypatch.setattr(json_to_sqlite, "JSON_PATH", json_path)
    monkeypatch.setattr(json_to_sqlite, "DB_PATH", database_path)
    build_timetable.main()
    json_to_sqlite.main()
    return json_path, database_path


def test_real_build_entry_points_preserve_service_times_and_source_order(built_timetable):
    json_path, database_path = built_timetable
    timetable = json.loads(json_path.read_text())
    assert [86700, "overnight", 20] in timetable["stop_times"]["4400B"]
    with sqlite3.connect(database_path) as connection:
        for trip_id, times in (
            ("overnight", [86100, 86700, 87000]),
            ("tied", [32400, 32400, 32460]),
        ):
            rows = connection.execute(
                "SELECT s.stop_id, st.dep_secs FROM stop_times st "
                "JOIN trips t ON t.tid = st.tid JOIN stops s ON s.sid = st.sid "
                "WHERE t.trip_id = ? ORDER BY st.seq", (trip_id,),
            ).fetchall()
            assert rows == list(zip(["4400A", "4400B", "4400C"], times))
    assert check_timetable.main(database_path, allow_small=True) == 0


@pytest.mark.parametrize("corruption", [
    "UPDATE stop_times SET dep_secs = dep_secs % 86400",
    "DELETE FROM calendar_dates",
])
def test_release_gate_rejects_corrupt_semantics(built_timetable, corruption):
    _, database_path = built_timetable
    with sqlite3.connect(database_path) as connection:
        connection.execute(corruption)
    assert check_timetable.main(database_path, allow_small=True) == 1


@pytest.mark.parametrize("now_text,service_date,departure_seconds,expected_text", [
    ("2026-09-08T23:50:00+01:00", date(2026, 9, 8), 86700,
     "2026-09-09T00:05:00+01:00"),
    ("2026-09-09T00:01:00+01:00", date(2026, 9, 8), 86700,
     "2026-09-09T00:05:00+01:00"),
    ("2026-09-08T23:50:00+01:00", date(2026, 9, 9), 300,
     "2026-09-09T00:05:00+01:00"),
    ("2026-09-08T23:50:00+01:00", date(2026, 9, 7), 86700, None),
    ("2026-03-29T00:50:00+00:00", date(2026, 3, 29), 7500,
     "2026-03-29T02:05:00+01:00"),
    ("2026-10-25T01:50:00+01:00", date(2026, 10, 25), 3900,
     "2026-10-25T01:05:00+00:00"),
])
def test_departure_board_reads_rebuilt_service_days(
    monkeypatch, now_text, service_date, departure_seconds, expected_text,
):
    now = datetime.fromisoformat(now_text)

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz else now

    class FixtureTimetable:
        stops = {"4400B": {"name": "Second"}}
        trips = {"trip": {"route_id": "route", "service_id": "service"}}
        routes = {"route": {"short_name": "700"}}
        calendar = {}
        calendar_dates = {"service": {service_date.strftime("%Y%m%d"): "1"}}
        runs_on = Timetable.runs_on

        def stop_times_for(self, stop_id):
            return [(departure_seconds, "trip")]

    monkeypatch.setattr(main, "datetime", FrozenDatetime)
    departures = main._departures_for_stop(FixtureTimetable(), "4400B")["departures"]
    if expected_text is None:
        assert departures == []
    else:
        assert len(departures) == 1
        assert departures[0]["aimed_departure"] == expected_text


@pytest.mark.parametrize("service_date,expected_text", [
    (date(2026, 3, 29), "2026-03-28T23:00:00+00:00"),
    (date(2026, 10, 25), "2026-10-25T00:00:00+00:00"),
])
def test_service_day_epoch_uses_elapsed_hours_across_clock_changes(service_date, expected_text):
    assert main._service_day_start(service_date).astimezone(timezone.utc).isoformat() == expected_text
