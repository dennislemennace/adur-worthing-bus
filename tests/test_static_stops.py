"""Tests for the stop list the map is drawn from.

The list used to come only from `/api/stops`, on a free instance that sleeps
after fifteen minutes; the first visitor after a quiet spell watched an empty
map for twenty seconds while a container started, for data that changes once a
week. It is now published as a static file too.

That makes one question worth testing hard: **the two must not drift.** They
share `Timetable.stop_list()` for exactly that reason, so these tests are about
what that method promises — which stops are in, which are out, and which fields
the frontend is entitled to find.

Run with:  pytest
"""

import csv
import io
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import build_timetable as bt        # noqa: E402
import json_to_sqlite as j2s        # noqa: E402
from api.timetable_db import Timetable   # noqa: E402


# ── A network small enough to reason about ──────────────────
#
# Two poles a street apart share the name "Colebrook Road" — the real case
# that motivated the direction fields. "Marine Parade" is unique. "Far Field"
# sits outside the narrow bbox used below, and "Disused Halt" has a platform
# and a name but no bus has called there since the feed was written.

STOPS = [
    ("4400A", "Colebrook Road", 50.8315, -0.2327),
    ("4400B", "Colebrook Road", 50.8313, -0.2316),
    ("4400C", "Marine Parade",  50.8095, -0.3730),
    ("4400D", "Disused Halt",   50.8100, -0.3700),
    ("4400Z", "Far Field",      50.8600, -0.1100),
]

# 700 eastbound calls at pole A, westbound at pole B: the headsigns are what
# tells them apart.
STOP_TIMES = [
    ("T_EAST",  1, "4400A", "09:00:00"),
    ("T_EAST",  2, "4400C", "09:10:00"),
    ("T_WEST",  1, "4400B", "10:00:00"),
    ("T_WEST",  2, "4400C", "10:10:00"),
    # The night service, and the only thing that calls at the far stop.
    ("T_NIGHT", 1, "4400A", "23:50:00"),
    ("T_NIGHT", 2, "4400Z", "24:05:00"),
]


def _csv(rows, header):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue()


@pytest.fixture(scope="module")
def gtfs_zip(tmp_path_factory):
    path = tmp_path_factory.mktemp("gtfs") / "feed.zip"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("agency.txt", _csv(
            [("AG1", "Test Operator", "TSTO")],
            ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(
            STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv(
            [("R1", "AG1", "700", "Coast", 3),
             ("R2", "AG1", "N7",  "Night", 3)],
            ["route_id", "agency_id", "route_short_name", "route_long_name",
             "route_type"]))
        z.writestr("trips.txt", _csv(
            [("T_EAST",  "R1", "SVC1", "Old Steine", ""),
             ("T_WEST",  "R1", "SVC1", "Durrington Tesco", ""),
             ("T_NIGHT", "R2", "SVC1", "Old Steine", "")],
            ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", _csv(
            [(t, seq, sid, hms, hms) for (t, seq, sid, hms) in STOP_TIMES],
            ["trip_id", "stop_sequence", "stop_id", "arrival_time",
             "departure_time"]))
        z.writestr("calendar.txt", _csv(
            [("SVC1", 1, 1, 1, 1, 1, 1, 1, "20260101", "20271231")],
            ["service_id", "monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday", "start_date", "end_date"]))
    return path


@pytest.fixture(scope="module")
def db_path(gtfs_zip, tmp_path_factory, monkeypatch_module):
    monkeypatch_module.setenv("SKIP_OSRM", "1")
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(bt.parse_gtfs(str(gtfs_zip)), out)
    return out


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture(scope="module")
def tt(db_path):
    return Timetable(db_path, allow_fetch=False)


# Everything except the far stop.
WIDE   = (50.78, 50.87, -0.42, -0.10)
NARROW = (50.78, 50.84, -0.42, -0.20)


def by_id(stops):
    return {s["atco_code"]: s for s in stops}


# ── A build script must not redownload what it is publishing ─

def test_a_build_does_not_refresh_the_database_it_is_about_to_publish(db_path):
    """`allow_fetch=False` exists for one reason.

    The API refreshes its copy when the local hash differs from the published
    sidecar. A build script runs seconds after json_to_sqlite.py has written a
    *newer* database, whose hash necessarily differs — so the same check would
    read "newer" as "stale" and replace the new file with last week's, and the
    static stop list would silently describe the previous timetable.
    """
    calls = []
    original = Timetable._ensure_fresh
    Timetable._ensure_fresh = lambda self: calls.append(1)
    try:
        offline = Timetable(db_path, allow_fetch=False)
        assert calls == [], "the build path reached the refresh check"
        assert offline.ok(), "opening without a refresh did not open the DB"

        Timetable(db_path)     # the API's default, unchanged
        assert calls == [1], "the API no longer checks whether its copy is stale"
    finally:
        Timetable._ensure_fresh = original


# ── Which stops are in the list ─────────────────────────────

def test_a_stop_no_bus_calls_at_is_not_on_the_map(tt):
    ids = by_id(tt.stop_list(WIDE))
    assert "4400C" in ids, "a stop with departures is missing"
    assert "4400D" not in ids, \
        "a stop with no scheduled departure is on the map; tapping it can " \
        "only ever say there is nothing"


def test_the_bbox_is_applied(tt):
    assert "4400Z" in by_id(tt.stop_list(WIDE))
    assert "4400Z" not in by_id(tt.stop_list(NARROW)), \
        "a stop outside the bounding box was returned"


def test_night_services_are_marked(tt):
    ids = by_id(tt.stop_list(WIDE))
    assert ids["4400A"]["night_serving"] is True, "the N7 stop is not marked"
    assert ids["4400C"]["night_serving"] is False, \
        "a stop with no night service is marked as having one"


# ── The fields the frontend reads ───────────────────────────

def test_every_stop_carries_what_the_map_needs(tt):
    """renderStopMarker() and the search index read these by name.

    Renaming one here would leave markers at `undefined, undefined`, which
    Leaflet reports as a coordinate error a long way from the cause.
    """
    required = {"atco_code", "name", "latitude", "longitude", "night_serving"}
    for s in tt.stop_list(WIDE):
        assert required <= set(s), f"{s.get('atco_code')} is missing {required - set(s)}"
        assert isinstance(s["latitude"], float)
        assert isinstance(s["longitude"], float)


def test_two_poles_of_one_name_can_be_told_apart(tt):
    ids = by_id(tt.stop_list(WIDE))
    a, b = ids["4400A"], ids["4400B"]
    assert a.get("towards") and b.get("towards"), \
        "a shared name with no direction: the search cannot tell the poles apart"
    assert a["towards"] != b["towards"], \
        f"both poles say they go to {a['towards']}"
    assert "700" in a.get("services", []), "the services calling there are missing"


def test_a_unique_name_carries_no_direction(tt):
    """Not a detail: it is 1,520 stops' worth of bytes on every first visit.

    Marine Parade is one pole under one name. There is nothing to
    disambiguate, and a `towards` on it would be padding.
    """
    marine = by_id(tt.stop_list(WIDE))["4400C"]
    assert "towards" not in marine and "services" not in marine, \
        "direction fields were added to a stop whose name is already unique"


# ── The build script ────────────────────────────────────────

def test_the_build_refuses_to_publish_a_near_empty_list(db_path, tmp_path):
    """The fixture network has five stops; the real bbox has about 1,520.

    Publishing a short file is worse than publishing none: the frontend's
    fallback fires when the fetch *fails*, so a successful download of a
    nearly-empty list gives every visitor a blank map and never asks the API.
    """
    out = tmp_path / "stops.json"
    r = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build_stops_json.py"),
         "--db", str(db_path), "--out", str(out)],
        capture_output=True, text=True)
    assert r.returncode != 0, "a five-stop list was published as the whole network"
    assert not out.exists(), "a file was written despite the failure"
    assert "refusing to publish" in r.stderr


# ── "Does this journey work at all?" ────────────────────────

def test_the_weekday_probe_skips_the_weekend():
    """A journey that fails today may simply be a Saturday.

    Shoreham to the universities is the 700 and then the 5B Monday to Friday
    and nothing at all at the weekend. The preset drew a blank map on a
    Saturday, which reads as a broken tool rather than as the finding it is.
    """
    from api.main import _next_weekday
    from datetime import date

    # Friday stays put — the probe must not wander to next week and answer a
    # question about a different timetable.
    assert _next_weekday(date(2026, 9, 11)) == date(2026, 9, 11)
    assert _next_weekday(date(2026, 9, 14)) == date(2026, 9, 14)   # Monday
    # Saturday and Sunday both land on the following Monday.
    assert _next_weekday(date(2026, 9, 12)) == date(2026, 9, 14)
    assert _next_weekday(date(2026, 9, 13)) == date(2026, 9, 14)


def test_the_weekday_probe_always_lands_on_a_weekday():
    from api.main import _next_weekday
    from datetime import date, timedelta

    day = date(2026, 1, 1)
    for _ in range(400):
        probe = _next_weekday(day)
        assert probe.weekday() < 5, f"{day} probed to {probe}, a weekend"
        assert probe >= day, "the probe went backwards"
        assert (probe - day).days <= 2, "a weekday is never more than two days off"
        day += timedelta(days=1)
