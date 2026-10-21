"""A wait the GTFS feed counts twice is taken out once.

From the 18 October 2026 BODS feed, every Stagecoach journey with a timed wait
carried that wait twice: at the timing point, and again at the stop before it.
The 12:06 700 from Durrington waited five minutes at The Lido (12:29-12:34) and
five more at Worthing Marine Parade (12:36-12:41), so every stop east of
Worthing was timetabled five minutes later than the buses run. The operator's
own TransXChange, as bustimes.org reads it, has one wait (Marine Parade
12:31-12:36); so do TransportAPI and the buses' own positions. The board called
every eastbound 700 at Shoreham Port five minutes early, and the nightly
measurement would have too. Compass's feed has the same fault.

Coaches really do wait at consecutive stops: the 025 stands five minutes at
Heathrow and five more at Gatwick, fifty minutes apart. Those must survive.

Run with:  pytest tests/test_duplicate_waits.py
"""

import csv
import io
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import build_timetable as bt      # noqa: E402

STOPS = [
    ("4400TESCO", "Tesco", 50.83, -0.40),
    ("4400LIDO", "The Lido", 50.81, -0.37),
    ("4400MARINE", "Marine Parade", 50.81, -0.369),
    ("4400PORT", "Shoreham Port", 50.83, -0.236),
    ("4400SOUTHST", "South Street", 50.812, -0.371),
    ("4400HOSP", "Hospital", 50.815, -0.36),
    # Outside the map box and outside West Sussex: dropped by the ingest,
    # but a wait counted twice there still delays every stop after it.
    ("2400FAR", "Far Away", 51.30, 0.50),
    ("4400NORTH", "North Terminal", 50.86, -0.16),
    ("4400SOUTH", "South Terminal", 50.85, -0.15),
    ("4400BRIGHTON", "Brighton", 50.82, -0.14),
]

# (trip, seq, stop, arrival, departure, timepoint)
STOP_TIMES = [
    # Stagecoach 700 as fed: the Marine Parade wait repeated at The Lido.
    ("T700", 0, "4400TESCO", "12:06:00", "12:06:00", 1),
    ("T700", 1, "4400LIDO", "12:29:00", "12:34:00", 0),
    ("T700", 2, "4400MARINE", "12:36:00", "12:41:00", 1),
    ("T700", 3, "4400PORT", "13:11:00", "13:11:00", 0),
    # Compass 16 as fed: both stops are timing points, a minute apart.
    ("T16", 0, "4400SOUTHST", "12:04:00", "12:07:00", 1),
    ("T16", 1, "4400MARINE", "12:08:00", "12:11:00", 1),
    ("T16", 2, "4400HOSP", "12:15:00", "12:15:00", 1),
    # The duplicate falls outside the area kept; the stops after it do not.
    ("TFAR", 0, "2400FAR", "09:00:00", "09:05:00", 0),
    ("TFAR", 1, "4400MARINE", "09:07:00", "09:12:00", 1),
    ("TFAR", 2, "4400PORT", "09:40:00", "09:40:00", 0),
    # A coach that genuinely stands at two timing points fifty minutes apart.
    ("TCOACH", 0, "4400NORTH", "08:30:00", "08:35:00", 1),
    ("TCOACH", 1, "4400SOUTH", "09:25:00", "09:30:00", 1),
    ("TCOACH", 2, "4400BRIGHTON", "10:10:00", "10:10:00", 1),
    # One wait, said once: nothing to take out.
    ("TONE", 0, "4400LIDO", "10:00:00", "10:00:00", 0),
    ("TONE", 1, "4400MARINE", "10:02:00", "10:07:00", 1),
    ("TONE", 2, "4400PORT", "10:37:00", "10:37:00", 0),
    # Two waits of different lengths are two waits.
    ("TTWO", 0, "4400LIDO", "11:00:00", "11:02:00", 0),
    ("TTWO", 1, "4400MARINE", "11:04:00", "11:09:00", 1),
    ("TTWO", 2, "4400PORT", "11:39:00", "11:39:00", 0),
]


def _csv(rows, header):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue()


@pytest.fixture(scope="module")
def timetable(tmp_path_factory):
    path = tmp_path_factory.mktemp("gtfs") / "feed.zip"
    trips = sorted({r[0] for r in STOP_TIMES})
    # Written in reverse, as a feed is entitled to: the correction may not
    # rely on a trip's rows arriving together or in order.
    rows = [(t, seq, sid, arr, dep, tp) for (t, seq, sid, arr, dep, tp) in reversed(STOP_TIMES)]
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("agency.txt", _csv([("AG1", "Test Operator", "TSTO")],
                                      ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv([("R1", "AG1", "700", "Coast", 3)],
                                      ["route_id", "agency_id", "route_short_name",
                                       "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv([(t, "R1", "SVC1", "Somewhere", "") for t in trips],
                                     ["trip_id", "route_id", "service_id", "trip_headsign",
                                      "shape_id"]))
        z.writestr("stop_times.txt", _csv(rows, ["trip_id", "stop_sequence", "stop_id",
                                                 "arrival_time", "departure_time",
                                                 "timepoint"]))
        z.writestr("calendar.txt", _csv(
            [("SVC1", 1, 1, 1, 1, 1, 1, 1, "20260101", "20271231")],
            ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday",
             "saturday", "sunday", "start_date", "end_date"]))
    mp = pytest.MonkeyPatch()
    mp.setenv("SKIP_OSRM", "1")
    try:
        yield bt.parse_gtfs(str(path))
    finally:
        mp.undo()


def _times(tt, trip):
    """{stop_id: HH:MM} for one trip, as the timetable will publish it."""
    out = {}
    for stop_id, entries in tt["stop_times"].items():
        for entry in entries:
            dep, trip_id, _seq, _tp = bt.read_stop_time(entry)
            if trip_id == trip:
                out[stop_id] = f"{dep // 3600:02d}:{dep % 3600 // 60:02d}"
    return out


def test_stagecoach_wait_repeated_at_the_stop_before_is_counted_once(timetable):
    assert _times(timetable, "T700") == {
        "4400TESCO": "12:06", "4400LIDO": "12:29", "4400MARINE": "12:36", "4400PORT": "13:06"}


def test_a_repeated_wait_between_two_timing_points_a_minute_apart_is_counted_once(timetable):
    # bustimes.org: South Street 12:04, Marine Parade 12:05 (leaving 12:08), Hospital 12:12.
    assert _times(timetable, "T16") == {
        "4400SOUTHST": "12:04", "4400MARINE": "12:08", "4400HOSP": "12:12"}


def test_a_repeated_wait_outside_the_area_still_moves_the_stops_inside_it(timetable):
    assert "2400FAR" not in timetable["stops"]
    assert _times(timetable, "TFAR") == {"4400MARINE": "09:07", "4400PORT": "09:35"}


def test_genuine_waits_at_two_distant_timing_points_are_kept(timetable):
    assert _times(timetable, "TCOACH") == {
        "4400NORTH": "08:35", "4400SOUTH": "09:30", "4400BRIGHTON": "10:10"}


def test_a_single_wait_is_left_alone(timetable):
    assert _times(timetable, "TONE") == {
        "4400LIDO": "10:00", "4400MARINE": "10:07", "4400PORT": "10:37"}


def test_waits_of_different_lengths_are_both_kept(timetable):
    assert _times(timetable, "TTWO") == {
        "4400LIDO": "11:02", "4400MARINE": "11:09", "4400PORT": "11:39"}


def test_each_stop_stays_in_departure_order(timetable):
    for entries in timetable["stop_times"].values():
        deps = [bt.read_stop_time(e)[0] for e in entries]
        assert deps == sorted(deps)
