"""Coaches in the timetable: FlixBus and National Express.

Coaches are not local buses and the site is careful to keep them apart — they
are excluded from punctuality matching, from the gap monitor and from the fare
recommendation. But their timetables are still worth carrying, because a coach
calling in Brighton is a real departure a reader can take.

Getting that wrong has three specific shapes, all of them measured in the BODS
South East feed rather than imagined:

  * FlixBus's Brighton stops are coded 9000/9100 (rail and coach codes), not
    4400 or 1490, so the ingest's "keep every operator at a West Sussex stop"
    rule never sees them. Before UK066 and UK998 were named in EXTRA_ROUTES the
    feed's 969 FlixBus trips reached our data with a single Gatwick call each.

  * FlixBus publishes every journey six to eight times under different trip
    and service ids. On 2026-09-22 its two Brighton routes ran 160 trips that
    are 22 distinct timed journeys; Brighton Station's 64 trips are 9 real
    departures. A board keyed on trip id shows one coach nine times and fills
    its fifteen rows with copies.

  * Coach operators write a headsign as "origin - destination" where local
    operators write a destination, so the board at a coach's own origin
    announced it as the destination.

Run with:  pytest
"""

import csv
import io
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import build_timetable as bt              # noqa: E402
import json_to_sqlite as j2s              # noqa: E402
import api.main as main                   # noqa: E402
from api.main import UK_TZ                # noqa: E402
from api.timetable_db import Timetable    # noqa: E402
from api.trip_match import COACH_NOCS     # noqa: E402

# The two real FlixBus routes that call in Sussex, and one that does not.
BRIGHTON_COACH = "UK066"
AIRPORT_COACH  = "UK998"
ELSEWHERE_COACH = "UK940"      # runs nowhere near here; deliberately unnamed

# Real codes and coordinates, so the fixture exercises the same filter paths
# the weekly build does. Gatwick is 4400 but far outside the bbox; the two
# Brighton stops are inside the bbox but carry neither prefix the ingest
# keeps unconditionally.
STOPS = [
    ("4400SH01",     "Shoreham High Street",  50.8320, -0.2750),
    ("9000F22A3795", "Brighton Railway Station - Stroudley Road",
                                              50.8322, -0.1414),
    ("9100PRSPBUS",  "Preston Park - London Road",
                                              50.8459, -0.1515),
    ("4400CY0365",   "South Terminal Coach Station",
                                              51.1537, -0.1821),
]

# (trip, route, headsign, [(seq, stop, time), ...])
#
# The three T_FLIX_DUP trips are the artefact: one coach, published three
# times. T_FLIX_OTHER leaves in the same minute for somewhere else, which is a
# second real departure and must survive. T_FLIX_UNNAMED is the same shape on
# a route this ingest does not name.
TRIPS = [
    ("T_LOCAL",         "R700",   "Brighton Station B3",
     [(1, "4400SH01", "08:00:00"), (2, "9000F22A3795", "08:45:00")]),
    ("T_FLIX_DUP_A",    "RUK066", "Brighton Railway Station - Stroudley Road - Parkside",
     [(1, "9000F22A3795", "08:15:00"), (2, "9100PRSPBUS", "08:20:00"),
      (3, "4400CY0365", "09:10:00")]),
    ("T_FLIX_DUP_B",    "RUK066", "Brighton Railway Station - Stroudley Road - Parkside",
     [(1, "9000F22A3795", "08:15:00"), (2, "9100PRSPBUS", "08:20:00"),
      (3, "4400CY0365", "09:10:00")]),
    ("T_FLIX_DUP_C",    "RUK066", "Brighton Railway Station - Stroudley Road - Parkside",
     [(1, "9000F22A3795", "08:15:00"), (2, "9100PRSPBUS", "08:20:00"),
      (3, "4400CY0365", "09:10:00")]),
    ("T_FLIX_OTHER",    "RUK998", "Brighton Railway Station - Stroudley Road - Central Railway Station",
     [(1, "9000F22A3795", "08:15:00"), (2, "9100PRSPBUS", "08:20:00"),
      (3, "4400CY0365", "09:05:00")]),
    ("T_FLIX_UNNAMED",  "RUK940", "Brighton Railway Station - Stroudley Road - Somewhere",
     [(1, "9000F22A3795", "08:30:00"), (2, "4400CY0365", "09:20:00")]),
]

ROUTES = [
    ("R700",   "AG_SC",   "700",           "Coastliner"),
    ("RUK066", "AG_FLIX", BRIGHTON_COACH,  ""),
    ("RUK998", "AG_FLIX", AIRPORT_COACH,   ""),
    ("RUK940", "AG_FLIX", ELSEWHERE_COACH, ""),
]

TUESDAY = datetime(2026, 9, 22, 8, 0, tzinfo=UK_TZ)


def _csv(rows, header):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue()


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture(scope="module")
def tt(tmp_path_factory, monkeypatch_module):
    """A timetable built by the real ingest, against the real EXTRA_ROUTES.

    Nothing here is patched: the point of the fixture is that UK066 and UK998
    are on that list and UK940 is not, so removing one would fail these tests
    rather than quietly emptying a departure board.
    """
    monkeypatch_module.setenv("SKIP_OSRM", "1")
    feed = tmp_path_factory.mktemp("gtfs") / "feed.zip"
    with zipfile.ZipFile(feed, "w") as z:
        z.writestr("agency.txt", _csv(
            [("AG_SC", "Stagecoach South", "SCSO"),
             ("AG_FLIX", "FlixBus", "FLIX")],
            ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", _csv(
            STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", _csv(
            [(r, a, s, l, 3) for r, a, s, l in ROUTES],
            ["route_id", "agency_id", "route_short_name",
             "route_long_name", "route_type"]))
        z.writestr("trips.txt", _csv(
            [(t, r, "ALL", h, "") for t, r, h, _ in TRIPS],
            ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", _csv(
            [(t, seq, sid, hms, hms)
             for t, _, _, calls in TRIPS for seq, sid, hms in calls],
            ["trip_id", "stop_sequence", "stop_id",
             "arrival_time", "departure_time"]))
        z.writestr("calendar.txt", _csv(
            [("ALL", 1, 1, 1, 1, 1, 1, 1, "20260101", "20271231")],
            ["service_id", "monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday", "start_date", "end_date"]))
    out = tmp_path_factory.mktemp("db") / "timetable.sqlite"
    j2s.convert(bt.parse_gtfs(str(feed)), out)
    return Timetable(out, allow_fetch=False)


def _board(tt, stop_id, monkeypatch, when=TUESDAY):
    """The departure board for a stop, with the clock frozen."""
    class FrozenDatetime(main.datetime):
        @classmethod
        def now(cls, tz=None):
            return when if tz is None else when.astimezone(tz)

    monkeypatch.setattr(main, "datetime", FrozenDatetime)
    return main._departures_for_stop(tt, stop_id)


def _services_at(tt, stop_id):
    return {tt.routes[tt.trips[t]["route_id"]]["short_name"]
            for _, t in tt.stop_times_for(stop_id)}


# ── Ingest ───────────────────────────────────────────────────

def test_a_coach_calling_in_brighton_is_kept(tt):
    # 9000/9100 are rail and coach codes. Neither is the West Sussex prefix
    # that keeps a stop for every operator, so these calls survive only
    # because UK066 and UK998 are named in EXTRA_ROUTES.
    assert "9000F22A3795" in tt.stops, \
        "Brighton Station lost — is UK066 still in build_timetable.EXTRA_ROUTES?"
    assert "9100PRSPBUS" in tt.stops, "Preston Park lost from the ingest"
    services = _services_at(tt, "9000F22A3795")
    assert {BRIGHTON_COACH, AIRPORT_COACH} <= services, services


def test_a_coach_that_does_not_come_here_is_not_kept_for_its_brighton_call(tt):
    # The mechanism, stated as a negative: an unnamed route's bbox-only calls
    # are dropped. Without this the previous test would pass for the wrong
    # reason — every coach, rather than the two that were chosen.
    assert ELSEWHERE_COACH not in _services_at(tt, "9000F22A3795"), \
        "an unnamed route kept a stop the West Sussex rule does not cover"


def test_gatwick_keeps_every_coach_because_it_is_a_west_sussex_stop(tt):
    # 4400CY is West Sussex however far outside the bbox it sits, so the
    # unnamed route is still here — with only this one call, which is what
    # the whole FlixBus feed looked like before Brighton was added.
    assert ELSEWHERE_COACH in _services_at(tt, "4400CY0365")


# ── One journey, published many times ────────────────────────

def test_one_coach_published_three_times_is_one_departure(tt, monkeypatch):
    board = _board(tt, "9000F22A3795", monkeypatch)["departures"]
    at_0815 = [d for d in board
               if d["aimed_departure"].startswith("2026-09-22T08:15")]
    assert len(at_0815) == 2, \
        f"expected the duplicated coach once and the other coach once: {at_0815}"
    assert sorted(d["service"] for d in at_0815) == [BRIGHTON_COACH,
                                                     AIRPORT_COACH]


def test_two_real_departures_in_one_minute_both_survive(tt, monkeypatch):
    # The same minute, the same operator, different journeys. A dedup keyed on
    # the time alone would lose one of these.
    board = _board(tt, "9000F22A3795", monkeypatch)["departures"]
    dests = {d["destination"] for d in board
             if d["aimed_departure"].startswith("2026-09-22T08:15")}
    assert dests == {"Parkside", "Central Railway Station"}, dests


# ── "origin - destination" headsigns ─────────────────────────

def test_the_board_does_not_send_a_coach_to_the_stop_it_leaves_from(tt,
                                                                    monkeypatch):
    board = _board(tt, "9000F22A3795", monkeypatch)["departures"]
    flix = next(d for d in board if d["service"] == BRIGHTON_COACH)
    assert flix["destination"] == "Parkside", flix


def test_the_board_further_along_the_route_names_the_far_end(tt, monkeypatch):
    # Preston Park is the coach's second call, so its own name is not the
    # prefix — the trip's origin is, and that is the half to drop.
    board = _board(tt, "9100PRSPBUS", monkeypatch)["departures"]
    flix = next(d for d in board if d["service"] == BRIGHTON_COACH)
    assert flix["destination"] == "Parkside", flix


def test_a_hyphen_alone_does_not_make_a_destination_two_halves():
    # FlixBus stop names contain hyphens, so splitting on the separator would
    # answer "Stroudley Road" for a coach going to Brighton. With no name
    # proven to be the near end, the feed's own wording stands.
    keep = "Parkside - Brighton Railway Station - Stroudley Road"
    assert main._destination_of(keep, "Preston Park - London Road") == keep


def test_a_local_destination_is_left_exactly_as_the_operator_wrote_it():
    assert main._destination_of("Portslade Academy", "Shoreham High Street") \
        == "Portslade Academy"
    assert main._destination_of("", "Anywhere") == "Unknown"


# ── Coaches stay out of the local-service views ──────────────

def test_the_flixbus_line_codes_are_not_offered_as_local_services(tt):
    # UK066 is an internal line code, not a number on the front of a coach.
    services = {p["service"] for p in tt.representative_polylines()}
    assert "700" in services, "the local route vanished from the route filter"
    assert not ({BRIGHTON_COACH, AIRPORT_COACH} & services), services


def test_flixbus_is_treated_as_a_coach_everywhere_else():
    # Punctuality matching and the gap monitor both key off this set, so a
    # coach can never enter a published reliability figure.
    assert "FLIX" in COACH_NOCS
