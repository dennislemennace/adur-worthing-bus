"""FlixBus's own timetable, in place of the copy in the BODS bundle.

FlixBus's live feeds name journeys by FlixBus's own trip ids, which only its
own timetable carries, so the weekly build takes FlixBus's journeys from
FlixBus. Three things have to survive that, each measured on the real files:

  * Times. FlixBus writes them in UTC and means it: the 07:00 from Brighton is
    07:00 in its file in winter and 06:00 in summer, where BODS has 07:00 all
    year. Read as UK time, every summer coach would be an hour early.

  * Stops. FlixBus names its stops by its own ids. Each must become the stop
    the site already shows, and at Gatwick that is the coach bay BODS uses,
    132 m off, not the local bus stop 20 m away.

  * BODS's copy is the fallback. If FlixBus's file cannot be had, or has
    nothing here, the build keeps what BODS published rather than losing the
    coaches altogether.

Run with:  pytest
"""

import copy
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import build_timetable as bt              # noqa: E402
import check_timetable                    # noqa: E402
import flixbus_fixture as fx              # noqa: E402
import flixbus_gtfs as fg                 # noqa: E402
import json_to_sqlite as j2s              # noqa: E402
import api.main as main                   # noqa: E402
from api.main import UK_TZ                # noqa: E402
from api.timetable_db import Timetable    # noqa: E402

BRIGHTON = "9000F22A3795"
PRESTON = "9100PRSPBUS"
GATWICK_BAY = "4400CY0375"


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    mp = pytest.MonkeyPatch()
    mp.setenv("SKIP_OSRM", "1")
    d = tmp_path_factory.mktemp("flixbus")
    fx.write_bods_zip(d / "bods.zip")
    fx.write_flixbus_zip(d / "flixbus.zip")
    tt = bt.parse_gtfs(str(d / "bods.zip"))
    stats = fg.merge(tt, str(d / "flixbus.zip"), shape_box=bt.SHAPE_BOX)
    db = d / "timetable.sqlite"
    j2s.convert(copy.deepcopy(tt), db)
    mp.undo()
    return {"tt": tt, "stats": stats, "db": db}


@pytest.fixture(scope="module")
def tt_db(built):
    return Timetable(built["db"], allow_fetch=False)


def _times_at(tt, stop):
    return {trip: secs for secs, trip, *_ in tt["stop_times"].get(stop, [])}


# ── Stops ────────────────────────────────────────────────────

def test_each_flixbus_stop_becomes_the_stop_bods_uses_for_it(built):
    assert built["tt"]["stop_aliases"] == {
        fx.SRBG: BRIGHTON,
        fx.PRESTON: PRESTON,
        # Not 4400CY0388, the local bus stop 20 m closer.
        fx.GATWICK_N: GATWICK_BAY,
    }


# ── Journeys ─────────────────────────────────────────────────

def test_flixbus_journeys_replace_the_bods_copy(built):
    tt = built["tt"]
    assert not {"T_BODS_FLIX_A", "T_BODS_FLIX_B"} & tt["trips"].keys(), \
        "BODS's copy of FlixBus was kept alongside FlixBus's own"
    assert "RUK066" not in tt["routes"]
    assert fx.T_WINTER in tt["trips"]
    assert "T_LOCAL" in tt["trips"], "a local bus went with BODS's FlixBus copy"
    route = tt["routes"][tt["trips"][fx.T_WINTER]["route_id"]]
    assert route == {"short_name": "UK066", "long_name": "London - Brighton", "noc": "FLIX"}


def test_a_journey_that_calls_nowhere_here_is_left_out(built):
    assert fx.T_ELSEWHERE not in built["tt"]["trips"]
    assert built["stats"]["trips"] == 6


def test_times_are_moved_from_utc_to_uk_time(built):
    at = _times_at(built["tt"], BRIGHTON)
    assert at[fx.T_WINTER] == 7 * 3600, "winter: UTC is UK time"
    assert at[fx.T_SUMMER] == 7 * 3600, "summer: 06:00 UTC is 07:00 in Brighton"
    assert at[fx.T_INBOUND] == 7 * 3600 + 300
    # Still the service day it started on, past midnight, as GTFS writes it.
    assert at[fx.T_LATE] == 24 * 3600 + 30 * 60


def test_a_journey_either_side_of_the_clock_change_is_split(built, tt_db):
    at = _times_at(built["tt"], BRIGHTON)
    assert at[fx.T_SPANS] == 6 * 3600
    assert at[fx.T_SPANS + "@1"] == 7 * 3600
    before = tt_db.trips[fx.T_SPANS]["service_id"]
    after = tt_db.trips[fx.T_SPANS + "@1"]["service_id"]
    assert tt_db.runs_on(before, date(2027, 3, 27))
    assert not tt_db.runs_on(before, date(2027, 3, 28))
    assert tt_db.runs_on(after, date(2027, 3, 28))
    assert not tt_db.runs_on(after, date(2027, 3, 27))


def test_calendars_and_their_exceptions_carry_over(tt_db):
    service = tt_db.trips[fx.T_WINTER]["service_id"]
    assert tt_db.runs_on(service, date(2026, 12, 24))
    assert not tt_db.runs_on(service, date(2026, 12, 25)), "Christmas Day came back"
    assert not tt_db.runs_on(service, date(2027, 3, 28))


def test_the_headsign_is_where_the_coach_is_going(built):
    trips = built["tt"]["trips"]
    assert trips[fx.T_WINTER]["headsign"] == "London Victoria Coach Station"
    assert trips[fx.T_INBOUND]["headsign"] == "Brighton Railway Station - Stroudley Road"


def test_shapes_near_the_coast_are_kept_and_the_rest_are_not(built):
    tt = built["tt"]
    shape = tt["trips"][fx.T_WINTER]["shape_id"]
    assert shape == "FLIX:SH_BRIGHTON" and len(tt["shapes"][shape]) == 3
    assert tt["trips"][fx.T_GATWICK]["shape_id"] == ""
    assert "FLIX:SH_NORTH" not in tt["shapes"]


# ── Through to the database ──────────────────────────────────

def test_the_stop_aliases_reach_the_database(built, tt_db):
    assert tt_db.stop_aliases == built["tt"]["stop_aliases"]


def test_the_release_check_passes_the_merged_database(built):
    assert check_timetable.main(built["db"], allow_small=True) == 0


def test_the_board_shows_each_coach_once_in_uk_time(tt_db, monkeypatch):
    def board(when):
        class Frozen(main.datetime):
            @classmethod
            def now(cls, tz=None):
                return when if tz is None else when.astimezone(tz)
        monkeypatch.setattr(main, "datetime", Frozen)
        return [d for d in main._departures_for_stop(tt_db, BRIGHTON)["departures"]
                if d["operator"] == "FLIX"]

    winter = board(datetime(2026, 11, 3, 6, 30, tzinfo=UK_TZ))
    assert [(d["aimed_departure"], d["destination"]) for d in winter] == [
        ("2026-11-03T07:00:00+00:00", "London Victoria Coach Station"),
        ("2026-11-03T07:05:00+00:00", "Brighton Railway Station - Stroudley Road"),
    ]
    summer = board(datetime(2027, 3, 29, 6, 30, tzinfo=UK_TZ))
    # The summer coach and the second half of the split one leave together.
    assert [d["aimed_departure"] for d in summer] == ["2027-03-29T07:00:00+01:00"]


# ── When FlixBus's file is no use ────────────────────────────

def test_a_flixbus_journey_never_overwrites_another_operators(tmp_path, monkeypatch):
    # Trip ids are FlixBus's own and unprefixed, so the live feeds can name
    # them. One that happens to equal a local bus's id must not replace it:
    # the release test's stub, serving one archive for every download, did
    # exactly that, and the 700's times arrived twice, an hour apart.
    monkeypatch.setenv("SKIP_OSRM", "1")
    fx.write_bods_zip(tmp_path / "bods.zip")
    clash = [("T_LOCAL", "UK066", "S_WINTER", "", [(1, fx.SRBG, "09:00:00")])]
    fx.write_flixbus_zip(tmp_path / "flixbus.zip", trips=clash + fx.FLIX_TRIPS[:1])
    tt = bt.parse_gtfs(str(tmp_path / "bods.zip"))
    before = copy.deepcopy(tt["trips"]["T_LOCAL"])
    stats = fg.merge(tt, str(tmp_path / "flixbus.zip"), shape_box=bt.SHAPE_BOX)
    assert tt["trips"]["T_LOCAL"] == before
    assert sorted(t for _s, t, *_ in tt["stop_times"][BRIGHTON]) == [fx.T_WINTER]
    assert stats["trips"] == 1

def test_without_flixbus_journeys_here_the_bods_copy_stays(tmp_path, monkeypatch):
    monkeypatch.setenv("SKIP_OSRM", "1")
    fx.write_bods_zip(tmp_path / "bods.zip")
    far = [s for s in fx.FLIX_STOPS if s[0] in (fx.VICTORIA, fx.SHEFFIELD)]
    trips = [t for t in fx.FLIX_TRIPS if t[0] == fx.T_ELSEWHERE]
    fx.write_flixbus_zip(tmp_path / "flixbus.zip", stops=far, trips=trips)
    tt = bt.parse_gtfs(str(tmp_path / "bods.zip"))
    stats = fg.merge(tt, str(tmp_path / "flixbus.zip"), shape_box=bt.SHAPE_BOX)
    assert stats["trips"] == 0
    assert {"T_BODS_FLIX_A", "T_BODS_FLIX_B"} <= tt["trips"].keys()


def test_a_failed_download_keeps_the_bods_copy_and_says_so(tmp_path, monkeypatch):
    monkeypatch.setenv("SKIP_OSRM", "1")
    notices = tmp_path / "notices.tsv"
    monkeypatch.setenv("NOTICES", str(notices))
    fx.write_bods_zip(tmp_path / "bods.zip")
    tt = bt.parse_gtfs(str(tmp_path / "bods.zip"))

    def unreachable(url, dest):
        raise RuntimeError("GTFS download failed after 3 attempts: 503")

    bt.add_flixbus(tt, download=unreachable)
    assert {"T_BODS_FLIX_A", "T_BODS_FLIX_B"} <= tt["trips"].keys()
    key, state, message = notices.read_text().strip().split("\t")
    assert (key, state) == ("flixbus-timetable", "warn")
    assert "BODS" in message


def test_a_good_download_clears_the_notice(tmp_path, monkeypatch):
    monkeypatch.setenv("SKIP_OSRM", "1")
    notices = tmp_path / "notices.tsv"
    monkeypatch.setenv("NOTICES", str(notices))
    fx.write_bods_zip(tmp_path / "bods.zip")
    tt = bt.parse_gtfs(str(tmp_path / "bods.zip"))

    def from_fixture(url, dest):
        fx.write_flixbus_zip(dest)

    bt.add_flixbus(tt, download=from_fixture)
    assert fx.T_WINTER in tt["trips"]
    assert notices.read_text() == "flixbus-timetable\tclear\n"
