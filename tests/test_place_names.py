"""Names shown in place of the published ones (data/place_names.json).

The coach stop behind Brighton station is one place to a passenger under two
operators' names, and a coach's destination reads as the place it is going,
with its exact stop beneath. Asked for on 6 November 2026.

Run with:  pytest
"""

import json
import re
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import api.main as main  # noqa: E402
from api import place_names  # noqa: E402

DOC = json.loads((ROOT / "data" / "place_names.json").read_text(encoding="utf-8"))
DB = ROOT / "data" / "timetable.sqlite"
STROUDLEY = "Brighton Station (Stroudley Road car park)"
# What a destination still needing a name looks like: a district before its
# town, a bay or stand, a bus station's full title, or FlixBus's " - ".
UNPLAIN = re.compile(r",|\(|\)| - |\bBay\b|\bStop\b|Bus Station|Coach Park|Train Station", re.I)


def test_the_file_says_what_each_name_replaces_and_why():
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", DOC["checked_on"])
    for atco, entry in DOC["stops"].items():
        assert entry["name"] and entry["published"] and entry["why"], atco
        assert entry["name"] != entry["published"], atco
    for raw, entry in DOC["coach_destinations"].items():
        assert entry["place"] and isinstance(entry.get("stop", ""), str), raw
        assert not UNPLAIN.search(entry["place"]), f"{raw}: the place still reads as a stop"
    text = (ROOT / "data" / "place_names.json").read_text(encoding="utf-8")
    assert "—" not in text


def test_the_stroudley_road_stops_are_one_place():
    assert place_names.stop_name("900059056", "x") == STROUDLEY
    assert place_names.stop_name("9000F22A3795", "x") == STROUDLEY
    assert place_names.stop_name("149000007936", "St Peter's Church") == "St Peter's Church"
    stops = {s["atco_code"]: s for s in json.loads((ROOT / "data" / "stops.json").read_text())["stops"]}
    for atco in ("900059056", "9000F22A3795"):
        assert stops[atco]["name"] == STROUDLEY, "the map and search still use the published name"
        assert "towards" not in stops[atco], "a name shared on purpose was given a direction"


def test_a_coach_destination_reads_as_its_place():
    assert place_names.coach_destination("Swansea City Bus Station (Bay B and C)", "FLIX") == \
        ("Swansea", "City Bus Station, bays B and C")
    # National Express writes the journey; its far end is the destination.
    assert place_names.coach_destination("Brighton - Belgravia, Victoria", "NATX") == \
        ("London Victoria", "Victoria Coach Station")
    assert place_names.coach_destination("Belgravia, Victoria - Brighton", "NATX") == ("Brighton", "")
    # FlixBus's own names have hyphens; an unlisted one is not split.
    assert place_names.coach_destination("Leeds - Somewhere New", "FLIX") == ("Leeds - Somewhere New", "")
    # A bus is shown as published.
    assert place_names.coach_destination("Portslade Academy", "BHBC") == ("Portslade Academy", "")


def test_a_board_at_the_renamed_stop_still_finds_the_far_end():
    # Headsigns are written with the published name, so that is what the
    # near end is matched against, not the name the board shows.
    parts = main._destination_parts("Brighton Rail Station Stroudley Road opposite - Victoria, London",
                                    "Brighton Rail Station Stroudley Road opposite", "", "NATX")
    assert parts == ("London Victoria", "Victoria Coach Station")


@pytest.mark.skipif(not DB.exists(), reason="no local timetable")
def test_every_coach_destination_in_the_timetable_has_a_name():
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    rows = con.execute("SELECT DISTINCT r.noc, t.headsign FROM trips t JOIN routes r ON r.rid = t.rid "
                       "WHERE r.noc IN ('NATX', 'FLIX')").fetchall()
    missing = sorted({raw for noc, raw in rows
                      if UNPLAIN.search(place_names.coach_destination(raw, noc)[0])})
    assert not missing, f"coach destinations with no name in data/place_names.json: {missing}"
