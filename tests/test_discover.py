"""Discover: the places a bus reaches without changing (api/main.py
/api/direct-from, Timetable.direct_from) and the curated places
(data/destinations.json). Asked for on 6 November 2026.

Run with:  pytest
"""

import json
import re
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import api.main as main  # noqa: E402
from api.timetable_db import Timetable  # noqa: E402

STOPS = {s["atco_code"] for s in json.loads((ROOT / "data" / "stops.json").read_text())["stops"]}


def fake_timetable():
    """Two journeys from A: one runs today, one does not; one left before now."""
    calls = {"t1": [(36000, "A"), (36600, "B"), (37200, "C")],
             "t2": [(39600, "A"), (40500, "C")],
             "t3": [(36000, "A"), (36300, "D")],          # not running today
             "t0": [(30000, "A"), (30600, "E")]}          # already gone
    tt = SimpleNamespace(
        trips={"t1": {"service_id": "WK", "route_id": "r700"}, "t2": {"service_id": "WK", "route_id": "r2"},
               "t3": {"service_id": "SUN", "route_id": "r700"}, "t0": {"service_id": "WK", "route_id": "r700"}},
        routes={"r700": {"short_name": "700"}, "r2": {"short_name": "2"}},
        stops={"A": {}, "B": {}, "C": {}, "D": {}, "E": {}},
        stop_times_for=lambda s: sorted((c[0][0], t) for t, c in calls.items() if c[0][1] == s),
        trip_stops_for=lambda t: calls[t],
        runs_on=lambda service_id, day: service_id == "WK",
        noc_for_route=lambda r: "SCSO" if r == "r700" else "BHBC",
    )
    tt.direct_from = lambda ids, day, after, until=28 * 3600: Timetable.direct_from(tt, ids, day, after, until)
    return tt


def test_every_stop_reached_without_changing_keeps_its_soonest_journey():
    reach = fake_timetable().direct_from(["A"], date(2026, 11, 6), 35000)
    assert set(reach) == {"B", "C"}, "a journey not running today, or already gone, was counted"
    assert reach["B"]["ride_secs"] == 600 and reach["B"]["service"] == "700"
    # C is reached by both journeys: the soonest is kept, and both counted.
    assert reach["C"]["next_secs"] == 36000 and reach["C"]["trips"] == 2
    assert reach["C"]["routes"] == ["2", "700"]


def test_the_endpoint_takes_one_to_six_stops_and_answers_from_the_timetable(monkeypatch):
    tt = fake_timetable()

    async def timetable():
        return tt
    monkeypatch.setattr(main, "_get_timetable", timetable)
    monkeypatch.setattr(main, "_cache", {})
    client = TestClient(main.app)
    assert client.get("/api/direct-from?stops=" + ",".join(["A"] * 7)).status_code == 400, "seven stops were taken"
    assert client.get("/api/direct-from?stops=../x").status_code == 400
    assert client.get("/api/direct-from?stops=ZZZ").status_code == 404
    r = client.get("/api/direct-from?stops=A").json()
    assert r["from"] == ["A"] and isinstance(r["reach"], dict)
    for e in r["reach"].values():
        assert {"next", "ride_mins", "service", "routes", "trips"} <= set(e)


def test_each_place_has_its_own_page_a_position_and_a_stop_to_get_off_at():
    doc = json.loads((ROOT / "data" / "destinations.json").read_text(encoding="utf-8"))
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", doc["checked_on"])
    ids = [p["id"] for p in doc["places"]]
    assert len(ids) == len(set(ids)) and len(ids) >= 12
    for p in doc["places"]:
        assert p["name"] and p["line"] and p["category"], p["id"]
        assert p["source_url"].startswith("https://"), p["id"]
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", p["checked_on"]), p["id"]
        assert 50.7 < p["lat"] < 51.0 and -0.6 < p["lon"] < 0.1, p["id"]
        assert p["stops"], p["id"]
        for s in p["stops"]:
            # A stop off the site's map (Devil's Dyke's terminus) or a walk
            # given by hand (Highdown's 20 minutes) was chosen by hand.
            by_hand = bool(p.get("stops_by_hand"))
            assert s["atco"] in STOPS or (by_hand and s.get("outside_map")), (p["id"], s)
            assert s["metres"] <= (2500 if by_hand else 1500), (p["id"], s)
            assert s.get("walk_mins") is None or 1 <= s["walk_mins"] <= 40, (p["id"], s)
        assert p.get("wheelchair") in (None, "yes", "limited", "no"), p["id"]
        for field in ("line", "about"):
            assert p[field] and "—" not in p[field], (p["id"], field)


FREE = re.compile(r"^(public domain|cc0|cc by(-sa)? \d\.\d)$", re.I)


def test_each_place_has_a_free_photo_credited_on_the_about_page():
    """Asked for 7 November 2026: a photo of each place, free to use with
    credit, credited under the photo and on the About page."""
    doc = json.loads((ROOT / "data" / "destinations.json").read_text(encoding="utf-8"))
    about = (ROOT / "about.html").read_text(encoding="utf-8")
    for p in doc["places"]:
        ph = p.get("photo") or {}
        assert ph, f"{p['id']} has no photo"
        for key in ("src", "thumb"):
            assert ph[key].startswith("images/discover/") and (ROOT / ph[key]).is_file(), (p["id"], key)
        assert FREE.match(ph["licence"]), (p["id"], ph["licence"])
        assert ph["author"] and ph["alt"], p["id"]
        assert ph["source_url"].startswith("https://commons.wikimedia.org/wiki/File:"), p["id"]
        assert ph["licence_url"] or ph["licence"].lower() in ("public domain", "cc0"), p["id"]
        assert ph["source_url"] in about, f"{p['id']}'s photo is not credited on about.html"
    named = {ph.stem.removesuffix("-thumb") for ph in (ROOT / "images" / "discover").glob("*.jpg")}
    assert named == {p["id"] for p in doc["places"]}, "a photo in images/discover/ belongs to no place"
