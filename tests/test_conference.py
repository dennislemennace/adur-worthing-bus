"""data/conference.json: the conference travel guide's facts carry sources, its
stops exist, and its walking times follow from its coordinates."""

import json
import math
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOC = json.loads((ROOT / "data" / "conference.json").read_text())
STOPS = {s["atco_code"] for s in json.loads((ROOT / "data" / "stops.json").read_text())["stops"]}


def walk(lat, lon, origin):
    d = math.hypot((lat - origin[0]) * 111_195,
                   (lon - origin[1]) * 111_195 * math.cos(math.radians(origin[0])))
    return max(1, round(d * 1.3 / 80))


PLACES = ([DOC["station"]] + DOC["coaches"] + DOC["social_venues"] + DOC["taxi_ranks"]
          + DOC["hotel_areas"] + DOC["driving"]["car_parks"])
LOCATOR = json.loads((ROOT / "data" / "conference_stops.json").read_text())["stops"]


def test_dates_and_sources():
    date.fromisoformat(DOC["checked_on"])
    date.fromisoformat(DOC["show_until"])
    urls = [DOC["conference"]["url"], DOC["venue"]["source_url"], DOC["venue"]["access_url"],
            DOC["socials_url"], DOC["tickets_url"], DOC["driving"]["source_url"],
            DOC["station"]["source_url"], DOC["taxi_source"]] + DOC["coach_sources"]
    for url in urls:
        assert url.startswith("https://"), url
    for item in DOC["social_venues"]:
        assert item["source_url"].startswith("https://"), item["id"]


def test_every_stop_named_is_a_real_stop():
    named = [s["atco"] for s in DOC["nearest_stops"]] + [s["atco"] for s in LOCATOR]
    assert named and all(a in STOPS for a in named), [a for a in named if a not in STOPS]


def test_locator_stops_have_routes_and_places():
    assert len(LOCATOR) > 100
    for s in LOCATOR:
        assert s["routes"] and 50.7 < s["lat"] < 51.2 and -1.0 < s["lon"] < 0.4, s["atco"]


def test_walking_times_are_worked_out_not_guessed():
    origin = DOC["venue"]["walk_point"]
    for item in PLACES:
        assert item["walk_minutes"] == walk(item["lat"], item["lon"], origin), item.get("id", item["name"])


def test_hotel_quickest_way_follows_walking_time():
    for h in DOC["hotel_areas"]:
        assert h["quickest"] == ("walk" if h["walk_minutes"] <= 15 else "bus"), h["id"]
        assert h["quickest"] == "walk" or h["buses"], h["id"]


def test_places_are_in_brighton():
    for item in [DOC["venue"]] + PLACES:
        assert 50.80 < item["lat"] < 50.87 and -0.20 < item["lon"] < -0.09, item.get("id", item.get("name"))
