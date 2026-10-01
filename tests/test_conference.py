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


def test_dates_and_sources():
    date.fromisoformat(DOC["checked_on"])
    date.fromisoformat(DOC["show_until"])
    for url in (DOC["conference"]["url"], DOC["venue"]["source_url"], DOC["venue"]["access_url"],
                DOC["hotels_url"], DOC["socials_url"], DOC["tickets_url"], DOC["driving"]["source_url"]):
        assert url.startswith("https://"), url
    for item in DOC["arrivals"] + DOC["socials"]:
        assert item["source_url"].startswith("https://"), item["id"]


def test_every_stop_named_is_a_real_stop():
    named = [s["atco"] for s in DOC["nearest_stops"]] + [a["atco"] for a in DOC["arrivals"] if a.get("atco")]
    assert named and all(a in STOPS for a in named), [a for a in named if a not in STOPS]


def test_walking_times_are_worked_out_not_guessed():
    origin = DOC["venue"]["walk_point"]
    for item in DOC["arrivals"] + DOC["socials"] + DOC["driving"]["car_parks"]:
        assert item["walk_minutes"] == walk(item["lat"], item["lon"], origin), item["id"]


def test_places_are_in_brighton():
    for item in [DOC["venue"]] + DOC["arrivals"] + DOC["socials"]:
        assert 50.80 < item["lat"] < 50.87 and -0.20 < item["lon"] < -0.09, item.get("id", item.get("name"))
