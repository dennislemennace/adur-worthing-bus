"""Disruptions we record by hand (data/disruptions.json, api/local_disruptions.py).

Nobody in this area publishes to the BODS disruptions feed, so a road closed
for six weeks exists here only if it is copied in from the operator's page.
These pin what such an entry must do: reach the same boards the feed's notices
reach, stop a closed stop's board promising buses, and say where to go instead.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import add_disruption                                            # noqa: E402
import api.main as main                                          # noqa: E402
from api import disruptions as sx                                # noqa: E402
from api import local_disruptions as ld                          # noqa: E402

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


class LineTimetable:
    """One road, west to east: A B C D E, about 300 m apart. Route 1 runs A to
    E and back. C and D are the stops the closure takes."""

    def __init__(self):
        self.stops = {s: {"name": f"Stop {s}", "lat": 50.825, "lon": -0.170 + i * 0.0043}
                      for i, s in enumerate("ABCDE")}
        self.routes = {"R1": {"short_name": "1"}}
        self.trips = {"EAST": {"route_id": "R1", "headsign": "E"},
                      "WEST": {"route_id": "R1", "headsign": "A"}}
        self._calls = {"EAST": [(36000 + i * 120, s) for i, s in enumerate("ABCDE")],
                       "WEST": [(36000 + i * 120, s) for i, s in enumerate("EDCBA")]}

    def stop_times_for(self, stop_id):
        return [(secs, t) for t, calls in self._calls.items()
                for secs, a in calls if a == stop_id]

    def trip_stops_for(self, trip_id):
        return self._calls.get(trip_id, [])


def an_entry(**over):
    e = {
        "id": "test-closure", "summary": "Main Road closed", "description": "Their words.",
        "advice": "Allow extra time.", "reason": "roadworks", "severity": "severe",
        "starts": "2026-09-28T07:00:00+01:00", "ends": "2026-11-06T23:59:00+00:00",
        "publisher": "Brighton & Hove Buses", "source_url": "https://example.org/notice",
        "checked_on": "2026-09-28",
        "lines": [{"operator": "BHBC", "line": "1"}],
        "stops_not_served": [{"atco": "C", "as_named": "Stop C"}, {"atco": "D", "as_named": "Stop D"}],
        "diversion_stops": [],
        "diversions": [{"towards": "E", "routes": "All routes", "via": ["Back Lane"], "rejoins": "Stop E"}],
    }
    e.update(over)
    return e


def a_file(tmp_path, *entries):
    path = tmp_path / "disruptions.json"
    path.write_text(json.dumps({"disruptions": list(entries)}))
    return path


def _frozen():
    import datetime as dt

    class Frozen(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)
    return Frozen


def test_a_closed_stop_is_offered_the_nearest_stops_still_served_either_way():
    alts = ld.still_served_nearby(LineTimetable(), "C", {"C", "D"})
    by_stop = {a["atco"]: a for a in alts}
    assert set(by_stop) == {"B", "E"}, "the walk did not stop at the first stop still served"
    assert by_stop["B"]["direction"] == "west" and by_stop["E"]["direction"] == "east"
    assert alts[0]["atco"] == "B", "nearest first"
    assert by_stop["B"]["routes"] == ["1"]
    assert by_stop["B"]["side"] == "both", "B is before C going east and after it going west"


def test_only_stops_in_force_are_closed(tmp_path):
    path = a_file(tmp_path, an_entry())
    assert set(ld.closures_now(NOW, path)) == {"C", "D"}
    assert ld.closures_now(datetime(2026, 11, 7, 12, tzinfo=timezone.utc), path) == {}
    assert ld.closures_now(datetime(2026, 9, 28, 5, tzinfo=timezone.utc), path) == {}


def test_a_closed_board_stops_promising_buses(tmp_path, monkeypatch):
    monkeypatch.setattr(ld, "PATH", a_file(tmp_path, an_entry()))
    monkeypatch.setattr(main, "datetime", _frozen())
    monkeypatch.setattr(main, "cache_get", lambda _k: None)
    monkeypatch.setattr(main, "cache_set", lambda *_a: None)
    board = {"departures": [{"service": "1", "operator": "BHBC", "aimed_departure": "x",
                             "expected_departure": "y", "delay_seconds": 60,
                             "live_source": "feed", "status": "Late"}]}
    out = main._apply_stop_closure(board, LineTimetable(), "C")
    row = out["departures"][0]
    assert row["not_served"] is True and row["status"] == "Not served"
    assert "expected_departure" not in row, "a live estimate survived for a stop no bus calls at"
    closure = out["stop_closure"]
    assert closure["until"].startswith("2026-11-06")
    assert [a["atco"] for a in closure["still_served"]] == ["B", "E"]
    assert closure["source_url"] == "https://example.org/notice"
    assert main._apply_stop_closure(board, LineTimetable(), "B") is board, \
        "an open stop's board was touched"


def test_our_notices_ride_the_same_path_as_the_feeds(tmp_path, monkeypatch):
    monkeypatch.setattr(ld, "PATH", a_file(tmp_path, an_entry()))
    now = sx.current(ld.as_situations(ld.load()), NOW)
    assert [d["id"] for d in now] == ["test-closure"]
    d = now[0]
    assert d["source"] == "curated" and d["checked_on"] == "2026-09-28"
    assert d["stops_not_served"] == ["C", "D"]
    assert d["link"] == "https://example.org/notice"
    board = {"departures": [{"service": "1", "operator": "BHBC"}]}
    attached = main._attach_disruptions(board, "B", now)
    assert attached["departures"][0]["disruption_ids"] == ["test-closure"], \
        "the affected line's row at an open stop lost the notice"


def test_the_recorded_file_is_sound():
    """The real data/disruptions.json, through the checks add_disruption.py
    applies before it writes."""
    doc = json.loads((ROOT / "data" / "disruptions.json").read_text())
    stops = add_disruption.stop_index()
    ids = [e["id"] for e in doc["disruptions"]]
    assert len(ids) == len(set(ids)), "two entries share an id"
    for e in doc["disruptions"]:
        assert add_disruption.validate(e, stops) == [], (e["id"], add_disruption.validate(e, stops))


def test_the_validator_catches_what_would_mislead():
    stops = {"C": {}, "D": {}}
    assert add_disruption.validate(an_entry(), stops) == []
    bad = " ".join(add_disruption.validate(
        an_entry(source_url="", stops_not_served=[{"atco": "ZZ"}],
                 starts="2026-09-28T07:00:00"), stops))
    assert "source_url" in bad, "an entry with no source would be accepted"
    assert "ZZ" in bad, "a stop code that does not exist would be accepted"
    assert "offset" in bad, "a time with no UTC offset would be accepted"
