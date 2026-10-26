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
    assert row["not_served"] is True and row["status"] == "Not calling"
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
        problems = add_disruption.validate(e, stops, set(ids))
        assert problems == [], (e["id"], problems)


def test_the_validator_catches_what_would_mislead():
    stops = {"C": {}, "D": {}}
    assert add_disruption.validate(an_entry(), stops) == []
    bad = " ".join(add_disruption.validate(
        an_entry(source_url="", stops_not_served=[{"atco": "ZZ"}],
                 starts="2026-09-28T07:00:00"), stops))
    assert "source_url" in bad, "an entry with no source would be accepted"
    assert "ZZ" in bad, "a stop code that does not exist would be accepted"
    assert "offset" in bad, "a time with no UTC offset would be accepted"


# Sub-entries: one notice, different words for different routes (the Old
# Steine works, October 2026). See api/local_disruptions.py.

def a_sub_entry(**over):
    e = an_entry(id="test-closure-route-2", parent="test-closure",
                 summary="Route 2 not serving Stop B",
                 lines=[{"operator": "BHBC", "line": "2"}],
                 stops_not_served=[{"atco": "B", "as_named": "Stop B"}], diversions=[])
    e.update(over)
    return e


def test_a_sub_entry_closes_its_stops_to_its_own_routes_only(tmp_path):
    path = a_file(tmp_path, an_entry(stops_not_served=[]), a_sub_entry())
    assert ld.closures_now(NOW, path) == {}, "a route's stop was closed to every bus"
    assert set(ld.closed_for(NOW, "BHBC", "2", path=path)) == {"B"}
    assert ld.closed_for(NOW, "BHBC", "1", path=path) == {}, "route 1 still calls at B"
    assert ld.closed_for(NOW, "BHBC", "N2", path=path) == {}, \
        "a notice about the 2 said which stops the N2 calls at"


def test_a_sub_entry_can_be_one_direction_only(tmp_path):
    path = a_file(tmp_path, an_entry(stops_not_served=[]), a_sub_entry(only_journeys_via=["E"]))
    assert set(ld.closed_for(NOW, "BHBC", "2", lambda: ["A", "B", "E"], path=path)) == {"B"}
    assert ld.closed_for(NOW, "BHBC", "2", lambda: ["A", "B", "C"], path=path) == {}, \
        "a journey that never reaches E was taken off B"
    assert ld.closed_for(NOW, "BHBC", "2", None, path=path) == {}, \
        "with no journey to look at, a one-way closure was applied both ways"


def test_a_daily_sub_entry_is_in_force_only_in_its_hours_across_the_clock_change(tmp_path):
    entry = a_sub_entry(daily={"from": "07:00", "to": "18:00"},
                        starts="2026-10-23T00:00:00+01:00", ends="2026-10-27T23:59:00+00:00")
    path = a_file(tmp_path, an_entry(stops_not_served=[]), entry)
    at = lambda *a: datetime(*a, tzinfo=timezone.utc)                       # noqa: E731
    assert ld.closed_for(at(2026, 10, 24, 6, 30), "BHBC", "2", path=path), "07:30 BST is in"
    assert not ld.closed_for(at(2026, 10, 24, 17, 30), "BHBC", "2", path=path), "18:30 BST is out"
    assert not ld.closed_for(at(2026, 10, 26, 6, 30), "BHBC", "2", path=path), "06:30 GMT is out"
    assert ld.closed_for(at(2026, 10, 26, 17, 30), "BHBC", "2", path=path), "17:30 GMT is in"
    situations = ld.as_situations(ld.load(path))
    assert [d["id"] for d in sx.current(situations, at(2026, 10, 24, 12))] == \
        ["test-closure", "test-closure-route-2"]
    evening = sx.current(situations, at(2026, 10, 24, 19))
    assert [d["id"] for d in evening] == ["test-closure"], "a daily notice showed outside its hours"
    noon = {d["id"]: d for d in sx.current(situations, at(2026, 10, 24, 12))}
    assert datetime.fromisoformat(noon["test-closure-route-2"]["ends"]) == at(2026, 10, 24, 17), \
        "until 18:00 BST today"
    assert noon["test-closure-route-2"]["final_ends"] == "2026-10-27T23:59:00+00:00"


def test_a_sub_entry_reaches_boards_through_its_routes_not_its_stops(tmp_path):
    path = a_file(tmp_path, an_entry(stops_not_served=[], stops_affected=[{"atco": "C"}]), a_sub_entry())
    now = sx.current(ld.as_situations(ld.load(path)), NOW)
    by_id = {d["id"]: d for d in now}
    assert by_id["test-closure"]["stops"] == ["C"], "the notice's affected stops lost their board"
    assert by_id["test-closure-route-2"]["stops"] == [] and by_id["test-closure-route-2"]["parent"] == "test-closure"
    board = {"departures": [{"service": "1", "operator": "BHBC"}]}
    assert [d["id"] for d in sx.affecting(now, stop_id="B", services={("BHBC", "1")})] == ["test-closure"], \
        "route 2's notice showed at a board route 2 does not serve"
    assert main._attach_disruptions(board, "B", now)["departures"][0]["disruption_ids"] == ["test-closure"]


def test_a_board_marks_only_the_routes_a_sub_entry_keeps_off(tmp_path, monkeypatch):
    monkeypatch.setattr(ld, "PATH", a_file(tmp_path, an_entry(stops_not_served=[]),
                                          a_sub_entry(lines=[{"operator": "BHBC", "line": "1"}])))
    monkeypatch.setattr(main, "datetime", _frozen())
    monkeypatch.setattr(main, "cache_get", lambda _k: None)
    monkeypatch.setattr(main, "cache_set", lambda *_a: None)
    live = {"aimed_departure": "x", "expected_departure": "y", "delay_seconds": 60,
            "live_source": "feed", "status": "Late"}
    board = {"departures": [{"service": "1", "operator": "BHBC", **live},
                            {"service": "2", "operator": "BHBC", **live}]}
    out = main._apply_stop_closure(board, LineTimetable(), "B")
    one, two = out["departures"]
    assert one["not_served"] is True and "expected_departure" not in one
    assert two == board["departures"][1], "a route still calling here lost its live time"
    assert "stop_closure" not in out, "the board said no buses stop here"
    [note] = out["line_closures"]
    assert note["lines"] == ["1"] and note["id"] == "test-closure-route-2"
    # So the board can fold it into the works' main notice, not a box of its own.
    assert note["parent"] == "test-closure"
    assert {a["atco"] for a in note["still_served"]} == {"A", "C"}
    assert main._apply_stop_closure(board, LineTimetable(), "D") is board


def test_the_validator_checks_a_sub_entry():
    stops = {"B": {}, "C": {}, "D": {}}
    assert add_disruption.validate(a_sub_entry(), stops, {"test-closure"}) == []
    bad = " ".join(add_disruption.validate(
        a_sub_entry(parent="nope", daily={"from": "18:00", "to": "07:00"}, lines=[]), stops, {"test-closure"}))
    assert "nope" in bad and "daily" in bad and "--lines" in bad
    assert "parent" in " ".join(add_disruption.validate(an_entry(only_journeys_via=["C"]), stops))


def test_a_second_notice_of_the_same_works_says_which_it_joins(tmp_path):
    """The council's Western Road notice for the 700 and Brighton & Hove
    Buses' own are one closure: the board shows them as one notice."""
    other = an_entry(id="test-closure-700", same_event_as="test-closure",
                     lines=[{"operator": "SCSO", "line": "700"}])
    path = a_file(tmp_path, an_entry(), other)
    by_id = {d["id"]: d for d in sx.current(ld.as_situations(ld.load(path)), NOW)}
    assert by_id["test-closure-700"]["same_event_as"] == "test-closure"
    assert "same_event_as" not in by_id["test-closure"]
    stops = {"B": {}, "C": {}, "D": {}}
    assert add_disruption.validate(other, stops, {"test-closure"}) == []
    assert "nope" in " ".join(add_disruption.validate(
        {**other, "same_event_as": "nope"}, stops, {"test-closure"}))
    assert "itself" in " ".join(add_disruption.validate(
        {**other, "same_event_as": "test-closure-700"}, stops, {"test-closure-700"}))


def test_a_sub_entry_notice_rides_only_its_own_journeys(tmp_path):
    path = a_file(tmp_path, an_entry(stops_not_served=[]),
                  a_sub_entry(lines=[{"operator": "BHBC", "line": "1"}], only_journeys_via=["A"]))
    now = sx.current(ld.as_situations(ld.load(path)), NOW)
    board = {"departures": [{"service": "1", "operator": "BHBC", "_trip_id": "WEST"},
                            {"service": "1", "operator": "BHBC", "_trip_id": "EAST"},
                            {"service": "N1", "operator": "BHBC", "_trip_id": "WEST"}]}

    class OneWay(LineTimetable):
        def trip_stops_for(self, trip_id):          # EAST stops short of A here
            calls = super().trip_stops_for(trip_id)
            return calls[1:] if trip_id == "EAST" else calls
    out = main._attach_disruptions(board, "C", now, OneWay())
    west, east, night = (d.get("disruption_ids") for d in out["departures"])
    assert west == ["test-closure", "test-closure-route-2"]
    assert east == ["test-closure"], "the other direction was given a one-way notice"
    assert night == ["test-closure"], "the N1 was given the 1's sub-entry"
    alone = main._attach_disruptions({"departures": board["departures"][1:]}, "C", now, OneWay())
    assert [d["id"] for d in alone["disruptions"]] == ["test-closure"], \
        "the board listed a sub-entry none of its journeys is under"
