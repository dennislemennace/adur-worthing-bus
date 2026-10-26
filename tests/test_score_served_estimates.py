"""scripts/score_served_estimates.py: the board as it was, scored.

The replay must judge each estimate by RTIG's window for how far ahead it said
the bus was, say why a departure had no live time, and catch a board still
listing a bus that has gone.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import score_served_estimates as ss                               # noqa: E402


class TT:
    """One journey, three stops, ten minutes apart from 10:00."""
    trips = {"T1": {"route_id": "R1", "service_id": "S"}}
    routes = {"R1": {"short_name": "7"}}
    stops = {"A": {}, "B": {}, "C": {}}

    def trip_stops_for(self, _trip):
        return [(36000, "A"), (36600, "B"), (37200, "C")]

    def noc_for_route(self, _route):
        return "BHBC"

    def runs_on(self, _service, _day):
        return True


def observation(atco, secs, left):
    return {"trip_id": "T1", "atco": atco, "scheduled_secs": secs, "observed_epoch": left,
            "match": "declared", "estimated": False, "quality_flags": []}


def a_day(*obs):
    return ss.Day(TT(), list(obs))


AT_10 = datetime.fromtimestamp(36000, timezone.utc)


def test_each_estimate_is_judged_by_how_far_ahead_it_said():
    day = a_day(observation("B", 36600, 36700))
    # At 10:00 the board says B at 10:12 (12 minutes ahead: 6-12, 120 s early
    # and 210 s late allowed). The bus left at 10:11:40.
    day.score_minute(AT_10, [("T1", "B", 36600, 1, 36720)], origin=0)
    day.score_minute(AT_10, [("T1", "B", 36600, 1, 36900)], origin=0)
    assert day.accuracy["all"] == {"ok": 1, "early": 1}, "a bus 200 s before the estimate is early"
    assert day.accuracy["range|6-12 min"]["ok"] == 1
    assert day.accuracy["operator|BHBC"]["early"] == 1
    assert day.accuracy["position|stops 2-3"]["ok"] == 1


def test_a_departure_with_no_live_time_says_why():
    day = a_day(observation("B", 36600, 36700))
    day.score_minute(AT_10, [("T1", "B", 36600, 1, 36720)], origin=0)
    calls = [("T1", "A", 36000), ("T1", "B", 36600), ("T1", "C", 37200)]
    cov = day.report(calls, 30000, 40000, origin=0)["coverage"]["all"]
    assert cov["with_estimate"] == 1 and cov["seen_not_named"] == 2, \
        "a journey the recorder saw but no feed named was blamed on no bus"
    day.declared_trips.add("T1")
    cov = day.report(calls, 30000, 40000, origin=0)["coverage"]["all"]
    assert cov["named_no_estimate"] == 2
    assert day.report(calls, 30000, 40000, origin=0)["journeys"]["all"] == {"journeys": 1, "with_estimate": 1}


def test_a_bus_that_left_early_lingers_on_the_board_until_its_timetabled_time():
    day = a_day(observation("B", 36600, 36300))
    key = ("T1", "B", 36600)
    for stamp in range(36360, 36900, 60):
        # No estimate once the bus has gone: the row falls back to its
        # timetabled time, and the API keeps it until that has passed.
        day.cleardown_minute(stamp, {}, 0, [(key, 36300)])
    out = day.report([], 30000, 40000, origin=0)["cleardown"]["all"]
    assert out.get("listed_5_min_after") == 1, "five minutes of a ghost row went uncounted"
    assert out["left_before_timetable"] == 1


def test_days_add_up_and_methods_do_not_mix():
    one = {"method_version": ss.METHOD_VERSION, "days": ["2026-10-23"], "inputs": {"siri_minutes": 2},
           "accuracy": {"all": {"ok": 3}}, "coverage": {}, "journeys": {}, "cleardown": {}}
    two = {**one, "days": ["2026-10-24"], "accuracy": {"all": {"ok": 1, "early": 2}}}
    merged = ss.merge([one, two])
    assert merged["accuracy"]["all"] == {"ok": 4, "early": 2}
    assert merged["days"] == ["2026-10-23", "2026-10-24"] and merged["inputs"]["siri_minutes"] == 4
    with pytest.raises(ValueError):
        ss.merge([one, {**two, "method_version": 0}])


def test_minute_files_are_keyed_by_the_epoch_minute(tmp_path):
    (tmp_path / "0800-1792828800.xml").write_text("x")
    (tmp_path / "notes.xml").write_text("x")
    assert list(ss.minute_files(tmp_path, "xml")) == [1792828800 // 60]


def a_served_day(day, ok=1):
    return {"method_version": ss.METHOD_VERSION, "days": [day], "method": "m", "caveats": ["c"],
            "as_of": f"{day}T03:00:00+00:00", "inputs": {}, "accuracy": {"all": {"ok": ok}},
            "coverage": {"all": {"calls": 2, "with_estimate": 1, "no_bus_seen": 1}},
            "journeys": {"all": {"journeys": 1, "with_estimate": 1}}, "cleardown": {}}


def test_the_window_takes_tonight_then_the_last_generation_then_the_backfill(tmp_path):
    import json
    import collect_served_window as cw
    tonight = tmp_path / "served-2026-10-26.json"
    tonight.write_text(json.dumps(a_served_day("2026-10-26", 5)))
    seed = tmp_path / "seed"
    seed.mkdir()
    (seed / "served-2026-10-24.json").write_text(json.dumps(a_served_day("2026-10-24", 2)))
    published = json.dumps(a_served_day("2026-10-25", 3)).encode()
    fetched = []
    found, missing = cw.collect(
        ["2026-10-23", "2026-10-24", "2026-10-25", "2026-10-26"], tonight,
        {"served-2026-10-25.json": {"file": "builds/x/served-2026-10-25.json"}}, seed,
        lambda entry: fetched.append(entry["file"]) or published)
    assert sorted(found) == ["2026-10-24", "2026-10-25", "2026-10-26"]
    assert missing == ["2026-10-23"], "a day found nowhere was not named"
    assert fetched == ["builds/x/served-2026-10-25.json"]
    merged = ss.merge([json.loads(raw) for raw in found.values()])
    assert merged["accuracy"]["all"]["ok"] == 10


def test_published_scores_must_add_up():
    import check_published as cp
    good = a_served_day("2026-10-24")
    fails = cp.Failures()
    cp.check_served(good, "served-2026-10-24.json", fails)
    assert not fails.items
    bad = {**good, "coverage": {"all": {"calls": 3, "with_estimate": 1, "no_bus_seen": 1}}}
    fails = cp.Failures()
    cp.check_served(bad, "served-2026-10-24.json", fails)
    assert fails.items, "reasons that do not add up to the departures were published"


def test_transportapis_prediction_is_scored_beside_ours_on_the_same_departure():
    day = a_day(observation("B", 36600, 36700), observation("C", 37200, 37300))
    rows = [
        # Fetched at 10:00: B at 10:12 (ours said 10:12 too), C cancelled but it ran.
        {"fetched_at": 36000, "stop": "B", "trip_id": "T1", "cancelled": False,
         "aimed": datetime.fromtimestamp(36600, timezone.utc).isoformat(),
         "expected": datetime.fromtimestamp(36900, timezone.utc).isoformat()},
        {"fetched_at": 36000, "stop": "C", "trip_id": "T1", "cancelled": True, "expected": None,
         "aimed": datetime.fromtimestamp(37200, timezone.utc).isoformat()},
    ]
    day.compare(rows, 36000, {("T1", "B", 36600): 36720})
    assert day.versus["both|nextbuses|all"] == {"early": 1}, "theirs said 10:15; the bus left 10:11:40"
    assert day.versus["both|ours|all"] == {"ok": 1}
    assert day.versus["cancelled|all"]["said_cancelled_but_ran"] == 1
    day.compare(rows[:1], 36000, {})
    assert day.versus["nextbuses_only|nextbuses|all"] == {"early": 1}, \
        "a departure only TransportAPI had a time for was counted as compared"
