"""The timetable, recorded beside what the buses actually did.

Every observation row carries the scheduled time it was measured against, but
only for journeys a bus was seen making. The timetable itself was enumerated
every night and thrown away, keeping counts — so no chart could draw what the
timetable promised across the day, a scheduled bus that was never seen left no
trace, and the weekly rebuild replaced the only copy.

These tests hold the record to four promises:

  * it is every scheduled local journey on the service day, and nothing else —
    including one timed after 26:00, which the matching window cannot see;
  * it reproduces the timetable exactly;
  * it joins to the observations, because both hash one route pattern the same
    way; and
  * publication refuses a day without it, because a night that failed to
    record it would lose that timetable for good.

Run with:  pytest
"""

import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import build_journey_times as bjt                      # noqa: E402
import process_snapshots as ps                         # noqa: E402
from observation_contract import route_pattern_id      # noqa: E402
from publication_bundle import validate_schedule       # noqa: E402
import check_published as checks                       # noqa: E402
from api.trip_match import COACH_NOCS                  # noqa: E402
from test_gtfs_rt import a_feed, a_vehicle             # noqa: E402
from test_process_snapshots import (                   # noqa: E402,F401
    DAY, monkeypatch_module, tt, west_xy)


def _oracle(tt, day):
    """Every non-coach trip calling in the box that runs on `day`, computed
    without build_instances, so the test does not share the code it checks."""
    wanted = set()
    for atco in ps.stops_in_box(tt):
        for _secs, trip_id in tt.stop_times_for(atco):
            trip = tt.trips.get(trip_id) or {}
            if tt.noc_for_route(trip.get("route_id", "")) in COACH_NOCS:
                continue
            if tt.runs_on(trip.get("service_id", ""), day):
                wanted.add(trip_id)
    return wanted


@pytest.fixture(scope="module")
def schedule(tt):
    return ps.record_schedule(tt, DAY)


def test_the_record_is_every_scheduled_local_journey_on_the_day(tt, schedule):
    recorded = {t[0] for t in schedule["trips"]}
    assert recorded == _oracle(tt, DAY)
    assert schedule["counts"]["trips"] == len(schedule["trips"]) == len(recorded)


def test_a_journey_timed_after_2600_is_recorded_on_its_own_day(schedule):
    # 28:40 is twenty to five the next morning, still this service day. The
    # matching window stops at 26:00, so a record taken from it lost this trip
    # on both nights: outside the window on its own day, and filed under the
    # other service day on the next.
    morning = [t for t in schedule["trips"] if t[0] == "MORNING"]
    assert morning, "a journey scheduled after 26:00 was not recorded"
    assert morning[0][3] == 28 * 3600 + 40 * 60


def test_coaches_are_not_recorded(schedule):
    assert not any(p["operator"] in COACH_NOCS for p in schedule["patterns"].values())
    assert not any(t[0].startswith("C") for t in schedule["trips"])


def test_the_record_reproduces_the_timetable_exactly(tt, schedule):
    for trip_id, pattern_id, profile_ref, start, _headsign in schedule["trips"]:
        calls = [(s, a) for s, a in tt.trip_stops_for(trip_id) if s is not None]
        pattern = schedule["patterns"][pattern_id]
        profile = schedule["profiles"][profile_ref]
        assert pattern["atcos"] == [a for _s, a in calls], trip_id
        assert [start + o for o in profile["offsets"]] == [s for s, _a in calls], trip_id


def test_the_pattern_hash_is_the_one_old_observations_used():
    # The formula every observation published before this change was hashed
    # with. If it moved, no archived day would join to a recorded timetable.
    route, atcos = "R700", ["4400A", "4400B", "4400C"]
    old = hashlib.sha256(json.dumps([route, atcos], separators=(",", ":")).encode()).hexdigest()
    assert route_pattern_id(route, atcos) == old


@pytest.fixture(scope="module")
def processed(tt, tmp_path_factory):
    """One real run of the processor: an RT-only morning of three westbound buses."""
    base = tmp_path_factory.mktemp("run")
    raw, rt = base / "raw" / DAY.isoformat(), base / "rt" / DAY.isoformat()
    raw.mkdir(parents=True)
    rt.mkdir(parents=True)
    for n, minute in enumerate((615, 616, 617, 618)):
        lat, lon = west_xy(15 + n)
        stamp = int(datetime(2026, 9, 16, 9, 15 + n, tzinfo=timezone.utc).timestamp())
        (rt / f"{minute // 60:02d}{minute % 60:02d}.pb").write_bytes(
            a_feed([a_vehicle(trip=f"W{start}", vehicle=f"SCSO-{start}", lat=lat,
                              lon=lon, stamp=stamp) for start in (600, 610, 620)]))
    out = base / "observations.json"
    assert ps.main(["--day", DAY.isoformat(), "--snapshots", str(raw), "--gtfs-rt", str(rt),
                    "--timetable", str(tt.db_path), "--out", str(out)]) == 0
    return out, json.loads(out.read_text())


def test_the_observations_file_carries_the_days_timetable(processed):
    _out, doc = processed
    assert doc["schedule"]["day"] == doc["day"]
    validate_schedule(doc)


def test_every_observation_joins_to_the_recorded_timetable(processed):
    _out, doc = processed
    patterns = doc["schedule"]["patterns"]
    trips = {t[0] for t in doc["schedule"]["trips"]}
    rows = [r for r in doc["observations"] if r["day"] == doc["day"]]
    assert rows
    for row in rows:
        assert row["route_pattern"] in patterns, row["trip_id"]
        assert row["trip_id"] in trips, row["trip_id"]


def test_the_service_document_draws_from_its_own_timetable(processed, tmp_path, tt):
    out, _doc = processed
    derived = tmp_path / "journeys"
    assert bjt.main(["--observations", str(out), "--out", str(derived),
                     "--timetable", str(tt.db_path)]) == 0
    doc = json.loads((derived / "700-SCSO.json").read_text())
    schedule = doc["schedule"]
    assert DAY.isoformat() in schedule["days"]
    trips = schedule["sets"][schedule["days"][DAY.isoformat()]]
    # Only this document's own service: the coach sharing these stops is not
    # a 700, and a line drawn from its timetable would be a promise the 700
    # never made.
    assert trips and not any(t[0].startswith("C") for t in trips)
    for calls in schedule["profiles"]:
        assert all(0 <= c[0] < len(doc["stops"]) for c in calls)
    failures = checks.Failures()
    checks.check_document_schedule(doc, "700-SCSO.json", failures)
    assert not failures.items, failures.items


def _schedule_meta(days, schedules):
    return {"days": days, "schedules": schedules}


def _one_day(day, trips):
    pattern = {"route_id": "R1", "operator": "SCSO", "service": "700",
               "direction": "westbound", "atcos": ["A", "B"]}
    # Another operator's service over the same two stops. A document is one
    # service as one operator runs it, so this must never reach the 700's line.
    other = {"route_id": "R2", "operator": "BHBC", "service": "2",
             "direction": "westbound", "atcos": ["A", "B"]}
    return {"day": day, "patterns": {"p": pattern, "q": other},
            "profiles": [{"offsets": [0, 600], "timepoints": [1, 1]}],
            "trips": [[t, "p", 0, start, "Worthing"] for t, start in trips]
                     + [["OTHER", "q", 0, 30000, "Shoreham"]]}


def test_identical_days_share_one_set_and_unrecorded_days_are_named():
    index = {"A": 0, "B": 1}
    weekday = [("T1", 28800), ("T2", 32400)]
    meta = _schedule_meta(
        ["2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24"],
        {"2026-09-21": _one_day("2026-09-21", weekday),
         "2026-09-22": _one_day("2026-09-22", weekday),
         "2026-09-23": _one_day("2026-09-23", [("T9", 36000)])})
    built = bjt.schedule_for_document("700", "SCSO", index, meta)
    assert len(built["sets"]) == 2
    assert built["days"]["2026-09-21"] == built["days"]["2026-09-22"]
    assert built["days"]["2026-09-23"] != built["days"]["2026-09-21"]
    assert built["unrecorded_days"] == ["2026-09-24"]
    assert not any(t[0] == "OTHER" for trips in built["sets"] for t in trips)
    assert built["profiles"] == [[[0, 0, 1], [1, 600, 1]]]


def test_publication_refuses_a_day_without_its_timetable(processed):
    # Through the publication entry point, not the helper: what matters is that
    # a day missing its timetable cannot be published, not that a function
    # exists which would notice.
    from publication_bundle import validate_observations
    _out, doc = processed
    validate_observations(doc)
    without = {k: v for k, v in doc.items() if k != "schedule"}
    with pytest.raises(ValueError, match="no recorded timetable"):
        validate_observations(without)


def test_publication_refuses_a_timetable_running_backwards(processed):
    _out, doc = processed
    broken = json.loads(json.dumps(doc))
    offsets = broken["schedule"]["profiles"][0]["offsets"]
    offsets[-1] = offsets[-2] - 60
    with pytest.raises(ValueError, match="backwards"):
        validate_schedule(broken)


def test_publication_refuses_counts_that_contradict_the_contents(processed):
    _out, doc = processed
    broken = json.loads(json.dumps(doc))
    broken["schedule"]["counts"]["trips"] += 1
    with pytest.raises(ValueError, match="counts"):
        validate_schedule(broken)


def test_a_published_profile_naming_a_missing_stop_fails_the_check():
    doc = {"stops": [{"atco": "A"}], "window_days": ["2026-09-21"],
           "schedule": {"profiles": [[[0, 0, 1], [5, 600, 1]]], "sets": [[["T", 0, 0, "x"]]],
                        "days": {"2026-09-21": 0}}}
    failures = checks.Failures()
    checks.check_document_schedule(doc, "x.json", failures)
    assert any("not in the stops list" in str(item) for item in failures.items)
