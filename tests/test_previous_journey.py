"""Estimates before a journey starts, from the bus that works it next
(live_eta.from_previous_journey, main._apply_own_feed_estimates).

Only for operators switched on after the night's scoring showed they beat the
timetable, and marked as what they are."""

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api import main                                               # noqa: E402

NOW = datetime(2026, 10, 26, 11, 0, tzinfo=timezone.utc)
BUS = {"trip_id": "T1", "trip_source": "feed", "lateness_secs": 300, "operator_ref": "SCSC", "vehicle_ref": "B1"}


class TT:
    trips = {"T1": {"service_id": "S"}}

    def runs_on(self, *_a):
        return True


def board():
    return {"departures": [{"_trip_id": "T2", "aimed_departure": "2026-10-26T12:00:00+00:00",
                            "status": "Scheduled"}]}


def test_a_journey_no_bus_is_on_yet_starts_as_late_as_its_bus_will_get_there(monkeypatch):
    monkeypatch.setattr(main, "cache_get", lambda _k: {"vehicles": [BUS]})
    monkeypatch.setattr(main, "PREVIOUS_JOURNEY_OPERATORS", frozenset({"SCSO"}))
    monkeypatch.setattr(main.live_eta, "from_previous_journey",
                        lambda *_a, **_k: {"trip_id": "T2", "lateness_secs": 240})
    row = main._apply_own_feed_estimates(board(), TT(), "A", NOW)["departures"][0]
    assert row["live_source"] == "previous_journey", "the board could not tell it from a bus on the road"
    assert row["expected_departure"].startswith("2026-10-26T12:04")
    assert row["status"] == "Late" and row["vehicle_ref"] == "B1"


def test_off_for_operators_not_switched_on(monkeypatch):
    monkeypatch.setattr(main, "cache_get", lambda _k: {"vehicles": [BUS]})
    monkeypatch.setattr(main, "PREVIOUS_JOURNEY_OPERATORS", frozenset())
    monkeypatch.setattr(main.live_eta, "from_previous_journey",
                        lambda *_a, **_k: {"trip_id": "T2", "lateness_secs": 240})
    row = main._apply_own_feed_estimates(board(), TT(), "A", NOW)["departures"][0]
    assert "expected_departure" not in row, "an operator nobody had checked was estimated"
