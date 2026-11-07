"""Ramp use logged by hand on ride.html (scripts/build_ramp_log.py): a stop is
shown only past its floor, timed only where both taps were made.

Run with:  pytest
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import build_ramp_log as brl  # noqa: E402


def row(i, day, atco="A", out="2026-11-01T10:00:00Z", back="2026-11-01T10:01:00Z", note="", event="stop"):
    return {"id": f"r{i}", "ride_id": f"ride{day}", "day": day, "atco": atco, "stop_name": f"Stop {atco}",
            "event": event, "note": note, "ramp_out_at": out, "ramp_in_at": back}


def test_a_stop_is_shown_only_past_its_floor_and_timed_where_both_taps_were_made():
    rows = [row(i, f"2026-11-0{1 + i % 5}") for i in range(10)]
    rows.append(row(99, "2026-11-01", out="", back="", note="Wheelchair ramp used"))   # noted, untimed
    rows.append(row(98, "2026-11-01", back="2026-11-02T10:00:00Z"))                     # a tap forgotten
    rows.append(row(97, "2026-11-01", atco="B"))                                        # under the floor
    rows.append(row(96, "2026-11-01", atco="C", event="skipped"))                       # not a stop
    doc = brl.build(rows, "2026-11-07")
    assert list(doc["stops"]) == ["A"]
    a = doc["stops"]["A"]
    assert a["deployments"] == 12 and a["days"] == 5 and a["timed"] == 10 and a["median_secs"] == 60
    assert doc["totals"]["deployments"] == 13 and doc["totals"]["stops"] == 2


def test_the_published_file_is_hand_logged_and_names_its_floor():
    doc = json.loads((ROOT / "data" / "ramp_use.json").read_text(encoding="utf-8"))
    assert doc["floor"] == {"deployments": brl.MIN_DEPLOYMENTS, "days": brl.MIN_DAYS}
    assert any("by hand" in c for c in doc["caveats"])
