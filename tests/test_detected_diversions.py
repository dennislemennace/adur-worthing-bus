"""Diversions detected in the buses' own positions
(scripts/build_detected_diversions.py): stops usually passed that buses have
stopped passing. Each check here is a way a one-minute record could fool it.

Run with:  pytest
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import add_disruption  # noqa: E402
import build_detected_diversions as bdd  # noqa: E402

DAYS = ["2026-10-28", "2026-10-29", "2026-10-30", "2026-10-31", "2026-11-01", "2026-11-02", "2026-11-03"]
STOPS = [("S0", "Start"), ("S1", "Brunswick Place"), ("S2", "Norfolk Square"), ("S3", "Palmeira Square"), ("S4", "End")]


def rows(seen_share_by_day, journeys=10, stop_seen=None):
    """`journeys` a day on the 700 eastbound past five stops. `seen_share_by_day`
    gives, per day, the share seen at S1 and S2; the rest are always seen
    unless `stop_seen` says otherwise."""
    out = []
    for day in DAYS:
        share = seen_share_by_day.get(day, 0.9)
        for j in range(journeys):
            for i, (atco, name) in enumerate(STOPS):
                base = (stop_seen or {}).get(atco)
                seen = (j < round(share * journeys)) if atco in ("S1", "S2") else (base if base is not None else True)
                out.append({"day": day, "operator": "SCSO", "service": "700", "direction": "eastbound",
                            "atco": atco, "stop_name": name, "stop_index": i, "estimated": not seen,
                            "quality_flags": [] if seen else ["interpolated"]})
    return out


def test_a_stretch_skipped_for_the_last_two_days_is_found_as_one():
    found, days = bdd.detect(rows({"2026-11-02": 0.0, "2026-11-03": 0.0}))
    assert len(found) == 1, found
    d = found[0]
    assert [s["atco"] for s in d["stops"]] == ["S1", "S2"]
    assert d["first_seen"] == "2026-11-02" and d["last_seen"] == "2026-11-03"
    assert d["confidence"] == "high" and d["recent_share"] == 0


def test_ordinary_misses_one_odd_day_and_a_stop_never_seen_are_not_diversions():
    # A stop passed quickly, seen on half the journeys every day.
    assert bdd.detect(rows({d: 0.5 for d in DAYS}))[0] == []
    # One bad day only.
    assert bdd.detect(rows({"2026-11-03": 0.0}))[0] == []
    # A stop the record never sees: nothing to compare with.
    assert bdd.detect(rows({d: 0.0 for d in DAYS}))[0] == []
    # Too few journeys to say.
    assert bdd.detect(rows({"2026-11-02": 0.0, "2026-11-03": 0.0}, journeys=3))[0] == []


def test_each_detection_is_a_notice_and_one_that_ends_is_cleared():
    found, _ = bdd.detect(rows({"2026-11-02": 0.0, "2026-11-03": 0.0}))
    lines = bdd.notice_lines(found, {"old0000000ab"})
    assert lines[0] == "detected-diversion:old0000000ab\tclear\n"
    assert lines[1].startswith(f"detected-diversion:{found[0]['id']}\twarn\tThe 700")
    assert "--from-detected" in lines[1]


def test_a_detection_drafts_the_record_and_leaves_the_operators_words_to_a_person():
    found, _ = bdd.detect(rows({"2026-11-02": 0.0, "2026-11-03": 0.0}))
    cmd = add_disruption.draft_from_detected(found[0])
    assert "--not-served S1,S2" in cmd and "--lines SCSO:700" in cmd
    assert "--starts 2026-11-02" in cmd
    assert "FILL IN" in cmd and "FILL-IN-the-notice-link" in cmd, "a source was invented"


def test_the_nightly_build_publishes_the_detections():
    wf = (ROOT / ".github" / "workflows" / "process-snapshots.yml").read_text(encoding="utf-8")
    assert "scripts/build_detected_diversions.py --observations inputs/window" in wf
    assert '--artifact "detected-diversions.json=detected-diversions.json"' in wf
