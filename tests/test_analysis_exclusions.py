"""The permanent register of events that distort measured data
(data/analysis_exclusions.json, scripts/analysis_exclusions.py): what it
matches, that the builders mark rather than drop by default, and that a notice
flagged for it cannot be pruned before it is recorded."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import analysis_exclusions as ax                  # noqa: E402
import add_disruption as ad                        # noqa: E402
from test_delay_hotspots import pair               # noqa: E402

WESTERN_ROAD = "bhbc-western-road-hove-2026-09-28"
AREA = [[50.8240, -0.1622], [50.8315, -0.1622], [50.8315, -0.1518], [50.8240, -0.1518]]
PALMEIRA = (50.82679, -0.162682)     # just west of the area
CLOCK_TOWER = (50.8247, -0.1414)     # well east of it
DURING = datetime(2026, 10, 7, 8, 30, tzinfo=timezone.utc)


def entries():
    return ax.load()


def test_a_journey_through_the_closed_streets_during_the_dates_is_matched():
    assert ax.affecting(entries(), "BHBC", "1", DURING, path=[PALMEIRA, CLOCK_TOWER]) == [WESTERN_ROAD], \
        "a stretch crossing the area, with neither end inside it, was missed"
    assert ax.affecting(entries(), "BHBC", "49", DURING, stop_ids=["149000006946"]) == [WESTERN_ROAD]
    # Metrobus runs under Brighton & Hove's licence, but its routes are not named.
    assert ax.affecting(entries(), "METR", "270", DURING, path=[PALMEIRA, CLOCK_TOWER]) == []


def test_other_routes_days_and_streets_are_not():
    e = entries()
    assert ax.affecting(e, "BHBC", "7", DURING, path=[PALMEIRA, CLOCK_TOWER]) == [], "a route not diverted"
    assert ax.affecting(e, "SCSO", "1", DURING, path=[PALMEIRA, CLOCK_TOWER]) == [], "Stagecoach's 1"
    assert ax.affecting(e, "BHBC", "1", datetime(2026, 11, 8, tzinfo=timezone.utc),
                        path=[PALMEIRA, CLOCK_TOWER]) == [], "after it ended"
    assert ax.affecting(e, "BHBC", "1", DURING, path=[(50.8400, -0.1400), (50.8450, -0.1300)]) == [], \
        "a stretch nowhere near"


def test_the_delay_map_marks_by_default_and_drops_only_when_asked():
    from build_delay_hotspots import build_hotspots
    rows = pair(day="2026-10-07", operator="BHBC")
    for r in rows:
        r["service"] = "1"
    coords = {"S0": PALMEIRA, "S1": CLOCK_TOWER}
    marked = build_hotspots(rows, {}, exclusions=entries(), coords=coords)
    assert marked["traversals"][0]["exclusions"] == [WESTERN_ROAD]
    assert marked["cells"][0]["excluded_by"] == {WESTERN_ROAD: 1}
    dropped = build_hotspots(rows, {}, exclusions=entries(), coords=coords, drop_excluded=True)
    assert dropped["traversals"] == [] and dropped["excluded"]["analysis_exclusion"] == 1
    plain = build_hotspots(rows, {})
    assert "excluded_by" not in plain["cells"][0], "marked with no register given"


def test_a_notice_flagged_for_analysis_is_not_pruned_until_recorded():
    ended = {"id": "x-ended-closure", "ends": "2026-10-01T00:00:00+01:00", "record_for_analysis": True}
    plain = {"id": "y-ended-notice", "ends": "2026-10-01T00:00:00+01:00"}
    now = datetime(2026, 12, 1, tzinfo=timezone.utc)
    kept, removed, held = ad.prunable({"disruptions": [ended, plain]}, {"exclusions": []}, now)
    assert [e["id"] for e in kept] == ["x-ended-closure"] and held == ["x-ended-closure"]
    assert removed == ["y-ended-notice"]
    kept, removed, held = ad.prunable({"disruptions": [ended]},
                                      {"exclusions": [{"disruption_id": "x-ended-closure"}]}, now)
    assert kept == [] and removed == ["x-ended-closure"]


def test_the_register_is_well_formed_and_every_flagged_notice_is_in_it():
    doc = json.loads((ROOT / "data" / "analysis_exclusions.json").read_text())
    ids = set()
    for e in doc["exclusions"]:
        for field in ("id", "kind", "summary", "from", "operator", "routes", "area",
                      "note", "source_url", "recorded_on"):
            assert e.get(field), f"{e.get('id')}: no {field}"
        assert e["id"] not in ids
        ids.add(e["id"])
        start = datetime.fromisoformat(e["from"])
        assert start.tzinfo is not None
        if e.get("to"):
            assert datetime.fromisoformat(e["to"]) > start
        assert len(e["area"]) >= 3 and all(len(p) == 2 for p in e["area"])
        assert e["source_url"].startswith("https://")
    recorded = {e.get("disruption_id") for e in doc["exclusions"]}
    notices = json.loads((ROOT / "data" / "disruptions.json").read_text())["disruptions"]
    for n in notices:
        if n.get("record_for_analysis"):
            assert n["id"] in recorded, f"{n['id']} is flagged but has no permanent record"
