"""scripts/build_identification.py: how each operator's buses were matched to
their journeys, published beside the figures that rest on it.

Since 7 October 2026 Stagecoach's live journey IDs are not in our timetable, so
nearly all of its journeys are identified by line, first stop and start time
instead. A reader of a Stagecoach figure should be able to see that.
"""

import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import build_identification as bi                                  # noqa: E402
import check_published as checks                                   # noqa: E402


def row(op, svc, trip, match="declared", by=None, estimated=False, day="2026-10-14"):
    r = {"day": day, "operator": op, "service": svc, "trip_id": trip, "match": match, "estimated": estimated}
    if by:
        r["declared_by"] = by
    return r


def doc(rows, cohorts=()):
    return {"day": "2026-10-14", "observations": rows, "coverage": {"cohorts": list(cohorts)}}


def test_a_journey_is_counted_once_by_the_strongest_way_it_was_known():
    counts = bi.count_day(doc([
        row("BHBC", "1", "a"), row("BHBC", "1", "a"),                         # two calls, one journey
        row("SCSO", "700", "b", by="start"), row("SCSO", "700", "b", by="start"),
        row("SCSO", "700", "c", match="inferred"),
        # Mixed: one call declared by start, one inferred. Known by start.
        row("SCSO", "700", "d", by="start"), row("SCSO", "700", "d", match="inferred"),
        # A journey ID beats everything else on the same journey.
        row("SCSO", "700", "e", match="inferred"), row("SCSO", "700", "e"),
    ]))
    assert counts[("BHBC", "1")]["by"] == {"journey_id": 1, "start": 0, "position": 0}
    assert counts[("SCSO", "700")]["by"] == {"journey_id": 1, "start": 2, "position": 1}
    assert counts[("SCSO", "700")]["tracked"] == 4


def test_only_measured_journeys_are_tracked_and_cohorts_give_the_denominator():
    counts = bi.count_day(doc(
        [row("SCSO", "700", "a"), row("SCSO", "700", "z", estimated=True)],
        cohorts=[{"operator": "SCSO", "service": "700", "day": "2026-10-14", "scheduled_in_recording_span": 9},
                 {"operator": "SCSO", "service": "700", "day": "2026-10-13", "scheduled_in_recording_span": 1}]))
    assert counts[("SCSO", "700")]["tracked"] == 1, "a journey with only interpolated calls was not seen"
    assert counts[("SCSO", "700")]["scheduled"] == 10


def test_one_operator_under_two_codes_is_one_operator():
    counts = bi.count_day(doc([row("SCSC", "700", "a"), row("CMPA", "8", "b")]))
    assert set(counts) == {("SCSO", "700"), ("COMT", "8")}


def test_the_window_adds_up_by_operator_service_and_day():
    day1 = ("2026-10-13", "h1", bi.count_day(doc([row("SCSO", "700", "a"), row("SCSO", "9", "b", by="start")],
        cohorts=[{"operator": "SCSO", "service": "700", "scheduled_in_recording_span": 3}])))
    day2 = ("2026-10-14", "h2", bi.count_day(doc([row("SCSO", "700", "a", by="start")])))
    out = bi.build([day2, day1], as_of="2026-10-15T03:30:00+00:00")
    assert out["days"] == ["2026-10-13", "2026-10-14"]
    assert out["operators"]["SCSO"] == {"scheduled": 3, "tracked": 3,
                                        "by": {"journey_id": 1, "start": 2, "position": 0}}
    assert out["services"]["700|SCSO"]["by"] == {"journey_id": 1, "start": 1, "position": 0}
    assert out["by_day"]["SCSO"]["2026-10-14"]["by"]["start"] == 1
    assert out["sources"] == [{"day": "2026-10-13", "sha256": "h1"}, {"day": "2026-10-14", "sha256": "h2"}]
    fails = checks.Failures()
    checks.check_identification(json.loads(json.dumps(out)), "identification.json", fails)
    assert not fails.items, fails.items


def test_the_check_refuses_counts_that_do_not_add_up():
    out = bi.build([("2026-10-14", "h", bi.count_day(doc([row("SCSO", "700", "a")])))])
    out["operators"]["SCSO"]["by"]["start"] = 5           # five more ways than journeys
    out["services"]["700|SCSO"]["tracked"] = -1
    del out["method"]
    fails = checks.Failures()
    checks.check_identification(out, "identification.json", fails)
    found = {check for check, _ in fails.items}
    assert "identification ways do not add up to the journeys tracked" in found
    assert "identification counts are not whole counts" in found
    assert "identification counts are missing their provenance" in found


def test_the_bundle_validates_it_and_the_nightly_run_publishes_it():
    source = (ROOT / "scripts/publication_bundle.py").read_text()
    assert 'name == "identification.json"' in source and "check_identification" in source
    workflow = yaml.safe_load((ROOT / ".github/workflows/process-snapshots.yml").read_text())
    runs = "\n".join(s.get("run") or "" for s in workflow["jobs"]["process"]["steps"])
    assert "build_identification.py --observations inputs/window --out identification.json" in runs
    assert '--artifact "identification.json=identification.json"' in runs
