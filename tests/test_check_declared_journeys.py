"""scripts/check_declared_journeys.py: noticing an operator's timetable has moved
on without ours, as Stagecoach's did on 7 October 2026."""

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import check_declared_journeys as cdj                              # noqa: E402

FLAG = cdj.FLAG


def day(operators, by_start=None):
    """`{operator: (journeys, unresolved)}` as observation rows, two stops each.
    `by_start`: `{operator: n}` of the unresolved declared by their start."""
    rows = []
    for op, (n, bad) in operators.items():
        starts = (by_start or {}).get(op, 0)
        for i in range(n):
            row = {"day": "2026-10-07", "operator": op, "trip_id": f"{op}{i}", "quality_flags": []}
            if i < bad - starts:
                row["quality_flags"] = [FLAG]
            elif i < bad:
                row.update(match="declared", declared_by="start")
            rows += [row] * 2
    return {"day": "2026-10-07", "observations": rows}


def test_the_seventh_of_october_names_stagecoach_only():
    doc = day({"SCSO": (619, 606), "BHBC": (3548, 157), "COMT": (203, 0)})
    assert cdj.stale_operators(doc) == ["SCSO"]
    assert cdj.unresolved_by_operator(doc)["SCSO"] == (619, 606), "a journey is counted once, not per stop"


def test_journeys_declared_by_their_start_still_count():
    # Method 7 declares most of Stagecoach's journeys by their start, so they
    # carry no flag. GTFS-RT still gave no id we hold, which is the signal.
    doc = day({"SCSO": (619, 606)}, by_start={"SCSO": 590})
    assert cdj.unresolved_by_operator(doc)["SCSO"] == (619, 606)
    assert cdj.stale_operators(doc) == ["SCSO"]


def test_the_threshold_and_the_floor():
    assert cdj.stale_operators(day({"X": (20, 6)})) == ["X"], "30% of 20"
    assert cdj.stale_operators(day({"X": (20, 5)})) == []
    assert cdj.stale_operators(day({"X": (19, 19)})) == [], "too few journeys to say"


def test_the_run_is_warned_and_not_failed(tmp_path, capsys):
    import gzip, json
    obs = tmp_path / "observations-2026-10-07.json.gz"
    obs.write_bytes(gzip.compress(json.dumps(day({"SCSO": (40, 39), "BHBC": (100, 1)})).encode()))
    assert cdj.main([str(obs)]) == 0
    out = capsys.readouterr().out
    assert "::warning::SCSO: 39 of 40 journeys" in out
    assert "BODS" in out, "the warning must say where to look"
    assert "::warning::BHBC" not in out


def test_the_nightly_run_checks_and_does_not_rebuild():
    # A rebuild was triggered here at first. While BODS lacked Stagecoach
    # South's timetable, every one would have failed validation, nightly.
    workflow = yaml.safe_load((ROOT / ".github/workflows/process-snapshots.yml").read_text())
    steps = workflow["jobs"]["process"]["steps"]
    names = [s.get("name", "") for s in steps]
    check = names.index("Check every operator's journeys are in our timetable")
    assert check == names.index("Measure the day") + 1
    assert "check_declared_journeys.py" in steps[check]["run"]
    assert not any("update-timetable.yml" in (s.get("run") or "") for s in steps)
