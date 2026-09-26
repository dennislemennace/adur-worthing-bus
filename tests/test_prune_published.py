"""Retiring old published generations without touching the ones that matter.

The public bucket gains a whole generation every night and would stop
publication within weeks of the 35-day window filling. The retention rule keeps
the newest week and, whatever their age, the live generation and its rollback:
deleting either would break the site or the only way back from a bad night.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import prune_published as pp                                     # noqa: E402

B = [f"{n:x}" * 64 for n in range(1, 13)]                          # 12 build ids
B = [b[:64] for b in B]


def listing(*builds):
    """An `aws s3 ls --recursive` listing, oldest first, one day apart."""
    lines = []
    for day, build in enumerate(builds, start=1):
        for name in ("index.json", "700-SCSO.json"):
            lines.append(f"2026-10-{day:02d} 03:40:0{len(lines) % 10}     1234 journey-times/builds/{build}/{name}")
    return "\n".join(lines) + "\n"


def index(live, previous=None):
    return {"build_id": live, "previous_build": previous,
            "rollback_index": f"journey-times/builds/{live}/previous-index.json" if previous else None}


def test_the_newest_seven_are_kept_and_the_rest_retired():
    doomed = pp.to_delete(listing(*B[:10]), index(B[9], B[8]), keep=7)
    assert doomed == sorted(B[:3])


def test_the_live_generation_and_its_rollback_survive_however_old():
    # The pointer can lag: a night that uploaded but never switched leaves the
    # live build older than newer, unused ones.
    doomed = pp.to_delete(listing(*B[:10]), index(B[0], B[1]), keep=7)
    assert B[0] not in doomed and B[1] not in doomed
    assert doomed == [B[2]]


def test_nothing_goes_while_there_are_no_more_than_seven():
    assert pp.to_delete(listing(*B[:7]), index(B[6], B[5]), keep=7) == []


def test_lines_that_are_not_builds_are_never_deleted():
    text = listing(*B[:9]) + "2026-09-01 00:00:00  10 journey-times/index.json\n" \
           "2026-09-01 00:00:00  10 journey-times/builds/not-a-build/index.json\n"
    doomed = pp.to_delete(text, index(B[8], B[7]), keep=7)
    assert doomed == sorted(B[:2])


def test_it_refuses_to_keep_fewer_than_the_live_one_and_its_rollback():
    with pytest.raises(ValueError):
        pp.to_delete(listing(*B[:3]), index(B[2]), keep=1)


def test_an_index_naming_no_build_deletes_nothing(tmp_path, capsys):
    (tmp_path / "l.txt").write_text(listing(*B[:10]))
    (tmp_path / "i.json").write_text("{}")
    assert pp.main(["--listing", str(tmp_path / "l.txt"), "--index", str(tmp_path / "i.json")]) == 1
    assert capsys.readouterr().out == ""
