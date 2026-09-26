"""The compact wire form of the journey-times documents loses nothing.

The files are written compact because the 700's had reached a megabyte
compressed and grows by more every day. That is only a saving if every call a
reader's figures rest on comes back exactly, so the promise tested here is the
round trip, on a fixture built to hit every rule: an interval, an unbounded
one and none at all, a call whose flags differ from its journey's, a journey
past midnight, the day the clocks go back, and what the rules do not describe
(an epoch off its origin, an interval that does not open at its observation).

The browser's expansion is tested against the same fixture in
tests/test_journey_times.mjs, so the two readers cannot drift apart.
"""

import copy
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT))

import pytest                                                     # noqa: E402

import build_journey_times as bjt                                 # noqa: E402
import check_published as cp                                      # noqa: E402
import journey_times_codec as codec                               # noqa: E402
from test_build_journey_times import journey                      # noqa: E402

FIXTURE = json.loads((ROOT / "tests/fixtures/journey_times_codec.json").read_text(encoding="utf-8"))


def test_the_fixture_is_still_what_the_codec_writes():
    # If the encoding changes, this fails first, and the fixture the browser
    # test reads has to be regenerated on purpose rather than drift.
    assert codec.compact(FIXTURE["expanded"]) == FIXTURE["compact"]


def test_the_compact_fixture_expands_to_exactly_the_original():
    assert codec.expand(FIXTURE["compact"]) == FIXTURE["expanded"]


def test_the_fixture_exercises_every_rule():
    # A round trip over easy calls proves little. Pin down that the fixture
    # really contains each case the decoder has to get right.
    compact = FIXTURE["compact"]
    rows = [row for j in compact["journeys"] for row in j["calls"]]
    exceptions = [j.get("call_exceptions", {}) for j in compact["journeys"]]
    assert any(len(r) > 5 and r[5] == -1 for r in rows), "no unbounded interval"
    assert any(len(r) > 5 and r[5] is None for r in rows), "no call without an interval"
    assert any(len(r) > 6 for r in rows), "no call with its own flags"
    assert any(r[0] < 0 for r in rows), "no stop index stepping backwards"
    assert any(e.get("epochs") for e in exceptions), "no explicit epoch"
    assert any(e.get("intervals") for e in exceptions), "no explicit interval"
    assert any(c[1] >= 86_400 for j in FIXTURE["expanded"]["journeys"] for c in j["calls"]), \
        "no call past midnight"
    # 25 October 2026: the GTFS origin is noon less twelve hours, 00:00 UTC,
    # an hour after local midnight.
    assert compact["day_origins"]["2026-10-25"] == 1_792_886_400


def test_a_document_that_is_not_compact_is_returned_untouched():
    doc = copy.deepcopy(FIXTURE["expanded"])
    assert codec.expand(doc) is doc


def test_expanding_does_not_change_what_it_was_given():
    # The bundle checks a document and then publishes that same object.
    compact = copy.deepcopy(FIXTURE["compact"])
    codec.expand(compact)
    assert compact == FIXTURE["compact"]


@pytest.mark.parametrize("damage, message", [
    (lambda d: d.update(call_format=d["call_format"][:5]), "unknown call_format"),
    (lambda d: d["journeys"][0]["calls"][0].__setitem__(1, 85200.5), "not whole seconds"),
    (lambda d: d.update(encoding=codec.ENCODING), "already compact"),
    (lambda d: d["journeys"][0].update(call_tag=0), "already uses"),
])
def test_what_it_cannot_encode_exactly_it_refuses(damage, message):
    doc = copy.deepcopy(FIXTURE["expanded"])
    damage(doc)
    with pytest.raises(ValueError, match=message):
        codec.compact(doc)


def test_the_builder_writes_compact_files_that_expand_to_the_document(tmp_path):
    rows = []
    for n in range(3):
        rows += journey(f"SC{n}", "700", "SCSO", ["A1", "A2", "A3", "A4"], "Brighton")
    obs = tmp_path / "obs.json"
    obs.write_text(json.dumps({"day": "2026-09-17", "data_version": "v1",
                               "observations": rows}), encoding="utf-8")
    out = tmp_path / "out"
    assert bjt.main(["--observations", str(obs), "--out", str(out)]) == 0

    written = json.loads((out / "700-SCSO.json").read_text(encoding="utf-8"))
    assert codec.is_compact(written)
    # The document main() builds, read the way main() reads its input.
    loaded, meta = bjt.load_observations([str(obs)], compact=True, with_schedules=True)
    expected = bjt.build(loaded, meta)[("700", "SCSO")]
    assert codec.expand(written)["journeys"] == json.loads(json.dumps(expected["journeys"]))


def _failures(doc):
    fails = cp.Failures()
    cp.check_journey_document(copy.deepcopy(doc), "fixture", fails)
    return sorted(check for check, _detail in fails.items)


def test_a_compact_document_is_checked_as_the_browser_reads_it():
    assert _failures(FIXTURE["compact"]) == _failures(FIXTURE["expanded"])


def test_a_compact_document_that_does_not_expand_fails_the_check():
    broken = copy.deepcopy(FIXTURE["compact"])
    broken["journeys"][0]["call_tag"] = 99            # no such entry in tables.call_tags
    assert "compact document does not expand" in _failures(broken)
