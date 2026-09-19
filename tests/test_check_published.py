"""The check that reads published files rather than fixtures.

This exists because the suite was green while the site published 69 journey
times in which a bus arrived before it set off. Every test in the repo asserted
something about the code; none asserted anything about the output, and the
defect lived in the gap — a route coming back past one of its own early stops,
which no fixture described.

So each test here feeds `check_published` a document with one specific fault and
insists it is caught. A checker that cannot fail is worse than no checker: it is
a green tick over an unread file.
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import check_published as cp                                      # noqa: E402


def doc(journeys, stops=4):
    return {"service": "700",
            "stops": [{"atco": f"A{i}", "name": f"Stop {i}"} for i in range(stops)],
            "journeys": journeys}


def journey(calls, day="2026-09-17", start="08:15"):
    return {"day": day, "start": start, "direction": "westbound", "calls": calls}


def failures(fn, *args):
    fails = cp.Failures()
    fn(*args, fails)
    return [check for check, _detail in fails.items]


# ── Journeys that could not have happened ───────────────────

def test_a_negative_journey_time_is_caught():
    # The one that reached readers: the scheduled order advances and the
    # observed order does not, so subtracting gives a bus arriving before it
    # left. Sixty-nine of these were published, the worst -12 minutes.
    bad = doc([journey([[0, 30600, 30600, 0], [2, 29700, 31800, 0]])])
    assert "journey time is negative" in failures(
        cp.check_journey_document, bad, "700")


def test_a_journey_that_holds_together_passes():
    good = doc([journey([[0, 30600, 30600, 0], [2, 31800, 31800, 0],
                         [3, 32400, 32400, 0]])])
    assert failures(cp.check_journey_document, good, "700") == []


def test_a_zero_length_journey_time_is_noted_not_failed():
    # Two stops covered inside one reporting interval. The feed reports every
    # few minutes, so this is a limit of the evidence rather than an error, and
    # the first version of this script failed 1,327 documents on it — of which
    # 1,325 were exactly this. A check that cannot tell "impossible" from
    # "below our resolution" is one that gets switched off.
    same = doc([journey([[0, 30600, 30600, 0], [2, 30600, 31800, 0]])])
    fails = cp.Failures()
    cp.check_journey_document(same, "700", fails)
    assert fails.items == [], "a zero-length journey time blocked publishing"
    assert sum(fails.counts.values()) == 1, "and it was not reported either"


def test_two_stops_sharing_a_scheduled_minute_are_not_a_fault():
    # Stops metres apart share a timetable minute. There is no order to
    # violate, and flagging it would fire on every real service.
    same = doc([journey([[0, 30600, 30600, 0], [1, 30540, 30600, 0]])])
    assert failures(cp.check_journey_document, same, "700") == []


def test_a_journey_calling_at_one_stop_twice_is_reported():
    # The browser cannot tell which visit a reader means, so it drops the
    # journey. Published unremarked, that is data lost in silence.
    loop = doc([journey([[0, 30600, 30600, 0], [2, 31200, 31200, 0],
                         [0, 33000, 33000, 0]])])
    assert "journey calls at one stop more than once" in failures(
        cp.check_journey_document, loop, "700")


def test_a_call_naming_a_stop_that_is_not_in_the_list_is_caught():
    # The index into `stops` is how the browser resolves a name. Out of range
    # renders "undefined" to a reader, which looks like our mistake because it
    # is.
    bad = doc([journey([[0, 30600, 30600, 0], [9, 31800, 31800, 0]])], stops=4)
    assert "call names a stop not in the stops list" in failures(
        cp.check_journey_document, bad, "700")


# ── Summaries that do not say what they rest on ─────────────

def summary(**over):
    base = {"day": "2026-09-17", "method": "M", "caveats": ["c"],
            "as_of": "2026-09-18T02:00:00+00:00", "data_version": "v1",
            "method_version": 3, "measured_only": True, "observations": 10,
            "bands": {"early": 2, "on_time": 6, "late": 1, "very_late": 1},
            "coverage": {"snapshots_by_hour": {"10": 60}}}
    base.update(over)
    return base


def test_a_clean_summary_passes():
    assert failures(cp.check_summary, summary(), "day") == []


def test_a_summary_that_does_not_exclude_estimates_is_caught():
    assert "summary does not state that estimates were excluded" in failures(
        cp.check_summary, summary(measured_only=None), "day")


def test_bands_that_do_not_add_up_are_caught():
    # The signature of a filter applied in one place and not another — which is
    # exactly how estimates came to be counted in one summary and not another.
    assert "band counts do not add up to the observations" in failures(
        cp.check_summary, summary(observations=99), "day")


def test_a_daily_summary_with_no_coverage_by_hour_is_caught():
    assert "daily summary does not report coverage by hour" in failures(
        cp.check_summary, summary(coverage={}), "day")


def test_a_rollup_may_name_several_data_versions():
    # A daily summary names one timetable build; a rollup spans several and
    # names each. Demanding the singular field would fail every rollup.
    rollup = summary(month="2026-09", data_version=None,
                     data_versions=["v1", "v2"], coverage={})
    rollup.pop("day")
    assert failures(cp.check_summary, rollup, "2026-09") == []


def test_a_summary_with_no_provenance_at_all_is_caught():
    bare = summary(method=None, as_of=None, data_version=None, method_version=None)
    found = failures(cp.check_summary, bare, "day")
    assert found.count("summary is missing its provenance") == 4


def test_a_figure_must_say_which_method_produced_it():
    # data_version says what a figure was measured against; method_version says
    # how. Without it a figure from before the arrival picker was made monotonic
    # is indistinguishable from one after, and on the journeys that were wrong
    # those answers differ by up to an hour.
    assert "summary is missing its provenance" in failures(
        cp.check_summary, summary(method_version=None), "day")
    assert failures(cp.check_summary, summary(method_version=3), "day") == []


# ── Cells published as fact ─────────────────────────────────

def cell(**over):
    base = {"observations": 40, "journeys": 8, "median_secs": 60,
            "p90_secs": 300, "measured_only": True}
    base.update(over)
    return base


def test_a_percentile_below_the_floor_is_caught():
    # "9 in 10 journeys" over two journeys is arithmetic wearing the clothes of
    # evidence.
    assert "percentile published below the observation floor" in failures(
        cp.check_cells, {"700": cell(observations=4)}, "by_service")
    assert "percentile published below the journey floor" in failures(
        cp.check_cells, {"700": cell(journeys=1)}, "by_service")


def test_a_cell_is_judged_against_the_floor_its_own_file_declares():
    # The monthly rollup uses 20 arrivals and 20 journeys where the module
    # defaults to 30 and 5 — looser on one, four times stricter on the other,
    # and it defeats the reason MIN_OBS exists, since 30 arrivals can be one
    # journey's worth of stops and 20 journeys cannot. Judging that trade is not
    # this script's business. A file contradicting its own stated floor is.
    fails = cp.Failures()
    cp.check_cells({"700": cell(observations=22, journeys=20)}, "by_service",
                   fails, min_obs=20, min_journeys=20)
    assert fails.items == [], "a cell meeting the file's declared floor was failed"

    fails = cp.Failures()
    cp.check_cells({"700": cell(observations=9, journeys=20)}, "by_service",
                   fails, min_obs=20, min_journeys=20)
    assert [c for c, _ in fails.items] == [
        "percentile published below the observation floor"]


def test_a_cell_that_counted_estimates_is_caught():
    assert "published cell counted interpolated times as measured" in failures(
        cp.check_cells, {"700": cell(measured_only=None)}, "by_service")


def test_plain_band_counts_are_not_mistaken_for_statistics():
    # The daily summary publishes bare counts under the same key names. They
    # carry no denominators and make no percentile claim, so demanding
    # `measured_only` of them would fire on every correct file.
    counts = {"700": {"early": 2, "on_time": 6, "late": 1, "very_late": 0}}
    assert failures(cp.check_cells, counts, "by_service") == []


# ── The index and the files it names ────────────────────────

def test_an_index_naming_a_missing_file_is_caught(tmp_path):
    # Publishing the index before the documents is how a reader gets half a
    # dataset: the page fetches a service that is not there and shows nothing,
    # with no error anyone sees.
    (tmp_path / "700.json").write_text("{}", encoding="utf-8")
    index = {"services": [{"service": "700", "file": "700.json"},
                          {"service": "37", "file": "37.json"}]}
    assert failures(cp.check_index, index, tmp_path) == [
        "index names a file that was not published"]


# ── The script as a whole ───────────────────────────────────

def test_it_exits_non_zero_so_the_action_stops(tmp_path):
    out = tmp_path / "journey-times"
    out.mkdir()
    (out / "700.json").write_text(json.dumps(
        doc([journey([[0, 30600, 30600, 0], [2, 29700, 31800, 0]])])),
        encoding="utf-8")
    assert cp.main(["--journey-times", str(out)]) == 1


def test_it_exits_zero_on_clean_output(tmp_path):
    out = tmp_path / "journey-times"
    out.mkdir()
    (out / "700.json").write_text(json.dumps(
        doc([journey([[0, 30600, 30600, 0], [2, 31800, 31800, 0]])])),
        encoding="utf-8")
    assert cp.main(["--journey-times", str(out), "--quiet"]) == 0


def test_finding_nothing_to_check_is_itself_a_failure(tmp_path):
    # A path typo in the workflow would otherwise read as a clean run, which is
    # the most dangerous possible result: a green tick over an unread file.
    empty = tmp_path / "nothing"
    empty.mkdir()
    assert cp.main(["--journey-times", str(empty)]) == 1
