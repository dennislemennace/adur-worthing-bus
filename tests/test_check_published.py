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


def test_a_journey_calling_at_one_stop_twice_is_counted_not_refused():
    # 843 trips in this timetable call at one stop more than once: circular
    # services and estate loops are an ordinary route shape. This check began
    # by refusing them and blocked a publish over three late-night journeys on
    # the 5, 5B and 46 — the same mistake as failing a zero-length duration,
    # which is confusing "unusual" with "impossible".
    #
    # The browser drops such a journey only from pairs naming the repeated
    # stop, where it genuinely cannot tell which visit is meant.
    loop = doc([journey([[0, 30600, 30600, 0], [2, 31200, 31200, 0],
                         [0, 33000, 33000, 0]])])
    fails = cp.Failures()
    cp.check_journey_document(loop, "700", fails)
    assert [c for c, _ in fails.items] == [], \
        f"a circular route blocked publishing: {fails.items}"
    assert "journeys calling at one stop more than once" in fails.counts


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
            # A real day has journeys scheduled and services measured. Both sit
            # in the fixture because their absence is now a failure in its own
            # right — see the empty-day tests at the end of this file.
            # Plain band counts, which is the shape a daily summary really
            # publishes. A cell carrying `observations` would be read as a
            # statistics cell and held to the measured-only rule as well.
            "by_service": {"700 (SCSO)": {"early": 2, "on_time": 6,
                                          "late": 1, "very_late": 1}},
            "coverage": {"snapshots_by_hour": {"10": 60},
                         "snapshots": 1170, "scheduled_journeys": 12}}
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


# ── A day that measured nothing ─────────────────────────────
#
# The gap these close is the one that let two real days through. Every other
# check in this file compares one published number against another, so a file in
# which every number is zero satisfies all of them at once: the bands sum to
# zero, which equals zero observations, and nothing disagrees with anything.
#
# 18 and 19 September 2026 were published exactly that way — about 1,170
# snapshots each, zero observations, zero scheduled journeys, an empty
# by_service — and `check_published` passed them both.


def test_a_day_with_no_observations_is_a_failure():
    caught = failures(cp.check_summary, summary(
        observations=0, bands={"early": 0, "on_time": 0, "late": 0,
                               "very_late": 0}), "2026-09-18")
    assert "a day published no observations at all" in caught


def test_the_failure_says_which_of_the_two_causes_it_was():
    # Zero observations *with* journeys scheduled is a broken matcher. Zero of
    # both is a day measured against a timetable that does not cover it. They
    # are fixed in different files, so the message has to tell them apart —
    # finding that out by hand took an hour.
    fails = cp.Failures()
    cp.check_summary(summary(observations=0,
                             bands={"early": 0, "on_time": 0, "late": 0,
                                    "very_late": 0},
                             coverage={"snapshots_by_hour": {"10": 60},
                                       "snapshots": 1170,
                                       "scheduled_journeys": 0}),
                     "2026-09-18", fails)
    detail = " ".join(d for _c, d in fails.items)
    assert "timetable does not cover this day" in detail

    fails = cp.Failures()
    cp.check_summary(summary(observations=0,
                             bands={"early": 0, "on_time": 0, "late": 0,
                                    "very_late": 0},
                             coverage={"snapshots_by_hour": {"10": 60},
                                       "snapshots": 1170,
                                       "scheduled_journeys": 2698}),
                     "2026-09-18", fails)
    detail = " ".join(d for _c, d in fails.items)
    assert "matcher found nothing" in detail


def test_observations_against_nothing_scheduled_is_a_failure():
    # The other way round, and just as wrong: arrivals measured on a day the
    # timetable says has no service means the two were never comparable.
    assert "a day published no scheduled journeys" in failures(
        cp.check_summary, summary(coverage={"snapshots_by_hour": {"10": 60},
                                            "scheduled_journeys": 0}),
        "2026-09-18")


def test_a_summary_with_no_per_service_figures_is_a_failure():
    assert "summary has no per-service figures" in failures(
        cp.check_summary, summary(by_service={}), "2026-09-18")


def test_a_rollup_is_held_to_the_per_service_rule_too():
    # A rollup carries no top-level `observations`, so the day checks above do
    # not apply to it and this is the only one that does.
    rollup = {"month": "2026-09", "days": ["2026-09-16"], "method": "M",
              "caveats": ["c"], "as_of": "2026-09-20T08:00:00+00:00",
              "data_versions": ["v1"], "method_version": 3,
              "measured_only": True, "by_service": {}}
    assert "summary has no per-service figures" in failures(
        cp.check_summary, rollup, "2026-09")


def test_a_full_day_is_not_flagged():
    # The guard must not fire on the days that worked, or it gets switched off.
    assert failures(cp.check_summary, summary(), "2026-09-17") == []


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
