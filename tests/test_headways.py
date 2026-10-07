"""scripts/build_headways.py: waiting, bunching and punctuality against the
Traffic Commissioners' standard, on synthetic days whose answers are known."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import build_headways as bh                                       # noqa: E402
import analysis_exclusions as ax                                  # noqa: E402
from observation_contract import service_origin                   # noqa: E402

DAY = "2026-10-01"          # a Thursday
ATCOS = ("A", "B", "C", "D", "E", "F")
OFFSETS = (0, 300, 600, 900, 1200, 1500)


def day_of(starts, svc="7", op="BHBC", direction="eastbound"):
    """A recorded timetable: one pattern A..F, every call a timing point."""
    return {"patterns": {"p": {"atcos": list(ATCOS), "operator": op, "service": svc, "direction": direction}},
            "profiles": [{"offsets": list(OFFSETS), "timepoints": [1] * len(ATCOS)}],
            "trips": [[f"T{s}", "p", 0, s, "F"] for s in starts]}


def call(trip_start, index, late=0, day=DAY, svc="7", op="BHBC", direction="eastbound",
         estimated=False, match="declared", interval=60, flags=()):
    origin = service_origin(day)
    scheduled = trip_start + OFFSETS[index]
    observed = scheduled + late
    return {"day": day, "operator": op, "service": svc, "direction": direction, "atco": ATCOS[index],
            "trip_id": f"T{trip_start}", "timepoint": 1, "stop_index": index, "stop_name": ATCOS[index],
            "observed_secs": observed, "observed_epoch": origin + observed, "scheduled_secs": scheduled,
            "observed_interval_epoch": [origin + observed, origin + observed + interval] if interval else None,
            "estimated": estimated, "match": match, "quality_flags": list(flags), "lateness_secs": late}


def run(schedules, rows, exclusions=()):
    meta = {"schedules": schedules, "days": sorted(schedules), "data_versions": ["t"], "method_versions": [6]}
    return bh.build(rows, meta, exclusions=exclusions, as_of="2026-10-07T00:00:00+00:00")


def cell(docs, atco="A", period="any", kind="frequent", svc="7", op="BHBC"):
    found = [c for c in docs[(op, svc)]["cells"]
             if c["atco"] == atco and c["period"] == period and c["kind"] == kind]
    assert len(found) == 1, found
    return found[0]


EVERY_5 = [7 * 3600 + i * 300 for i in range(36)]       # 07:00 to 09:55, 12 an hour


def test_buses_exactly_on_their_timetable_add_no_wait():
    rows = [call(s, i) for s in EVERY_5 for i in range(len(ATCOS))]
    c = cell(run({DAY: day_of(EVERY_5)}, rows))
    assert c["swt_secs"] == 150 and c["awt_secs"] == 150 and c["ewt_secs"] == 0
    assert (c["days_eligible"], c["days_incomplete"]) == (1, 0)
    assert (c["hours_tested"], c["hours_six_plus"], c["gaps_over_15"]) == (3, 3, 0)
    assert c["bunched_definite"] == c["bunched_possible"] == 0


def test_paired_buses_on_a_ten_minute_timetable_add_about_five_minutes():
    """Every second bus runs with the one after it: a bus every 20 min in
    practice. Passengers turning up from 07:00 to the last bus at 09:55: by the
    timetable the first wait is to 07:05, then a bus every 10 min, (5 x 2.5 +
    170 x 5) / 175 min = 296 s; recorded, the first is at 07:15, then one every
    20, (15 x 7.5 + 160 x 10) / 175 = 587 s."""
    starts = [7 * 3600 + i * 600 for i in range(18)]
    rows = []
    for k, s in enumerate(starts):
        late = 600 if k % 2 == 0 else 0
        rows += [call(s, i, late=late if i else 0) for i in range(len(ATCOS))]
    docs = run({DAY: day_of(starts)}, rows)
    at_b = cell(docs, atco="B")
    assert (at_b["swt_secs"], at_b["awt_secs"], at_b["ewt_secs"]) == (296, 587, 291)
    # Together at B, but they left A ten minutes apart: closed up on the way.
    # Nine pairs; the last leaves at 09:55, when the window closes.
    assert at_b["bunched_definite"] == 8
    assert (at_b["closed_up"], at_b["left_together"]) == (8, 0)
    at_a = cell(docs, atco="A")
    assert at_a["ewt_secs"] == 0 and at_a["bunched_definite"] == 0


def test_buses_that_left_together_are_told_from_ones_that_closed_up():
    starts = [7 * 3600 + i * 600 for i in range(18)]
    rows = []
    for k, s in enumerate(starts):
        late = 600 if k % 2 == 0 else 0
        rows += [call(s, i, late=late) for i in range(len(ATCOS))]     # together from the start
    at_b = cell(run({DAY: day_of(starts)}, rows), atco="B")
    assert (at_b["bunched_definite"], at_b["left_together"], at_b["closed_up"]) == (8, 8, 0)


def test_ninety_seconds_apart_by_the_reports_is_possible_not_definite():
    """Reports arrive about a minute apart: two buses 90 s apart could be up to
    150 s apart by their report bounds, so they are possibly together only."""
    starts = [7 * 3600 + i * 600 for i in range(18)]
    rows = []
    for k, s in enumerate(starts):
        late = 510 if k % 2 == 0 else 0
        rows += [call(s, i, late=late if i else 0, interval=60) for i in range(len(ATCOS))]
    at_b = cell(run({DAY: day_of(starts)}, rows), atco="B")
    assert at_b["bunched_possible"] == 9
    assert at_b["bunched_definite"] == 0


def test_two_untracked_buses_in_thirty_six_leave_the_day_out_of_waiting():
    """34 of 36 is 94%: under the 95% bar, so no false gap reaches waiting or
    the gap test."""
    rows = [call(s, i) for n, s in enumerate(EVERY_5) for i in range(len(ATCOS)) if n not in (10, 20)]
    c = cell(run({DAY: day_of(EVERY_5)}, rows))
    assert (c["days_eligible"], c["days_incomplete"]) == (0, 1)
    assert c["ewt_secs"] is None and c["gaps_tested"] == 0
    assert (c["scheduled_passages"], c["accounted_passages"]) == (36, 34)


def test_a_bus_seen_either_side_of_a_stop_still_counts_as_passing_it():
    rows = [call(s, i, estimated=(n == 10 and i == 2)) for n, s in enumerate(EVERY_5) for i in range(len(ATCOS))]
    c = cell(run({DAY: day_of(EVERY_5)}, rows), atco="C")
    assert c["days_eligible"] == 1 and c["accounted_passages"] == c["scheduled_passages"] == 36
    assert c["judged"] == 35, "an interpolated time is never judged for punctuality"


def test_ten_minutes_apart_is_frequent_and_eleven_is_not():
    ten = [7 * 3600 + i * 600 for i in range(18)]
    eleven = [7 * 3600 + i * 660 for i in range(16)]
    assert cell(run({DAY: day_of(ten)}, [call(s, 0) for s in ten]), kind="frequent")
    assert cell(run({DAY: day_of(eleven)}, [call(s, 0) for s in eleven]), kind="non_frequent")


def test_the_hourly_tests_catch_a_late_bus_that_leaves_a_gap():
    """A 10-minute service where the 07:50 runs 15 min late: 07:00-08:00 has
    five departures, and 07:40 to 08:00 is a 20-minute gap."""
    starts = [7 * 3600 + i * 600 for i in range(18)]
    rows = [call(s, 0, late=900 if s == 7 * 3600 + 3000 else 0) for s in starts]
    c = cell(run({DAY: day_of(starts)}, rows))
    assert (c["hours_tested"], c["hours_six_plus"]) == (3, 2)
    assert c["gaps_over_15"] == 1


def test_on_time_runs_from_one_minute_early_to_five_fifty_nine_late():
    starts = [7 * 3600 + i * 1800 for i in range(6)]
    lates = [-61, -60, 0, 359, 360, 1000]
    rows = [call(s, 0, late=late) for s, late in zip(starts, lates)]
    c = cell(run({DAY: day_of(starts)}, rows), kind="non_frequent")
    assert (c["judged"], c["early"], c["on_time"], c["late"]) == (6, 1, 3, 2)


def test_only_declared_measured_departures_without_flags_are_judged():
    starts = [7 * 3600 + i * 1800 for i in range(4)]
    rows = [call(starts[0], 0), call(starts[1], 0, match="inferred"),
            call(starts[2], 0, flags=["unbounded_departure"]), call(starts[3], 0, estimated=True)]
    c = cell(run({DAY: day_of(starts)}, rows), kind="non_frequent")
    assert c["judged"] == 1 and c["accounted_passages"] == 4


def test_late_from_the_start_and_late_on_the_way_are_told_apart():
    """Judged at F, the sixth call. X was 7 min late at A already; Y left A on
    time and lost it on the way; Z was first seen at D, too far in to say."""
    x, y, z = 7 * 3600, 7 * 3600 + 1800, 7 * 3600 + 3600
    rows = [call(x, 0, late=420), call(x, 5, late=420),
            call(y, 0, late=0), call(y, 5, late=420),
            call(z, 3, late=420), call(z, 5, late=420)]
    c = cell(run({DAY: day_of([x, y, z])}, rows), atco="F", kind="non_frequent")
    assert (c["late_here"], c["late_from_start"], c["late_on_the_way"], c["late_start_unknown"]) == (3, 1, 1, 1)


def test_first_and_last_buses_are_compared_with_the_rest():
    starts = [6 * 3600 + i * 1800 for i in range(6)]
    rows = [call(starts[0], i, late=600) for i in range(len(ATCOS))]          # the first: late everywhere
    rows += [call(s, i) for s in starts[1:-1] for i in range(len(ATCOS))]      # the last: never tracked
    groups = {g["group"]: g for g in run({DAY: day_of(starts)}, rows)[("BHBC", "7")]["groups"]}
    assert (groups["first"]["scheduled"], groups["first"]["tracked"], groups["first"]["late"]) == (1, 1, 6)
    assert (groups["last"]["scheduled"], groups["last"]["tracked"], groups["last"]["judged"]) == (1, 0, 0)
    assert (groups["day"]["scheduled"], groups["day"]["on_time"]) == (4, 24)


def test_a_recorded_disruption_marks_the_cells_it_touches(tmp_path):
    register = tmp_path / "exclusions.json"
    register.write_text(json.dumps({"exclusions": [{
        "id": "closure", "operator": "BHBC", "routes": ["7"], "stops_not_served": ["A"],
        "from": "2026-10-01T06:00:00+01:00", "to": "2026-10-01T12:00:00+01:00"}]}))
    rows = [call(s, i) for s in EVERY_5 for i in range(len(ATCOS))]
    docs = run({DAY: day_of(EVERY_5)}, rows, exclusions=ax.load(register))
    assert cell(docs)["excluded_by"] == ["closure"]
    assert "excluded_by" not in cell(docs, atco="B")


def test_floors_and_the_document_carry_their_method():
    days = [f"2026-10-{d:02d}" for d in (1, 2, 5, 6, 7)]          # five weekdays
    schedules = {d: day_of(EVERY_5) for d in days}
    rows = [call(s, i, day=d) for d in days for s in EVERY_5 for i in range(len(ATCOS))]
    assert cell(run(schedules, rows))["sample_sufficient"] is True
    four = run({d: schedules[d] for d in days[:4]}, [r for r in rows if r["day"] in days[:4]])
    assert cell(four)["sample_sufficient"] is False
    doc = run(schedules, rows)[("BHBC", "7")]
    assert doc["method_version"] == bh.METHOD_VERSION and doc["caveats"] and doc["as_of"]
    assert doc["thresholds"]["frequent_max_gap_secs"] == 600
    assert bh.file_name("N700", "SCSO") == "headways-N700-SCSO.json"


def test_a_bus_matched_hours_late_moves_no_gap():
    """On 1 October 2026 a route 1 bus due at 15:27 was recorded at 20:00,
    almost certainly another journey's bus. Placed by its scheduled time it
    opened a four-hour gap and put Excess Wait Time at sixteen minutes.
    Passages fall where they were seen, so it lands among the late buses."""
    starts = [10 * 3600 + i * 300 for i in range(72)] + [19 * 3600 + i * 900 for i in range(8)]
    rows = [call(s, 0, late=(8 * 3600 + 1800 if s == 12 * 3600 else 0)) for s in starts]
    docs = run({DAY: day_of(starts)}, rows)
    daytime = cell(docs, period="10-16")
    assert daytime["days_eligible"] == 1, "71 of 72 seen in the period is complete enough"
    assert daytime["ewt_secs"] < 30 and daytime["gaps_over_15"] == 0
    assert (daytime["judged"], daytime["late"]) == (72, 1), "it is still a late departure for its journey"


def test_periods_follow_simple_views_times_of_day():
    assert bh.period_of(7 * 3600 + 3599) == "early"
    assert bh.period_of(8 * 3600) == "08-10"
    assert bh.period_of(15 * 3600 + 3599) == "10-16"
    assert bh.period_of(17 * 3600) == "16-18"
    assert bh.period_of(18 * 3600) == "late"
    assert bh.period_of(25 * 3600) == "late", "past midnight is still the same late evening"
    assert bh.day_type("2026-10-03") == bh.day_type("2026-10-04") == "weekend"


def test_a_midnight_bus_and_the_night_without_buses_add_no_wait():
    """Route 7's timetable starts its day with a bus at 00:00, then nothing until
    05:12. Taken over the whole day that gap made the timetable's wait 37 min
    and the day's Excess Wait Time minus two. Waiting is measured only over
    hours with six or more buses due."""
    starts = [5] + EVERY_5
    rows = [call(s, i) for s in EVERY_5 for i in range(len(ATCOS))]      # the midnight bus is not seen
    c = cell(run({DAY: day_of(starts)}, rows))
    assert c["swt_secs"] == 150 and c["ewt_secs"] == 0
    assert c["days_eligible"] == 1


def test_published_files_are_checked_for_counts_that_add_up():
    import check_published as cp
    rows = [call(s, i) for s in EVERY_5 for i in range(len(ATCOS))]
    doc = run({DAY: day_of(EVERY_5)}, rows)[("BHBC", "7")]
    fails = cp.Failures()
    cp.check_headways(json.loads(json.dumps(doc)), "headways-7-BHBC.json", fails)
    assert not fails.items
    for change, message in ((lambda c: c.update(on_time=c["on_time"] + 1), "punctuality does not add up"),
                            (lambda c: c.update(sample_sufficient=True, days_eligible=1, kind="frequent"),
                             "claims sufficiency below the floor"),
                            (lambda c: c.update(bunched_definite=c.get("bunched_possible", 0) + 1, kind="frequent"),
                             "more definite than possible bunching")):
        bad = json.loads(json.dumps(doc))
        change(bad["cells"][0])
        fails = cp.Failures()
        cp.check_headways(bad, "headways-7-BHBC.json", fails)
        assert any(message in item[0] for item in fails.items), (message, fails.items)


def test_a_late_last_bus_still_counts_its_wait_and_its_gap():
    """Codex review: 24 buses every 5 min from 08:00, the 09:55 running 20 min
    late. Counting only gaps inside 08:00-10:00 hid the 09:50-10:15 wait. Those
    turning up from 09:50 wait to 10:15: (110 x 2.5 + 5 x 22.5) / 115 min."""
    starts = [8 * 3600 + i * 300 for i in range(24)]
    rows = [call(s, 0, late=1200 if s == starts[-1] else 0) for s in starts]
    c = cell(run({DAY: day_of(starts)}, rows), period="08-10")
    assert c["days_eligible"] == 1
    assert (c["swt_secs"], c["awt_secs"], c["ewt_secs"]) == (150, 202, 52)
    assert (c["gaps_over_15"], c["gaps_tested"]) == (1, 23)


def test_a_gap_the_timetable_has_is_not_a_failed_gap():
    starts = [10 * 3600 + i * 300 for i in range(12)] + [14 * 3600 + i * 300 for i in range(12)]
    c = cell(run({DAY: day_of(starts)}, [call(s, 0) for s in starts]))
    assert c["gaps_over_15"] == 0 and c["ewt_secs"] == 0


def test_definite_bunching_holds_whichever_bus_really_left_first():
    """Codex review: A reported at 07:00 but may have left any time to 07:05;
    B left between 07:01 and 07:02. A at 07:05 is three minutes from B, so the
    pair is possibly together, not definitely."""
    starts = [7 * 3600 + i * 600 for i in range(18)]
    rows = [call(s, 0, interval=(300 if s == starts[0] else 60), late=(-540 if s == starts[1] else 0))
            for s in starts]
    c = cell(run({DAY: day_of(starts)}, rows))
    assert c["bunched_possible"] == 1
    assert c["bunched_definite"] == 0


def test_a_loop_calling_twice_at_a_stop_is_two_passages():
    """Codex review: keyed without its place in the route, a loop's second
    call reused the first's time. Here every bus is on time at its first call
    to A and seven minutes late back at A."""
    starts = [7 * 3600 + i * 300 for i in range(12)]
    loop = {"patterns": {"p": {"atcos": ["A", "B", "A"], "operator": "BHBC", "service": "7",
                              "direction": "eastbound"}},
            "profiles": [{"offsets": [0, 600, 1200], "timepoints": [1, 1, 1]}],
            "trips": [[f"T{s}", "p", 0, s, "A"] for s in starts]}
    origin = service_origin(DAY)
    rows = []
    for s in starts:
        for index, (atco, offset, late) in enumerate((("A", 0, 0), ("B", 600, 0), ("A", 1200, 420))):
            observed = s + offset + late
            rows.append({"day": DAY, "operator": "BHBC", "service": "7", "direction": "eastbound",
                         "atco": atco, "trip_id": f"T{s}", "timepoint": 1, "stop_index": index,
                         "stop_name": atco, "observed_secs": observed, "observed_epoch": origin + observed,
                         "observed_interval_epoch": [origin + observed, origin + observed + 60],
                         "estimated": False, "match": "declared", "quality_flags": [], "lateness_secs": late})
    docs = run({DAY: loop}, rows)
    at_a = [c for c in docs[("BHBC", "7")]["cells"] if c["atco"] == "A" and c["period"] == "any"]
    assert sum(c["scheduled_passages"] for c in at_a) == sum(c["accounted_passages"] for c in at_a) == 24
    assert sum(c["judged"] for c in at_a) == 24
    assert sum(c["late"] for c in at_a) == 12, "the return visit's own lateness, not the first visit's"
