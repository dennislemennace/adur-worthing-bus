"""The tool that answers "which service is worst, and where?"

`reliability_stats` is tested separately for the arithmetic. What matters here
is that the answers arrive with the things that stop them being misread: which
series they came from, how many journeys are behind them, which timetable build
produced them, and what the tool refuses to show.

The defaults carry most of that weight, so they are what these tests pin.
"""

import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import query_reliability as qr                                  # noqa: E402


def observation(trip="T1", service="700", stop="A", index=0, sched=36_000,
                late=0, timepoint=1, direction="westbound", day="2026-09-16"):
    return {"day": day, "trip_id": trip, "service": service, "operator": "SCSO",
            "vehicle": "1", "atco": f"4400{stop}", "stop_name": stop,
            "stop_index": index, "scheduled_secs": sched,
            "observed_secs": sched + late, "lateness_secs": late,
            "timepoint": timepoint, "direction": direction,
            "journey_start": "10:00", "nearest_m": 10, "samples": 9}


def write_day(directory, day, rows, version="timetable.sqlite sha256:abc123", gz=True):
    payload = {"day": day, "data_version": version, "observations": rows}
    path = Path(directory) / f"observations-{day}.json{'.gz' if gz else ''}"
    text = json.dumps(payload)
    if gz:
        path.write_bytes(gzip.compress(text.encode()))
    else:
        path.write_text(text)
    return path


def spread(service="700", journeys=6, stops=8, late=0, timepoint=1, day="2026-09-16"):
    rows = []
    for j in range(journeys):
        for i in range(stops):
            rows.append(observation(trip=f"{service}-{j}", service=service,
                                    stop=f"S{i}", index=i,
                                    sched=36_000 + i * 120 + j * 600,
                                    late=late, timepoint=timepoint, day=day))
    return rows


class Args:
    """The parsed options, with the CLI's own defaults."""
    def __init__(self, **over):
        self.by = "service"
        self.service = self.direction = self.day = None
        self.from_hour = self.to_hour = None
        self.all_stops = self.mean = self.segment_hours = self.json = False
        self.min, self.min_journeys, self.limit = 30, 5, 25
        self.sort = "worst"
        self.report = self.rollup = None
        self.observations = []
        self.__dict__.update(over)


META = {"days": ["2026-09-16"], "data_versions": ["sha256:abc"], "files": 1}


# ── Reading a day, or a week ────────────────────────────────

def test_days_are_read_from_a_directory_gzipped_or_not(tmp_path):
    write_day(tmp_path, "2026-09-16", spread())
    write_day(tmp_path, "2026-09-17", spread(day="2026-09-17"), gz=False)
    rows, meta = qr.load_observations([str(tmp_path)])
    assert len(rows) == 96
    assert meta["days"] == ["2026-09-16", "2026-09-17"]
    assert meta["files"] == 2


def test_the_timetable_builds_behind_a_figure_are_carried_through(tmp_path):
    # A week can span a rebuild. A figure that cannot name its data cannot be
    # checked, so both versions travel with the answer.
    write_day(tmp_path, "2026-09-16", spread(), version="sha256:aaa")
    write_day(tmp_path, "2026-09-17", spread(day="2026-09-17"), version="sha256:bbb")
    _rows, meta = qr.load_observations([str(tmp_path)])
    assert meta["data_versions"] == ["sha256:aaa", "sha256:bbb"]


# ── What the default series is ──────────────────────────────

def test_interpolated_stops_are_left_out_unless_asked_for():
    rows = spread(timepoint=1) + spread(service="2", timepoint=0)
    assert {r["service"] for r in qr.filtered(rows, Args())} == {"700"}
    assert {r["service"] for r in qr.filtered(rows, Args(all_stops=True))} == {"700", "2"}


def test_a_table_names_its_series_and_its_denominators():
    rows = spread(journeys=6, stops=8, late=0)
    text = "\n".join(qr.render_table(rows, Args()) + qr.provenance(rows, META, Args()))
    assert "timing points only" in text, "a reader cannot tell which stops were counted"
    assert "48 across 6 journeys" in text, "the journey denominator is missing"
    assert "sha256:abc" in text, "the figure does not say which timetable produced it"
    assert "BUS09" in text and "censored" in text


def test_hours_are_labelled_as_the_hour_due():
    text = "\n".join(qr.provenance(spread(), META, Args(by="hour")))
    assert "hour the bus was due" in text


def test_segments_are_labelled_as_the_hour_traversed():
    text = "\n".join(qr.provenance(spread(), META, Args(by="segment")))
    assert "hour traversed" in text


# ── What it refuses to show ─────────────────────────────────

def test_a_thin_cell_is_shown_as_suppressed_not_omitted():
    # Silently dropping it would hide how little the table rests on.
    rows = spread(journeys=6, stops=8) + spread(service="19", journeys=1, stops=4)
    text = "\n".join(qr.render_table(rows, Args()))
    assert "700" in text
    assert "19" not in text.split("suppressed")[0], "a four-arrival service was printed"
    assert "1 cell(s) suppressed" in text


def test_a_row_resting_on_the_matching_window_says_so():
    rows = spread(journeys=6, stops=8, late=-300)
    text = "\n".join(qr.render_table(rows, Args()))
    assert "matching window" in text and "floor" in text


def test_the_mean_only_appears_with_its_warning():
    rows = spread(journeys=6, stops=8, late=120)
    assert "mean" not in "\n".join(qr.render_table(rows, Args()))
    with_mean = "\n".join(qr.render_table(rows, Args(mean=True)))
    assert "mean" in with_mean and "censored" in with_mean


# ── End to end, including what it writes ────────────────────

def test_a_run_writes_a_report_and_a_rollup(tmp_path):
    write_day(tmp_path, "2026-09-16", spread(journeys=6, stops=8, late=420))
    report, rollup = tmp_path / "report.md", tmp_path / "2026-09.json"
    rc = qr.main(["--observations", str(tmp_path), "--by", "service",
                  "--report", str(report), "--rollup", str(rollup)])
    assert rc == 0
    text = report.read_text()
    assert "## Method" in text and "## Caveats" in text and "2026-09-16" in text
    doc = json.loads(rollup.read_text())
    for field in ("method", "caveats", "as_of", "data_versions", "series",
                  "hour_basis", "floor", "suppressed"):
        assert doc.get(field), f"the rollup publishes figures without {field}"
    assert doc["series"] == "timing_point"
    assert doc["by_service"]["700"]["journeys"] == 6
    assert doc["month"] == "2026-09"


def test_a_file_with_no_timing_points_explains_itself(tmp_path, capsys):
    # Days processed before the timing-point flag reached the pipeline carry no
    # flag at all. The default series then empties, which must not look like a
    # broken tool.
    write_day(tmp_path, "2026-09-16", spread(timepoint=None))
    rc = qr.main(["--observations", str(tmp_path)])
    assert rc == 1
    assert "timing point" in capsys.readouterr().err


def test_filters_narrow_to_what_was_asked_for():
    rows = spread(service="700") + spread(service="2")
    both = qr.filtered(rows, Args())
    assert {r["service"] for r in qr.filtered(rows, Args(service=["2"]))} == {"2"}
    assert len(qr.filtered(rows, Args(from_hour=11))) < len(both)
    assert qr.filtered(rows, Args(direction="eastbound")) == []


# ── Comparing like with like ────────────────────────────────

def test_the_same_hour_can_be_compared_across_days():
    rows = spread(day="2026-09-16") + spread(day="2026-09-17")
    cells = qr.rs.group_stats(rows, qr.KEYS["day-hour"][0])
    assert {k.split()[0] for k in cells} == {"2026-09-16", "2026-09-17"}


def test_comparison_rows_can_be_ordered_by_key_not_by_worst():
    # Worst-first interleaves the two days and makes a comparison unreadable.
    rows = spread(day="2026-09-16", late=600) + spread(day="2026-09-17", late=0)
    text = "\n".join(qr.render_table(rows, Args(by="day-hour", sort="key",
                                                min=8, min_journeys=6)))
    days = [line.split()[0] for line in text.splitlines()
            if line.strip().startswith("2026-")]
    assert days == sorted(days), f"rows are not in day order: {days}"
    worst_first = "\n".join(qr.render_table(rows, Args(by="day-hour", sort="worst",
                                                      min=8, min_journeys=6)))
    assert worst_first.splitlines()[2].split()[0] == "2026-09-16", \
        "worst-first no longer leads with the worse day"
