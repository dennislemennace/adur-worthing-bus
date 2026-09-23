"""Review regressions: service-day identity, route progression and evidence."""
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))

import process_snapshots as ps
from query_reliability import load_observations
from test_timetable_semantics import _windowed
from test_process_snapshots import tt, monkeypatch_module, DAY, west_xy
from test_gtfs_rt import a_feed, a_vehicle


def test_first_stop_uses_original_visit_even_when_return_pass_is_closer():
    stops = {"A": {"lat": 50.83, "lon": -.30},
             "B": {"lat": 50.83, "lon": -.29},
             "C": {"lat": 50.83, "lon": -.28}}
    samples = [(0, 50.8301, -.30, "bus"), (600, 50.83, -.29, "bus"),
               (1200, 50.83, -.28, "bus"), (1800, 50.83, -.30, "bus")]
    got = list(ps.arrivals_along(samples, [(0, "A"), (600, "B"), (1200, "C")], stops))
    assert [(r[0], samples[r[3]][0]) for r in got] == [(0, 0), (1, 600), (2, 1200)]


def test_legacy_midnight_rows_are_normalized_from_the_source_folder_day(tmp_path):
    p = tmp_path / "observations.json"
    p.write_text(json.dumps({"day": "2026-09-18", "method_version": 3,
        "data_version": "historical", "observations": [{"day": "2026-09-17",
        "trip_id": "night", "stop_index": 3, "scheduled_secs": 300,
        "observed_secs": 420, "journey_start_secs": -600}]}))
    rows, meta = load_observations([str(p)])
    assert rows[0]["scheduled_secs"] == 86700
    assert rows[0]["observed_secs"] == 86820
    assert rows[0]["journey_start_secs"] == 85800
    assert meta["method_versions"] == [3]
    assert rows[0]["source_file_sha256"]


def test_old_removal_exception_does_not_make_future_calendar_cover_old_day():
    tt = _windowed({"S": {"start_date": "20260920", "end_date": "20261231"}},
                   {"S": {"20260918": "2"}})
    assert not tt.covers_day(date(2026, 9, 18))


def test_rt_only_recording_produces_observations_with_source_evidence(tt, tmp_path):
    raw, rt = tmp_path / "raw" / DAY.isoformat(), tmp_path / "rt" / DAY.isoformat()
    raw.mkdir(parents=True); rt.mkdir(parents=True)
    for n, minute in enumerate((615, 616, 617, 618)):
        lat, lon = west_xy(15 + n)
        stamp = int(datetime(2026, 9, 16, 9, 15+n, tzinfo=timezone.utc).timestamp())
        (rt / f"{minute // 60:02d}{minute % 60:02d}.pb").write_bytes(
            a_feed([a_vehicle(trip=f"W{start}", vehicle=f"SCSO-{start}", lat=lat, lon=lon, stamp=stamp) for start in (600, 610, 620)]))
    out = tmp_path / "out.json"
    assert ps.main(["--day", DAY.isoformat(), "--snapshots", str(raw), "--gtfs-rt", str(rt),
                    "--timetable", str(tt.db_path), "--out", str(out)]) == 0
    doc = json.loads(out.read_text())
    assert doc["observations"]
    row = next(r for r in doc["observations"] if not r["estimated"])
    assert row["source_reports"] and row["observed_epoch"] and row["scheduled_epoch"]
    assert row["match"] == "declared"
    assert doc["coverage"]["feeds"]["gtfs_rt"]["parsed_snapshots"] == 4

    # Continue through consumer normalisation, derivative validation and exact input archival.
    import build_journey_times as bjt
    from archive_reliability_evidence import archive
    from publication_bundle import build_bundle
    from observation_contract import digest_file
    derived = tmp_path / "journeys"
    assert bjt.main(["--observations", str(out), "--out", str(derived), "--timetable", str(tt.db_path)]) == 0
    evidence = archive(out, raw.parent, rt.parent, tt.db_path, tmp_path / "evidence.tar.gz")
    sources = [{"day": DAY.isoformat(), "sha256": digest_file(out), "method_version": 4}]
    index = build_bundle(derived, {f"observations-{DAY.isoformat()}.json": out}, tmp_path / "candidate", sources, evidence)
    assert index["build_id"] and index["observation_sources"] == sources
    assert index["evidence"]["timetable_sha256"] == digest_file(tt.db_path)


def test_gtfs_service_origin_handles_both_london_dst_transitions():
    from observation_contract import service_origin
    assert service_origin("2026-03-29") == int(datetime(2026, 3, 28, 23, tzinfo=timezone.utc).timestamp())
    assert service_origin("2026-10-25") == int(datetime(2026, 10, 25, 0, tzinfo=timezone.utc).timestamp())


def test_timing_point_flags_follow_call_order_not_repeated_stop_code(tt):
    calls = tt.trip_stops_for("W600")
    flags = tt.timepoints_by_call("W600")
    assert len(flags) == len(calls)
    assert flags == [tt.timepoints_for("W600").get(atco) for _, atco in calls]


def test_segment_hour_filter_preserves_exit_after_the_selected_entry_hour():
    import query_reliability as qr
    from test_query_reliability import Args
    from test_reliability_stats import obs
    rows = [obs(stop="A", index=0, sched=17*3600+55*60),
            obs(stop="B", index=1, sched=18*3600+5*60, late=600)]
    selected = qr.filtered(rows, Args(by="segment", from_hour=17, to_hour=17))
    assert len(selected) == 2
    cells = qr.segment_cells(selected, Args(by="segment", from_hour=17, to_hour=17))
    assert len(cells) == 1
    assert next(iter(cells.values()))["median_gained_secs"] == 600


def test_segment_cohorts_do_not_merge_operators_or_timetable_versions():
    import reliability_stats as rs
    from test_reliability_stats import obs
    rows = []
    for operator, version, gain in [("SCSO", "old", 600), ("BHBC", "old", 0), ("SCSO", "new", 120)]:
        rows += [dict(obs(stop="A", index=0, sched=36000), operator=operator, data_version=version),
                 dict(obs(stop="B", index=1, sched=36600, late=gain), operator=operator, data_version=version)]
    cells = rs.segment_stats(rows)
    assert len(cells) == 3
    assert sorted(c["median_gained_secs"] for c in cells.values()) == [0, 120, 600]


def test_coverage_checks_local_service_cohorts_not_an_unrelated_calendar(tt, monkeypatch):
    future = {sid: {"start_date": "20260920", "end_date": "20261231"} for sid in tt.calendar}
    monkeypatch.setattr(tt, "calendar", {**future, "unrelated": {"start_date": "20260101", "end_date": "20261231"}})
    assert tt.covers_day(DAY)
    assert tt.uncovered_cohorts(DAY, ps.stops_in_box(tt))


def test_inconsistent_declared_start_cannot_be_inferred_onto_another_bus(tt):
    from api import trip_match
    from test_corridor_gaps import bus_at
    inst, _ = trip_match.build_instances(tt, DAY, ps.stops_in_box(tt), (0, 86400))
    vehicle = dict(bus_at(15), trip_id="W600", start_date=DAY.strftime("%Y%m%d"), start_time="11:00:00")
    placed, claimed = trip_match.place_declared(tt, [vehicle], inst, 615*60)
    assert not placed
    assert claimed == {0}


def test_stale_rt_identity_is_not_joined_to_a_fresh_siri_position(tt, tmp_path):
    from recorded_inputs import recorded_stream
    from test_process_snapshots import SNAPSHOT
    lat, lon = west_xy(15)
    stamp = int(datetime(2026,9,16,10,15,tzinfo=timezone.utc).timestamp())
    xml = tmp_path / "1115.xml"; pb = tmp_path / "1115.pb"
    xml.write_text(SNAPSHOT.format(lat=lat, lon=lon))
    pb.write_bytes(a_feed([a_vehicle(trip="W600", vehicle="SCSO-1234", lat=lat, lon=lon, stamp=stamp-300)]))
    health, manifest = {}, []
    snapshots = list(recorded_stream(tt, DAY, [(40500, xml)], [(40500, pb)], health, manifest, ps.parse_snapshot))
    siri = next(v for v in snapshots[0][1] if v["source_report"]["feed"] == "siri")
    assert "trip_id" not in siri
    assert health["gtfs_rt"]["fresh_vehicles"] == 1  # Fresh enough to retain, too old for this join.


import pytest


@pytest.mark.parametrize("service_day", [date(2026,9,17), date(2026,3,28), date(2026,10,24)])
def test_rt_trip_across_recording_folders_keeps_one_time_origin(service_day, tmp_path):
    from datetime import timedelta
    from types import SimpleNamespace
    from observation_contract import service_origin, TIME_BASIS, LONDON
    from recorded_inputs import recorded_stream
    import build_journey_times as bjt
    import check_published as checks
    calls = [(85800 + i*240, f"stop-{i}") for i in range(6)]
    stops = {atco: {"lat": 50.83, "lon": -.3 + i*.005, "name": atco} for i, (_,atco) in enumerate(calls)}
    tt = SimpleNamespace(stops=stops, trips={"T": {"route_id": "R1", "service_id": "ALL"}},
        routes={"R1": {"short_name": "700"}}, noc_for_route=lambda r: "SCSO",
        stop_times_for=lambda atco: [(secs,"T") for secs, code in calls if code == atco],
        trip_stops_for=lambda trip: calls, timepoints_by_call=lambda trip: [1]*6,
        runs_on=lambda sid, day: day == service_day)
    origin = service_origin(service_day)
    for secs, atco in calls:
        stamp = origin + secs
        local = datetime.fromtimestamp(stamp, LONDON)
        folder = tmp_path / local.date().isoformat(); folder.mkdir(exist_ok=True)
        (folder / f'{local:%H%M}-{stamp}.pb').write_bytes(a_feed([a_vehicle(trip="T", lat=stops[atco]["lat"], lon=stops[atco]["lon"], stamp=stamp)]))
    files = []
    for folder in sorted(p for p in tmp_path.iterdir() if p.is_dir()):
        day = date.fromisoformat(folder.name); health, manifest = {}, []
        rows, coverage = ps.observe_day(tt, day, recorded_stream(tt, day, [], ps.gtfs_rt_files(folder), health, manifest, ps.parse_snapshot))
        out = tmp_path / f'observations-{day}.json'; files.append(str(out))
        out.write_text(json.dumps({"day": str(day), "data_version": "frozen-timetable", "method_version": 4,
            "time_basis": TIME_BASIS, "observations": rows}))
    rows, meta = load_observations(files)
    measured = sorted((r for r in rows if not r["estimated"]), key=lambda r: r["stop_index"])
    assert len(files) == 2 and len(measured) == 6
    assert [r["scheduled_secs"] for r in measured] == [secs for secs,_ in calls]
    assert all(r["observed_epoch"] == origin+r["observed_secs"] for r in measured)
    assert all(r["day"] == str(service_day) for r in measured)
    doc = bjt.build(rows, meta)[("700", "SCSO")]
    assert len(doc["journeys"]) == 1 and len(doc["journeys"][0]["calls"]) == 6
    failures = checks.Failures(); checks.check_journey_document(doc, "night.json", failures)
    assert not failures.items
