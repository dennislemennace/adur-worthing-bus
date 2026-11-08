import gzip
import json
from pathlib import Path
import sys
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))


def journey_files(tmp_path, bad=False):
    folder = tmp_path / "journeys"; folder.mkdir()
    doc = {"service": "700", "operator": "SCSO", "days": ["2026-09-21"],
           "stops": [{"atco": "A"}, {"atco": "B"}, {"atco": "C"}],
           "journeys": [{"day": "2026-09-21", "trip_id": "T", "start": "08:00",
                         "calls": [[0, 28800, 28800, 0, 0], [1, 100 if bad else 29400, 29400, 0, 1],
                                   [2, 30000, 30000, 0, 2]]}]}
    (folder / "700-SCSO.json").write_text(json.dumps(doc))
    (folder / "index.json").write_text(json.dumps({"days": doc["days"], "services": [{"file": "700-SCSO.json"}]}))
    return folder


def test_invalid_candidate_does_not_create_a_generation(tmp_path):
    from publication_bundle import build_bundle
    folder = journey_files(tmp_path, bad=True)
    with pytest.raises(ValueError):
        build_bundle(folder, {}, tmp_path / "candidate", [], {})
    assert not (tmp_path / "candidate").exists()


def test_manifest_pins_all_documents_to_one_immutable_generation(tmp_path):
    from publication_bundle import build_bundle
    folder = journey_files(tmp_path)
    out = tmp_path / "candidate"
    index = build_bundle(folder, {}, out, [], {})
    assert len(index["build_id"]) == 64
    entry = index["services"][0]
    assert entry["file"].startswith(f'builds/{index["build_id"]}/')
    assert (out / entry["file"]).is_file()
    assert json.loads((out / entry["file"]).read_text())["build_id"] == index["build_id"]
    assert entry["sha256"]
    assert (out / f'builds/{index["build_id"]}/index.json').exists()


def test_bad_observation_arithmetic_stops_publication(tmp_path):
    from publication_bundle import build_bundle
    folder = journey_files(tmp_path)
    obs = tmp_path / "observations-2026-09-21.json.gz"
    obs.write_bytes(gzip.compress(json.dumps({"time_basis": "gtfs_service_day_elapsed_seconds",
        "method_version": 4, "provenance": {"sources": [{"sha256": "test"}]}, "observations": [{"day": "2026-09-21", "observed_secs": 300,
        "scheduled_secs": 100, "lateness_secs": 99}]}).encode()))
    with pytest.raises(ValueError, match="lateness contradicts"):
        build_bundle(folder, {obs.name: obs}, tmp_path / "candidate", [], {})


def test_workflow_validates_before_any_release_or_public_bucket_write():
    workflow = Path(".github/workflows/process-snapshots.yml").read_text()
    validate = workflow.index("scripts/check_published.py")
    assert validate < workflow.index("gh release upload")
    assert validate < workflow.index("aws s3 sync journey-times/") if "aws s3 sync journey-times/" in workflow else True


def test_archive_rejects_changed_inputs_and_contains_replay_files(tmp_path):
    from archive_reliability_evidence import archive
    from observation_contract import digest_file
    import tarfile
    root = tmp_path / "repo"; root.mkdir()
    code = root / "processor.py"; code.write_text("# exact code\n")
    tt = tmp_path / "timetable.sqlite"; tt.write_bytes(b"historical timetable")
    raw = tmp_path / "raw" / "2026-09-21"; raw.mkdir(parents=True)
    report = raw / "0800.xml"; report.write_bytes(b"<snapshot/>")
    obs = tmp_path / "obs.json"
    obs.write_text(json.dumps({"provenance": {"processing_commit": "commit", "timetable_sha256": digest_file(tt),
        "code_sha256": {"processor.py": digest_file(code)},
        "sources": [{"object": "raw/2026-09-21/0800.xml", "sha256": digest_file(report)}]}}))
    out = tmp_path / "evidence.tar.gz"
    info = archive(obs, raw.parent, tmp_path / "rt", tt, out, root)
    assert info["sha256"] == digest_file(out)
    with tarfile.open(out) as tar:
        assert tar.extractfile("timetable.sqlite").read() == tt.read_bytes()
        assert tar.extractfile("raw/2026-09-21/0800.xml").read() == report.read_bytes()
        assert tar.extractfile("code/processor.py").read() == code.read_bytes()
    report.write_bytes(b"changed")
    with pytest.raises(ValueError, match="Input changed"):
        archive(obs, raw.parent, tmp_path / "rt", tt, out, root)
    assert digest_file(out) == info["sha256"]


def test_missing_known_day_cannot_shrink_the_published_window(tmp_path, monkeypatch):
    from prepare_reliability_inputs import prepare
    import urllib.request
    def unavailable(*args, **kwargs):
        raise OSError("missing known input")
    monkeypatch.setattr(urllib.request, "urlopen", unavailable)
    previous = {"days": ["2026-09-20"], "observation_sources": [{"day": "2026-09-20", "immutable": True, "url": "https://example.invalid/obs", "sha256": "abc"}]}
    with pytest.raises(OSError, match="missing known input"):
        prepare("2026-09-21", tmp_path / "current", previous, "https://example.invalid/release", "owner/repo", tmp_path / "inputs", tmp_path / "summaries")
    assert not (tmp_path / "inputs/sources.json").exists()


def test_reprocessing_old_month_restores_the_month_without_widening_current_window(tmp_path, monkeypatch):
    from prepare_reliability_inputs import prepare
    from observation_contract import digest_file
    import io
    import urllib.request
    older = gzip.compress(json.dumps({"day": "2026-07-01", "method_version": 4}).encode())
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **kw: io.BytesIO(older))
    current = tmp_path / "observations.json.gz"
    current.write_bytes(gzip.compress(json.dumps({"day": "2026-07-02", "method_version": 4}).encode()))
    previous = {"days": ["2026-09-21"], "observation_sources": [{"day": "2026-07-01", "url": "https://example.invalid/old", "immutable": True}]}
    sources = prepare("2026-07-02", current, previous, "https://example.invalid/new", "owner/repo", tmp_path / "inputs", tmp_path / "summaries")
    assert {s["day"] for s in sources} == {"2026-07-01", "2026-07-02"}
    assert len(list((tmp_path / "inputs/month").glob("*.gz"))) == 2
    assert not list((tmp_path / "inputs/window").glob("*.gz"))


def _observations(day):
    return gzip.compress(json.dumps({"day": day, "method_version": 7}).encode())


def test_a_gateway_timeout_is_tried_again_and_a_missing_file_is_not(tmp_path, monkeypatch):
    """8 November 2026 (run 42471232953): one 504 from a release download
    failed the night. A 404 is an answer, and is not asked again."""
    import io
    import urllib.error
    import urllib.request
    import prepare_reliability_inputs as pri
    waits = []
    monkeypatch.setattr(pri.time, "sleep", waits.append)
    calls = []
    def flaky(request, timeout):
        calls.append(request.full_url)
        if len(calls) < 3:
            raise urllib.error.HTTPError(request.full_url, 504, "Gateway Time-out", {}, None)
        return io.BytesIO(_observations("2026-11-06"))
    monkeypatch.setattr(urllib.request, "urlopen", flaky)
    current = tmp_path / "observations.json.gz"
    current.write_bytes(_observations("2026-11-07"))
    previous = {"days": ["2026-11-06"], "observation_sources": [
        {"day": "2026-11-06", "url": "https://example.invalid/6", "immutable": True}]}
    pri.prepare("2026-11-07", current, previous, "https://example.invalid/new", "owner/repo",
                tmp_path / "inputs", tmp_path / "summaries")
    assert len(calls) == 3 and waits == [5, 20]
    assert (tmp_path / "inputs/window/observations-2026-11-06.json.gz").exists()

    calls.clear(); waits.clear()
    def gone(request, timeout):
        calls.append(request.full_url)
        raise urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, None)
    monkeypatch.setattr(urllib.request, "urlopen", gone)
    with pytest.raises(urllib.error.HTTPError):
        pri.prepare("2026-11-07", current, previous, "https://example.invalid/new", "owner/repo",
                    tmp_path / "again", tmp_path / "summaries")
    assert len(calls) == 1 and not waits


def test_the_delay_map_reads_nine_weeks_and_everything_else_five(tmp_path, monkeypatch):
    """A stretch's quieter hours need longer than five weeks to reach 30
    journeys over 5 days; nothing else's window changes."""
    import io
    import urllib.request
    from prepare_reliability_inputs import prepare
    monkeypatch.setattr(urllib.request, "urlopen",
                        lambda request, timeout: io.BytesIO(_observations(request.full_url.rsplit("/", 1)[-1])))
    # 7 November back: 34 days is 4 October, 62 days 6 September.
    days = ["2026-09-05", "2026-09-06", "2026-10-03", "2026-10-04", "2026-11-06"]
    previous = {"days": days, "observation_sources": [
        {"day": d, "url": f"https://example.invalid/{d}", "immutable": True} for d in days]}
    current = tmp_path / "observations.json.gz"
    current.write_bytes(_observations("2026-11-07"))
    prepare("2026-11-07", current, previous, "https://example.invalid/new", "owner/repo",
            tmp_path / "inputs", tmp_path / "summaries")
    held = lambda folder: sorted(p.name[13:23] for p in (tmp_path / "inputs" / folder).glob("*.gz"))
    assert held("window") == ["2026-10-04", "2026-11-06", "2026-11-07"]
    assert held("window-long") == ["2026-09-06", "2026-10-03", "2026-10-04", "2026-11-06", "2026-11-07"]
    workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/process-snapshots.yml").read_text()
    assert "build_delay_hotspots.py --observations inputs/window-long" in workflow
    assert workflow.count("inputs/window-long") == 1, "only the delay map reads the long window"


def _previous(*names):
    build = "a" * 64
    return {"build_id": build, "artifacts": {
        name: {"file": f"builds/{build}/{name}", "sha256": "x"} for name in names}}


def test_a_compressed_artifact_replaces_its_uncompressed_twin(tmp_path):
    """Carried forward, the uncompressed delay-map evidence (559 MB a night by
    5 October 2026) would stay linked, and so stored, after its .gz replaced it."""
    from publication_bundle import build_bundle
    folder = journey_files(tmp_path)
    preview = tmp_path / "hotspot-preview.json.gz"
    preview.write_bytes(gzip.compress(b'{"cells": []}'))
    index = build_bundle(folder, {preview.name: preview}, tmp_path / "candidate",
                         [{"day": "2026-09-21"}], {}, _previous("hotspot-preview.json"))
    assert "hotspot-preview.json" not in index["artifacts"]
    assert index["artifacts"]["hotspot-preview.json.gz"]["file"].startswith(f'builds/{index["build_id"]}/')


def test_day_artifacts_stop_being_carried_once_their_day_leaves_the_window(tmp_path):
    """Every carried day stayed linked, so retention could never retire it: about
    20 MB a day, the whole 4 GiB budget within months. A day out of the window is
    still in its own release, which observation_sources names."""
    from publication_bundle import build_bundle
    folder = journey_files(tmp_path)
    previous = _previous(
        "observations-2026-08-01.json.gz", "summary-2026-08-01.json", "rollup-2026-08.json",
        "observations-2026-09-20.json.gz", "summary-2026-09-20.json", "rollup-2026-09.json",
        "hotspot-map-700-SCSO.json")
    index = build_bundle(folder, {}, tmp_path / "candidate",
                         [{"day": "2026-09-20"}, {"day": "2026-09-21"}], {}, previous)
    kept = set(index["artifacts"])
    assert {"observations-2026-09-20.json.gz", "summary-2026-09-20.json",
            "rollup-2026-09.json", "hotspot-map-700-SCSO.json"} <= kept
    assert not kept & {"observations-2026-08-01.json.gz", "summary-2026-08-01.json",
                       "rollup-2026-08.json"}


def test_workflow_stores_the_preview_compressed_and_says_why_it_will_not_publish():
    workflow = Path(".github/workflows/process-snapshots.yml").read_text()
    assert workflow.index("gzip -9nf hotspot-preview.json") < workflow.index("scripts/publication_bundle.py")
    assert '"hotspot-preview.json=hotspot-preview.json"' not in workflow
    # The 6 October 2026 failure was a bare `test` that printed nothing.
    assert 'over the $(gib "$BUDGET") budget' in workflow


def test_workflow_names_itself_on_every_python_download():
    # The Worker refuses urllib's default User-Agent with a 403: on 26
    # October 2026 that stopped the board being scored (issue #7).
    import re
    workflow = Path(".github/workflows/process-snapshots.yml").read_text()
    bare = re.findall(r"urlopen\(\s*f?[\"']", workflow)
    assert not bare, f"a download sends Python's default User-Agent: {bare}"


def test_a_waiting_file_whose_counts_do_not_add_up_stops_publication(tmp_path):
    from publication_bundle import build_bundle
    folder = journey_files(tmp_path)
    bad = tmp_path / "headways-7-BHBC.json"
    bad.write_text(json.dumps({"schema_version": 1, "service": "7", "operator": "BHBC", "method": "m",
        "method_version": 1, "caveats": ["c"], "as_of": "2026-10-07", "days": ["2026-09-21"],
        "floors": {"days": 5, "judged": 30}, "groups": [], "cells": [{
            "atco": "A", "direction": "eastbound", "day_type": "weekday", "period": "10-16",
            "kind": "non_frequent", "days": 1, "scheduled_passages": 4, "accounted_passages": 4,
            "judged": 4, "on_time": 4, "early": 0, "late": 1, "late_here": 0, "late_from_start": 0,
            "late_on_the_way": 0, "late_start_unknown": 0, "sample_sufficient": False}]}))
    with pytest.raises(ValueError, match="punctuality does not add up"):
        build_bundle(folder, {bad.name: bad}, tmp_path / "candidate", [{"day": "2026-09-21"}], {})


def test_the_two_storage_caps_together_stay_inside_the_free_ten_gigabytes():
    """R2 bills past 10 GB with no switch to stop it, so the recorder's cap and
    the published bucket's are the whole control. Counted as decimal gigabytes,
    the stricter reading. Raising either without the other fails here."""
    import re
    root = Path(__file__).resolve().parents[1]
    recorder = (root / "worker/src/recorder.js").read_text()
    raw = re.search(r"const MAX_STORED_BYTES = ([\d\s*]+);", recorder).group(1)
    raw_bytes = 1
    for factor in raw.split("*"):
        raw_bytes *= int(factor)
    workflow = (root / ".github/workflows/process-snapshots.yml").read_text()
    budgets = re.findall(r"BUDGET=(\d+)", workflow)
    assert len(budgets) == 1, budgets
    published = int(budgets[0])
    assert raw_bytes + published <= 10_000_000_000, (raw_bytes, published)


def test_files_are_stored_compressed_only_once_the_worker_can_serve_them():
    """8 November 2026: each .json goes into the bucket gzip-compressed, about
    an eighth of its size. Uploaded before the Worker could pass it on, every
    visitor would have been handed compressed bytes as if they were JSON."""
    workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/process-snapshots.yml").read_text()
    gate = workflow.index("x-published-encoding: gzip")
    assert gate < workflow.index("gzip -9nc") < workflow.index("--content-encoding gzip")
    # The .json.gz files are already compressed: a second Content-Encoding
    # would make every client unpack the evidence on arrival.
    line = workflow[workflow.index("--content-encoding gzip") - 200:workflow.index("--content-encoding gzip")]
    assert '--exclude "*.gz"' in line
    assert 'SIZE=$(( $(du -sb upload' in workflow, "the budget must count the bytes actually stored"
    assert 'base = Path("upload")' in workflow, "read-back must compare the bytes actually stored"
    assert "journey-times/README.txt" in workflow
    worker = (Path(__file__).resolve().parents[1] / "worker/src/index.js").read_text()
    assert '"x-published-encoding": "gzip"' in worker


class _Reply:
    def __init__(self, body, encoding=None):
        self._body, self.headers = body, {"Content-Encoding": encoding} if encoding else {}
    def read(self):
        return self._body
    def __enter__(self):
        return self
    def __exit__(self, *exc):
        return False


def test_our_scripts_ask_for_published_files_compressed_and_unpack_them():
    """Unpacking a 10 MB file in the Worker costs more CPU than the free plan
    allows a request (30 to 50 ms measured, against 10), so our own scripts ask
    for gzip and unpack it themselves (8 November 2026)."""
    from published_fetch import read_published, unpack_stored
    body = json.dumps({"service": "700"}).encode()
    asked = []
    def compressed(request, timeout):
        asked.append(dict(request.header_items()))
        return _Reply(gzip.compress(body), "gzip")
    assert read_published("https://example.invalid/700.json", opener=compressed) == body
    assert asked[0]["Accept-encoding"] == "gzip"
    assert asked[0]["User-agent"] == "bus-reliability-build", "the Worker refuses urllib's own name"
    # Stored before compression, or a .json.gz file: exactly the bytes sent.
    assert read_published("https://example.invalid/700.json", opener=lambda r, timeout: _Reply(body)) == body
    evidence = gzip.compress(body)
    assert read_published("https://example.invalid/x.json.gz", opener=lambda r, timeout: _Reply(evidence)) == evidence
    # Read from the bucket itself, where nothing undoes the encoding.
    assert unpack_stored(gzip.compress(body)) == body and unpack_stored(body) == body


def test_every_script_reading_published_files_uses_the_compressed_fetch():
    root = Path(__file__).resolve().parents[1]
    for name in ("collect_served_window.py", "build_detected_diversions.py", "add_disruption.py",
                 "validate_hand_timings.py"):
        assert "read_published(" in (root / "scripts" / name).read_text(), name
    assert "unpack_stored(" in (root / "scripts/prune_published.py").read_text()
    workflow = (root / ".github/workflows/process-snapshots.yml").read_text()
    assert "urllib.request.urlopen" not in workflow[workflow.index("PYDIV"):workflow.rindex("PYDIV")]
