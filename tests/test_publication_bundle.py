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
