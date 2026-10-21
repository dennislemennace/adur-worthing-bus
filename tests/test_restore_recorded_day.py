"""A day's recordings come back from its replay archive exactly, or not at all.

Raw objects leave R2 seven days after a day is published. Re-measuring an
older day (20 September to 17 October 2026 were measured against timetables
that counted Compass's waits twice) has to start from the archive the night
run kept, and must get back the very objects that night read.

Run with:  pytest tests/test_restore_recorded_day.py
"""

import gzip
import hashlib
import io
import json
import sys
import tarfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from archive_reliability_evidence import archive       # noqa: E402
from observation_contract import digest_file           # noqa: E402
from restore_recorded_day import restore               # noqa: E402

DAY = "2026-10-17"


def _sha(b):
    return hashlib.sha256(b).hexdigest()


def _night(tmp_path):
    """An archive made by the night run's own archiver, and its evidence.json."""
    root = tmp_path / "repo"; root.mkdir()
    (root / "processor.py").write_text("# exact code\n")
    tt = tmp_path / "timetable.sqlite"; tt.write_bytes(b"historical timetable")
    recorded = {
        f"raw/{DAY}/0800-1792220400.xml": b"<snapshot/>",
        f"raw/{DAY}/0801-1792220460.xml": b"<snapshot two/>",
        f"rt/{DAY}/0800-1792220400.pb": b"\x0a\x01pb",
    }
    for name, payload in recorded.items():
        feed, day, file = name.split("/")
        folder = tmp_path / ("raw" if feed == "raw" else "rt") / day
        folder.mkdir(parents=True, exist_ok=True)
        (folder / file).write_bytes(payload)
    obs = tmp_path / "observations.json"
    obs.write_text(json.dumps({"day": DAY, "provenance": {
        "timetable_sha256": digest_file(tt), "processing_commit": "abc",
        "code_sha256": {"processor.py": digest_file(root / "processor.py")},
        "sources": [{"object": n, "sha256": _sha(p), "bytes": len(p)} for n, p in recorded.items()]}}))
    out = tmp_path / "night" / "evidence.tar.gz"
    meta = archive(obs, tmp_path / "raw", tmp_path / "rt", tt, out, root=root)
    evidence = tmp_path / "night" / "evidence.json"
    evidence.write_text(json.dumps(meta))
    return out, evidence, recorded


def _tar(path, members):
    with gzip.open(path, "wb") as gz, tarfile.open(fileobj=gz, mode="w|") as tar:
        for name, payload in members.items():
            info = tarfile.TarInfo(name); info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))
    return path


def _evidence(path, archive_path, day=DAY):
    path.write_text(json.dumps({"day": day, "sha256": digest_file(archive_path)}))
    return path


def test_a_day_comes_back_byte_for_byte_into_the_folders_the_run_reads(tmp_path):
    out, evidence, recorded = _night(tmp_path)
    counts = restore(out, evidence, DAY, tmp_path / "snapshots", tmp_path / "gtfs-rt")
    assert counts == {"siri": 2, "gtfs_rt": 1}
    for name, payload in recorded.items():
        feed, day, file = name.split("/")
        base = tmp_path / ("snapshots" if feed == "raw" else "gtfs-rt")
        assert (base / day / file).read_bytes() == payload
    # The timetable and code in the archive are not recordings.
    assert not list((tmp_path / "snapshots").rglob("timetable.sqlite"))


def test_an_archive_for_another_day_is_refused(tmp_path):
    out, evidence, _ = _night(tmp_path)
    with pytest.raises(ValueError, match="is of 2026-10-17, not 2026-10-16"):
        restore(out, evidence, "2026-10-16", tmp_path / "snapshots", tmp_path / "gtfs-rt")


def test_an_archive_that_is_not_the_one_published_is_refused(tmp_path):
    out, evidence, _ = _night(tmp_path)
    doc = json.loads(evidence.read_text()); doc["sha256"] = "0" * 64
    evidence.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="sha256"):
        restore(out, evidence, DAY, tmp_path / "snapshots", tmp_path / "gtfs-rt")


def test_a_recording_that_does_not_match_its_manifest_is_refused(tmp_path):
    name = f"raw/{DAY}/0800-1792220400.xml"
    bad = _tar(tmp_path / "evidence.tar.gz", {
        "manifest.json": json.dumps({name: _sha(b"<snapshot/>")}).encode(),
        name: b"<altered/>"})
    with pytest.raises(ValueError, match="0800-1792220400.xml"):
        restore(bad, _evidence(tmp_path / "evidence.json", bad), DAY,
                tmp_path / "snapshots", tmp_path / "gtfs-rt")
    assert not list((tmp_path / "snapshots").rglob("*.xml"))


def test_a_recording_named_in_the_manifest_but_missing_is_refused(tmp_path):
    there, gone = f"raw/{DAY}/0800-1792220400.xml", f"rt/{DAY}/0800-1792220400.pb"
    short = _tar(tmp_path / "evidence.tar.gz", {
        "manifest.json": json.dumps({there: _sha(b"<snapshot/>"), gone: _sha(b"pb")}).encode(),
        there: b"<snapshot/>"})
    with pytest.raises(ValueError, match="missing"):
        restore(short, _evidence(tmp_path / "evidence.json", short), DAY,
                tmp_path / "snapshots", tmp_path / "gtfs-rt")


@pytest.mark.parametrize("name", [f"raw/{DAY}/../../escape.xml", "raw/2026-10-16/0800.xml",
                                  f"raw/{DAY}/sub/0800.xml", f"/raw/{DAY}/0800.xml"])
def test_a_recording_outside_its_day_folder_is_refused(tmp_path, name):
    payload = b"<snapshot/>"
    bad = _tar(tmp_path / "evidence.tar.gz", {
        "manifest.json": json.dumps({name: _sha(payload)}).encode(), name: payload})
    with pytest.raises(ValueError):
        restore(bad, _evidence(tmp_path / "evidence.json", bad), DAY,
                tmp_path / "snapshots", tmp_path / "gtfs-rt")
    assert not (tmp_path / "escape.xml").exists()


def test_an_archive_with_no_recordings_is_refused(tmp_path):
    empty = _tar(tmp_path / "evidence.tar.gz", {"manifest.json": b"{}"})
    with pytest.raises(ValueError, match="no recordings"):
        restore(empty, _evidence(tmp_path / "evidence.json", empty), DAY,
                tmp_path / "snapshots", tmp_path / "gtfs-rt")
