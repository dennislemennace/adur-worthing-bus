"""Put a day's recorded feed objects back from its replay archive.

Raw objects leave R2 seven days after a day is published, so re-measuring an
older day starts from the archive its night run attached to the
`reliability-run-*` release (`scripts/archive_reliability_evidence.py`). The
archive is checked against the evidence record published beside it, and every
recording against the archive's manifest, before anything lands where the
processor reads: SIRI-VM in `snapshots/<day>/`, GTFS-RT in `gtfs-rt/<day>/`,
just as the R2 sync leaves them. Nothing is written unless all of it checks.

    python scripts/restore_recorded_day.py --archive evidence.tar.gz \\
        --evidence evidence.json --day 2026-10-17 --snapshots snapshots --rt gtfs-rt
"""

import argparse
import hashlib
import json
import shutil
import sys
import tarfile
from pathlib import Path, PurePosixPath

from observation_contract import digest_file

FEEDS = {"raw": "siri", "rt": "gtfs_rt"}


def _recording(name, day):
    """The feed folder of a recording of this day, or None for any other member."""
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Unsafe path in the archive: {name}")
    if path.parts[0] not in FEEDS:
        return None
    if len(path.parts) != 3 or path.parts[1] != day or path.parts[2].startswith("."):
        raise ValueError(f"Not a recording of {day}: {name}")
    return path.parts[0]


def restore(archive, evidence, day, snapshots, rt):
    archive, snapshots, rt = Path(archive), Path(snapshots), Path(rt)
    record = json.loads(Path(evidence).read_text())
    if record.get("day") != day:
        raise ValueError(f"The archive is of {record.get('day')}, not {day}")
    if digest_file(archive) != record.get("sha256"):
        raise ValueError("The archive's sha256 is not the one its evidence record publishes")

    staging = snapshots.parent / f".restore-{day}"
    shutil.rmtree(staging, ignore_errors=True)
    try:
        with tarfile.open(archive, "r:gz") as tar:
            try:
                manifest = json.load(tar.extractfile(tar.getmember("manifest.json")))
            except KeyError:
                raise ValueError("The archive has no manifest") from None
            expected = {name: sha for name, sha in manifest.items() if _recording(name, day)}
            seen = {}
            for member in tar:
                feed = _recording(member.name, day)
                if feed is None:
                    continue
                if not member.isfile() or member.name in seen:
                    raise ValueError(f"Not a single plain file: {member.name}")
                payload = tar.extractfile(member).read()
                if hashlib.sha256(payload).hexdigest() != expected.get(member.name):
                    raise ValueError(f"{member.name} does not match the archive's manifest")
                target = staging / member.name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(payload)
                seen[member.name] = feed
        missing = sorted(expected.keys() - seen.keys())
        if missing:
            raise ValueError(f"{len(missing)} recordings in the manifest are missing, first {missing[0]}")
        if not seen:
            raise ValueError(f"The archive holds no recordings of {day}")

        counts = {"siri": 0, "gtfs_rt": 0}
        for name, feed in sorted(seen.items()):
            file = PurePosixPath(name).name
            target = (snapshots if feed == "raw" else rt) / day / file
            target.parent.mkdir(parents=True, exist_ok=True)
            source = staging / name
            if target.exists() and target.read_bytes() != source.read_bytes():
                raise ValueError(f"{target} already holds something else")
            source.replace(target)
            counts[FEEDS[feed]] += 1
        return counts
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    for name in ("archive", "evidence", "day", "snapshots", "rt"):
        ap.add_argument(f"--{name}", required=True)
    args = ap.parse_args(argv)
    try:
        counts = restore(args.archive, args.evidence, args.day, args.snapshots, args.rt)
    except ValueError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1
    print(json.dumps(counts, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
