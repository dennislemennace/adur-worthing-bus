"""Create a bounded, replayable archive from exactly the processor's inputs."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import tarfile

from observation_contract import digest_file


def archive(observations, snapshots, rt, timetable, out, root=None, max_bytes=250 * 1024 * 1024):
    root = Path(root or Path(__file__).resolve().parents[1])
    observations, out = Path(observations), Path(out)
    raw = observations.read_bytes()
    doc = json.loads(gzip.decompress(raw) if observations.suffix == ".gz" else raw)
    provenance = doc["provenance"]
    files = {"timetable.sqlite": (Path(timetable), provenance["timetable_sha256"])}
    for source in provenance["sources"]:
        key = PurePosixPath(source["object"])
        if key.is_absolute() or ".." in key.parts or len(key.parts) != 3 or key.parts[0] not in ("raw", "rt"):
            raise ValueError("Unsafe recorded object path")
        base = Path(snapshots if key.parts[0] == "raw" else rt)
        files[str(key)] = (base / key.parts[1] / key.parts[2], source["sha256"])
    for name, expected in provenance["code_sha256"].items():
        if PurePosixPath(name).is_absolute() or ".." in PurePosixPath(name).parts:
            raise ValueError("Unsafe code path")
        files[f"code/{name}"] = (root / name, expected)
    for name in ("requirements.txt", "requirements-dev.txt", ".python-version"):
        if (root / name).exists():
            files[f"code/{name}"] = (root / name, digest_file(root / name))
    for name, (path, expected) in files.items():
        if digest_file(path) != expected:
            raise ValueError(f"Input changed before archival: {name}")
    out.parent.mkdir(parents=True, exist_ok=True)
    temporary = out.with_suffix(out.suffix + ".tmp")
    try:
        with temporary.open("wb") as handle, gzip.GzipFile(fileobj=handle, mode="wb", mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|") as tar:
                def add(name, payload):
                    info = tarfile.TarInfo(name); info.size = len(payload); info.mode = 0o644
                    tar.addfile(info, io.BytesIO(payload))
                add("provenance.json", json.dumps(provenance, sort_keys=True, indent=1).encode())
                add("manifest.json", json.dumps({n: h for n, (_p, h) in files.items()}, sort_keys=True).encode())
                for name, (path, _expected) in sorted(files.items()):
                    add(name, path.read_bytes())
        if temporary.stat().st_size > max_bytes:
            raise ValueError("Compressed evidence exceeds the 250 MiB per-day archive budget")
        if out.exists() and digest_file(out) != digest_file(temporary):
            raise ValueError("Refusing to replace a different evidence archive")
        temporary.replace(out)
    finally:
        temporary.unlink(missing_ok=True)
    return {"day": doc.get("day"), "file": out.name, "sha256": digest_file(out), "bytes": out.stat().st_size,
            "timetable_sha256": provenance["timetable_sha256"], "processing_commit": provenance["processing_commit"]}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    for name in ("observations", "snapshots", "rt", "timetable", "out"):
        ap.add_argument(f"--{name}", required=True)
    args = ap.parse_args(argv)
    print(json.dumps(archive(args.observations, args.snapshots, args.rt, args.timetable, args.out), sort_keys=True))


if __name__ == "__main__":
    main()
