"""Validate a complete candidate and prepare immutable objects plus one pointer.

This script has no network/publication side effects. The workflow uploads the
objects, verifies upload success, and only then replaces index.json.
"""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re

import check_published as checks
from observation_contract import TIME_BASIS, service_origin


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def validate_observations(doc):
    if doc.get("method_version", 0) < 4 or doc.get("time_basis") != TIME_BASIS:
        raise ValueError("New observations require explicit service-day time basis and method >=4")
    if not doc.get("provenance", {}).get("sources"):
        raise ValueError("Missing archived source manifest")
    for row in doc.get("observations", []):
        origin = service_origin(row["day"])
        if row["observed_secs"] - row["scheduled_secs"] != row["lateness_secs"]:
            raise ValueError("Observation lateness contradicts its times")
        if any(row.get(f + "_epoch") != origin + row[f + "_secs"] for f in ("observed", "scheduled")):
            raise ValueError("Observation instants contradict their service-day origin")
        if row.get("stop_index") is None or not row.get("trip_id"):
            raise ValueError("Missing trip/call identity")
        if not row.get("estimated") and not row.get("source_reports"):
            raise ValueError("Measured call has no report evidence")
    if not doc.get("observations"):
        raise ValueError("An empty processing result cannot replace the public generation")
    validate_schedule(doc)


def validate_schedule(doc):
    """The day's recorded timetable must be present and internally sound.

    Required, not optional: the timetable is rebuilt weekly and this is the only
    copy of what it promised on the day, so a night that failed to record it
    would lose that permanently and nobody would notice until a chart needed it.
    """
    schedule = doc.get("schedule")
    if not isinstance(schedule, dict):
        raise ValueError("Observations carry no recorded timetable")
    if schedule.get("day") != doc.get("day") or schedule.get("time_basis") != TIME_BASIS:
        raise ValueError("Recorded timetable describes a different day or time basis")
    patterns, profiles, trips = (schedule.get("patterns") or {}, schedule.get("profiles") or [],
                                 schedule.get("trips") or [])
    if not trips:
        raise ValueError("Recorded timetable schedules no journeys")
    counts = schedule.get("counts") or {}
    if (counts.get("trips"), counts.get("patterns"), counts.get("profiles")) != (
            len(trips), len(patterns), len(profiles)):
        raise ValueError("Recorded timetable counts contradict its contents")
    for profile in profiles:
        offsets = profile.get("offsets") or []
        if not offsets or offsets[0] != 0 or any(b < a for a, b in zip(offsets, offsets[1:])):
            raise ValueError("Recorded timetable has a profile running backwards")
        if len(profile.get("timepoints") or []) != len(offsets):
            raise ValueError("Recorded timetable profile has mismatched timepoints")
    for trip_id, pattern, profile, _start, _headsign in trips:
        if pattern not in patterns or not 0 <= profile < len(profiles):
            raise ValueError(f"Recorded trip {trip_id} names a missing pattern or profile")
        if len(profiles[profile]["offsets"]) != len(patterns[pattern].get("atcos") or []):
            raise ValueError(f"Recorded trip {trip_id} has a profile of the wrong length")


def build_bundle(journey_dir, extras, out, sources, evidence, previous=None):
    journey_dir, out = Path(journey_dir), Path(out)
    failures, documents, binary = checks.Failures(), {}, {}
    index = json.loads((journey_dir / "index.json").read_text())
    checks.check_index(index, journey_dir, failures)
    if not index.get("services"):
        raise ValueError("An empty index cannot replace the public generation")
    for path in sorted(journey_dir.glob("*.json")):
        if path.name == "index.json":
            continue
        doc = json.loads(path.read_text())
        checks.check_journey_document(doc, path.name, failures)
        documents[path.name] = doc
    for name, path in extras.items():
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}\.json(?:\.gz)?", name):
            raise ValueError(f"Unsafe artifact name: {name}")
        raw = Path(path).read_bytes()
        doc = json.loads(gzip.decompress(raw) if name.endswith(".gz") else raw)
        if name.startswith("observations-"):
            validate_observations(doc)
        elif name.startswith(("summary-", "rollup-")):
            checks.check_summary(doc, name, failures)
        elif name.startswith("hotspot-map-") and name != "hotspot-map-index.json":
            checks.check_hotspot_map(doc, name, failures, raw=raw)
        binary[name] = raw
    if failures.items:
        raise ValueError(f"Candidate failed validation: {failures.items[:5]}")
    seed = {"documents": documents, "index": index, "artifacts": {
        name: hashlib.sha256(raw).hexdigest() for name, raw in binary.items()},
        "sources": sources, "evidence": evidence, "previous_build": (previous or {}).get("build_id")}
    build_id = hashlib.sha256(encoded(seed)).hexdigest()
    prefix = f"builds/{build_id}"
    objects = {name: encoded({**doc, "build_id": build_id}) for name, doc in documents.items()}
    objects.update(binary)
    if previous:
        objects["previous-index.json"] = encoded(previous)
    index.update(build_id=build_id, observation_sources=sources, evidence=evidence,
                 previous_build=(previous or {}).get("build_id"),
                 rollback_index=f"{prefix}/previous-index.json" if previous else None)
    for entry in index["services"]:
        name = entry["file"]
        entry.update(file=f"{prefix}/{name}", sha256=hashlib.sha256(objects[name]).hexdigest())
    index["artifacts"] = {**(previous or {}).get("artifacts", {}), **{name: {"file": f"{prefix}/{name}", "sha256": hashlib.sha256(raw).hexdigest()}
                          for name, raw in binary.items()}}
    objects["index.json"] = encoded(index)
    # All validation precedes writes, and a build path is never overwritten.
    for name, raw in objects.items():
        path = out / prefix / name
        if path.exists() and path.read_bytes() != raw:
            raise ValueError(f"Immutable object differs: {path}")
    for name, raw in objects.items():
        path = out / prefix / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    (out / "index.json").write_bytes(encoded(index))
    return index


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--journey-times", required=True)
    ap.add_argument("--artifact", action="append", default=[], help="public-name=local-file")
    ap.add_argument("--sources", required=True)
    ap.add_argument("--evidence", required=True)
    ap.add_argument("--previous-index")
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    previous = json.loads(Path(args.previous_index).read_text()) if args.previous_index else None
    index = build_bundle(args.journey_times, dict(x.split("=", 1) for x in args.artifact), args.out,
                         json.loads(Path(args.sources).read_text()), json.loads(Path(args.evidence).read_text()), previous)
    print(index["build_id"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
