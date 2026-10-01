"""Exact code and event inputs for an independently replayable publication."""
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATHS = (
    'scripts/process_snapshots.py', 'api/trip_match.py', 'api/gtfs_rt.py',
    'api/timetable_db.py', 'scripts/observation_contract.py',
    'scripts/reliability_stats.py', 'scripts/recorded_inputs.py',
    'scripts/build_journey_times.py', 'scripts/journey_times_codec.py',
    'scripts/query_reliability.py', 'scripts/build_delay_hotspots.py',
    'scripts/check_published.py', 'scripts/publication_bundle.py',
    'scripts/prepare_reliability_inputs.py', 'scripts/archive_reliability_evidence.py',
    'scripts/analysis_exclusions.py', 'scripts/replay_inputs.py',
    'data/analysis_exclusions.json', 'data/stops.json',
    '.github/workflows/process-snapshots.yml', 'requirements.txt', 'requirements-dev.txt',
)


def snapshot(root=ROOT):
    files = {}
    for name in PATHS:
        raw = (Path(root) / name).read_bytes()
        files[name] = {'sha256': hashlib.sha256(raw).hexdigest(), 'text': raw.decode('utf-8')}
    return {'schema_version': 1, 'files': files}


def restore(document, target):
    """Verify everything before writing; never allow archive path traversal."""
    target = Path(target).resolve()
    verified = []
    for name, entry in document['files'].items():
        path = (target / name).resolve()
        if not path.is_relative_to(target) or Path(name).is_absolute():
            raise ValueError('Unsafe replay path')
        raw = entry['text'].encode('utf-8')
        if hashlib.sha256(raw).hexdigest() != entry['sha256']:
            raise ValueError('Replay input hash mismatch: ' + name)
        verified.append((path, raw))
    for path, raw in verified:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)


if __name__ == '__main__':
    import argparse
    import json
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('snapshot', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    restore(json.loads(args.snapshot.read_text()), args.destination)
