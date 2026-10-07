"""Which published generations to retire, so the public bucket never fills.

Every nightly run uploads a complete generation of the journey-times window
under `journey-times/builds/<sha256>/` and never deletes one. At a full 35-day
window a generation is hundreds of megabytes, and the bucket's 4 GiB budget
would stop publication within weeks of the window filling.

The rule: keep the newest `--keep` generations by upload time, and always keep
the one the live index points at and the one its rollback names, however old.
In --objects mode, exact dependencies of every retained index survive too,
without retaining all the old generation's large route files. Retirement breaks any
link pinned to it with `?jt-build=`, which is why the count is a setting and
the default is a week.

This script only decides. It reads an `aws s3 ls --recursive` listing and the
live index, and prints candidates, one per line. The workflow uses --objects
and reads retained manifests via --endpoint before printing object keys.
Anything it cannot parse is kept; an unreadable manifest prevents deletion.

    aws s3 ls s3://adur-worthing-published/journey-times/builds/ --recursive > listing.txt
    python scripts/prune_published.py --listing listing.txt --index index.json --keep 7
"""

import argparse
import json
import re
import sys
import subprocess
from pathlib import Path

BUILD_KEY = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+\d+\s+journey-times/builds/([a-f0-9]{64})/\S+$")


def generations(listing_text):
    """`{build id: newest upload time}` from an `aws s3 ls --recursive` listing."""
    newest = {}
    for line in listing_text.splitlines():
        m = BUILD_KEY.match(line.strip())
        if m:
            when, build = m.groups()
            newest[build] = max(newest.get(build, ""), when)
    return newest


def protected(index):
    """The live generation, its rollback target, and every build it links to."""
    keep = current_builds(index)
    keep.update(re.search(r'builds/([a-f0-9]{64})/', key).group(1)
                for key in references(index))
    return {b for b in keep if b}


def references(value):
    """Exact local object dependencies, including nested artifact entries."""
    if isinstance(value, dict):
        return set().union(*(references(v) for v in value.values()), set())
    if isinstance(value, list):
        return set().union(*(references(v) for v in value), set())
    if isinstance(value, str) and re.fullmatch(
            r'(?:journey-times/)?builds/[a-f0-9]{64}/[A-Za-z0-9_.-]+', value):
        return {value if value.startswith('journey-times/') else 'journey-times/' + value}
    return set()


def retained_builds(listing_text, index, keep):
    if keep < 2:
        raise ValueError('keep at least the live generation and its rollback')
    builds = generations(listing_text)
    return set(sorted(builds, key=lambda b: (builds[b], b), reverse=True)[:keep]) | {
        b for b in (index.get('build_id'), index.get('previous_build')) if b}


# Evidence rebuilt in full every night and archived in that run's release
# (publication.tar.gz). Only the live generation and its rollback keep a copy in
# the bucket: on 6 October 2026 seven retained copies of the delay map's, then
# 559 MB each uncompressed, filled the 4 GiB budget and stopped publication.
NIGHTLY_EVIDENCE = frozenset({"hotspot-preview.json", "hotspot-preview.json.gz"})


def current_builds(index):
    """The live generation and the one it rolls back to."""
    keep = {index.get("build_id"), index.get("previous_build")}
    m = re.search(r"builds/([a-f0-9]{64})/", index.get("rollback_index") or "")
    if m:
        keep.add(m.group(1))
    return {b for b in keep if b}


def deletable_objects(listing_text, index, manifests=(), keep=7):
    """Retire large old route files without retiring still-linked evidence.

    The caller supplies the indices of all retained generations. Their exact
    dependencies survive even where the rest of an old generation is retired.
    Historical build-pinned route links still have the documented finite life.
    """
    retained = retained_builds(listing_text, index, keep)
    required = set().union(*(references(m) for m in [index, *manifests]))
    current = current_builds(index)
    live_refs = references(index)
    doomed = []
    for line in listing_text.splitlines():
        match = BUILD_KEY.fullmatch(line.strip())
        if match:
            key = line.split()[-1]
            if match.group(2) not in retained and key not in required:
                doomed.append(key)
            elif (key.rsplit("/", 1)[-1] in NIGHTLY_EVIDENCE and match.group(2) not in current
                  and key not in live_refs):
                doomed.append(key)
    return sorted(doomed)


def remote_manifests(listing_text, index, keep, endpoint):
    """Bounded reads only; an unreadable retained index prevents all deletion."""
    out = []
    for build in sorted(retained_builds(listing_text, index, keep)):
        raw = subprocess.check_output([
            'aws', 's3', 'cp',
            f's3://adur-worthing-published/journey-times/builds/{build}/index.json',
            '-', '--endpoint-url', endpoint, '--only-show-errors'])
        doc = json.loads(raw)
        if doc.get('build_id') != build:
            raise ValueError('Retained manifest names a different generation')
        out.append(doc)
    return out


def to_delete(listing_text, index, keep=7):
    if keep < 2:
        raise ValueError("keep at least the live generation and its rollback")
    builds = generations(listing_text)
    newest_first = sorted(builds, key=lambda b: (builds[b], b), reverse=True)
    kept = set(newest_first[:keep]) | protected(index)
    return sorted(b for b in builds if b not in kept)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--listing", required=True)
    ap.add_argument("--index", required=True, help="the live journey-times/index.json")
    ap.add_argument("--keep", type=int, default=7)
    ap.add_argument('--objects', action='store_true', help='print individual unreferenced keys')
    ap.add_argument('--endpoint', help='R2 endpoint; required to verify retained manifests in object mode')
    args = ap.parse_args(argv)
    index = json.loads(Path(args.index).read_text())
    if not index.get("build_id"):
        print("the live index names no build: deleting nothing", file=sys.stderr)
        return 1
    listing_text = Path(args.listing).read_text()
    if args.objects:
        if not args.endpoint:
            raise ValueError('Object retirement requires all retained remote manifests')
        manifests = remote_manifests(listing_text, index, args.keep, args.endpoint)
        candidates = deletable_objects(listing_text, index, manifests, args.keep)
    else:
        candidates = to_delete(listing_text, index, args.keep)
    for build in candidates:
        print(build)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
