"""Which published generations to retire, so the public bucket never fills.

Every nightly run uploads a complete generation of the journey-times window
under `journey-times/builds/<sha256>/` and never deletes one. At a full 35-day
window a generation is hundreds of megabytes, and the bucket's 4 GiB budget
would stop publication within weeks of the window filling.

The rule: keep the newest `--keep` generations by upload time, and always keep
the one the live index points at and the one its rollback names, however old.
Everything else under `builds/` goes. A generation that is deleted breaks any
link pinned to it with `?jt-build=`, which is why the count is a setting and
the default is a week.

This script only decides. It reads an `aws s3 ls --recursive` listing and the
live index, and prints the build ids to delete, one per line; the workflow
deletes them. Anything it cannot parse is kept.

    aws s3 ls s3://adur-worthing-published/journey-times/builds/ --recursive > listing.txt
    python scripts/prune_published.py --listing listing.txt --index index.json --keep 7
"""

import argparse
import json
import re
import sys
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
    """The live generation and its rollback target."""
    keep = {index.get("build_id"), index.get("previous_build")}
    rollback = index.get("rollback_index") or ""
    m = re.search(r"builds/([a-f0-9]{64})/", rollback)
    if m:
        keep.add(m.group(1))
    return {b for b in keep if b}


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
    args = ap.parse_args(argv)
    index = json.loads(Path(args.index).read_text())
    if not index.get("build_id"):
        print("the live index names no build: deleting nothing", file=sys.stderr)
        return 1
    for build in to_delete(Path(args.listing).read_text(), index, args.keep):
        print(build)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
