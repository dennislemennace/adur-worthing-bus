#!/usr/bin/env python3
"""Gather a day's-worth of served-estimate scores for every day in the window,
and add them up.

Each night scores one day (scripts/score_served_estimates.py) from that day's
raw feeds, which exist only on the night itself. The window's other days come
from the previous generation, which publishes each day's scores as
`served-<day>.json` (checked against the previous index's hash), or, for days
scored before the nightly step existed, from the backfill committed in
`data/reliability/served/`. A day found nowhere is left out and named, not
guessed: the merged file lists the days it covers.

    python scripts/collect_served_window.py --window inputs/window \\
        --tonight served-2026-10-26.json --previous-index previous-index.json \\
        --base https://.../journey-times --seed data/reliability/served \\
        --out-dir served --merged served-accuracy.json
"""
import argparse
import hashlib
import json
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import score_served_estimates as ss                               # noqa: E402

DAY = re.compile(r"observations-(\d{4}-\d{2}-\d{2})\.json(?:\.gz)?")


def window_days(window_dir):
    return sorted(m.group(1) for p in Path(window_dir).iterdir() if (m := DAY.fullmatch(p.name)))


def fetch_published(base, entry):
    """The previous generation's copy, or None if it cannot be had intact."""
    try:
        req = urllib.request.Request(f"{base}/{entry['file']}", headers={"User-Agent": "bus-reliability-build"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
    except Exception:                            # noqa: BLE001 — a missing day is named, not fatal
        return None
    if entry.get("sha256") and hashlib.sha256(raw).hexdigest() != entry["sha256"]:
        return None
    return raw


def collect(days, tonight, previous_artifacts, seed_dir, fetch):
    """`({day: raw bytes}, [days found nowhere])`."""
    found, missing = {}, []
    tonight_doc = json.loads(Path(tonight).read_bytes()) if tonight else None
    for day in days:
        if tonight_doc and tonight_doc.get("days") == [day]:
            found[day] = Path(tonight).read_bytes()
            continue
        entry = previous_artifacts.get(f"served-{day}.json")
        raw = fetch(entry) if entry else None
        if raw is None and seed_dir and (Path(seed_dir) / f"served-{day}.json").exists():
            raw = (Path(seed_dir) / f"served-{day}.json").read_bytes()
        if raw is None:
            missing.append(day)
        else:
            found[day] = raw
    return found, missing


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--window", required=True, help="the folder of the window's observation files")
    ap.add_argument("--tonight", help="tonight's served-<day>.json")
    ap.add_argument("--previous-index")
    ap.add_argument("--base", help="where the previous index's files are served from")
    ap.add_argument("--seed", default=str(ROOT / "data" / "reliability" / "served"))
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--merged", required=True)
    args = ap.parse_args(argv)
    previous = json.loads(Path(args.previous_index).read_text()) if args.previous_index else {}
    found, missing = collect(window_days(args.window), args.tonight, previous.get("artifacts") or {},
                             args.seed, lambda e: fetch_published(args.base, e) if args.base else None)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for day, raw in found.items():
        (out / f"served-{day}.json").write_bytes(raw)
    if not found:
        print("No served-estimate scores for any day in the window; nothing merged.")
        return 0
    doc = ss.merge([json.loads(raw) for raw in found.values()])
    doc["days_missing"] = missing
    doc["as_of"] = max(json.loads(raw).get("as_of", "") for raw in found.values())
    Path(args.merged).write_text(json.dumps(doc, indent=1, sort_keys=True) + "\n")
    print(f"Served-estimate scores: {len(found)} days merged; missing {', '.join(missing) or 'none'}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
