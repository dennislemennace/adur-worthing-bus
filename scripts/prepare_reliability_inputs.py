"""Restore the known analysis inputs; a failed download is never a missing day.

Three folders come out: `window`, about the last five weeks, which every
nightly builder reads; `window-long`, the last nine weeks, which only the delay
map reads (scripts/build_delay_hotspots.py), because a stretch's quieter hours
need that long to reach its floor of 30 journeys over 5 days (8 November 2026:
35% of drawn cells after five weeks, and flat); and `month`, the day's month.
"""
import argparse
from datetime import date, timedelta
import gzip
import http.client
import json
from pathlib import Path
import re
import shutil
import time
import urllib.error
import urllib.request

from observation_contract import digest_file

WINDOW_DAYS = 35
LONG_WINDOW_DAYS = 63

# A single "504 Gateway Time-out" from a release download failed the whole
# night on 8 November 2026 (run 42471232953). Server errors, timeouts and
# dropped connections are tried again, a little later each time; a 404 or 403
# is an answer, and fails at once.
TRIES = 4
BACKOFF_SECS = (5, 20, 60)


def _worth_retrying(err):
    if isinstance(err, urllib.error.HTTPError):
        return err.code >= 500 or err.code == 429
    return isinstance(err, (urllib.error.URLError, TimeoutError, ConnectionError,
                            http.client.IncompleteRead))


def download(url, path, pause=None):
    for attempt in range(TRIES):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "bus-reliability-build"})
            with urllib.request.urlopen(request, timeout=120) as response:
                with path.open("wb") as handle:
                    shutil.copyfileobj(response, handle)
            return
        except Exception as err:                                  # noqa: BLE001
            if attempt == TRIES - 1 or not _worth_retrying(err):
                raise
            wait = BACKOFF_SECS[min(attempt, len(BACKOFF_SECS) - 1)]
            print(f"::warning::{url} failed ({err}); trying again in {wait}s", flush=True)
            (pause or time.sleep)(wait)


def prepare(day, current, previous, release_base, repository, out, summaries):
    out = Path(out)
    prior = {s["day"]: dict(s) for s in previous.get("observation_sources", [])}
    end = max([day, *previous.get("days", [])])
    first = (date.fromisoformat(end) - timedelta(days=WINDOW_DAYS - 1)).isoformat()
    first_long = (date.fromisoformat(end) - timedelta(days=LONG_WINDOW_DAYS - 1)).isoformat()
    relevant = lambda d: first_long <= d <= end or d[:7] == day[:7]
    expected = {d for d in prior if relevant(d)}
    # Dated summaries establish legacy days; preserve the complete immutable
    # catalog so reprocessing an old month cannot silently shrink that month.
    expected |= {p.stem for p in Path(summaries).glob("*.json")
                 if re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.stem) and relevant(p.stem)}
    expected.add(day)
    catalog = dict(prior)
    for source_day in sorted(expected):
        name = f"observations-{source_day}.json.gz"
        path = out / "restored" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        source = prior.get(source_day, {})
        if source_day == day:
            shutil.copyfile(current, path)
        else:
            url = source.get("url") or f"https://github.com/{repository}/releases/download/reliability-{source_day[:7]}/{name}"
            download(url, path)
            if source.get("sha256") and digest_file(path) != source["sha256"]:
                raise ValueError(f"Published input changed: {source_day}")
        doc = json.loads(gzip.decompress(path.read_bytes()))
        if doc.get("day") != source_day:
            raise ValueError(f"Wrong observation day: {name}")
        if source_day == day or not source.get("immutable"):
            upload = out / "publish-assets" / name
            upload.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, upload)
            source["url"] = f"{release_base}/{name}"
        source.update(day=source_day, sha256=digest_file(path), immutable=True,
                      method_version=doc.get("method_version", "unknown"),
                      data_version=doc.get("data_version", "unknown"))
        catalog[source_day] = source
        for folder, start in (("window", first), ("window-long", first_long)):
            if start <= source_day <= end:
                window = out / folder / name
                window.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, window)
        if source_day[:7] == day[:7]:
            month = out / "month" / name
            month.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, month)
    sources = [catalog[d] for d in sorted(catalog)]
    (out / "sources.json").write_text(json.dumps(sources, indent=1, sort_keys=True) + "\n")
    return sources


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    for name in ("day", "current", "previous-index", "release-base", "repository", "out"):
        ap.add_argument(f"--{name}", required=True)
    ap.add_argument("--summaries", default="data/reliability")
    args = ap.parse_args(argv)
    prepare(args.day, args.current, json.loads(Path(args.previous_index).read_text()),
            args.release_base, args.repository, args.out, args.summaries)


if __name__ == "__main__":
    main()
