"""Restore the known analysis inputs; a failed download is never a missing day."""
import argparse
from datetime import date, timedelta
import gzip
import json
from pathlib import Path
import re
import shutil
import urllib.request

from observation_contract import digest_file


def prepare(day, current, previous, release_base, repository, out, summaries):
    out = Path(out)
    prior = {s["day"]: dict(s) for s in previous.get("observation_sources", [])}
    end = max([day, *previous.get("days", [])])
    first = (date.fromisoformat(end) - timedelta(days=34)).isoformat()
    relevant = lambda d: first <= d <= end or d[:7] == day[:7]
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
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "bus-reliability-build"}), timeout=120) as response:
                with path.open("wb") as handle:
                    shutil.copyfileobj(response, handle)
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
        if first <= source_day <= end:
            window = out / "window" / name
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
