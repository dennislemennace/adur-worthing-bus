#!/usr/bin/env python3
"""Approve a community route proposal by adding it to data/proposals.json.

Route proposals drawn in the site's in-app editor are filed as GitHub issues by
the submission Worker (see worker/README.md). The issue body carries a
ready-to-publish JSON object. This script takes that blob (or an issue number),
guarantees a unique id, forces a safe category, validates the geometry, appends
it to data/proposals.json and re-validates the whole file.

This is the moderation gate: submissions sit as issues until someone runs this.

Ideas already had `add_suggestion.py` and updates `add_update.py`; proposals
were the odd one out, published by hand-pasting a JSON object with a couple of
hundred coordinates in it into a data file. That is easy to corrupt, and
nothing checked it until pytest ran, which is late.

Two things this refuses to do silently
--------------------------------------
**It will not mark a submission "official".** `isOfficialProposal()` in app.js
treats `category == "official"` as project-maintained and draws those lines on
the Improvements map automatically; anything else stays hidden until a reader
clicks it in. A community route promoted to official by accident would be the
site presenting someone else's proposal as its own. So the default is
`community`, and `--official` has to be typed.

**It will not accept geometry it cannot place.** Latitude and longitude are two
numbers of similar magnitude here once you drop the sign, so a swapped pair
produces a perfectly valid-looking list that draws a line into the Indian
Ocean. Every coordinate is bounds-checked, and a swap is named as such rather
than reported as "out of area".

Usage
-----
  # 1) Straight from the issue (needs the `gh` CLI, authenticated):
  python scripts/add_proposal.py --from-issue 2

  # 2) Paste the JSON blob out of the issue body:
  python scripts/add_proposal.py '{"id":"...","name":"...","summary":"..."}'

  # 3) Or pipe it in:
  wl-paste | python scripts/add_proposal.py

Then review the diff and publish:
  git add data/proposals.json && git commit -m "Publish community proposal" && git push

Pass --commit to stage + commit automatically (it never pushes for you).
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROPOSALS = ROOT / "data" / "proposals.json"

# Generous enough for any plausible extension of the corridor - the published
# proposals span 50.81..50.89 / -0.37..-0.13 - and tight enough that a swapped
# lat/lon pair (which would read as lat -0.25, lon 50.83) cannot get through.
LAT_RANGE = (50.5, 51.2)
LON_RANGE = (-1.2, 0.4)

# "official" is the project's own; anything else is somebody else's idea that a
# reader has to opt into seeing. See isOfficialProposal() in app.js.
CATEGORIES = ("community", "official")

# app.js only distinguishes "limited" (hidden behind the Show limited services
# toggle) from everything else, and the in-app editor always emits
# frequent_all_day. Both are accepted; an unknown value is a typo worth
# catching, because it would land silently in the "not limited" bucket.
FREQUENCY_CLASSES = ("frequent_all_day", "limited")

HEX_COLOUR = re.compile(r"^#[0-9a-fA-F]{6}$")

# Carried through from the submission, beyond the required three.
OPTIONAL_FIELDS = (
    "description", "color", "frequency_class", "polyline", "polylines",
    "stops", "from", "to", "links", "is_night",
)


def slugify(text: str) -> str:
    """Mirror of slugify() in app.js: lowercase, non-alphanumerics to hyphens."""
    s = re.sub(r"[^a-z0-9]+", "-", (text or "proposal").lower()).strip("-")
    return (s[:48] or "proposal")


def load() -> dict:
    if not PROPOSALS.exists():
        return {"proposals": []}
    with PROPOSALS.open(encoding="utf-8") as fh:
        data = json.load(fh)
    data.setdefault("proposals", [])
    return data


def unique_id(base: str, existing: set) -> str:
    base = slugify(base)
    if base not in existing:
        return base
    n = 2
    while f"{base}-{n}" in existing:
        n += 1
    return f"{base}-{n}"


def read_blob(arg: str | None) -> dict | None:
    """Get the proposal from an argument, or piped stdin, if present."""
    raw = arg
    if raw is None and not sys.stdin.isatty():
        raw = sys.stdin.read()
    if not raw or not raw.strip():
        return None
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        sys.exit(f"Could not parse JSON: {exc}\nPaste the json block from the issue body.")
    if not isinstance(obj, dict):
        sys.exit("Expected a single JSON object.")
    return obj


def read_issue(number: int) -> dict:
    """Pull the publishable JSON out of a submission issue, via the gh CLI.

    Takes the last fenced json block in the body. The proposal's own JSON is
    the only one there, but "last" is the safer pick if a commenter ever pastes
    another into the description.
    """
    try:
        out = subprocess.run(
            ["gh", "issue", "view", str(number), "--json", "body,title,state"],
            cwd=ROOT, check=True, capture_output=True, text=True,
        ).stdout
    except FileNotFoundError:
        sys.exit("The `gh` CLI isn't installed — see https://cli.github.com")
    except subprocess.CalledProcessError as exc:
        sys.exit(f"Could not read issue #{number}:\n{exc.stderr.strip()}")

    issue = json.loads(out)
    if issue.get("state") == "CLOSED":
        print(f"Note: issue #{number} is already closed.")

    blocks = re.findall(r"```json\s*\n(.*?)\n```", issue.get("body") or "", re.S)
    if not blocks:
        sys.exit(f"Issue #{number} has no ```json block — is it a route proposal? "
                 f"Ideas go through scripts/add_suggestion.py.")
    try:
        obj = json.loads(blocks[-1])
    except json.JSONDecodeError as exc:
        sys.exit(f"The JSON block in issue #{number} didn't parse: {exc}")
    if not isinstance(obj, dict):
        sys.exit(f"Issue #{number} JSON block is not a single object.")
    return obj


def close_issue(number: int, entry_id: str) -> None:
    """Close a published issue and drop its `unverified` label.

    Never fatal — the proposal is already saved by this point, and a tracker
    that disagrees with the site is a smaller problem than losing the work.

    The label matters: `unverified` is what the Worker stamps on everything it
    files, and it is the flag that says "nothing here has been looked at".
    Leaving it on something now live on the site tells the next person to
    read the tracker exactly the wrong thing.
    """
    try:
        subprocess.run(
            ["gh", "issue", "close", str(number),
             "--comment", f"Published to the site as `{entry_id}`. Thanks!"],
            cwd=ROOT, check=True, capture_output=True, text=True,
        )
        print(f"Closed issue #{number}.")
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        detail = getattr(exc, "stderr", "") or str(exc)
        print(f"Note: couldn't close issue #{number} ({detail.strip()}). "
              f"The proposal was still added — close it by hand.")
        return

    try:
        subprocess.run(
            ["gh", "issue", "edit", str(number), "--remove-label", "unverified"],
            cwd=ROOT, check=True, capture_output=True, text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        detail = getattr(exc, "stderr", "") or str(exc)
        print(f"Note: issue #{number} still carries the 'unverified' label "
              f"({detail.strip()}) — remove it by hand.")


def check_point(pt, where: str) -> None:
    """One [lat, lon] pair, inside the area this site covers."""
    if not isinstance(pt, (list, tuple)) or len(pt) != 2:
        sys.exit(f"{where}: expected a [lat, lon] pair, got {pt!r}")
    lat, lon = pt
    if not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in (lat, lon)):
        sys.exit(f"{where}: coordinates must be numbers, got {pt!r}")
    if not (LAT_RANGE[0] <= lat <= LAT_RANGE[1] and LON_RANGE[0] <= lon <= LON_RANGE[1]):
        hint = ""
        if LAT_RANGE[0] <= lon <= LAT_RANGE[1] and LON_RANGE[0] <= lat <= LON_RANGE[1]:
            hint = " — that reads as [lon, lat]; this format is [lat, lon]"
        sys.exit(f"{where}: {pt!r} is outside Adur, Worthing and Brighton{hint}")


def check_geometry(entry: dict) -> None:
    lines = []
    if entry.get("polyline"):
        lines.append(("polyline", entry["polyline"]))
    for i, line in enumerate(entry.get("polylines") or []):
        lines.append((f"polylines[{i}]", line))
    if not lines:
        sys.exit("No 'polyline' — a route proposal with no line cannot be drawn.")

    for label, line in lines:
        if not isinstance(line, list) or len(line) < 2:
            sys.exit(f"{label}: needs at least two points to draw a line.")
        for i, pt in enumerate(line):
            check_point(pt, f"{label}[{i}]")

    for i, stop in enumerate(entry.get("stops") or []):
        if not isinstance(stop, dict):
            sys.exit(f"stops[{i}]: expected an object, got {stop!r}")
        if not str(stop.get("name", "")).strip():
            sys.exit(f"stops[{i}]: a stop with no name cannot be labelled on the map")
        check_point([stop.get("lat"), stop.get("lon")], f"stops[{i}] ({stop.get('name')})")


def normalise(obj: dict, existing_ids: set, category: str) -> dict:
    name = (obj.get("name") or "").strip()
    summary = (obj.get("summary") or "").strip()
    if not name:
        sys.exit("Missing 'name'.")
    if not summary:
        sys.exit("Missing 'summary' — the one line shown on the proposal card.")

    entry = {
        "id": unique_id(obj.get("id") or name, existing_ids),
        "name": name,
        "summary": summary,
        # Set here, never read from the submission: the blob is written by
        # whoever filled in the form, and category is the one field that
        # decides whether the site presents a route as its own.
        "category": category,
    }
    for key in OPTIONAL_FIELDS:
        value = obj.get(key)
        if value not in (None, "", [], {}):
            entry[key] = value

    colour = entry.get("color")
    if colour and not HEX_COLOUR.match(str(colour)):
        sys.exit(f"'color' must be a #rrggbb hex colour, got {colour!r}")
    freq = entry.get("frequency_class")
    if freq and freq not in FREQUENCY_CLASSES:
        sys.exit(f"'frequency_class' {freq!r} is not one of {list(FREQUENCY_CLASSES)}")

    check_geometry(entry)
    return entry


def validate(data: dict) -> None:
    """Re-check the whole file, not just the new entry — the same contract as
    tests/test_curated_data.py::test_proposals_schema, so a mistake fails here
    rather than in CI."""
    ids = [p.get("id") for p in data["proposals"]]
    dupes = {i for i in ids if ids.count(i) > 1}
    if dupes:
        sys.exit(f"Validation failed: duplicate id(s): {sorted(dupes)}")
    for p in data["proposals"]:
        for key in ("id", "name", "summary"):
            if not str(p.get(key, "")).strip():
                sys.exit(f"Validation failed: entry {p.get('id')!r} missing '{key}'")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Approve a community route proposal into data/proposals.json")
    ap.add_argument("blob", nargs="?", help="publishable JSON from the issue body")
    ap.add_argument("--from-issue", type=int, metavar="N",
                    help="read the proposal straight from GitHub issue N "
                         "(needs the gh CLI), and close it once published")
    ap.add_argument("--no-close", action="store_true",
                    help="with --from-issue, leave the issue open")
    ap.add_argument("--official", action="store_true",
                    help="publish as a project proposal, drawn on the map by "
                         "default. Only for routes this project is itself "
                         "putting forward — a community submission is not one.")
    ap.add_argument("--commit", action="store_true", help="git add + commit (does not push)")
    args = ap.parse_args()

    data = load()
    existing = {p.get("id") for p in data["proposals"]}

    if args.from_issue is not None:
        obj = read_issue(args.from_issue)
    else:
        obj = read_blob(args.blob)
    if obj is None:
        sys.exit("Nothing to publish. Pass --from-issue N, a JSON blob, or pipe one in.")

    category = "official" if args.official else "community"
    entry = normalise(obj, existing, category)
    data["proposals"].append(entry)
    validate(data)

    PROPOSALS.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n",
                         encoding="utf-8")

    stops = len(entry.get("stops") or [])
    points = len(entry.get("polyline") or [])
    print(f"\nAdded proposal '{entry['id']}' as {category} — "
          f"{points} points, {stops} stops.")
    print(f"{PROPOSALS.relative_to(ROOT)} now holds {len(data['proposals'])} proposal(s).")
    if category == "community":
        print("It stays off the map until a reader opens it, which is what "
              "'community' means here.")

    # Close only after the file is safely written — a failed close must never
    # cost us the proposal.
    if args.from_issue is not None and not args.no_close:
        close_issue(args.from_issue, entry["id"])

    if args.commit:
        subprocess.run(["git", "add", str(PROPOSALS)], cwd=ROOT, check=True)
        subprocess.run(["git", "commit", "-m",
                        f"Publish community proposal: {entry['name']}"],
                       cwd=ROOT, check=True)
        print("Committed. Run `git push` to publish.")
    else:
        print("Review it, then:\n"
              "  git add data/proposals.json && "
              "git commit -m \"Publish community proposal\" && git push")


if __name__ == "__main__":
    main()
