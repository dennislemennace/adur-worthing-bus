#!/usr/bin/env python3
"""
Add a Network Updates article.

    python scripts/add_update.py                      # type it in
    python scripts/add_update.py --community          # publish a passenger report
    python scripts/add_update.py '{"title": "...", "body": "..."}'
    wl-paste | python scripts/add_update.py

Writes to data/updates.json (official) or data/community_updates.json
(community), gives the entry an id and today's date, forces status
"published", and re-validates the whole file before saving. Nothing is
committed or pushed — review the diff, then commit.

An article may carry an `image`: put the file in media/updates/ first, then
give its repo-relative path. The path is checked against the working tree
here, because a hero that 404s is only visible once the page is live.

The two files are kept apart for the same reason objectives and ideas are:
one is written here, the other comes from passengers and is published only
after somebody has read it.
"""
import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OFFICIAL = ROOT / "data" / "updates.json"
COMMUNITY = ROOT / "data" / "community_updates.json"


def slugify(title: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return s[:60] or "update"


def unique_id(base: str, taken: set) -> str:
    if base not in taken:
        return base
    n = 2
    while f"{base}-{n}" in taken:
        n += 1
    return f"{base}-{n}"


def prompt(label: str, *, required: bool = True, multiline: bool = False) -> str:
    if multiline:
        print(f"{label} (blank line to finish):")
        lines = []
        while True:
            try:
                line = input()
            except EOFError:
                break
            if not line and lines:
                break
            lines.append(line)
        value = "\n".join(lines).strip()
    else:
        value = input(f"{label}: ").strip()
    if required and not value:
        sys.exit(f"'{label}' is required.")
    return value


def load(path: Path) -> dict:
    if not path.exists():
        sys.exit(f"{path} is missing.")
    return json.loads(path.read_text())


def validate(doc: dict, path: Path, community: bool) -> None:
    """A subset of tests/test_curated_data.py, run before we write rather
    than after — a bad file that only fails in CI has already been saved."""
    seen = set()
    for u in doc["updates"]:
        uid = u.get("id", "?")
        for field in ("id", "title", "summary", "body", "status", "date"):
            if not isinstance(u.get(field), str) or not u[field].strip():
                sys.exit(f"{path.name}: '{uid}' needs a non-empty '{field}'")
        if u["status"] != "published":
            sys.exit(f"{path.name}: '{uid}' status must be 'published'")
        date.fromisoformat(u["date"])
        if uid in seen:
            sys.exit(f"{path.name}: duplicate id '{uid}'")
        seen.add(uid)
        for link in u.get("links", []):
            if not link.get("label") or not link.get("url"):
                sys.exit(f"{path.name}: '{uid}' has a link missing label or url")
        image = u.get("image")
        if image is not None:
            src = image.get("src", "")
            if not src or src.startswith(("/", "http://", "https://")):
                sys.exit(f"{path.name}: '{uid}' image src must be a relative "
                         f"repo path, e.g. media/updates/name.jpg")
            if not (ROOT / src).exists():
                sys.exit(f"{path.name}: '{uid}' image {src} is not in the repo")
            if not str(image.get("alt", "")).strip():
                sys.exit(f"{path.name}: '{uid}' image needs 'alt' — the picture "
                         f"carries the article, so it is not decoration")


def main() -> None:
    ap = argparse.ArgumentParser(description="Add a Network Updates article.")
    ap.add_argument("payload", nargs="?", help="JSON object, or omit to be prompted")
    ap.add_argument("--community", action="store_true",
                    help="publish into community_updates.json instead")
    args = ap.parse_args()

    path = COMMUNITY if args.community else OFFICIAL
    doc = load(path)

    raw = args.payload
    if raw is None and not sys.stdin.isatty():
        raw = sys.stdin.read().strip() or None

    if raw:
        try:
            entry = json.loads(raw)
        except json.JSONDecodeError as err:
            sys.exit(f"That isn't valid JSON: {err}")
    else:
        entry = {
            "title":   prompt("Title"),
            "summary": prompt("One-line summary"),
            "body":    prompt("Body — blank line between paragraphs", multiline=True),
        }
        if args.community:
            entry["name"] = prompt("Reported by (blank for anonymous)", required=False)
        topic = prompt("Topic tag, e.g. Fares (blank for none)", required=False)
        if topic:
            entry["topic"] = topic
        img = prompt("Image path under media/updates/ (blank for none)", required=False)
        if img:
            entry["image"] = {
                "src": img,
                "alt": prompt("Describe the image for anyone who cannot see it"),
                "focus": prompt("Focus, a CSS object-position (blank for centre)",
                                required=False) or "50% 50%",
                "credit": prompt("Credit — whose photograph is it? (blank for none)",
                                 required=False),
            }
        url = prompt("Source link URL (blank for none)", required=False)
        if url:
            entry["links"] = [{"label": prompt("Source link label"), "url": url}]

    entry.setdefault("date", date.today().isoformat())
    entry["status"] = "published"
    entry["id"] = unique_id(
        entry.get("id") or slugify(entry.get("title", "")),
        {u["id"] for u in doc["updates"]},
    )
    if not entry.get("name"):
        entry.pop("name", None)

    doc["updates"].append(entry)
    validate(doc, path, args.community)

    path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")
    kind = "community" if args.community else "official"
    print(f"Added {kind} update '{entry['id']}' — {path.name} now has "
          f"{len(doc['updates'])} article(s).")
    print("Review the diff, then commit. Nothing has been pushed.")


if __name__ == "__main__":
    main()
