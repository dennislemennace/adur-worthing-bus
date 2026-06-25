#!/usr/bin/env python3
"""Approve a community idea by adding it to data/suggestions.json.

Each idea submitted through the site's "Ideas" form is emailed to you by
Web3Forms. That email includes a `suggestion_json` line — a ready-to-publish
JSON object. This script takes that blob (or individual fields), guarantees a
unique id, fills the date, forces status="published", appends it to
data/suggestions.json, and re-validates the file.

Usage
-----
  # 1) Paste the suggestion_json blob from the approval email:
  python scripts/add_suggestion.py '{"id":"...","title":"...","body":"..."}'

  # 2) Or pipe it in:
  pbpaste | python scripts/add_suggestion.py        # macOS
  wl-paste | python scripts/add_suggestion.py       # Wayland

  # 3) Or build one interactively (no email blob handy):
  python scripts/add_suggestion.py

Then review the diff and publish:
  git add data/suggestions.json && git commit -m "Publish community idea" && git push

Pass --commit to stage + commit automatically (it never pushes for you).
"""

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SUGGESTIONS = ROOT / "data" / "suggestions.json"

# Keep these in sync with the <select id="sg-area"> options in index.html.
AREAS = ["New route", "More frequency", "Stops & shelters",
         "Accessibility", "Fares", "Other"]


def slugify(text: str) -> str:
    """Mirror of slugify() in app.js: lowercase, non-alphanumerics to hyphens."""
    s = re.sub(r"[^a-z0-9]+", "-", (text or "idea").lower()).strip("-")
    return (s[:48] or "idea")


def load() -> dict:
    if not SUGGESTIONS.exists():
        return {"suggestions": []}
    with SUGGESTIONS.open(encoding="utf-8") as fh:
        data = json.load(fh)
    data.setdefault("suggestions", [])
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
    """Get the idea from an arg, or piped stdin, if present."""
    raw = arg
    if raw is None and not sys.stdin.isatty():
        raw = sys.stdin.read()
    if not raw or not raw.strip():
        return None
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        sys.exit(f"Could not parse JSON: {exc}\nPaste the suggestion_json line from the email.")
    if not isinstance(obj, dict):
        sys.exit("Expected a single JSON object.")
    return obj


def prompt_fields() -> dict:
    print("No JSON given — enter the idea (Ctrl-C to cancel).\n")
    title = input("Title (one line): ").strip()
    body = input("Details: ").strip()
    print(f"Area options: {', '.join(AREAS)}")
    area = input("Area [Other]: ").strip() or "Other"
    name = input("Name (optional): ").strip()
    return {"title": title, "body": body, "area": area, "name": name}


def normalise(obj: dict, existing_ids: set) -> dict:
    title = (obj.get("title") or "").strip()
    body = (obj.get("body") or obj.get("details") or "").strip()
    if not title:
        sys.exit("Missing 'title'.")
    if not body:
        sys.exit("Missing 'body' (the idea's details).")
    entry = {
        "id": unique_id(obj.get("id") or title, existing_ids),
        "title": title,
        "body": body,
        "area": (obj.get("area") or "Other").strip(),
        "name": (obj.get("name") or "").strip(),
        "date": (obj.get("date") or date.today().isoformat()).strip(),
        "status": "published",  # only published ideas belong in the feed
    }
    return entry


def validate(data: dict) -> None:
    ids = [s["id"] for s in data["suggestions"]]
    dupes = {i for i in ids if ids.count(i) > 1}
    if dupes:
        sys.exit(f"Validation failed: duplicate id(s): {sorted(dupes)}")
    for s in data["suggestions"]:
        for key in ("id", "title", "body", "status"):
            if not str(s.get(key, "")).strip():
                sys.exit(f"Validation failed: entry {s.get('id')!r} missing '{key}'")
        if s["status"] != "published":
            sys.exit(f"Validation failed: {s['id']!r} status must be 'published'")


def main() -> None:
    ap = argparse.ArgumentParser(description="Approve a community idea into data/suggestions.json")
    ap.add_argument("blob", nargs="?", help="suggestion_json from the approval email")
    ap.add_argument("--commit", action="store_true", help="git add + commit (does not push)")
    args = ap.parse_args()

    data = load()
    existing = {s.get("id") for s in data["suggestions"]}

    obj = read_blob(args.blob) or prompt_fields()
    entry = normalise(obj, existing)
    data["suggestions"].append(entry)
    validate(data)

    SUGGESTIONS.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"\nAdded idea '{entry['id']}' — {SUGGESTIONS.relative_to(ROOT)} now has "
          f"{len(data['suggestions'])} published idea(s).")

    if args.commit:
        import subprocess
        subprocess.run(["git", "add", str(SUGGESTIONS)], cwd=ROOT, check=True)
        subprocess.run(["git", "commit", "-m", f"Publish community idea: {entry['title']}"],
                       cwd=ROOT, check=True)
        print("Committed. Run `git push` to publish.")
    else:
        print("Review it, then:\n"
              "  git add data/suggestions.json && git commit -m \"Publish community idea\" && git push")


if __name__ == "__main__":
    main()
