#!/usr/bin/env python3
"""Approve a community idea by adding it to data/suggestions.json.

Each idea submitted through the site's "Ideas" form is filed as a GitHub issue
by the submission Worker (see worker/README.md). The issue body carries a
ready-to-publish JSON object. This script takes that blob (or an issue number,
or individual fields), guarantees a unique id, fills the date, forces
status="published", appends it to data/suggestions.json, and re-validates.

This is the moderation gate: submissions sit as issues until someone runs this.

Usage
-----
  # 1) Straight from the issue (needs the `gh` CLI, authenticated):
  python scripts/add_suggestion.py --from-issue 42

  # 2) Paste the JSON blob out of the issue body:
  python scripts/add_suggestion.py '{"id":"...","title":"...","body":"..."}'

  # 3) Or pipe it in:
  pbpaste | python scripts/add_suggestion.py        # macOS
  wl-paste | python scripts/add_suggestion.py       # Wayland

  # 4) Or build one interactively:
  python scripts/add_suggestion.py

Then review the diff and publish:
  git add data/suggestions.json && git commit -m "Publish community idea" && git push

Pass --commit to stage + commit automatically (it never pushes for you).
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SUGGESTIONS = ROOT / "data" / "suggestions.json"

# Keep these in sync with the <select id="sg-area"> options in index.html.
AREAS = ["New route", "More frequency", "Stops & shelters",
         "Accessibility", "Fares", "Other"]

# Who would have to act. Kept in sync with RESPONSIBLE_BODIES in app.js.
BODIES = {
    "SCSO": "Stagecoach South",
    "BHBC": "Brighton & Hove Buses",
    "METR": "Metrobus",
    "COMT": "Compass Travel",
    "WSCC": "West Sussex County Council",
    "ESCC": "East Sussex County Council",
    "BHCC": "Brighton & Hove City Council",
    "ADUR_WORTHING": "Adur & Worthing Councils",
}

# The area a submitter picked hints at who owns the problem, but only hints:
# a "New route" could be a commercial decision or a council-supported service.
# So this is offered as a default to accept or override, never applied silently
# — an idea filed against the wrong body sends someone to the wrong place.
AREA_HINTS = {
    "Stops & shelters": "WSCC",
    "Accessibility":    "WSCC",
    "Fares":            "SCSO",
}


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


def read_issue(number: int) -> dict:
    """Pull the publishable JSON out of a submission issue, via the gh CLI.

    The Worker writes the blob inside a ```json fence in a <details> block, so
    we take the last fenced json block in the body — the idea's own JSON is the
    only one there, but "last" is the safer pick if a commenter ever pastes
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
        sys.exit(f"Issue #{number} has no ```json block — publish it by hand, "
                 f"or paste the fields with no arguments.")
    try:
        obj = json.loads(blocks[-1])
    except json.JSONDecodeError as exc:
        sys.exit(f"The JSON block in issue #{number} didn't parse: {exc}")
    if not isinstance(obj, dict):
        sys.exit(f"Issue #{number} JSON block is not a single object.")
    return obj


def close_issue(number: int, entry_id: str) -> None:
    """Close a published issue. Never fatal — the idea is already saved."""
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
              f"The idea was still added — close it by hand.")


def prompt_fields() -> dict:
    print("No JSON given — enter the idea (Ctrl-C to cancel).\n")
    title = input("Title (one line): ").strip()
    body = input("Details: ").strip()
    print(f"Area options: {', '.join(AREAS)}")
    area = input("Area [Other]: ").strip() or "Other"
    name = input("Name (optional): ").strip()
    return {"title": title, "body": body, "area": area, "name": name}


def choose_body(obj: dict) -> str | None:
    """Ask who would have to act on this idea, defaulting to the area's hint."""
    given = (obj.get("responsible") or "").strip().upper()
    if given in BODIES:
        return given
    suggested = AREA_HINTS.get((obj.get("area") or "").strip())
    print("\nWho would have to act on this? Leave blank to file it as "
          "'Not yet assigned'.")
    for code, name in BODIES.items():
        print(f"  {code:14} {name}")
    prompt = f"Body [{suggested}]: " if suggested else "Body: "
    try:
        choice = input(prompt).strip().upper() or (suggested or "")
    except EOFError:
        choice = suggested or ""
    if not choice:
        return None
    if choice not in BODIES:
        sys.exit(f"Unknown body {choice!r}. Expected one of: {', '.join(BODIES)}")
    return choice


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
    body = obj.get("_responsible")
    if body:
        entry["responsible"] = body
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
    ap.add_argument("blob", nargs="?", help="publishable JSON from the issue body")
    ap.add_argument("--from-issue", type=int, metavar="N",
                    help="read the idea straight from GitHub issue N (needs the gh CLI), "
                         "and close it once published")
    ap.add_argument("--no-close", action="store_true",
                    help="with --from-issue, leave the issue open")
    ap.add_argument("--responsible", metavar="CODE",
                    help=f"who would have to act ({', '.join(BODIES)}); "
                         f"prompts if omitted")
    ap.add_argument("--commit", action="store_true", help="git add + commit (does not push)")
    args = ap.parse_args()

    data = load()
    existing = {s.get("id") for s in data["suggestions"]}

    if args.from_issue is not None:
        obj = read_issue(args.from_issue)
    else:
        obj = read_blob(args.blob) or prompt_fields()
    if args.responsible:
        code = args.responsible.strip().upper()
        if code not in BODIES:
            sys.exit(f"Unknown body {code!r}. Expected one of: {', '.join(BODIES)}")
        obj["_responsible"] = code
    else:
        obj["_responsible"] = choose_body(obj)

    entry = normalise(obj, existing)
    data["suggestions"].append(entry)
    validate(data)

    SUGGESTIONS.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"\nAdded idea '{entry['id']}' — {SUGGESTIONS.relative_to(ROOT)} now has "
          f"{len(data['suggestions'])} published idea(s).")

    # Close only after the file is safely written — a failed close must never
    # cost us the idea.
    if args.from_issue is not None and not args.no_close:
        close_issue(args.from_issue, entry["id"])

    if args.commit:
        subprocess.run(["git", "add", str(SUGGESTIONS)], cwd=ROOT, check=True)
        subprocess.run(["git", "commit", "-m", f"Publish community idea: {entry['title']}"],
                       cwd=ROOT, check=True)
        print("Committed. Run `git push` to publish.")
    else:
        print("Review it, then:\n"
              "  git add data/suggestions.json && git commit -m \"Publish community idea\" && git push")


if __name__ == "__main__":
    main()
