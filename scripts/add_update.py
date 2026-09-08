#!/usr/bin/env python3
"""
Add a Network Updates article.

    python scripts/add_update.py                      # type it in
    python scripts/add_update.py --community          # publish a passenger report
    python scripts/add_update.py --from-issue 42      # publish an approved submission
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
import subprocess
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


def read_issue(number: int) -> dict:
    """Turn a submission issue into a draft community article.

    Two shapes arrive here. A **news submission** carries its own ```json
    block, exactly like an idea or a proposal, and is used as written. A
    **passenger report** — a stop issue or a bus issue filed from the live
    view — carries no such block, because it was never meant to be an
    article. For those, the report's own fields are lifted into a draft and
    the maintainer edits it before it is published.

    That second path is the point of this flag: a resident reporting a bus
    they could not board is a piece of local news, and until now there was no
    route from the report to the page.
    """
    try:
        out = subprocess.run(
            ["gh", "issue", "view", str(number), "--json",
             "body,title,state,labels"],
            cwd=ROOT, check=True, capture_output=True, text=True,
        ).stdout
    except FileNotFoundError:
        sys.exit("The `gh` CLI isn't installed — see https://cli.github.com")
    except subprocess.CalledProcessError as exc:
        sys.exit(f"Could not read issue #{number}:\n{exc.stderr.strip()}")

    issue = json.loads(out)
    if issue.get("state") == "CLOSED":
        print(f"Note: issue #{number} is already closed.")
    body = issue.get("body") or ""
    title = (issue.get("title") or "").strip()
    labels = {l.get("name", "") for l in issue.get("labels", [])}

    blocks = re.findall(r"```json\s*\n(.*?)\n```", body, re.S)
    if blocks:
        try:
            obj = json.loads(blocks[-1])
        except json.JSONDecodeError as exc:
            sys.exit(f"The JSON block in issue #{number} didn't parse: {exc}")
        if not isinstance(obj, dict):
            sys.exit(f"Issue #{number} JSON block is not a single object.")
        return obj

    if not (labels & {"stop-issue", "bus-issue"}):
        sys.exit(f"Issue #{number} has no ```json block and is not a stop or "
                 f"bus report — publish it by hand, or run with no arguments.")

    # A report, not an article. Lift what the Worker recorded, and leave the
    # prose deliberately thin: this is a draft for a person to rewrite, not
    # something to publish as it stands.
    quoted = re.search(r"### Reported problem\s*\n\s*\n((?:> .*\n?)+)", body)
    said = ""
    if quoted:
        said = "\n".join(line.lstrip("> ").rstrip()
                         for line in quoted.group(1).splitlines()).strip()

    def field(label):
        m = re.search(rf"^\*\*{re.escape(label)}:\*\*\s*(.+)$", body, re.M)
        return m.group(1).strip().strip("`") if m else ""

    reporter = field("From")
    subject = field("Service") or field("Stop") or "the network"
    kind = "bus" if "bus-issue" in labels else "stop"

    return {
        "title": title.replace("Bus issue:", "Reported:")
                      .replace("Stop issue:", "Reported:").strip() or f"Reported: {subject}",
        "summary": f"A passenger report about {subject}.",
        "body": (said or "(no description was given)")
                + f"\n\nReported from the live {kind} view"
                + (f" by {reporter}." if reporter and reporter != "anonymous" else "."),
        "topic": "Passenger reports",
        "name": "" if reporter == "anonymous" else reporter,
        "links": [{"label": f"Original report (#{number})",
                   "url": f"https://github.com/dennislemennace/adur-worthing-bus/issues/{number}"}],
        "_needs_editing": True,
    }


def close_issue(number: int, entry_id: str) -> None:
    """Close a published issue and drop its `unverified` label.

    Never fatal — the article is already saved by this point. Says "approved
    for publication" rather than "published", because at this moment a local
    file has been written and nothing has been committed or deployed.
    """
    try:
        subprocess.run(
            ["gh", "issue", "close", str(number),
             "--comment", f"Approved for publication in Community News as "
                          f"`{entry_id}` — it will appear on the site with the "
                          f"next deploy. Thanks!"],
            cwd=ROOT, check=True, capture_output=True, text=True,
        )
        print(f"Closed issue #{number}.")
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        detail = getattr(exc, "stderr", "") or str(exc)
        print(f"Note: couldn't close issue #{number} ({detail.strip()}). "
              f"The article was still added — close it by hand.")
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
    ap.add_argument("--from-issue", type=int, metavar="N",
                    help="publish from submission issue N (implies --community)")
    ap.add_argument("--keep-open", action="store_true",
                    help="with --from-issue, leave the issue open")
    args = ap.parse_args()

    # Anything arriving from an issue came from the public, so it belongs in
    # the community file whatever else was asked for.
    if args.from_issue:
        args.community = True
    path = COMMUNITY if args.community else OFFICIAL
    doc = load(path)

    raw = args.payload
    if raw is None and not sys.stdin.isatty():
        raw = sys.stdin.read().strip() or None

    if args.from_issue:
        entry = read_issue(args.from_issue)
        if entry.pop("_needs_editing", False):
            print("\nThis came from a passenger report, not a written article.\n"
                  "A draft has been added — read it, rewrite it in your own\n"
                  "words, and check it says only what the report supports.\n")
    elif raw:
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

    if not entry.get("links"):
        entry.pop("links", None)

    doc["updates"].append(entry)
    validate(doc, path, args.community)

    path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")
    kind = "community" if args.community else "official"
    print(f"Added {kind} update '{entry['id']}' — {path.name} now has "
          f"{len(doc['updates'])} article(s).")
    print("Review the diff, then commit. Nothing has been pushed.")

    if args.from_issue and not args.keep_open:
        close_issue(args.from_issue, entry["id"])


if __name__ == "__main__":
    main()
