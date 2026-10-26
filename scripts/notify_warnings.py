"""Tell the maintainer about a night's warnings, once, as GitHub issues.

A `::warning::` in a workflow log reaches nobody. GitHub emails when a run
fails, not when it warns, and the night's data is usually fine when it warns:
Stagecoach's journey IDs stopped matching our timetable on 7 October 2026 and
the warning fired every night after, unread, until an outside review found it
in the log.

Each step that can warn appends a line to the notices file (`$NOTICES`):

    key<TAB>warn<TAB>message      the condition holds tonight
    key<TAB>clear                 it was checked and does not

and this, run last whatever happened before it, turns them into issues:

- warned, and no issue for the key: open one, mentioning the owner, so it
  reaches them as a notification;
- warned, and its issue is open: update the issue's "last seen" line, silently;
- warned, and its last issue was closed as **not planned**: nothing. Closing
  as not planned is how to mute a condition already known about;
- warned, and its last issue was closed as completed: open a new one, since
  it has come back;
- cleared, and its issue is open: comment and close it.

A run for an older day (a catch-up, or a re-run) never closes or rewinds an
issue a newer day raised. It never fails the run either: the night's data has
already been published or refused by then.

    python scripts/notify_warnings.py --notices notices.tsv --day 2026-10-14 \\
        --run-url https://github.com/OWNER/REPO/actions/runs/1
"""

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BOT = "github-actions[bot]"
MARKER = re.compile(r"<!-- nightly-warning: (?P<key>[a-z0-9:_-]+); last-seen: (?P<day>\d{4}-\d{2}-\d{2}) -->")
KEY = re.compile(r"[a-z0-9:_-]{1,80}")

TITLES = {
    "declared-journeys": "{arg}'s live journey IDs are not in our timetable",
    "recording-gaps": "The recorder missed part of a day",
    "recording-stopped": "The recorder saved nothing for a day",
    "backlog": "A recorded day was not published",
    "published-budget": "The published bucket is filling up",
    "retention": "Old published files could not be cleared",
    "timetable-expiry": "The timetable runs out for {arg}",
    "objective-drift": "A figure quoted in the objectives no longer matches the evidence",
    "served-scores": "The stop board's estimates could not be scored for a day",
}

ADVICE = {
    "declared-journeys": (
        "The operator's live feed names journeys our timetable does not hold. Most are still identified "
        "by line, first stop and start time (method 7), so the figures stand, but check the operator's BODS "
        "timetable dataset: if BODS has the new timetable, rebuild ours (`update-timetable.yml`). "
        "docs/reliability/bods-stagecoach-south-2026-10-09.md describes the last time."),
    "recording-gaps": (
        "Some clock hours were only partly recorded, or not at all. Check the recorder "
        "(`worker/src/recorder.js`) and the raw bucket. The day was still published; its coverage block "
        "names the hours."),
    "recording-stopped": (
        "Too few snapshots reached the bucket to publish the day, so the recorder has probably stopped. "
        "Every minute it is down is lost for good: BODS keeps no history. Watch what it says each minute "
        "with `cd worker && npx wrangler tail adur-worthing-submissions`. HTTP 403 from the feed means BODS "
        "refuses the recorder's key: check the account at bus-data.dft.gov.uk, then set a working key with "
        "`npx wrangler secret put BODS_API_KEY`. \"snapshots paused\" means the storage budget "
        "(`worker/src/recorder.js`)."),
    "backlog": (
        "A recorded day never reached the site, so a catch-up run was started for it. If the catch-up "
        "fails it is left for a person: see docs/RUNBOOK.md."),
    "published-budget": "See LIMITS.md for the published-bucket arithmetic and `scripts/prune_published.py`.",
    "retention": (
        "Retention could not verify what retained builds depend on, so it deleted nothing. The bucket "
        "grows until this is fixed: see `scripts/prune_published.py`."),
    "timetable-expiry": (
        "The timetable the site serves has no trips for this operator or route on some of the next 14 "
        "days: its calendars have run out or the operator has dropped out of the feed. Look at why the "
        "weekly `update-timetable.yml` has not replaced it, fix that, and run it by hand."),
    "objective-drift": (
        "The weekly rebuild published the new timetable and evidence, but a figure quoted in "
        "data/objectives.json now differs from data/boundary_evidence.json. Update the `quoted` value and "
        "its sentence to the new figure; until then CI on the next push fails the same check."),
    "served-scores": (
        "`scripts/score_served_estimates.py` failed, or its counts did not validate, so the day's board "
        "was not scored and the published window leaves it out (it names the days it covers). The rest "
        "of the night published as normal. Re-run it by hand from the day's evidence archive: see "
        "docs/RUNBOOK.md, \"What the board showed\"."),
}


def read_notices(text):
    """`{key: message or None}`: None means checked and clear. A key that both
    warned and cleared in one run warned."""
    out = {}
    for line in text.splitlines():
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 2 or not KEY.fullmatch(parts[0]):
            continue
        key, status = parts[0], parts[1]
        if status == "warn":
            out[key] = (parts[2] if len(parts) > 2 else "").strip() or key
        elif status == "clear":
            out.setdefault(key, None)
    return out


def title_for(key, message):
    kind, _, arg = key.partition(":")
    template = TITLES.get(kind)
    return template.format(arg=arg.upper()) if template else f"Nightly warning: {message[:90]}"


def _run_link(run_url):
    return f" ([the run]({run_url}))" if run_url else ""


def body_for(key, message, day, run_url, owner):
    kind = key.partition(":")[0]
    mention = f"@{owner} " if owner else ""
    return (f"{mention}the nightly run for {day} raised this:\n\n> {message}\n\n"
            f"**Last seen:** {day}{_run_link(run_url)}\n\n"
            f"**What to do.** {ADVICE.get(kind, 'See the run log.')}\n\n"
            "This issue keeps its last-seen date up to date while the warning continues, and closes itself "
            "when a later night finds the condition cleared. To stop hearing about a condition you already "
            "know about, close it as **not planned**: it then stays quiet until it clears and comes back.\n\n"
            f"<!-- nightly-warning: {key}; last-seen: {day} -->\n")


def _marked(issue):
    m = MARKER.search(issue.get("body") or "")
    return (m.group("key"), m.group("day")) if m else (None, None)


def plan(notices, issues, day, run_url="", owner=""):
    """The actions to take: `[(verb, issue number or None, payload)]`.

    `issues` are the bot's issues, newest first, as the API returns them.
    """
    latest = {}
    for issue in issues:
        if issue.get("pull_request"):
            continue
        key, seen = _marked(issue)
        if key and key not in latest:
            latest[key] = (issue, seen)
    actions = []
    for key, message in sorted(notices.items()):
        issue, seen = latest.get(key, (None, None))
        is_open = issue is not None and issue.get("state") == "open"
        if message is None:
            if is_open and day >= (seen or ""):
                actions.append(("close", issue["number"], {
                    "comment": f"Cleared: the nightly run for {day} found this no longer holds{_run_link(run_url)}."}))
            continue
        if is_open:
            if day > (seen or ""):
                body = re.sub(r"\*\*Last seen:\*\* [^\n]*", lambda _: f"**Last seen:** {day}{_run_link(run_url)}",
                              issue.get("body") or "")
                body = MARKER.sub(lambda _: f"<!-- nightly-warning: {key}; last-seen: {day} -->", body)
                actions.append(("update", issue["number"], {"body": body}))
            continue
        if issue is not None and issue.get("state_reason") == "not_planned":
            continue
        actions.append(("open", None, {"title": title_for(key, message),
                                       "body": body_for(key, message, day, run_url, owner)}))
    return actions


class GitHub:
    """The calls this needs, with the run's own token."""

    def __init__(self, repo, token, api="https://api.github.com"):
        self.repo, self.token, self.api = repo, token, api

    def _call(self, method, path, payload=None):
        req = urllib.request.Request(
            f"{self.api}/repos/{self.repo}{path}", method=method,
            data=None if payload is None else json.dumps(payload).encode(),
            headers={"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "adur-worthing-bus-nightly"})
        with urllib.request.urlopen(req, timeout=30) as res:
            return json.loads(res.read() or b"null")

    def bot_issues(self, pages=3):
        out = []
        for page in range(1, pages + 1):
            batch = self._call("GET", f"/issues?state=all&creator={urllib.parse.quote(BOT)}"
                                      f"&sort=created&direction=desc&per_page=100&page={page}")
            out += batch
            if len(batch) < 100:
                break
        return out

    def apply(self, verb, number, payload):
        if verb == "open":
            return self._call("POST", "/issues", payload)
        if verb == "update":
            return self._call("PATCH", f"/issues/{number}", payload)
        if verb == "close":
            self._call("POST", f"/issues/{number}/comments", {"body": payload["comment"]})
            return self._call("PATCH", f"/issues/{number}", {"state": "closed", "state_reason": "completed"})
        raise ValueError(verb)


def main(argv=None, client=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--notices", required=True)
    ap.add_argument("--day", default="")
    ap.add_argument("--run-url", default="")
    args = ap.parse_args(argv)
    path = Path(args.notices)
    notices = read_notices(path.read_text(encoding="utf-8")) if path.exists() else {}
    if not notices or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", args.day):
        print("No notices to send.")
        return 0
    try:
        if client is None:
            client = GitHub(os.environ["GITHUB_REPOSITORY"], os.environ["GH_TOKEN"])
        owner = os.environ.get("GITHUB_REPOSITORY_OWNER", "")
        for verb, number, payload in plan(notices, client.bot_issues(), args.day, args.run_url, owner):
            client.apply(verb, number, payload)
            print(f"{verb} {number or payload.get('title')}")
    except (KeyError, OSError, ValueError, urllib.error.URLError) as err:
        # The data is already published or refused; a notice that cannot be
        # sent is itself only a warning.
        print(f"::warning::Could not send the night's notices: {err!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
