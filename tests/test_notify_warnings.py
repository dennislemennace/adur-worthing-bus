"""scripts/notify_warnings.py: a night's warnings reach the maintainer once, as
issues, instead of sitting unread in a workflow log.

The Stagecoach journey-ID warning fired every night from 7 October 2026 and
nobody saw it until an outside review read the log.
"""

import gzip
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import check_declared_journeys as cdj                              # noqa: E402
import notify_warnings as nw                                       # noqa: E402

KEY = "declared-journeys:scso"
MSG = "SCSO: 612 of 612 journeys on 2026-10-14 were not named by a journey id our timetable holds."


def issue(number, key, seen, state="open", reason=None):
    return {"number": number, "state": state, "state_reason": reason,
            "body": nw.body_for(key, "earlier", seen, "https://x/run/1", "owner")}


def test_notices_read_warn_over_clear_and_skip_junk():
    text = (f"{KEY}\twarn\t{MSG}\n"
            "backlog\tclear\n"
            f"{KEY}\tclear\n"
            "Not A Key\twarn\tx\n"
            "half-a-line\n")
    assert nw.read_notices(text) == {KEY: MSG, "backlog": None}


def test_a_new_warning_opens_one_issue_that_mentions_the_owner():
    [(verb, number, payload)] = nw.plan({KEY: MSG}, [], "2026-10-14", "https://x/run/9", "dennis")
    assert (verb, number) == ("open", None)
    assert payload["title"] == "SCSO's live journey IDs are not in our timetable"
    assert payload["body"].startswith("@dennis "), "a mention is what makes it a notification"
    assert MSG in payload["body"] and "BODS" in payload["body"]
    assert "<!-- nightly-warning: declared-journeys:scso; last-seen: 2026-10-14 -->" in payload["body"]
    assert "not planned" in payload["body"], "the issue must say how to mute it"


def test_a_continuing_warning_updates_quietly_and_never_rewinds():
    [(verb, number, payload)] = nw.plan({KEY: MSG}, [issue(5, KEY, "2026-10-13")], "2026-10-14", "https://x/run/9")
    assert (verb, number) == ("update", 5), "a second issue for the same thing is noise"
    assert "**Last seen:** 2026-10-14 ([the run](https://x/run/9))" in payload["body"]
    assert "last-seen: 2026-10-14 -->" in payload["body"]
    assert nw.plan({KEY: MSG}, [issue(5, KEY, "2026-10-14")], "2026-10-14") == [], "same day: nothing to change"
    assert nw.plan({KEY: MSG}, [issue(5, KEY, "2026-10-14")], "2026-10-03") == [], \
        "a catch-up for an older day moved last-seen backwards"


def test_closing_as_not_planned_mutes_it_and_completed_lets_it_come_back():
    muted = issue(5, KEY, "2026-10-10", state="closed", reason="not_planned")
    assert nw.plan({KEY: MSG}, [muted], "2026-10-14") == []
    done = issue(5, KEY, "2026-10-10", state="closed", reason="completed")
    assert [a[0] for a in nw.plan({KEY: MSG}, [done], "2026-10-14")] == ["open"]
    # Only the newest issue for a key decides: an old muted one does not
    # silence a condition that came back after being fixed.
    newer_done = issue(9, KEY, "2026-10-12", state="closed", reason="completed")
    assert [a[0] for a in nw.plan({KEY: MSG}, [newer_done, muted], "2026-10-14")] == ["open"]


def test_a_cleared_condition_closes_its_issue_but_not_from_an_older_day():
    [(verb, number, payload)] = nw.plan({KEY: None}, [issue(5, KEY, "2026-10-14")], "2026-10-15")
    assert (verb, number) == ("close", 5)
    assert "2026-10-15" in payload["comment"]
    assert nw.plan({KEY: None}, [issue(5, KEY, "2026-10-14")], "2026-10-05") == [], \
        "a catch-up of a day before the problem closed it"
    assert nw.plan({KEY: None}, [], "2026-10-15") == [], "nothing open, nothing to say"


class FakeClient:
    def __init__(self, issues=(), fail=False):
        self.issues, self.fail, self.calls = list(issues), fail, []

    def bot_issues(self):
        if self.fail:
            raise OSError("api down")
        return self.issues

    def apply(self, verb, number, payload):
        self.calls.append((verb, number))


def test_main_sends_and_never_fails_the_run(tmp_path, capsys):
    notices = tmp_path / "notices.tsv"
    notices.write_text(f"{KEY}\twarn\t{MSG}\nbacklog\tclear\n")
    client = FakeClient()
    assert nw.main(["--notices", str(notices), "--day", "2026-10-14"], client=client) == 0
    assert client.calls == [("open", None)]
    broken = FakeClient(fail=True)
    assert nw.main(["--notices", str(notices), "--day", "2026-10-14"], client=broken) == 0
    assert "::warning::Could not send" in capsys.readouterr().out
    # No day (the run failed before choosing one) or no file: nothing to send.
    assert nw.main(["--notices", str(notices), "--day", ""], client=FakeClient()) == 0
    assert nw.main(["--notices", str(tmp_path / "none.tsv"), "--day", "2026-10-14"], client=FakeClient()) == 0


def test_the_declared_check_writes_a_notice_per_operator_it_could_judge(tmp_path):
    rows = []
    for op, n, bad in (("SCSO", 40, 39), ("BHBC", 100, 1), ("TINY", 5, 5)):
        for i in range(n):
            row = {"day": "2026-10-14", "operator": op, "trip_id": f"{op}{i}", "quality_flags": []}
            if i < bad:
                row["quality_flags"] = [cdj.FLAG]
            rows.append(row)
    obs = tmp_path / "observations-2026-10-14.json.gz"
    obs.write_bytes(gzip.compress(json.dumps({"day": "2026-10-14", "observations": rows}).encode()))
    notices = tmp_path / "notices.tsv"
    assert cdj.main([str(obs), "--notices", str(notices)]) == 0
    got = nw.read_notices(notices.read_text())
    assert set(got) == {"declared-journeys:scso", "declared-journeys:bhbc"}, "TINY is too small to judge"
    assert got["declared-journeys:scso"].startswith("SCSO: 39 of 40 journeys")
    assert got["declared-journeys:bhbc"] is None


def test_the_nightly_workflow_sends_its_notices_last_whatever_happened():
    workflow = yaml.safe_load((ROOT / ".github/workflows/process-snapshots.yml").read_text())
    assert workflow["permissions"].get("issues") == "write"
    assert "NOTICES" in workflow["env"]
    steps = workflow["jobs"]["process"]["steps"]
    last = steps[-1]
    assert "notify_warnings.py" in last["run"] and last.get("if") == "always()"
    runs = "\n".join(s.get("run") or "" for s in steps)
    assert '--notices "$NOTICES"' in runs, "the declared check does not record its notices"
    for kind in ("recording-gaps", "backlog", "published-budget", "retention"):
        assert f"{kind}\\twarn" in runs and f"{kind}\\tclear" in runs, f"{kind} cannot both warn and clear"
