"""data/tour.json: the guided tour's words, written once for the in-app tour,
the slides and the PDF.

What these hold:
- the shape tour.js and tour.html read;
- every "Show me" points at something real (a stop that exists, a ticket
  preset that exists, a view the tour can open);
- a claim with a percentage or a price in it, outside a placeholder the
  pipeline fills, names where it came from and when that was checked. The tour
  is shown to the industry; an unsourced number is the one that gets quoted
  back.
"""

import json
import re
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOC = json.loads((ROOT / "data" / "tour.json").read_text())
STOPS = {s["atco_code"] for s in json.loads((ROOT / "data" / "stops.json").read_text())["stops"]}
TICKET_PRESETS = {p["id"] for p in json.loads((ROOT / "data" / "journey_presets.json").read_text())["presets"]}

BADGES = {"live", "recorded", "timetable", "sourced", "experimental", "about"}
ACTIONS = {"board", "live-map", "evidence", "route-view", "tickets", "planner",
           "journey-times", "delay-map", "link"}
ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def is_iso(value):
    if not isinstance(value, str) or not ISO.match(value):
        return False
    date.fromisoformat(value)
    return True


def test_the_top_level_shape():
    assert DOC["schema_version"] == 1
    assert isinstance(DOC["preview_only"], bool)
    assert is_iso(DOC["checked_on"])
    assert DOC["menu"]["label"] and DOC["menu"]["badge"]
    assert is_iso(DOC["event"]["show_until"])
    assert DOC["opening"]["headline"] and DOC["opening"]["lead"]
    assert [m["id"] for m in DOC["opening"]["modes"]] == ["explore", "present", "pdf"], \
        "the opening offers the tour itself first, then the slides and the PDF"


def test_every_step_has_what_the_panel_and_the_slides_read():
    ids = [s["id"] for s in DOC["steps"]]
    assert len(ids) == len(set(ids)), "two steps share an id"
    for s in DOC["steps"]:
        assert s["title"] and s["lead"], s["id"]
        assert s["badge"]["kind"] in BADGES, f'{s["id"]}: badge {s["badge"]["kind"]}'
        assert s["badge"]["label"], s["id"]
        assert isinstance(s["points"], list) and all(isinstance(p, str) and p for p in s["points"]), s["id"]
        if "headline" in s:
            head = s["headline"]
            figures = head.get("figures") or [{"value": head.get("value"), "label": head.get("label")}]
            assert all(f.get("value") and f.get("label") for f in figures), f'{s["id"]}: a headline needs a figure and what it counts'
            if "figures" in head:
                # A comparison is two or more figures, each saying what it is.
                assert len(head["figures"]) >= 2 and "value" not in head, f'{s["id"]}: figures is for a comparison'
                # Set side by side with no word on what was counted, the
                # numbers say nothing (the maintainer, 22 October 2026).
                assert head.get("measure"), f'{s["id"]}: a comparison must say what was measured'
            for key in ("claim", "measure"):
                assert key not in head or (isinstance(head[key], str) and head[key]), f'{s["id"]}: empty {key}'


def test_every_show_me_points_at_something_real():
    for s in DOC["steps"]:
        for t in s["try"]:
            where = f'{s["id"]}: {t.get("label")}'
            assert t["label"], where
            assert t["action"] in ACTIONS, where
            if t["action"] == "board":
                assert t["atco"] in STOPS and t["name"], where
            if t["action"] == "tickets":
                assert t["preset"] in TICKET_PRESETS, where
            if t["action"] == "journey-times":
                assert t["from"] in STOPS and t["to"] in STOPS, where
            if t["action"] == "link":
                href = t["href"]
                assert (href.startswith(("https://", "mailto:"))
                        or (ROOT / href.split("#")[0]).exists()), where


def test_sources_are_dated_and_reachable():
    for s in DOC["steps"]:
        for src in s["sources"]:
            where = f'{s["id"]}: {src.get("label")}'
            assert src["label"], where
            assert is_iso(src["checked_on"]), where
            url = src["url"]
            assert url.startswith("https://") or (ROOT / url).exists(), where


def test_a_number_from_outside_the_pipeline_names_its_source():
    """A percentage or a price typed into the copy, rather than filled from the
    evidence files, needs an outside source on the same step."""
    for s in DOC["steps"]:
        head = s.get("headline") or {}
        hints = [t.get("hint", "") for t in s["try"]]
        figures = [x for f in head.get("figures", []) for x in (f["value"], f["label"])]
        text = " ".join([s["title"], s["lead"], *s["points"], head.get("value", ""), head.get("label", ""),
                         head.get("claim", ""), head.get("measure", ""), *figures, *hints])
        typed = re.sub(r"\{[a-z_]+\}%?", "", text)
        if re.search(r"\d\s?%|£\s?\d", typed):
            assert any(src["url"].startswith("https://") for src in s["sources"]), (
                f'{s["id"]} quotes a figure that is not filled from the evidence, with no outside source')
