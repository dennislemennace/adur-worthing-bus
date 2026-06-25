"""Integrity checks for the hand-curated JSON data files.

These are the project's first tests. They don't exercise the API — they lock
down the curated content that the frontend fetches at runtime (proposals,
objectives, community ideas) so a typo in a committed JSON file fails CI rather
than silently breaking a tab in the browser.

Each file has its OWN schema:
  * proposals.json   — route proposals (id / name / summary), no `status`.
  * objectives.json  — network goals with a `status` from a fixed enum.
  * suggestions.json — published community ideas (status == "published").

Run with:  pytest
"""

import json
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Allowed `status` values for objectives. Kept in sync with OBJECTIVE_STATUS in
# app.js — these track delivery by councils/operators, not our own roadmap.
# Anything outside this set renders without a styled badge.
OBJECTIVE_STATUSES = {"not_considered", "discussed", "in_progress", "delivered"}


def _load(name):
    path = DATA_DIR / name
    assert path.exists(), f"{name} is missing from {DATA_DIR}"
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)  # raises (failing the test) on invalid JSON


def _assert_unique_ids(items, name):
    ids = [it["id"] for it in items]
    dupes = {i for i in ids if ids.count(i) > 1}
    assert not dupes, f"{name}: duplicate id(s): {sorted(dupes)}"


def _nonempty_str(value):
    return isinstance(value, str) and value.strip() != ""


def test_proposals_schema():
    data = _load("proposals.json")
    proposals = data.get("proposals")
    assert isinstance(proposals, list), "proposals.json: 'proposals' must be a list"
    for p in proposals:
        assert isinstance(p, dict), "proposals.json: each entry must be an object"
        for key in ("id", "name", "summary"):
            assert _nonempty_str(p.get(key)), f"proposals.json: entry missing '{key}': {p.get('id')!r}"
    _assert_unique_ids(proposals, "proposals.json")


def test_objectives_schema():
    data = _load("objectives.json")
    objectives = data.get("objectives")
    assert isinstance(objectives, list), "objectives.json: 'objectives' must be a list"
    for o in objectives:
        assert isinstance(o, dict), "objectives.json: each entry must be an object"
        for key in ("id", "title", "summary", "status"):
            assert _nonempty_str(o.get(key)), f"objectives.json: entry missing '{key}': {o.get('id')!r}"
        assert o["status"] in OBJECTIVE_STATUSES, (
            f"objectives.json: '{o['id']}' has status {o['status']!r}; "
            f"expected one of {sorted(OBJECTIVE_STATUSES)}"
        )
        links = o.get("links", [])
        assert isinstance(links, list), f"objectives.json: '{o['id']}' links must be a list"
        for link in links:
            assert _nonempty_str(link.get("label")) and _nonempty_str(link.get("url")), (
                f"objectives.json: '{o['id']}' has a link missing label/url"
            )
    _assert_unique_ids(objectives, "objectives.json")


def test_suggestions_schema():
    data = _load("suggestions.json")
    suggestions = data.get("suggestions")
    assert isinstance(suggestions, list), "suggestions.json: 'suggestions' must be a list"
    for s in suggestions:
        assert isinstance(s, dict), "suggestions.json: each entry must be an object"
        for key in ("id", "title", "body", "status"):
            assert _nonempty_str(s.get(key)), f"suggestions.json: entry missing '{key}': {s.get('id')!r}"
        # Only published ideas should ever be committed to the curated feed.
        assert s["status"] == "published", (
            f"suggestions.json: '{s['id']}' has status {s['status']!r}; expected 'published'"
        )
    _assert_unique_ids(suggestions, "suggestions.json")


if __name__ == "__main__":  # allow `python tests/test_curated_data.py`
    import pytest

    raise SystemExit(pytest.main([__file__, "-q"]))
