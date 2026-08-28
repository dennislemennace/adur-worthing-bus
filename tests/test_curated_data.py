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
from datetime import date
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Allowed `status` values for objectives. Kept in sync with OBJECTIVE_STATUS in
# app.js — these track delivery by councils/operators, not our own roadmap.
# Anything outside this set renders without a styled badge.
OBJECTIVE_STATUSES = {"not_considered", "discussed", "in_progress", "delivered"}

# Themes an objective can be filed under. Kept small on purpose: a long list of
# near-synonyms makes the grouped view useless.
OBJECTIVE_CATEGORIES = {
    "Network & coverage",
    "Frequency & hours",
    "Ticketing & fares",
    "Accessibility",
    "Information",
    "Planning & governance",
}

# Bodies an objective can be assigned to. Operators make commercial decisions;
# authorities own the infrastructure, the funding and the partnerships. Kept in
# sync with RESPONSIBLE_BODIES in app.js.
RESPONSIBLE_BODIES = {
    "SCSO", "BHBC", "METR", "COMT",                 # operators
    "WSCC", "ESCC", "BHCC", "ADUR_WORTHING",        # authorities
}


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
        assert o["category"] in OBJECTIVE_CATEGORIES, (
            f"objectives.json: '{o['id']}' has category {o.get('category')!r}; "
            f"expected one of {sorted(OBJECTIVE_CATEGORIES)}"
        )

        # Who would have to act. `lead` is a list because some asks have no
        # single owner — fitting audio-visual announcements is every operator's
        # job, not one nominated operator's. `shared` is everyone else without
        # whom it won't happen.
        lead = o.get("lead")
        assert isinstance(lead, list) and lead, (
            f"objectives.json: '{o['id']}' needs a non-empty 'lead' list — "
            f"an objective nobody leads can't be acted on"
        )
        shared = o.get("shared", [])
        assert isinstance(shared, list), f"objectives.json: '{o['id']}' 'shared' must be a list"

        for code in lead + shared:
            assert code in RESPONSIBLE_BODIES, (
                f"objectives.json: '{o['id']}' names body {code!r}; "
                f"expected one of {sorted(RESPONSIBLE_BODIES)}"
            )
        # A body is either responsible for doing it or needed alongside whoever
        # is — being listed as both would file the same objective twice in one
        # group, once under each heading.
        overlap = set(lead) & set(shared)
        assert not overlap, (
            f"objectives.json: '{o['id']}' lists {sorted(overlap)} as both lead and shared"
        )
        assert len(set(lead)) == len(lead), f"objectives.json: '{o['id']}' repeats a lead body"

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
        # Optional: who would have to act on it. Assigned when the idea is
        # published, not by the submitter — they pick an "area", not a body.
        # Unassigned ideas are grouped under "Not yet assigned" rather than
        # being guessed at.
        if s.get("responsible") is not None:
            assert s["responsible"] in RESPONSIBLE_BODIES, (
                f"suggestions.json: '{s['id']}' is assigned to {s['responsible']!r}; "
                f"expected one of {sorted(RESPONSIBLE_BODIES)}"
            )
        # Only published ideas should ever be committed to the curated feed.
        assert s["status"] == "published", (
            f"suggestions.json: '{s['id']}' has status {s['status']!r}; expected 'published'"
        )
    _assert_unique_ids(suggestions, "suggestions.json")


def test_ticket_zones_schema():
    """Ticket zones drive a public claim about money, so the fare fields are
    held to a stricter standard than the rest of the curated data: every price
    must be an integer number of pence, and must say where it came from and
    when it was last checked."""
    data = _load("ticket_zones.json")
    zones = data.get("zones")
    assert isinstance(zones, list), "ticket_zones.json: 'zones' must be a list"

    for z in zones:
        zid = z.get("id")
        for key in ("id", "operator", "name", "coverage_rule"):
            assert _nonempty_str(z.get(key)), f"ticket_zones.json: {zid!r} missing '{key}'"
        assert z["coverage_rule"] in COVERAGE_RULES, (
            f"ticket_zones.json: {zid!r} has coverage_rule {z['coverage_rule']!r}; "
            f"expected one of {sorted(COVERAGE_RULES)}"
        )
        # A polygon-ruled zone with no geometry would silently cover nothing.
        if z["coverage_rule"] == "polygon":
            assert z.get("polygon") or z.get("polygons") or z.get("polygons_from"), (
                f"ticket_zones.json: {zid!r} is coverage_rule 'polygon' but has no geometry"
            )

        # Which operators accept this ticket. Geography alone doesn't decide
        # validity — a Stagecoach ticket isn't valid on a Metrobus, and
        # Metrovoyager IS valid on Brighton & Hove. Missing this field makes the
        # calculator fall back to "own operator only", which is wrong for the
        # two tickets that cross over.
        accepted = z.get("valid_on_operators")
        assert isinstance(accepted, list) and accepted, (
            f"ticket_zones.json: {zid!r} needs a non-empty valid_on_operators list"
        )
        for op in accepted:
            assert _nonempty_str(op), f"ticket_zones.json: {zid!r} has a blank operator"
        assert z["operator"] in accepted, (
            f"ticket_zones.json: {zid!r} is sold by {z['operator']} but that operator "
            f"isn't in its own valid_on_operators list"
        )

        _assert_fares(z.get("fares"), zid)

    _assert_unique_ids(zones, "ticket_zones.json")


COVERAGE_RULES = {"polygon", "operator_network", "reach_points"}
FARE_KINDS = ("adult_day", "adult_day_cash", "adult_weekly")


def _assert_fares(fares, zid):
    """`fares` may be null (unknown), but anything present must be complete."""
    if fares is None:
        return
    assert isinstance(fares, dict), f"ticket_zones.json: {zid!r} fares must be an object"

    priced = False
    for kind in FARE_KINDS:
        fare = fares.get(kind)
        if fare is None:
            continue
        assert isinstance(fare, dict), f"ticket_zones.json: {zid!r} {kind} must be an object"
        pence = fare.get("price_pence")
        assert isinstance(pence, int) and not isinstance(pence, bool), (
            f"ticket_zones.json: {zid!r} {kind}.price_pence must be an integer "
            f"number of pence, got {pence!r} — float money is a rounding bug waiting to happen"
        )
        assert 0 < pence < 100_000, f"ticket_zones.json: {zid!r} {kind}.price_pence out of range"
        assert _nonempty_str(fare.get("label")), f"ticket_zones.json: {zid!r} {kind} missing label"
        priced = True

    if priced:
        # A price with no provenance can't be defended when someone disputes it.
        assert _nonempty_str(fares.get("source_url")), (
            f"ticket_zones.json: {zid!r} has a price but no source_url"
        )
        checked = fares.get("checked_on")
        assert _nonempty_str(checked), f"ticket_zones.json: {zid!r} has a price but no checked_on"
        try:
            date.fromisoformat(checked)
        except ValueError:
            raise AssertionError(
                f"ticket_zones.json: {zid!r} checked_on {checked!r} is not an ISO date"
            )


def test_fares_meta_schema():
    data = _load("ticket_zones.json")
    meta = data.get("fares_meta")
    assert isinstance(meta, dict), "ticket_zones.json: 'fares_meta' must be an object"
    assert meta.get("currency") == "GBP"

    days = meta.get("commute_days_per_week")
    assert isinstance(days, int) and 0 < days <= 7, (
        "ticket_zones.json: commute_days_per_week must be a sane integer — "
        "it multiplies the headline weekly saving. Day tickets are bought per "
        "day, so this counts days, not journeys."
    )

    # May be null (no agreed comparator yet); if set, it must be complete.
    unified = meta.get("unified_ticket")
    if unified is not None:
        assert isinstance(unified.get("price_pence"), int), (
            "ticket_zones.json: unified_ticket.price_pence must be an integer"
        )
        assert _nonempty_str(unified.get("basis")), (
            "ticket_zones.json: the unified fare must state its basis — "
            "it is the number the whole campaign message rests on"
        )
        # A real ticket must be citable; a hypothetical one has nothing to cite.
        if unified.get("is_hypothetical") is False:
            assert _nonempty_str(unified.get("source_url")), (
                "ticket_zones.json: unified_ticket is marked real, so it needs a "
                "source_url — a claimed existing ticket must be checkable"
            )
            assert _nonempty_str(unified.get("name"))


if __name__ == "__main__":  # allow `python tests/test_curated_data.py`
    import pytest

    raise SystemExit(pytest.main([__file__, "-q"]))
