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

import pytest

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

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


# The reform block is the site's actual argument — "here is what this journey
# costs, and here is what it would cost if you changed one rule". It had no
# schema coverage at all, so a malformed entry failed silently at render time
# rather than in CI, which on the one page where a wrong number does real
# damage is the worst place to find out.
REFORM_APPLIES = {"same_operator_multi_zone", "multi_operator"}


def test_reforms_schema():
    meta = _load("ticket_zones.json")["fares_meta"]
    reforms = meta.get("reforms")
    assert isinstance(reforms, list) and reforms, (
        "ticket_zones.json: 'fares_meta.reforms' must be a non-empty list")

    _assert_unique_ids(reforms, "fares_meta.reforms")
    objective_ids = {o["id"] for o in _load("objectives.json")["objectives"]}

    for r in reforms:
        rid = r.get("id", "?")
        assert _nonempty_str(r.get("id")), "a reform needs an 'id'"
        assert _nonempty_str(r.get("headline")), f"reform '{rid}' needs a 'headline'"
        assert r.get("applies") in REFORM_APPLIES, (
            f"reform '{rid}': 'applies' must be one of {sorted(REFORM_APPLIES)}")

        # A reform points at the objective that argues for it. A dangling
        # pointer here means the campaign asks for something the objectives
        # page no longer lists.
        assert r.get("objective_id") in objective_ids, (
            f"reform '{rid}': objective_id {r.get('objective_id')!r} "
            f"does not exist in objectives.json")

        price = r.get("price_pence")
        assert price is None or (
            isinstance(price, int) and not isinstance(price, bool) and 0 < price < 100000
        ), f"reform '{rid}': 'price_pence' must be a sensible int, or null"


def test_single_fare_schema():
    meta = _load("ticket_zones.json")["fares_meta"]
    sf = meta.get("single_fare")
    if sf is None:
        return
    for key in ("price_pence", "short_hop_pence"):
        v = sf.get(key)
        assert isinstance(v, int) and not isinstance(v, bool) and 0 < v < 100000, (
            f"single_fare.{key} must be a sensible int in pence")
    assert _nonempty_str(sf.get("source_url")), (
        "single_fare needs a source_url — it is a national cap, so it is "
        "citable, and an uncited fare can't be defended")
    for key in ("checked_on", "review_by"):
        assert _nonempty_str(sf.get(key)), f"single_fare needs '{key}'"
        date.fromisoformat(sf[key])
    # The cap has an end date and a scheduled change. A site quoting an
    # expired fare cap is worse than one quoting none.
    assert date.fromisoformat(sf["review_by"]) > date.fromisoformat(sf["checked_on"])


# Network Updates. Two files, same shape: one written here, one published from
# passenger reports after review. Both are articles people will read as fact,
# so the schema insists on a date and treats a bare claim with a link as
# better than one without.
def _assert_updates(name, *, needs_name):
    data = _load(name)
    updates = data.get("updates")
    assert isinstance(updates, list), f"{name}: 'updates' must be a list"
    _assert_unique_ids(updates, name)

    for u in updates:
        uid = u.get("id", "?")
        for field in ("id", "title", "summary", "body", "status"):
            assert _nonempty_str(u.get(field)), f"{name}: '{uid}' needs a '{field}'"
        assert u["status"] == "published", (
            f"{name}: '{uid}' has status {u['status']!r} — only published items "
            f"belong in a file the site renders")
        # A dated article can be judged stale; an undated one cannot.
        assert _nonempty_str(u.get("date")), f"{name}: '{uid}' needs a 'date'"
        date.fromisoformat(u["date"])

        for link in u.get("links", []):
            assert _nonempty_str(link.get("label")), f"{name}: '{uid}' link needs a label"
            assert _nonempty_str(link.get("url")), f"{name}: '{uid}' link needs a url"

        if needs_name and u.get("name") is not None:
            assert _nonempty_str(u["name"]), (
                f"{name}: '{uid}' has an empty 'name' — omit it rather than "
                f"crediting nobody")

        _assert_update_image(u.get("image"), name, uid)


def _assert_update_image(image, name, uid):
    """An article's picture, when it has one.

    Optional, but not optional to get right: a path that does not resolve
    leaves a grey box where the hero should be, and a picture with no
    description is invisible to anyone using a screen reader.
    """
    if image is None:
        return
    assert isinstance(image, dict), f"{name}: '{uid}' image must be an object"
    src = image.get("src")
    assert _nonempty_str(src), f"{name}: '{uid}' image needs a 'src'"
    assert not src.startswith(("/", "http://", "https://")), (
        f"{name}: '{uid}' image src {src!r} must be a relative path — the site "
        f"is served from a project subpath on GitHub Pages")
    assert (ROOT / src).exists(), f"{name}: '{uid}' image {src!r} is not in the repo"
    assert _nonempty_str(image.get("alt")), (
        f"{name}: '{uid}' image needs 'alt' — a photograph carrying the point "
        f"of the article is not decoration")
    if image.get("focus") is not None:
        assert _nonempty_str(image["focus"]), (
            f"{name}: '{uid}' image 'focus' is empty — omit it to centre")


def test_official_updates_schema():
    _assert_updates("updates.json", needs_name=False)


def test_community_updates_schema():
    _assert_updates("community_updates.json", needs_name=True)


# ── Council boundaries ──────────────────────────────────────
#
# The dashed line between Brighton & Hove and West Sussex is the whole reason
# the network stops where it does, so it is drawn on both the Route and Live
# maps. A line with no label is just a line; these check that what is drawn
# can still say what it is.

def test_council_boundaries_schema():
    data = _load("council_boundaries.json")
    boundaries = data.get("boundaries")
    assert isinstance(boundaries, list) and boundaries, (
        "council_boundaries.json: 'boundaries' must be a non-empty list")
    _assert_unique_ids(boundaries, "council_boundaries.json")

    for b in boundaries:
        bid = b.get("id", "?")
        for field in ("id", "kind", "name", "summary", "source_url", "licence"):
            assert _nonempty_str(b.get(field)), (
                f"council_boundaries.json: '{bid}' needs a '{field}'")
        assert _nonempty_str(b.get("checked_on")), (
            f"council_boundaries.json: '{bid}' needs a 'checked_on'")
        date.fromisoformat(b["checked_on"])

        assert b["kind"] == "line", (
            f"council_boundaries.json: '{bid}' kind {b['kind']!r} — only 'line' "
            f"is drawn today, and an unrecognised kind is silently skipped")
        line = b.get("polyline")
        assert isinstance(line, list) and len(line) >= 2, (
            f"council_boundaries.json: '{bid}' needs a polyline of two points or more")
        for point in line:
            assert (isinstance(point, list) and len(point) == 2
                    and all(isinstance(c, (int, float)) for c in point)), (
                f"council_boundaries.json: '{bid}' polyline points are [lat, lon] pairs")

        bodies = b.get("bodies") or []
        assert bodies, f"council_boundaries.json: '{bid}' needs the bodies either side"
        for code in bodies:
            assert code in RESPONSIBLE_BODIES, (
                f"council_boundaries.json: '{bid}' names body {code!r}, which "
                f"app.js cannot colour or name")


def test_council_boundary_labels_name_both_sides():
    """West and east are drawn left and right on the map, so both have to be
    named — a label saying 'boundary' and nothing else explains nothing."""
    for b in _load("council_boundaries.json")["boundaries"]:
        bid = b.get("id", "?")
        anchors = b.get("label_at") or []
        sides = b.get("sides") or {}
        if not anchors:
            continue
        for key in ("west", "east"):
            entry = sides.get(key)
            assert isinstance(entry, dict), (
                f"council_boundaries.json: '{bid}' is labelled but has no "
                f"{key} side")
            assert entry.get("body") in RESPONSIBLE_BODIES, (
                f"council_boundaries.json: '{bid}' {key} side names "
                f"{entry.get('body')!r}, which app.js cannot colour")
            assert _nonempty_str(entry.get("label")), (
                f"council_boundaries.json: '{bid}' {key} side needs a short "
                f"'label' — the council's full title is unreadable on a map")
        assert sides["west"]["body"] != sides["east"]["body"], (
            f"council_boundaries.json: '{bid}' has the same body both sides")
        named = {sides[k]["body"] for k in ("west", "east")}
        assert named <= set(b.get("bodies") or []), (
            f"council_boundaries.json: '{bid}' labels a body it does not list "
            f"in 'bodies'")
        for at in anchors:
            assert (isinstance(at, list) and len(at) == 2
                    and all(isinstance(c, (int, float)) for c in at)), (
                f"council_boundaries.json: '{bid}' label_at entries are [lat, lon]")


# ── Journey presets ─────────────────────────────────────────
#
# Worked examples for the Ticket view checker. A preset pointing at a stop that
# is not in the timetable does not error — it quietly reads as "pick both stops
# from the suggestions", which looks like the checker is broken.

def test_journey_presets_schema():
    data = _load("journey_presets.json")
    presets = data.get("presets")
    assert isinstance(presets, list) and presets, (
        "journey_presets.json: 'presets' must be a non-empty list")
    _assert_unique_ids(presets, "journey_presets.json")
    assert _nonempty_str(data.get("checked_on")), (
        "journey_presets.json needs a 'checked_on' — a preset can go stale when "
        "the network changes under it")
    date.fromisoformat(data["checked_on"])

    for p in presets:
        pid = p.get("id", "?")
        for field in ("id", "label", "from", "to", "from_name", "to_name", "why"):
            assert _nonempty_str(p.get(field)), (
                f"journey_presets.json: '{pid}' needs a '{field}'")
        assert p["from"] != p["to"], (
            f"journey_presets.json: '{pid}' starts and ends at the same stop")


@pytest.mark.skipif(not (ROOT / "data" / "timetable.sqlite").exists(),
                    reason="data/timetable.sqlite is a build artefact and isn't present")
def test_journey_preset_stops_exist():
    """Every preset points at a stop the timetable actually knows about."""
    import sqlite3
    con = sqlite3.connect(ROOT / "data" / "timetable.sqlite")
    known = {row[0] for row in con.execute("SELECT stop_id FROM stops")}
    for p in _load("journey_presets.json")["presets"]:
        for end in ("from", "to"):
            assert p[end] in known, (
                f"journey_presets.json: '{p['id']}' {end}-stop {p[end]!r} is not "
                f"in the timetable — the checker would report it as unpickable")


# ── Derived statistics ──────────────────────────────────────
#
# A cited fare carries source_url and checked_on. A number this site works out
# for itself has to carry more: the method, the data it came from, when it was
# computed, and what would make it wrong. See .claude/skills/evidence-provenance.

def test_boundary_evidence_carries_its_provenance():
    data = _load("boundary_evidence.json")

    for field in ("id", "headline", "as_of"):
        assert _nonempty_str(data.get(field)), (
            f"boundary_evidence.json needs a '{field}'")
    date.fromisoformat(data["as_of"])

    version = data.get("data_version")
    assert isinstance(version, dict), "boundary_evidence.json needs a 'data_version'"
    for field in ("source", "artefact", "sha256"):
        assert _nonempty_str(version.get(field)), (
            f"boundary_evidence.json: data_version needs '{field}' — a figure "
            f"whose data version is unknown cannot be reproduced")

    method = data.get("method")
    assert isinstance(method, dict), "boundary_evidence.json needs a 'method'"
    for field in ("summary", "denominator", "script"):
        assert _nonempty_str(method.get(field)), (
            f"boundary_evidence.json: method needs '{field}'")
    assert isinstance(method.get("steps"), list) and method["steps"], (
        "boundary_evidence.json: method needs reproducible 'steps'")
    assert (ROOT / method["script"]).exists(), (
        f"boundary_evidence.json: method.script {method['script']!r} is not in "
        f"the repo, so the figures cannot be recomputed")


def test_boundary_evidence_states_which_way_its_biases_run():
    """The caveat that weakens our own case is the one that earns the rest.

    A reader who finds an unstated bias themselves stops believing everything
    else on the page, so every known one is declared with its direction.
    """
    caveats = _load("boundary_evidence.json").get("caveats")
    assert isinstance(caveats, list) and caveats, (
        "boundary_evidence.json needs 'caveats' — a derived figure with no "
        "stated weaknesses is not being honest about being derived")
    directions = {"understates", "overstates", "unknown"}
    for c in caveats:
        assert _nonempty_str(c.get("text")), "each caveat needs 'text'"
        assert c.get("direction") in directions, (
            f"caveat direction {c.get('direction')!r} must be one of "
            f"{sorted(directions)} — 'it might be wrong' is not a caveat")
    assert any(c["direction"] == "understates" for c in caveats), (
        "boundary_evidence.json states no caveat that works against its own "
        "conclusion. The build filter under-counts the Brighton side; if that "
        "has genuinely stopped being true, say so here rather than dropping it")


def test_boundary_evidence_figures_are_present_and_sane():
    days = _load("boundary_evidence.json").get("days")
    assert isinstance(days, dict) and days, "boundary_evidence.json needs 'days'"
    for day, block in days.items():
        for side in ("west", "east"):
            s = block.get(side)
            assert isinstance(s, dict), f"{day}: missing '{side}'"
            assert s.get("stops", 0) > 0, f"{day}/{side}: no stops in the band"
            assert s.get("departures_per_stop") is not None, (
                f"{day}/{side}: no departures_per_stop — the published figure is "
                f"per stop, and a total would only restate that one side is bigger")


def test_weekend_figure_stays_per_day_and_keeps_both_days():
    """The merged panel must not become a two-day total wearing a daily label.

    Saturday and Sunday are shown as one panel, so the number behind it is
    divided by stops x 2. Get that wrong and the weekend bar is twice as long
    as the weekday one for a service that is materially thinner — the chart
    would say the opposite of the truth.
    """
    days = _load("boundary_evidence.json")["days"]
    weekend = days.get("weekend")
    assert isinstance(weekend, dict), (
        "boundary_evidence.json: 'days' needs a 'weekend' block — the UI merges "
        "Saturday and Sunday into one panel")
    assert weekend.get("combines") == ["saturday", "sunday"]
    assert _nonempty_str(weekend.get("denominator")), (
        "the weekend block needs to state what it divided by, because it is the "
        "one figure on the page that averages two different services")

    for side in ("west", "east"):
        sat, sun, wk = days["saturday"][side], days["sunday"][side], weekend[side]
        assert wk["departures"] == sat["departures"] + sun["departures"]
        lo, hi = sorted((sat["departures_per_stop"], sun["departures_per_stop"]))
        assert lo <= wk["departures_per_stop"] <= hi, (
            f"weekend/{side}: {wk['departures_per_stop']} per stop is outside "
            f"the two days it averages ({lo}-{hi}). A per-day figure cannot be "
            f"— this is a two-day total mislabelled as a daily rate")
        # Saturday and Sunday are different services, and the panel says so
        # rather than letting the average speak for both.
        assert wk.get("routes_saturday") == sat["routes"]
        assert wk.get("routes_sunday") == sun["routes"]


def test_place_comparison_is_traceable_to_published_boundaries():
    """Lancing against South Portslade, with the polygons that defined them.

    "The nearest comparable place" is the whole weight of this comparison, so
    the areas have to be named by their ONS codes: a reader who disagrees can
    then look up exactly what was measured instead of taking the pairing on
    trust.
    """
    data = _load("boundary_evidence.json")
    places = data.get("places")
    assert isinstance(places, dict), (
        "boundary_evidence.json needs 'places' — the band gives the average "
        "effect of the line, and this is what it means somewhere specific")

    areas = _load("comparison_areas.json")
    by_side = {a["side"]: a for a in areas["areas"]}
    assert _nonempty_str(areas.get("source", {}).get("attribution")), (
        "comparison_areas.json: ONS boundary data is OGL v3.0 and carries an "
        "attribution requirement")

    for side in ("west", "east"):
        p = places.get(side)
        assert isinstance(p, dict), f"places: missing '{side}'"
        for field in ("name", "council", "ons_code", "ons_name"):
            assert _nonempty_str(p.get(field)), f"places/{side}: needs '{field}'"
        assert p["ons_code"] == by_side[side]["ons_code"], (
            f"places/{side}: the published figure names {p['ons_code']} but the "
            f"polygon it was measured from is {by_side[side]['ons_code']}")
        assert p.get("stops", 0) > 0, f"places/{side}: no stops inside the polygon"

    for day, block in places["days"].items():
        for side in ("west", "east"):
            assert block[side].get("departures_per_stop") is not None, (
                f"places/{day}/{side}: no departures_per_stop")

    method = data["method"].get("places")
    assert isinstance(method, dict) and _nonempty_str(method.get("selection")), (
        "method needs a 'places' section saying how a stop was assigned to an "
        "area — point-in-polygon is not the only defensible choice, so it has "
        "to be the stated one")


# ── Served map icons ────────────────────────────────────────
#
# The icons were once 1536x1024 and drawn in a 56px box — a 27x oversample that
# cost about 70 MB of decoded bitmap on a phone. A browser decodes a PNG to
# width x height x 4 bytes whatever its palette holds, so shrinking the colour
# count fixed the download and none of the memory. These stop a master being
# dropped back into the served directory, where nothing would look wrong.

ICON_DIR = ROOT / "icons"
ICON_SOURCE_DIR = ICON_DIR / "source"
MAX_SERVED_ICON_PX = 200      # the build targets 168; this is headroom, not a target


def _served_icons():
    return sorted(p for p in ICON_DIR.glob("*.png"))


def test_served_icons_are_display_sized():
    from PIL import Image
    oversized = []
    for path in _served_icons():
        with Image.open(path) as im:
            if max(im.size) > MAX_SERVED_ICON_PX:
                oversized.append(f"{path.name} is {im.width}x{im.height}")
    assert not oversized, (
        "served icons far larger than the 56px box they are drawn in: "
        + ", ".join(oversized)
        + ". Run scripts/build_icons.py — masters belong in icons/source/.")


def test_every_served_icon_has_a_master():
    """A served icon with no master cannot be re-exported at another size, and
    a master with no served icon is a marker that 404s on the map."""
    served = {p.name for p in _served_icons()}
    masters = {p.name for p in ICON_SOURCE_DIR.glob("*.png")}
    assert served == masters, (
        f"served-only: {sorted(served - masters)}; "
        f"master-only: {sorted(masters - served)}")


def test_served_icons_are_small_enough_to_ship():
    """Twelve icons load before the first bus appears; the whole set should cost
    less than a single photograph."""
    total = sum(p.stat().st_size for p in _served_icons())
    assert total < 120 * 1024, (
        f"served icons total {total/1024:.0f} KB — the build produces about 45 KB")
