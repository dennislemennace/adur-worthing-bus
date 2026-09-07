"""Tests for the community-proposal moderation gate.

`scripts/add_proposal.py` is the only code path that takes a stranger's JSON off
a public form and puts it on the site. Its two load-bearing properties are not
things a schema test can see, because both concern what the script *refuses*:

  * a submission cannot decide how the site presents it, and
  * geometry the script cannot place on this coast never reaches the map.

The pure functions are tested directly rather than by driving the CLI, so no
test can touch data/proposals.json.

Run with:  pytest
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import add_proposal as ap  # noqa: E402


def _submission(**overrides):
    """The minimum a proposal needs: a name, a one-line summary, and a line
    with at least two points on it."""
    base = {
        "name": "Example Route",
        "summary": "A line for the tests to look at",
        "polyline": [[50.8285, -0.2524], [50.8277, -0.2556]],
    }
    base.update(overrides)
    return base


# ── The category is the site's word, not the submitter's ────

def test_a_submission_cannot_publish_itself_as_official():
    """`category` decides whether the site presents a route as its own.

    isOfficialProposal() in app.js draws `official` lines on the Improvements
    map automatically; everything else waits for a reader to click it in. If
    the submitted blob could set that field, anyone with the form could have
    the site advertise their route as a project proposal.
    """
    entry = ap.normalise(_submission(category="official"), set(), "community")
    assert entry["category"] == "community"


def test_official_is_reachable_but_only_deliberately():
    # The maintainer's own routes still need a way in — via the argument the
    # caller passes, which --official sets, never via the blob.
    entry = ap.normalise(_submission(), set(), "official")
    assert entry["category"] == "official"


# ── Geometry that cannot be placed ──────────────────────────

def test_swapped_coordinates_are_rejected_and_named():
    """[lon, lat] parses fine and draws a line into the Indian Ocean.

    Both numbers are plausible on their own, so nothing but a bounds check
    catches this — and the message has to say *what* is wrong, or the reader
    goes looking for a bad stop instead of a transposed pair.
    """
    with pytest.raises(SystemExit) as exc:
        ap.normalise(_submission(polyline=[[-0.2524, 50.8285], [-0.2556, 50.8277]]),
                     set(), "community")
    assert "[lon, lat]" in str(exc.value)


@pytest.mark.parametrize("polyline, because", [
    ([[50.8285, -0.2524]], "one point is not a line"),
    ([], "no line at all"),
    ([[51.5074, -0.1278], [51.5080, -0.1280]], "central London, not this coast"),
    ([[50.8285, "-0.2524"], [50.8277, -0.2556]], "a coordinate that is a string"),
    ([[50.8285], [50.8277, -0.2556]], "a pair with one number missing"),
])
def test_unusable_geometry_is_refused(polyline, because):
    with pytest.raises(SystemExit):
        ap.normalise(_submission(polyline=polyline), set(), "community")


def test_stops_are_bounds_checked_too():
    # A stop off the coast is as wrong as a polyline point off the coast, and
    # it is the stop names that get drawn as labels.
    with pytest.raises(SystemExit):
        ap.normalise(
            _submission(stops=[{"name": "Nowhere", "lat": 0, "lon": 0}]),
            set(), "community")


def test_an_unnamed_stop_is_refused():
    with pytest.raises(SystemExit):
        ap.normalise(
            _submission(stops=[{"lat": 50.8285, "lon": -0.2524}]),
            set(), "community")


# ── The fields the frontend depends on ──────────────────────

@pytest.mark.parametrize("missing", ["name", "summary"])
def test_the_card_cannot_be_rendered_without_its_text(missing):
    obj = _submission()
    del obj[missing]
    with pytest.raises(SystemExit):
        ap.normalise(obj, set(), "community")


def test_a_colour_that_is_not_a_colour_is_refused():
    # Fed straight into a style attribute, so "red" would work in a browser and
    # "javascript:…" would not be a colour at all. Only #rrggbb passes.
    with pytest.raises(SystemExit):
        ap.normalise(_submission(color="red"), set(), "community")


def test_an_unknown_frequency_class_is_refused():
    # app.js only tests for "limited"; a typo would land silently in the
    # everything-else bucket and quietly change what the filters show.
    with pytest.raises(SystemExit):
        ap.normalise(_submission(frequency_class="hourly"), set(), "community")


# ── Identity ────────────────────────────────────────────────

def test_a_clashing_id_is_given_a_new_one_rather_than_overwriting():
    entry = ap.normalise(_submission(id="example-route"), {"example-route"}, "community")
    assert entry["id"] == "example-route-2"


def test_a_missing_id_is_derived_from_the_name():
    assert ap.normalise(_submission(), set(), "community")["id"] == "example-route"


def test_whitespace_is_trimmed_from_the_text_shown_on_the_card():
    entry = ap.normalise(_submission(name="  Spaced  ", summary="  Padded  "),
                         set(), "community")
    assert entry["name"] == "Spaced"
    assert entry["summary"] == "Padded"


def test_empty_optional_fields_are_dropped_rather_than_stored_blank():
    # An empty description is not the same as a description, and a blank string
    # in the file reads as "someone wrote nothing here" rather than "no field".
    entry = ap.normalise(_submission(description="", links=[], to=None),
                         set(), "community")
    for key in ("description", "links", "to"):
        assert key not in entry


def test_the_published_entry_satisfies_the_schema_the_site_enforces():
    # Same three fields tests/test_curated_data.py::test_proposals_schema
    # requires, checked here so the script fails before CI does.
    entry = ap.normalise(_submission(), set(), "community")
    for key in ("id", "name", "summary"):
        assert str(entry.get(key, "")).strip()
