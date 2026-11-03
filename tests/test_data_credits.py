"""Every source the site shows data from is credited where the others are.

FlixBus's timetable and live feeds come under the Open Database Licence, whose
one firm condition for a site like this is attribution: the producer and the
licence, named where readers can find them. The site already credits its
sources in three places (the About page's list of where the data comes from,
both footers, and THIRD_PARTY_NOTICES.md), so FlixBus belongs in each.

Run with:  pytest
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ABOUT = (ROOT / "about.html").read_text(encoding="utf-8")
INDEX = (ROOT / "index.html").read_text(encoding="utf-8")
NOTICES = (ROOT / "THIRD_PARTY_NOTICES.md").read_text(encoding="utf-8")

ODBL = "https://opendatacommons.org/licenses/odbl/1-0/"
FLIXBUS_DATASET = ("https://transport.data.gouv.fr/datasets/"
                   "flixbus-horaires-theoriques-du-reseau-europeen-1")


def _between(text, start, end):
    assert start in text, f"{start!r} is missing"
    return text.split(start, 1)[1].split(end, 1)[0]


def test_the_about_page_credits_flixbus_and_its_licence():
    sources = _between(ABOUT, '<h2 id="data">', "</dl>")
    assert "FlixBus" in sources
    assert FLIXBUS_DATASET in sources, "the credit does not link the dataset"
    assert ODBL in sources, "the credit does not link the licence"


def test_both_footers_credit_flixbus():
    full = _between(INDEX, '<footer class="site-footer">', '<p class="site-footer-links">')
    compact = _between(INDEX, '<p class="site-footer-compact">', "</p>")
    assert "FlixBus" in full, "the desktop footer leaves FlixBus out"
    assert "FlixBus" in compact, "the phone footer leaves FlixBus out"


def test_the_third_party_notices_list_flixbus():
    section = _between(NOTICES, "## FlixBus", "\n## ")
    assert "Open Database Licence" in section
    assert ODBL in section


# bustimes.org's vehicle list draws each bus in its own livery and names its
# fleet number and model (data/fleet.json, scripts/build_fleet.py). Shown to
# everyone from 3 November 2026, so it is credited where the other sources are.

def test_the_about_page_credits_bustimes_org():
    sources = _between(ABOUT, '<h2 id="data">', "</dl>")
    assert "https://bustimes.org/" in sources, "the About page leaves bustimes.org out"


def test_both_footers_credit_bustimes_org():
    full = _between(INDEX, '<footer class="site-footer">', '<p class="site-footer-links">')
    compact = _between(INDEX, '<p class="site-footer-compact">', "</p>")
    assert "bustimes.org" in full, "the desktop footer leaves bustimes.org out"
    assert "bustimes.org" in compact, "the phone footer leaves bustimes.org out"


def test_the_third_party_notices_list_bustimes_org():
    section = _between(NOTICES, "## bustimes.org", "\n## ")
    assert "https://bustimes.org/" in section and "data/fleet.json" in section
