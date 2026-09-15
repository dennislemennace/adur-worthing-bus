"""Tests for how the site's own writing reads.

The site's distinctive material is real: named journeys, measured fare
comparisons, departure counts taken from the published timetable. A review
found that uniform phrasing was flattening it — seven of eight objectives
ending on the same construction, the same intensifiers throughout, and em
dashes used as a general-purpose joint about a hundred and fifty times.

None of that is a bug a normal test can see, which is exactly why it drifted.
These checks are deliberately coarse: they do not judge writing, they count the
things that were repeated until they stopped meaning anything.

**What counts as reader-facing.** Only text that reaches a screen. Comments in
`app.js`, `index.html` and the `_comment` keys in the data files are notes to
whoever maintains this, and they are exempt — an em dash inside an explanation
of why a constant is 400 metres is doing an honest job.

Run with:  pytest
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DASH = "\u2014"

# Keys holding rationale for maintainers rather than copy for readers.
# `why` explains to us why a journey preset was chosen; the site never shows it.
SKIP_KEYS = {"why"}

# Data files that are not prose at all.
SKIP_FILES = {"timetable.json", "stops.json"}


def strip_js_comments(src: str) -> str:
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r'(?m)(^|[^:"\'\\])//.*$', r"\1", src)


def strip_html_comments(src: str) -> str:
    """HTML comments, and CSS comments inside a <style> block.

    about.html inlines its stylesheet so the page still works when everything
    else is unavailable, and the note explaining why is a maintainer comment
    like any other.
    """
    src = re.sub(r"<!--.*?-->", "", src, flags=re.S)
    return re.sub(r"/\*.*?\*/", "", src, flags=re.S)


def json_strings(node):
    """Every string the site could render: skips `_`-prefixed and rationale keys."""
    if isinstance(node, dict):
        for k, v in node.items():
            if k.startswith("_") or k in SKIP_KEYS:
                continue
            yield from json_strings(v)
    elif isinstance(node, list):
        for v in node:
            yield from json_strings(v)
    elif isinstance(node, str):
        yield node


def reader_facing_text():
    """{label: text} for everything a visitor can read."""
    out = {}
    out["index.html"] = strip_html_comments((ROOT / "index.html").read_text())
    out["about.html"] = strip_html_comments((ROOT / "about.html").read_text())
    out["privacy.html"] = strip_html_comments((ROOT / "privacy.html").read_text())
    out["terms.html"] = strip_html_comments((ROOT / "terms.html").read_text())
    out["app.js"] = strip_js_comments((ROOT / "app.js").read_text())
    for p in sorted((ROOT / "data").glob("*.json")):
        if p.name in SKIP_FILES:
            continue
        try:
            d = json.loads(p.read_text())
        except ValueError:
            continue
        out[f"data/{p.name}"] = "\n".join(json_strings(d))
    return out


def objectives():
    return json.loads((ROOT / "data" / "objectives.json").read_text())["objectives"]


# ── Em dashes ───────────────────────────────────────────────

# A budget, not a ban. Some earn their place, and `"—"` is also the site's
# placeholder for a value it does not have, which is typography rather than
# prose. The count was 152 when this was written.
DASH_BUDGET = 30


def test_em_dashes_are_not_the_default_joint():
    text = reader_facing_text()
    counts = {k: v.count(DASH) for k, v in text.items() if v.count(DASH)}
    total = sum(counts.values())
    breakdown = "\n".join(f"    {n:4d}  {k}" for k, n in
                          sorted(counts.items(), key=lambda kv: -kv[1]))
    assert total <= DASH_BUDGET, (
        f"{total} em dashes in reader-facing copy, budget is {DASH_BUDGET}:\n"
        f"{breakdown}\n"
        "  An aside that is really a second sentence wants a full stop; a\n"
        "  parenthetical wants commas; `Label — value` wants a colon."
    )


def test_no_single_surface_hoards_them():
    """A passing total hiding one page that is still all dashes."""
    text = reader_facing_text()
    for label, body in text.items():
        n = body.count(DASH)
        assert n <= 12, f"{label} alone carries {n} em dashes"


# ── The cadence ─────────────────────────────────────────────

def test_the_ask_is_is_not_every_objectives_last_word():
    """Seven of eight objectives ended on a paragraph opening "The ask is".

    Keeping the ask is the point of the page. Arriving at it the same way eight
    times is what made the set read as generated.
    """
    hits = [o["id"] for o in objectives()
            if re.search(r"(^|\n)\s*The ask is", o.get("description", ""))]
    assert len(hits) <= 2, (
        f"{len(hits)} of {len(objectives())} objectives open a paragraph with "
        f'"The ask is": {", ".join(hits)}'
    )


TICS = ["actually", "genuinely", "simply", "really"]


def test_intensifiers_are_gone_from_the_objectives():
    """Words that add emphasis instead of information.

    Each was used where the sentence was already true without it, which is the
    tell: the writing is reaching for force it has not earned. The measured
    figures next to them do the work these words were pretending to do.
    """
    bad = []
    for o in objectives():
        for tic in TICS:
            for m in re.finditer(rf"\b{tic}\b", o.get("description", ""), re.I):
                s = o["description"]
                bad.append(f'{o["id"]}: …{s[max(0, m.start() - 45):m.end() + 45]}…')
    assert not bad, "intensifiers left in objective copy:\n  " + "\n  ".join(bad)


# ── One boundary, described one way ─────────────────────────

def test_the_council_boundary_is_in_one_place():
    """About said Shoreham; the case document says Portslade.

    Two different places, four miles apart, in the same project's published
    claims. `data/council_boundaries.json` is the authority and agrees with the
    case document. The ticket-zone boundary is a separate line and may be
    described separately, but the council one has to be consistent.
    """
    sources = {
        "about.html": (ROOT / "about.html").read_text(),
        "data/updates.json": (ROOT / "data" / "updates.json").read_text(),
        "docs/THE_CASE.md": (ROOT / "docs" / "THE_CASE.md").read_text(),
    }
    wrong = []
    for label, text in sources.items():
        for m in re.finditer(r"(council )?boundary at Shoreham|Shoreham boundary",
                             text, re.I):
            wrong.append(f"{label}: …{text[max(0, m.start() - 60):m.end() + 60]}…")
    assert not wrong, (
        "the council boundary is described as being at Shoreham; it runs "
        "through Portslade:\n  " + "\n  ".join(wrong)
    )


def test_an_articles_summary_does_not_outrun_its_body():
    """The Ticketer piece claimed more in its standfirst than in its text.

    The summary said shared equipment "removes the technical excuse". The body
    says it "makes that objection harder to sustain" and then, four paragraphs
    down and behind the fold, that "it does not by itself make acceptance
    possible". On a campaign site the overstated half is the half that gets
    quoted back at you.
    """
    updates = json.loads((ROOT / "data" / "updates.json").read_text())
    items = updates if isinstance(updates, list) else updates["updates"]
    for u in items:
        summary = u.get("summary", "")
        assert "removes the technical excuse" not in summary, (
            f'{u["id"]}: the summary states flatly what the body qualifies'
        )
