"""Tests that the privacy notice, terms and credits describe the site that ships.

`about.html` once said "There is no analytics", which was true until it was
not, and it once described submissions as public issues after they became
private. A notice drifts the same way comments do: nobody changes it because
nothing fails. These checks make it fail.

  * every outside host a page loads a script or stylesheet from is named in the
    notice, and the fonts and map library are served from the site itself,
  * the notice names who is responsible, gives a private contact route, and
    tells readers their rights and how to complain,
  * visit counting is described, can be switched off, and every kind of action
    counted is one the notice lists,
  * the browser storage table lists exactly the keys the code uses,
  * every form that sends something in carries the publication box,
  * every page's footer reaches the privacy notice and the terms, and
  * the credits the data providers ask for are present.

Run with:  pytest
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
APP = (ROOT / "app.js").read_text()
INDEX = (ROOT / "index.html").read_text()
ABOUT = (ROOT / "about.html").read_text()
PRIVACY = (ROOT / "privacy.html").read_text()
TERMS = (ROOT / "terms.html").read_text()
PAGE_LOADER = (ROOT / "analytics-page.js").read_text()
PAGES = {"index.html": INDEX, "about.html": ABOUT, "privacy.html": PRIVACY, "terms.html": TERMS}
CONTACT = "privacy@worthingbrightonbus.co.uk"

# How the notice refers to each outside host a script comes from. A host that
# is missing here fails the first test, which is the point: somebody has to
# decide how readers are told about it.
SCRIPT_HOSTS = {
    "challenges.cloudflare.com": "Cloudflare Turnstile",
    "gc.zgo.at": "GoatCounter",
}

# Each family of counted action, and the words the notice uses for it.
EVENT_FAMILIES = {
    "view": "which part of the site is opened",
    "journey": "a journey checked",
    "councillor": "a councillor letter",
    "submission": "a form sent",
    "gap-alert": "a gap alert shown",
    "delay-map": "a stretch of road opened on the delay map",
    "api-waking": "live service being woken up",
    "a11y": "an accessibility setting changed",
    "near-me": "\"stops near me\" used",
}


def text_of(html: str) -> str:
    html = re.sub(r"<(script|style)\b.*?</\1>", " ", html, flags=re.S)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


NOTICE = text_of(PRIVACY)


def outside_hosts() -> set:
    hosts = set()
    for text in (*PAGES.values(), APP, PAGE_LOADER):
        # Things a page loads: scripts, and stylesheets. Not <link rel="canonical">,
        # which names this site's own address and fetches nothing.
        for m in re.finditer(r'<script[^>]+src="https://([^/"]+)', text):
            hosts.add(m.group(1))
        for m in re.finditer(r'<link[^>]+rel="stylesheet"[^>]+href="https://([^/"]+)', text):
            hosts.add(m.group(1))
        for m in re.finditer(r'(?:\.src\s*=\s*|_SCRIPT_URL\s*=\s*)"https://([^/"]+)', text):
            hosts.add(m.group(1))
    return hosts


# ── Outside hosts ───────────────────────────────────────────

def test_every_outside_script_host_is_named_in_the_privacy_notice():
    hosts = outside_hosts()
    assert hosts, "found no outside scripts at all, so this test is looking in the wrong place"
    unknown = sorted(h for h in hosts if h not in SCRIPT_HOSTS)
    assert not unknown, (
        f"pages load from {unknown}, which the privacy notice has not been checked for; "
        "add each to SCRIPT_HOSTS with the name the notice uses, and name it in privacy.html")
    missing = sorted(SCRIPT_HOSTS[h] for h in hosts if SCRIPT_HOSTS[h].lower() not in NOTICE.lower())
    assert not missing, f"the privacy notice does not mention: {missing}"


def test_fonts_and_the_map_library_are_served_from_the_site():
    # Every visit used to send the reader's IP address to Google (fonts) and to
    # unpkg (Leaflet), and run whatever unpkg returned.
    for name, html in PAGES.items():
        for host in ("fonts.googleapis.com", "fonts.gstatic.com", "unpkg.com"):
            assert host not in html, f"{name} still loads from {host}"
    for path in ("vendor/leaflet-1.9.4/leaflet.js", "vendor/leaflet-1.9.4/leaflet.css",
                 "vendor/leaflet-1.9.4/LICENSE", "fonts/fonts.css"):
        assert (ROOT / path).exists(), f"{path} is referenced but missing"
    for m in re.finditer(r"url\(\./([^)]+)\)", (ROOT / "fonts/fonts.css").read_text()):
        assert (ROOT / "fonts" / m.group(1)).exists(), f"fonts.css names a missing file: {m.group(1)}"
    for font in ("fraunces", "outfit", "jetbrains-mono"):
        assert (ROOT / "fonts" / f"OFL-{font}.txt").exists(), f"no OFL licence text shipped for {font}"


def test_map_tiles_come_from_the_one_host_the_osm_policy_names():
    tiles = re.search(r'url:\s*"(https://[^"]*tile\.openstreetmap\.org[^"]*)"', APP)
    assert tiles, "the OpenStreetMap tile URL was not found"
    assert tiles.group(1) == "https://tile.openstreetmap.org/{z}/{x}/{y}.png", tiles.group(1)


# ── What UK GDPR asks the notice to say ─────────────────────

def test_the_notice_names_the_controller_and_a_private_contact():
    assert "Connor R" in NOTICE and "data controller" in NOTICE
    assert f'href="mailto:{CONTACT}"' in PRIVACY


def test_the_notice_covers_lawful_basis_retention_recipients_rights_and_complaints():
    for phrase in ("legitimate interests", "consent", "How long it is kept",
                   "Who else handles it", "United States", "Your rights",
                   "one month", "Information Commissioner", "ico.org.uk"):
        assert phrase in NOTICE, f"the privacy notice does not cover: {phrase}"


def test_nobody_is_told_to_make_a_personal_request_in_public():
    for name, html in (("about.html", ABOUT), ("privacy.html", PRIVACY)):
        assert CONTACT in html, f"{name} gives no private contact"
    assert "public issues on GitHub straight away" not in ABOUT + PRIVACY
    assert "say so on the same\n    tracker" not in ABOUT


# ── Visit counting ──────────────────────────────────────────

def test_a_site_that_counts_visits_describes_it_and_its_opt_outs():
    if "gc.zgo.at" not in outside_hosts():
        return
    notice = NOTICE.lower()
    assert "no analytics" not in notice and "no analytics" not in text_of(ABOUT).lower()
    assert "goatcounter" in notice
    assert "do not track" in notice and "global privacy control" in notice
    assert 'id="counting-switch"' in PRIVACY, "the notice offers no switch to stop counting"


def test_every_counted_action_is_one_the_notice_lists():
    names = set(re.findall(r'track(?:Once)?\(\s*[`"]([a-z0-9-]+?)(?:-\$\{|[`"])', APP))
    assert names, "found no track() calls; the pattern is out of date"
    notice = NOTICE.lower()
    unlisted = []
    for name in sorted(names):
        family = next((f for f in EVENT_FAMILIES if name == f or name.startswith(f + "-")), None)
        if family is None:
            unlisted.append(f"{name} (no family: add it to EVENT_FAMILIES and to privacy.html)")
        elif EVENT_FAMILIES[family].lower() not in notice:
            unlisted.append(f"{name} (the notice does not say '{EVENT_FAMILIES[family]}')")
    assert not unlisted, "counted actions the privacy notice does not describe:\n  " + "\n  ".join(unlisted)


def test_every_loader_uses_the_same_code_and_honours_every_opt_out():
    app_code = re.search(r'GOATCOUNTER_CODE:\s*"([^"]*)"', APP)
    page_code = re.search(r'var code = "([^"]*)"', PAGE_LOADER)
    assert app_code and page_code, "a GoatCounter loader is missing its site code"
    assert app_code.group(1) == page_code.group(1), (
        f"app.js counts to '{app_code.group(1)}' and analytics-page.js to '{page_code.group(1)}'")
    opt_out_key = re.search(r'ANALYTICS_OPT_OUT_KEY = "([^"]+)"', APP).group(1)
    for label, src in (("analytics-page.js", PAGE_LOADER), ("privacy.html", PRIVACY)):
        assert f'"{opt_out_key}"' in src, f"{label} does not use the opt-out key {opt_out_key}"
    for check in ("globalPrivacyControl", "doNotTrack", "localhost", opt_out_key):
        assert check in PAGE_LOADER, f"analytics-page.js does not check {check}"
    for name in ("about.html", "privacy.html", "terms.html"):
        assert 'src="analytics-page.js"' in PAGES[name], f"{name} does not use the shared loader"


# ── Storage, forms, footers, credits ────────────────────────

def test_the_storage_table_lists_exactly_the_keys_the_code_uses():
    consts = dict(re.findall(r'const ([A-Z0-9_]+_KEY)\s*=\s*"([^"]+)"', APP))
    used = set()
    for m in re.finditer(r'localStorage\.(?:getItem|setItem|removeItem)\(\s*([A-Z0-9_]+|"[^"]+")', APP):
        token = m.group(1)
        used.add(token.strip('"') if token.startswith('"') else consts[token])
    used.add(consts["ANALYTICS_OPT_OUT_KEY"])
    listed = set(re.findall(r"<tr><td><code>([^<]+)</code></td>", PRIVACY))
    assert used == listed, (
        f"stored in the browser but not listed: {sorted(used - listed)}; "
        f"listed but no longer stored: {sorted(listed - used)}")


def test_every_submission_form_carries_the_publication_box():
    # Five ways to send something in: three forms in index.html, and the bus
    # report and the route editor built in app.js.
    for form_id in ("report-stop-form", "news-form", "suggest-form"):
        start = INDEX.index(f'id="{form_id}"')
        end = INDEX.index("</form>", start)
        assert 'name="publishAck"' in INDEX[start:end], f"#{form_id} has no publication box"
    assert APP.count('<input type="checkbox" id="rb-ack" name="publishAck">') == 1
    assert APP.count('<input type="checkbox" id="ed-ack" name="publishAck">') == 1
    assert APP.count("publishAck: publishAcknowledged(") == 5, "a submission does not send its box"


def test_every_page_footer_reaches_the_privacy_notice_and_the_terms():
    for name, html in PAGES.items():
        footer = html[html.index("<footer"):html.index("</footer>")]
        for target in ("privacy.html", "terms.html"):
            if name == target:
                continue
            assert f'href="{target}"' in footer, f"the footer of {name} does not link to {target}"


def test_the_credits_data_providers_ask_for_are_given():
    about = text_of(ABOUT)
    for credit in (
        "source: http://transportapi.com/",
        "Source: Office for National Statistics licensed under the Open Government Licence v.3.0",
        "Contains OS data © Crown copyright and database right",
        "Contains Royal Mail data © Royal Mail copyright and database right",
        "Contains public sector information licensed under the Open Government Licence v3.0",
        "OpenStreetMap contributors",
    ):
        assert credit.lower() in about.lower(), f"about.html is missing the credit: {credit}"


def test_the_code_and_content_have_licences():
    assert (ROOT / "LICENSE").read_text().startswith("MIT License")
    content = (ROOT / "LICENSE-content.md").read_text()
    assert "CC BY 4.0" in content and "does not cover" in content
    assert "MIT" in TERMS and "CC BY 4.0" in TERMS


# ── Online Safety Act record ────────────────────────────────

def test_the_online_safety_record_covers_what_the_act_asks_for():
    record = " ".join((ROOT / "docs" / "ONLINE_SAFETY.md").read_text().split())
    for section in ("User-to-user service", "Search service", "Illegal content risk assessment",
                    "Children's access assessment", "children's risk assessment",
                    "accountable", "Review by", "What would change this"):
        assert section.lower() in record.lower(), f"docs/ONLINE_SAFETY.md does not cover: {section}"
    # Ofcom's seventeen kinds of priority illegal harm, each assessed.
    for harm in ("Terrorism", "Child sexual exploitation", "Hate", "Harassment",
                 "Controlling or coercive", "Intimate image abuse", "Extreme pornography",
                 "Sexual exploitation of adults", "Human trafficking", "Unlawful immigration",
                 "Fraud", "Proceeds of crime", "Drugs", "Firearms", "suicide",
                 "Foreign interference", "Animal cruelty"):
        assert harm.lower() in record.lower(), f"the risk assessment does not assess: {harm}"


def test_the_terms_give_a_way_to_report_content_and_complain():
    section = TERMS[TERMS.index('id="reporting"'):]
    section = " ".join(text_of(section[:section.index("<h2>Reusing")]).split())
    assert CONTACT in TERMS[TERMS.index('id="reporting"'):]
    # The heading says "complaints", so check the sentences, not the word.
    for phrase in ("illegal", "harmful to children", "complain about a decision",
                   "looked at again", "removed as soon as it is found"):
        assert phrase in section, f"the reporting section does not mention: {phrase}"
    assert 'href="terms.html#reporting"' in ABOUT
