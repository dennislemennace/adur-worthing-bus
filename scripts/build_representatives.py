#!/usr/bin/env python3
"""
Build data/representatives.json — who to write to about an objective.

    python scripts/build_representatives.py

The site's "Email your councillor" flow turns a postcode into the people who
answer for it. postcodes.io does the geography at runtime and hands back ONS
codes; this script builds the other half, mapping those codes to the councillors
who sit for them.

**Keyed on ONS codes, never names.** postcodes.io returns `codes.ced` and
`codes.admin_ward`, and those codes survive the renaming and re-spelling that
council directories do constantly — "Buckingham Ward, Shoreham-by-Sea" in one
source is "Buckingham" in the other. The join between the two happens here,
once, where a mismatch is a build error rather than a resident emailing nobody.

**Nothing is derived or guessed.** Councillor addresses come from each council's
own ModernGov directory service, which is the list the council publishes for
constituents to use. Where a councillor's official address cannot be read out of
that feed they are left out, and the site falls back to the council's contact
page. Guessing firstname.lastname@council.gov.uk would be right often enough to
be trusted and wrong often enough to send someone's email into a void.

Only official council-domain addresses are taken. Directories sometimes carry a
personal address as well; republishing that is not this script's business.

Re-run after an election. Every area records `checked_on`, so staleness is
visible on the page rather than silent.
"""
import json
import re
import sys
import unicodedata
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "representatives.json"

# How much of the previous file's coverage a rebuild must keep. Elections do
# move boundaries, so this is not 100%; it is set to catch a directory that
# has moved or a feed answering partially, not ordinary churn.
COVERAGE_FLOOR = 0.9

UA = {"User-Agent": "adur-worthing-bus/1.0 (+https://github.com/dennislemennace/adur-worthing-bus)"}

ONS = ("https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services"
       "/{service}/FeatureServer/0/query")

# Each council publishes its own members through a ModernGov web service. The
# `level` says which postcodes.io field identifies one of its areas: a county
# councillor sits for an electoral division, a district or unitary councillor
# for a ward.
COUNCILS = [
    {"body": "WSCC", "level": "ced",
     "name": "West Sussex County Council",
     "host": "westsussex.moderngov.co.uk",
     "domains": ("westsussex.gov.uk",),
     "county": "West Sussex", "districts": None},
    # East Sussex is here because the corridor does not stop at Brighton: the
    # integrated-plan objective needs all three authorities, and a reader in
    # Lewes or Seaford has a county councillor with a say in it.
    {"body": "ESCC", "level": "ced",
     "name": "East Sussex County Council",
     "host": "democracy.eastsussex.gov.uk",
     "domains": ("eastsussex.gov.uk",),
     "county": "East Sussex", "districts": None},
    {"body": "ADUR_WORTHING", "level": "ward",
     "name": "Adur District Council and Worthing Borough Council",
     "host": "democracy.adur-worthing.gov.uk",
     "domains": ("adur.gov.uk", "worthing.gov.uk"),
     "county": None, "districts": ("Adur", "Worthing")},
    {"body": "BHCC", "level": "ward",
     "name": "Brighton & Hove City Council",
     "host": "democracy.brighton-hove.gov.uk",
     "domains": ("brighton-hove.gov.uk",),
     "county": None, "districts": ("Brighton and Hove",)},
]

EMAIL = re.compile(r"[\w.+-]+@[\w.-]+\.\w+")

# Adur and Worthing publish one combined directory, and both have a Marine
# ward. The feed tells them apart with a settlement suffix — "Marine Ward,
# Shoreham-by-Sea" against "Marine Ward, Worthing" — but a settlement is not a
# district, so the suffix has to be translated before it can pick a code.
# Explicit rather than inferred: getting this wrong sends a Shoreham resident's
# email to a Worthing councillor, which looks like it worked.
PLACE_DISTRICTS = {
    "shoreham by sea": "Adur",
    "southwick": "Adur",
    "southwick and fishersgate": "Adur",
    "lancing": "Adur",
    "sompting": "Adur",
    "worthing": "Worthing",
}


def get(url: str, params: dict | None = None) -> bytes:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=90) as r:
        return r.read()


def norm(s: str) -> str:
    """Ward and division names, reduced to something two sources can agree on.

    Drops the "Ward"/"ED" noun the sources disagree about attaching, spells out
    the ampersand, and strips punctuation — so "St Mary's Ward, Shoreham-by-Sea"
    and "St Marys" meet in the middle.
    """
    s = unicodedata.normalize("NFKD", s or "").replace("&", " and ").lower()
    s = re.sub(r"\b(ward|ed|electoral division)\b", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def ons_areas() -> tuple[dict, dict]:
    """ONS code -> name, for the electoral divisions and wards we cover.

    Two tables because Brighton & Hove is a unitary authority: it has wards but
    no electoral divisions, so it is absent from the two-tier England lookup
    entirely and has to come from the UK-wide one.
    """
    two_tier = json.loads(get(ONS.format(service="WD25_LAD25_CTY25_CED25_EN_LU"), {
        "where": "CTY25NM IN ('West Sussex', 'East Sussex')",
        "outFields": "WD25CD,WD25NM,LAD25NM,CTY25NM,CED25CD,CED25NM",
        "returnGeometry": "false", "f": "json", "resultRecordCount": 4000,
    }))["features"]
    unitary = json.loads(get(ONS.format(service="WD25_LAD25_CTYUA25_RGN25_CTRY25_UK_LU"), {
        "where": "LAD25NM='Brighton and Hove'",
        "outFields": "WD25CD,WD25NM,LAD25NM",
        "returnGeometry": "false", "f": "json", "resultRecordCount": 2000,
    }))["features"]

    ceds, wards = {}, {}
    for f in two_tier:
        a = f["attributes"]
        # `district` on a division is its county: divisions do not sit inside a
        # single district, and the county is what has to keep West Sussex's
        # "Lancing ED" apart from an identically-named division elsewhere.
        ceds[a["CED25CD"]] = {"name": a["CED25NM"], "district": a["CTY25NM"]}
        wards[a["WD25CD"]] = {"name": a["WD25NM"], "district": a["LAD25NM"]}
    for f in unitary:
        a = f["attributes"]
        wards[a["WD25CD"]] = {"name": a["WD25NM"], "district": a["LAD25NM"]}
    return ceds, wards


def moderngov(host: str) -> list[tuple[str, list[dict]]]:
    """(ward title, members) from a council's ModernGov directory service."""
    root = ET.fromstring(get(f"https://{host}/mgWebService.asmx/GetCouncillorsByWard"))
    out = []
    for ward in root.findall(".//ward"):
        title = (ward.findtext("wardtitle") or "").strip()
        if not title or norm(title) == "":
            continue                       # placeholder rows appear in some feeds
        members = []
        for c in ward.findall(".//councillor"):
            members.append({
                "id": (c.findtext("councillorid") or "").strip(),
                "name": re.sub(r"^Councillor\s+", "", (c.findtext("fullusername") or "").strip()),
                "party": (c.findtext("politicalpartytitle") or "").strip(),
                "blob": " ".join("".join(e.itertext()) for e in c),
            })
        out.append((title, members))
    return out


def official_email(blob: str, domains: tuple[str, ...]) -> str | None:
    for addr in EMAIL.findall(blob or ""):
        if addr.lower().split("@")[-1] in domains:
            return addr.lower()
    return None


def main() -> None:
    today = date.today().isoformat()
    print("Fetching ONS area codes…")
    ceds, wards = ons_areas()
    print(f"  {len(ceds)} electoral divisions, {len(wards)} wards")

    areas, unmatched, no_email = {}, [], []

    for council in COUNCILS:
        print(f"\n{council['name']} ({council['host']})")
        table = ceds if council["level"] == "ced" else wards
        # Candidates restricted to this council's own districts, so "Marine"
        # in Adur cannot be matched against "Marine" in Worthing.
        wanted = council["districts"] or ((council["county"],) if council["county"] else None)
        pool = {code: meta for code, meta in table.items()
                if wanted is None or meta["district"] in wanted}
        by_name: dict[str, list[str]] = {}
        for code, meta in pool.items():
            by_name.setdefault(norm(meta["name"]), []).append(code)

        for title, members in moderngov(council["host"]):
            # Councils decorate their ward titles in ways ONS does not, and the
            # decorations differ per council, so the forms are tried in order of
            # decreasing trust rather than by guessing which council this is.
            #
            #   1. The whole title. Load-bearing: East Sussex has a division
            #      genuinely called "Arlington, East Hoathly and Hellingly", and
            #      splitting on its comma first would tear the name in half.
            #   2. After " - ", for feeds that prefix the town
            #      ("Eastbourne - Meads", "Hastings - Baird and Ore").
            #   3. Before ",", for feeds that suffix the settlement
            #      ("Buckingham Ward, Shoreham-by-Sea").
            candidates = by_name.get(norm(title), [])
            place = ""
            if not candidates and " - " in title:
                candidates = by_name.get(norm(title.split(" - ", 1)[1]), [])
            if not candidates and "," in title:
                base, _, place = title.partition(",")
                candidates = by_name.get(norm(base), [])

            if len(candidates) > 1 and place.strip():
                # Two wards of the same name in one feed — Adur and Worthing
                # both have a Marine. The settlement suffix decides.
                want = PLACE_DISTRICTS.get(norm(place))
                if want is None:
                    unmatched.append((council["body"], title,
                                      f"{len(candidates)} codes, and {norm(place)!r} "
                                      f"is not a place PLACE_DISTRICTS knows"))
                    continue
                candidates = [c for c in candidates if pool[c]["district"] == want]
            if len(candidates) != 1:
                unmatched.append((council["body"], title, f"{len(candidates)} codes"))
                continue

            code = candidates[0]
            people = []
            for m in members:
                email = official_email(m["blob"], council["domains"])
                if not email:
                    no_email.append((council["body"], title, m["name"]))
                    continue
                person = {"name": m["name"], "email": email}
                if m["party"]:
                    person["party"] = m["party"]
                if m["id"]:
                    person["url"] = f"https://{council['host']}/mgUserInfo.aspx?UID={m['id']}"
                people.append(person)
            if not people:
                continue

            areas[code] = {
                "name": pool[code]["name"],
                "body": council["body"],
                "council": council["name"],
                "members": people,
                "source_url": f"https://{council['host']}/mgMemberIndex.aspx",
                "checked_on": today,
            }
        got = sum(1 for a in areas.values() if a["body"] == council["body"])
        print(f"  {got} areas with at least one published address")

    if unmatched:
        print(f"\n{len(unmatched)} areas left out — no single ONS code matched:")
        for body, title, why in unmatched:
            print(f"  {body:14} {title!r} — {why}")
    if no_email:
        print(f"\n{len(no_email)} councillors left out — no official address in the feed:")
        for body, title, name in no_email[:20]:
            print(f"  {body:14} {title:38} {name}")

    if not areas:
        sys.exit("No areas resolved — refusing to write an empty file.")

    # Non-empty is a very low bar for a file that decides who a resident's
    # letter is addressed to. A council site that reorganises its directory,
    # or a feed that answers partially, produces a smaller file rather than an
    # error — and the failure is invisible, because what is left still looks
    # perfectly correct.
    #
    # So the new file is compared against the one it would replace, and a
    # material loss of coverage stops the run. --force is for the case where
    # the loss is real, which does happen: boundaries change at an election.
    previous = {}
    if OUT.exists():
        try:
            previous = json.loads(OUT.read_text()).get("areas", {})
        except (json.JSONDecodeError, OSError) as exc:
            print(f"Note: couldn't read the existing {OUT.name} ({exc}) — "
                  "publishing without a coverage comparison.")

    if previous:
        lost = sorted(set(previous) - set(areas))
        was = sum(len(a.get("members", [])) for a in previous.values())
        now = sum(len(a.get("members", [])) for a in areas.values())
        print(f"\nCoverage: {len(previous)} → {len(areas)} areas, "
              f"{was} → {now} councillors")
        if lost:
            print(f"{len(lost)} areas would disappear:")
            for code in lost[:20]:
                print(f"  {code}  {previous[code].get('name', '?')} "
                      f"({previous[code].get('council', '?')})")
        shrunk = (len(areas) < len(previous) * COVERAGE_FLOOR
                  or now < was * COVERAGE_FLOOR)
        if shrunk and "--force" not in sys.argv:
            sys.exit(
                f"\nRefusing to write: coverage fell below "
                f"{COVERAGE_FLOOR:.0%} of the existing file. A resident whose "
                f"area vanished is told there is nobody to write to, which "
                f"reads as a fact about their council rather than a broken "
                f"build. Re-run with --force if the loss is real.")

    OUT.write_text(json.dumps({
        "_comment": (
            "Who to write to about an objective, keyed by ONS area code. "
            "postcodes.io returns codes.ced and codes.admin_ward for a postcode "
            "at runtime; these are the same codes. Built by "
            "scripts/build_representatives.py from each council's own published "
            "directory — no address here is derived from a name pattern, and a "
            "councillor whose official address is not published is absent rather "
            "than guessed. Re-run after an election."
        ),
        "generated_on": today,
        "geography": {
            "source": "ONS Open Geography Portal",
            "datasets": ["WD25_LAD25_CTY25_CED25_EN_LU", "WD25_LAD25_CTYUA25_RGN25_CTRY25_UK_LU"],
            "licence": "Open Government Licence v3.0",
            "attribution": "Contains OS data © Crown copyright and database right 2025. "
                           "Contains National Statistics data © Crown copyright and database right 2025.",
        },
        "areas": dict(sorted(areas.items())),
    }, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"\nWrote {OUT.relative_to(ROOT)} — {len(areas)} areas, "
          f"{sum(len(a['members']) for a in areas.values())} councillors")


if __name__ == "__main__":
    main()
