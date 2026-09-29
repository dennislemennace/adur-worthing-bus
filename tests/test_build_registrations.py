"""scripts/build_registrations.py: the Traffic Commissioner's register, kept to
this map's routes. Rows are cut down from the DVSA CSV's shape; nothing is
downloaded."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import build_registrations as br  # noqa: E402

PLACES = br.place_words(["Brighton", "Hove", "Worthing", "Shoreham-by-Sea", "Portslade"])


def reg(reg_no, lic, number, start, finish, pub="", subsidy="No", details="", via=""):
    return {"Reg_No": reg_no, "Lic_No": lic, "Service Number": number,
            "start_point": start, "finish_point": finish, "via": via,
            "Registration Status": "Registered", "Pub_Text": pub,
            "Subsidies_Description": subsidy, "Subsidies_Details": details,
            "Service_Type_Description": "Normal Stopping", "effective_date": "13/04/26",
            "Service_Type_Other_Details": "Vary route"}


def var(reg_no, n, effective, change, short="No"):
    return {"Reg_No": reg_no, "Variation Number": str(n), "effective_date": effective,
            "received_date": "01/01/20", "Service_Type_Other_Details": change,
            "Short Notice": short}


REGISTERED = [
    # Brighton & Hove register several numbers together and name one.
    reg("PK0001213/13", "PK0001213", "1", "Brighton Station", "Downs Park",
        pub="Operating between Brighton Station and Downs Park given service number "
            "1 / 1X / 6 / 71 effective from 13 April 2026."),
    # The same licence's Crawley route of the same number, not on this map.
    reg("PK0001213/96", "PK0001213", "1", "Crawley Bus Station", "Broadfield"),
    # Stagecoach's 1 in Worthing, subsidised, and its 1 in Guildford.
    reg("PK0002571/45", "PK0002571", "1", "Worthing", "Midhurst",
        subsidy="In Part", details="West Sussex County Council"),
    reg("PK0002571/87", "PK0002571", "1", "Guildford", "Manor Park"),
    # A Brighton school run named only by its streets.
    reg("PK0001213/59", "PK0001213", "91", "Coombe Road", "Cardinal Newman School",
        subsidy="Yes", details="Brighton and Hove City Council"),
    # A registration that has lapsed is not in force.
    {**reg("PK0001213/200", "PK0001213", "7", "Brighton", "Hove"), "Registration Status": "Cancelled"},
]
VARIATIONS = [
    var("PK0001213/13", 66, "13/04/26", "Vary route, stopping places & timetable."),
    var("PK0001213/13", 65, "08/06/25", "Timetable variation to service 1X"),
    var("PK0001213/13", 65, "08/06/25", "Timetable variation to service 1X"),
    var("PK0001213/13", 12, "01/01/15", "Before the history window"),
    var("PK0002571/45", 10, "20/11/23", "Registration refresh", short="Yes"),
]


def build(services):
    return br.build(REGISTERED, VARIATIONS, set(services), PLACES)


def test_a_route_registered_under_another_number_is_found_and_says_so():
    out = build({"6"})
    [six] = out["services"]
    assert (six["operator"], six["reg_no"], six["registered_as"]) == ("BHBC", "PK0001213/13", "1")


def test_the_same_number_elsewhere_on_the_licence_is_left_out():
    regs = {(s["operator"], s["reg_no"]) for s in build({"1"})["services"]}
    assert regs == {("BHBC", "PK0001213/13"), ("SCSO", "PK0002571/45")}, \
        "a Crawley or Guildford route of the same number was attached to ours"


def test_subsidy_names_the_council():
    [sc] = [s for s in build({"1"})["services"] if s["operator"] == "SCSO"]
    assert sc["subsidy"] == "In Part" and sc["subsidised_by"] == ["West Sussex County Council"]
    [school] = build({"91"})["services"]
    assert school["subsidised_by"] == ["Brighton & Hove City Council"], \
        "a school route named by its streets was dropped as not on this map"


def test_history_is_newest_first_once_each_and_within_the_window():
    [bh] = [s for s in build({"1"})["services"] if s["operator"] == "BHBC"]
    assert [c["variation"] for c in bh["changes"]] == [66, 65]
    assert bh["changes"][0]["effective"] == "2026-04-13"
    [sc] = [s for s in build({"1"})["services"] if s["operator"] == "SCSO"]
    assert sc["changes"][0]["short_notice"] is True


def test_a_lapsed_registration_is_not_in_force():
    assert build({"7"})["unmatched"] == ["7"]


def test_the_published_file_carries_its_provenance():
    data = json.loads((ROOT / "data" / "registrations.json").read_text())
    for field in ("generated_at", "as_of", "source_url", "method", "caveats", "services"):
        assert data.get(field), f"registrations.json has no {field}"
    for s in data["services"]:
        assert s["operator"] in {"BHBC", "SCSO", "COMT"} and s["reg_no"].startswith("PK")
        assert s["subsidy"] in {"Yes", "No", "In Part", None}
        for c in s["changes"]:
            assert c["effective"] >= br.HISTORY_FROM
