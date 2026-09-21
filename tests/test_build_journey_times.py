"""Publishing one document per service, as one operator runs it.

This file exists because the script it tests had none, and shipped the defect
that prompted it: a service number was treated as the identity of a route, so
the 1, the 5 and the 7 were each published as one document merging two
operators' routes.

They are not variants of one route. Measured over 16-18 September, Brighton &
Hove's and Stagecoach's versions of each of those three numbers share **zero
stops** — the 7 is Marina Cinema and George Street for one, Swandean Hospital
and Lancing for the other. Merged, the stop pickers offered pairs spanning both
routes, no bus had ever run them, and the view answered "no bus we tracked made
that trip", which reads as a broken tool rather than an impossible question.
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import build_journey_times as bjt                                 # noqa: E402

META = {"days": ["2026-09-17"], "data_versions": ["v1"]}


def call(trip, service, operator, atco, index, secs, day="2026-09-17",
         headsign="Somewhere", timepoint=1):
    return {
        "day": day, "trip_id": trip, "service": service, "operator": operator,
        "atco": atco, "stop_name": f"Stop {atco}", "stop_index": index,
        "observed_secs": secs, "scheduled_secs": secs,
        "journey_start": "08:00", "direction": "westbound",
        "headsign": headsign, "match": "declared",
        "timepoint": timepoint, "estimated": False,
    }


def journey(trip, service, operator, atcos, headsign="Somewhere"):
    """One run calling at `atcos`, a minute apart."""
    return [call(trip, service, operator, atco, i, 28_800 + i * 60,
                 headsign=headsign)
            for i, atco in enumerate(atcos)]


# ── One number is not one route ─────────────────────────────

def test_two_operators_running_one_number_are_published_apart():
    rows = []
    for n in range(3):
        rows += journey(f"BH{n}", "1", "BHBC", ["A1", "A2", "A3"], "Portslade")
        rows += journey(f"SC{n}", "1", "SCSO", ["B1", "B2", "B3"], "Midhurst")
    docs = bjt.build(rows, META)

    assert set(docs) == {("1", "BHBC"), ("1", "SCSO")}, \
        f"two operators' routes were merged into one document: {set(docs)}"
    bh, sc = docs[("1", "BHBC")], docs[("1", "SCSO")]
    assert {s["atco"] for s in bh["stops"]} == {"A1", "A2", "A3"}
    assert {s["atco"] for s in sc["stops"]} == {"B1", "B2", "B3"}, \
        "one operator's stops appeared in the other's document"


def test_a_document_says_whose_service_it_is():
    # Without it the browser cannot label the picker, and a reader cannot tell
    # which company a figure is about — which is the point of naming one.
    rows = journey("T1", "1", "BHBC", ["A1", "A2", "A3"])
    doc = bjt.build(rows, META)[("1", "BHBC")]
    assert doc["operator"] == "BHBC"
    assert doc["service"] == "1"


def test_a_document_never_offers_a_pair_no_bus_runs():
    # The reader-facing consequence. Merged, the stop list spanned both routes
    # and the picker offered A1 to B3 — a pair with no journey behind it, which
    # the view can only answer with "no bus we tracked made that trip".
    rows = journey("BH1", "1", "BHBC", ["A1", "A2", "A3"])
    rows += journey("SC1", "1", "SCSO", ["B1", "B2", "B3"])
    for doc in bjt.build(rows, META).values():
        atcos = {s["atco"] for s in doc["stops"]}
        assert not (atcos & {"A1", "A2", "A3"} and atcos & {"B1", "B2", "B3"}), \
            "one document offers stops from two different routes"


def test_a_service_run_by_one_operator_is_unaffected():
    rows = journey("T1", "700", "SCSO", ["A1", "A2", "A3"])
    docs = bjt.build(rows, META)
    assert set(docs) == {("700", "SCSO")}


# ── What the files are called ───────────────────────────────

def test_the_operator_is_always_in_the_name():
    # Even where only one operator runs the number. Naming it only on a clash
    # means the name changes the day a second operator appears — breaking every
    # link to it on precisely the day someone is looking.
    assert bjt.document_name("700", "SCSO") == "700-SCSO"
    assert bjt.document_name("1", "BHBC") == "1-BHBC"


def test_a_name_cannot_escape_its_directory():
    # These become URLs and file paths, and a service number arrives from GTFS,
    # which is not ours to trust.
    assert "/" not in bjt.document_name("../../etc/passwd", "X")
    assert ".." not in bjt.document_name("..", "..")
    assert bjt.document_name("", "") == "unknown-unknown"


def test_a_name_survives_punctuation_in_a_service_number():
    # Real numbers carry letters and the odd X; none of that may be lost, or
    # two services collide on one file.
    assert bjt.document_name("13X", "COMT") == "13X-COMT"
    assert bjt.document_name("N7", "BHBC") == "N7-BHBC"
    assert bjt.document_name("5B", "BHBC") != bjt.document_name("5", "BHBC")


# ── The index the browser reads ─────────────────────────────

def test_the_index_names_the_operator_and_the_file(tmp_path):
    rows = []
    for n in range(4):
        rows += journey(f"BH{n}", "1", "BHBC", ["A1", "A2", "A3"], "Portslade")
        rows += journey(f"SC{n}", "1", "SCSO", ["B1", "B2", "B3"], "Midhurst")
    obs = tmp_path / "obs.json"
    obs.write_text(json.dumps({"day": "2026-09-17", "data_version": "v1",
                               "observations": rows}), encoding="utf-8")
    out = tmp_path / "out"
    assert bjt.main(["--observations", str(obs), "--out", str(out)]) == 0

    index = json.loads((out / "index.json").read_text(encoding="utf-8"))
    entries = {(e["service"], e["operator"]): e for e in index["services"]}
    assert set(entries) == {("1", "BHBC"), ("1", "SCSO")}
    for key, entry in entries.items():
        assert (out / entry["file"]).exists(), \
            f"the index names {entry['file']}, which was never written"
        # The browser fetches this name rather than deriving one from the
        # service number, which only worked while a number meant one route.
        assert entry["file"] == f"{key[0]}-{key[1]}.json"


# ── A document names the days it is built from ──────────────
#
# The live 700 document claimed 16, 17 and 18 September 2026 and contained
# journeys from two of them. The 18th was measured against a timetable that
# begins on the 20th, so nothing was scheduled, nothing matched, and the day
# contributed no journeys at all — while still being named as evidence.


WINDOW = {"days": ["2026-09-16", "2026-09-17", "2026-09-18"],
          "data_versions": ["v1"]}


def test_days_lists_only_the_days_that_contributed():
    rows = [dict(r, day="2026-09-17") for r in
            journey("T1", "700", "SCSO", ["A1", "A2", "A3"])]
    doc = bjt.build(rows, WINDOW)[("700", "SCSO")]
    assert doc["days"] == ["2026-09-17"], \
        "a day with no journeys was named as evidence for this document"


def test_the_requested_window_is_still_published_beside_it():
    # Absence has to be visible, not merely absent: a reader should be able to
    # see that a day was asked for and produced nothing.
    rows = [dict(r, day="2026-09-17") for r in
            journey("T1", "700", "SCSO", ["A1", "A2", "A3"])]
    doc = bjt.build(rows, WINDOW)[("700", "SCSO")]
    assert doc["window_days"] == ["2026-09-16", "2026-09-17", "2026-09-18"]
    assert set(doc["days"]) < set(doc["window_days"]), \
        "the gap between what was asked for and what arrived is not visible"


def test_every_named_day_has_a_journey_behind_it():
    # The general form, which is what a reader relies on.
    rows = []
    for day in ("2026-09-16", "2026-09-17"):
        rows += [dict(r, day=day, trip_id=f"T{day}") for r in
                 journey("T", "700", "SCSO", ["A1", "A2", "A3"])]
    doc = bjt.build(rows, WINDOW)[("700", "SCSO")]
    assert doc["days"] == ["2026-09-16", "2026-09-17"]
    for day in doc["days"]:
        assert any(j["day"] == day for j in doc["journeys"]), \
            f"{day} is named but no journey comes from it"


def test_the_index_names_the_days_it_really_has(tmp_path):
    rows = []
    for n in range(3):
        rows += [dict(r, day="2026-09-17", trip_id=f"T{n}") for r in
                 journey(f"T{n}", "700", "SCSO", ["A1", "A2", "A3"])]
    obs = tmp_path / "obs.json"
    obs.write_text(json.dumps({"day": "2026-09-17", "data_version": "v1",
                               "observations": rows}), encoding="utf-8")
    out = tmp_path / "out"
    assert bjt.main(["--observations", str(obs), "--out", str(out)]) == 0
    index = json.loads((out / "index.json").read_text(encoding="utf-8"))
    assert index["days"] == ["2026-09-17"]
    assert index["services"][0]["days"] == ["2026-09-17"]


# ── Which town a destination is in ──────────────────────────
#
# Directions are named by place, not by stop, so five destinations on service 2
# become three places. Getting that wrong is not cosmetic: it puts a town on a
# label that the service never goes near.


class FakeTimetable:
    """Just the stop table, which is all Places reads."""

    def __init__(self, stops):
        self.stops = {atco: {"name": name, "locality": place}
                      for atco, name, place in stops}


SUSSEX = FakeTimetable([
    ("4400AD0185", "The Red Lion", "Shoreham-by-Sea"),
    ("4400ST0010", "Shooting Field", "Steyning"),
    ("1490BRI0001", "Old Steine", "Brighton"),
    ("1490MOU0001", "Birdham Road South End", "Moulsecoomb"),
    # The same name in two towns: it can settle nothing on its own.
    ("4400WO0001", "Station Road", "Worthing"),
    ("4400AD0002", "Station Road", "Lancing"),
])


def test_a_destination_beyond_the_recorded_area_still_names_its_town():
    # Shooting Field is in Steyning, which is outside the box, so no bus is
    # ever observed reaching it. The headsign naming one of our own stops is
    # the only way to know — and it is why the lookup exists at all.
    assert bjt.Places(SUSSEX).of("Shooting Field", "4400AD0185") == "Steyning"


def test_a_headsign_that_names_no_stop_falls_back_to_where_it_was_last_seen():
    assert bjt.Places(SUSSEX).of("Shoreham High Street",
                                 "4400AD0185") == "Shoreham-by-Sea"


def test_red_lion_is_shoreham_not_handcross():
    # The trap that shaped the rule. "Red Lion" is a unique stop in NaPTAN —
    # in Handcross, twenty miles away — while service 2's Red Lion journeys end
    # at "The Red Lion" in Shoreham. Searching every stop in Sussex by name
    # found the unique wrong answer, so the lookup is restricted to stops our
    # own timetable holds, where "Red Lion" matches nothing and the terminus
    # settles it.
    assert bjt.Places(SUSSEX).of("Red Lion", "4400AD0185") == "Shoreham-by-Sea"


def test_a_name_shared_by_two_towns_settles_nothing():
    # "Station Road" is in both Worthing and Lancing. Picking either would be a
    # coin toss presented as a fact; the terminus is evidence.
    places = bjt.Places(SUSSEX)
    assert "station road" not in places.by_name
    assert places.of("Station Road", "4400WO0001") == "Worthing"
    assert places.of("Station Road", "4400AD0002") == "Lancing"


def test_a_headsign_qualifier_does_not_prevent_a_match():
    # Real headsigns carry one: "George Street (stop J)".
    assert bjt.Places(SUSSEX).of("Old Steine (stop S3)", None) == "Brighton"


def test_with_no_localities_the_destination_text_is_used_unchanged():
    # NaPTAN was unreachable, or the timetable predates the column. Labels are
    # worse, nothing is wrong, and the build does not fail.
    assert bjt.Places().of("Shooting Field", "4400AD0185") == "Shooting Field"
    assert bjt.Places(FakeTimetable([])).of("Old Steine", None) == "Old Steine"


def test_the_published_journeys_carry_their_place():
    rows = journey("T1", "2", "BHBC", ["4400AD0185", "4400ST0010", "1490BRI0001"])
    doc = bjt.build(rows, META, places=bjt.Places(SUSSEX))[("2", "BHBC")]
    assert doc["journeys"][0]["place"] == "Brighton", \
        "a journey was published without the town it was heading for"
    localities = {s["atco"]: s["locality"] for s in doc["stops"]}
    assert localities["4400ST0010"] == "Steyning"


def test_a_destination_is_placed_by_where_most_of_it_ends_not_each_journey():
    # Journeys are often observed only part of the way — the bus stops
    # reporting, or the recording window closes — so an individual journey's
    # last call names wherever the evidence ran out rather than where it was
    # going.
    #
    # Resolved per journey, the 700 came out "Towards South Lancing / West
    # Worthing", because most of its runs were last seen short of Durrington.
    # Resolved per destination, from the stop most of them reach, it reads
    # "Towards Worthing / Durrington" as it should.
    tt = FakeTimetable([
        ("S0", "Brighton Old Steine", "Brighton"),
        ("S1", "Lancing Parade", "South Lancing"),
        ("S2", "Worthing Pier", "Worthing"),
        ("S3", "Montreal Way", "Durrington"),
    ])
    rows = []
    for n in range(3):                      # three full runs to Durrington
        rows += journey(f"FULL{n}", "700", "SCSO", ["S0", "S1", "S2", "S3"])
    # …and one cut short at Lancing, which must not rename the destination.
    rows += journey("SHORT", "700", "SCSO", ["S0", "S1", "S2"])
    doc = bjt.build(rows, META, places=bjt.Places(tt))[("700", "SCSO")]
    places = {j["place"] for j in doc["journeys"]}
    assert places == {"Durrington"}, \
        f"a part-observed journey renamed the destination: {places}"
