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
