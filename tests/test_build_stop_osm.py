"""What OpenStreetMap records about our stops, joined by NaPTAN code.

The stop panel turns these values into words a wheelchair user relies on, so
the build keeps only values whose meaning is settled and refuses to publish a
file that has lost most of the area.
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import build_stop_osm as bso                                      # noqa: E402


def overpass(*elements):
    return {"osm3s": {"timestamp_osm_base": "2026-10-13T19:33:34Z"},
            "elements": [{"type": "node", "id": i + 1, "tags": tags} for i, tags in enumerate(elements)]}


def test_only_settled_values_are_kept():
    got = bso.parse_overpass(overpass({
        "naptan:AtcoCode": "A", "shelter": "yes", "bench": "No", "wheelchair": "maybe",
        "kerb": "raised", "tactile_paving": "yes;no", "lit": "yes", "name": "High St",
        "check_date": "2025-03-04"}))
    assert got == {"A": {"id": 1, "shelter": "yes", "bench": "no", "kerb": "raised",
                         "lit": "yes", "check_date": "2025-03-04"}}


def test_a_stop_without_a_naptan_code_or_a_good_date_is_not_guessed():
    got = bso.parse_overpass(overpass({"shelter": "yes"},
                                      {"naptan:AtcoCode": "B", "check_date": "last spring"}))
    assert got == {"B": {"id": 2}}


def test_where_two_objects_claim_a_stop_the_better_mapped_one_wins():
    got = bso.parse_overpass(overpass({"naptan:AtcoCode": "C", "shelter": "yes"},
                                      {"naptan:AtcoCode": "C", "shelter": "no", "bench": "yes",
                                       "wheelchair": "yes"}))
    assert got["C"] == {"id": 2, "shelter": "no", "bench": "yes", "wheelchair": "yes"}


def test_the_file_names_its_source_and_licence_and_only_our_stops():
    stops = [{"atco_code": "A"}, {"atco_code": "Z"}]
    out = bso.build(stops, {"A": {"id": 1}, "ELSEWHERE": {"id": 9}}, "2026-10-13T19:33:34Z")
    assert out["stops"] == {"A": {"id": 1}} and out["count"] == 1
    assert out["licence"] == "ODbL-1.0" and "OpenStreetMap contributors" in out["source"]
    assert out["osm_base"] == "2026-10-13T19:33:34Z"


def test_a_response_missing_most_of_the_area_is_refused(tmp_path, monkeypatch):
    stops = tmp_path / "stops.json"
    stops.write_text(json.dumps({"stops": [{"atco_code": f"S{i}"} for i in range(10)]}))
    saved = tmp_path / "overpass.json"
    saved.write_text(json.dumps(overpass(*[{"naptan:AtcoCode": f"S{i}"} for i in range(6)])))
    out = tmp_path / "stop_osm.json"
    out.write_text("previous")
    monkeypatch.setattr(sys, "argv", ["build_stop_osm.py", "--stops", str(stops),
                                      "--overpass-json", str(saved), "--out", str(out)])
    assert bso.main() == 1
    assert out.read_text() == "previous", "last week's file must survive a bad answer"
    saved.write_text(json.dumps(overpass(*[{"naptan:AtcoCode": f"S{i}"} for i in range(8)])))
    assert bso.main() == 0
    assert json.loads(out.read_text())["count"] == 8
