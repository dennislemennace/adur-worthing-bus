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


def test_a_stop_mapped_twice_keeps_what_each_object_says():
    # The pole says it is lit; the platform way carries the shelter and seat.
    got = bso.parse_overpass({"elements": [
        {"type": "node", "id": 1, "lat": 50.8, "lon": -0.3, "tags": {"naptan:AtcoCode": "D", "lit": "yes"}},
        {"type": "way", "id": 2, "center": {"lat": 50.8, "lon": -0.3},
         "tags": {"naptan:AtcoCode": "D", "shelter": "yes", "bench": "yes", "covered": "yes"}}]})
    assert got["D"] == {"id": 2, "way": True, "lit": "yes", "shelter": "yes", "bench": "yes", "covered": "yes"}


def _stop_and_kerbs(*kerbs):
    stop = {"type": "node", "id": 1, "lat": 50.8, "lon": -0.3, "tags": {"naptan:AtcoCode": "E"}}
    return {"elements": [stop] + [{"type": "node", "id": 10 + i, "lat": 50.8 + dy, "lon": -0.3,
                                   "tags": {"barrier": "kerb", **tags}} for i, (dy, tags) in enumerate(kerbs)]}


def test_a_kerb_mapped_beside_the_stop_is_its_kerb():
    # 0.00007 degrees of latitude is about 8 m; 0.0003 about 33 m.
    got = bso.parse_overpass(_stop_and_kerbs((0.00007, {"kerb": "raised", "kerb:height": "0.16"}),
                                             (0.0003, {"kerb": "lowered"})))
    assert got["E"] == {"id": 1, "kerb": "raised", "kerb_nearby": True, "kerb_height_cm": 16}
    # Two kerbs beside it that disagree say nothing.
    got = bso.parse_overpass(_stop_and_kerbs((0.00007, {"kerb": "raised"}), (-0.00007, {"kerb": "lowered"})))
    assert "kerb" not in got["E"]


def test_kerb_heights_are_read_in_their_own_units():
    assert [bso.kerb_height_cm(v) for v in ("0.16", "0.16 m", "16 cm", "160 mm", "12cm")] == [16, 16, 16, 16, 12]
    assert [bso.kerb_height_cm(v) for v in ("high", "", None, "2", "75 cm")] == [None] * 5
