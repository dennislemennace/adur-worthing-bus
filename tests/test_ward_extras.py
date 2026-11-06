"""Areas, phase B: time lost on the road by ward, and who lives there.

ward-delays.json is built nightly from the delay map; data/ward_census.json
once from Census 2021. Each counts only what its source can stand behind: a
stretch the delay map colours, an output area summed whole into one ward.

Run with:  pytest
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import build_ward_census as bwc  # noqa: E402
import build_ward_delays as bwd  # noqa: E402

WARD = {"code": "E05000001", "rings": [[[50.80, -0.30], [50.80, -0.29], [50.81, -0.29], [50.81, -0.30], [50.80, -0.30]]]}


def stretch(sid, lat, lon):
    return {"id": sid, "from_name": f"From {sid}", "to_name": f"To {sid}", "direction": "eastbound",
            "geometry": [[lat, lon - 0.001], [lat, lon], [lat, lon + 0.001]]}


def cell(sid, secs, journeys=40, days=6, period="10-16", latest=True):
    return {"stretch": sid, "day_type": "weekday", "resolution": "period", "period": period,
            "journeys": journeys, "distinct_days": days, "median_gained_secs": secs, "latest": latest}


def test_a_ward_counts_only_the_stretches_the_delay_map_colours():
    doc = {"service": "700", "operator": "SCSO", "floor": {"journeys": 30, "distinct_days": 5},
           "days_collected": ["2026-10-01", "2026-10-30"],
           "stretches": [stretch("in1", 50.805, -0.295), stretch("in2", 50.806, -0.294),
                         stretch("thin", 50.807, -0.293), stretch("old", 50.808, -0.292),
                         stretch("out", 50.70, -0.50)],
           "cells": [cell("in1", 120), cell("in2", 300), cell("thin", 900, journeys=10),
                     cell("old", 999, latest=False), cell("out", 50)]}
    out = bwd.build([doc], {"wards": [WARD]}, as_of="2026-11-06T03:00:00+00:00")
    day = out["wards"]["E05000001"]["weekday:10-16"]
    assert day["stretches"] == 2, "a stretch under the floor, or off the latest timetable, was counted"
    assert day["median_lost_secs"] == 210
    assert day["worst"]["from"] == "From in2" and day["worst"]["lost_secs"] == 300
    assert out["wards"]["E05000001"]["weekday:16-18"] == {"stretches": 0}, "no figure is too few, not zero"
    assert out["window"] == {"from": "2026-10-01", "to": "2026-10-30", "days": 2}


def test_census_output_areas_are_summed_whole_into_their_wards():
    lookup = {"OA1": "W1", "OA2": "W1", "OA3": "W2"}
    cars = ('"GEOGRAPHY_CODE","C2021_CARS_5_NAME","OBS_VALUE"\n'
            '"OA1","Total: All households",100\n"OA1","No cars or vans in household",20\n'
            '"OA1","1 car or van in household",80\n'
            '"OA2","Total: All households",50\n"OA2","No cars or vans in household",25\n'
            '"OA3","Total: All households",10\n"OA3","No cars or vans in household",1\n'
            '"OA9","Total: All households",999\n')
    age = ('"GEOGRAPHY_CODE","C2021_AGE_19_NAME","OBS_VALUE"\n'
           '"OA1","Total",300\n"OA1","Aged 65 to 69 years",10\n"OA1","Aged 85 years and over",5\n'
           '"OA1","Aged 60 to 64 years",40\n"OA2","Total",100\n"OA3","Total",20\n')
    out = bwc.build(lookup, cars, age, "2026-11-06")
    assert out["wards"]["W1"] == {"households": 150, "households_no_car": 45, "residents": 400, "residents_65_plus": 15}
    assert out["wards"]["W2"]["households"] == 10
    assert "OA9" not in json.dumps(out) and len(out["wards"]) == 2


def test_the_census_file_covers_every_ward_shown_and_adds_up():
    census = json.loads((ROOT / "data" / "ward_census.json").read_text(encoding="utf-8"))
    wards = json.loads((ROOT / "data" / "localities.json").read_text(encoding="utf-8"))["wards"]
    assert all(w["code"] in census["wards"] for w in wards), "a ward on the map has no census figures"
    total = sum(w["residents"] for w in census["wards"].values())
    # Adur, Worthing and Brighton & Hove at Census 2021: about 453,000 people.
    # The first build lost 51,000 to a page Nomis did not send.
    assert 440_000 < total < 470_000, total
    for code, w in census["wards"].items():
        assert 0 <= w["households_no_car"] <= w["households"] and 0 <= w["residents_65_plus"] <= w["residents"], code
    assert "Open Government Licence" in census["licence"] and census["census_day"] == "2021-03-21"


def test_the_nightly_build_publishes_the_ward_delays():
    wf = (ROOT / ".github" / "workflows" / "process-snapshots.yml").read_text(encoding="utf-8")
    assert "scripts/build_ward_delays.py --map-dir hotspot-map" in wf
    assert '--artifact "ward-delays.json=ward-delays.json"' in wf
