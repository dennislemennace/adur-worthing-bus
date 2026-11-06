"""The locality browser's wards and their bus service (data/localities.json).

Each ward's figures are scheduled departures on the evidence's dated week,
counted the way scripts/build_evidence.py counts them, on a 04:00-to-04:00
day. Asked for on 6 November 2026.

Run with:  pytest
"""

import json
import re
import sqlite3
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import build_localities as bl  # noqa: E402

WEEK = {"monday": date(2026, 11, 2), "saturday": date(2026, 11, 7), "sunday": date(2026, 11, 8)}


def square(lon, lat, size=0.01):
    return [[[lon, lat], [lon + size, lat], [lon + size, lat + size], [lon, lat + size], [lon, lat]]]


def ward(code, name, district, lon, lat):
    return {"type": "Feature", "properties": {"WD25CD": code, "WD25NM": name, "LAD25CD": "x", "LAD25NM": district},
            "geometry": {"type": "Polygon", "coordinates": square(lon, lat)}}


def timetable(tmp_path):
    con = sqlite3.connect(tmp_path / "tt.sqlite")
    con.executescript("""
      CREATE TABLE stops (sid INTEGER, stop_id TEXT, name TEXT, lat REAL, lon REAL, locality TEXT, locality_parent TEXT);
      CREATE TABLE routes (rid INTEGER, route_id TEXT, short_name TEXT, long_name TEXT, noc TEXT);
      CREATE TABLE trips (tid INTEGER, trip_id TEXT, rid INTEGER, service_id TEXT, headsign TEXT, shape_id TEXT, block_id TEXT);
      CREATE TABLE stop_times (tid INTEGER, seq INTEGER, sid INTEGER, dep_secs INTEGER, timepoint INTEGER);
      CREATE TABLE calendar (service_id TEXT, monday TEXT, tuesday TEXT, wednesday TEXT, thursday TEXT,
                             friday TEXT, saturday TEXT, sunday TEXT, start_date TEXT, end_date TEXT);
      CREATE TABLE calendar_dates (service_id TEXT, date TEXT, exception TEXT);
    """)
    con.executemany("INSERT INTO stops VALUES (?,?,?,?,?,'','')", [
        (1, "A1", "a1", 50.805, -0.295), (2, "A2", "a2", 50.806, -0.294),
        (3, "B1", "b1", 50.805, -0.195), (4, "OUT", "out", 50.70, -0.5)])
    con.execute("INSERT INTO routes VALUES (1, 'r1', '700', '', 'SCSO')")
    con.executemany("INSERT INTO trips VALUES (?,?,1,'WK','x','','')", [(1, "t1"), (2, "t2"), (3, "t3")])
    con.executemany("INSERT INTO stop_times VALUES (?,?,?,?,1)", [
        (1, 1, 1, 8 * 3600), (1, 2, 2, 8 * 3600 + 300), (1, 3, 3, 8 * 3600 + 1800),
        (2, 1, 1, 24 * 3600 + 1800),          # 00:30 tonight, written as 24:30
        (3, 1, 1, 3 * 3600 + 55 * 60)])       # 03:55 on the service day: the night before
    con.execute("INSERT INTO calendar VALUES ('WK','1','1','1','1','1','0','0','20260101','20271231')")
    con.commit()
    return con


def test_a_ward_counts_its_buses_on_a_day_that_runs_from_four_to_four(tmp_path):
    geo = {"features": [ward("E05000001", "Alpha", "Adur", -0.30, 50.80),
                        ward("E05000002", "Beta", "Brighton and Hove", -0.20, 50.80),
                        ward("E05000003", "Far East", "Brighton and Hove", -0.06, 50.80),
                        ward("E05000004", "Elsewhere", "Arun", -0.31, 50.80)]}
    osm = {"A1": {"shelter": "yes", "wheelchair": "yes"}, "A2": {"wheelchair": "no"}}
    reps = {"generated_on": "2026-09-07", "areas": {"E05000001": {
        "council": "Adur District Council", "members": [{"name": "A Person", "party": "Ind", "url": "https://x",
                                                          "email": "never@copied.example"}]}}}
    doc = bl.build(geo, timetable(tmp_path), WEEK, osm, reps, "2026-11-06")
    names = [w["name"] for w in doc["wards"]]
    assert names == ["Alpha", "Beta"], "a ward centred outside the area, or outside the three districts, was kept"
    a = doc["wards"][0]
    mon = a["days"]["monday"]
    assert a["stops"] == 2 and mon["departures"] == 4 and mon["departures_per_stop"] == 2.0
    assert (mon["first"], mon["last"]) == ("08:00", "03:55"), "the night's buses moved the day's first and last"
    assert mon["night"] == 2 and mon["departures_after_2300"] == 1
    assert a["days"]["saturday"]["departures"] == 0 and a["days"]["saturday"]["first"] is None
    assert a["access"] == {"stops_in_osm": 2, "shelter_yes": 1, "step_free_recorded": 2, "step_free_yes": 1}
    assert a["councillors"]["members"] == [{"name": "A Person", "party": "Ind", "url": "https://x"}]
    assert "email" not in json.dumps(doc), "councillors' addresses are not repeated here"
    assert a["rings"][0][0] == [50.8, -0.3], "rings are [lat, lon], like the site's other shapes"
    assert doc["measured_week"]["monday"] == "2026-11-02"


def test_the_published_file_matches_the_evidence_week_and_names_its_source():
    path = ROOT / "data" / "localities.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    evidence = json.loads((ROOT / "data" / "boundary_evidence.json").read_text(encoding="utf-8"))
    assert doc["measured_week"] == {d: evidence["measured_week"][d] for d in ("monday", "saturday", "sunday")}, \
        "a ward's figure and the boundary comparison describe different weeks"
    assert "Open Government Licence" in doc["source"]["licence"] and doc["caveats"]
    assert doc["count"] == len(doc["wards"]) >= 40
    for w in doc["wards"]:
        assert re.fullmatch(r"E05\d{6}", w["code"]), w["name"]
        assert w["rings"] and all(len(r) >= 4 for r in w["rings"]), w["name"]
        for day in ("monday", "saturday", "sunday"):
            d = w["days"][day]
            assert d["first"] is None or re.fullmatch(r"\d\d:\d\d", d["first"]), (w["name"], day)
    assert "\u2014" not in path.read_text(encoding="utf-8")


def test_the_weekly_build_rebuilds_the_wards():
    wf = (ROOT / ".github" / "workflows" / "update-timetable.yml").read_text(encoding="utf-8")
    assert "scripts/build_localities.py" in wf
    commit = wf[wf.index("Commit refreshed statistics"):]
    assert "data/localities.json" in commit.split("git commit")[0]


def test_a_week_the_ons_does_not_answer_keeps_last_weeks_boundaries(tmp_path):
    con = timetable(tmp_path)
    first = bl.build({"features": [ward("E05000001", "Alpha", "Adur", -0.30, 50.80)]},
                     con, WEEK, {}, {}, "2026-11-06")
    saved = tmp_path / "localities.json"
    saved.write_text(json.dumps(first))
    again = bl.build(bl.previous_wards(saved), con, WEEK, {}, {}, "2026-11-13")
    assert [w["rings"] for w in again["wards"]] == [w["rings"] for w in first["wards"]]
    assert again["wards"][0]["days"] == first["wards"][0]["days"]
