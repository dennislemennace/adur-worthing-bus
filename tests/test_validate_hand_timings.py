"""scripts/validate_hand_timings.py: checking our stop times against buses
timed by hand on timing.html.

The match has to find exactly the bus that was watched (by the fleet number
on it), score the error the right way round, and say plainly why a logged bus
could not be matched rather than quietly dropping it.
"""

import csv
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import validate_hand_timings as vh                                  # noqa: E402

STOPS = {
    "A": {"atco_code": "A", "name": "High Street", "latitude": 50.832, "longitude": -0.275},
    "A2": {"atco_code": "A2", "name": "High Street", "latitude": 50.8321, "longitude": -0.2752},
    "B": {"atco_code": "B", "name": "Elsewhere", "latitude": 50.85, "longitude": -0.30},
}
T0 = 1_792_000_000   # an epoch on 20 October 2026


def row(vehicle, atco, at, service="700", operator="SCSO", **over):
    return {"day": "2026-10-20", "vehicle": vehicle, "atco": atco, "observed_epoch": at,
            "observed_interval_epoch": [at, at + 60], "estimated": False, "quality_flags": [],
            "match": "declared", "service": service, "operator": operator,
            "stop_index": 5, "calls_total": 40, "timepoint": 1, **over}


def entry(vehicle, atco="A", departed=T0, arrived=None, route="700"):
    return {"id": "x", "day": "2026-10-20", "atco": atco, "route": route, "vehicle": vehicle,
            "departed": departed, "arrived": arrived}


def test_the_fleet_number_on_the_bus_finds_the_bus_the_feed_recorded():
    assert vh.same_vehicle("SCSO-10950", "10950") and vh.same_vehicle("832", "832")
    assert vh.same_vehicle("YX12_FRC", "yx12 frc")
    assert not vh.same_vehicle("SCSO-10950", "950"[:2]), "two characters match half the fleet"
    assert not vh.same_vehicle("SCSO-10951", "10950")


def test_error_is_our_time_minus_the_hand_time_and_bounds_are_checked():
    rows = [row("SCSO-10950", "A", T0 + 25), row("SCSO-10951", "A", T0 + 5)]
    [r] = vh.compare([entry("10950", arrived=T0 - 40)], rows, STOPS)
    assert (r["matched"], r["error_secs"], r["dwell_secs"]) == ("fleet number", 25, 40)
    assert r["inside_bounds"] is False, "the true departure was before our window opened"
    [r] = vh.compare([entry("10950")], [row("SCSO-10950", "A", T0 - 20)], STOPS)
    assert (r["error_secs"], r["inside_bounds"]) == (-20, True)


def test_the_other_side_of_the_road_counts_but_another_stop_does_not():
    [r] = vh.compare([entry("10950")], [row("SCSO-10950", "A2", T0 + 10)], STOPS)
    assert r["matched"] == "fleet number"
    [r] = vh.compare([entry("10950")], [row("SCSO-10950", "B", T0 + 10)], STOPS)
    assert r["matched"] is None and r["reason"] == "bus not recorded at this stop"


def test_unmatched_buses_say_why_and_a_bus_without_a_number_is_a_weak_match():
    rows = [row("SCSO-10950", "A", T0 + 30 * 60), row("SCSO-26203", "A", T0 + 40)]
    out = vh.compare([entry("10950"), entry("", route="700"), entry("", route="9")], rows, STOPS)
    assert out[0]["reason"] == "bus recorded here, but not near this time"
    assert (out[1]["matched"], out[1]["error_secs"]) == ("route and time", 40)
    assert out[2]["reason"].startswith("no fleet number")
    summary = vh.summarise(out)
    assert summary["strong"]["matched"] == 0 and summary["weak"]["matched"] == 1
    assert sum(summary["unmatched"].values()) == 2


def test_interpolated_rows_are_never_matched():
    [r] = vh.compare([entry("10950")], [row("SCSO-10950", "A", T0, estimated=True)], STOPS)
    assert r["matched"] is None


def test_the_csv_from_timing_html_is_read_with_the_clock_offset_corrected(tmp_path):
    path = tmp_path / "timings-2026-10-20.csv"
    with open(path, "w", newline="") as h:
        w = csv.writer(h)
        w.writerow(["id", "day", "atco", "stop_name", "towards", "route", "vehicle",
                    "arrived_at", "departed_at", "clock_offset_ms", "note"])
        w.writerow(["a", "2026-10-20", "A", "High Street", "", "700", "10950",
                    "2026-10-20T09:00:00.000Z", "2026-10-20T09:00:30.000Z", "3000", ""])
    [e] = vh.read_timings([path])
    assert e["departed"] == vh._epoch("2026-10-20T09:00:27Z"), "a phone 3 s fast is corrected"
    assert e["arrived"] == vh._epoch("2026-10-20T08:59:57Z")


def test_the_command_reports_from_local_files(tmp_path, capsys):
    obs = tmp_path / "obs"
    obs.mkdir()
    dep = vh._epoch("2026-10-20T09:00:30Z")
    doc = {"observations": [row("SCSO-10950", "4400AD0204", dep + 12)]}
    (obs / "observations-2026-10-20.json.gz").write_bytes(gzip.compress(json.dumps(doc).encode()))
    path = tmp_path / "t.csv"
    path.write_text("id,day,atco,stop_name,towards,route,vehicle,arrived_at,departed_at,clock_offset_ms,note\n"
                    "a,2026-10-20,4400AD0204,High Street,,700,10950,,2026-10-20T09:00:30.000Z,0,\n")
    out = tmp_path / "r.json"
    assert vh.main(["--timings", str(path), "--observations", str(obs), "--out", str(out)]) == 0
    report = json.loads(out.read_text())
    assert report["summary"]["strong"]["matched"] == 1 and report["results"][0]["error_secs"] == 12
    assert "Typical error: **12 s**" in capsys.readouterr().out
