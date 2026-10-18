"""scripts/validate_hand_timings.py: checking our stop times against buses
timed by hand on timing.html.

The match has to find exactly the bus that was watched, score the error the
right way round, and say plainly why a logged bus could not be matched rather
than quietly dropping it. Most buses are logged by route alone and matched on
route, pole and time; that match must refuse to guess between two buses, or
it would choose whichever made our times look best.
"""

import csv
import gzip
import io
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
PLATES = {"SK67FKV": [["BHBC", "840"]], "WA11CHV": [["SCSO", "36256"]]}


def row(vehicle, atco, at, service="700", operator="SCSO", **over):
    return {"day": "2026-10-20", "vehicle": vehicle, "atco": atco, "observed_epoch": at,
            "observed_interval_epoch": [at, at + 60], "estimated": False, "quality_flags": [],
            "match": "declared", "service": service, "operator": operator,
            "stop_index": 5, "calls_total": 40, "timepoint": 1, **over}


def entry(vehicle="", atco="A", departed=T0, arrived=None, route="700"):
    return {"id": "x", "day": "2026-10-20", "atco": atco, "route": route, "vehicle": vehicle,
            "departed": departed, "arrived": arrived}


def test_a_registration_finds_the_bus_the_feed_recorded_by_its_fleet_number():
    # Compass's feed carries the plate itself; the others carry a fleet
    # number, which the plate is looked up to.
    assert vh.same_vehicle("YX12_FRC", "yx12 frc")
    assert vh.same_vehicle("840", "SK67 FKV", PLATES, "BHBC")
    assert vh.same_vehicle("SCSO-36256", "wa11chv", PLATES, "SCSO")
    assert not vh.same_vehicle("840", "SK67 FKV", PLATES, "METR"), "another operator's 840"
    assert not vh.same_vehicle("841", "SK67 FKV", PLATES, "BHBC")
    assert not vh.same_vehicle("840", "SK67 FKV"), "no fleet list, no match"
    # I and 1, O and 0 are misread both ways on a plate.
    assert vh.same_vehicle("SCSO-36256", "WAII CHV", PLATES, "SCSO")
    assert vh.same_vehicle("YX12_FRC", "YXI2 FRC")
    # A fleet number typed directly still works, but not two characters of one.
    assert vh.same_vehicle("SCSO-10950", "10950") and vh.same_vehicle("832", "832")
    assert not vh.same_vehicle("SCSO-10950", "95")
    assert not vh.same_vehicle("SCSO-10951", "10950")


def test_most_buses_match_on_route_pole_and_time():
    rows = [row("SCSO-10950", "A", T0 + 25), row("SCSO-10951", "A", T0 + 15 * 60),
            row("840", "A", T0 + 5, service="2", operator="BHBC")]
    [r] = vh.compare([entry(arrived=T0 - 40)], rows, STOPS)
    assert (r["matched"], r["error_secs"], r["dwell_secs"]) == ("route and time", 25, 40)
    assert r["inside_bounds"] is False, "the true departure was before our window opened"
    [r] = vh.compare([entry()], [row("SCSO-10950", "A", T0 - 20)], STOPS)
    assert (r["error_secs"], r["inside_bounds"]) == (-20, True)
    [r] = vh.compare([entry(route="n700")], [row("x", "A", T0, service="N700")], STOPS)
    assert r["matched"] == "route and time"
    [r] = vh.compare([entry(route="025")], [row("x", "A", T0, service="25")], STOPS)
    assert r["matched"] is None, "the 025 and the 25 are different routes (both stop at St Peter's Church)"


def test_two_buses_of_a_route_too_close_to_tell_apart_are_set_aside():
    # Choosing the nearer of two would choose the smaller error.
    rows = [row("SCSO-10950", "A", T0 + 30), row("SCSO-10951", "A", T0 - 70)]
    [r] = vh.compare([entry()], rows, STOPS)
    assert r["matched"] is None and r["reason"] == "two buses of that route too close to tell apart"
    # Clearly apart, the nearer one is the bus.
    rows = [row("SCSO-10950", "A", T0 + 30), row("SCSO-10951", "A", T0 - 150)]
    [r] = vh.compare([entry()], rows, STOPS)
    assert (r["matched"], r["error_secs"]) == ("route and time", 30)
    # A registration settles it.
    rows = [row("SCSO-36256", "A", T0 + 30), row("SCSO-10951", "A", T0 - 20)]
    [r] = vh.compare([entry("WA11 CHV")], rows, STOPS, PLATES)
    assert (r["matched"], r["error_secs"]) == ("registration", 30)


def test_route_and_time_never_reaches_across_the_road():
    # The opposite pole is usually the opposite direction: a bus of the same
    # route there is a different journey.
    [r] = vh.compare([entry()], [row("SCSO-10950", "A2", T0 + 10)], STOPS)
    assert r["matched"] is None and r["reason"] == "no bus of that route here; one on the other side of the road"
    # By registration it is unambiguous, so the other pole counts.
    [r] = vh.compare([entry("WA11CHV")], [row("SCSO-36256", "A2", T0 + 10)], STOPS, PLATES)
    assert r["matched"] == "registration"


def test_unmatched_buses_say_why():
    rows = [row("SCSO-36256", "A", T0 + 30 * 60), row("SCSO-26203", "A", T0 + 5 * 60),
            row("SCSO-26204", "B", T0)]
    out = vh.compare([entry("WA11CHV", route=""), entry(), entry(route="9"), entry(route="")],
                     rows, STOPS, PLATES)
    assert out[0]["reason"] == "bus recorded here, but not near this time"
    assert out[1]["reason"] == "no bus of that route recorded here within 3 minutes"
    assert out[2]["reason"] == "no bus of that route recorded here within 3 minutes"
    assert out[3]["reason"] == "no route or registration logged"
    summary = vh.summarise(out)
    assert summary["all"]["matched"] == 0 and sum(summary["unmatched"].values()) == 4


def test_a_registration_that_finds_nothing_falls_back_to_the_route():
    [r] = vh.compare([entry("AB12 CDE")], [row("SCSO-10950", "A", T0 + 40)], STOPS, PLATES)
    assert (r["matched"], r["error_secs"]) == ("route and time", 40)


def test_the_summary_keeps_the_two_ways_of_matching_apart():
    rows = [row("SCSO-36256", "A", T0 + 20), row("SCSO-10950", "A", T0 + 3600 + 40)]
    out = vh.compare([entry("WA11CHV"), entry(departed=T0 + 3600)], rows, STOPS, PLATES)
    s = vh.summarise(out)
    assert (s["all"]["matched"], s["registration"]["matched"], s["route_and_time"]["matched"]) == (2, 1, 1)
    assert s["by_operator"]["SCSO"]["matched"] == 2


def test_interpolated_rows_are_never_matched():
    [r] = vh.compare([entry("WA11CHV")], [row("SCSO-36256", "A", T0, estimated=True)], STOPS, PLATES)
    assert r["matched"] is None


def test_the_fleet_list_maps_current_and_previous_plates(tmp_path):
    NEXT = "https://bustimes.org/api/vehicles/?limit=500&offset=500&operator=BHBC"
    pages = {
        "https://bustimes.org/api/vehicles/?operator=BHBC&limit=500": {
            "results": [{"fleet_code": "840", "fleet_number": 840, "reg": "SK67FKV", "previous_reg": ""}],
            "next": NEXT},
        NEXT: {"results": [{"fleet_code": "", "fleet_number": 412, "reg": "BF63HDE",
                               "previous_reg": "YN08 MPY, Y26COM"},
                              {"fleet_code": "", "fleet_number": None, "reg": "AB12CDE"}], "next": None},
    }

    def opener(request, timeout=None):
        return io.BytesIO(json.dumps(pages[request.full_url]).encode())

    path = tmp_path / "fleet.json"
    plates = vh.fetch_fleet(path, ["BHBC"], opener=opener, pause=0)
    assert plates["SK67FKV"] == [["BHBC", "840"]]
    assert plates[vh.plate_key("YN08 MPY")] == plates[vh.plate_key("Y26COM")] == [["BHBC", "412"]]
    assert vh.plate_key("AB12CDE") not in plates, "a vehicle with no fleet number maps to nothing"
    assert json.loads(path.read_text())["plates"] == plates


def test_the_csv_from_timing_html_is_read_with_the_clock_offset_corrected(tmp_path):
    path = tmp_path / "timings-2026-10-20.csv"
    with open(path, "w", newline="") as h:
        w = csv.writer(h)
        w.writerow(["id", "day", "atco", "stop_name", "towards", "route", "vehicle",
                    "arrived_at", "departed_at", "clock_offset_ms", "note"])
        w.writerow(["a", "2026-10-20", "A", "High Street", "", "700", "",
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
    fleet = tmp_path / "fleet.json"
    fleet.write_text(json.dumps({"plates": {}}))
    path = tmp_path / "t.csv"
    path.write_text("id,day,atco,stop_name,towards,route,vehicle,arrived_at,departed_at,clock_offset_ms,note\n"
                    "a,2026-10-20,4400AD0204,High Street,,700,,,2026-10-20T09:00:30.000Z,0,\n")
    out = tmp_path / "r.json"
    assert vh.main(["--timings", str(path), "--observations", str(obs), "--fleet", str(fleet),
                    "--out", str(out)]) == 0
    report = json.loads(out.read_text())
    assert report["summary"]["all"]["matched"] == 1 and report["results"][0]["error_secs"] == 12
    assert "Typical error: **12 s**" in capsys.readouterr().out


def test_a_departure_we_could_not_bound_is_scored_but_not_bounds_checked():
    # The feed's last report at a stop with no report after it: "unbounded_departure".
    rows = [row("SCSO-10950", "A", T0 + 30, observed_interval_epoch=[T0 + 30, None],
                quality_flags=["unbounded_departure"])]
    [r] = vh.compare([entry()], rows, STOPS)
    assert (r["matched"], r["error_secs"], r["inside_bounds"], r["bounds_secs"]) == ("route and time", 30, None, None)
    assert r["flagged"] is True
    assert vh.summarise([r])["all"]["bounds_checked"] == 0


def call(trip_id, at, atco="A", service="700"):
    return {"day": "2026-10-20", "trip_id": trip_id, "atco": atco, "service": service, "scheduled_epoch": at}


def test_a_journey_we_never_recorded_could_be_the_bus_so_it_is_set_aside():
    # The bus timed was journey u, which we never saw; matching its neighbour
    # would score the neighbour's error as ours.
    rows = [row("SCSO-10950", "A", T0 + 100, trip_id="t1", scheduled_epoch=T0 + 40, lateness_secs=60)]
    calls = [call("t1", T0 + 40), call("u", T0 - 20)]
    [r] = vh.compare([entry()], rows, STOPS, schedule_calls=calls)
    assert r["matched"] is None and r["reason"] == "a journey we did not record could be this bus"
    # Due well away from the moment timed, it is no rival.
    [r] = vh.compare([entry()], rows, STOPS, schedule_calls=[call("t1", T0 + 40), call("u", T0 + 900)])
    assert (r["matched"], r["error_secs"]) == ("route and time", 100)
    # Seen elsewhere on its journey, its time here is estimated from those sightings.
    est = row("SCSO-10951", "A", T0 + 700, trip_id="u", estimated=True)
    [r] = vh.compare([entry()], rows + [est], STOPS, schedule_calls=calls)
    assert r["matched"] == "route and time"
    # Another route's missing journey is no rival.
    [r] = vh.compare([entry()], rows, STOPS, schedule_calls=[call("t1", T0 + 40), call("u", T0, service="2")])
    assert r["matched"] == "route and time"


def test_the_schedule_in_an_observation_file_gives_each_call_its_time():
    doc = {"schedule": {"day": "2026-10-20", "trip_format": ["trip_id", "route_pattern", "profile", "start_secs", "headsign"],
                        "patterns": {"p": {"service": "700", "atcos": ["Z", "A", "B"]}},
                        "profiles": [{"offsets": [0, 300, 600], "timepoints": [1, 0, 1]}],
                        "trips": [["t1", "p", 0, 9 * 3600, "Brighton"]]}}
    [c] = vh.scheduled_calls(doc, {"A"})
    # Noon minus twelve hours, London (BST on 20 October): 09:05 BST is 08:05 UTC.
    assert c == {"day": "2026-10-20", "trip_id": "t1", "atco": "A", "service": "700",
                 "scheduled_epoch": vh._epoch("2026-10-20T08:05:00Z")}


def test_a_journey_never_seen_could_be_anywhere_the_route_was_running():
    # The 700 at Shoreham in the evening peak runs 0 to 20 minutes late: a
    # journey we never saw, due 15 minutes before the moment timed, could be
    # the bus; one due half an hour after it could not.
    rows = [row("SCSO-10950", "A", T0 + 30, trip_id="t0", lateness_secs=600)] + [
        row(f"SCSO-1{n}", "A", T0 - 3600 + 600 * n, trip_id=f"t{n}", lateness_secs=late)
        for n, late in enumerate((0, 300, 600, 900, 1200), start=1)]
    [r] = vh.compare([entry()], rows, STOPS, schedule_calls=[call("u", T0 - 900)])
    assert r["reason"] == "a journey we did not record could be this bus"
    [r] = vh.compare([entry()], rows, STOPS, schedule_calls=[call("u", T0 + 1800)])
    assert (r["matched"], r["error_secs"]) == ("route and time", 30)
