"""Turning recorded snapshots into arrival observations.

These figures are the point of the whole recording exercise: a punctuality
number published against an operator has to survive that operator reading it.
Each test here pins a way the processing could be quietly wrong in a direction
that matters.

Two of them exist because running the processor over real recorded data, before
any test was written, showed it doing exactly this:

* **A stop the bus never reached was recorded as an arrival.** Recording
  stopped at 15:29, and a stop due at 15:31 was "observed" at 15:28, because
  the last snapshot was the nearest the bus ever got. That reads as a bus three
  minutes early — the opposite of the truth, in the flattering direction. An
  arrival now requires an interior nearest approach: the bus seen coming *and*
  going.
* **The operator was blank on all 127 observations.** The route row carries no
  NOC; `noc_for_route` does. A figure with no operator on it cannot be put to
  an operator.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import process_snapshots as ps                               # noqa: E402
from test_corridor_gaps import (                             # noqa: E402,F401
    WATCH, at, bus_at, monkeypatch_module, tt, west_xy,
)

ATCOS = [atco for atco, _ in WATCH]
DAY = at(12, 0).date()


def day_of(minutes, offset=0, service="700", operator="SCSO", destination="Worthing",
           toward_next=0.0, north_m=0.0, lag_min=0, repeat=1):
    """Snapshots of one westbound bus, `offset` minutes behind its timetable.

    W600 leaves stop 0 at 10:00 and calls at stop *i* at 10:00 + i minutes, so
    a bus `offset` late is at stop `t - 600 - offset` at minute `t`. `offset`
    may be a function of the minute, for a bus falling progressively behind.
    `north_m` shifts the whole track sideways, onto a parallel road.
    """
    out = []
    for minute in minutes:
        late = offset(minute) if callable(offset) else offset
        # `lag_min` is feed latency: the snapshot taken at `minute` carries the
        # position the bus held `lag_min` earlier, stamped with that earlier
        # time. `repeat` holds each report for several snapshots, as the real
        # feed does — 38% of its reports are repeats.
        reported_minute = minute - lag_min
        if repeat > 1:
            reported_minute -= (reported_minute - min(minutes)) % repeat
        idx = reported_minute - 600 - late
        vehicles = []
        if 0 <= idx < 60:
            bus = bus_at(idx, service=service, operator=operator,
                         destination=destination, toward_next=toward_next)
            bus["latitude"] += north_m / 111_000
            # Real snapshots carry one; the consolidation that keeps a bus on
            # one journey groups by it, so a fixture without one tests nothing.
            bus["vehicle_ref"] = "SCSO-1234"
            bus["recorded_secs"] = reported_minute * 60
            vehicles.append(bus)
        out.append((minute * 60, vehicles))
    return out


def observations(tt, samples):
    return ps.observe_day(tt, DAY, samples, atcos=ATCOS)


def for_stop(obs, index):
    """The observation for west stop `index`, or None."""
    atco = f"4400TW{index:04d}"
    return next((o for o in obs if o["atco"] == atco), None)


# ── A bus running to time ───────────────────────────────────

def test_a_bus_on_time_is_measured_as_on_time(tt):
    obs, _ = observations(tt, day_of(range(600, 640)))
    assert obs, "a bus tracked for forty minutes produced no observations"
    assert {o["trip_id"] for o in obs} == {"W600"}
    assert all(o["lateness_secs"] == 0 for o in obs), \
        sorted({o["lateness_secs"] for o in obs})


def test_a_bus_three_minutes_late_is_measured_as_three_minutes_late(tt):
    # Under half the ten-minute headway, so there is no other journey it could
    # plausibly be: the lateness is unambiguous and must be reported as it is.
    obs, _ = observations(tt, day_of(range(600, 646), offset=3))
    assert obs
    assert {o["trip_id"] for o in obs} == {"W600"}
    assert all(o["lateness_secs"] == 180 for o in obs), \
        sorted({o["lateness_secs"] for o in obs})
    one = for_stop(obs, 20)
    assert (one["scheduled"], one["observed"]) == ("10:20", "10:23")


def test_a_bus_falling_behind_is_one_journey_not_two(tt):
    # The drift case, and the reason one bus must keep one journey: this bus
    # starts three minutes late, where its own journey is the better fit, and
    # ends eight minutes late, where the journey ten minutes behind it fits
    # better as four minutes early. Matched snapshot by snapshot, one run is
    # reported as two half-journeys with opposite lateness — worse than either
    # answer alone, and an operator would be right to throw it out.
    def falling_behind(minute):
        return 3 + (minute - 600) // 6          # 3 late at 10:00, 8 by 10:36
    obs, _ = observations(tt, day_of(range(600, 646), offset=falling_behind))
    assert obs
    assert len({o["trip_id"] for o in obs}) == 1, \
        f'one run was split across journeys: {sorted({o["trip_id"] for o in obs})}'
    # Lateness should climb along the route — that is the finding, a bus losing
    # time as it goes — not jump about as the matcher changes its mind.
    along = [o["lateness_secs"] for o in sorted(obs, key=lambda o: o["scheduled_secs"])]
    assert along == sorted(along), f"lateness moved backwards along the route: {along}"
    assert along[-1] > along[0], "a bus that lost five minutes shows no loss"


def test_the_ambiguity_is_written_down_where_the_figures_are(tt):
    # The bias runs one way — a very late bus is recorded as the next journey
    # running early — so measured lateness is a floor. Publishing the number
    # without that sentence would overstate how good the service is.
    ambiguity = [c for c in ps.CAVEATS if "half a headway" in c]
    assert ambiguity, "the matching ambiguity is not stated in the caveats"
    assert "floor" in ambiguity[0] or "worse" in ambiguity[0], \
        "the caveat does not say which way the bias runs"


def test_every_observation_names_its_operator_and_service(tt):
    # Without these a figure cannot be put to anyone.
    obs, _ = observations(tt, day_of(range(600, 640)))
    assert {o["operator"] for o in obs} == {"SCSO"}
    assert {o["service"] for o in obs} == {"700"}
    assert all(o["day"] == DAY.isoformat() for o in obs)


# ── The stop the bus never reached ──────────────────────────

def test_a_stop_beyond_the_last_snapshot_is_not_an_arrival(tt):
    # Recording stops at 10:20 with the bus at stop 20. Stop 21, due at 10:21,
    # was never reached, and the nearest the bus came to it is the final
    # sample, 360 m away and closing. Publishing that would invent a bus
    # running early.
    obs, _ = observations(tt, day_of(range(600, 621)))
    assert for_stop(obs, 21) is None, "a stop the bus never reached was recorded"
    assert for_stop(obs, 22) is None
    assert for_stop(obs, 10) is not None, "a stop the bus did pass was dropped"


def test_a_stop_before_the_first_snapshot_is_not_an_arrival_either(tt):
    # The mirror case: recording starts with the bus already at stop 10, so
    # stop 9 is behind it and its nearest approach is the first sample.
    obs, _ = observations(tt, day_of(range(610, 640)))
    assert for_stop(obs, 9) is None, "a stop passed before recording was recorded"
    assert for_stop(obs, 20) is not None


# ── What is not counted ─────────────────────────────────────

def test_a_journey_with_no_bus_produces_no_lateness(tt):
    # The most important rule here: a journey nobody tracked is missing
    # coverage, never a late or an early bus.
    obs, coverage = observations(tt, day_of(range(600, 640), service="999"))
    assert obs == []
    assert coverage["tracked_journeys"] == 0
    assert coverage["scheduled_journeys"] > 0, "nothing was scheduled to measure against"


def test_coverage_counts_only_journeys_that_could_have_been_seen(tt):
    # Nine minutes of snapshots against a whole day of journeys would read as
    # 3% coverage and make every figure look unusable. The denominator is
    # journeys running while the recorder was.
    _obs, coverage = observations(tt, day_of(range(600, 640)))
    assert coverage["scheduled_journeys"] < coverage["scheduled_journeys_all_day"]
    assert coverage["tracked_journeys"] <= coverage["scheduled_journeys"]
    assert coverage["recorded_from"] == "10:00" and coverage["recorded_to"] == "10:39"


def test_a_coach_is_not_a_bus(tt):
    obs, _ = observations(tt, day_of(range(600, 640), service="025",
                                     operator="NATX", destination="London"))
    assert obs == [], "a coach produced punctuality observations"


def test_a_bus_seen_twice_is_not_evidence(tt):
    # Two sightings are not a tracked journey, and must not be counted as one
    # in the coverage figure every other number is judged against.
    obs, coverage = observations(tt, day_of(range(600, 602)))
    assert obs == []
    assert coverage["tracked_journeys"] == 0, "two sightings counted as a tracked journey"


def test_a_bus_on_the_next_road_over_does_not_arrive_anywhere(tt):
    # 250 m north of the route: close enough to be matched to the journey (the
    # matcher allows 400 m) but never near enough to any stop to have called
    # there. Without an arrival radius every stop would be "reached".
    obs, coverage = observations(tt, day_of(range(600, 640), north_m=250))
    assert coverage["tracked_journeys"] == 1, "the bus was not tracked at all"
    assert obs == [], f"a bus 250 m from every stop was recorded as calling at {len(obs)}"


def test_a_stop_reached_between_snapshots_is_not_guessed_at(tt):
    # Every sample sits 30% of the way past a stop, ~108 m on: inside the 150 m
    # arrival radius but not at the stop. For interior stops that is fine — the
    # bus was seen either side. For the last sample it is not: the nearest
    # approach may lie beyond the recording, which is how a stop due at 15:31
    # was once "observed" at 15:28 on real data.
    obs, _ = observations(tt, day_of(range(600, 621), toward_next=0.3))
    assert for_stop(obs, 20) is None, "the last stop was recorded from a passing position"
    assert for_stop(obs, 10) is not None, "an interior stop was dropped"


def test_a_bus_standing_at_the_last_stop_it_reached_does_count(tt):
    # The exception: at the stop itself, a boundary sample is an arrival, not a
    # guess. Otherwise every journey loses its first and last stop.
    obs, _ = observations(tt, day_of(range(600, 621)))
    assert for_stop(obs, 20) is not None, "a bus standing at a stop was not recorded there"


# ── Reading the recorded XML ────────────────────────────────

SNAPSHOT = """<?xml version="1.0"?>
<Siri xmlns="http://www.siri.org.uk/siri">
 <ServiceDelivery>
  <VehicleMonitoringDelivery>
   <VehicleActivity>
    <RecordedAtTime>2026-09-16T10:14:30+00:00</RecordedAtTime>
    <MonitoredVehicleJourney>
     <LineRef>700</LineRef><PublishedLineName>700</PublishedLineName>
     <OperatorRef>SCSO</OperatorRef><DestinationName>Worthing</DestinationName>
     <VehicleLocation><Longitude>{lon}</Longitude><Latitude>{lat}</Latitude></VehicleLocation>
     <Bearing>256.0</Bearing><VehicleRef>SCSO-1234</VehicleRef>
    </MonitoredVehicleJourney>
   </VehicleActivity>
   <VehicleActivity>
    <RecordedAtTime>2026-09-16T09:40:00+00:00</RecordedAtTime>
    <MonitoredVehicleJourney>
     <PublishedLineName>700</PublishedLineName><OperatorRef>SCSO</OperatorRef>
     <VehicleLocation><Longitude>-0.30</Longitude><Latitude>50.80</Latitude></VehicleLocation>
     <VehicleRef>SCSO-STALE</VehicleRef>
    </MonitoredVehicleJourney>
   </VehicleActivity>
  </VehicleMonitoringDelivery>
 </ServiceDelivery>
</Siri>"""


def test_a_snapshot_reads_back_as_vehicles():
    lat, lon = west_xy(15)
    # 10:15 UTC, which is 11:15 in London: the snapshot key is London time and
    # the feed publishes UTC, and comparing the two as clock readings is how a
    # 35-minute-old position first passed for current.
    at_utc = datetime(2026, 9, 16, 10, 15, tzinfo=timezone.utc)
    vehicles = ps.parse_snapshot(SNAPSHOT.format(lat=lat, lon=lon), at_utc=at_utc)
    assert [v["vehicle_ref"] for v in vehicles] == ["SCSO-1234"], \
        "a position the feed had been repeating for 35 minutes was taken as current"
    v = vehicles[0]
    assert (v["service_ref"], v["operator_ref"], v["bearing"]) == ("700", "SCSO", 256.0)
    assert (round(v["latitude"], 4), round(v["longitude"], 4)) == (round(lat, 4), round(lon, 4))


def test_a_truncated_snapshot_is_not_a_crash():
    # A snapshot can be cut short: the recorder streams whatever the feed sent.
    assert ps.parse_snapshot("<Siri><ServiceDelivery>") == []
    assert ps.parse_snapshot("") == []


def test_snapshots_are_read_in_time_order(tmp_path):
    for name in ("1015.xml", "0900.xml", "notes.txt", "2359.xml"):
        (tmp_path / name).write_text("<Siri/>")
    assert [secs for secs, _ in ps.snapshot_files(tmp_path)] == [
        9 * 3600, 10 * 3600 + 15 * 60, 23 * 3600 + 59 * 60]


# ── Counting arrivals into bands ────────────────────────────
#
# The definition is DfT's, not ours, so that an operator arguing with a figure
# has to argue with the department's yardstick. Its boundaries are exact and
# off-by-a-second errors here would move a published percentage.

def test_the_on_time_boundaries_are_the_official_ones():
    # BUS09: no more than 1 minute early, no more than 5 minutes 59 late.
    assert ps.band(-61) == "early"
    assert ps.band(-60) == "on_time", "a bus exactly a minute early is on time"
    assert ps.band(0) == "on_time"
    assert ps.band(359) == "on_time", "5 min 59 sec late is on time"
    assert ps.band(360) == "late", "6 minutes late is not on time"
    assert ps.band(900) == "late"
    assert ps.band(901) == "very_late"


def test_a_summary_carries_its_denominators_and_no_percentages(tt):
    obs, coverage = observations(tt, day_of(range(600, 640), offset=10))
    summary = ps.summarise(obs, coverage)
    assert summary["observations"] == len(obs)
    assert sum(summary["bands"].values()) == len(obs), "arrivals went missing between bands"
    assert summary["coverage"] == coverage, "a figure was published without its coverage"
    # A share computed here would travel without its denominator.
    flat = json.dumps(summary)
    assert "percent" not in flat and "%" not in flat
    assert "BUS09" in summary["on_time_definition"]


def test_a_summary_splits_by_service_and_hour(tt):
    # The whole point is naming a service and an hour: "the 700 at 17:00", not
    # "buses are late". Both breakdowns must add up to the same total.
    obs, coverage = observations(tt, day_of(range(600, 640)))
    summary = ps.summarise(obs, coverage)
    assert set(summary["by_service"]) == {"700"}
    assert sum(sum(v.values()) for v in summary["by_service"].values()) == len(obs)
    assert sum(sum(v.values()) for v in summary["by_hour"].values()) == len(obs)
    assert set(summary["by_hour"]) <= {"10", "11"}


# ── Whose clock times an arrival ────────────────────────────
#
# Measured over 2,297 reports in real recorded snapshots: the feed runs a
# median 186 seconds behind, three quarters of reports are over a minute stale,
# and 866 of them are the same position sent again. Timing arrivals by when we
# fetched them added about three minutes to every bus, against bands one and
# six minutes wide. This is the defect that a green suite did not catch.

def test_feed_latency_is_not_counted_as_lateness(tt):
    # A bus running exactly to time, whose positions reach us four minutes
    # late. Timed by the fetch it is four minutes late — six of those would
    # cross DfT's boundary and put an operator in the wrong column.
    obs, _ = observations(tt, day_of(range(600, 646), lag_min=4))
    assert obs, "a bus reporting four minutes behind produced no observations"
    assert all(o["lateness_secs"] == 0 for o in obs), \
        f'our latency was charged to the bus: {sorted({o["lateness_secs"] for o in obs})}'


def test_a_late_bus_reporting_late_is_still_late(tt):
    # The mirror: the correction must not swallow real lateness.
    obs, _ = observations(tt, day_of(range(600, 646), offset=3, lag_min=4))
    assert all(o["lateness_secs"] == 180 for o in obs), \
        sorted({o["lateness_secs"] for o in obs})


def test_a_repeated_position_is_one_sighting_not_four(tt):
    # Repeats would inflate the sample count, and enough of them can turn a
    # first or last report into an apparently interior one.
    obs, _ = observations(tt, day_of(range(600, 640), repeat=4))
    assert obs
    distinct = len(range(600, 640)) // 4
    assert max(o["samples"] for o in obs) <= distinct, \
        f'{max(o["samples"] for o in obs)} sightings from {distinct} reports'


def test_a_feed_without_timestamps_falls_back_to_the_fetch(tt):
    # Some operators omit RecordedAtTime. Refusing to measure them at all would
    # bias coverage towards the operators that report well.
    samples = [(secs, [{**v, "recorded_secs": None} for v in vs])
               for secs, vs in day_of(range(600, 640))]
    obs, _ = observations(tt, samples)
    assert obs, "a feed with no timestamps produced nothing at all"
    assert all(o["lateness_secs"] == 0 for o in obs)


def test_a_report_from_before_midnight_is_not_twelve_hours_out():
    # The recording window runs to 00:30, so this crosses in both directions.
    assert ps._same_day(23 * 3600 + 58 * 60, 10 * 60) == -120, \
        "a 23:58 report read at 00:10 was placed twelve hours away"
    assert ps._same_day(2 * 60, 23 * 3600 + 59 * 60) == 86_520
    assert ps._same_day(10 * 3600, 10 * 3600 + 60) == 10 * 3600
    assert ps._same_day(None, 600) is None


# ── A bus runs more than one journey a day ──────────────────

def test_a_bus_running_two_journeys_keeps_both(tt):
    # The first fix for drift kept only each bus's best-sampled journey, which
    # threw away the rest of its day: out, back, out again is one vehicle and
    # three journeys. W600 is scheduled 10:00–10:59 and W660 11:00–11:59, so
    # they cannot be confused with one drifting run.
    samples = []
    for minute in range(600, 720):
        idx = minute - 600 if minute < 660 else minute - 660
        bus = bus_at(idx, service="700", operator="SCSO", destination="Worthing")
        bus["vehicle_ref"] = "SCSO-1234"
        bus["recorded_secs"] = minute * 60
        samples.append((minute * 60, [bus]))
    obs, _ = observations(tt, samples)
    trips = {o["trip_id"] for o in obs}
    assert {"W600", "W660"} <= trips, f"a bus lost half its day: {sorted(trips)}"


# ── Provenance ──────────────────────────────────────────────

def test_the_figures_say_which_timetable_produced_them(tmp_path, tt):
    # Required by the evidence-provenance skill: the timetable is rebuilt
    # weekly, so a figure that cannot name its data cannot be reproduced.
    db = tmp_path / "timetable.sqlite"
    db.write_bytes(b"not a database")
    (tmp_path / "timetable.sqlite.sha256").write_text("a" * 64 + "  timetable.sqlite\n")
    assert ps._timetable_version(db) == "timetable.sqlite sha256:" + "a" * 16
    assert ps._timetable_version(tmp_path / "missing.sqlite") == "unknown"


def test_both_outputs_carry_their_provenance(tt, tmp_path):
    # The house rule (.claude/skills/evidence-provenance): a derived figure
    # ships with its method, its data version, when it was computed and what
    # would make it wrong. The review found all four missing from the payload.
    snaps = tmp_path / "raw"
    snaps.mkdir()
    lat, lon = west_xy(15)
    for minute in (615, 616, 617, 618):
        (snaps / f"{minute // 60:02d}{minute % 60:02d}.xml").write_text(
            SNAPSHOT.format(lat=lat, lon=lon))
    sidecar = Path(str(tt.db_path) + ".sha256")
    sidecar.write_text("b" * 64 + "  timetable.sqlite\n")
    obs_out, sum_out = tmp_path / "obs.json", tmp_path / "sum.json"
    rc = ps.main(["--day", DAY.isoformat(), "--snapshots", str(snaps),
                  "--timetable", str(tt.db_path),
                  "--out", str(obs_out), "--summary-out", str(sum_out)])
    assert rc == 0
    for path in (obs_out, sum_out):
        doc = json.loads(path.read_text())
        missing = [k for k in ("method", "caveats", "as_of", "data_version")
                   if not doc.get(k)]
        assert not missing, f"{path.name} publishes figures without {missing}"
        assert doc["data_version"].startswith("timetable.sqlite sha256:"), \
            f'{path.name} cannot say which timetable produced it: {doc["data_version"]}'
    sidecar.unlink()
