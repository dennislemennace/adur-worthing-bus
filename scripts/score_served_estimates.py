#!/usr/bin/env python3
"""What the stop board said, minute by minute, against what the buses did.

scripts/backtest_live_eta.py replays our estimate from the moment a bus left a
timing point, which is the best the estimate can do: a ceiling. A passenger
sees something worse, because the feed reaches us a median three minutes late
and the bus may be between stops. RTIG's guidance (RTIGT041-1.0, 4.2.4 and
7.4) is to measure what the system actually said at one moment against when
the bus then came. This does that.

For each recorded minute of a day it rebuilds the board from that minute's
recorded feeds with the API's own code: SIRI-VM parsed by
`main._parse_siri_vm`, each bus joined to the journey GTFS-RT names, placed on
it by `main._attach_declared_journeys`, and every stop ahead estimated by
`live_eta.project_trip`, as `main._apply_own_feed_estimates` does for a board.
Each estimate is judged against when that bus really left the stop (the day's
observations: declared, measured, no quality flags) by RTIG's window for how
far ahead it said the bus was (scripts/rtig_windows.py).

It also counts, as RTIGT041 5.2 suggests:

* **coverage**: of the timetabled calls inside the recorded box, how many had a
  live estimate at some minute in the 15 minutes before they were due, and,
  for the rest, why not (no bus seen on the journey; a bus seen but its
  journey not named by the feed; named, but no estimate for that stop);
* **journeys**: how many timetabled journeys had an estimate at any stop;
* **cleardown** (3.6.8): how long after a bus really left a stop the board
  would still have listed it, by the API's own rule (a row goes once its
  expected time, or its timetabled time where there is no estimate, has
  passed: `main._drop_departed`).

Counts only, as the other published files are: the site divides. Not modelled:
rows where the board showed TransportAPI's prediction instead of ours, the
board's row cap, and the live API's 15-second poll (the recorder samples once
a minute). Times are when buses left stops, not arrivals.

    python scripts/score_served_estimates.py day --day 2026-10-24 \\
        --timetable timetable.sqlite --snapshots snapshots/2026-10-24 \\
        --rt gtfs-rt/2026-10-24 --observations observations-2026-10-24.json.gz \\
        [--diversions diversion_adjustments.json] --out served-2026-10-24.json
    python scripts/score_served_estimates.py merge served-*.json --out served_accuracy.json
"""
import argparse
import gzip
import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import rtig_windows as rtig                                       # noqa: E402

LONDON = ZoneInfo("Europe/London")
METHOD_VERSION = 1
# RTIGT041 5.2: a departure that should have had a prediction within 15
# minutes of the bus coming.
COVERAGE_WINDOW_SECS = 15 * 60
# How long after a bus left to keep looking for a board that still lists it.
CLEARDOWN_HORIZON_SECS = 30 * 60
CLEARDOWN_STEPS = (60, 180, 300)

METHOD = ("Each recorded minute's feeds put through the API's own code to rebuild every "
          "estimate the stop board would have shown, each judged against when that bus "
          "really left the stop by RTIG's accuracy window for how far ahead it said the bus "
          "was (RTIGT041-1.0, section 6.9). Coverage: timetabled calls in the recorded area "
          "with a live estimate in the 15 minutes before they were due (RTIGT041 5.2).")
CAVEATS = [
    "Our own estimates only: where the board showed TransportAPI's prediction instead, "
    "that is not reproduced here.",
    "Times are when buses left stops; RTIG's windows are written for arrivals.",
    "The recorder samples once a minute; the live API looks every 15 seconds while "
    "someone is watching, so live estimates can be a little fresher than these.",
    "The board's row limit is not modelled: every stop ahead of every bus is scored.",
]


def minute_files(directory, suffix):
    """`{epoch minute: path}` for files named `HHMM-<epoch>.<suffix>`."""
    out = {}
    for path in Path(directory).glob(f"*.{suffix}"):
        stem = path.stem
        if "-" in stem and stem.split("-", 1)[1].isdigit():
            out[int(stem.split("-", 1)[1]) // 60] = path
    return out


def position(index):
    return "first stop" if index == 0 else "stops 2-3" if index <= 2 else "later stops"


class Day:
    """One day's board, rebuilt and scored."""

    def __init__(self, tt, observations, diversions_doc=None, events=(), coords=None, origin=0, day=None,
                 recovery_doc=None):
        from api import main, trip_match
        self.main, self.trip_match = main, trip_match
        self.tt = tt
        self.doc = diversions_doc
        self.recovery = recovery_doc
        # Journeys through a recorded disruption (data/analysis_exclusions.json)
        # are scored apart, as scripts/backtest_live_eta.py scores them: the
        # headline is the board on an ordinary day.
        self.events, self.coords, self.origin = list(events), coords or {}, origin
        self._scope = {}
        self.versus = defaultdict(Counter)
        self.day = day
        self.previous = []
        self.accuracy = defaultdict(Counter)
        self.outcomes = {}
        self.seen_trips = set()
        for row in observations:
            if row.get("trip_id"):
                self.seen_trips.add(row["trip_id"])
            if (row.get("match") == "declared" and not row.get("estimated")
                    and not row.get("quality_flags") and row.get("observed_epoch") is not None
                    and row.get("scheduled_secs") is not None):
                self.outcomes[(row["trip_id"], row["atco"], int(row["scheduled_secs"]))] = int(row["observed_epoch"])
        self.covered = set()          # calls with an estimate in their 15 minutes
        self.declared_trips = set()   # journeys a bus named at some minute
        self.estimated_trips = set()
        self.last_listed = {}         # call -> last minute listed after the bus left
        self._trip_info = {}

    def trip_info(self, trip_id):
        info = self._trip_info.get(trip_id)
        if info is None:
            trip = self.tt.trips.get(trip_id) or {}
            route_id = trip.get("route_id", "")
            calls = [(s, a) for s, a in (self.tt.trip_stops_for(trip_id) or []) if s is not None]
            info = (self.tt.noc_for_route(route_id) or "?",
                    (self.tt.routes.get(route_id) or {}).get("short_name", "") or "?", calls)
            self._trip_info[trip_id] = info
        return info

    def scope(self, trip_id):
        """"" for an ordinary journey, "disrupted|" for one a recorded event affects."""
        hit = self._scope.get(trip_id)
        if hit is None:
            import analysis_exclusions as ax
            op, svc, calls = self.trip_info(trip_id)
            hit = ""
            if self.events and calls:
                when = datetime.fromtimestamp(self.origin + calls[0][0], timezone.utc)
                atcos = [a for _s, a in calls]
                path = [self.coords[a] for a in atcos if a in self.coords]
                if ax.affecting(self.events, op, svc, when, atcos, path):
                    hit = "disrupted|"
            self._scope[trip_id] = hit
        return hit

    def board(self, xml_text, rt_bytes, now):
        """`[(trip_id, atco, scheduled secs, call index, expected epoch)]`: every
        estimate the board would have had at `now`."""
        from api import gtfs_rt, live_eta
        vehicles = self.main._parse_siri_vm(xml_text, now)
        if rt_bytes:
            try:
                _header, reports = gtfs_rt.parse_feed(rt_bytes)
            except Exception:                    # noqa: BLE001 — the API falls back the same way
                reports = []
            declared = {r["vehicle_id"]: r["trip_id"] for r in reports
                        if r.get("vehicle_id") and r.get("trip_id")}
            for v in vehicles:
                trip = declared.get(v.get("vehicle_ref", ""))
                if trip:
                    v["declared_trip_id"] = trip
        self.main._attach_declared_journeys(vehicles, self.tt, now_local=now.astimezone(LONDON))
        out = []
        running = {v["trip_id"] for v in vehicles if v.get("trip_source") in self.trip_match.DECLARED_SOURCES}
        self.previous = []
        for v in vehicles:
            if v.get("trip_source") not in self.trip_match.DECLARED_SOURCES:
                continue
            self.declared_trips.add(v["trip_id"])
            if v.get("lateness_secs") is None:
                continue
            _op, _svc, calls = self.trip_info(v["trip_id"])
            for r in live_eta.project_trip(self.tt, v["trip_id"], v, now, self.doc, self.recovery):
                if r["passed"] or not r["expected"] or r.get("not_served"):
                    continue
                expected = int(datetime.fromisoformat(r["expected"]).timestamp())
                out.append((v["trip_id"], r["stop_id"], calls[r["seq"]][0], r["seq"], expected))
            # The journey this bus works next, where the board has no bus on it
            # yet (live_eta.from_previous_journey): scored apart, below.
            if self.day is not None:
                nxt = live_eta.from_previous_journey(self.tt, v, now, self.day, self.doc, self.recovery)
                if nxt and nxt["trip_id"] not in running:
                    _o, _s, next_calls = self.trip_info(nxt["trip_id"])
                    for index, (secs, atco) in enumerate(next_calls):
                        self.previous.append((nxt["trip_id"], atco, secs, index,
                                              self.origin + secs + nxt["lateness_secs"]))
        return out

    def score_previous(self, now):
        """Estimates from a bus's previous journey, beside the timetable's own
        time for the same departures: the gate for showing them (batch 5)."""
        stamp = int(now.timestamp())
        for trip_id, atco, secs, index, expected in self.previous:
            actual = self.outcomes.get((trip_id, atco, secs))
            if actual is None or expected < stamp:
                continue
            op, _svc, _calls = self.trip_info(trip_id)
            scope = self.scope(trip_id)
            due = self.origin + secs
            for who, said in (("ours", expected), ("timetable", due)):
                ahead = said - stamp
                verdict = rtig.classify(ahead, said - actual) or "unscored"
                for group in ("all", f"operator|{op}", f"position|{position(index)}"):
                    self.versus[f"{scope}previous_journey|{who}|{group}"][verdict] += 1

    def score_minute(self, now, estimates, origin):
        stamp = int(now.timestamp())
        listed = {}
        for trip_id, atco, secs, index, expected in estimates:
            key = (trip_id, atco, secs)
            listed[key] = expected
            due = origin + secs
            if due - COVERAGE_WINDOW_SECS <= stamp <= due:
                self.covered.add(key)
            self.estimated_trips.add(trip_id)
            actual = self.outcomes.get(key)
            if actual is None:
                continue
            ahead, err = expected - stamp, expected - actual
            verdict = rtig.classify(ahead, err) or "unscored"
            op, svc, _calls = self.trip_info(trip_id)
            scope = self.scope(trip_id)
            for group in ("all", f"operator|{op}", f"service|{svc}|{op}",
                          f"range|{rtig.label(ahead)}", f"position|{position(index)}"):
                self.accuracy[scope + group][verdict] += 1
        return listed

    def compare(self, rows, stamp, listed):
        """TransportAPI's predictions fetched in this minute (api/prediction_log.py)
        against ours on the board at the same minute, each judged by RTIG's
        window against when that bus left. Only departures the scorer can time."""
        for r in rows:
            aimed = int(datetime.fromisoformat(r["aimed"]).timestamp())
            key = (r["trip_id"], r["stop"], aimed - self.origin)
            actual = self.outcomes.get(key)
            if actual is None:
                continue
            op, _svc, _calls = self.trip_info(r["trip_id"])
            scope = self.scope(r["trip_id"])
            if r.get("cancelled"):
                self.versus[scope + "cancelled|all"]["said_cancelled_but_ran"] += 1
                continue
            theirs = int(datetime.fromisoformat(r["expected"]).timestamp())
            ahead = theirs - r["fetched_at"]
            verdict = rtig.classify(ahead, theirs - actual) or "unscored"
            ours = listed.get(key)
            both = "both" if ours is not None else "nextbuses_only"
            for group in ("all", f"operator|{op}", f"range|{rtig.label(ahead)}"):
                self.versus[f"{scope}{both}|nextbuses|{group}"][verdict] += 1
                if ours is not None:
                    mine = rtig.classify(ours - stamp, ours - actual) or "unscored"
                    self.versus[f"{scope}both|ours|{group}"][mine] += 1

    def cleardown_minute(self, stamp, listed, origin, active):
        """For calls whose bus left in the last half hour: is the board still
        listing them at `stamp`? By its own rule, an estimate where there is
        one, the timetabled time where there is not."""
        for key, actual in active:
            expected = listed.get(key)
            due = origin + key[2]
            if (expected if expected is not None else due) >= stamp:
                self.last_listed[key] = stamp

    def report(self, calls_in_box, recorded_from, recorded_to, origin):
        coverage = defaultdict(Counter)
        journeys = defaultdict(lambda: {"journeys": set(), "with_estimate": set()})
        for trip_id, atco, secs in calls_in_box:
            due = origin + secs
            if not (recorded_from + COVERAGE_WINDOW_SECS <= due <= recorded_to):
                continue
            op, svc, _calls = self.trip_info(trip_id)
            key = (trip_id, atco, secs)
            if key in self.covered:
                why = "with_estimate"
            elif trip_id in self.declared_trips:
                why = "named_no_estimate"
            elif trip_id in self.seen_trips:
                why = "seen_not_named"
            else:
                why = "no_bus_seen"
            for group in ("all", f"operator|{op}", f"service|{svc}|{op}"):
                coverage[group]["calls"] += 1
                coverage[group][why] += 1
                journeys[group]["journeys"].add(trip_id)
                if trip_id in self.estimated_trips:
                    journeys[group]["with_estimate"].add(trip_id)
        cleardown = defaultdict(Counter)
        for key, actual in self.outcomes.items():
            due = origin + key[2]
            if not (recorded_from <= actual <= recorded_to - CLEARDOWN_HORIZON_SECS):
                continue
            after = max(0, self.last_listed.get(key, actual) - actual)
            op, _svc, _calls = self.trip_info(key[0])
            for group in ("all", f"operator|{op}"):
                cleardown[group]["calls"] += 1
                for step in CLEARDOWN_STEPS:
                    if after >= step:
                        cleardown[group][f"listed_{step // 60}_min_after"] += 1
                if actual < due:
                    cleardown[group]["left_before_timetable"] += 1
        return {
            "accuracy": {k: {str(v): n for v, n in c.items()} for k, c in sorted(self.accuracy.items())},
            "coverage": {k: dict(c) for k, c in sorted(coverage.items())},
            "journeys": {k: {"journeys": len(v["journeys"]), "with_estimate": len(v["with_estimate"])}
                         for k, v in sorted(journeys.items())},
            "cleardown": {k: dict(c) for k, c in sorted(cleardown.items())},
            "versus": {k: dict(c) for k, c in sorted(self.versus.items())},
        }


def prediction_rows(paths):
    """`{epoch minute: [row]}` from the Worker's prediction log files (JSON lines)."""
    by_minute = defaultdict(list)
    for path in paths:
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if isinstance(r, dict) and r.get("trip_id") and r.get("aimed") and isinstance(r.get("fetched_at"), int):
                by_minute[r["fetched_at"] // 60].append(r)
    return by_minute


def run_day(day, tt, siri_dir, rt_dir, observations, diversions_doc=None, box=None,
            events=(), coords=None, predictions=None, recovery_doc=None):
    from api import main
    siri = minute_files(siri_dir, "xml")
    rt = minute_files(rt_dir, "pb") if rt_dir else {}
    origin = int(main._service_day_start(day).timestamp())
    scorer = Day(tt, observations, diversions_doc, events, coords, origin, day, recovery_doc)
    predictions = predictions or {}
    by_actual = sorted((actual, key) for key, actual in scorer.outcomes.items())
    lo = 0
    minutes = sorted(siri)
    for minute in minutes:
        now = datetime.fromtimestamp(minute * 60, timezone.utc)
        stamp = minute * 60
        xml_text = siri[minute].read_text(encoding="utf-8", errors="replace")
        rt_path = rt.get(minute)
        estimates = scorer.board(xml_text, rt_path.read_bytes() if rt_path else None, now)
        listed = scorer.score_minute(now, estimates, origin)
        scorer.score_previous(now)
        if predictions.get(minute):
            scorer.compare(predictions[minute], stamp, listed)
        while lo < len(by_actual) and by_actual[lo][0] < stamp - CLEARDOWN_HORIZON_SECS:
            lo += 1
        active = []
        for actual, key in by_actual[lo:]:
            if actual >= stamp:
                break
            active.append((key, actual))
        scorer.cleardown_minute(stamp, listed, origin, active)
    box = box if box is not None else set(tt.stops)
    calls_in_box = []
    for trip_id, trip in tt.trips.items():
        if not tt.runs_on(trip.get("service_id", ""), day):
            continue
        for secs, atco in tt.trip_stops_for(trip_id) or []:
            if secs is not None and atco in box:
                calls_in_box.append((trip_id, atco, secs))
    recorded_from = minutes[0] * 60 if minutes else 0
    recorded_to = minutes[-1] * 60 if minutes else 0
    return {"method_version": METHOD_VERSION, "days": [day.isoformat()],
            "method": METHOD, "caveats": CAVEATS,
            "inputs": {"siri_minutes": len(siri), "gtfs_rt_minutes": len(rt)},
            **scorer.report(calls_in_box, recorded_from, recorded_to, origin)}


def merge(docs):
    """Counts summed over days; the site divides."""
    out = {"method_version": METHOD_VERSION, "days": [], "inputs": Counter(),
           "method": METHOD, "caveats": CAVEATS}
    sections = {"accuracy": defaultdict(Counter), "coverage": defaultdict(Counter),
                "journeys": defaultdict(Counter), "cleardown": defaultdict(Counter),
                "versus": defaultdict(Counter)}
    for doc in docs:
        if doc.get("method_version") != METHOD_VERSION:
            raise ValueError(f"method {doc.get('method_version')} is not {METHOD_VERSION}")
        out["days"] += doc["days"]
        out["inputs"].update(doc.get("inputs") or {})
        for name, groups in sections.items():
            for key, counts in (doc.get(name) or {}).items():
                groups[key].update(counts)
    out["days"] = sorted(set(out["days"]))
    out["inputs"] = dict(out["inputs"])
    for name, groups in sections.items():
        out[name] = {k: dict(c) for k, c in sorted(groups.items())}
    return out


def load_json(path):
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    one = sub.add_parser("day")
    one.add_argument("--day", required=True)
    one.add_argument("--timetable", required=True)
    one.add_argument("--snapshots", required=True, help="the day's SIRI-VM folder")
    one.add_argument("--rt", help="the day's GTFS-RT folder")
    one.add_argument("--observations", required=True)
    one.add_argument("--diversions", help="diversion_adjustments.json in force that day")
    one.add_argument("--recovery", help="running-recovery.json in force that day (api/recovery.py)")
    one.add_argument("--prediction-log", nargs="*", default=[],
                     help="the Worker's prediction log files for the day (api/prediction_log.py)")
    one.add_argument("--out", required=True)
    many = sub.add_parser("merge")
    many.add_argument("files", nargs="+")
    many.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    if args.cmd == "merge":
        doc = merge([load_json(p) for p in args.files])
    else:
        import analysis_exclusions as ax
        from api.timetable_db import Timetable
        from process_snapshots import stops_in_box
        tt = Timetable(Path(args.timetable), allow_fetch=False)
        obs = load_json(args.observations)
        diversions_doc = load_json(args.diversions) if args.diversions else None
        doc = run_day(date.fromisoformat(args.day), tt, args.snapshots, args.rt,
                      obs.get("observations") or [], diversions_doc, set(stops_in_box(tt)),
                      ax.load(), ax.stop_coords(), prediction_rows(args.prediction_log),
                      load_json(args.recovery) if args.recovery else None)
        doc["timetable_data_version"] = obs.get("data_version")
    doc["as_of"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    Path(args.out).write_text(json.dumps(doc, indent=1, sort_keys=True) + "\n")
    print(json.dumps({k: doc[k] for k in ("days", "inputs")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
