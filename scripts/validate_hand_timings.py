#!/usr/bin/env python3
"""How close are our recorded stop times to times seen by hand?

Every stop time on the site is inferred: the last location report within
150 m of the stop, bounded by the next report about a minute later. Nothing
had checked one against a bus someone watched leave. This takes the departures
logged by hand on timing.html (the route, optionally the registration, and the
moment the doors opened and the bus pulled away) and matches each to the
observation row for the same bus at the same stop on the same day.

**Error is our recorded time minus the hand-timed departure, in seconds.**
Positive means we record the bus as leaving later than it did. Reported:

- the share of hand-timed buses we recorded at all, and why the rest were not;
- the median, 90th-percentile and signed bias of the error;
- how often the true departure fell inside our recorded bounds (the last
  report in the stop area and the next report after it);
- how long buses stood with their doors open;
- all of it by kind of stop (timing point, end of route, other) and operator.

Most buses are logged by route alone. They are matched on **route, pole and
time**: the bus of that route we recorded leaving that same pole nearest the
hand-timed moment, within three minutes. The other side of the road never
counts (it is usually the other direction), and when a second bus of the route
was within 90 seconds of being as close, the bus is set aside rather than
guessed: picking the nearer of two would pick the smaller error. The same goes
for a journey the timetable ran past that pole which we never recorded there:
if it could have been the bus timed (its time there estimated from our other
sightings of it, or its scheduled time plus how late that route was running),
the bus is set aside. Otherwise a bus we missed would be scored as its
neighbour's error.

A registration, where one was typed, settles which bus it was. Compass's feed
carries the plate itself; Brighton & Hove, Metrobus and Stagecoach carry fleet
numbers, so plates are looked up in bustimes.org's community-maintained fleet
list (fetched once and cached as fleet.json). Both kinds of match are reported
apart as well as together, so the route-and-time method can be checked against
the registration-matched buses.

    python scripts/validate_hand_timings.py --timings timings-2026-10-20.csv \\
        --observations obs/ --out hand-check.json
    python scripts/validate_hand_timings.py --timings 'timings-*.csv' --fetch obs-cache/
"""

import argparse
import csv
import glob
import gzip
import json
import re
import statistics
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
PUBLISHED = "https://adur-worthing-submissions.dennislemennace.workers.dev/journey-times"

# A bus's recorded departure this far from the hand-timed one is not the same
# departure: the next pass of a looping route, or the bus logged at the wrong
# pole.
STRONG_WINDOW_SECS = 15 * 60
ROUTE_WINDOW_SECS = 3 * 60
# A second bus of the route this nearly as close makes the choice a guess.
AMBIGUITY_MARGIN_SECS = 90
# A journey we did not record is a rival this nearly as close: wider, because
# its time at the stop is an estimate.
RIVAL_MARGIN_SECS = 120
# How late the route ran at the pole around the moment timed, from this many
# of our own departures within this window, bounds where a journey we never
# saw could have been.
LATENESS_WINDOW_SECS = 90 * 60
MIN_LATENESS_SAMPLE = 5
LONDON = ZoneInfo("Europe/London")

FLEET_API = "https://bustimes.org/api/vehicles/"
FLEET_OPERATORS = ("BHBC", "METR", "SCSO", "COMT")
USER_AGENT = "worthingbrightonbus.co.uk hand-timing check (privacy@worthingbrightonbus.co.uk)"
# Operator codes the feed and the fleet list may spell differently.
FAMILIES = {"SCSC": "SCSO", "CMPA": "COMT"}
# Poles of one stop name within this distance are the same place for a
# passenger, and a person can log the wrong side of the road.
SIBLING_METRES = 120


def vehicle_key(text):
    """As timing.js's timingVehicleKey: "SCSO-10950" -> "SCSO10950"."""
    return "".join(ch for ch in str(text or "").upper() if ch.isalnum())


def plate_key(text):
    """A plate as a key: I and 1, O and 0 are misread both ways on a plate."""
    return vehicle_key(text).replace("O", "0").replace("I", "1")


def route_key(text):
    """Routes compare whole: the 025 and the 25 are different services."""
    return vehicle_key(text)


def _tail_is(recorded, code):
    """ "SCSO10950" and "10950" are fleet 10950; "840" is not fleet 40."""
    return bool(code) and (recorded == code or
                           (recorded.endswith(code) and recorded[:-len(code)].isalpha()))


def same_vehicle(recorded, typed, plates=None, operator=None):
    """Whether the feed's vehicle reference is the bus whose plate (or fleet
    number) was typed. Compass's feed carries the plate ("YX12_FRC"); the
    others carry a fleet number ("832", "SCSO-10950"), which a plate is looked
    up to in `plates` ({plate_key: [[operator, fleet_code], ...]}), for the
    row's own operator only."""
    r, t = vehicle_key(recorded), vehicle_key(typed)
    if not (r and t):
        return False
    if plate_key(r) == plate_key(t):
        return True
    family = FAMILIES.get(operator, operator)
    for op, code in (plates or {}).get(plate_key(t), ()):
        if operator and FAMILIES.get(op, op) != family:
            continue
        if _tail_is(r, vehicle_key(code)):
            return True
    return len(t) >= 3 and _tail_is(r, t)


def _epoch(iso):
    if not iso:
        return None
    return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).timestamp()


def read_timings(paths):
    """Logged buses, one dict each, with `departed` and `arrived` as epochs
    corrected by the phone's measured clock offset (positive: phone fast)."""
    out = []
    for path in paths:
        with open(path, newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                offset = float(row.get("clock_offset_ms") or 0) / 1000
                dep, arr = _epoch(row.get("departed_at")), _epoch(row.get("arrived_at"))
                out.append({**row, "source": Path(path).name,
                            "departed": dep - offset if dep is not None else None,
                            "arrived": arr - offset if arr is not None else None})
    return out


def siblings(stops, atco):
    """The pole itself and same-named poles within SIBLING_METRES."""
    here = stops.get(atco)
    if not here:
        return {atco}
    out = {atco}
    for other, s in stops.items():
        if s["name"] == here["name"]:
            dy = (s["latitude"] - here["latitude"]) * 111_320
            dx = (s["longitude"] - here["longitude"]) * 111_320 * 0.63
            if (dx * dx + dy * dy) ** 0.5 <= SIBLING_METRES:
                out.add(other)
    return out


def stop_kind(row):
    if row.get("stop_index") == 0 or (row.get("calls_total") and row.get("stop_index") == row["calls_total"] - 1):
        return "end of route"
    return "timing point" if row.get("timepoint") == 1 else "other stop"


def match(entry, by_atco, stops, plates=None, unseen=None):
    """`(observation row or None, how it matched or why not)` for one logged bus.
    `unseen` maps a pole to the scheduled calls there we did not record."""
    dep = entry.get("departed")
    if dep is None:
        return None, "no departure logged"
    atco = entry.get("atco")
    poles = siblings(stops, atco)
    gap = lambda r: abs(r["observed_epoch"] - dep)                  # noqa: E731
    why = None
    if entry.get("vehicle"):
        same = [r for a in poles for r in by_atco.get(a, ())
                if same_vehicle(r.get("vehicle"), entry["vehicle"], plates, r.get("operator"))]
        near = [r for r in same if gap(r) <= STRONG_WINDOW_SECS]
        if near:
            return min(near, key=gap), "registration"
        why = "bus recorded here, but not near this time" if same else "bus not recorded at this stop"
    route = route_key(entry.get("route"))
    if not route:
        return None, why or "no route or registration logged"

    def of_route(rows):
        return sorted((r for r in rows if route_key(r.get("service")) == route
                       and gap(r) <= ROUTE_WINDOW_SECS), key=gap)

    near = of_route(by_atco.get(atco, ()))
    if not near:
        across = of_route(r for a in poles - {atco} for r in by_atco.get(a, ()))
        return None, why or ("no bus of that route here; one on the other side of the road" if across else
                             f"no bus of that route recorded here within {ROUTE_WINDOW_SECS // 60} minutes")
    if len(near) > 1 and gap(near[1]) - gap(near[0]) < AMBIGUITY_MARGIN_SECS:
        return None, why or "two buses of that route too close to tell apart"
    rivals = [c for c in (unseen or {}).get(atco, ())
              if route_key(c["service"]) == route and abs(c["scheduled_epoch"] - dep) <= 3 * 3600]
    if rivals:
        late = sorted(r["lateness_secs"] for r in by_atco.get(atco, ())
                      if route_key(r.get("service")) == route and r.get("lateness_secs") is not None
                      and abs(r["observed_epoch"] - dep) <= LATENESS_WINDOW_SECS)
        for c in rivals:
            if c.get("estimated_epoch") is not None:
                # Seen elsewhere on its journey: its time here is estimated.
                plausible = abs(c["estimated_epoch"] - dep) < gap(near[0]) + RIVAL_MARGIN_SECS
            elif len(late) >= MIN_LATENESS_SAMPLE:
                # Never seen: it could be anywhere the route was running, from
                # its earliest tenth to its latest.
                lo, hi = late[len(late) // 10], late[-1 - len(late) // 10]
                plausible = (c["scheduled_epoch"] + lo - RIVAL_MARGIN_SECS <= dep
                             <= c["scheduled_epoch"] + hi + RIVAL_MARGIN_SECS)
            else:
                typical = statistics.median(late) if late else 0
                plausible = abs(c["scheduled_epoch"] + typical - dep) < gap(near[0]) + RIVAL_MARGIN_SECS
            if plausible:
                return None, why or "a journey we did not record could be this bus"
    return near[0], "route and time"


def compare(entries, observation_rows, stops, plates=None, schedule_calls=None):
    """One result per logged bus. `schedule_calls` (from `scheduled_calls`)
    lets a journey we never recorded at a pole stand as a rival there."""
    by_atco, estimated = defaultdict(list), {}
    for r in observation_rows:
        if r.get("observed_epoch") is None:
            continue
        if r.get("estimated"):
            estimated[(r.get("day"), r.get("trip_id"), r.get("atco"))] = r["observed_epoch"]
        else:
            by_atco[r.get("atco")].append(r)
    seen = {(r.get("day"), r.get("trip_id"), a) for a, rs in by_atco.items() for r in rs}
    unseen = defaultdict(list)
    for c in schedule_calls or ():
        key = (c["day"], c["trip_id"], c["atco"])
        if key not in seen:
            unseen[c["atco"]].append({**c, "estimated_epoch": estimated.get(key)})
    results = []
    for e in entries:
        row, how = match(e, by_atco, stops, plates, unseen)
        result = {"id": e.get("id"), "day": e.get("day"), "atco": e.get("atco"), "stop": e.get("stop_name"),
                  "route": e.get("route"), "vehicle": e.get("vehicle"), "matched": how if row else None,
                  "reason": None if row else how,
                  "dwell_secs": (round(e["departed"] - e["arrived"])
                                 if e.get("departed") is not None and e.get("arrived") is not None else None)}
        if row:
            lo, hi = (row.get("observed_interval_epoch") or [row["observed_epoch"], row["observed_epoch"]])[:2]
            # No report after the last one at the stop: no upper bound to check.
            bounded = lo is not None and hi is not None
            result.update({
                "error_secs": round(row["observed_epoch"] - e["departed"]),
                "inside_bounds": lo - 1 <= e["departed"] <= hi + 1 if bounded else None,
                "bounds_secs": round(hi - lo) if bounded else None,
                "operator": row.get("operator"), "kind": stop_kind(row), "identity": row.get("match"),
                "flagged": bool(row.get("quality_flags")),
            })
        results.append(result)
    return results


def _quantile(values, q):
    s = sorted(values)
    return s[min(len(s) - 1, int(q * len(s)))] if s else None


def _block(rs):
    errs = [r["error_secs"] for r in rs if r.get("error_secs") is not None]
    dwell = [r["dwell_secs"] for r in rs if r.get("dwell_secs") is not None]
    inside = [r["inside_bounds"] for r in rs if r.get("inside_bounds") is not None]
    return {"matched": len(errs),
            "median_abs_secs": statistics.median(abs(x) for x in errs) if errs else None,
            "p90_abs_secs": _quantile([abs(x) for x in errs], 0.9),
            "bias_secs": statistics.median(errs) if errs else None,
            "within_30_secs": sum(abs(x) <= 30 for x in errs),
            "within_60_secs": sum(abs(x) <= 60 for x in errs),
            "inside_bounds": sum(inside), "bounds_checked": len(inside),
            "median_dwell_secs": statistics.median(dwell) if dwell else None}


def summarise(results):
    matched = [r for r in results if r["matched"]]
    reasons = defaultdict(int)
    for r in results:
        if r["reason"]:
            reasons[r["reason"]] += 1

    def by(key):
        return {k: _block([r for r in matched if r.get(key) == k])
                for k in sorted({r.get(key) for r in matched if r.get(key)})}

    return {"logged": len(results), "all": _block(matched),
            "registration": _block([r for r in matched if r["matched"] == "registration"]),
            "route_and_time": _block([r for r in matched if r["matched"] == "route and time"]),
            "unmatched": dict(sorted(reasons.items())), "by_kind": by("kind"), "by_operator": by("operator"),
            "days": sorted({r["day"] for r in results if r.get("day")})}


def scheduled_calls(doc, atcos):
    """Every call the day's timetable scheduled at `atcos`, from the
    `schedule` an observation file carries, with its time as an epoch."""
    sched = doc.get("schedule") or {}
    day = sched.get("day")
    if not day:
        return []
    # GTFS times count from noon minus twelve hours, London.
    origin = (datetime.fromisoformat(f"{day}T12:00").replace(tzinfo=LONDON) - timedelta(hours=12)).timestamp()
    out = []
    for trip_id, pattern, profile, start, *_ in sched.get("trips") or []:
        stops_on = sched["patterns"][pattern]["atcos"]
        offsets = sched["profiles"][profile]["offsets"]
        for i, atco in enumerate(stops_on):
            if atco in atcos:
                out.append({"day": day, "trip_id": trip_id, "atco": atco,
                            "service": sched["patterns"][pattern]["service"],
                            "scheduled_epoch": origin + start + offsets[i]})
    return out


def load_observations(days, folder, calls_at=None, calls=None):
    """Observation rows for `days`; with `calls_at` (poles) and a `calls`
    list, also the scheduled calls at those poles."""
    rows = []
    for day in days:
        for path in sorted(Path(folder).glob(f"observations-{day}.json*")):
            opener = gzip.open if path.suffix == ".gz" else open
            with opener(path, "rt", encoding="utf-8") as handle:
                doc = json.load(handle)
            rows += doc.get("observations") or []
            if calls is not None and calls_at:
                calls += scheduled_calls(doc, calls_at)
    return rows


def fetch(days, folder, base=PUBLISHED):
    """Download the published observation files for `days` into `folder`."""
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(f"{base}/index.json", timeout=60) as res:
        index = json.load(res)
    for day in days:
        name = f"observations-{day}.json.gz"
        entry = (index.get("artifacts") or {}).get(name)
        if not entry:
            print(f"{name}: not published yet")
            continue
        if not (folder / name).exists():
            urllib.request.urlretrieve(f"{base}/{entry['file']}", folder / name)


def fetch_fleet(path, operators=FLEET_OPERATORS, opener=urllib.request.urlopen, pause=1.0):
    """Plates to fleet numbers from bustimes.org's public vehicle list, saved
    to `path`: {plate_key: [[operator, fleet_code], ...]}, current and
    previous plates both. Used only to read a typed plate; never published."""
    plates = defaultdict(list)
    for op in operators:
        url = f"{FLEET_API}?operator={op}&limit=500"
        while url:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with opener(request, timeout=60) as res:
                page = json.load(res)
            for v in page.get("results") or []:
                number = v.get("fleet_number")
                code = str(v.get("fleet_code") or (number if number is not None else "")).strip()
                if not code:
                    continue
                for reg in [v.get("reg") or ""] + re.split(r"[,;/]", v.get("previous_reg") or ""):
                    key = plate_key(reg)
                    if key and [op, code] not in plates[key]:
                        plates[key].append([op, code])
            url = page.get("next")
            if url and pause:
                time.sleep(pause)
    plates = dict(plates)
    Path(path).write_text(json.dumps({
        "source": FLEET_API, "operators": list(operators),
        "fetched_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "note": "Community-maintained (bustimes.org). Reads typed plates only; not for publication.",
        "plates": plates}) + "\n")
    return plates


def markdown(summary):
    def secs(v, signed=False):
        if v is None:
            return "–"
        return f"{v:+.0f} s" if signed else f"{v:.0f} s"

    def share(a, b):
        return f"{a} of {b}" + (f" ({round(100 * a / b)}%)" if b else "")

    s = summary["all"]
    lines = [f"Days: {', '.join(summary['days'])}. Buses logged: {summary['logged']}.", "",
             f"- Matched: **{s['matched']}** of {summary['logged']}: by registration "
             f"{summary['registration']['matched']}, by route, pole and time "
             f"{summary['route_and_time']['matched']}.",
             f"- Typical error: **{secs(s['median_abs_secs'])}**; 9 in 10 within **{secs(s['p90_abs_secs'])}**; "
             f"bias **{secs(s['bias_secs'], True)}** (positive: we record buses leaving later than they did).",
             f"- Within 30 s: {share(s['within_30_secs'], s['matched'])}; within 60 s: "
             f"{share(s['within_60_secs'], s['matched'])}.",
             f"- The true departure fell inside our recorded bounds: "
             f"{share(s['inside_bounds'], s['bounds_checked'])}.",
             f"- Doors open for a median {secs(s['median_dwell_secs'])}.", "",
             f"A bus logged by route alone is matched to the bus of that route we recorded leaving the same "
             f"pole nearest the moment timed, within {ROUTE_WINDOW_SECS // 60} minutes. It is set aside when "
             f"another bus of the route was within {AMBIGUITY_MARGIN_SECS} seconds of being as close, or when "
             f"a journey we never recorded there could have been it.", ""]
    if summary["unmatched"]:
        lines += ["Not matched: " + "; ".join(f"{n} {why}" for why, n in summary["unmatched"].items()), ""]
    for title, key in (("By how the bus was matched", None), ("By kind of stop", "by_kind"),
                       ("By operator", "by_operator")):
        lines += [f"### {title}", "",
                  "| | Matched | Typical error | 9 in 10 within | Bias | Inside bounds |",
                  "|---|---|---|---|---|---|"]
        blocks = summary[key].items() if key else (("registration", summary["registration"]),
                                                   ("route, pole and time", summary["route_and_time"]))
        for name, b in blocks:
            lines.append(f"| {name} | {b['matched']} | {secs(b['median_abs_secs'])} | {secs(b['p90_abs_secs'])} "
                         f"| {secs(b['bias_secs'], True)} | {share(b['inside_bounds'], b['bounds_checked'])} |")
        lines.append("")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--timings", nargs="+", required=True, help="CSV files exported from timing.html")
    ap.add_argument("--observations", help="folder holding observations-YYYY-MM-DD.json.gz")
    ap.add_argument("--fetch", help="download the published observations into this folder first")
    ap.add_argument("--fleet", help="plate-to-fleet-number list (default: fleet.json in the observations folder; "
                                    "fetched from bustimes.org when missing and a plate was typed)")
    ap.add_argument("--refresh-fleet", action="store_true", help="fetch the fleet list again")
    ap.add_argument("--out", help="write the per-bus results and summary as JSON")
    args = ap.parse_args(argv)
    paths = sorted({p for pattern in args.timings for p in glob.glob(pattern)})
    entries = read_timings(paths)
    days = sorted({e["day"] for e in entries if e.get("day")})
    # A bus after midnight belongs to the service day before.
    wanted = sorted(set(days) | {(date.fromisoformat(d) - timedelta(days=1)).isoformat() for d in days})
    folder = args.fetch or args.observations
    if not folder:
        ap.error("give --observations or --fetch")
    if args.fetch:
        fetch(wanted, args.fetch)
    stops = {s["atco_code"]: s for s in json.loads((ROOT / "data/stops.json").read_text())["stops"]}
    plates = {}
    if any(e.get("vehicle") for e in entries):
        fleet = Path(args.fleet or Path(folder) / "fleet.json")
        try:
            if args.refresh_fleet or not fleet.exists():
                fleet.parent.mkdir(parents=True, exist_ok=True)
                fetch_fleet(fleet)
            plates = json.loads(fleet.read_text())["plates"]
        except (OSError, ValueError, KeyError) as exc:
            print(f"Fleet list unavailable ({exc}): plates match Compass buses only.", file=sys.stderr)
    calls = []
    poles = {a for e in entries for a in siblings(stops, e.get("atco"))}
    rows = load_observations(wanted, folder, poles, calls)
    results = compare(entries, rows, stops, plates, calls)
    summary = summarise(results)
    if args.out:
        Path(args.out).write_text(json.dumps({"summary": summary, "results": results}, indent=1) + "\n")
    print(markdown(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
