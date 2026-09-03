#!/usr/bin/env python3
"""
scripts/build_evidence.py
=========================
Compute the boundary-effect statistics the site publishes, and write them to
data/boundary_evidence.json with everything needed to check them.

Run after scripts/json_to_sqlite.py, in the same weekly workflow:

    python scripts/build_evidence.py

Why a build step rather than an endpoint: the figures only change when a new
timetable is published, the Render free tier sleeps, and a number recomputed by
the pipeline on every rebuild cannot go stale the way one typed into a JSON file
would. See .claude/skills/evidence-provenance.

The site's central claim is that bus service drops sharply crossing the
Brighton & Hove / West Sussex boundary westward. That is checkable against the
timetable this project already ships, so it should be checked rather than
asserted — and published with its method, its data version and its caveats, so
a reader can disagree with it on the evidence.
"""
import hashlib
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "timetable.sqlite"
OUT  = ROOT / "data" / "boundary_evidence.json"

# ── The comparison ───────────────────────────────────────────
# A band either side of the council line, along the same coastal strip, so the
# two sides are like for like: same kind of place, same distance from the sea,
# same latitude range. Widening it would fold in Brighton's dense centre and
# Worthing's suburbs and prove nothing except that cities differ from suburbs.
LINE_LON   = -0.216          # the boundary through Portslade
BAND_LON   = 0.057           # ~4 km at this latitude
LAT_MIN    = 50.818
LAT_MAX    = 50.855
NIGHT_FROM = 23 * 3600       # "late" for the late-service comparison

WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday",
            "friday", "saturday", "sunday")
DAYS = ("monday", "saturday", "sunday")


def sample_week(from_day: date = None) -> dict:
    """A concrete week to measure, so a figure can be quoted with its date.

    The first week starting on or after today; measuring "a Monday" means
    picking one, and saying which.
    """
    from datetime import timedelta
    base = from_day or date.today()
    monday = base + timedelta(days=(0 - base.weekday()) % 7)
    return {name: monday + timedelta(days=i) for i, name in enumerate(WEEKDAYS)}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def measure(con: sqlite3.Connection) -> dict:
    stops = {}
    for sid, stop_id, name, lat, lon in con.execute(
        "SELECT sid, stop_id, name, lat, lon FROM stops WHERE lat BETWEEN ? AND ?",
        (LAT_MIN, LAT_MAX),
    ):
        if lat is None or lon is None:
            continue
        if abs(lon - LINE_LON) <= BAND_LON:
            stops[sid] = "west" if lon < LINE_LON else "east"

    counts = Counter(stops.values())
    if not counts["west"] or not counts["east"]:
        sys.exit("No stops on one side of the line — check the band constants.")

    # Calendars carry their validity window, because "does this row mention
    # Monday" is not the same question as "does this bus run on that Monday".
    # This feed describes one real service with several calendars covering term
    # time, holidays and seasonal variants; counting them all inflates a stop
    # several-fold, and not by the same factor on both sides of the line.
    calendar = {}
    for row in con.execute(
        "SELECT service_id, monday, tuesday, wednesday, thursday, friday, "
        "saturday, sunday, start_date, end_date FROM calendar"
    ):
        calendar[row[0]] = {
            **{d: str(row[i + 1]) for i, d in enumerate(WEEKDAYS)},
            "start_date": row[8] or "",
            "end_date": row[9] or "",
        }
    exceptions = {}
    for service_id, date_str, exc in con.execute(
        "SELECT service_id, date, exception FROM calendar_dates"
    ):
        exceptions.setdefault(service_id, {})[date_str] = str(exc)

    week = sample_week()

    def runs(service_id, day):
        stamp = week[day].strftime("%Y%m%d")
        ex = exceptions.get(service_id, {})
        if stamp in ex:
            return ex[stamp] == "1"
        cal = calendar.get(service_id)
        if not cal:
            return False
        if cal["start_date"] and stamp < cal["start_date"]:
            return False
        if cal["end_date"] and stamp > cal["end_date"]:
            return False
        return cal.get(day) == "1"
    trip_service = {}
    trip_route = {}
    for tid, rid, service_id in con.execute("SELECT tid, rid, service_id FROM trips"):
        trip_service[tid] = service_id
        trip_route[tid] = rid
    routes = {rid: short for rid, short in con.execute(
        "SELECT rid, short_name FROM routes")}

    per_day = {d: {"departures": Counter(), "late": Counter(),
                   "routes": defaultdict(set)} for d in DAYS}

    runs_memo = {}
    for sid, tid, dep in con.execute("SELECT sid, tid, dep_secs FROM stop_times"):
        side = stops.get(sid)
        if side is None:
            continue
        service_id = trip_service.get(tid, "")
        short = routes.get(trip_route.get(tid), "")
        for day in DAYS:
            key = (service_id, day)
            if key not in runs_memo:
                runs_memo[key] = runs(service_id, day)
            if not runs_memo[key]:
                continue
            per_day[day]["departures"][side] += 1
            if short:
                per_day[day]["routes"][side].add(short)
            if dep is not None and dep >= NIGHT_FROM:
                per_day[day]["late"][side] += 1

    def block(day):
        d = per_day[day]
        out = {}
        for side in ("west", "east"):
            n = counts[side]
            out[side] = {
                "stops": n,
                "departures": d["departures"][side],
                "departures_per_stop": round(d["departures"][side] / n, 1),
                "routes": len(d["routes"][side]),
                "departures_after_2300": d["late"][side],
                "route_list": sorted(d["routes"][side]),
            }
        west, east = out["west"], out["east"]
        out["ratio"] = {
            "departures_per_stop": round(
                west["departures_per_stop"] / east["departures_per_stop"], 3)
            if east["departures_per_stop"] else None,
            "routes": round(west["routes"] / east["routes"], 3) if east["routes"] else None,
        }
        return out

    return {day: block(day) for day in DAYS}


def main() -> None:
    if not DB.exists():
        sys.exit(f"{DB} is missing. Run scripts/json_to_sqlite.py first.")
    con = sqlite3.connect(DB)
    days = measure(con)

    doc = {
        "_comment": (
            "Derived statistics, recomputed by scripts/build_evidence.py on every "
            "timetable rebuild. Do not hand-edit: a figure typed in here will drift "
            "away from the data it claims to describe. See "
            ".claude/skills/evidence-provenance."),
        "id": "bhcc-wscc-boundary-effect",
        "sides": {"west": "West Sussex", "east": "Brighton & Hove"},
        "headline": (
            "Bus service is measurably thinner on the West Sussex side of the "
            "Brighton & Hove boundary."),
        "as_of": date.today().isoformat(),
        "computed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "data_version": {
            "source": "Bus Open Data Service, South East regional GTFS bundle",
            "artefact": "data/timetable.sqlite",
            "sha256": sha256(DB),
        },
        "measured_week": {d: sample_week()[d].isoformat() for d in DAYS},
        "method": {
            "summary": (
                "Stops in a 4 km band either side of the council boundary, along "
                "the same coastal strip, bucketed east or west of the line and "
                "compared per stop."),
            "band": {
                "line_lon": LINE_LON,
                "half_width_lon_deg": BAND_LON,
                "approx_half_width_km": 4,
                "lat_min": LAT_MIN,
                "lat_max": LAT_MAX,
            },
            "steps": [
                "Select stops with latitude between the band's lat_min and lat_max "
                "and longitude within half_width_lon_deg of line_lon.",
                "Bucket each stop west or east of line_lon.",
                "Join stop_times to trips to calendar; keep a trip only if its "
                "service actually runs on the dated day being measured — its "
                "calendar window must cover that date, and calendar_dates "
                "exceptions override it. Counting every calendar that merely "
                "lists the weekday counts one bus several times over, because "
                "term-time, holiday and seasonal variants each have their own.",
                "Count departures and distinct route short_names per bucket.",
                "Divide by the number of stops in that bucket.",
            ],
            "denominator": (
                "Departures are divided by the number of stops in the same bucket, "
                "so the figure is departures per stop per day — not a total, which "
                "would only restate that one side has more stops."),
            "script": "scripts/build_evidence.py",
        },
        "caveats": [
            {
                "text": (
                    "scripts/build_timetable.py keeps a route only if it touches a "
                    "West Sussex (ATCO 4400) stop or appears on a hand-maintained "
                    "allowlist of cross-boundary services. Routes running purely "
                    "inside Brighton & Hove are therefore missing from this "
                    "database."),
                "direction": "understates",
                "effect": (
                    "The east side of every comparison is under-counted, so the "
                    "real gap is wider than these figures show."),
            },
            {
                "text": (
                    "Departures counted are scheduled, not operated. Cancellations "
                    "and short-workings are not visible in a timetable feed."),
                "direction": "unknown",
                "effect": "Could move the comparison either way.",
            },
            {
                "text": (
                    "A band drawn at a different width would give different "
                    "figures. 4 km was chosen to keep the strip comparable in "
                    "character; widening it folds in Brighton city centre."),
                "direction": "unknown",
                "effect": (
                    "Widening the band favours the east; narrowing it makes the "
                    "two sides more alike."),
            },
        ],
        "days": days,
    }

    OUT.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    wk = days["monday"]
    print(f"Wrote {OUT.relative_to(ROOT)}")
    print(f"  weekday departures per stop: west {wk['west']['departures_per_stop']} "
          f"vs east {wk['east']['departures_per_stop']} "
          f"({wk['ratio']['departures_per_stop']:.0%})")
    print(f"  weekday routes: west {wk['west']['routes']} vs east {wk['east']['routes']}")
    print(f"  Sunday routes:  west {days['sunday']['west']['routes']} "
          f"vs east {days['sunday']['east']['routes']}")


if __name__ == "__main__":
    main()
