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

ROOT  = Path(__file__).resolve().parent.parent
DB    = ROOT / "data" / "timetable.sqlite"
OUT   = ROOT / "data" / "boundary_evidence.json"
AREAS = ROOT / "data" / "comparison_areas.json"

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

# Saturday and Sunday are reported as one "weekend" figure, because two panels
# spent on them crowded out the comparison that makes the case concrete. They
# are still measured separately and still published separately in `days`: the
# merge happens in weekend_block, which divides by stops x 2 so the unit stays
# departures per stop per *day*, and which keeps both route counts so the
# average cannot hide Sunday being the thinner of the two.
WEEKEND = ("saturday", "sunday")


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


def load_areas() -> dict:
    """The two named places compared directly, with their polygons.

    Kept in data/comparison_areas.json rather than fetched, so the build is
    reproducible offline and the figures cannot move because a remote service
    was updated between two runs.
    """
    if not AREAS.exists():
        sys.exit(f"{AREAS} is missing — the place comparison cannot be built.")
    return json.loads(AREAS.read_text(encoding="utf-8"))


def in_rings(rings, lon: float, lat: float) -> bool:
    """Even-odd ray casting across every ring of a polygon.

    Testing all rings together rather than outer-then-holes is what makes a
    hole behave like a hole: a point inside both the outer ring and an enclosed
    one crosses twice, lands even, and counts as outside. Separate parts of a
    multi-part area each cross once and count as inside.
    """
    inside = False
    for ring in rings:
        n = len(ring)
        for i in range(n):
            x1, y1 = ring[i]
            x2, y2 = ring[(i + 1) % n]
            if (y1 > lat) != (y2 > lat):
                if x1 + (lat - y1) * (x2 - x1) / (y2 - y1) > lon:
                    inside = not inside
    return inside


def bbox(rings):
    xs = [x for ring in rings for x, _ in ring]
    ys = [y for ring in rings for _, y in ring]
    return min(xs), min(ys), max(xs), max(ys)


def bucket_stops(con: sqlite3.Connection, areas: dict) -> tuple:
    """Assign every stop to a band side, a named place, or neither.

    Returns ({sid: [(group, bucket), ...]}, {group: {bucket: stop count}}).
    A stop can belong to both groups — Portslade stops sit in the band and in
    the South Portslade ward — so membership is a list, and the single pass
    over stop_times below counts each row once per group it belongs to.
    """
    boxes = {a["id"]: bbox(a["rings"]) for a in areas["areas"]}
    members = {}
    counts = {"band": Counter(), "places": Counter()}

    for sid, lat, lon in con.execute("SELECT sid, lat, lon FROM stops"):
        if lat is None or lon is None:
            continue
        found = []
        if LAT_MIN <= lat <= LAT_MAX and abs(lon - LINE_LON) <= BAND_LON:
            side = "west" if lon < LINE_LON else "east"
            found.append(("band", side))
            counts["band"][side] += 1
        for area in areas["areas"]:
            x0, y0, x1, y1 = boxes[area["id"]]
            if not (x0 <= lon <= x1 and y0 <= lat <= y1):
                continue
            if in_rings(area["rings"], lon, lat):
                found.append(("places", area["side"]))
                counts["places"][area["side"]] += 1
                break
        if found:
            members[sid] = found

    for group in ("band", "places"):
        for side in ("west", "east"):
            if not counts[group][side]:
                sys.exit(f"No stops on the {side} side of the {group} comparison "
                         f"— check the band constants or the polygons.")
    return members, counts


def measure(con: sqlite3.Connection, areas: dict) -> tuple:
    members, counts = bucket_stops(con, areas)

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

    per_day = {g: {d: {"departures": Counter(), "late": Counter(),
                       "routes": defaultdict(set)} for d in DAYS}
               for g in ("band", "places")}

    runs_memo = {}
    for sid, tid, dep in con.execute("SELECT sid, tid, dep_secs FROM stop_times"):
        where = members.get(sid)
        if not where:
            continue
        service_id = trip_service.get(tid, "")
        short = routes.get(trip_route.get(tid), "")
        late = dep is not None and dep >= NIGHT_FROM
        for day in DAYS:
            key = (service_id, day)
            if key not in runs_memo:
                runs_memo[key] = runs(service_id, day)
            if not runs_memo[key]:
                continue
            for group, side in where:
                slot = per_day[group][day]
                slot["departures"][side] += 1
                if short:
                    slot["routes"][side].add(short)
                if late:
                    slot["late"][side] += 1

    def block(group, day):
        d = per_day[group][day]
        out = {}
        for side in ("west", "east"):
            n = counts[group][side]
            out[side] = {
                "stops": n,
                "departures": d["departures"][side],
                "departures_per_stop": round(d["departures"][side] / n, 1),
                "routes": len(d["routes"][side]),
                "departures_after_2300": d["late"][side],
                "route_list": sorted(d["routes"][side]),
            }
        return with_ratio(out)

    out = {g: {day: block(g, day) for day in DAYS} for g in ("band", "places")}
    for g in out:
        out[g]["weekend"] = weekend_block(out[g])
    return out, counts


def with_ratio(out: dict) -> dict:
    west, east = out["west"], out["east"]
    out["ratio"] = {
        "departures_per_stop": round(
            west["departures_per_stop"] / east["departures_per_stop"], 3)
        if east["departures_per_stop"] else None,
        "routes": round(west["routes"] / east["routes"], 3) if east["routes"] else None,
    }
    return out


def weekend_block(days_out: dict) -> dict:
    """Saturday and Sunday as one figure, without losing the difference.

    Departures are summed and divided by stops x 2, so the published number is
    still departures per stop per *day* and sits on the same axis as the
    weekday one — a two-day total would be twice as long a bar for the same
    level of service. Route counts do not average meaningfully, so both days
    are carried through and the panel names them: on this corridor Saturday
    runs close to a weekday and Sunday collapses, and an average that hid that
    would be doing the reader's thinking for them.
    """
    sat, sun = days_out["saturday"], days_out["sunday"]
    out = {}
    for side in ("west", "east"):
        n = sat[side]["stops"]
        departures = sat[side]["departures"] + sun[side]["departures"]
        route_list = sorted(set(sat[side]["route_list"]) | set(sun[side]["route_list"]))
        out[side] = {
            "stops": n,
            "departures": departures,
            "departures_per_stop": round(departures / (n * 2), 1),
            "routes": len(route_list),
            "routes_saturday": sat[side]["routes"],
            "routes_sunday": sun[side]["routes"],
            "departures_after_2300": (sat[side]["departures_after_2300"]
                                      + sun[side]["departures_after_2300"]),
            "route_list": route_list,
        }
    out = with_ratio(out)
    out["combines"] = list(WEEKEND)
    out["denominator"] = (
        "Departures across both weekend days divided by stops x 2, so the "
        "figure is departures per stop per day and is comparable with the "
        "weekday one. Route counts are given for each day separately, because "
        "Saturday and Sunday are different services and an average would hide "
        "the thinner of the two.")
    return out


def main() -> None:
    if not DB.exists():
        sys.exit(f"{DB} is missing. Run scripts/json_to_sqlite.py first.")
    con = sqlite3.connect(DB)
    areas = load_areas()
    measured, counts = measure(con, areas)
    days = measured["band"]
    by_side = {a["side"]: a for a in areas["areas"]}

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
                "Report Saturday and Sunday as one weekend figure: departures "
                "over both days divided by stops x 2, so the unit stays "
                "departures per stop per day. Both days' route counts are kept "
                "and shown, because Saturday and Sunday are different services.",
                "Repeat the whole count for two named places either side of the "
                "line — Lancing and South Portslade — selecting stops by whether "
                "their coordinates fall inside the published ONS boundary "
                "polygon rather than by a distance band.",
            ],
            "denominator": (
                "Departures are divided by the number of stops in the same bucket, "
                "so the figure is departures per stop per day — not a total, which "
                "would only restate that one side has more stops."),
            "places": {
                "summary": (
                    "The band shows the average effect of the line. The place "
                    "comparison shows what it means somewhere specific: Lancing "
                    "against South Portslade, six miles apart along the same "
                    "coast road, one either side of the boundary."),
                "pairing": areas.get("pairing", ""),
                "selection": (
                    "A stop belongs to a place when its coordinates fall inside "
                    "that area's ONS polygon, tested by even-odd ray casting so "
                    "enclosed holes count as outside."),
                "boundaries": areas.get("source", {}),
                "areas": [
                    {k: a[k] for k in
                     ("id", "side", "name", "council", "ons_code", "ons_name", "ons_type")}
                    for a in areas["areas"]
                ],
            },
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
                    "South Portslade sits on a trunk corridor where routes 1, "
                    "1X, 2, 2B, 46, 49 and 6 all pass. Part of the gap is that "
                    "Lancing has no corridor of that kind, which is geography "
                    "as much as it is council policy."),
                "direction": "overstates",
                "effect": (
                    "Some of the Lancing/Portslade difference would exist "
                    "whoever ran the buses. It is the point being made, but it "
                    "should be said rather than left implied."),
            },
            {
                "text": (
                    "The weekend figure averages Saturday and Sunday, which are "
                    "different services on both sides of the line. The panel "
                    "gives each day's route count for that reason."),
                "direction": "unknown",
                "effect": (
                    "The averaged ratio sits between the two days' ratios, which "
                    "are close; the route counts are what the average would "
                    "otherwise flatten."),
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
        "places": {
            "id": "lancing-vs-south-portslade",
            "headline": (
                "Six miles apart on the same coast road, and a quarter of the "
                "service."),
            "west": {**{k: by_side["west"][k] for k in
                        ("id", "name", "council", "ons_code", "ons_name", "ons_type")},
                     "stops": counts["places"]["west"]},
            "east": {**{k: by_side["east"][k] for k in
                        ("id", "name", "council", "ons_code", "ons_name", "ons_type")},
                     "stops": counts["places"]["east"]},
            "days": measured["places"],
        },
    }

    OUT.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {OUT.relative_to(ROOT)}")
    for label, block in (("weekday", days["monday"]), ("weekend", days["weekend"])):
        print(f"  band {label}: west {block['west']['departures_per_stop']} "
              f"vs east {block['east']['departures_per_stop']} per stop "
              f"({block['ratio']['departures_per_stop']:.0%}), "
              f"{block['west']['routes']} routes vs {block['east']['routes']}")
    pl = measured["places"]["monday"]
    print(f"  Lancing ({counts['places']['west']} stops) vs South Portslade "
          f"({counts['places']['east']} stops), weekday: "
          f"{pl['west']['departures_per_stop']} vs "
          f"{pl['east']['departures_per_stop']} per stop "
          f"({pl['ratio']['departures_per_stop']:.0%}), "
          f"{pl['west']['routes']} routes vs {pl['east']['routes']}")


if __name__ == "__main__":
    main()
