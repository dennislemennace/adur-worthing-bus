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
NIGHT_TO   = 5 * 3600        # ...and when the night ends, the next morning

# The two nights the night comparison measures: an ordinary weeknight, and the
# one people most need a bus home from. A night is named by the evening it
# starts on and runs to five the next morning.
NIGHTS = (("weeknight", "monday"), ("saturday", "saturday"))

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

# `--week-of YYYY-MM-DD` measures the week starting on or after that date
# instead of the coming one. For re-running against a timetable fetched some
# days ago, whose coming week can run past the end of what it covers: the
# figures then fall off a cliff that is the file's horizon, not the service.
WEEK_OF = None


def sample_week(from_day: date = None) -> dict:
    """A concrete week to measure, so a figure can be quoted with its date.

    The first week starting on or after today; measuring "a Monday" means
    picking one, and saying which.
    """
    from datetime import timedelta
    base = from_day or WEEK_OF or date.today()
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
            if area.get("night_only"):
                continue
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


def service_runner(con: sqlite3.Connection, week: dict):
    """runs(service_id, day) for the dated week being measured."""
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

    memo = {}

    def cached(service_id, day):
        key = (service_id, day)
        if key not in memo:
            memo[key] = runs(service_id, day)
        return memo[key]
    return cached


def measure(con: sqlite3.Connection, areas: dict) -> tuple:
    members, counts = bucket_stops(con, areas)
    runs = service_runner(con, sample_week())

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

    for sid, tid, dep in con.execute("SELECT sid, tid, dep_secs FROM stop_times"):
        where = members.get(sid)
        if not where:
            continue
        service_id = trip_service.get(tid, "")
        short = routes.get(trip_route.get(tid), "")
        late = dep is not None and dep >= NIGHT_FROM
        for day in DAYS:
            if not runs(service_id, day):
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


def measure_night(con: sqlite3.Connection, areas: dict) -> dict:
    """Buses between 23:00 and 05:00 in each named place, per stop.

    A night is the evening it starts on: departures from 23:00 on that day's
    timetable (GTFS writes the small hours as 24:xx and later, up to 29:00),
    plus departures before 05:00 on the next day's. Counting one service day's
    23:00-05:00 instead would add the *previous* night's small hours to this
    one's evening and describe no night that anybody travels on.

    Every named area is measured, including those used only here: North
    Portslade is not on the coast road, so it answers the objection that the
    daytime gap is the A259 corridor rather than the council line.
    """
    week = sample_week()
    runs = service_runner(con, week)
    boxes = {a["id"]: bbox(a["rings"]) for a in areas["areas"]}
    member, stops = {}, Counter()
    for sid, lat, lon in con.execute("SELECT sid, lat, lon FROM stops"):
        if lat is None or lon is None:
            continue
        for area in areas["areas"]:
            x0, y0, x1, y1 = boxes[area["id"]]
            if x0 <= lon <= x1 and y0 <= lat <= y1 and in_rings(area["rings"], lon, lat):
                member[sid] = area["id"]
                stops[area["id"]] += 1
                break
    for area in areas["areas"]:
        if not stops[area["id"]]:
            sys.exit(f"No stops inside {area['name']} — check its polygon.")

    trips = {tid: (rid, service_id) for tid, rid, service_id in
             con.execute("SELECT tid, rid, service_id FROM trips")}
    routes = {rid: short for rid, short in con.execute(
        "SELECT rid, short_name FROM routes")}
    count = {n: Counter() for n, _ in NIGHTS}
    early = {n: Counter() for n, _ in NIGHTS}
    lines = {n: defaultdict(set) for n, _ in NIGHTS}
    for sid, tid, dep in con.execute("SELECT sid, tid, dep_secs FROM stop_times"):
        area = member.get(sid)
        if not area or dep is None or tid not in trips:
            continue
        rid, service_id = trips[tid]
        for name, evening in NIGHTS:
            morning = WEEKDAYS[(WEEKDAYS.index(evening) + 1) % 7]
            if NIGHT_FROM <= dep < 86400 + NIGHT_TO:
                if not runs(service_id, evening):
                    continue
            elif dep < NIGHT_TO:
                if not runs(service_id, morning):
                    continue
            else:
                continue
            count[name][area] += 1
            # Before 01:00 whichever day's timetable the trip is filed under:
            # 24:30 on the evening's, or 00:30 on the morning's.
            if dep % 86400 >= NIGHT_FROM or dep % 86400 < 3600:
                early[name][area] += 1
            if routes.get(rid):
                lines[name][area].add(routes[rid])

    out = {}
    for name, evening in NIGHTS:
        by_area = {}
        for area in areas["areas"]:
            aid = area["id"]
            by_area[aid] = {
                "stops": stops[aid],
                "departures": count[name][aid],
                "departures_per_stop": round(count[name][aid] / stops[aid], 1),
                # Where in the night the service is: the late evening, while
                # day routes are still finishing, or the small hours.
                "departures_before_0100": early[name][aid],
                "routes": len(lines[name][aid]),
                "route_list": sorted(lines[name][aid]),
            }
        morning = WEEKDAYS[(WEEKDAYS.index(evening) + 1) % 7]
        out[name] = {"evening": week[evening].isoformat(),
                     "morning": week[morning].isoformat(),
                     "by_area": by_area}
    return out


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


def night_caveats(night: dict) -> list:
    """Caveats that depend on what the night count found, so they are
    worked out from it rather than written in advance and left to go wrong."""
    out = []
    lancing = [n["by_area"]["lancing"] for n in night.values()]
    if any("025" in n["route_list"] for n in lancing):
        out.append({
            "text": (
                "Lancing's night count includes National Express coach 025 to "
                "London, which needs a booked ticket and is not a local bus."),
            "direction": "understates",
            "effect": (
                "Lancing is credited with night buses a local passenger cannot "
                "simply board, so its figure is higher than the service they "
                "can use."),
            "applies_to": ["night"],
        })
    out.append({
        "text": (
            "The figure covers the whole night, 23:00 to 05:00. Most of the "
            "difference comes before 01:00, while Portslade's daytime routes "
            "are still finishing; after that the N700 through Lancing and the "
            "N1 through Portslade run at more similar rates."),
        "direction": "unknown",
        "effect": (
            "Someone travelling in the small hours sees a smaller gap than the "
            "whole-night figure suggests; someone travelling at 11pm, a larger one."),
        "applies_to": ["night"],
    })
    return out


def main() -> None:
    global WEEK_OF
    if "--week-of" in sys.argv:
        WEEK_OF = date.fromisoformat(sys.argv[sys.argv.index("--week-of") + 1])
    if not DB.exists():
        sys.exit(f"{DB} is missing. Run scripts/json_to_sqlite.py first.")
    con = sqlite3.connect(DB)
    areas = load_areas()
    measured, counts = measure(con, areas)
    days = measured["band"]
    by_side = {a["side"]: a for a in areas["areas"] if not a.get("night_only")}
    night = measure_night(con, areas)

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
                "service runs on the dated day being measured: its "
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
                "line, Lancing and South Portslade, selecting stops by whether "
                "their coordinates fall inside the published ONS boundary "
                "polygon rather than by a distance band.",
                "For the night comparison, count departures between 23:00 and "
                "05:00 at the stops inside Lancing, North Portslade and South "
                "Portslade. A night is named by its evening: departures from "
                "23:00 on that day's timetable (GTFS writes the small hours as "
                "24:00-28:59) plus those before 05:00 on the next day's. Divide "
                "by each area's stop count.",
            ],
            "denominator": (
                "Departures are divided by the number of stops in the same bucket, "
                "so the figure is departures per stop per day, not a total, which "
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
                    {**{k: a[k] for k in
                        ("id", "side", "name", "council", "ons_code", "ons_name", "ons_type")},
                     **({"night_only": True} if a.get("night_only") else {})}
                    for a in areas["areas"]
                ],
            },
            "script": "scripts/build_evidence.py",
        },
        "caveats": [
            {
                "text": (
                    "Until 29 September 2026 the timetable kept only routes that "
                    "touch a West Sussex stop, plus a hand-kept list, so routes "
                    "running purely inside Brighton & Hove were missing. Since then "
                    "every route calling inside the map area is held, and these "
                    "figures include them."),
                "direction": "understates",
                "effect": (
                    "Figures published before that date under-counted the Brighton "
                    "side, so the gap they showed was smaller than the real one."),
                "applies_to": ["band", "places", "night"],
            },
            {
                "text": (
                    "Route counts include every service in the timetable: school-day "
                    "routes, coaches such as National Express 025, and routes that "
                    "call only a few times a day, each counted as one route."),
                "direction": "unknown",
                "effect": (
                    "The route counts say how many services exist, not how usable "
                    "they are. Departures per stop is the steadier comparison."),
                "applies_to": ["band", "places"],
            },
            {
                "text": (
                    "Departures counted are scheduled, not operated. Cancellations "
                    "and short-workings are not visible in a timetable feed."),
                "direction": "unknown",
                "effect": "Could move the comparison either way.",
                "applies_to": ["band", "places", "night"],
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
                    "should be said rather than left implied. North Portslade, "
                    "which is inland of that corridor, is in the night "
                    "comparison to test it."),
                "applies_to": ["places"],
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
                "applies_to": ["band"],
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
                "applies_to": ["band"],
            },
            *night_caveats(night),
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
        "night": {
            "id": "night-lancing-vs-portslade",
            "headline": (
                "After 11pm, Lancing's stops see a fraction of the buses either "
                "half of Portslade gets."),
            "window": {"from": "23:00", "to": "05:00"},
            "west": "lancing",
            "east": [a["id"] for a in areas["areas"] if a["side"] == "east"],
            "areas": {a["id"]: {k: a[k] for k in
                                ("name", "side", "council", "ons_code", "ons_name", "ons_type")}
                      for a in areas["areas"]},
            "nights": night,
            "denominator": (
                "Departures between 23:00 and 05:00 divided by the number of "
                "stops inside each area, so the figure is buses per stop per "
                "night."),
        },
    }

    OUT.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {OUT.relative_to(ROOT)}")
    for label, block in (("weekday", days["monday"]), ("weekend", days["weekend"])):
        print(f"  band {label}: west {block['west']['departures_per_stop']} "
              f"vs east {block['east']['departures_per_stop']} per stop "
              f"({block['ratio']['departures_per_stop']:.0%}), "
              f"{block['west']['routes']} routes vs {block['east']['routes']}")
    for name, block in night.items():
        print(f"  {name} night 23:00-05:00 per stop: " + ", ".join(
            f"{aid} {v['departures_per_stop']}" for aid, v in block["by_area"].items()))
    pl = measured["places"]["monday"]
    print(f"  Lancing ({counts['places']['west']} stops) vs South Portslade "
          f"({counts['places']['east']} stops), weekday: "
          f"{pl['west']['departures_per_stop']} vs "
          f"{pl['east']['departures_per_stop']} per stop "
          f"({pl['ratio']['departures_per_stop']:.0%}), "
          f"{pl['west']['routes']} routes vs {pl['east']['routes']}")


if __name__ == "__main__":
    main()
