"""
scripts/flixbus_gtfs.py
=======================
FlixBus's own timetable, in place of the copy in the BODS bundle.

Called by scripts/build_timetable.py after the BODS bundle is parsed.

Why FlixBus's rather than BODS's:

  * FlixBus's live feeds (api/flixbus.py) name each journey by FlixBus's own
    trip id. Only FlixBus's timetable carries those ids; BODS republishes the
    same coaches under ids of its own, which nothing live ever mentions.
  * BODS publishes every FlixBus journey six to eight times
    (tests/test_coach_services.py). FlixBus publishes each once.

Published by FlixBus France on France's national access point,
transport.data.gouv.fr, dataset "Réseau européen FlixBus et FlixTrain", under
the Open Database Licence. The file covers FlixBus's whole European network,
Great Britain included: every trip in FlixBus's separate GB file is in it under
the same id. The site credits FlixBus and the licence wherever it lists its
sources (tests/test_data_credits.py).

**FlixBus's times are UTC.** Its agency.txt says agency_timezone UTC, and it
means it: on the 25 October 2026 file the 07:00 from Brighton is 07:00 in
winter and 06:00 from 28 March 2027, while BODS has the same coach at 07:00
all year. Every time is moved to UK time here, using the offset in force when
each journey sets off on each date it runs. A journey whose dates straddle a
clock change is split in two, the second half taking an id ending "@1" and
api/flixbus.py choosing between them by date. FlixBus avoids that itself (its
summer 07:00 is a different trip, at 06:00 UTC), so the split is a safeguard.

**Stops.** FlixBus names stops by its own ids. Each becomes the stop the site
already shows: the stop BODS's own copy of FlixBus uses, if one is within
300 m, or else the nearest stop within 60 m. At Gatwick North the coach bay
BODS uses is 132 m from FlixBus's point and a local bus stop only 20 m, so the
order matters. The pairs are kept (`stop_aliases`), because the live feeds
name stops by FlixBus's ids too.
"""

import csv
import io
import logging
import math
import zipfile
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

log = logging.getLogger("build_timetable")

FLIXBUS_GTFS_URL = "https://gtfs.gis.flix.tech/gtfs_generic_eu.zip"
NOC = "FLIX"
# Route, service and shape ids get this prefix: BODS's are short numbers, and
# FlixBus has a route called "700". Trip ids keep FlixBus's own, unprefixed,
# because they are what the live feeds name.
ID_PREFIX = "FLIX:"
COACH_STOP_METRES = 300
ANY_STOP_METRES = 60
LONDON = ZoneInfo("Europe/London")
WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


def merge(timetable: dict, zip_path: str, shape_box=None) -> dict:
    """Put FlixBus's journeys here into `timetable`, replacing BODS's copy.

    `shape_box` is `(min_lat, max_lat, min_lon, max_lon)`: a shape is kept,
    whole, if any point of it is inside, as the BODS shapes are.

    Returns counts. With no FlixBus journey calling at any of our stops,
    nothing changes and BODS's copy stays: a smaller network is not a reason to
    lose the coaches we had.
    """
    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        stops = {r["stop_id"]: r for r in _rows(zf, "stops.txt")}
        aliases = map_stops(stops, timetable)

        first: dict = {}     # trip -> (seq, departure), its first call anywhere
        last: dict = {}      # trip -> (seq, stop), its last call anywhere
        calls: dict = {}     # trip -> [(seq, our stop, departure)], ours only
        for r in _rows(zf, "stop_times.txt"):
            trip = r["trip_id"]
            seq = int(r["stop_sequence"])
            secs = _secs(r.get("departure_time") or r.get("arrival_time") or "")
            if secs is None:
                continue
            if trip not in first or seq < first[trip][0]:
                first[trip] = (seq, secs)
            if trip not in last or seq > last[trip][0]:
                last[trip] = (seq, r["stop_id"])
            ours = aliases.get(r["stop_id"])
            if ours:
                calls.setdefault(trip, []).append((seq, ours, secs))

        if not calls:
            log.warning("FlixBus: no journey calls at any of our stops; keeping BODS's copy")
            return {"trips": 0, "stops": len(aliases)}

        trips = {r["trip_id"]: r for r in _rows(zf, "trips.txt") if r["trip_id"] in calls}
        routes = {r["route_id"]: r for r in _rows(zf, "routes.txt")}
        services = {t["service_id"] for t in trips.values()}
        calendar = {r["service_id"]: r for r in _rows(zf, "calendar.txt")
                    if r["service_id"] in services} if "calendar.txt" in names else {}
        exceptions: dict = {}
        if "calendar_dates.txt" in names:
            for r in _rows(zf, "calendar_dates.txt"):
                if r["service_id"] in services:
                    exceptions.setdefault(r["service_id"], {})[r["date"]] = (
                        r.get("exception_type") == "1")
        wanted_shapes = {t.get("shape_id") for t in trips.values()} - {"", None}
        shapes = (_shapes(zf, wanted_shapes, shape_box)
                  if "shapes.txt" in names and wanted_shapes else {})

    # Everything is worked out before anything is touched, so a file that
    # fails halfway cannot leave the build with neither copy of FlixBus.
    add = {"routes": {}, "trips": {}, "calendar": {}, "calendar_dates": {}}
    add_calls = []
    kept = split = 0
    # Trip ids stay FlixBus's own, unprefixed, so the live feeds can name them;
    # one that is already another operator's journey is not ours to replace.
    others = {tid for tid, t in timetable["trips"].items()
              if (timetable["routes"].get(t.get("route_id")) or {}).get("noc") != NOC}
    clashes = trips.keys() & others
    if clashes:
        log.warning("FlixBus: %d journeys share an id with another operator's and were "
                    "left out, e.g. %s", len(clashes), min(clashes))
    for trip_id, trip in trips.items():
        if trip_id in clashes:
            continue
        route = routes.get(trip["route_id"]) or {}
        sid = trip["service_id"]
        days = service_dates(calendar.get(sid), exceptions.get(sid, {}))
        if not days:
            continue
        groups = _by_offset(days, first[trip_id][1])
        route_id = ID_PREFIX + trip["route_id"]
        add["routes"][route_id] = {
            "short_name": short_name(route, trip["route_id"]),
            "long_name": route.get("route_long_name") or "",
            "noc": NOC,
        }
        end_stop = (stops.get(last[trip_id][1]) or {}).get("stop_name") or ""
        shape = trip.get("shape_id") or ""
        for i, (offset, group) in enumerate(groups):
            our_trip = trip_id if i == 0 else f"{trip_id}@{i}"
            if len(groups) == 1:
                our_service = ID_PREFIX + sid
                _copy_calendar(add, our_service, calendar.get(sid), exceptions.get(sid, {}))
            else:
                our_service = f"{ID_PREFIX}{sid}@{i}"
                add["calendar_dates"][our_service] = {
                    d.strftime("%Y%m%d"): "1" for d in group}
            add["trips"][our_trip] = {
                "route_id": route_id,
                "service_id": our_service,
                # FlixBus leaves trip_headsign empty; the coach is going to
                # its last stop, wherever that is.
                "headsign": end_stop,
                "shape_id": ID_PREFIX + shape if shape in shapes else "",
            }
            add_calls.extend((stop, (secs + offset, our_trip, seq, None))
                             for seq, stop, secs in calls[trip_id])
        kept += 1
        split += len(groups) > 1

    if not kept:
        log.warning("FlixBus: no journey here runs on any date; keeping BODS's copy")
        return {"trips": 0, "stops": len(aliases)}

    _drop_operator(timetable, NOC)
    for table, rows in add.items():
        timetable[table].update(rows)
    for stop, entry in add_calls:
        timetable["stop_times"].setdefault(stop, []).append(entry)
    for shape_id, points in shapes.items():
        timetable["shapes"][ID_PREFIX + shape_id] = points
    timetable.setdefault("stop_aliases", {}).update(aliases)
    for stop in set(aliases.values()):
        if stop in timetable["stop_times"]:
            timetable["stop_times"][stop].sort(key=lambda t: t[0])

    log.info("FlixBus: %d journeys at %d stops (%d split at a clock change), %d shapes",
             kept, len(aliases), split, len(shapes))
    return {"trips": kept, "stops": len(aliases), "split": split, "shapes": len(shapes)}


def map_stops(flix_stops: dict, timetable: dict) -> dict:
    """`{FlixBus stop id: our stop id}` for FlixBus's stops in our area."""
    ours = timetable["stops"]
    if not ours:
        return {}
    coach = _stops_served_by(timetable, NOC)
    lats = [s["lat"] for s in ours.values()]
    lons = [s["lon"] for s in ours.values()]
    # Anything outside our stops' own box by more than the largest distance
    # allowed cannot match, and skipping it saves comparing thousands of
    # European stops against thousands of ours.
    pad = 0.01
    lat_lo, lat_hi, lon_lo, lon_hi = min(lats) - pad, max(lats) + pad, min(lons) - pad, max(lons) + pad
    out = {}
    for fid, row in flix_stops.items():
        try:
            lat, lon = float(row["stop_lat"]), float(row["stop_lon"])
        except (KeyError, TypeError, ValueError):
            continue
        if not (lat_lo <= lat <= lat_hi and lon_lo <= lon <= lon_hi):
            continue
        best = _nearest(lat, lon, ((sid, ours[sid]) for sid in coach if sid in ours),
                        COACH_STOP_METRES)
        if best is None:
            best = _nearest(lat, lon, ours.items(), ANY_STOP_METRES)
        if best is not None:
            out[fid] = best
    return out


def service_dates(cal, exceptions: dict) -> list:
    """Every date a GTFS service runs, from its calendar row and exceptions."""
    days = set()
    if cal:
        day = _date(cal["start_date"])
        end = _date(cal["end_date"])
        while day <= end:
            if str(cal.get(WEEKDAYS[day.weekday()], "0")) == "1":
                days.add(day)
            day += timedelta(days=1)
    for stamp, added in exceptions.items():
        if added:
            days.add(_date(stamp))
        else:
            days.discard(_date(stamp))
    return sorted(days)


def short_name(route: dict, route_id: str) -> str:
    """"FlixBus UK066" is the line UK066, as BODS and the site have called it."""
    name = (route.get("route_short_name") or "").strip()
    if name.lower().startswith("flixbus "):
        name = name[len("flixbus "):].strip()
    return name or route_id


def _by_offset(days: list, first_secs: int) -> list:
    """`[(UK offset in seconds, [dates])]`, earliest first.

    The offset is the one in force when the journey sets off on that date:
    a coach at 00:30 UTC on 28 March 2027 leaves before the clocks change.
    """
    groups: dict = {}
    for day in days:
        moment = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(
            seconds=first_secs)
        offset = int(moment.astimezone(LONDON).utcoffset().total_seconds())
        groups.setdefault(offset, []).append(day)
    return sorted(groups.items(), key=lambda kv: kv[1][0])


def _copy_calendar(timetable: dict, service: str, cal, exceptions: dict) -> None:
    if cal:
        timetable["calendar"][service] = {
            **{day: str(cal.get(day, "0")) for day in WEEKDAYS},
            "start_date": cal["start_date"],
            "end_date": cal["end_date"],
        }
    if exceptions:
        # The build's own encoding: "1" added, "0" removed.
        timetable["calendar_dates"][service] = {
            stamp: "1" if added else "0" for stamp, added in exceptions.items()}


def _drop_operator(timetable: dict, noc: str) -> None:
    """Remove one operator's routes, trips, calls, and what only they used."""
    routes = {rid for rid, r in timetable["routes"].items() if r.get("noc") == noc}
    if not routes:
        return
    trips = {tid for tid, t in timetable["trips"].items() if t.get("route_id") in routes}
    for rid in routes:
        del timetable["routes"][rid]
    for stop in list(timetable["stop_times"]):
        kept = [e for e in timetable["stop_times"][stop] if e[1] not in trips]
        if kept:
            timetable["stop_times"][stop] = kept
        else:
            del timetable["stop_times"][stop]
    gone = {tid: timetable["trips"].pop(tid) for tid in trips}
    services = {t.get("service_id") for t in timetable["trips"].values()}
    shapes = {t.get("shape_id") for t in timetable["trips"].values()}
    for t in gone.values():
        if t.get("service_id") not in services:
            timetable["calendar"].pop(t.get("service_id"), None)
            timetable["calendar_dates"].pop(t.get("service_id"), None)
        if t.get("shape_id") and t["shape_id"] not in shapes:
            timetable["shapes"].pop(t["shape_id"], None)
    log.info("FlixBus: removed BODS's copy, %d trips on %d routes", len(trips), len(routes))


def _stops_served_by(timetable: dict, noc: str) -> set:
    routes = {rid for rid, r in timetable["routes"].items() if r.get("noc") == noc}
    trips = {tid for tid, t in timetable["trips"].items() if t.get("route_id") in routes}
    return {stop for stop, entries in timetable["stop_times"].items()
            if any(e[1] in trips for e in entries)}


def _nearest(lat: float, lon: float, candidates, limit_m: float):
    best, best_m = None, None
    for sid, stop in candidates:
        m = _metres(lat, lon, stop["lat"], stop["lon"])
        if m <= limit_m and (best_m is None or m < best_m):
            best, best_m = sid, m
    return best


def _metres(lat1, lon1, lat2, lon2) -> float:
    dlat = (lat2 - lat1) * 111_195
    dlon = (lon2 - lon1) * 111_195 * math.cos(math.radians((lat1 + lat2) / 2))
    return math.hypot(dlat, dlon)


def _shapes(zf, wanted: set, box) -> dict:
    points: dict = {}
    for r in _rows(zf, "shapes.txt"):
        if r["shape_id"] not in wanted:
            continue
        try:
            points.setdefault(r["shape_id"], []).append(
                (int(r["shape_pt_sequence"]), float(r["shape_pt_lat"]), float(r["shape_pt_lon"])))
        except (TypeError, ValueError):
            continue
    out = {}
    for shape_id, pts in points.items():
        if len(pts) < 2:
            continue
        if box and not any(box[0] <= lat <= box[1] and box[2] <= lon <= box[3]
                           for _s, lat, lon in pts):
            continue
        pts.sort()
        out[shape_id] = [[lat, lon] for _s, lat, lon in pts]
    return out


def _rows(zf, name: str):
    with zf.open(name) as f:
        yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))


def _secs(hms: str):
    parts = hms.strip().split(":")
    if len(parts) != 3:
        return None
    try:
        h, m, s = (int(p) for p in parts)
    except ValueError:
        return None
    return h * 3600 + m * 60 + s


def _date(stamp: str) -> date:
    return date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:8]))
