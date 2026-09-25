"""Derive stop-departure evidence from recorded SIRI-VM and GTFS-RT.

See METHOD/CAVEATS for the versioned method. Times retain their source report,
service-day origin and UTC instant. Interpolated calls are explicitly marked;
missing data and uncertain matching must not become precise hotspot claims.
"""

import argparse
from importlib import metadata
import json
import struct
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

LONDON = ZoneInfo("Europe/London")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from api import gtfs_rt                      # noqa: E402
import reliability_stats                      # noqa: E402
from api import trip_match                    # noqa: E402
from api.timetable_db import Timetable        # noqa: E402
from observation_contract import TIME_BASIS, digest_file, route_pattern_id, service_origin
from recorded_inputs import recorded_stream

SIRI_NS = "http://www.siri.org.uk/siri"

# The A259 corridor this project started from. Six poles, kept for
# --corridor-only, which reproduces the original figures.
CORRIDOR_ATCOS = (
    "4400AD0330", "4400AD0203", "4400AD0063",   # towards Worthing
    "4400AD0064", "4400AD0204", "4400AD0329",   # towards Brighton
)

# What is recorded, from worker/src/recorder.js: Adur, Worthing and the
# Brighton coast. Every journey calling inside it is measured, not just those
# on the corridor — a stop-pair journey-time tool that only works along one
# road is not much of a tool, and the recording costs the same either way.
BOX_MIN_LAT, BOX_MAX_LAT = 50.78, 50.87
BOX_MIN_LON, BOX_MAX_LON = -0.42, -0.10


def stops_in_box(tt):
    """Every stop inside the recorded box, as ATCO codes."""
    return [atco for atco, stop in tt.stops.items()
            if stop.get("lat") is not None
            and BOX_MIN_LAT <= stop["lat"] <= BOX_MAX_LAT
            and BOX_MIN_LON <= stop["lon"] <= BOX_MAX_LON]

# How near a bus must come to a stop for its nearest approach to count as an
# arrival. Generous enough for GPS scatter and a stop set back from the road;
# tight enough that a bus on a parallel street does not qualify.
ARRIVAL_RADIUS_M = 150

# A journey seen fewer times than this is not evidence of anything.
MIN_SAMPLES = 3

# A bus this close to a stop is at it, even in the first or last snapshot of
# its track. Without the exception the first and last stop of every journey
# that starts or ends outside recording hours would be thrown away.
AT_THE_STOP_M = 40

# A position the feed has been repeating for this long is not where the bus is.
VEHICLE_STALE_SECS = 10 * 60

# Only journeys plausibly on the road at the moment of a snapshot are
# considered, which is what keeps a day's processing to minutes rather than
# hours. Asymmetric, like the matcher: buses run late far more than early.
BEFORE_FIRST_SECS = 15 * 60
AFTER_LAST_SECS = 30 * 60

# How the figures were produced, as a number that changes when the answer would.
#
# `data_version` already names the timetable build, which says what a figure was
# measured *against*. It says nothing about how it was measured, so a figure
# from before the arrival picker was made monotonic is indistinguishable from
# one after it — and those two answers differ by up to an hour on the journeys
# that were wrong. A reader comparing two months of published data has no way to
# know which side of the fix each came from.
#
# Raise this whenever a change would move a published number:
#   1  the original method
#   2  arrivals timed by departure rather than nearest approach
#   3  arrivals must advance along the route; interpolated stops carry their own
#      timing-point flag; declared journeys matched outside the active window;
#      estimates excluded from every statistic
#   4  report-level evidence from both recorded feeds; a declared journey must
#      agree with its declared start
#   5  a declared start is read as local time or as a UTC instant. Stagecoach,
#      Metrobus and Compass publish UTC, and read only as local every one of
#      their declared buses was discarded: the 700 was measured on 12 journeys
#      on 24 September 2026, against 173 on the 22nd
METHOD_VERSION = 5

METHOD = (
    "Recorded SIRI-VM and GTFS-RT positions are evaluated at their report timestamps. "
    "GTFS-RT trip identity is preferred; a declared start date and time must match the "
    "journey's scheduled first departure, read as local time or as a UTC instant. "
    "Inference uses route, position, heading and "
    "a -5/+25 minute matching window. Candidate stop visits are aligned to ordered "
    "route calls. A departure estimate uses the last report within 150 m of the "
    "selected visit, with the following report retained as an interval bound where "
    "available. UTC instants and GTFS service-day elapsed seconds are retained. "
    "Missing calls are interpolated only between observations and explicitly flagged. "
    "Measured statistics exclude interpolation. Unobserved journeys are missing "
    "coverage, never on-time journeys or confirmed cancellations. Coaches are excluded."
)

CAVEATS = [
    "Report spacing and a 150 m stop radius limit timing and spatial precision. "
    "The report interval bounds a radius departure, not an exact door-closing time; "
    "unbounded, ambiguous and missing-timestamp observations are flagged.",
    "Inferred identity can confuse a bus more than half a headway late with the "
    "next journey. Very late running may be understated: the reported tail can be a "
    "floor. Wrong trip or visit assignments can also move individual delay gains "
    "in either direction. The inference window does not censor declared matches.",
    "Inferred matches are censored by the matching window; tail counts may be a "
    "floor and on-time shares optimistic. Declared matches are not window-censored.",
    "The last report may be up to 150 m past the pole, overstating lateness; "
    "sparse reports before departure may understate it. Neither bias has a "
    "universal seconds allowance. Inspect the retained report interval.",
    "A declared label describes the selected report's identity, not a guarantee "
    "of correct stop timing. Mixed identities and ambiguous visits remain visible.",
    "A bus whose declared start matches its journey in neither local time nor UTC "
    "(a garage departure the timetable does not hold, say) is neither certified nor "
    "inferred, so its journey counts as missing coverage. Coverage is understated, "
    "never inflated; coverage.declared_start counts both readings and the rejections.",
    "A journey that ran without reporting its position is indistinguishable "
    "from one that did not run. Neither produces an observation, so coverage "
    "is published beside every figure and absence is never counted as lateness.",
    "Every journey calling at a stop inside the recorded area is measured "
    "(Adur, Worthing and the Brighton coast). Journeys that only touch the "
    "area outside it are not, and a journey is measured only where it is "
    "inside the box.",
    "Lateness at a stop GTFS interpolated between two timing points is partly "
    "a measure of that interpolation, not of the service. Arrivals are "
    "therefore reported separately for the operator's own timing points, for "
    "interpolated stops, and for feeds that do not say which is which.",
    "Section duration includes stop dwell and holding. Delay gained between two "
    "observed timing points does not establish which road or traffic condition caused it.",
]


def _reported_at(iso_text):
    """A feed timestamp as an absolute moment, or None.

    Absolute, not seconds-from-midnight: the snapshot key is London time and
    the feed publishes UTC, so comparing the two as clock readings makes every
    record look an hour fresh in summer — which is exactly how a position the
    feed had been repeating for 35 minutes was first taken as current.
    """
    try:
        dt = datetime.fromisoformat(iso_text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def parse_snapshot(xml_text, at_utc=None):
    """The vehicles in one snapshot, in the shape `trip_match` expects.

    Unlike the API's parser this does not drop vehicles for being stale against
    the wall clock — every record here is old by definition. Records stale
    *relative to their own snapshot* are dropped instead, because the feed
    repeats a vehicle's last known position long after it has finished.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    ns = {"s": SIRI_NS}
    out = []
    for activity in root.findall(".//s:VehicleActivity", ns):
        journey = activity.find("s:MonitoredVehicleJourney", ns)
        if journey is None:
            continue

        def jtext(tag):
            el = journey.find(f"s:{tag}", ns)
            return el.text.strip() if el is not None and el.text else ""

        loc = journey.find("s:VehicleLocation", ns)
        if loc is None:
            continue
        try:
            lat = float(loc.find("s:Latitude", ns).text)
            lon = float(loc.find("s:Longitude", ns).text)
        except (TypeError, ValueError, AttributeError):
            continue

        rec = activity.find("s:RecordedAtTime", ns)
        recorded_at = rec.text.strip() if rec is not None and rec.text else ""
        reported_at = _reported_at(recorded_at) if recorded_at else None
        if at_utc is not None and reported_at is not None:
            if not -60 <= (at_utc - reported_at).total_seconds() <= VEHICLE_STALE_SECS:
                continue

        bearing = jtext("Bearing")
        try:
            bearing = float(bearing) if bearing else None
        except ValueError:
            bearing = None

        out.append({
            "vehicle_ref": jtext("VehicleRef"),
            "service_ref": jtext("PublishedLineName") or jtext("LineRef"),
            "operator_ref": jtext("OperatorRef"),
            "destination": jtext("DestinationName"),
            "latitude": lat,
            "longitude": lon,
            "bearing": bearing,
            "recorded_at": recorded_at,
            "recorded_epoch": int(reported_at.timestamp()) if reported_at else None,
            # When the operator says the bus was there, in seconds from London
            # midnight. None when the feed omits it, and then the fetch time
            # has to stand in.
            "recorded_secs": _local_secs(reported_at),
        })
    return out


def _local_secs(when):
    """An absolute moment as seconds from London midnight, or None."""
    if when is None:
        return None
    local = when.astimezone(LONDON)
    return local.hour * 3600 + local.minute * 60 + local.second


def _same_day(rec_secs, at_secs):
    """`rec_secs` moved onto the snapshot's day.

    A report at 23:58 read in the 00:10 snapshot is twelve hours from it by
    clock arithmetic and two minutes from it in fact. Both directions matter:
    the recording window runs to 00:30.
    """
    if rec_secs is None:
        return None
    if rec_secs - at_secs > 43_200:
        return rec_secs - 86_400
    if at_secs - rec_secs > 43_200:
        return rec_secs + 86_400
    return rec_secs


def declared_journeys(path):
    """`{vehicle id: trip id}` from one minute of GTFS-RT.

    The feed states which journey each bus is running. SIRI-VM does not — its
    journey reference matched 0 of 256 timetable trips — so this is the one
    thing the second feed is for.
    """
    try:
        _header, vehicles = gtfs_rt.parse_feed(Path(path).read_bytes())
    except (OSError, ValueError, IndexError, struct.error) as err:
        print(f"unreadable GTFS-RT {path}: {err}", file=sys.stderr)
        return {}
    return {v["vehicle_id"]: v["trip_id"]
            for v in vehicles if v.get("vehicle_id") and v.get("trip_id")}


def gtfs_rt_files(directory):
    """`(seconds from midnight, path)` for every `HHMM.pb`, in time order."""
    found = []
    for path in sorted(Path(directory).glob("*.pb")):
        name = path.stem
        if "-" in name and name.split("-", 1)[1].isdigit():
            found.append((int(name.split("-", 1)[1]), path))
            continue
        if len(name) != 4 or not name.isdigit():
            continue
        found.append((int(name[:2]) * 3600 + int(name[2:]) * 60, path))
    return sorted(found)


def snapshot_files(directory):
    """`(seconds from midnight, path)` for every `HHMM.xml`, in time order."""
    found = []
    for path in sorted(Path(directory).glob("*.xml")):
        name = path.stem
        if "-" in name and name.split("-", 1)[1].isdigit():
            found.append((int(name.split("-", 1)[1]), path))
            continue
        if len(name) != 4 or not name.isdigit():
            continue
        found.append((int(name[:2]) * 3600 + int(name[2:]) * 60, path))
    return sorted(found)


def arrivals_along(samples, calls, stops, diagnostics=None):
    """Align bounded candidate visits to the complete ordered route.

    Maximise supported calls before minimising spatial distance. Unlike a
    greedy closest first point, a later return cannot discard an earlier run.
    Equal-support alternative visits remain flagged as ambiguous evidence.
    The beam bounds work for long circular routes; it is not ground truth.
    """
    if not samples:
        return
    # (matched calls, distance cost, last approach, last departure, path)
    states = [(0, 0, 0, -1, ())]
    for stop_index, (scheduled, atco) in enumerate(calls):
        stop = stops.get(atco) or {}
        if stop.get("lat") is None:
            continue
        distances = [trip_match.km(s[1], s[2], stop["lat"], stop["lon"]) * 1000
                     for s in samples]
        candidates, i = [], 0
        while i < len(samples):
            if distances[i] > ARRIVAL_RADIUS_M:
                i += 1
                continue
            begin = i
            while i + 1 < len(samples) and distances[i + 1] <= ARRIVAL_RADIUS_M:
                i += 1
            end = i
            nearest = min(range(begin, end + 1), key=distances.__getitem__)
            metres = round(distances[nearest])
            if not ((nearest == 0 or end == len(samples) - 1) and metres > AT_THE_STOP_M):
                candidates.append((nearest, end, metres))
            i += 1
        next_states = list(states)       # missing calls stay missing
        for count, cost, floor, left, path in states:
            for nearest, end, metres in candidates:
                if nearest < floor or end < left:
                    continue
                if nearest == floor and metres > AT_THE_STOP_M:
                    continue
                # A repeated ATCO in a loop requires a distinct visit.
                if any(c[2] == atco and c[3] == end for c in path):
                    continue
                call = (stop_index, scheduled, atco, end, metres)
                next_states.append((count + 1, cost + metres, nearest, end, path + (call,)))
        states = sorted(next_states, key=lambda x: (-x[0], x[1], x[3]))[:64]
    best = states[0]
    if diagnostics is not None:
        alternatives = [x for x in states[1:] if x[0] == best[0] and x[1] <= best[1] + 20
                        and tuple(c[3] for c in x[4]) != tuple(c[3] for c in best[4])]
        if alternatives:
            diagnostics.append("ambiguous_visit_alignment")
    yield from best[4]


def advancing_only(arrivals):
    """Drop any arrival whose *departure* precedes the one before it.

    `arrivals_along` constrains which sample may time each stop, which fixes
    the large errors — a journey timing an early stop from a later pass. It
    does not quite finish the job, because a stop is timed by when the bus
    *left* it, the last sample within 150 m: a brief pass at one stop can end
    its run before a long dwell at the stop before it ends, and the published
    times then run backwards although the approaches did not.

    Measured after the nearest-approach fix: 2 pairs of 1,049 journeys on
    16 September still inverted, the worst by 2.1 minutes. Two is not many, and
    2.1 minutes is small enough to read as a real figure on a chart rather than
    an error — which is exactly why it is refused rather than tolerated.

    Separate from the geometry because it is a separate claim. The arrangement
    of stops and samples needed to provoke the case is contrived enough that no
    fixture built from real stop spacings reproduces it, so the search cannot
    be tested for this; the rule can, and is.
    """
    left_at = -1
    for arrival in arrivals:
        if arrival[3] < left_at:
            continue
        left_at = arrival[3]
        yield arrival


def journey_direction(tt, calls):
    """Which way along the coast a journey runs, from its first and last stop.

    Shared by the observations and the recorded timetable, so a scheduled
    journey and a measured one cannot be filed under different directions.
    """
    first_stop = tt.stops.get(calls[0][1]) or {}
    last_stop = tt.stops.get(calls[-1][1]) or {}
    if first_stop.get("lon") is not None and last_stop.get("lon") is not None:
        shift = last_stop["lon"] - first_stop["lon"]
        if abs(shift) >= 0.002:          # ~140 m, more than one pole apart
            return "westbound" if shift < 0 else "eastbound"
    return "unknown"


# What the recorded timetable is, stated in the file so it reads on its own.
SCHEDULE_NOTE = (
    "Every journey the timetable scheduled on this service day at stops in the "
    "recording area, coaches excluded, whether or not a bus was ever seen. A "
    "call's scheduled time is start_secs + offsets[i] seconds from the GTFS "
    "service-day origin (noon minus twelve hours, London). timepoints[i] is 1 "
    "where the operator commits to the time and 0 where GTFS interpolated it.")


def record_schedule(tt, day, atcos=None):
    """The day's timetable, kept beside what the buses actually did.

    The comparison between a measured journey and its promise is only as
    durable as the promise. Each observation row already carries its own
    scheduled time, but only for journeys a bus was seen making: the timetable
    itself was enumerated here every night and thrown away, keeping counts. So
    no chart could draw the timetable across the day, a scheduled bus that was
    never seen left no trace, and the weekly rebuild replaced the only copy.

    Recorded per service day, not per processing window. The window reaches two
    hours either side of the day to catch journeys crossing midnight, so it
    also holds the neighbouring days' late and early trips; keeping only this
    day's instances files every journey exactly once across the archive.

    Deduplicated rather than listed: most journeys on a pattern share a
    run-time profile, so each trip names its pattern and profile and carries
    only its start time and headsign.
    """
    atcos = stops_in_box(tt) if atcos is None else list(atcos)
    # Wider than the matching window, deliberately. Matching looks two hours
    # past the day because that is when buses are on the road to be seen; a
    # timetable can schedule a service day's journey as late as 47:59, and one
    # that first calls at 28:40 falls outside the matching window on its own
    # day and is filed under the neighbouring day's window, so a record taken
    # from that window would lose it on both nights.
    instances, _scheduled = trip_match.build_instances(tt, day, atcos, (-7200, 48 * 3600))
    patterns, profiles, profile_index, trips = {}, [], {}, []
    for (trip_id, service_day), inst in sorted(instances.items(),
                                               key=lambda item: (item[0][0], item[0][1])):
        if service_day != day:
            continue
        # The same calls, in the same order and with the same filter, that the
        # observations are built from; anything else would hash differently.
        calls = [(secs, atco) for secs, atco in tt.trip_stops_for(trip_id)
                 if secs is not None]
        if not calls:
            continue
        timepoints = dict(enumerate(tt.timepoints_by_call(trip_id)))
        route_id = inst["trip"].get("route_id", "")
        pattern = route_pattern_id(route_id, [atco for _secs, atco in calls])
        if pattern not in patterns:
            patterns[pattern] = {
                "route_id": route_id,
                "operator": tt.noc_for_route(route_id),
                "service": inst["service"],
                "direction": journey_direction(tt, calls),
                "atcos": [atco for _secs, atco in calls],
            }
        start = calls[0][0]
        profile = ([secs - start for secs, _atco in calls],
                   [1 if timepoints.get(i) == 1 else 0 for i in range(len(calls))])
        key = (tuple(profile[0]), tuple(profile[1]))
        if key not in profile_index:
            profile_index[key] = len(profiles)
            profiles.append({"offsets": profile[0], "timepoints": profile[1]})
        trips.append([trip_id, pattern, profile_index[key], start,
                      (inst["trip"].get("headsign") or "").strip()])
    trips.sort(key=lambda t: (t[3], t[1], t[0]))
    return {
        "schema_version": 1,
        "day": day.isoformat(),
        "time_basis": TIME_BASIS,
        "note": SCHEDULE_NOTE,
        "trip_format": ["trip_id", "route_pattern", "profile", "start_secs", "headsign"],
        "patterns": patterns,
        "profiles": profiles,
        "trips": trips,
        "counts": {"trips": len(trips), "patterns": len(patterns), "profiles": len(profiles)},
    }


def observe_day(tt, day, snapshots, atcos=None):
    """Arrival observations for one day.

    `snapshots` is an iterable of `(seconds from midnight, vehicles)`. Returns
    `(observations, coverage)`. Coverage counts scheduled journeys against
    those ever tracked — the denominator every figure derived from this needs.
    """
    atcos = stops_in_box(tt) if atcos is None else list(atcos)
    instances, _scheduled = trip_match.build_instances(
        tt, day, atcos, (-7200, 93600))
    origin = service_origin(day)
    for (_trip, service_day), inst in instances.items():
        shift = origin - service_origin(service_day)
        inst["calls"] = [(secs + inst["shift"] - shift, atco) for secs, atco in inst["calls"]]
        inst["shift"] = shift

    # Where each journey's bus was, every time it reported.
    tracks = {key: [] for key in instances}
    declared_journeys_seen = set()   # journeys the feed named rather than us
    # How each declared journey's start was read, and the declarations whose
    # start matched no journey at all — the size of the UTC problem, and of
    # what is still refused, so neither has to be taken on trust.
    declared_start_readings = {}
    contradicted = set()
    seen = set()            # (journey, vehicle, report time): the feed repeats
    report_details = {}
    repeated_reports = 0
    matched_reports = 0
    matched_by_feed = {"siri": 0, "gtfs_rt": 0, "unknown": 0}
    matched_by_feed_hour = {}
    window_times = []
    for at_secs, vehicles in snapshots:
        window_times.append((at_secs, None))
        # Only journeys plausibly running now: without this a day of snapshots
        # is matched against a day of journeys, which is hours of work.
        active = {k: i for k, i in instances.items()
                  if i["calls"]
                  and i["calls"][0][0] - BEFORE_FIRST_SECS <= at_secs + 60
                  <= i["calls"][-1][0] + AFTER_LAST_SECS + VEHICLE_STALE_SECS}
        # Nothing to match. This was "if not active" — abandoning the whole
        # snapshot when no journey was plausibly running — which is the same
        # window by another route, and incoherent now that declared journeys
        # are matched against every instance. No test here distinguishes the
        # two: the corridor fixture always has a journey in window, so the
        # case needs a timetable whose day has genuinely ended.
        if not vehicles:
            continue
        # Each position is judged at the moment it was recorded, not when it
        # was read. Matching on the fetch time shifts every bus about three
        # minutes down the timetable and onto the following journey, which
        # reads as a bus running early. Measured: a bus three minutes late,
        # reported four minutes behind, came out as 7 minutes early.
        when_of = [(v["recorded_epoch"] - origin if v.get("recorded_epoch") is not None
                    else _same_day(v.get("recorded_secs"), at_secs)) for v in vehicles]
        when_of = [at_secs if w is None else w for w in when_of]

        # What the feed states, before anything we infer — against *every*
        # journey, not the active window.
        #
        # The window exists so the inference path does not compare a day of
        # snapshots against a day of journeys, and it is 15 minutes early to 30
        # late. A declared journey needs no such tolerance: the operator has
        # named the trip. Passing the window here censored exactly the buses
        # worth reporting — one 40 minutes down is outside it, so the feed named
        # its journey and we threw the answer away, leaving the journey counted
        # as missing coverage instead of very late. It is a dict lookup by trip
        # id, so the wider search costs nothing.
        declared, claimed = trip_match.place_declared(tt, vehicles, instances, at_secs)
        for key in declared:
            declared_journeys_seen.add(key)
            reading = vehicles[declared[key][0]].get("declared_start") or "unstated"
            declared_start_readings.setdefault(key, reading)
        for v in vehicles:
            if v.get("declared_start") == "contradicts":
                contradicted.add((v.get("trip_id"), v.get("start_date"), v.get("start_time")))
        inferred = trip_match.place_vehicles(
            tt, vehicles, active, at_secs, times=when_of,
            skip_vehicles=claimed, skip_journeys=set(declared))
        for key, (vi, _idx) in {**declared, **inferred}.items():
            v = vehicles[vi]
            when = when_of[vi]
            ref = v.get("vehicle_ref", "")
            if (key, ref, when) in seen:
                repeated_reports += 1
                continue        # the same report, fetched again a minute later
            seen.add((key, ref, when))
            tracks[key].append((when, v["latitude"], v["longitude"], ref))
            report_details[(key, ref, when)] = {**v, "match": "declared" if key in declared else "inferred"}
            matched_reports += 1
            feed = (v.get("source_report") or {}).get("feed", "unknown")
            matched_by_feed[feed] = matched_by_feed.get(feed, 0) + 1
            fetched = (v.get("source_report") or {}).get("fetched_epoch", origin + at_secs)
            hour = datetime.fromtimestamp(fetched, LONDON).strftime("%H")
            bucket = matched_by_feed_hour.setdefault(feed, {})
            bucket[hour] = bucket.get(hour, 0) + 1

    # Journeys scheduled to be running while snapshots were actually being
    # taken. Counting the whole day against a part-day of snapshots would
    # understate coverage, which is the figure everything else is judged by.
    seen_from, seen_to = (min(t for t, _ in window_times), max(t for t, _ in window_times)) \
        if window_times else (0, 0)
    in_window = [k for k, i in instances.items()
                 if i["calls"] and i["calls"][0][0] <= seen_to
                 and i["calls"][-1][0] >= seen_from]

    # Reports arrive out of order once they carry their own timestamps.
    for samples in tracks.values():
        samples.sort()
    spans = {k: (i["calls"][0][0], i["calls"][-1][0])
             for k, i in instances.items() if i["calls"]}
    tracks = _resolve_vehicle_journeys(tracks, spans, declared_journeys_seen)

    observations = []
    observed_by_journey = {}     # journey -> [(stop index, scheduled, observed)]
    calls_by_journey = {}        # journey -> {stop index: (scheduled, atco, name)}
    tracked = 0
    for key, samples in tracks.items():
        if len(samples) < MIN_SAMPLES:
            continue
        tracked += 1
        inst = instances[key]
        trip_id, service_day = key
        route_id = inst["trip"].get("route_id", "")
        # Whether this journey's identity came from the feed or from our
        # matching. Only the second is censored by the tolerance window, and a
        # figure that mixes them without saying so is two measurements.
        # Which of this journey's times the operator actually commits to.
        timepoints = dict(enumerate(tt.timepoints_by_call(trip_id)))
        # Which way along the coast, and the operator's own word for where it
        # is going. Both are settled here, not in analysis later: trip ids
        # belong to one timetable build, and joining observations to whatever
        # database happens to be on disk would silently mix two builds.
        calls = inst["calls"]
        direction = journey_direction(tt, calls)
        headsign = (inst["trip"].get("headsign") or "").strip()
        journey_start = calls[0][0]
        route_pattern = route_pattern_id(route_id, [atco for _secs, atco in calls])
        timing_indices = [i for i, (_secs, atco) in enumerate(calls) if timepoints.get(i) == 1]

        # One journey can be matched to more than one vehicle across a day if a
        # bus is swapped; the one seen most often is the journey's bus.
        refs = [s[3] for s in samples if s[3]]
        vehicle = max(set(refs), key=refs.count) if refs else ""
        seen_here = []          # (stop index, scheduled, observed) for this journey
        alignment_flags = []
        arrivals = list(arrivals_along(samples, calls, tt.stops, alignment_flags))
        for stop_index, scheduled_secs, atco, sample_i, metres in advancing_only(arrivals):
            stop = tt.stops.get(atco) or {}
            best = samples[sample_i]
            detail = report_details.get((key, best[3], best[0]), {})
            next_sample = samples[sample_i + 1] if sample_i + 1 < len(samples) else None
            flags = list(alignment_flags) + detail.get("identity_flags", [])
            if detail.get("recorded_epoch") is None:
                flags.append("missing_report_timestamp")
            if next_sample is None or next_sample[3] != best[3]:
                flags.append("unbounded_departure")
                next_sample = None
            next_detail = report_details.get((key, next_sample[3], next_sample[0]), {}) if next_sample else {}
            source_reports = [d[k] for d in (detail, next_detail)
                              for k in ("source_report", "identity_source") if d.get(k)]
            if next_sample and next_sample[0] - best[0] > 180:
                flags.append("wide_departure_interval")
            service_shift = inst["shift"]
            seen_here.append((stop_index, scheduled_secs, best[0]))
            observations.append({
                "day": service_day.isoformat(),
                "trip_id": trip_id,
                "service": inst["service"],
                "operator": tt.noc_for_route(route_id),
                "vehicle": best[3] or vehicle,
                "route_id": route_id,
                "route_pattern": route_pattern,
                "time_basis": TIME_BASIS,
                "scheduled_epoch": origin + scheduled_secs,
                "observed_epoch": origin + best[0],
                "observed_clock_secs": _local_secs(datetime.fromtimestamp(origin + best[0], timezone.utc)),
                "source_reports": source_reports,
                "observed_interval_epoch": [origin + best[0], origin + next_sample[0] if next_sample else None],
                "quality_flags": flags,
                "timing_point_order": timing_indices.index(stop_index) if stop_index in timing_indices else None,
                "atco": atco,
                "stop_name": stop.get("name", ""),
                "scheduled": trip_match.clock(scheduled_secs),
                "scheduled_secs": scheduled_secs + service_shift,
                "observed": trip_match.clock(best[0]),
                "observed_secs": best[0] + service_shift,
                "lateness_secs": best[0] - scheduled_secs,
                "nearest_m": metres,
                "samples": len(samples),
                # 1 a timing point, 0 a time GTFS interpolated, None unstated.
                "timepoint": timepoints.get(stop_index),
                "match": detail.get("match", "inferred"),
                # A real sighting, not a time worked out from its neighbours.
                "estimated": False,
                "direction": direction,
                "headsign": headsign,
                # Where this stop sits in the journey. A stop near the end
                # inherits every minute lost upstream, so any ranking of stops
                # needs to know that rather than discover it as a finding.
                "stop_index": stop_index,
                "calls_total": len(calls),
                # The journey's own identity to a reader: "the 17:22".
                "journey_start": trip_match.clock(journey_start),
                "journey_start_secs": journey_start + service_shift,
            })
        observed_by_journey[key] = seen_here
        # A dict a stop, not a tuple: this grew a field once already and the
        # positional version let a caller unpack three values from four
        # without complaining.
        calls_by_journey[key] = {
            i: {"scheduled_secs": secs, "atco": atco,
                "stop_name": (tt.stops.get(atco) or {}).get("name", ""),
                # This stop's own flag. Interpolated observations used to
                # inherit it from whichever stop happened to be the template,
                # which labelled GTFS's own guesses as operator promises.
                "timepoint": timepoints.get(i)}
            for i, (secs, atco) in enumerate(calls)
            if (tt.stops.get(atco) or {}).get("lat") is not None}

    observations += _fill_gaps(observations, observed_by_journey, calls_by_journey)
    # Interpolation operates in the processing day's coordinates. Canonicalise
    # its new rows and derive their own instants instead of copying the template.
    for row in observations:
        if row.get("estimated"):
            shift = origin - service_origin(row["day"])
            row["scheduled_epoch"] = origin + row["scheduled_secs"]
            row["observed_epoch"] = origin + row["observed_secs"]
            row["scheduled_secs"] += shift
            row["observed_secs"] += shift
            row["observed_clock_secs"] = _local_secs(datetime.fromtimestamp(row["observed_epoch"], timezone.utc))
            row["quality_flags"] = ["interpolated"]
            row["observed_interval_epoch"] = None
            # Endpoint references are retained by _fill_gaps, never passed off as direct sightings.
            row["timing_point_order"] = None

    by_hour, partial, absent = coverage_by_hour(
        _local_secs(datetime.fromtimestamp(origin + t, timezone.utc)) for t, _ in window_times)
    coverage = {
        "snapshots": len(window_times),
        "recorded_from": trip_match.clock(seen_from) if window_times else None,
        "recorded_to": trip_match.clock(seen_to) if window_times else None,
        # Scheduled journeys running while the recorder was actually running.
        "scheduled_journeys": len(in_window),
        "tracked_journeys": tracked,
        "scheduled_journeys_all_day": len(instances),
        "observations": len(observations),
        "matched_reports": matched_reports,
        "matched_reports_by_feed": matched_by_feed,
        "matched_reports_by_feed_hour": matched_by_feed_hour,
        "repeated_reports": repeated_reports,
        # Declared journeys by how their start was read ("unstated": the feed
        # gave none), and distinct declarations refused for naming a start the
        # journey does not have.
        "declared_start": {
            **{basis: sum(1 for r in declared_start_readings.values() if r == basis)
               for basis in ("local", "utc", "unstated")},
            "contradicted_declarations": len(contradicted),
        },
        "measured_observations": sum(not r.get("estimated") for r in observations),
        "quality_flagged_observations": sum(bool(r.get("quality_flags")) for r in observations),
        # Minutes captured in each hour. "snapshots: 900" hides a three-hour
        # hole; twenty-four counts cannot. An hour that is *partly* recorded is
        # the interesting case — recording stopped and started again — so it is
        # named apart from an hour that was never in the window at all.
        "snapshots_by_hour": by_hour,
        "hours_partial": partial,
        "hours_absent": absent,
        "hours_note": ("minutes of feed captured per clock hour, London time. "
                       "An absent hour may be outside the recording window; a "
                       "partial one means recording was interrupted."),
    }
    cohorts = {}
    measured_keys = {(r["trip_id"], r["day"]) for r in observations if not r.get("estimated")}
    window_keys = set(in_window)
    for key, inst in instances.items():
        cohort = (tt.noc_for_route(inst["trip"].get("route_id", "")), inst["service"], key[1].isoformat())
        cell = cohorts.setdefault(cohort, {"operator": cohort[0], "service": cohort[1], "day": cohort[2],
            "scheduled_journeys": 0, "scheduled_in_recording_span": 0, "journeys_with_measured_calls": 0})
        cell["scheduled_journeys"] += 1
        cell["scheduled_in_recording_span"] += key in window_keys
        cell["journeys_with_measured_calls"] += (key[0], key[1].isoformat()) in measured_keys
    coverage["cohorts"] = list(cohorts.values())
    coverage["cohorts_note"] = "Recording span runs from first to last parsed snapshot and may contain gaps; these counts do not establish section-level coverage."
    return observations, coverage


# Below this many minutes an hour is only partly recorded. Two-thirds rather
# than "any missing minute": the feed drops the odd minute under load, and a
# flag that fires most days is one nobody reads.
HOUR_COMPLETE_MINS = 40


def coverage_by_hour(snapshot_secs):
    """`({hour: minutes captured}, partial hours, absent hours)`.

    Every hour of the clock appears, because a zero is the thing worth seeing
    and a missing key is not seen at all.
    """
    minutes = {f"{h:02d}": set() for h in range(24)}
    for secs in snapshot_secs:
        minutes[f"{int(secs) // 3600 % 24:02d}"].add(int(secs) // 60)
    counts = {hour: len(mins) for hour, mins in minutes.items()}
    partial = [h for h, n in sorted(counts.items()) if 0 < n < HOUR_COMPLETE_MINS]
    absent = [h for h, n in sorted(counts.items()) if n == 0]
    return counts, partial, absent


# DfT's yardstick, defined once in reliability_stats so the daily summary and
# any later analysis cannot drift apart on what "on time" means. BUS09 judges
# frequent services (6+ an hour) on excess waiting time instead, which needs
# headways rather than single arrivals — a later figure, and this does not
# claim to be it.
ON_TIME_FROM_SECS = reliability_stats.ON_TIME_FROM_SECS
ON_TIME_TO_SECS = reliability_stats.ON_TIME_TO_SECS
band = reliability_stats.band


def timepoint_class(flag):
    """Whose time an arrival was judged against.

    A bus is "late" only against a time somebody promised. GTFS marks the
    operator's own timing points, and interpolates the stops between them —
    so lateness at an interpolated stop is partly a measure of the
    interpolation. DfT assesses timing points for exactly this reason, and
    these are kept apart rather than averaged into one headline.

    "unstated" is its own answer: a feed that omits the flag entirely is not
    the same as a feed calling every stop a timing point, though the GTFS
    spec would let us read it that way.
    """
    if flag == 1:
        return "timing_point"
    if flag == 0:
        return "interpolated"
    return "unstated"


def summarise(observations, coverage):
    """Counts, with their denominators, small enough to keep for ever.

    Counts only — no percentages. A share computed here would be read without
    its denominator; the site can divide when it has the caveats beside it.
    Nothing is suppressed at this stage either: thin cells are dropped when a
    figure is published, and dropping them now would lose the count that says
    a cell is thin.
    """
    # Estimates are excluded from every punctuality count. An interpolated
    # stop cannot be late: its time is the lateness of the stops either side,
    # divided by the timetable. Counting it would measure our own arithmetic.
    observations = [o for o in observations if not o.get("estimated")]

    bands = {k: 0 for k in ("early", "on_time", "late", "very_late")}
    by_service = {}
    by_hour = {}
    by_timepoint = {}
    for o in observations:
        b = band(o["lateness_secs"])
        bands[b] += 1
        # Named with its operator. The 1, the 5 and the 7 are each run by two
        # companies here, and a punctuality figure under the wrong one is worse
        # than no figure at all.
        svc = by_service.setdefault(reliability_stats.service_key(o),
                                    {k: 0 for k in bands})
        svc[b] += 1
        # Bucketed on the *scheduled* hour. A bus due 17:45 and seen 18:05
        # belongs to the 17:00 timetable; counting it at 18:00 moves delay out
        # of the hour that caused it and flatters the peak.
        hour = reliability_stats.scheduled_hour(o)
        hr = by_hour.setdefault(hour, {k: 0 for k in bands})
        hr[b] += 1
        tp = by_timepoint.setdefault(timepoint_class(o.get("timepoint")),
                                     {k: 0 for k in bands})
        tp[b] += 1
    return {
        "observations": len(observations),
        "measured_only": True,
        "bands": bands,
        "by_service": by_service,
        "by_hour": by_hour,
        "by_hour_basis": "scheduled departure hour, not the hour observed",
        # The headline belongs to "timing_point" where there is one. The other
        # two series are published beside it, never merged into it.
        "by_timepoint": by_timepoint,
        "coverage": coverage,
        "on_time_definition": (
            "DfT BUS09: no more than 1 minute early and no more than "
            "5 minutes 59 seconds late, per observed arrival. Judge a service "
            "by its timing_point figures: at interpolated stops the scheduled "
            "time is GTFS's estimate, not the operator's promise."
        ),
    }


# Two journeys by one bus may abut at a terminus — it arrives on the inbound
# and is matched to the outbound within a minute or two. Overlap up to this is
# a changeover, not a contradiction.
OVERLAP_GRACE_SECS = 180


def _fill_gaps(observations, observed_by_journey, calls_by_journey):
    """Estimate a stop that was missed between two that were not.

    Buses do not report continuously: a stop can fall between two readings,
    or in a gap where the bus sent nothing at all. Dropping those stops leaves
    a journey-time chart full of holes, and Open Innovations' tool interpolates
    them rather than lose them — flagged as estimates, which is the part that
    makes it honest.

    The estimate is the observed time either side, divided in the proportion
    the timetable gives. It is never counted in punctuality: an estimate
    cannot be late, it can only inherit the lateness of its neighbours, and a
    figure built on that would be measuring our own arithmetic.
    """
    # Keyed as the journeys are: (trip id, service day). Keying it the other
    # way round silently filled nothing at all, because every lookup missed.
    by_journey = {}
    for o in observations:
        by_journey.setdefault((o["trip_id"], o["day"]), o)

    endpoint_sources = {(o["trip_id"], o["day"], o.get("stop_index")): o.get("source_reports", []) for o in observations}
    filled = []
    for key, seen in observed_by_journey.items():
        template = by_journey.get((key[0], key[1].isoformat()))
        if template is None or len(seen) < 2:
            continue
        seen = sorted(seen)
        known = {index for index, _sched, _obs in seen}
        # Only stops *between* two sightings. Beyond either end there is
        # nothing to interpolate from, and guessing there is what produced a
        # bus "arriving" at a stop it had not reached.
        for (i_before, sched_before, obs_before), (i_after, sched_after, obs_after) \
                in zip(seen, seen[1:]):
            span = sched_after - sched_before
            if span <= 0 or i_after - i_before < 2:
                continue
            for index in range(i_before + 1, i_after):
                if index in known:
                    continue
                call = calls_by_journey.get(key, {}).get(index)
                if call is None:
                    continue
                scheduled_secs = call["scheduled_secs"]
                share = (scheduled_secs - sched_before) / span
                observed_secs = round(obs_before + share * (obs_after - obs_before))
                filled.append({
                    **template,
                    "atco": call["atco"],
                    "stop_name": call["stop_name"],
                    "scheduled": trip_match.clock(scheduled_secs),
                    "scheduled_secs": scheduled_secs,
                    "observed": trip_match.clock(observed_secs),
                    "observed_secs": observed_secs,
                    "lateness_secs": observed_secs - scheduled_secs,
                    "nearest_m": None,
                    "stop_index": index,
                    "timepoint": call["timepoint"],
                    # This stop's own flag, never the template's. The template
                    # is whichever observation of the journey came first, so
                    # inheriting it labelled roughly a fifth of interpolated
                    # stops as times the operator had committed to.
                    "estimated": True,
                    "interpolation_endpoints": [i_before, i_after],
                    "source_reports": [ref for i in (i_before, i_after)
                        for ref in endpoint_sources.get((key[0], key[1].isoformat(), i), [])],
                })
    return filled


def _resolve_vehicle_journeys(tracks, scheduled_spans, declared=()):
    """Stop one bus being on two journeys *at once* — while letting it run several.

    Matching each snapshot independently lets a bus drift between journeys: a
    bus six minutes late is, at every moment, a better fit for the journey ten
    minutes behind it running four minutes early. Snapshot by snapshot it lands
    on either, and one run is reported as two half-journeys with opposite
    lateness.

    The obvious fix — one journey per bus, the one it was matched to most —
    is wrong, and was written that way first: a bus runs a journey out, back,
    and out again all day, and keeping only its best-sampled journey silently
    threw the rest of its day away. It passed because it was tested against a
    single 46-minute run.

    Resolving by *when the bus was seen* does not work either, and was the
    second attempt: a drifting bus is matched to its own journey early in the
    run and to the following one later, so its two claims are sequential in
    sightings — indistinguishable from a bus that really did run two journeys.

    The discriminator is the **timetable**. One bus cannot run two journeys that
    are scheduled to be on the road at the same time; it can run any number that
    follow one another. So journeys are taken best-sampled first, and one is
    rejected only where its *scheduled* span overlaps that of a journey already
    accepted for that bus. The 10:00 and the 10:10 along the same road overlap
    almost entirely and cannot both be this bus; an outbound at 10:00 and its
    return at 11:15 do not, and both stand.

    **The underlying ambiguity remains, and its bias runs one way.** With no
    journey identifier in the feed that matches the timetable, a bus more than
    half a headway late is indistinguishable from the next journey running
    early, and is recorded as the latter. Measured lateness is a floor: the real
    figure is this or worse, never better. Said plainly wherever it is published.
    """
    claims = {}                       # vehicle -> [(sightings, journey)]
    for key, samples in tracks.items():
        counts = {}
        for _t, _lat, _lon, ref in samples:
            if ref:
                counts[ref] = counts.get(ref, 0) + 1
        for ref, n in counts.items():
            claims.setdefault(ref, []).append((n, key))

    keep = {}
    for ref, claimed in claims.items():
        accepted = []
        for n, key in sorted(claimed, key=lambda c: (c[1] not in declared, -c[0], scheduled_spans[c[1]][0])):
            lo, hi = scheduled_spans[key]
            clash = any(min(hi, a_hi) - max(lo, a_lo) > OVERLAP_GRACE_SECS
                        for a_lo, a_hi in accepted)
            if clash:
                continue
            accepted.append((lo, hi))
            keep.setdefault(ref, set()).add(key)

    # A report with no vehicle reference cannot be attributed to a run, so it
    # is left alone rather than silently dropped.
    return {key: [s for s in samples if not s[3] or key in keep.get(s[3], {key})]
            for key, samples in tracks.items()}


def _timetable_version(db_path):
    """Hash the actual input bytes, not a possibly stale sidecar."""
    return f"timetable.sqlite sha256:{digest_file(db_path)}" if db_path.exists() else "unknown"


def main(argv=None):
    ap = argparse.ArgumentParser(description="Process a day of feed snapshots.")
    ap.add_argument("--day", required=True, help="service day, YYYY-MM-DD")
    ap.add_argument("--snapshots", required=True, help="directory of HHMM.xml files")
    ap.add_argument("--gtfs-rt", help="directory of HHMM.pb files for the same day, "
                                      "which state which journey each bus is running")
    ap.add_argument("--timetable", default=str(ROOT / "data" / "timetable.sqlite"))
    ap.add_argument("--corridor-only", action="store_true",
                    help="measure only journeys calling at the six A259 poles, "
                         "as the first published figures did")
    ap.add_argument("--out", help="write observations here as JSON")
    ap.add_argument("--summary-out", help="write the day's counts here as JSON")
    ap.add_argument("--timetable-version",
                    help="optional human label; the actual database SHA-256 always identifies the input")
    args = ap.parse_args(argv)

    day = date.fromisoformat(args.day)
    tt = Timetable(Path(args.timetable), allow_fetch=False)

    # Measure a day only against a timetable that describes it.
    #
    # BODS bundles look forward. The build fetched on 20 September 2026 ran from
    # 20260920 to 20270621 and said nothing about the 18th, so processing the
    # 18th against it left every service inactive: nothing scheduled, nothing
    # matched, and a summary of zeroes that is perfectly self-consistent and
    # therefore passed every downstream check. Two days were published that way
    # before anyone noticed, and the site stated a three-day evidence base built
    # from two.
    #
    # There is deliberately no flag to override this. A day the timetable does
    # not cover cannot be measured against it at all — the answer is to fetch
    # the build that was in force, not to proceed and publish nothing.
    missing_cohorts = tt.uncovered_cohorts(day, CORRIDOR_ATCOS if args.corridor_only else stops_in_box(tt))
    if not tt.covers_day(day) or missing_cohorts:
        first, last = tt.service_window()
        where = f"{first}..{last}" if first else "no dated calendar at all"
        print(f"{args.timetable} does not cover {args.day} (it describes "
              f"{where}; uncovered local cohorts: {missing_cohorts}) — the day would "
              f"have unverified timetable coverage. Fetch the build in force on "
              f"{args.day}.", file=sys.stderr)
        return 1

    # A derived figure that cannot say which data produced it cannot be checked,
    # and the timetable is rebuilt weekly. See the evidence-provenance skill.
    version = _timetable_version(Path(args.timetable))
    files = snapshot_files(args.snapshots)
    rt_files = gtfs_rt_files(args.gtfs_rt) if args.gtfs_rt else []
    if not files and not rt_files:
        print(f"no snapshots in {args.snapshots}", file=sys.stderr)
        return 1

    health, manifest = {}, []
    observations, coverage = observe_day(
        tt, day, recorded_stream(tt, day, files, rt_files, health, manifest, parse_snapshot),
        atcos=CORRIDOR_ATCOS if args.corridor_only else None)
    coverage["feeds"] = health
    for feed, count in coverage["matched_reports_by_feed"].items():
        if feed in health:
            health[feed]["unique_matched_reports"] = count
            for hour, matched in coverage["matched_reports_by_feed_hour"].get(feed, {}).items():
                health[feed]["hours"][hour]["matched_reports"] = matched
    for row in observations:
        if row.get("estimated") or not row.get("source_reports"):
            continue
        source = row["source_reports"][0]
        feed = source.get("feed")
        if feed in health and source.get("fetched_epoch") is not None:
            hour = datetime.fromtimestamp(source["fetched_epoch"], LONDON).strftime("%H")
            health[feed]["hours"][hour]["measured_calls"] += 1
    coverage["feed_health_note"] = "Fetched means downloaded recorded objects, not confirmed upstream requests; failed fetch attempts cannot be reconstructed from missing objects. Hour counts use London fetch hour; both autumn 01:00 hours share one bucket with 120 expected RT minutes."
    # The timetable, beside what the buses did — over the same stops the day
    # was measured at, so the two describe the same network.
    schedule = record_schedule(
        tt, day, CORRIDOR_ATCOS if args.corridor_only else None)
    declared = sum(1 for o in observations if o.get("match") == "declared")
    coverage["declared_share"] = round(declared / len(observations), 3) if observations else 0
    payload = {
        "day": args.day,
        "method": METHOD,
        "method_version": METHOD_VERSION,
        "caveats": CAVEATS,
        "as_of": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "data_version": version,
        "coverage": coverage,
        "observations": observations,
        "schedule": schedule,
        "time_basis": TIME_BASIS,
        "provenance": {
            "runtime": {"python": sys.version, "packages": {d.metadata["Name"]: d.version for d in metadata.distributions() if d.metadata["Name"]}},
            "processing_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "code_sha256": {str(p.relative_to(ROOT)): digest_file(p) for p in (
                Path(__file__), ROOT / "api/trip_match.py", ROOT / "api/gtfs_rt.py",
                ROOT / "api/timetable_db.py", ROOT / "scripts/observation_contract.py",
                ROOT / "scripts/reliability_stats.py", ROOT / "scripts/recorded_inputs.py",
                ROOT / "scripts/build_journey_times.py", ROOT / "scripts/query_reliability.py",
                ROOT / "scripts/build_delay_hotspots.py", ROOT / "scripts/check_published.py",
                ROOT / "scripts/publication_bundle.py", ROOT / "scripts/prepare_reliability_inputs.py",
                ROOT / "scripts/archive_reliability_evidence.py",
                ROOT / ".github/workflows/process-snapshots.yml")},
            "timetable_sha256": digest_file(Path(args.timetable)),
            "timetable_label": args.timetable_version,
            "sources": manifest,
            "config": {"arrival_radius_m": ARRIVAL_RADIUS_M, "min_samples": MIN_SAMPLES,
                       "stale_secs": VEHICLE_STALE_SECS, "timezone": "Europe/London",
                       "corridor_only": args.corridor_only},
        },
    }
    if args.summary_out:
        summary = {
            "day": args.day,
            "method": METHOD,
            "method_version": METHOD_VERSION,
            "caveats": CAVEATS,
            "as_of": payload["as_of"],
            "data_version": version,
            "provenance": {k: v for k, v in payload["provenance"].items() if k != "sources"},
            **summarise(observations, coverage),
        }
        out = Path(args.summary_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(summary, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        print(f'{summary["bands"]["on_time"]} of {len(observations)} arrivals on time '
              f'→ {out}')

    text = json.dumps(payload, indent=1, sort_keys=True)
    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8")
        print(f"{len(observations)} observations from {len(files)} snapshots → {out}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
