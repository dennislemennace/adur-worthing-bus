"""Turn a day of recorded feed snapshots into arrival observations.

The Worker records where every bus was, once a minute, all day
(`worker/src/recorder.js`). This reads a day of that and answers the question
the snapshots exist for: **when did each bus actually reach each stop, against
when the timetable said it would?**

**How an arrival is decided.** Each snapshot is matched to scheduled journeys by
`api/trip_match.py`, the same matcher the live gap monitor uses, so a published
figure and a live alert cannot disagree. A journey's arrival at one of its stops
is the report in which its bus came nearest that stop, provided it came within
150 m. Nothing is interpolated: the answer is the time of a real report.

**Whose clock.** Every observation is timed *and matched* by the operator's own
`RecordedAtTime`, never by when we fetched it. This is not a nicety. Measured
over 2,297 reports in recorded snapshots, the feed is a **median 186 seconds
behind**: three quarters of reports are over a minute stale and half over three
minutes. Timing arrivals by the fetch would have added about three minutes to
every bus, against punctuality bands one and six minutes wide — an operator
would have been shown as late for our latency. Worse, *matching* on the fetch
time shifts a bus three minutes down its route and onto the journey behind it,
which turns a late bus into an early one.

**What the resolution really is.** Not one minute. A vehicle's position repeats
unchanged across snapshots — 866 of those 2,297 reports were repeats — so the
real interval is the operator's own reporting rate, a median of about three
minutes, and repeats are discarded rather than counted as sightings. An arrival
is therefore accurate to roughly half that interval, and the error is symmetric:
it neither flatters nor damns an operator.

**What is missing, and which way that cuts.** A bus that stops reporting leaves
no observation, so its lateness goes uncounted. A journey that never ran leaves
no observation either — so *absence is not lateness and must never be published
as if it were*. Coverage, the share of scheduled journeys that produced any
observation, is reported beside every figure for exactly that reason.

Usage:

    python scripts/process_snapshots.py --day 2026-09-16 \\
        --snapshots raw/2026-09-16 --timetable data/timetable.sqlite \\
        --out data/reliability/2026-09-16.json
"""

import argparse
import json
import struct
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

LONDON = ZoneInfo("Europe/London")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import gtfs_rt                                # noqa: E402
import reliability_stats                      # noqa: E402
from api import trip_match                    # noqa: E402
from api.timetable_db import Timetable        # noqa: E402

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

METHOD = (
    "Bus positions recorded from the Bus Open Data Service (SIRI-VM) once a "
    "minute are matched to scheduled journeys by position, service number and "
    "time (at most 5 minutes early or 25 minutes late, within 400 m of the "
    "route, heading-checked where published), each position judged at the time "
    "the operator recorded it rather than when it was read. A journey's arrival at a stop is "
    "the recorded position nearest that stop, where it came within 150 m, so "
    "times are accurate to about 30 seconds either way. Journeys with no "
    "tracked bus produce no observation and are counted as missing coverage, "
    "never as lateness. Lateness outside 5 minutes early to 25 minutes late "
    "cannot be observed at all: such a bus is matched to a neighbouring "
    "journey, so both tails of the distribution are censored and the share on "
    "time is optimistic. Coaches are excluded."
)

CAVEATS = [
    "A journey that ran without reporting its position is indistinguishable "
    "from one that did not run. Neither produces an observation, so coverage "
    "is published beside every figure and absence is never counted as lateness.",
    "Observed times are the nearest recorded position to a stop, sampled once "
    "a minute, so each is accurate to about 30 seconds either way.",
    "Journeys are matched to the timetable by position and time, not by a "
    "journey identifier: the feed's journey reference matches no timetable "
    "trip here. A mismatch would move a journey's lateness, not invent it.",
    "Every journey calling at a stop inside the recorded area is measured "
    "(Adur, Worthing and the Brighton coast). Journeys that only touch the "
    "area outside it are not, and a journey is measured only where it is "
    "inside the box.",
    "Lateness at a stop GTFS interpolated between two timing points is partly "
    "a measure of that interpolation, not of the service. Arrivals are "
    "therefore reported separately for the operator's own timing points, for "
    "interpolated stops, and for feeds that do not say which is which.",
    "A bus more than half a headway late cannot be told apart from the next "
    "journey running early, because the feed publishes no journey identifier "
    "that matches the timetable. Such a journey is recorded as the later one, "
    "so measured lateness is a floor and the real figure is this or worse.",
    "A stop is timed by the last report within 150 m of it — when the bus "
    "left — because the scheduled time it is compared against is a departure "
    "time. For a bus that does not stop, that report can be up to 150 m past "
    "the stop, so such arrivals read up to about 20 seconds late. The earlier "
    "rule, nearest approach, leaned the other way and much harder: it timed "
    "the middle of a layover and reported 23% of timing-point arrivals as "
    "early, against 4% once corrected.",
    "Lateness is censored at the matching window: a bus more than 5 minutes "
    "early or 25 minutes late for a journey is attributed to a neighbouring "
    "one instead, so no observation can fall outside that range. In a measured "
    "day of 5,079 observations, none did. Both tails are therefore cut off, "
    "every count of early or very late running is a floor, and the share on "
    "time is correspondingly optimistic.",
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
            if (at_utc - reported_at).total_seconds() > VEHICLE_STALE_SECS:
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
        if len(name) != 4 or not name.isdigit():
            continue
        found.append((int(name[:2]) * 3600 + int(name[2:]) * 60, path))
    return sorted(found)


def snapshot_files(directory):
    """`(seconds from midnight, path)` for every `HHMM.xml`, in time order."""
    found = []
    for path in sorted(Path(directory).glob("*.xml")):
        name = path.stem
        if len(name) != 4 or not name.isdigit():
            continue
        found.append((int(name[:2]) * 3600 + int(name[2:]) * 60, path))
    return sorted(found)


def observe_day(tt, day, snapshots, atcos=None):
    """Arrival observations for one day.

    `snapshots` is an iterable of `(seconds from midnight, vehicles)`. Returns
    `(observations, coverage)`. Coverage counts scheduled journeys against
    those ever tracked — the denominator every figure derived from this needs.
    """
    atcos = stops_in_box(tt) if atcos is None else list(atcos)
    instances, _scheduled = trip_match.build_instances(
        tt, day, atcos, (-BEFORE_FIRST_SECS, 86400 + AFTER_LAST_SECS))

    # Where each journey's bus was, every time it reported.
    tracks = {key: [] for key in instances}
    declared_journeys_seen = set()   # journeys the feed named rather than us
    seen = set()            # (journey, vehicle, report time): the feed repeats
    window_times = []
    for at_secs, vehicles in snapshots:
        window_times.append((at_secs, None))
        # Only journeys plausibly running now: without this a day of snapshots
        # is matched against a day of journeys, which is hours of work.
        active = {k: i for k, i in instances.items()
                  if i["calls"]
                  and i["calls"][0][0] - BEFORE_FIRST_SECS <= at_secs
                  <= i["calls"][-1][0] + AFTER_LAST_SECS}
        if not active:
            continue
        # Each position is judged at the moment it was recorded, not when it
        # was read. Matching on the fetch time shifts every bus about three
        # minutes down the timetable and onto the following journey, which
        # reads as a bus running early. Measured: a bus three minutes late,
        # reported four minutes behind, came out as 7 minutes early.
        when_of = [_same_day(v.get("recorded_secs"), at_secs) for v in vehicles]
        when_of = [at_secs if w is None else w for w in when_of]

        # What the feed states, before anything we infer. A declared journey
        # needs no tolerance window, so nothing it produces is censored.
        declared, claimed = trip_match.place_declared(tt, vehicles, active, at_secs)
        for key in declared:
            declared_journeys_seen.add(key)
        inferred = trip_match.place_vehicles(
            tt, vehicles, active, at_secs, times=when_of,
            skip_vehicles=claimed, skip_journeys=set(declared))
        for key, (vi, _idx) in {**declared, **inferred}.items():
            v = vehicles[vi]
            when = when_of[vi]
            ref = v.get("vehicle_ref", "")
            if (key, ref, when) in seen:
                continue        # the same report, fetched again a minute later
            seen.add((key, ref, when))
            tracks[key].append((when, v["latitude"], v["longitude"], ref))

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
    tracks = _resolve_vehicle_journeys(tracks, spans)

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
        matched_by = "declared" if key in declared_journeys_seen else "inferred"
        # Which of this journey's times the operator actually commits to.
        timepoints = tt.timepoints_for(trip_id)
        # Which way along the coast, and the operator's own word for where it
        # is going. Both are settled here, not in analysis later: trip ids
        # belong to one timetable build, and joining observations to whatever
        # database happens to be on disk would silently mix two builds.
        calls = inst["calls"]
        first_stop = tt.stops.get(calls[0][1]) or {}
        last_stop = tt.stops.get(calls[-1][1]) or {}
        direction = "unknown"
        if first_stop.get("lon") is not None and last_stop.get("lon") is not None:
            shift = last_stop["lon"] - first_stop["lon"]
            if abs(shift) >= 0.002:          # ~140 m, more than one pole apart
                direction = "westbound" if shift < 0 else "eastbound"
        headsign = (inst["trip"].get("headsign") or "").strip()
        journey_start = calls[0][0]

        # One journey can be matched to more than one vehicle across a day if a
        # bus is swapped; the one seen most often is the journey's bus.
        refs = [s[3] for s in samples if s[3]]
        vehicle = max(set(refs), key=refs.count) if refs else ""
        seen_here = []          # (stop index, scheduled, observed) for this journey
        for stop_index, (scheduled_secs, atco) in enumerate(calls):
            stop = tt.stops.get(atco) or {}
            if stop.get("lat") is None:
                continue
            dists = [trip_match.km(s[1], s[2], stop["lat"], stop["lon"]) for s in samples]
            nearest = min(range(len(dists)), key=dists.__getitem__)
            metres = round(dists[nearest] * 1000)
            if metres > ARRIVAL_RADIUS_M:
                continue
            # When the bus *left*, not when it first got there. The timetable
            # time this is judged against is a departure time — the builder
            # reads departure_time — and at a terminus a bus stands for
            # minutes. Taking its nearest approach timed the middle of the
            # layover and reported the bus as leaving early: measured on 17
            # September, 56% of arrivals at Old Steine and 51% at Marine
            # Parade came out early, against 13% across the route.
            #
            # Only the run of samples containing the nearest approach counts,
            # so a route that passes the same stop twice does not have its two
            # visits merged into one long dwell.
            i = nearest
            while i + 1 < len(dists) and dists[i + 1] * 1000 <= ARRIVAL_RADIUS_M:
                i += 1
            # The visit must be bounded by the recording: the bus seen coming
            # *and* going. At the first or last sample the real nearest
            # approach may lie outside the recording, which reads as a bus
            # arriving early at a stop it had not reached — measured on live
            # data, a stop due at 15:31 was recorded as reached at 15:28
            # because that was simply the last snapshot taken.
            if (nearest == 0 or i == len(samples) - 1) and metres > AT_THE_STOP_M:
                continue
            best = samples[i]
            seen_here.append((stop_index, scheduled_secs, best[0]))
            observations.append({
                "day": service_day.isoformat(),
                "trip_id": trip_id,
                "service": inst["service"],
                "operator": tt.noc_for_route(route_id),
                "vehicle": vehicle,
                "atco": atco,
                "stop_name": stop.get("name", ""),
                "scheduled": trip_match.clock(scheduled_secs),
                "scheduled_secs": scheduled_secs,
                "observed": trip_match.clock(best[0]),
                "observed_secs": best[0],
                "lateness_secs": best[0] - scheduled_secs,
                "nearest_m": metres,
                "samples": len(samples),
                # 1 a timing point, 0 a time GTFS interpolated, None unstated.
                "timepoint": timepoints.get(atco),
                "match": matched_by,
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
                "journey_start_secs": journey_start,
            })
        observed_by_journey[key] = seen_here
        calls_by_journey[key] = {
            i: (secs, atco, (tt.stops.get(atco) or {}).get("name", ""))
            for i, (secs, atco) in enumerate(calls)
            if (tt.stops.get(atco) or {}).get("lat") is not None}

    observations += _fill_gaps(observations, observed_by_journey, calls_by_journey)

    coverage = {
        "snapshots": len(window_times),
        "recorded_from": trip_match.clock(seen_from) if window_times else None,
        "recorded_to": trip_match.clock(seen_to) if window_times else None,
        # Scheduled journeys running while the recorder was actually running.
        "scheduled_journeys": len(in_window),
        "tracked_journeys": tracked,
        "scheduled_journeys_all_day": len(instances),
        "observations": len(observations),
    }
    return observations, coverage


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
        svc = by_service.setdefault(o["service"], {k: 0 for k in bands})
        svc[b] += 1
        # Bucketed on the *scheduled* hour. A bus due 17:45 and seen 18:05
        # belongs to the 17:00 timetable; counting it at 18:00 moves delay out
        # of the hour that caused it and flatters the peak.
        hour = f'{o["scheduled_secs"] // 3600 % 24:02d}'
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
                scheduled_secs, atco, name = call
                share = (scheduled_secs - sched_before) / span
                observed_secs = round(obs_before + share * (obs_after - obs_before))
                filled.append({
                    **template,
                    "atco": atco,
                    "stop_name": name,
                    "scheduled": trip_match.clock(scheduled_secs),
                    "scheduled_secs": scheduled_secs,
                    "observed": trip_match.clock(observed_secs),
                    "observed_secs": observed_secs,
                    "lateness_secs": observed_secs - scheduled_secs,
                    "nearest_m": None,
                    "stop_index": index,
                    "estimated": True,
                })
    return filled


def _resolve_vehicle_journeys(tracks, scheduled_spans):
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
        for n, key in sorted(claimed, key=lambda c: (-c[0], scheduled_spans[c[1]][0])):
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
    """Which build of the timetable this is, from the sidecar the release ships."""
    sidecar = db_path.with_suffix(db_path.suffix + ".sha256")
    if sidecar.exists():
        digest = sidecar.read_text(encoding="utf-8").split()[0].strip()
        if digest:
            return f"timetable.sqlite sha256:{digest[:16]}"
    return "unknown"


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
                    help="identifier for the timetable build, recorded with every "
                         "figure. Defaults to the .sha256 sidecar beside it.")
    args = ap.parse_args(argv)

    day = date.fromisoformat(args.day)
    tt = Timetable(Path(args.timetable), allow_fetch=False)
    # A derived figure that cannot say which data produced it cannot be checked,
    # and the timetable is rebuilt weekly. See the evidence-provenance skill.
    version = args.timetable_version or _timetable_version(Path(args.timetable))
    files = snapshot_files(args.snapshots)
    if not files:
        print(f"no snapshots in {args.snapshots}", file=sys.stderr)
        return 1

    london = ZoneInfo("Europe/London")

    # The same minute in the other feed, where it was recorded.
    rt_by_minute = dict(gtfs_rt_files(args.gtfs_rt)) if args.gtfs_rt else {}

    def stream():
        for secs, path in files:
            at_utc = datetime.combine(day, time(secs // 3600, secs % 3600 // 60),
                                      tzinfo=london).astimezone(timezone.utc)
            vehicles = parse_snapshot(path.read_text(encoding="utf-8"), at_utc)
            rt_path = rt_by_minute.get(secs)
            if rt_path:
                # Both feeds carry the same vehicle ids — checked vehicle by
                # vehicle against the same minute, agreeing to a median of 0 m
                # (docs/reliability/gtfs-rt-probe.md).
                stated = declared_journeys(rt_path)
                for v in vehicles:
                    trip = stated.get(v.get("vehicle_ref", ""))
                    if trip:
                        v["trip_id"] = trip
            yield secs, vehicles

    observations, coverage = observe_day(
        tt, day, stream(), atcos=CORRIDOR_ATCOS if args.corridor_only else None)
    declared = sum(1 for o in observations if o.get("match") == "declared")
    coverage["declared_share"] = round(declared / len(observations), 3) if observations else 0
    payload = {
        "day": args.day,
        "method": METHOD,
        "caveats": CAVEATS,
        "as_of": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "data_version": version,
        "coverage": coverage,
        "observations": observations,
    }
    if args.summary_out:
        summary = {
            "day": args.day,
            "method": METHOD,
            "caveats": CAVEATS,
            "as_of": payload["as_of"],
            "data_version": version,
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
