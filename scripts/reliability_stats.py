"""Turning arrival observations into figures that survive being argued with.

Every number the campaign publishes about reliability comes through here, so
this is where the statistical traps are handled rather than rediscovered:

**An observation is one stop, not one bus.** A 60-stop journey on the 700 files
sixty rows; a short service files a handful. Averaging lateness per observation
therefore ranks routes by length as much as by lateness. Worse, consecutive
stops on one journey are strongly correlated — on the first day measured, a
single journey supplied 30 of the 44 arrivals over fifteen minutes late. So
every figure carries **two denominators**, observations and journeys, and each
group also gets a per-journey median in which one bad journey counts once.

**The distribution is censored.** A bus more than 5 minutes early or 25 late
for a journey is matched to a neighbouring one instead, so no observation can
fall outside that window — of 5,079 measured, none did. A mean over a censored
distribution is biased, which is why medians and band shares are the default
and `mean` has to be asked for.

**A stop late in a route inherits every minute lost upstream.** Ranking stops
by lateness finds the end of the route, not the problem. `segment_stats` is the
honest version: lateness *gained* between two timing points, which is also the
form a bus-priority ask has to take.

**Lateness at an interpolated stop is partly the interpolation.** GTFS marks
the operator's own timing points; everything between them is its estimate.
Figures default to timing points only.

**An interpolated *observation* is not an observation.** Where the bus was not
seen at a stop, its time here is the lateness either side divided by the
timetable, and counting that measures this file rather than the service. Every
figure excludes estimates unless `include_estimates=True` is passed, and each
cell carries `measured_only` so a number cannot be quoted without the answer.
Reporting gaps are likelier in heavy traffic, so including them would have
flattered the worst hours specifically.
"""

import statistics

# DfT BUS09's yardstick for a non-frequent service, so the definition is not
# ours to argue about: on time is no more than one minute early and no more
# than five minutes 59 seconds late.
ON_TIME_FROM_SECS = -60
ON_TIME_TO_SECS = 359
VERY_LATE_SECS = 15 * 60
BANDS = ("early", "on_time", "late", "very_late")

# The matching window, from api/trip_match.py. Nothing outside it can be
# observed, so arrivals sitting on it are reported rather than presented as a
# measured extreme.
CENSORED_EARLY_SECS = -5 * 60
CENSORED_LATE_SECS = 25 * 60
AT_BOUND_SECS = 30

# A cell needs both: 30 arrivals could be one journey's worth of stops.
MIN_OBS = 30
MIN_JOURNEYS = 5


def band(lateness_secs):
    """Which punctuality band an arrival falls in."""
    if lateness_secs < ON_TIME_FROM_SECS:
        return "early"
    if lateness_secs <= ON_TIME_TO_SECS:
        return "on_time"
    if lateness_secs <= VERY_LATE_SECS:
        return "late"
    return "very_late"


def journey_key(row):
    """What counts as one bus's run: a trip on a service day."""
    return (row.get("day", ""), row.get("trip_id", ""))


def service_key(row):
    """A service number and the operator running it, which is the real unit.

    Three of the 42 service numbers measured here are run by two operators —
    the 1, the 5 and the 7 are each both Brighton & Hove and Stagecoach, some
    63,000 observations between them. Grouping on the number alone puts one
    operator's punctuality under another's name, which is the single worst
    thing a site like this can publish: the figure is then wrong about a named
    company, and every other figure on the page is fairly doubted with it.

    This repo already learned the shape of that trap once, in `noc_for_route`:
    "two routes can share a short name across operators", and resolving B&H
    routes at Portslade to Stagecoach reintroduced exactly the false positive
    the fare logic exists to prevent.

    A number with no operator stays as it is rather than gaining an empty
    bracket; it is unattributed either way, and saying so twice adds nothing.
    """
    service = row.get("service", "?")
    noc = (row.get("operator") or "").strip()
    return f"{service} ({noc})" if noc else service


def timing_points(rows):
    """Only arrivals judged against a time the operator committed to."""
    return [r for r in rows if r.get("timepoint") == 1]


def measured(rows):
    """Only arrivals that were actually seen.

    An interpolated observation has no punctuality of its own: its time is the
    lateness of the stops either side, divided in the timetable's proportion.
    Counting it measures our own arithmetic and, because a reporting gap is
    likelier in traffic, it does so in the flattering direction.

    The daily summary has always dropped these. The query tool and the rollups
    did not, so the same day could be summarised two ways — which is worse than
    either answer, because both were published as "the" figure.
    """
    return [r for r in rows if not r.get("estimated")]


def stats(rows, mean=False, include_estimates=False):
    """One cell of a table: counts, medians, and what is not trustworthy in it.

    Returns None for no rows. `per_journey_median_secs` is the median of each
    journey's own median, so a single badly delayed journey counts once rather
    than once per stop.

    Estimates are excluded unless asked for. `include_estimates=True` exists
    for exploring the data, never for publishing from it, and the cell says
    which it was so a figure cannot be quoted without the answer.
    """
    given = len(rows)
    if not include_estimates:
        rows = measured(rows)
    if not rows:
        return None
    late = sorted(r["lateness_secs"] for r in rows)
    n = len(late)
    counts = {b: 0 for b in BANDS}
    for value in late:
        counts[band(value)] += 1
    per_journey_rows = {}
    for row in rows:
        per_journey_rows.setdefault(journey_key(row), []).append(row["lateness_secs"])
    per_journey = [statistics.median(v) for v in per_journey_rows.values()]
    cell = {
        "observations": n,
        "journeys": len(per_journey_rows),
        "median_secs": statistics.median(late),
        "p90_secs": late[min(n - 1, int(n * 0.9))],
        "per_journey_median_secs": statistics.median(per_journey),
        "bands": counts,
        "on_time_share": counts["on_time"] / n,
        "not_on_time_share": 1 - counts["on_time"] / n,
        # How hard the matching window is pressing on this cell. Arrivals piled
        # against a bound mean a tail was cut off, so the figures are floors.
        "at_censoring_bound": sum(
            1 for v in late
            if v <= CENSORED_EARLY_SECS + AT_BOUND_SECS
            or v >= CENSORED_LATE_SECS - AT_BOUND_SECS),
        # Stated rather than implied. A cell resting on 40 arrivals of which 30
        # were interpolated is a different claim from one resting on 40 seen.
        "measured_only": not include_estimates,
        "estimates_excluded": given - len(rows),
    }
    if mean:
        # Asked for explicitly, and never without this note attached.
        cell["mean_secs"] = statistics.fmean(late)
        cell["mean_caveat"] = (
            "The distribution is censored at 5 minutes early and 25 late, so "
            "this mean is pulled towards the middle. Prefer the median."
        )
    return cell


def group_stats(rows, key, mean=False, include_estimates=False):
    """`{group: stats}` for a key function over the rows."""
    grouped = {}
    for row in rows:
        grouped.setdefault(key(row), []).append(row)
    return {k: stats(v, mean=mean, include_estimates=include_estimates)
            for k, v in sorted(grouped.items(), key=lambda kv: str(kv[0]))}


def scheduled_hour(row):
    """The hour the bus was *due*.

    Punctuality belongs to the departure that was promised: a bus due 17:45 and
    seen at 18:05 is the 17:00 timetable failing, and counting it at 18:00
    moves the delay out of the hour that caused it.
    """
    return f'{row["scheduled_secs"] // 3600 % 24:02d}'


def observed_hour(row):
    """The hour the bus was actually there.

    The right bucket for time *lost*: the traffic that delayed a bus is the
    traffic it was sitting in, whatever hour it was supposed to be there.
    """
    return f'{row["observed_secs"] // 3600 % 24:02d}'


def segment_stats(rows, hour=False, include_estimates=False):
    """Where time is lost: lateness gained between consecutive timing points.

    Keyed by `(from_stop, to_stop, direction)` — a segment is directional, and
    the two sides of a coast road are different journeys in different traffic.
    With `hour`, keyed additionally by the hour the bus *traversed* it.

    Only consecutive observed timing points on the same journey count. The pair
    need not be adjacent stops: barely a fifth of stops are timing points, so
    `median_stops_apart` records how much road a segment covers.
    """
    # Timing points *and* measured. A stop can be both a genuine timing point
    # and interpolated — the operator commits to a time there, and our bus was
    # not seen at it — and "lateness gained" between two guesses is arithmetic
    # about arithmetic.
    if not include_estimates:
        rows = measured(rows)
    by_journey = {}
    for row in timing_points(rows):
        by_journey.setdefault(journey_key(row), []).append(row)

    segments = {}
    for journey in by_journey.values():
        journey.sort(key=lambda r: (r.get("stop_index", 0), r["scheduled_secs"]))
        for first, second in zip(journey, journey[1:]):
            scheduled_gap = second["scheduled_secs"] - first["scheduled_secs"]
            if scheduled_gap <= 0:
                continue                    # same minute, or out of order
            key = (first.get("stop_name", ""), second.get("stop_name", ""),
                   first.get("direction", "unknown"))
            if hour:
                key = key + (observed_hour(second),)
            segments.setdefault(key, []).append({
                "gained_secs": second["lateness_secs"] - first["lateness_secs"],
                "scheduled_gap_secs": scheduled_gap,
                "stops_apart": second.get("stop_index", 0) - first.get("stop_index", 0),
                "journey": journey_key(second),
                "journey_start": second.get("journey_start", ""),
                "service": second.get("service", ""),
            })

    out = {}
    for key, legs in segments.items():
        gains = sorted(leg["gained_secs"] for leg in legs)
        n = len(gains)
        scheduled = statistics.median(leg["scheduled_gap_secs"] for leg in legs)
        median_gain = statistics.median(gains)
        out[key] = {
            "observations": n,
            "journeys": len({leg["journey"] for leg in legs}),
            "median_gained_secs": median_gain,
            "p90_gained_secs": gains[min(n - 1, int(n * 0.9))],
            "worst_gained_secs": gains[-1],
            "scheduled_secs": scheduled,
            # A segment timetabled 2 minutes that takes 4 is in worse trouble
            # than one timetabled 20 that takes 22, though both lose 2 minutes.
            # A ratio of seconds to seconds: 0.38 means it took 38% longer than
            # the timetable allows. Dividing seconds by *minutes* here first
            # produced "+24 minutes lost per scheduled minute", which is what
            # an unchecked unit does to a published figure.
            "over_scheduled_share": median_gain / scheduled,
            "median_stops_apart": statistics.median(leg["stops_apart"] for leg in legs),
            "services": sorted({leg["service"] for leg in legs if leg["service"]}),
            "measured_only": not include_estimates,
        }
    return out


def suppress(cells, min_obs=MIN_OBS, min_journeys=MIN_JOURNEYS):
    """Split cells into the publishable and the too-thin, keeping the count.

    Thin cells are returned rather than dropped: a table that quietly omits
    them hides how little it rests on, and "suppressed: 14 cells" is itself
    worth reading.
    """
    kept, thin = {}, {}
    for key, cell in cells.items():
        if cell is None:
            continue
        if cell["observations"] >= min_obs and cell["journeys"] >= min_journeys:
            kept[key] = cell
        else:
            thin[key] = cell
    return kept, thin
