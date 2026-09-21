"""Publish what each journey actually did, so the site can answer stop to stop.

Open Innovations' tool asks the question a passenger actually has: pick two
stops on a route, and see every observed journey time between them against the
timetable. That is far more legible than a punctuality percentage — "your 08:15
takes 52 minutes, not the 48 advertised" needs no explanation of DfT bands, and
nobody can argue about the definition of on time.

**The shape is theirs, and deliberately so.** Rather than precompute every stop
pair — n² per route, most never looked at — each journey is published as the
list of calls it made, and the browser subtracts one call from another. Files
stay small and every pair stays available.

Written per service, because that is how a reader thinks about a bus, and
because it keeps each file to something a phone can fetch.

    python scripts/build_journey_times.py --observations obs/ --out data/journey-times
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from process_snapshots import CAVEATS, METHOD, METHOD_VERSION    # noqa: E402
from query_reliability import load_observations                  # noqa: E402

# A journey with fewer calls than this says nothing about running time between
# two places, and would only add noise to a chart.
MIN_CALLS = 3

# Every stop, not just the operator's timing points.
#
# Publishing timing points only was tried first, to keep the files small enough
# to commit — and it cost the feature. A passenger wants the stop outside their
# house, and barely a fifth of stops are timing points. Since these files are
# served from R2 rather than committed, size is no longer the constraint, so
# the restriction went with it.
#
# What remains true is that a *scheduled* time at an interpolated stop is
# GTFS's estimate. Each call says which it is, and the chart draws the
# timetable line only between stops the operator commits to.
TIMING_POINTS_ONLY = False


class Places:
    """Turns a destination into the town or village a passenger would name.

    Two steps, in this order, and the order is the whole design:

    1. **The headsign names one of our own stops** — then use that stop's
       locality. This is the only thing that reaches a destination outside the
       recorded area: *Shooting Field* is in **Steyning**, which no bus is ever
       observed reaching because Steyning is beyond the box.
    2. **Otherwise the terminus we actually saw.** Always available, and never
       wrong about what was measured.

    Two rules were tried and rejected, each with a false positive found in real
    data:

    * *Searching all of NaPTAN by name.* "Red Lion" is a unique NaPTAN stop — in
      **Handcross**, twenty miles away — while service 2's Red Lion journeys end
      at "The Red Lion" in Shoreham. Being unique is no evidence of being right.
      Restricted to our own timetable the name misses, and step 2 answers
      correctly.
    * *Reading a place name off the front of the headsign.* That makes "Birdham
      Road South End" **Birdham**, a village near Chichester, when service 49 is
      a Brighton local ending at Moulsecoomb. Guarding against street words then
      broke *Rottingdean White Horses*, so the idea went entirely.
    """

    def __init__(self, timetable=None):
        self.by_atco = {}
        self.by_name = {}
        # What contains what: Bristol Estate is in Brighton, Shoreham Beach in
        # Shoreham-by-Sea. Two direction labels where one contains the other do
        # not tell a reader which is which.
        self.parent = {}
        if timetable is None:
            return
        names = {}
        for atco, stop in timetable.stops.items():
            place = (stop.get("locality") or "").strip()
            if not place:
                continue
            self.by_atco[atco] = place
            parent = (stop.get("locality_parent") or "").strip()
            if parent and parent != place:
                self.parent[place] = parent
            names.setdefault((stop.get("name") or "").strip().lower(),
                             set()).add(place)
        # Only names that mean one place. A name shared by two towns tells us
        # nothing, and guessing between them is how "Red Lion" became Handcross.
        self.by_name = {n: next(iter(p)) for n, p in names.items() if len(p) == 1}

    def of(self, headsign, terminus_atco):
        """The place a journey is heading for, or the headsign if unknown."""
        name = (headsign or "").strip()
        # Headsigns carry a stop qualifier: "George Street (stop J)".
        base = name.split(" (")[0].strip().lower()
        found = self.by_name.get(base) or self.by_name.get(name.lower())
        if found:
            return found
        return self.by_atco.get(terminus_atco or "") or name


def route_document(service, rows, meta, timing_points_only=TIMING_POINTS_ONLY,
                   operator="", places=None):
    """One service: its stops, and every journey observed along them."""
    if timing_points_only:
        rows = [r for r in rows if r.get("timepoint") == 1]
    stops, order = {}, {}
    for row in rows:
        atco = row["atco"]
        if atco not in stops:
            stops[atco] = {"atco": atco, "name": row.get("stop_name", ""),
                           "direction": row.get("direction", "unknown"),
                           # The town or village, so the browser can group
                           # destinations by place rather than by stop.
                           "locality": (places.by_atco.get(atco, "")
                                        if places else "")}
        # A stop's place in the route differs between journeys; the median
        # position is good enough to order a picker, and the browser reads the
        # real order from each journey's own calls.
        order.setdefault(atco, []).append(row.get("stop_index", 0))

    listed = sorted(stops.values(),
                    key=lambda s: (s["direction"],
                                   sorted(order[s["atco"]])[len(order[s["atco"]]) // 2]))
    index = {s["atco"]: i for i, s in enumerate(listed)}

    journeys = {}
    for row in rows:
        key = (row["day"], row["trip_id"])
        journey = journeys.setdefault(key, {
            "day": row["day"],
            "start": row.get("journey_start", ""),
            "direction": row.get("direction", "unknown"),
            "headsign": row.get("headsign", ""),
            "match": row.get("match", "inferred"),
            "calls": [],
        })
        journey["calls"].append([
            index[row["atco"]],
            row["observed_secs"],
            row["scheduled_secs"],
            # Two flags, because they mean different things to a chart: an
            # estimated *observation* was interpolated between two sightings,
            # while a non-timing-point *schedule* is GTFS's guess at when the
            # bus was due. The first affects the dot, the second the line.
            (1 if row.get("estimated") else 0) | (0 if row.get("timepoint") == 1 else 2),
        ])

    kept = []
    for journey in journeys.values():
        journey["calls"].sort(key=lambda call: call[2])
        if len(journey["calls"]) >= MIN_CALLS:
            kept.append(journey)
    kept.sort(key=lambda j: (j["day"], j["start"]))

    # Where each destination is, as a town rather than a stop, so the browser
    # can pool several destinations into one direction: service 2 runs to five
    # stops that are only three places.
    #
    # Resolved once per destination, from the stop *most* of its journeys were
    # last seen at — not per journey from its own last call. Journeys are
    # frequently observed only part way, so their individual last calls name
    # wherever the recording ran out: done that way the 700 came out as
    # "South Lancing / West Worthing" instead of "Worthing / Durrington".
    by_index = {i: st["atco"] for i, st in enumerate(listed)}
    termini = {}
    for journey in kept:
        termini.setdefault(journey["headsign"], []).append(journey["calls"][-1][0])
    place_of = {}
    for headsign, ends in termini.items():
        common = max(set(ends), key=ends.count)
        place_of[headsign] = (places.of(headsign, by_index.get(common, ""))
                              if places else headsign)
    for journey in kept:
        journey["place"] = place_of.get(journey["headsign"], journey["headsign"])

    return {
        "service": service,
        # A service number belongs to an operator. The 1, the 5 and the 7 are
        # each run by both Brighton & Hove and Stagecoach in this area, and
        # merging them made one document out of two different routes: a stop
        # list that is the union of both, directions from both, and journey
        # times between two stops no single bus has ever run in sequence.
        "operator": operator,
        # Which places contain which, for the places this document names. The
        # browser needs it to see that "Bristol Estate" and "Brighton" do not
        # distinguish two directions, and it is published rather than looked up
        # so the document stays readable on its own.
        "place_parents": {p: places.parent[p]
                          for p in sorted({j.get("place", "") for j in kept})
                          if places and p in places.parent},
        "as_of": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "days": meta["days"],
        "data_versions": meta["data_versions"],
        "method": METHOD,
        "method_version": METHOD_VERSION,
        "caveats": CAVEATS,
        # Calls are [stop, observed, scheduled, estimated] — seconds from
        # midnight, and estimated meaning the time was interpolated between two
        # sightings rather than observed. Said here because the browser reads
        # these arrays and a reader may open the file directly.
        # flags: 1 the observation was interpolated, 2 the scheduled time is
        # GTFS's estimate rather than a timing point.
        "call_format": ["stop_index", "observed_secs", "scheduled_secs", "flags"],
        "series": "timing_point" if timing_points_only else "all_stops",
        "stops": listed,
        "journeys": kept,
    }


def document_name(service, operator):
    """The file one service-and-operator is published as.

    The operator is always in the name, even where only one runs the number.
    Naming it only when there is a clash means the name changes the day a
    second operator appears, which breaks every link to it — and that day is
    exactly when someone is looking.
    """
    safe = "".join(c for c in service if c.isalnum() or c in "-_") or "unknown"
    noc = "".join(c for c in (operator or "") if c.isalnum()) or "unknown"
    return f"{safe}-{noc}"


def build(rows, meta, timing_points_only=TIMING_POINTS_ONLY, places=None):
    """`{(service, operator): document}` for everything with something to show."""
    grouped = {}
    for row in rows:
        key = (row.get("service", "?"), (row.get("operator") or "").strip())
        grouped.setdefault(key, []).append(row)
    return {key: route_document(key[0], rows_here, meta, timing_points_only,
                                operator=key[1], places=places)
            for key, rows_here in grouped.items()}


def main(argv=None):
    ap = argparse.ArgumentParser(description="Publish observed journey times.")
    ap.add_argument("--observations", nargs="+", required=True,
                    help="files, directories or globs of observation JSON")
    ap.add_argument("--out", default=str(ROOT / "data" / "journey-times"))
    ap.add_argument("--timing-points-only", action="store_true",
                    help="publish only stops the operator commits to a time for")
    ap.add_argument("--timetable",
                    help="timetable.sqlite, for the town each stop is in. "
                         "Without it directions are named by destination stop.")
    ap.add_argument("--min-journeys", type=int, default=3,
                    help="services with fewer observed journeys are not written")
    args = ap.parse_args(argv)

    places = Places()
    if args.timetable:
        try:
            from api.timetable_db import Timetable
            places = Places(Timetable(Path(args.timetable), allow_fetch=False))
            print(f"  localities for {len(places.by_atco)} stops, "
                  f"{len(places.by_name)} unambiguous stop names")
        except Exception as err:                   # noqa: BLE001
            # A missing or old timetable costs better labels, not the build.
            print(f"no localities ({err}); directions named by destination stop",
                  file=sys.stderr)

    rows, meta = load_observations(args.observations)
    if not rows:
        print(f"no observations found in {args.observations}", file=sys.stderr)
        return 1

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    written, skipped, index = [], [], []
    for (service, operator), doc in sorted(
            build(rows, meta, args.timing_points_only, places).items()):
        label = f"{service} ({operator})" if operator else service
        if len(doc["journeys"]) < args.min_journeys:
            skipped.append(label)
            continue
        # One file a service *as one operator runs it*: a reader wants one
        # route, not the county, and not two companies' routes overlaid.
        path = out / f"{document_name(service, operator)}.json"
        path.write_text(json.dumps(doc, separators=(",", ":"), sort_keys=True) + "\n",
                        encoding="utf-8")
        written.append((label, len(doc["journeys"]), path.stat().st_size))
        index.append({"service": service, "operator": operator, "file": path.name,
                      "journeys": len(doc["journeys"]), "stops": len(doc["stops"])})

    (out / "index.json").write_text(
        json.dumps({"as_of": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    "days": meta["days"], "services": index},
                   indent=1, sort_keys=True) + "\n", encoding="utf-8")

    total = sum(size for _s, _j, size in written)
    for label, journeys, size in written:
        print(f"  {label:<14} {journeys:>4} journeys  {size / 1024:>6.0f} KB")
    print(f"{len(written)} services, {total / 1024:.0f} KB total → {out}")
    if skipped:
        print(f"too thin to publish ({args.min_journeys} journeys needed): "
              f"{', '.join(skipped)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
