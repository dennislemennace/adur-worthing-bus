"""Refuse to publish a figure that contradicts itself.

Every other test in this repo checks the code against a fixture. This checks
the **output** against the claims made for it, which is a different question and
the one that has actually gone wrong: the code passed its whole suite while
shipping 69 journey times in which a bus arrived before it set off, because no
fixture described a route that comes back past one of its own early stops.

So these are assertions about published files, run after building them and
before the index is switched. Each names a way a reader could be misled, and
each is something that has happened or was one edit away from happening:

* a negative journey time — a bus arriving before it left;
* a percentile resting on fewer observations than the statistics module's own
  floor, so "9 in 10 journeys" describes two journeys;
* a punctuality figure that quietly counted interpolated times as measured;
* a journey calling at one stop twice, which the browser cannot subtract and
  silently drops;
* an index naming a file that was never written, which shows a reader an empty
  view with no error anyone sees;
* a figure with no method, version or date attached.

Exit status is 1 if anything fails, so the nightly Action stops rather than
publishing. Usage:

    python scripts/check_published.py --journey-times data/journey-times \\
        --summaries data/reliability
"""

import argparse
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import reliability_stats                                          # noqa: E402


class Failures:
    """Every problem found, not the first one.

    Stopping at the first failure turns a data check into a guessing game about
    how many more there are — and the count is itself the finding: one negative
    duration is a bug, four hundred is a broken method.
    """

    def __init__(self):
        self.items = []
        self.counts = {}

    def add(self, check, detail):
        self.items.append((check, detail))

    def note(self, label, n=1):
        """Something worth reporting that is not a reason to stop.

        The first version of this script had no such category, so a journey
        time of exactly zero — two adjacent stops covered inside one reporting
        interval — was failed alongside a bus arriving before it set off. That
        blocked publishing 1,327 times on the first real run, of which 1,325
        were a feed that reports every few minutes rather than any error at
        all. A check that cannot tell "impossible" from "below our resolution"
        gets switched off, and then it catches neither.
        """
        self.counts[label] = self.counts.get(label, 0) + n

    def report(self, out=sys.stderr):
        for label, n in sorted(self.counts.items()):
            print(f"note {label}: {n}", file=out)
        by_check = {}
        for check, detail in self.items:
            by_check.setdefault(check, []).append(detail)
        for check, details in sorted(by_check.items()):
            print(f"FAIL {check} ({len(details)})", file=out)
            for detail in details[:5]:
                print(f"       {detail}", file=out)
            if len(details) > 5:
                print(f"       ... and {len(details) - 5} more", file=out)
        return bool(self.items)


def check_journey_document(doc, name, fails):
    """One published service: its journeys have to be physically possible."""
    check_document_schedule(doc, name, fails)
    stops = doc.get("stops") or []
    for journey in doc.get("journeys") or []:
        calls = journey.get("calls") or []
        where = f'{name} {journey.get("day", "?")} {journey.get("start", "?")}'

        counts = {}
        for call in calls:
            if len(call) < 4:
                fails.add("call has fewer than four fields", f"{where}: {call}")
                continue
            index, observed, scheduled = call[0], call[1], call[2]
            if not isinstance(index, int) or not 0 <= index < len(stops):
                fails.add("call names a stop not in the stops list",
                          f"{where}: index {index!r} of {len(stops)}")
            counts[index] = counts.get(index, 0) + 1
            if observed is None or scheduled is None:
                fails.add("call has no time", f"{where}: {call}")

        for index, n in sorted(counts.items()):
            if n > 1:
                # Counted, not refused. 843 trips in the timetable genuinely
                # call at one stop more than once — circular services and
                # estate loops are an ordinary route shape, not a fault, and
                # this check first learned that by blocking a publish over
                # three late-night journeys on the 5, 5B and 46.
                #
                # The browser drops such a journey only from pairs naming the
                # repeated stop, where it cannot tell which visit is meant.
                # Every other pair on that journey is unaffected.
                fails.note("journeys calling at one stop more than once")

        # The one that reached readers. Wherever the scheduled times advance,
        # the observed times must advance too.
        usable = [c for c in calls if len(c) >= 4 and c[1] is not None and c[2] is not None]
        for before, after in zip(usable, usable[1:]):
            if len(before) >= 5 and len(after) >= 5 and after[4] <= before[4]:
                fails.add("call sequence does not advance", where)
            if after[2] < before[2]:
                fails.add("scheduled times run backwards in route order", where)
            if after[1] - before[1] > 12 * 3600:
                fails.add("implausible adjacent duration requires review", where)
            if after[2] <= before[2]:
                continue                    # same scheduled minute: nothing to compare
            if after[1] < before[1]:
                fails.add("journey time is negative",
                          f"{where}: stops {before[0]}->{after[0]} "
                          f"{(after[1] - before[1]) / 60:.1f} min")
            elif after[1] == before[1]:
                # Not an error. The feed reports every few minutes, so two
                # stops a few hundred metres apart are genuinely covered inside
                # one report. The browser drops such a pair from a chart —
                # there is no duration to draw — but the observation is true
                # and belongs in the file.
                fails.note("stop pairs covered within one report (zero length)")


def check_document_schedule(doc, name, fails):
    """The recorded timetable a document carries, if it carries one.

    Checked for the same reason the journeys are: the browser draws a line
    from it that reads as the timetable's promise, so a profile naming a stop
    the document does not list, or a trip pointing at a profile that is not
    there, would draw a promise nobody made. Documents built before the
    timetable was recorded carry none and are not failed for it.
    """
    schedule = doc.get("schedule")
    if schedule is None:
        return
    stops = doc.get("stops") or []
    profiles = schedule.get("profiles") or []
    for i, calls in enumerate(profiles):
        where = f"{name} schedule profile {i}"
        if len(calls) < 2:
            fails.add("schedule profile has fewer than two calls", where)
        for call in calls:
            if (len(call) != 3 or not isinstance(call[0], int)
                    or not 0 <= call[0] < len(stops)):
                fails.add("schedule names a stop not in the stops list", f"{where}: {call}")
        offsets = [c[1] for c in calls if len(c) == 3]
        if any(b < a for a, b in zip(offsets, offsets[1:])):
            fails.add("scheduled times run backwards in route order", where)
    sets = schedule.get("sets") or []
    for s, trips in enumerate(sets):
        for trip in trips:
            if len(trip) != 4 or not isinstance(trip[1], int) or not 0 <= trip[1] < len(profiles):
                fails.add("scheduled trip names a missing profile", f"{name} set {s}: {trip}")
    window = set(doc.get("window_days") or [])
    for day, s in (schedule.get("days") or {}).items():
        if not isinstance(s, int) or not 0 <= s < len(sets):
            fails.add("schedule day names a missing set", f"{name} {day}")
        if window and day not in window:
            fails.add("schedule describes a day outside the window", f"{name} {day}")


# A phone downloads one of these per service it looks at, compressed: the
# Worker serves them Brotli-encoded, and gzip is measured here as the cautious
# stand-in. The raw ceiling guards parse time and memory against runaway
# growth. A 500 KB raw cap stopped the whole night's publication on 24
# September 2026 over the 2's 507 KB file, which gzip takes to tens of KB.
HOTSPOT_MAP_MAX_COMPRESSED = 200_000
HOTSPOT_MAP_MAX_BYTES = 4_000_000


def check_hotspot_map(doc, name, fails, size=None, raw=None):
    """One service's delay map: small, self-consistent, and never overclaiming.

    The line colour a reader sees is a claim about a road, so a cell may only be
    marked sufficient if it really clears the floor it states, and must point at
    a stretch the file actually draws. The map is a derivative: the traversals
    and their source reports belong in the evidence file, and finding them here
    means the wrong thing was published to every phone.
    """
    if raw is not None:
        size = len(raw)
        compressed = len(gzip.compress(raw, 6))
        if compressed > HOTSPOT_MAP_MAX_COMPRESSED:
            fails.add("delay map file is too large for a phone",
                      f"{name}: {compressed} bytes compressed")
    if size is not None and size > HOTSPOT_MAP_MAX_BYTES:
        fails.add("delay map file is too large for a phone", f"{name}: {size} bytes")
    if "traversals" in doc or any("traversals" in s for s in doc.get("stretches") or []):
        fails.add("delay map carries raw traversals", name)
    floor = doc.get("floor") or {}
    stretches = {s.get("id") for s in doc.get("stretches") or []}
    for s in doc.get("stretches") or []:
        if len(s.get("geometry") or []) < 2:
            fails.add("delay map stretch has no line to draw", f"{name} {s.get('id')}")
    for cell in doc.get("cells") or []:
        where = f"{name} {cell.get('stretch')} {cell.get('period', cell.get('hour'))}"
        if cell.get("stretch") not in stretches:
            fails.add("delay map cell names a stretch it does not draw", where)
        clears = ((cell.get("journeys") or 0) >= floor.get("journeys", 30)
                  and (cell.get("distinct_days") or 0) >= floor.get("distinct_days", 5))
        if cell.get("sample_sufficient") and not clears:
            fails.add("delay map cell claims sufficiency below the floor", where)


def is_stats_cell(cell):
    """Whether a published cell came from `reliability_stats.stats`.

    The daily summary also publishes plain band counts under the same keys, and
    those carry no denominators to check. Telling them apart by shape rather
    than by which file they are in means a new table gets checked the moment it
    is published in the shape that makes claims.
    """
    return isinstance(cell, dict) and (
        "observations" in cell or "p90_secs" in cell or "p90_gained_secs" in cell)


def check_cells(cells, name, fails, min_obs=None, min_journeys=None):
    """Any table of statistics cells published as fact."""
    min_obs = reliability_stats.MIN_OBS if min_obs is None else min_obs
    min_journeys = (reliability_stats.MIN_JOURNEYS if min_journeys is None
                    else min_journeys)
    for key, cell in (cells or {}).items():
        if not is_stats_cell(cell):
            continue
        label = f"{name} {key}"
        if cell.get("measured_only") is not True:
            fails.add("published cell counted interpolated times as measured",
                      f'{label}: measured_only={cell.get("measured_only")!r}')
        # A percentile over a handful of arrivals is arithmetic, not evidence.
        if "p90_secs" in cell or "p90_gained_secs" in cell:
            if cell.get("observations", 0) < min_obs:
                fails.add("percentile published below the observation floor",
                          f'{label}: {cell.get("observations")} of {min_obs}')
            if cell.get("journeys", 0) < min_journeys:
                fails.add("percentile published below the journey floor",
                          f'{label}: {cell.get("journeys")} of {min_journeys}')


def check_summary(summary, name, fails):
    """One daily or monthly summary: it has to say what it rests on."""
    for field in ("method", "caveats", "as_of"):
        if not summary.get(field):
            fails.add("summary is missing its provenance", f"{name}: no {field}")
    # A daily summary names one timetable build; a rollup names each it spans.
    if not (summary.get("data_version") or summary.get("data_versions")):
        fails.add("summary is missing its provenance", f"{name}: no data version")
    # What the figure was measured *with*, not only what it was measured
    # against. Without it a figure from before the arrival picker was made
    # monotonic cannot be told from one after, and on the journeys that were
    # wrong those two answers differ by up to an hour.
    if not (isinstance(summary.get("method_version"), int)
            or (summary.get("method_version") == "mixed" and summary.get("method_versions")
                and all(isinstance(m, int) for m in summary["method_versions"]))):
        fails.add("summary is missing its provenance",
                  f'{name}: no method_version')
    if not (summary.get("day") or summary.get("month")):
        fails.add("summary is missing its provenance", f"{name}: no day or month")

    if summary.get("measured_only") is not True:
        fails.add("summary does not state that estimates were excluded",
                  f'{name}: measured_only={summary.get("measured_only")!r}')

    coverage = summary.get("coverage") or {}
    if summary.get("day") and "snapshots_by_hour" not in coverage:
        fails.add("daily summary does not report coverage by hour", name)

    # Band counts must add up to the observation count, or something was
    # filtered in one place and not in the other.
    bands = summary.get("bands") or {}
    total = summary.get("observations")
    if bands and isinstance(total, int) and sum(bands.values()) != total:
        fails.add("band counts do not add up to the observations",
                  f"{name}: {sum(bands.values())} vs {total}")

    # A day that measured nothing is a failure, not a figure.
    #
    # Every check above compares one published number with another, so a file in
    # which *every* number is zero satisfies all of them: the bands sum to zero,
    # which equals zero observations, and nothing else has anything to disagree
    # with. That is not hypothetical. The summaries committed for 18 and 19
    # September 2026 each recorded about 1,170 snapshots and published zero
    # observations, zero scheduled journeys and an empty by_service, and this
    # script passed them both — because the timetable they were measured against
    # begins on the 20th, so no service was scheduled on either date and nothing
    # could be matched. The site then stated a three-day evidence base built
    # from two days.
    #
    # `scheduled_journeys` is the field that tells the two causes apart. Zero
    # observations with journeys scheduled means the matcher found nothing, and
    # is a bug in the matcher; zero of both means the day was measured against a
    # timetable that does not cover it, and is a bug in which timetable was
    # fetched. Both are fatal, and saying which saves the next reader the hour
    # it took to find this one.
    if summary.get("day"):
        scheduled = coverage.get("scheduled_journeys")
        if total == 0:
            fails.add("a day published no observations at all",
                      f"{name}: 0 observations from "
                      f'{coverage.get("snapshots", "?")} snapshots, '
                      f"{scheduled} scheduled journeys"
                      + (" — the timetable does not cover this day"
                         if scheduled == 0 else " — the matcher found nothing"))
        elif scheduled == 0:
            fails.add("a day published no scheduled journeys",
                      f"{name}: {total} observations against nothing scheduled")
    # True of a rollup as well as a day: a summary whose per-service table is
    # empty has no figure in it to be right or wrong about.
    if not (summary.get("by_service") or {}):
        fails.add("summary has no per-service figures", name)

    # Against the floor the file itself declares, not the module's default.
    #
    # A file may reasonably choose a different floor — the monthly rollup uses
    # 20 arrivals and 20 journeys where the module's default is 30 and 5, which
    # is looser on one and four times stricter on the other, and defeats the
    # concern behind MIN_OBS directly: 30 arrivals can be one journey's worth of
    # stops, and 20 journeys cannot. Judging that trade is not this script's
    # business. What is checkable, and what this catches, is a file whose cells
    # do not respect the floor it prints at the top of itself.
    floor = summary.get("floor") or {}
    for table in ("by_service", "by_hour", "by_timepoint", "segments"):
        check_cells(summary.get(table), f"{name}/{table}", fails,
                    min_obs=floor.get("observations"),
                    min_journeys=floor.get("journeys"))


def check_index(index, out_dir, fails):
    """The index must not name a file that is not there.

    Publishing the index before the documents is how a reader gets half a
    dataset: the page fetches a service that has not been written and shows
    nothing, with no error anyone sees.
    """
    for entry in index.get("services") or []:
        name = entry.get("file")
        if not name or not (out_dir / name).exists():
            fails.add("index names a file that was not published",
                      f'{entry.get("service")} -> {name!r}')


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--journey-times",
                    help="directory of published per-service documents")
    ap.add_argument("--summaries", nargs="+",
                    help="summary files, or directories of them")
    ap.add_argument("--quiet", action="store_true", help="say nothing when it passes")
    args = ap.parse_args(argv)

    fails = Failures()
    checked = 0

    if args.journey_times:
        out_dir = Path(args.journey_times)
        for path in sorted(out_dir.glob("*.json")):
            doc = json.loads(path.read_text(encoding="utf-8"))
            if path.name == "index.json":
                check_index(doc, out_dir, fails)
            else:
                check_journey_document(doc, path.stem, fails)
            checked += 1

    for given in args.summaries or []:
        path = Path(given)
        # A file or a directory. The nightly Action passes the one summary it
        # just built: pointing it at the directory would fail every future run
        # on summaries written before these checks existed, which is a check
        # that has to be switched off to get any work done, which is no check.
        found = sorted(path.glob("*.json")) if path.is_dir() else [path]
        for each in found:
            check_summary(json.loads(each.read_text(encoding="utf-8")),
                          each.stem, fails)
            checked += 1

    if not checked:
        print("nothing to check — no published files found", file=sys.stderr)
        return 1
    if fails.report():
        print(f"\n{len(fails.items)} problems across {checked} published files",
              file=sys.stderr)
        return 1
    if not args.quiet:
        print(f"{checked} published files, nothing contradictory")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
