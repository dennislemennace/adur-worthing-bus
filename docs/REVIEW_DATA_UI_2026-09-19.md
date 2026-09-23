# Data pipeline and visual tool review — 19 September 2026

Reviewed `f752628..fb33311`, covering the recent recorder, timetable timing-point support, matching, observations, reliability analysis, publication, live journey display, and journey-time UI. The working tree was clean when the review began. This document records findings; application code and published data have not been changed.

**Recommendation: keep the journey-time view in preview. Correct the data defects and rebuild affected outputs before presenting the figures as reliable evidence. A week of additional collection will not correct these defects.**

There are useful foundations: original feed bytes are initially retained; the nightly processor uses operator timestamps; estimated observations have a separate flag; daily summaries exclude estimates; counts accompany many statistics; timetable downloads are checksum-checked; the published-data bucket is separate from the recording bucket; and the UI includes a table alongside the chart. The problems below arise where those protections do not carry through the complete pipeline.

## Verification and evidence

- Python: **332 passed** using `.venv/bin/python -m pytest -q`.
- Frontend and Worker JavaScript: **475 passed** using `node --test --test-isolation=none 'tests/*.mjs' 'worker/test/*.js'`.
- Existing browser harness: **426/426 passed**, against the local preview with `?preview=1`, in Chrome at five viewport sizes. Additional focused desktop/mobile probes inspected the rendered chart and the actual data behind it.
- Read the three public observation release assets for 16–18 September and the public journey-time index and service 700 document. The latest inspected processing workflow succeeded. This does not establish that all published measurements are correct.
- Verified that the local timetable has the exact full SHA-256 corresponding to the observations' recorded version: `f1daaae3ed886a18ad91f01a43fdeb9bc1d46436e07b5a7d6d4882863a89b705`. Comparisons of observation timing-point flags with the timetable therefore use the same build.
- Targeted reproductions demonstrated the object-budget mismatch, stale-position live lateness, and rejection of a feed-declared journey running 40 minutes late.
- The local browser emitted Turnstile error 110200. Public submissions were not exercised. No deployment, feed rebuild, bucket mutation, commit, or push was performed. Private R2 inventory, deployed secret permissions, and long-running recorder behaviour were not independently inspected.

The inspected public inputs were [the September observation release](https://github.com/dennislemennace/adur-worthing-bus/releases/tag/reliability-2026-09), [the journey-time index](https://adur-worthing-submissions.dennislemennace.workers.dev/journey-times/index.json), and [service 700](https://adur-worthing-submissions.dennislemennace.workers.dev/journey-times/700.json). These URLs are mutable. Checksums of the exact copies inspected appear below.

## Findings requiring correction

P1 means a correctness or collection problem to address before relying on the affected figures. P2 means a material limitation or misleading presentation to resolve before public release of the tool.

### 1. P1 — The recorder's normal retention exceeds its own object limit

**Location:** `worker/src/recorder.js:57–59`, `:134–145`, and `.github/workflows/process-snapshots.yml`.

The recording window contains 1,170 minutes per day. Two feeds now produce **2,340 objects per day**, so seven complete days require **16,380 objects**, already above `MAX_STORED_OBJECTS = 15_000`. Whole-day pruning retains the cutoff date as well, which can increase occupancy further. The nightly workflow downloads snapshots but does not delete them, despite comments describing processor cleanup.

Under successful, continuous collection, the budget therefore stops recording before the intended retention window fills. This is a deterministic configuration conflict, not an observed production outage. The exported `withinBudget()` returns `object_budget` for a normal seven-day inventory with only 2.3 GB of bytes.

**Correction:** reconcile both-feed object counts, retention semantics, and the byte ceiling. Add a simulated multi-day recorder test covering successful processing and absent processing. Record a visible collection-health signal when collection pauses.

### 2. P1 — Interpolated stops inherit another stop's timing-point flag

**Location:** `scripts/process_snapshots.py:619–634`; consumer: `scripts/build_journey_times.py:90`.

`_fill_gaps()` copies the first observation of the journey as a template but never replaces its `timepoint` with the interpolated stop's own timetable value. Consequently the schedule flag describes a different stop.

In the published 18 September observations, **8,461 of 13,825 estimated rows have the wrong timing-point flag** against the matching timetable: 8,119 approximate stops are labelled exact and 342 exact stops are labelled approximate. This changes whether the UI draws a timetable comparison and contaminates timing-point selection in analysis.

**Correction:** carry timing-point metadata by trip call/sequence into `_fill_gaps()` and set every stop-specific field explicitly. Reprocess affected observations and regenerate journey-time files. Test a journey whose timing-point flags alternate, with the first observed stop differing from the missing stop.

### 3. P1 — Analysis counts estimates as measured punctuality

**Location:** `scripts/reliability_stats.py:68–82`, `scripts/query_reliability.py:filtered`, and `segment_stats()`.

The daily `summarise()` function excludes `estimated` rows. The query/statistics pipeline does not: `timing_points()` checks only `timepoint == 1`, `stats()` consumes every supplied row, and `--all-stops` also retains estimates. Reports, rankings, suppression thresholds, and segment statistics can therefore count the interpolation as evidence.

For 18 September, the default timing-point selection contains **26,933 rows, including 8,860 estimates**. Its on-time share is **78.35%**, whereas the measured-only timing-point rows give **79.79%**. These are diagnostic figures illustrating disagreement between consumers, not validated replacement reliability claims. Correcting finding 2 alone does not correct this inclusion bug.

**Correction:** define measured-only input selection once and apply it to every punctuality/reporting path, including `--all-stops`, segments, JSON, and rollups. Keep estimates available explicitly for journey-time exploration. Test the complete observation-to-report path, not just the daily summary.

### 4. P1 — Stop extraction permits journeys that run backwards in time

**Location:** `scripts/process_snapshots.py:405–437`, `:608–621`; `app.js:1670–1683`.

Each stop independently chooses the globally nearest sample and its following within-radius run. Nothing requires successive stop calls to use successive visits. A loop, nearby roads, overlapping stop radii, or an incorrectly combined track can therefore assign a later visit to an earlier stop. `_fill_gaps()` then interpolates even when observed time decreases. The browser checks scheduled order but accepts negative observed duration.

Across the three downloaded observation files, sorting each service-day/trip by `stop_index` produces **487 decreases in observed time**: 475 are smaller than 12 hours and 12 are midnight discontinuities. This count detects invalid sequences; it does not identify the cause of every individual case.

The public service 700 file contains **nine adjacent call pairs with increasing scheduled time but decreasing observed time**. A concrete browser reproduction is **Queens Road → Seabright**, 17 September, journey starting 05:05: the tool accepts **−599 seconds**, approximately **−10 minutes**. Both endpoints are labelled measured.

**Correction:** match visits in route-call order, distinguishing repeated visits and vehicle/run changes. Quarantine contradictory tracks and record rejection reasons. Never interpolate across reversed observations. Add publication validation and a defensive browser check for invalid durations; simply hiding negative values would leave wrong positive durations unresolved.

### 5. P1 — Overnight observations use incompatible time origins when combined

**Location:** `scripts/process_snapshots.py:439–449`; `api/trip_match.py:125–128`; `scripts/build_journey_times.py:73–95`.

A row is labelled with its **service day**, but its numeric times remain relative to the **snapshot calendar day's midnight**. Processing the next calendar day's early snapshots subtracts 86,400 from the same trip's scheduled calls. Combining daily files by `(day, trip_id)` then combines incompatible numbers and sorts them as if they shared one origin.

For service-day 16 September, trip `VJc2356d90a538db0635ec4e97bc2cc3a6558dadf8` contains late-evening calls followed in route order by after-midnight calls represented with small positive seconds. The twelve large discontinuities detected above demonstrate this in the published inputs. Depending on the selected pair, the browser can omit a valid overnight journey or reverse the apparent call order.

**Correction:** persist UTC instants and/or seconds from the explicitly named service day's origin, including values above 86,400. Preserve route-call sequence in the published format. Test a journey assembled from two consecutive snapshot folders, not just a single-file midnight calculation. GTFS explicitly permits service-day times beyond midnight; see the [schedule reference](https://gtfs.org/documentation/schedule/reference/#stop_timestxt).

### 6. P1 — Live lateness includes feed latency

**Location:** `api/main.py:2094–2100`.

The new live enrichment compares the nearest stop's schedule with `datetime.now()`, even though each position already carries `recorded_at`. This reintroduces the latency bias that the offline processor was changed to avoid.

A focused reproduction placed a bus at its 14:26 stop with a 14:26 operator timestamp, fetched at 14:30. `_attach_declared_journeys()` reported **240 seconds late** for that on-time recorded position. Between-stop position and stop dwell create additional uncertainty, so the result also needs a qualified label.

**Correction:** use the validated operator observation timestamp and correct service-day instance; expose report age and distinguish an estimated nearest-stop deviation from an observed departure delay. Add a stale-but-accepted position regression case.

### 7. P1 — Feed-declared journeys are still censored by an inference window

**Location:** `scripts/process_snapshots.py:321–337`.

`place_declared()` receives only journeys in `active`, which ends 30 minutes after each journey's scheduled final call. A valid declared trip can disappear before the bus finishes. If the whole portion seen in the recording box is late enough, it contributes nothing, despite comments claiming declared journeys are uncensored.

A four-stop declared journey scheduled 10:00–10:06 produces four observations when on time, but **zero observations when the identical track runs 40 minutes late**. Longer journeys can lose their later, delayed stops instead.

**Correction:** resolve declared trip instances separately from inference candidate pruning, using the feed's service date where available and observation time. Apply the inference window only to inferred candidates. Preserve match provenance per contributing observation; one declared sample currently labels an entire mixed track `declared`.

### 8. P2 — The collection window omits real night services

**Location:** `worker/src/recorder.js:35–38`; `scripts/process_snapshots.py:355–359`.

The recorder stops between 00:30 and 05:00 on the assumption that night buses have finished. The matching timetable contains **seven N700 journeys with 503 scheduled calls inside the recording box in that omitted clock window**, running on the 18 September service day. These are scheduled calls, not evidence those buses actually ran.

Coverage uses only the first and last snapshot time, so a nominal 00:00–23:59 span also hides the overnight collection gap. A total snapshot threshold cannot describe which hours or services were actually observable.

**Correction:** either cover the scheduled night service within a recalculated budget or explicitly scope every result to collection hours. Publish per-hour expected/received snapshots and distinguish unrecorded periods from journeys absent from an otherwise working feed.

### 9. P2 — Published methodology contradicts the implemented measurement

**Location:** `scripts/process_snapshots.py:METHOD`, `CAVEATS`; `app.js:1917–1926`.

Published metadata still describes nearest approach, approximately ±30-second accuracy, and no usable declared journey identifier. The code now uses the last report within 150 m, deduplicates operator reports, interpolates some observations, and uses GTFS-RT identities. The file's own introductory discussion says effective reporting intervals can be several minutes. A once-a-minute fetch does not establish ±30-second observation accuracy.

The UI does not show `method`, `caveats`, `as_of`, match provenance, or coverage. Its provenance paragraph has no link to the downloadable data. An observer sees precise-looking results without the qualifications needed to interpret them.

**Correction:** version the method, derive uncertainty from actual surrounding reports, and publish metadata matching each historical processing version. Render a short quality explanation, build date, contributing days, measured/estimated and declared/inferred counts, plus an expandable method and source download.

### 10. P2 — “Departed” displays the scheduled start of the whole journey

**Location:** `app.js:1888–1913`, `journeyTimesChart()` tooltips.

The table uses `t.start`, which is the scheduled first-stop departure, rather than `departSecs`, which is the observed time at the selected origin. The chart plots `departSecs` but its tooltip also shows `start`. Choosing an intermediate stop can produce a large discrepancy; even the default service 1 origin had a row labelled 05:35 whose observed departure was 05:45:45.

**Correction:** show observed departure at the selected origin under “Departed”, and separately identify the scheduled trip start and scheduled departure at that origin. Use identical time semantics in the chart, tooltip, table, and export.

### 11. P2 — The timetable line represents one median across differing schedules

**Location:** `app.js:1713–1717`, `:1792–1798`, `:1894–1898`.

The chart draws a horizontal line labelled “timetable” using the median scheduled duration. For the default service 1 pair, the 24 contributing journeys have **16 distinct scheduled durations, from 61 to 85 minutes**, while the line says 77 minutes. A correctly scheduled longer journey can appear visually over its allowance, and a delayed shorter journey can appear under it.

The per-journey overrun calculation correctly compares each journey with its own schedule, but its label also omits the extra 60-second threshold. The headline can compare medians from different populations when only some journeys have exact endpoint schedules.

**Correction:** plot per-journey scheduled durations or paired observed-minus-scheduled deviations. If retaining a median line, label it explicitly as a median and do not present it as every journey's allowance. State the overrun tolerance and compare matched populations.

### 12. P2 — Sparse selections receive strong summaries and overstated date coverage

**Location:** `scripts/build_journey_times.py:103`, `:--min-journeys`; `app.js:1874–1926`.

The minimum-journey threshold applies to the entire service file, not the selected stop pair/day filter. The default service 7 pair has **two journeys on one day**, yet receives a median, fastest, slowest, and “9 in 10” summary. Waiting for a week of files does not guarantee enough observations for any particular pair.

Every service document inherits the input window's global `days`. The default service 1 chart lists 16, 17, and 18 September in its provenance even though **all 24 displayed journeys are from 18 September**. Earlier observations were processed for a smaller geographic scope, which further limits comparisons across days.

**Correction:** compute contributing days and sample size after every filter, carry coverage for the selected cohort, and label small samples as exploratory. Set explicit criteria for publishing percentile/general reliability summaries. Show observation counts by day and recording scope.

### 13. P2 — Service identity and stop direction are too coarse for the widened area

**Location:** `scripts/build_journey_times.py:55–60`, `:120–125`; `app.js:1853–1859`.

The builder groups only by service number, drops operator and trip ID from the public journey objects, and assigns each stop the direction of the first row encountered. Services **1 and 7 each contain both BHBC and SCSO observations**. These are different services with the same number, presented as one choice.

In the service 700 default pair, Willow Crescent is labelled “towards Brighton” while Old Steine is labelled “towards Worthing”, despite valid journeys between them. A stop used by different patterns cannot safely inherit the first journey's overall compass direction. The widened geography also includes routes for which “towards Worthing/Brighton” is not a useful destination.

**Correction:** retain operator, route, trip instance, headsign, and call sequence; let the user choose a real route/direction or destination. Constrain destination choices to reachable downstream calls for the selected origin and pattern. Use geographic directions only where they are meaningful.

### 14. P2 — Chart text and interaction are too small for practical analysis

**Location:** `app.js:1767–1815`, `style.css:6268–6278`, and the journey-time panel layout.

The SVG has a 640-unit viewBox and 11-unit labels, but measured only **359 CSS px wide on desktop** and **390 px on mobile**. Labels render at approximately **6.2 and 6.7 CSS px** respectively. The surrounding map occupies most desktop space while contributing nothing to the selected journey analysis. Native select labels truncate route and direction information.

Dots expose only SVG title hover text: none is focusable and there is no deliberate touch selection or zoom. The table is a useful fallback but currently has the time-label defect above. There is no visible legend explaining hollow dots.

**Correction:** give analysis enough screen space, use responsive tick density with readable text sizes, and provide tap/keyboard selection tied to a details table. Add a visible legend, a useful time range, and a linked selected route/map if retaining the map. Verify scaled SVG text and interactive point behaviour in browser checks, not just overflow and element visibility.

## Provability requires a durable evidence chain

**Locations:** `worker/src/recorder.js:134–148`, `scripts/process_snapshots.py:payload`, `scripts/build_journey_times.py:74–90`, `.github/workflows/process-snapshots.yml:Publish the observations`, and `.github/workflows/update-timetable.yml:137–146`.

The current system makes arithmetic from published observations checkable, but does not preserve enough material to verify the original observation derivation indefinitely:

- Raw snapshots are pruned by age regardless of whether processing succeeded or the results were independently checked.
- Observation rows omit source object/report references and the position/timestamp samples needed to replay a match or a stop visit. `nearest_m` alone cannot reconstruct a departure or interpolation.
- Results record a truncated timetable hash but no processing commit, explicit method/schema version, input manifest, or immutable observation artifact ID. The public chart format additionally discards the trip ID.
- Daily release assets are replaced with `--clobber`, so the same URL can later serve a different result without a revision chain.
- Only five dated timetable releases are retained. Reprocessing fetches `timetable-latest`, rather than resolving the timetable used for the original measurement.

This is an architectural limitation, not proof that the stored data was altered. It matters for the requested standard of “provable”: after source expiry, an analyst can recompute a percentage but cannot establish why a bus was assigned to that trip and time, or reproduce a correction to the matcher.

**Required design:** preserve compressed source batches or the complete source evidence needed to replay each published derivation; reference full hashes in a manifest; retain the exact timetable; record code, schema, configuration, and method versions; publish immutable revisions; and link chart → journey/call → observation artifact → source evidence. A hash identifies bytes but does not replace keeping those bytes. Fit the chosen archive policy to the documented storage budget before extending retention.

## Other publication and matching hardening

These deserve follow-up but were not independently demonstrated as production incidents:

- Both live and offline joins reduce GTFS-RT to `vehicle_id → trip_id`, discarding its timestamp and service date. Independently fetched snapshots can straddle a vehicle's trip change. Join compatible report times, validate service instances, and record conflicts rather than attaching a new trip to an old position. The [GTFS-RT vehicle position and trip descriptor reference](https://gtfs.org/documentation/realtime/reference/) defines the relevant timestamp and date fields.
- Malformed XML becomes an empty vehicle list while the snapshot still contributes to coverage. Fetch success, parse success, fresh reports, matched journeys, and observed calls need separate counters. A count of 600 downloaded files does not establish a representative day.
- Journey-time publication overwrites files in place with `aws s3 sync`; an index and route file can belong to different builds during publication or caching. Publish under a versioned prefix, validate it, then switch a manifest. Show stale-build status and make missing-day/fallback publication visible.
- The checked-in monthly rollup still contains only 16–17 September, while daily processing now publishes the 18th. There is no monthly-rollup update step in the nightly workflow. Treat the rollup as a dated manual artifact or automate its rebuild before any consumer treats it as current.
- `timepoint=0` means an approximate scheduled time, not necessarily a time invented by this project or an absence of a timetable. Preserve that distinction in user-facing copy; the [GTFS schedule reference](https://gtfs.org/documentation/schedule/reference/#stop_timestxt) distinguishes approximate and exact times.

## What would make the tool powerful and intuitive

| User task | Recommended behaviour |
|---|---|
| Find my journey | Search by operator/service and place; select direction/destination; show only compatible downstream stops; provide swap/reset and a few representative examples. |
| Understand what happens at my travel time | Individual dates, weekday/Saturday/Sunday, and departure-hour filters; selectable dots; clear observed versus scheduled departure; optional hourly distributions. |
| Distinguish evidence quality | Measured-only default or an explicit estimate toggle; declared/inferred identity filters; sample count and contributing days; missing-data and coverage indicators beside the result. |
| Compare fairly | Actual duration and paired schedule deviation as separate views; consistent cohorts and route patterns; explain approximate schedules. |
| Investigate an outlier | Select a point to see its route, ordered calls, timestamps, source reports, uncertainty, and rejection/quality flags. |
| Share or challenge a result | Stable filter permalink, CSV/JSON download, immutable dataset/version link, and a short methodology/citation panel. |
| Use it on a phone or keyboard | Analysis-first responsive layout, readable chart labels, visible point-selection state, keyboard/touch controls, and a sortable accessible table. |

These are product recommendations beyond defect correction. Prioritise trustworthy cohort selection, readable analysis space, and evidence drill-down before adding more chart types.

## Suggested repair and release sequence

1. Fix the recorder capacity conflict and decide what evidence must be preserved before the current source window expires. Make failed/partial collection visible.
2. Correct stop-specific metadata, measured-only statistics, service-day normalisation, ordered stop visits, declared-trip pruning, and live timestamp handling. Add meaningful regression cases at consumer boundaries.
3. Reprocess retained source data against its exact timetable, compare revised results with prior releases, and publish revisions with a change record. Reject impossible sequences and inconsistent flags before upload.
4. Correct UI time semantics, cohort/date reporting, sample-size handling, operator/direction identity, schedule comparisons, and chart legibility. Provide source links and quality explanations.
5. Release only after representative stop-pair checks across operators, directions, overnight trips, sparse routes, and late buses pass against real evidence. Seven calendar days of files alone is insufficient. Keep explicit limitations for anything the data cannot establish.

## Reproducing the main published-data checks

Download the observation assets into a separate directory; do not overwrite the repository's data:

```sh
mkdir -p /tmp/adur-review-observations
gh release download reliability-2026-09 \
  --pattern 'observations-*.json.gz' --dir /tmp/adur-review-observations
```

From the repository root, the following audits the published input without modifying it:

```python
import collections, sqlite3, sys
sys.path.insert(0, 'scripts')
from query_reliability import load_observations
import reliability_stats as rs

rows, meta = load_observations(['/tmp/adur-review-observations'])
latest, _ = load_observations([
    '/tmp/adur-review-observations/observations-2026-09-18.json.gz'])
tp = rs.timing_points(latest)
print('timing points including estimates:', len(tp))
print('estimates included:', sum(bool(r.get('estimated')) for r in tp))
print('on-time share:', rs.stats(tp)['on_time_share'])
measured = [r for r in tp if not r.get('estimated')]
print('measured-only share:', rs.stats(measured)['on_time_share'])

db = sqlite3.connect('file:data/timetable.sqlite?mode=ro', uri=True)
flags = {(tid, seq): flag for tid, seq, flag in db.execute('''
    SELECT t.trip_id, st.seq, st.timepoint
    FROM stop_times st JOIN trips t USING (tid)
''')}
estimated = [r for r in rows if r.get('estimated')]
print('wrong estimated flags:', sum(
    flags.get((r['trip_id'], r['stop_index'])) != r['timepoint']
    for r in estimated))

journeys = collections.defaultdict(list)
for row in rows:
    journeys[(row['day'], row['trip_id'])].append(row)
backwards = []
for calls in journeys.values():
    calls.sort(key=lambda r: r['stop_index'])
    backwards += [(a, b) for a, b in zip(calls, calls[1:])
                  if b['stop_index'] > a['stop_index']
                  and b['observed_secs'] < a['observed_secs']]
print('backwards adjacent calls:', len(backwards))
```

The timing-point comparison requires the exact timetable hash recorded above. Counts may change if release assets are replaced. Exact inspected compressed-file SHA-256 values:

| Artifact | SHA-256 |
|---|---|
| `observations-2026-09-16.json.gz` | `84a4aef9351c947e7c570e15143d101fa6e1cc385cf7cdb324d95cea13581a6a` |
| `observations-2026-09-17.json.gz` | `b538b13504b245e2c158c7175e6a84b5cd6587897bb53a1b61c80c8d32f163c0` |
| `observations-2026-09-18.json.gz` | `40b4a4916f05f652b3ba38ce8470b096abe9bb22a3955890096f26ead610deb3` |
| Public `700.json` | `3d4a52cac73ee83f05c87a5b44ebb716831e15d199d1d8556c7592d41bdf4674` |

The 18 September observations were built at `2026-09-18T23:44:51+00:00`; the inspected service 700 document at `2026-09-18T23:45:01+00:00`.
