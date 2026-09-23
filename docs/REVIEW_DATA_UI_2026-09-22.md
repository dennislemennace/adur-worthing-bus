# Review of data, publication and visual tools — 22 September 2026

Reviewed all **56 commits from `fb33311` to `ec9f12c`**, following the [19 September review](REVIEW_DATA_UI_2026-09-19.md). This includes collection, processing, statistics, timetable ingestion, publication, journey-time exploration, journey alternatives, fares, coach handling, tests and operational documentation: 47 changed files. Full reviewed HEAD: `ec9f12c7940ca0ba4bd452cb33ebf67ba8e0c28a`.

**Recommendation: keep journey times in preview. The changes materially improve the project, but the published measurements still contain midnight errors, the new overnight recordings are not processed, and some new UI and fare answers are incorrect. The evidence chain also remains insufficient for independently reproducing an old measurement after source retention expires.**

This is a review, with proposed corrections for each issue. Application code, published data and deployment have not been changed.

Follow-up saved: [Claude Code implementation handoff](CLAUDE_HANDOFF_DATA_UI_2026-09-22.md), covering all findings and reference lessons, and [recurring-delay hotspot feasibility](reliability/DELAY_HOTSPOT_FEASIBILITY_2026-09-22.md), with a reproducible local diagnostic. The handoff also records the persistent local cache of the exact reviewed public inputs.

## Findings and proposed corrections

P1: correct before relying on the affected measurements, prices or publication guarantees. P2: a material correctness, usability or reproducibility limitation to address before public release. “Reproduced” below distinguishes observed public defects from controlled examples of code defects.

### 1. P1 — Midnight still combines incompatible time origins

**Locations:** `scripts/process_snapshots.py:541–551`; `scripts/build_journey_times.py:139–161`; `scripts/check_published.py:123–143`.

Observation `day` identifies the service day, but numeric times remain relative to the snapshot folder's calendar day. Combining consecutive folders therefore mixes times around 86,400 with times around zero for the same trip. The builder then sorts by those incompatible scheduled times, moving the after-midnight calls to the front.

**Public evidence:** the five inspected observation files contain 357,859 rows. Grouping by service day/trip and ordering by route-call index finds **118 backwards transitions, all exceeding 12 hours**. The 38 published service documents contain **106 adjacent pairs with apparent positive durations over 12 hours**. The publication validator nevertheless reports `44 published files, nothing contradictory` when checking the index, service documents and five daily summaries.

For example, service 1/BHBC, service day 16 September, journey start 23:21, offers Drove Crescent → Swanborough Drive as **83,986 seconds, about 23 hours 20 minutes**. Calling the actual browser calculation with stop indices 106 and 0 accepts it. This is reversed route order caused by the time origins, not a real overnight journey of that duration.

**Resolve:** store UTC instants plus service day, or consistently normalise every call to that service day's origin. Retain call sequence and trip-instance identity in the chart format. Validate route order before sorting, and flag implausible gaps for investigation. Reprocess affected consecutive days and rebuild all derivatives. Test a trip assembled from two folders, including daylight-saving transitions. A duration cap alone would hide the error.

### 2. P1 — Recording the night does not make it into the measurements

**Locations:** `worker/src/recorder.js:37–43`; `scripts/process_snapshots.py:905–930`; `.github/workflows/process-snapshots.yml:143–158`.

GTFS-RT now records throughout the night, but SIRI-VM still stops between 00:30 and 05:00. Processing iterates only XML snapshots and uses protobuf files solely to attach trip IDs to SIRI vehicles in the same minute. It never processes positions from RT-only minutes; with no XML files it exits altogether.

The inspected 20 September output still reports zero snapshots in hours 01–04. That observation establishes the published coverage gap; the processing loop establishes why the new RT collection cannot close it by itself.

**Resolve:** process GTFS-RT positions directly, with timestamp/freshness checks and service-instance resolution. Merge compatible SIRI reports where useful without making them mandatory. Count expected, fetched, parsed and usable reports separately for each feed/hour. Add a complete RT-only night-journey fixture before rebuilding the affected nights.

### 3. P1 — The monotonic picker can still choose the wrong visit

**Locations:** `scripts/process_snapshots.py:341–386`, `:408–413`.

Constraining subsequent samples and rejecting backwards departures removes the old negative-duration symptom. However, the first stop still chooses its globally closest sample anywhere in the track. If a route later passes slightly closer to that stop, the picker anchors the beginning to the later visit and discards subsequent genuine calls that are now behind its floor.

**Controlled reproduction:** a track passes A within about 11 m at time 0, B at 600 seconds, C at 1,200 seconds, then passes A's coordinates at 1,800 seconds. For calls A → B → C, `advancing_only(arrivals_along(...))` returns only A at 1,800. The initial valid visit and the next two stops are lost. This reproduces the matching flaw; it is not an estimate of how often it occurs in production.

**Resolve:** identify candidate visits, then align the complete track to ordered route calls, distinguishing repeated passes. Use route progression, plausible movement, trip identity and uncertainty together. Quarantine ambiguous tracks with a reason. Retain the monotonic checks as validation, and add a loop/parallel-road regression with a later, closer pass.

### 4. P1 — Alternative fares inherit the representative route's context

**Locations:** `app.js:9194–9219`, `:9309–9315`; `costRoute()` at `:8899–8934`.

`renderJourneyResult()` derives `usable` stops and `endpointOperators` from one representative option, then supplies that same context when costing every alternative. `costRoute()` checks each alternative's operator list, but subsequently checks its tickets against the representative operator and path. An alternative can lose a valid ticket, or be checked against stops it never visits.

**Controlled reproduction using the repository's ticket data:** a two-leg BHBC route within the citySAVER area costs **630 pence** with its own BHBC context. With a representative SCSO context, both stop-coverage lists become empty and the same route costs **1,000 pence**. This is a calculation defect, not a new independent verification of today's ticket prices.

**Resolve:** construct the stop path and operator context separately for every candidate, including each interchange leg. Apply zone, time and service restrictions to that candidate. Drive the map, itinerary, ticket explanation and source links from the selected result. Test alternatives with different operators and different intermediate zones.

### 5. P1 — Validation runs after observations and summaries are published

**Locations:** `.github/workflows/process-snapshots.yml:186–224`, `:260–268`, `:279–285`.

The workflow uploads the observation asset and commits/pushes the daily summary before running `check_published.py`. A failed validation therefore leaves those public outputs changed while journey-time files and the monthly rollup can remain on an older build. The comment promising that a bad run publishes nothing is not true.

Uploading the index last improves one failure mode, but overwriting service files in place and deleting old files still permits mixed generations for cached clients or interrupted uploads. This is a reproducible ordering/design defect; no new failed production release was induced.

**Resolve:** build observations, daily/monthly summaries and journey documents locally, validate the entire candidate, then publish immutable versioned objects. Switch one manifest only after all files are available. Keep the previous generation available beyond cache lifetime and record a rollback target. Do not turn missing downloads into a silently smaller replacement month.

### 6. P2 — The timetable coverage guard is only a min/max date check

**Locations:** `api/timetable_db.py:1109–1146`; `scripts/timetable_covers.py`; `.github/workflows/process-snapshots.yml:79–128`.

The new guard prevents the specific case where the whole timetable starts after the requested date. It does not establish that services relevant to the measurement are covered. It includes all exception dates, even removals, and accepts any day between the global earliest and latest dates. An unrelated calendar can also make a mostly unsuitable bundle pass.

**Controlled reproduction:** a calendar beginning 20 September plus a removal exception dated 18 September makes `covers_day(18 September)` return **True**, although that service does not run and is the only service in the fixture.

**Resolve:** select and retain the historical timetable intended for the observation period. Check effective calendars and relevant route/operator coverage, distinguishing a genuine no-service day from missing timetable coverage. Fail or quarantine unexpectedly empty or sharply reduced cohorts. Keep the date-envelope test as an initial rejection check, rather than the publication guarantee.

### 7. P2 — Published evidence still lacks a durable replay chain

**Locations:** `worker/src/recorder.js:62`, `measureAndPrune()`; `scripts/process_snapshots.py:540–570`; `scripts/build_journey_times.py:139–156`; `.github/workflows/process-snapshots.yml:198`; `.github/workflows/update-timetable.yml:155–165`.

Method versioning is a useful addition, but raw reports still expire after seven days; only five dated timetable releases are retained; daily observation assets are overwritten with `--clobber`. Rows do not identify their source objects/reports or complete matching/interpolation evidence. Journey documents still omit trip IDs and call sequence. Published metadata does not name the processing commit and complete input manifest.

This allows someone to recompute arithmetic from the current observations, but not necessarily to reproduce why a historic bus was assigned to a particular trip and stop. It is an evidence-preservation limitation, not an allegation that data was altered.

**Resolve:** preserve compressed source batches, or sufficient complete source evidence to replay each published derivation, within a stated storage budget. Retain exact timetables and full hashes. Version code, method, schema and configuration. Publish immutable revisions linked chart → trip/call → observations → source reports. A checksum identifies bytes; it cannot replace retaining them.

### 8. P2 — Missing recorded coverage is presented as no direct service

**Locations:** `app.js:2333–2348`, `:2381–2395`, `:2497–2501`.

The entry flow searches only services present in the published measurement index. When that produces no candidates it tells the reader that the stops share no service and changing buses is necessary. A route can exist in the current timetable while having no recorded data in the published window.

**Reproduced in the rendered local tool with current public data:** George Street (`149000007440`) → Moulsecoomb Way (`149000006064`) displays that changing-buses explanation. Their shared service is **25**, absent from the inspected measurement index. The local timetable has seven active direct trip matches for these stop groups on 22 September. Both chosen stops are selectable in the entry layer because they also serve other published services.

**Resolve:** keep timetable connectivity separate from recorded coverage. Say “we have no recorded journeys for this connection in these dates” when that is what is known, and offer the current timetable or broader dates. Only assert that a change is required after checking schedule connectivity under explicit limits.

### 9. P2 — Method descriptions and uncertainty still contradict the data

**Locations:** `scripts/process_snapshots.py:127–160`; `scripts/reliability_stats.py:165–169`; `scripts/query_reliability.py:51–74`, `provenance()`; `app.js:3081–3165`.

Published descriptions still claim nearest-approach timing, roughly ±30-second accuracy, no usable trip identity and universal censoring. The implementation uses the last sample within 150 m, deduplicated operator reports, declared identities and interpolation. The 20 September payload reports a declared share of 99.6%; describing all its matches as inferred is wrong.

The new method number does not correct the prose. Builders import the current `METHOD_VERSION` while `load_observations()` drops source method versions, allowing a future mixed-history build to acquire a single misleading label. All five files inspected here currently say version 3, so mixed versions were not observed in this download.

The UI shows dates and timetable hash prefixes but still omits coverage, method/caveats, build time, match composition and source downloads. CLI provenance also counts the input rows before measured-only statistics exclude estimates. Zero-duration pairs are called “contradictory” in the UI even though the validator correctly distinguishes unresolved intervals from negative durations.

**Resolve:** generate method metadata from the actual source versions and configuration; describe declared and inferred cohorts separately. Derive uncertainty from report spacing and visit boundaries. Show concise coverage/quality information and an expandable explanation with downloads. Report actual included denominators. Label unresolved intervals separately from contradictions and approximate scheduled times separately from observed-time estimates.

### 10. P2 — “Departed” still names the whole trip's scheduled start

**Locations:** `app.js:1709–1734`, `:2810–2823`, `:3081–3084`, `:3140`.

The chart's horizontal position uses observed departure at the chosen origin. The tooltip and table use `t.start`, the scheduled start of the entire trip. Selecting an intermediate stop therefore shows a different time from the one plotted. Even the current default 700 pair has a 17 September row labelled 05:05 whose observed departure is 05:06:04.

**Resolve:** carry observed and scheduled origin/departure and destination/arrival times into each plotted row. Display observed origin departure under “Departed”; label the scheduled whole-trip start separately if useful. Use the same fields in chart details, table and export.

### 11. P2 — “Quickest” can exclude the fastest available candidate

**Locations:** `api/timetable_db.py:1430–1458`, `:1481–1483`; `api/main.py:1650–1669`; `app.js:9019–9042`, `:9486–9489`.

The API retains four candidates ranked by journey time plus walking and operator-change penalties. The browser then labels the fastest remaining candidate “Quickest”. Those are different objectives. A penalty can push the actual fastest journey out of the returned set before the browser sees it. The direct-route shortcut also decides whether to search alternatives using operator overlap, which cannot establish whether an interchange would be quicker.

**Controlled GTFS fixture:** four same-operator options take 55, 56, 57 and 58 minutes; a two-operator option takes 50. `limit=4` returns only the slower four. `limit=10` includes the 50-minute option. This is verified search behaviour, not a claim about the frequency of missed options on the deployed network.

**Resolve:** preserve candidates that are best on elapsed time and on fare-relevant dimensions before applying presentation limits. Avoid assuming a second operator always means a second ticket. Until the search supports the stronger claim, label results as the cheapest/quickest **of the options found**, and expose the anchor time, walking and change limits.

### 12. P2 — The new map-pair lookup has no failure or stale-request guard

**Locations:** `app.js:2307–2365`.

`journeyTimesResolvePair()` awaits the index and route documents without a catch and without the render-ownership guard used by `renderJourneyTimes()`. Rejecting the index fetch leaves “Looking for buses between…” in the result and rejects the promise. Selecting another pair, clearing the selection or leaving the view while requests are pending can also let an older lookup write newer state.

**Resolve:** capture both selected endpoints at request start, use a generation/ownership check after awaits, and handle errors with a retry action that preserves the selection. Cancel or ignore superseded lookups. Test an unavailable index, failed service document and rapid selection changes.

### 13. P2 — Mobile chart labels remain too small to analyse comfortably

**Locations:** `app.js:2756–2825`; `style.css:6455–6464`.

The desktop layout is substantially improved: the focused probe rendered a chart 720 CSS px wide. The fixed 640-unit SVG still scales its 11-unit labels to approximately **6.7 CSS px on a 390 px phone**. The mobile screenshot confirms very small axes. Focusable points and the visible legend are improvements, but point details still rely on SVG titles/labels; there is no deliberate tap-selected detail view or time zoom.

**Resolve:** make tick density and label sizes responsive, provide an expanded chart/time-range view, and show selected-point details on tap or keyboard activation. Preserve the accessible table. Check effective rendered text size and touch interaction, not just whether the SVG exists and fits the viewport.

### 14. P2 — Departure deduplication drops operator identity

**Location:** `api/main.py:2754–2772`.

The new coach deduplication key is service number, headsign, departure second and service day. Two operators using the same number and destination at the same time collapse into one row. That repeats the operator-identity problem correctly fixed in the journey-time publisher.

**Controlled reproduction:** two trips, one SCSO and one BHBC, both service 1 to Town Centre at 12:10, produce one departure. No current public timetable collision was established; the fixture demonstrates the regression condition.

**Resolve:** include operator and an appropriate route/stop-pattern identity in the equivalence key. Collapse known duplicate publications of the same service while preserving distinguishable buses. Add the two-operator case alongside the existing duplicated-coach tests.

## What changed since the previous review

| Previous concern | Current assessment |
|---|---|
| Recorder object budget | Fixed in configuration and tests: 30,000 objects accommodates the intended two-feed retention window. Deployed bucket occupancy was not inspected. |
| Interpolated timing-point flags | Corrected in code and covered by tests; downloaded observations have been rebuilt with method version 3. |
| Estimates entering punctuality statistics | Corrected in statistics and rollups; CLI provenance and methodology still need alignment. |
| Negative journey times | No negative adjacent durations found in the inspected service documents. Visit selection and midnight defects remain, findings 1 and 3. |
| Midnight origins | Unresolved, finding 1. |
| Live lateness using retrieval time | Corrected to use operator report time, with report age exposed. Nearest-stop deviation remains an estimate. |
| Declared-trip inference-window censoring | Corrected; the declared path now uses all trip instances. |
| Missing nights | Collection improved and hourly coverage added; processing remains incomplete, finding 2. |
| Method and evidence explanation | Version number added; substantive problems remain, findings 7 and 9. |
| Wrong “Departed” field | Unresolved, finding 10. |
| Misleading single timetable line | Default now compares each journey with its own scheduled duration; optional duration line explicitly says median. |
| Thin samples/global date claims | Percentile floor and actual contributing dates added. Missing coverage is still not adequately exposed. |
| Operator/direction ambiguity | Service/operator files, locality-based destinations and reachable stop pairs are substantial improvements. Trip/call identity is still absent from chart documents. |
| Small chart/interaction | Desktop space, focusable points and legend improved; mobile readability and touch details remain, finding 13. |
| Stale monthly rollup | Nightly rebuild added; publication still needs one coherent validated generation. |
| Timetable chosen for historic days | Date-envelope guard and archived fallback added; finding 6 explains its limits. |

The CI exception for the quoted-evidence check is scoped to the prebuild caller; the workflow performs the check again after regenerating the statistics. That sequencing is reasonable. Coach ingestion now retains the additional Brighton stops and the UI separates coaches from local ticket recommendations. The review found the narrower deduplication problem above, not a reason to undo those additions.

## Comparison with Open Innovations

Inspected the live tool, rendered its X70 Harrogate–Wetherby example, and read the repository at **`4a0e0b5967d3e5698742d88f3cb04c7d46d97987`**, including preprocessing, interpolation, browser code and document generation. Its notebooks were inspected, not executed end to end. The live site explicitly describes itself as a prototype using a fixed September 2024 sample. Its map tiles displayed an API-key warning during this visit; route selection and the chart still rendered. [Live example](https://open-innovations.github.io/bus-tracking/?itl=TLE&service=x70-harrogate-tadcaster&start=3200YND10690&end=450014896).

| Area | Reference approach and implication for this project |
|---|---|
| Trip/stop matching | Its current pipeline uses GTFS-RT stop sequence/status; its older approach used distance/bearing. Your [local feed probe](reliability/gtfs-rt-probe.md) found those stop fields absent in all 259 sampled vehicles. Prefer declared trip identity plus validated route progression; do not assume sequence-based matching can be adopted unchanged. |
| Time representation | Its web schema uses Unix timestamps. Adopt unambiguous instants/service days here to prevent finding 1, while retaining explicit London display semantics. |
| Interpolation | Both approaches estimate missing stops using timetable proportions between observations. Useful for exploration; estimates are not independent measurements of congestion at the missing stop. Preserve endpoint flags and allow measured-only exploration. |
| Chart explanation | Paired actual/scheduled points make changing schedule allowances visible. Detailed point information separates start/end times, duration and arrival lateness. Your default duration difference is valid, but needs an explicit explanation that it is not arrival punctuality. |
| Exploration | Borrow the shareable route/stop selection, linked point details and route geometry. Keep your operator separation, counts, accessible table and non-colour-only estimate markers. |
| Broader analysis | Its workflow continues to validated reconstructed GTFS and OpenTripPlanner accessibility/isochrones. That is a possible later extension; first establish reliable stop-call evidence and reproducible journey statistics. |

Sources: [reference README and schema](https://github.com/open-innovations/bus-tracking/tree/4a0e0b5967d3e5698742d88f3cb04c7d46d97987), [workflow](https://github.com/open-innovations/bus-tracking/blob/4a0e0b5967d3e5698742d88f3cb04c7d46d97987/WORKFLOW.md), [interpolation notebook](https://github.com/open-innovations/bus-tracking/blob/4a0e0b5967d3e5698742d88f3cb04c7d46d97987/pipelines/gtfsrt2gtfs_interpolation.ipynb), [browser implementation](https://github.com/open-innovations/bus-tracking/blob/4a0e0b5967d3e5698742d88f3cb04c7d46d97987/docs/resources/buses.js#L159).

The reference should not be treated as validated ground truth. Its interpolation notebook assigns the same chosen timestamp to arrival and departure and has a departure-interpolation TODO. Its browser uses local `Date` clock/day methods and silently filters points outside a straight-line speed range of 1–100 km/h. Those choices need evaluation before reuse: genuine slow journeys, loops and dwell are relevant evidence here. Prefer explicit quality flags and exclusion reasons over silently removing inconvenient points.

A crucial distinction for the UI: a bus leaving ten minutes late and arriving ten minutes late can take exactly its scheduled duration. A zero in the current “Against the timetable” chart therefore means **no extra journey time between those stops**, not an on-time arrival. Label the zero line accordingly and offer arrival/departure punctuality separately when the evidence supports it. The reference's [explanatory notes](https://open-innovations.github.io/bus-tracking/#notes) make this distinction explicit.

## Recommended method and product direction

1. **Preserve a stop-call evidence record.** Include trip instance, operator/route, service day, call sequence, scheduled and observed instants, source report IDs/times, vehicle identity, match method, estimated status, uncertainty bounds and rejection reasons. Resolve feed joins by compatible timestamps and service instance, not vehicle ID alone.
2. **Make collection quality part of every answer.** Distinguish fetch failure, parse failure, stale/repeated reports, missing trip identity, unobserved calls and unscheduled service. Show coverage for the chosen hours/days/operator, not merely whole-file snapshot totals.
3. **Validate and version the full publication.** Make frozen input fixtures exercise recording → processing → statistics → publication → UI. Preserve the inputs used for public claims and maintain an immutable revision chain.
4. **Make the selected journey the centre of the tool.** Add place/service search, direction and swap/reset, readable responsive charts, linked selected-route geometry and tap/keyboard point details. Distinguish journey duration, extra running time and punctuality.
5. **Make findings shareable and inspectable.** Persist operator/service, origin, destination, date/hour filters, metric and dataset version in a link. Offer the filtered table as CSV, the source JSON and a compact evidence/method panel. Add measured-only and declared/inferred controls with visible counts. Compare matched cohorts before drawing conclusions across days or operators.

Existing cross-feed joins still discard RT timestamp/start-date information; a same-minute download is not proof that both feeds describe the same report. Existing parse failures can also count towards snapshot coverage. These earlier hardening points remain relevant to steps 1 and 2. A five-day sample with changing declared-match coverage is useful exploration, but does not by itself establish representative long-term reliability or causation for a fare/administrative boundary.

## Verification, inspected data and limits

- Python: **426 passed** (`.venv/bin/python -m pytest -q`). Warnings included framework deprecations and duplicate OpenAPI operation IDs.
- Frontend/Worker JavaScript: **545 passed**, no failures (`node --test --test-isolation=none 'tests/*.mjs' 'worker/test/*.js'`).
- Browser: **461/461 passed** with the existing harness, local preview and headless Chrome across five viewport sizes. Additional focused probes inspected desktop/mobile charts and reproduced the false connectivity message. The chart probe used exact downloaded public documents in the browser cache for a stable comparison. Local Turnstile error 110200 occurred; submissions were not tested.
- Inspected all **38 service documents**, their index, and the five public observation assets for **16–20 September**. The service documents contain 9,553 journeys and 357,539 calls. Index build time: **21 September 2026, 16:07:29 UTC**. These are data-quality diagnostics, not replacement reliability estimates.
- Public journey documents contained **0 negative adjacent durations**, **13,070 zero-length adjacent pairs**, and **106 adjacent gaps exceeding 12 hours**, using increasing scheduled-time order. Zero intervals were counted separately, not declared impossible.
- The current publication validator passed the downloaded service documents/index and five checked-in daily summaries. Its passing result does not cover the defects above.
- Read recent successful processing workflow history. Did not alter or replay production workflows, private R2 objects, upstream feeds or deployed permissions. Passing local tests does not establish deployment parity.
- Local timetable used for current connectivity checks: SHA-256 `c4a758f1abfd1d1fa6276ce58fea4fb7e19ccd7c0060391d210c3995c485e8f3`; its service window begins 21 September. It was not used as a substitute for the historic timetable when diagnosing midnight observations.
- Synthetic reproductions exercised visit selection, the calendar guard, alternative fare context, itinerary ranking, failed map lookup and departure deduplication. They establish concrete failure conditions, not production incidence rates.
- Current fare prices, every fare-polygon vertex, actual bus ground truth and the reference's full research pipeline were not independently revalidated. The fare regression uses the project's existing configured prices. No full timetable rebuild, commit, push, deployment or public submission was performed.

Public inputs: [observation release](https://github.com/dennislemennace/adur-worthing-bus/releases/tag/reliability-2026-09), [journey-time index](https://adur-worthing-submissions.dennislemennace.workers.dev/journey-times/index.json), [service 1/BHBC](https://adur-worthing-submissions.dennislemennace.workers.dev/journey-times/1-BHBC.json), [service 700/SCSO](https://adur-worthing-submissions.dennislemennace.workers.dev/journey-times/700-SCSO.json). These URLs are mutable; the [SHA-256 manifest](reliability/review-2026-09-22-manifest.sha256) identifies all 44 downloaded files used in this review.

Temporary evidence is under `/tmp/adur-review22-*`: suite logs, downloaded files, diagnostic/reproduction scripts, browser captures and reference inspection. The report and checksum manifest are the durable review artifacts. The earlier review remains untouched.
