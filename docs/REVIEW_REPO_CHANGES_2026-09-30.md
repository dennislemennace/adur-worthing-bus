**Repository review — 30 September 2026**

Compared the previous review's commit `3c71322d1f3f154603f6d18ecc3501507116026f`
with `61b5fdf` (31 commits; 77 changed files). This review concerns repository
behaviour, not a verified inventory of what is currently deployed. The previous
review is [here](REVIEW_DATA_METHODS_2026-09-25.md).

The work improves timetable coverage, lossless delivery, recovery of missed
recording days, stop information and disruption notices. However, the cleanup
now risks breaking the live evidence catalogue, and the new annotations are
neither fully reproducible from the archive nor visible beside public results.
Those three issues should be addressed first.

P1 means high priority because it undermines published evidence or its
availability. P2 means a demonstrated correctness or disclosure issue to fix
in the next batch. Reproductions below use local fixtures and mocked upstreams;
they establish failure conditions, not how often real visitors encounter them.

1. **P1 — Cleanup can delete files still referenced by the live catalogue.**

   Locations: [prune_published.py](../scripts/prune_published.py#L43),
   [publication_bundle.py](../scripts/publication_bundle.py#L116), and
   [the upload workflow](../.github/workflows/process-snapshots.yml#L291).

   The pruner protects the newest seven generations, the live generation and
   its predecessor. But each new index inherits older `artifacts` entries.
   Daily summaries, daily observations and previous monthly rollups can still
   point into a generation outside the retained seven. Protecting the current
   directory does not protect those dependencies. The saved rollback index has
   the same dependency problem. Pruning also runs before uploading and switching
   the new generation, so a subsequent upload failure does not undo deletions.

   **Reproduced:** a ten-generation listing, with an artifact in the live index
   pointing at generation one, causes generation one to be selected for deletion.
   This was a prospective risk in the previous review; it is now automated.

   **Fix and why:** make the live and retained rollback manifests the roots of
   a dependency inventory. Retain all reachable artifact generations until
   artifacts are migrated to separately retained immutable storage or retained
   builds become self-contained. Simply protecting every inherited reference
   forever would defeat the storage budget, so migration/explicit retention is
   part of the fix. Before deleting, validate the candidate deletion set against
   both manifests; after publication, verify referenced objects and hashes.
   Test an old daily summary and an old monthly rollup through a rollback. This
   preserves the site's checkable evidence as well as its latest charts.

2. **P1 — The new builders cannot run from the promised replay archive.**

   Locations: [provenance manifest](../scripts/process_snapshots.py#L1098),
   [archive assembly](../scripts/archive_reliability_evidence.py#L26),
   [journey builder import](../scripts/build_journey_times.py#L33), and
   [delay builder import](../scripts/build_delay_hotspots.py#L20).

   Both builders now import `analysis_exclusions`, but that module is absent
   from `code_sha256`, which controls archive contents. Its inputs,
   `data/analysis_exclusions.json` and stop coordinates from `data/stops.json`,
   are also absent. Recovering source from Git is possible while the commit is
   retained, but that is a different recovery path from a self-contained,
   hash-verified archive. Using today's event registry could change old results.

   **Reproduced:** copied the exact code-file list specified by the current
   manifest into an isolated directory and ran the archived journey builder's
   `--help`. It exits with `ModuleNotFoundError: No module named
   'analysis_exclusions'`, before processing any data.

   **Fix and why:** archive and hash the new module and every effective input
   to derived publications, including the event registry and coordinates.
   Record these at publication level as well as day level: a later rebuild can
   apply newly recorded events to old observations. Add a smoke test that runs
   both builders from an extracted archive without access to the checkout, then
   compares derived values, event markings and lineage. Test results should
   ignore intentionally variable generation timestamps. This makes the
   reproducibility claim testable rather than dependent on the working tree.

3. **P1 — Diversion markings are invisible in the public performance answers.**

   Locations: [journey marking](../scripts/build_journey_times.py#L262),
   [delay marking](../scripts/build_delay_hotspots.py#L124),
   [browser timings](../app.js#L1861), and
   [delay explanation](../app.js#L4911).

   The builders retain affected observations by default and attach journey
   `exclusions` / cell `excluded_by` metadata. That is a reasonable way to
   preserve evidence. However, `journeyTimesBetween` does not carry the event
   information into its timings and the map explanation never reads
   `excluded_by`. A labelled experimental view still needs to distinguish
   diversion conditions from ordinary operation.

   **Reproduced:** a sufficient delay cell with all 40 traversals marked as
   affected renders exactly the same explanation as the cell without markings:
   it still says what buses “usually” lose. No claim is made here that a current
   production cell has that exact composition.

   **Fix and why:** carry event IDs through timings, filtering and exports;
   publish the corresponding event descriptions and dates with the build;
   show affected counts next to the result and map explanation. Offer a clearly
   labelled normal-operation comparison, recalculating sample floors and
   coverage when it excludes events. Preserve the inclusive result too: people
   travelling during the closure need it. Do not silently delete inconvenient
   delays. The reader must be able to distinguish a temporary diversion from
   evidence about the usual road or service.

4. **P2 — Live estimates use wall-clock arithmetic across the clock change.**

   Location: [live_eta.py](../api/live_eta.py#L122); also audit the related
   clock-based lateness calculation in [main.py](../api/main.py#L2827).

   `scheduled` is a London `ZoneInfo` datetime. Adding a `timedelta` to it
   performs local-clock arithmetic, which does not reliably represent elapsed
   seconds through the repeated autumn hour. Comparing/folding such values can
   also affect which stops are marked passed.

   **Reproduced:** on 25 October 2026, a call scheduled for
   `01:54+01:00` with 1,200 seconds of lateness becomes `02:14+00:00`.
   Those instants are 4,800 seconds apart: the requested 20-minute delay has
   become an 80-minute delay. The correct instant is `01:14+00:00`.

   **Fix and why:** retain the matched journey's service day and use UTC/epoch
   instants for scheduled times, lateness addition and passed-stop comparisons;
   convert to London time only for display. Include both occurrences of the
   autumn hour, spring transition, and trips whose GTFS times exceed 24:00.
   This prevents an hour's error in passenger-facing estimates.

5. **P2 — A slow planner response can overwrite the newer journey search.**

   Location: [planner submission handler](../app.js#L11824).

   Each submit writes its eventual response directly into `plan-results`.
   There is no request generation/ownership check, including in the error path.

   **Reproduced:** submit A→B, then A→C; resolve C first and B second.
   The destination field remains C while the result shows A→B. Existing
   departure-board ownership tests do not exercise this new form.

   **Fix and why:** give each request a sequence token and allow only the latest
   request to update results or errors. Invalidate outstanding results when the
   inputs change; abort obsolete fetches where possible. Include the chosen
   endpoints in the result heading. Test reversed completion and a late failure
   so a passenger cannot mistake another journey's advice for their selection.

6. **P2 — Planning “now” asks for journeys up to five minutes in the past.**

   Location: [planner cache/time selection](../api/main.py#L1676).

   The backend floors the current time to a five-minute boundary, then sends
   that floored value to the routing provider. Neither backend nor frontend
   removes options which have already departed. Cache sharing is changing the
   question asked, rather than just reusing a still-valid answer.

   **Reproduced:** with the local clock fixed at 14:04:59, a request for now
   sends `time=14:00` to the mocked provider. It can therefore return a journey
   which left at 14:01. The test establishes the requested time; it does not
   claim a particular live provider response.

   **Fix and why:** request the actual departure time and, on cache reuse,
   discard options that can no longer be started, accounting for the first
   walking leg. Fetch again when no usable option remains. Test the beginning
   and end of a cache period and midnight. A useful plan must still be catchable.

7. **P2 — The planner's request cap and cache do not cover several real cases.**

   Locations: [get_plan](../api/main.py#L1682) and
   [DailyQuota](../api/journey_planner.py#L34).

   Concurrent cache misses all call the provider. All caught HTTP/parse errors
   except 429 refund the request counter, even after a response was received.
   Thus this is not a cap on outgoing requests. This does not establish how the
   provider bills failures; it establishes that local accounting cannot enforce
   the advertised request budget. Date/time regexes also accept impossible
   values such as `99:99`, which can generate these upstream errors.

   **Reproduced:** five simultaneous identical searches make five upstream
   requests. With a local cap of two, five sequential mocked HTTP 400 responses
   all reach the provider and leave the recorded allowance at two.

   **Fix and why:** use the existing shared in-flight-request pattern for each
   cache key, with a cache recheck inside it. Count attempted upstream calls
   conservatively; only refund a failure when it is known not to have consumed
   the relevant allowance. Validate real dates/times before sending. Briefly
   cache failures/back off. Confirm counter persistence in the deployment and
   use shared atomic storage before adding processes or instances. Test bursts,
   HTTP failures, malformed responses and restart behaviour. These controls
   preserve the small allowance for actual passengers. The older TransportAPI
   overlay has analogous cache-miss/refund behaviour and warrants the same audit.

8. **P2 — Fare ingestion accepts future and expired prices as current.**

   Locations: [validity selection](../scripts/build_fares.py#L158),
   [table retention](../scripts/build_fares.py#L188), and
   [fare lookup](../app.js#L12406).

   `parse_table` searches globally for the latest `FromDate` not later than
   today. It does not enforce the applicable period's end date or reject a
   wholly future tariff. If all start dates are future, it emits a table with
   `valid_from=None`; the builder still retains it and the UI can quote it.
   Taking a date from an unrelated frame also does not establish that a price
   is currently valid.

   **Reproduced:** the existing adult-single fixture with its only start date
   changed to 2999 is retained. A fixture with an explicit validity interval
   ending in January 2000 is also retained. This demonstrates the parser gap;
   it does not prove a checked-in current price is wrong.

   **Fix and why:** resolve validity at the relevant frame/tariff/product/price
   scopes, retain effective start and end dates, and select the interval covering
   the date being quoted. Reject or clearly mark ambiguous/inactive prices;
   expose unavailable evidence rather than claiming a fare is in force. Test
   future-only, expired, overlapping, and nested periods with deterministic
   dates. Otherwise a scheduled fare refresh can silently change present-day
   advice to a price that does not apply.

9. **P2 — The public planner adds an undocumented location-sharing path.**

   Locations: [location capture and request](../app.js#L11810),
   [upstream coordinates](../api/main.py#L1689), and
   [privacy notice](../privacy.html#L77).

   The planner's “My location” captures device coordinates, sends them to
   `/api/plan` to five decimal places, and the backend forwards the coordinates
   to Buses & Trains. The privacy page describes local-only “Stops near me” and
   does not describe this new provider or flow. The local-only statement remains
   true for that separate stop-finding feature; the problem is the missing
   explanation for the planner. Coordinates also occur in the API query string,
   so logging configuration matters.

   **Fix and why:** explain this transfer beside the planner's location control
   and in the privacy notice, identify the recipient and purpose, and document
   actual log/retention behaviour. Minimise precision where routing permits and
   prevent unnecessary coordinate logging. Keep manual stop selection available.
   This is a source-level data-flow/disclosure finding, not a legal compliance
   opinion or a claim about the provider's undocumented retention policy.

**Status of the earlier review**

| Earlier issue | Current status and proposed next step |
| --- | --- |
| Compact files awaiting review/deployment | The codec and corresponding validation are now integrated in the repository. Round-trip coverage is in the passing suites. This review did not verify the deployed version. |
| Raw days deleted before archival; no recovery of missed nights | Improved: recorder retention consults the published source catalogue, and a successful processing run dispatches catch-up work. Test the complete failure/recovery path in an isolated environment and alert when publication stops. |
| Recorder advertised as a hard spending cutoff | Still open: `withinBudget` accepts missing measurements and does not reject stale `measured_at`. Add bounded handling for accounting failures and monitor combined storage. |
| Simple mixes flagged/unverified evidence into passenger guidance | Defaults still use `quality: all` and `identity: all`. Detailed is now public, but availability of filters does not repair the default. Establish an eligible passenger cohort, retain flagged evidence for inspection, and state uncertainty next to the result. |
| Coverage uses a different cohort/date range from the headline | Relevant coverage function/call remain. Show scheduled → tracked → measured endpoints → eligible timings with explicit common dates, including scheduled days with no usable measurements. The old live counts were not remeasured this turn. |
| Declared journeys lost by the schedule-overlap resolver | Reproduced again: the delayed declared trip loses all five reports while the other retains ten, despite disjoint actual tracks. Resolve declared conflicts using actual reports/continuity, with explicit quarantine reasons. |
| An operator can disappear without failing publication checks | Reproduced against the archived 24 September input using current validation: remove all SCSO observations, zero its measured cohorts, regenerate the summary; both checks still pass. Add per-operator/feed/service coverage gates and explain degraded publication. This did not exercise a remote upload. |
| Long gaps disappear from the frequency badge | Reproduced again: departures at 08:00, 08:10, 17:00, 17:10 return `plenty`, every 10 minutes. Include long daytime gaps and service span, distinguish burst-only services, and report Saturday/Sunday separately where materially different. |
| Retention dependencies | Worsened from a proposed-policy risk to executable deletion; finding 1 above. |

**Missing features and improvements worth doing next**

- **Independent measurement validation before stronger evidence claims.**
  Detailed and the delay map are now public. An experimental label does not
  validate the location-radius proxy for actual arrivals/departures. Sample
  declared identities and stop timings across operators, terminal dwell,
  diversions and night services against independent evidence; publish error
  rates and eligibility rules. Keep departure lateness separate from running
  time. The old review's distinction between leaving a 150 m radius and arriving
  at a stop still applies.
- **Operational evidence rather than more feature switches.** Add an alert for
  an old public build, a recorder pause, oldest unprocessed day and shrinking
  storage runway. Exercise replay and rollback on isolated copies first, and
  verify a full 35-day workload for memory, processing time, output size and
  mobile download cost. The runbook still says remote recovery has not been
  exercised; this review did not perform it.
- **Disruption precision.** The live notice begins at 07:00 on 28 September,
  whereas its permanent analysis entry begins at 00:00. Derive both from one
  source or record why their scopes differ. Analysis marking currently uses the
  first observed time of a whole journey; for events starting mid-journey,
  intersect the affected call/traversal times with the event interval. Preserve
  the operator's notice snapshot and checked date for later verification.
- **Public planner usefulness.** Expose departure date/time controls already
  supported by the API, carry relevant closure notices into plans, and state any
  coach/reservation restrictions if such legs are returned. Test walking access,
  rail connections and last services with known examples before presenting the
  planner as a substitute for the existing journey checker.
- **Accessibility and maintainability.** Complete the recorded keyboard,
  screen-reader and mobile audit and publish an accessibility statement. Add
  the planned install manifest/structured metadata if those remain desired
  features. Gradually extract pure timetable/evidence and planner logic from
  `app.js` into modules, preserving tests; the new planner's missing request
  ownership shows the cost of implementing similar interactions separately.

**Validation and limits**

- Full Python suite: **606 passed**. The sandboxed test-client run stalled and
  was interrupted; the authorised unrestricted run passed in 8.36 seconds.
  Existing deprecation/schema warnings remain.
- JavaScript and Worker suite, using `--test-isolation=none` for individual
  test reporting: **639 passed**. The first file-isolated run also passed all
  27 discovered files.
- Additional isolated probes demonstrated findings 1–8 as described; finding
  9 follows the explicit browser → backend → provider argument flow. Rechecked
  the earlier operator-loss gate, declared-trip resolver and frequency examples.
- Temporary reproduction scripts: `/tmp/review30_probes.py` and
  `/tmp/review30_probes.mjs`; outputs and suite logs are in `/tmp/review30-*`.
  Fixtures deliberately include synthetic failure cases, not asserted live
  operating conditions.
- No paid/public transport API requests, browser visual audit, production
  rollback, deletion, timetable rebuild, publication, commit or deployment was
  performed. Application code and pre-existing untracked files were preserved;
  this report is the only repository addition from this review.

Recommended order: repair retention and archive completeness; expose diversion
scope and resolve the earlier evidence gates; correct passenger estimates,
planner request handling and fare validity; then complete operational drills
and the accessibility review.
