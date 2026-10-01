# September 30 review fixes

These are working-tree changes against `61b5fdf`, following the
[repository review](REVIEW_REPO_CHANGES_2026-09-30.md) and its carried-forward
data-quality findings. Nothing was committed, pushed, deployed or published.
Existing logo files and review documents were preserved. The project conventions
and evidence-provenance instructions were used.

## October 1 review refinements

The earlier patch is retained with these requested corrections:

- Delay cells pool **only methods 5 and 6**, retaining `method_versions` and
  `method_family` in both the full evidence and compact map. v6 changes declared
  track eligibility, not the timing of retained traversals. Method 4 and unknown
  future versions stay separate; methods below 4 remain excluded. Tests cover
  pooling, sample thresholds and incompatible versions at both resolutions.
- Simple uses a closed **How we measure** disclosure for eligibility, coverage
  and stop-area timing limitations. Coverage dates use a short range, and the
  headline says **Allow X**, qualified by “9 in 10 recorded journeys took this
  long or less” and the observed count that took longer. The disruption note
  and comparison control remain visible. Detailed retains its evidence wording.
- `service_origin_epoch` remains internal to vehicle processing and is cleared
  if the heading check rejects the declared journey.
- Tickets route/stage lookup and final fare display share journey costing's
  validity check. Inactive routes disappear from choices, expired selections
  are refused, and an empty selection cannot accidentally select table zero.
- Privacy copy now says access logging is **configured** to be disabled and
  explicitly says the running Render setting has not been confirmed.
- Recorder fail-closed behaviour is unchanged. Pause visibility/alerting is an
  explicit follow-up: the Worker has no existing health endpoint, while the API
  health output runs in a different service with no recorder-state binding.
  Connecting these would require a new status interface and freshness contract,
  rather than simply adding a cheap field. No per-minute KV writes or API polls
  were introduced. Future status must carry the last reason and observation
  time, treat stale/unknown data as such, and use bounded writes; it does not
  replace an alert for unattended pauses.

The stricter passenger cohort and per-minute planner cache key remain deliberate
trade-offs: fewer eligible samples and potentially fewer cache hits against the
250-attempt daily planner cap. No commit or deployment is authorised by this
refinement request.

October 1 verification: the full Python suite passed **622 tests** (existing
deprecation/schema warnings remain). The requested full Node/Worker suite,
run with `--test-concurrency=1` at reduced priority, passed **28 test-file
groups**; the affected journey/fare/UI run passed **137 tests**. Browser
verification passed **226/226 checks** across all five viewports, including
closed measurement disclosures and passenger copy. Chrome and both local
servers were automatically closed. `git diff --check` passed. These are local
checks; no deployed Render setting or published data was changed or verified.

## Changes and reasons

| Change | Why and implementation |
| --- | --- |
| Evidence-safe retention | `prune_published.py` reads the indices of all retained builds before selecting individual obsolete objects. Exact older evidence dependencies survive; obsolete large route files can still be retired. The workflow no longer recursively deletes whole old generations, and warns/skips cleanup if verification fails. Live/rollback and seven recent builds remain protected. |
| Replay inputs | `replay_inputs.py` snapshots and hashes the builder code, event registry, stop coordinates and supporting inputs. Daily provenance includes them, and each new publication carries its own `analysis-inputs.json`. Restoration validates every hash and path before writing; an isolated smoke test runs both builders without the checkout. This prevents today's annotations being substituted silently for an old publication's inputs. |
| Visible disruption evidence | Journey timings retain event IDs; Simple shows affected counts and descriptions/dates, with an explicit comparison excluding recorded disruptions. Detailed has an event filter; CSV includes event IDs and shared links preserve the choice. Delay-map explanations show affected traversal counts and build-local event descriptions. Inclusive evidence remains available; the copy does not claim that every marked delay was caused by the event. |
| More precise event timing | Builders intersect each measured traversal interval with event dates instead of testing only a journey's first sighting. Legacy ambiguous timestamps are not replaced with invented noon times. The permanent Western Road entry now starts at 07:00, matching the existing notice. |
| Passenger evidence defaults | Simple and Detailed default to measured endpoints, declared identities and no quality flags. Flagged/inferred evidence remains available through Detailed filters. Location-report timing is described as a stop-area/departure proxy, and the 90th percentile is described as an observation rather than an instruction guaranteeing how long to allow. |
| Explicit coverage | Coverage separates scheduled, tracked, measured-endpoint and eligible counts using the recorded schedule dates, including zero-observation days. It states that the coverage uses scheduled departure hours while the chart uses observed hours. Schedule matching deduplicates trip identities. |
| Frequency gaps | Gaps over 90 minutes are no longer discarded. An all-day service with separated peaks is labelled irregular with its longest gap. Deliberately omitted hours in a selected period do not become artificial gaps; differing Saturday/Sunday frequencies are shown separately. |
| Declared trip overlap, method 6 | The resolver compares actual track overlap for two declared journeys, so a delayed trip is not discarded merely because scheduled windows overlap. Inferred identities retain the conservative timetable check. The published method version and description change; original observations retain their old versions. |
| Missing-operator gate | Summary and publication validation stop complete loss of a known operator with at least ten scheduled journeys inside the recording span. Another operator's healthy data can no longer hide that failure. This is a minimum gate, not proof of route-level completeness. |
| DST estimates | Live estimates add lateness and compare passed stops using epoch seconds. A journey keeps one service origin; the repeated autumn hour no longer turns a 20-minute delay into 80 minutes. Both repeated-hour cases have regressions. |
| Planner response ownership | Searches, edits and geolocation callbacks have request ownership. An old success or failure cannot overwrite a newer destination's results. |
| Planner departure time | “Now” rounds forward to a provider-supported minute, carries midnight into the date and removes already-departed immediate options. Calendar/time values are validated. Labelled date and UK-time controls expose the existing API capability. |
| Planner quota | Simultaneous identical requests share one upstream call. Failed attempts remain counted, successful answers cache for five minutes and failures back off for 30 seconds. Coordinate precision matches the cache key and transmitted value. |
| TransportAPI quota | Stop-board calls also coalesce by stop and back off for 30 seconds after failures. Attempts are not refunded speculatively. The SIRI diagnostic shares the stop-board gate; REST diagnostic failures retain their counted attempt and no longer return raw exception URLs. |
| Disconnect handling | The shared asynchronous producer owns its cache and cleanup. A first waiter disconnecting cannot remove the in-flight entry and trigger another upstream call for a later waiter. |
| Fare validity | Ingestion rejects inactive validity scopes before resolving prices and records start/end dates and the checked date. The browser rejects tables outside their recorded dates, including cached tables that have expired. Missing end dates in older data cannot be reconstructed without a fare rebuild. |
| Location disclosure | Planner copy and the privacy page explain coordinates going to the site's API and Buses & Trains, with manual stop selection retained. Coordinates are rounded to four decimals. Render's API command disables access logging; HTTP client info logs are suppressed. Infrastructure/provider retention remains unverified and the notice says so. |
| Recorder budget | Missing, invalid, unavailable or stale usage pauses writing. Budget checks reserve 142 objects/142 MiB for the maximum interval between measurements; individual feeds are capped at 1 MiB. Missing accounting can bootstrap; stale accounting waits for hourly measurement. Tests cover fail-closed behaviour. |
| Touch targets | The disruption comparison gets the required 44-pixel minimum. Date/time inputs use the existing styled input class. Browser checks cover the new controls and disclosure. |
| Documentation and regressions | `LIMITS.md` and the runbook explain the changed retention, replay, quota, method and recorder behaviour. Tests reproduce the reviewed failures; existing tests were updated for deliberate defaults/copy changes. Historical reviews remain historical. |

## Verification

The initial complete Python suite passed **618 tests**. After the TransportAPI
and disconnect changes, the affected backend suites passed **59 tests**.
The final affected pipeline/replay/retention/fare suites passed **145 tests**.
Existing FastAPI/Starlette deprecations and duplicate-operation-ID warnings
remain. The sandbox blocks the local API test client; stalled sandbox runs
were stopped and the successful runs used the approved unrestricted environment
at reduced priority.

The complete JavaScript/Worker run exercised **644 tests**: 643 passed and one
expected old-copy assertion failed. That assertion was updated for the observed
percentile wording, and the affected journey/fare/UI suites passed. A new fare
expiry regression was demonstrated failing before its fix. The affected
journey/fare/UI run passed **135/135**, and the recorder's affected suite passed
**51/51**. Unaffected tests from the complete run remained passing; they were
not repeatedly rerun merely to obtain a single combined total.

The first real-browser journey run passed **206/211** checks across five
viewports and exposed the comparison button's small touch target. The fix is
included. Browser runs use two CPU cores, low CPU/disk priority, two renderer
processes and an automatic timeout/cleanup wrapper. Only task-owned processes
are stopped; paid API credentials are blank during browser verification.
The corrected journey/planner browser run passed **221/221** across mobile,
narrow phone, landscape, tablet and desktop. Chrome and both local servers
closed automatically after each run. This used the harness's existing
`--journey-review` path; a full-site browser/WCAG audit was not performed.

## Release requirements and remaining review scope

- **Not deployed:** site, API, Worker and workflow changes need their normal
  release process. Rebuild fares and affected reliability data from verified
  inputs; method-6 code does not retroactively correct existing publications.
  Check dependency retention against a live listing before any production cleanup.
- **Independent evidence still needed:** stop-radius timing has not been
  validated against independent door/stop observations across operators,
  terminal dwell, diversions and night services. Stronger accuracy or ordinary
  traffic claims remain unjustified. No historical operator notice snapshot was
  recovered; the registry retains the existing cited source and recorded date.
- **Recovery/capacity not established:** no production rollback drill, full
  35-day memory/time/download measurement or end-to-end failure/catch-up drill
  was performed. The isolated replay test establishes executable inputs, not
  numerical equality for an independently downloaded production publication.
- **Coverage and identity follow-ups:** the new gate catches complete operator
  loss, not partial feed/service degradation. Full monitoring/alerts for build
  age, recorder pauses, backlog and storage runway remain to be implemented.
  Schedule-based coverage is deduplicated, but cross-method/cross-timetable
  midnight fragments still need a reviewed physical-journey identity for all
  aggregate statistics; their evidence has not been silently merged.
- **Quotas are not a billing guarantee:** local files survive only while the
  filesystem does. Durable shared atomic counters are still required before
  scaling to multiple processes/instances or claiming a cap across ephemeral
  redeploys. Cloudflare billing alerts and actual account usage require account
  verification. At the supplied £0.0009/hit, 600 hits cost £0.54; 100 visitors
  alone do not determine hit count.
- **Planner/fare follow-ups:** departure controls are implemented; closure
  notices in returned plans, coach/reservation restrictions and independent
  walking/rail/last-service examples remain. Real operator fare files need
  rebuilding and spot checks; unit fixtures do not establish complete support
  for every NeTEx validity/reference dialect.
- **Broader product work remains:** a recorded screen-reader/WCAG audit and
  accessibility statement, optional install manifest/structured metadata,
  departure-lateness chart, road-following map geometry and gradual `app.js`
  module extraction are not part of this corrective patch. These were review
  recommendations, not silently completed features.
- **Owner/account items remain:** domain verification, licence choice, photo
  provenance, Cloudflare deployment credentials and billing alert. No merged
  branches were deleted, no messages/submissions were sent, and no live
  publication or retained evidence was modified.
