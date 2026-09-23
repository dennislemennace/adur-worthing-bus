# Data and visual-tool implementation

Updated 23 September 2026 against application baseline `ec9f12c7940ca0ba4bd452cb33ebf67ba8e0c28a`. This is the implementation and validation record for the [Claude Code handoff](CLAUDE_HANDOFF_DATA_UI_2026-09-22.md). The original reviews remain historical records.

**Implemented and verified locally; not committed, deployed or verified in production.** The journey-time tool remains behind `?preview=1`. Historical derivative repair is not raw-feed rematching. The machine-readable [validation record](reliability/implementation-validation-2026-09-23.json) preserves the input hashes, counts and tested code hashes.

## Findings 1–14

| Finding | Implemented resolution | Local evidence / remaining release requirement |
|---|---|---|
| 1. Mixed midnight origins | Method 4 uses explicit GTFS service-day origins and UTC instants, with London display times. Legacy rows are normalised using their recording date; ambiguous DST evidence is flagged. Calls retain route sequence. | Midnight trips across two recording folders tested on ordinary, spring and autumn dates. Historical rebuild has no backwards or >12-hour adjacent pairs. Raw historical matching is still unverified. |
| 2. Night RT-only recordings omitted | Process the union of SIRI and RT recording times, including RT-only positions. Recorder keys include UTC minute identity so repeated autumn clock minutes cannot overwrite each other. | RT-only processing through archive/publication fixture; stale join and recorder DST regressions. Deploy recorder and processor together. |
| 3. Wrong first-stop visit | Route-wide ordered visit alignment replaces independent closest-first selection; ambiguous alignments, weak boundaries and report intervals retain quality flags. | Original visit versus closer return regression. Independent checks against actual journeys are still required. |
| 4. Fare inherits another alternative's route | Each option is priced from its own operators, legs and restrictions; selecting an option updates itinerary, map and fare evidence together. | BHBC 630p regression and rendered cheapest/quickest route selection. |
| 5. Partial/mixed publication | Validate the complete candidate before publication; upload immutable content-addressed generations; read back all objects; change only the root manifest after success. | Invalid candidate, immutability and workflow ordering tests. Remote credentials/upload/failure recovery were not exercised. |
| 6. Misleading timetable coverage | Check real calendar/addition dates and local operator/service cohorts. Removal exceptions and an unrelated calendar cannot validate a date. | Regression tests and actual local timetable check. **Current local build fails the 23 September all-cohort guard: BHBC 25X has only 27 September–3 October coverage.** An appropriate archive or reviewed explicit partial-cohort policy is needed; there is no override that publishes zeroes. |
| 7. Evidence disappears | Exact raw reports, timetable, code hashes, configuration and runtime versions are linked to observations and bounded replay archives. Observation history and publication generations are immutable; dated timetable releases are retained. | Archive content/hash mutation, changed/missing source rejection and old-month restoration tests. Existing raw recordings outside retention cannot be recovered from these derived files. |
| 8. Missing recordings imply no bus | Empty map-pair results explain recording/date/direction limits and link to the timetable checker. | UI regression and browser checks. |
| 9. Method/provenance overclaims | Preserve each source's actual method, measured/estimated and declared/inferred composition, matching limits, quality flags and coverage. Suppress thin samples consistently in text and JSON. | Statistics, query and publication tests. Legacy evidence remains labelled legacy. |
| 10. Wrong departure time | Chart, table, details, hour filters and exports use departure at the selected origin, with trip-origin time retained separately. | Selected-origin 08:22 regression and browser details. |
| 11. Fastest alternative discarded | Rank by actual elapsed time before preference scores; preserve operator combinations; search interchanges even when direct routes exist. Explain the bounded candidate search. | 50-minute quickest option regression. This is not an exhaustive global fare search. |
| 12. Map-pair races/errors | Request ownership and selected-pair checks prevent stale completion from replacing newer selections; errors offer retry. | Fetch/retry and UI checks. |
| 13. Unusable mobile chart | Responsive plotting dimensions and readable labels; selectable keyboard/touch points, 44px targets, paired timing details, time filters, expanded view and accessible table. Extra controls are grouped to reduce phone clutter. | Focused 44/44 and full 498/498 browser checks across five viewport sizes. |
| 14. Cross-operator departure deduplication | Deduplication includes operator, service day and ordered stop pattern. | Operator/pattern board regression and API suite. |

## Additional handoff recommendations

Timestamp-compatible SIRI/RT joins require compatible position and unique service identity; an old report cannot donate its trip identity to a fresh position. RT start date/time and unsupported schedule relationships are checked. Unresolved declared trips may remain flagged exploratory inference, but cannot qualify for strict hotspot evidence.

Collection metadata includes expected/fetched/parsed/fresh counts by feed/hour, matched report counts, measured calls and operator/service/day scheduled-versus-observed cohorts. “Fetched” counts stored objects, not upstream request attempts. Recording spans may contain gaps; these counters do not establish the fraction of every selected section's scheduled trips observed.

The visual tool offers measured-only defaults, identity/quality/cohort filters, three distinct metrics (elapsed section time, extra section time, destination lateness), measured/estimated markers, selected-point evidence, source downloads, immutable-build share links and filtered CSV/JSON exports. Pattern, timetable and matching-method cohorts remain distinguishable; mixed-cohort warnings explain the limitation. Existing map stop selection, service/direction browsing, reset paths and accessible table remain. Fare-map geometry follows the selected itinerary.

The broader exploration recommendation is **partial**: a dedicated reverse-stop/swap action, enhanced place/service search and historical road geometry linked to each measured traversal remain follow-up work. A present-day route line must not masquerade as archived road-level evidence. A separate departure-lateness chart is not provided; departure lateness is available in paired point details and exports.

Pipeline fixtures now cover raw RT → matching → observations → journey derivatives → validation → replay archive → immutable bundle, plus missing feeds, stale joins, DST, changed/missing archived input and invalid publications. Browser fixtures exercise the consumer separately. No live end-to-end recording/publication run was performed.

## Delay-hotspot readiness

`scripts/build_delay_hotspots.py` prepares evidence between adjacent measured timing points. It requires method 4, declared identity, matching operator/service/direction/pattern/timetable/method, ordered adjacent calls, consistent vehicle, report references and bounded endpoint intervals. Flagged or missing intermediate calls cannot be skipped to manufacture a longer eligible section.

A traversal measures signed lateness gained: exit lateness minus entry lateness. A bus ten minutes late at both ends gains zero minutes on that section. Output includes inherited delay, duration, conservative report-interval bounds, journey/day counts, daily recurrence and ten-minute exceedances. Peak/off-peak and weekday/weekend cohorts are explicit. Thirty journeys over five distinct days is a sample floor, not certification: every cell remains `publishable: false` while scheduled coverage and independent validation are unresolved.

The retained 16–20 September legacy inputs yield **zero eligible hotspot traversals**. Of 63,463 candidate timing-point pairs, 63,388 are excluded for legacy/unknown method and 75 for duplicate call identity. This establishes that old evidence is unsuitable for strict hotspot claims; it does not establish an absence of delays.

Next evidence work: collect/reprocess recoverable method-4 inputs with appropriate timetables, inspect representative declared trips against the raw reports, establish section/time-specific scheduled denominators and missingness, accumulate repeated days, then evaluate geometry and road-level localisation. Stops bound a section, and section time includes dwell/holding; attributing delay to busy traffic needs separate corroboration. Frequency/replacement instances unsupported by the static timetable are excluded rather than forced onto a scheduled trip.

## Historical rebuild and verification

Five exact retained observation files (357,859 rows; 66,114 measured timing points) were used. The current timetable supplied stop display locality labels only; it was not substituted for historical matching data.

| Published derivative measure | Reviewed | Locally rebuilt |
|---|---:|---:|
| Services | 38 | 38 |
| Journeys | 9,553 | 9,541 |
| Calls | 357,539 | 357,090 |
| Adjacent gaps over 12 hours | 106 | 0 |
| Backwards adjacent calls | 74 | 0 |
| Quarantined journeys | 0 | 12 |

Normalisation and quarantine together produce this result. Quarantine reasons and source identities remain in each derivative. The checker accepts all 39 output files, while explicitly noting 13,061 zero-duration pairs whose schedule advances: minute-resolution reports do not support precise instantaneous departures.

Validation completed locally:

- Python: **455 passed** (492 existing dependency/OpenAPI warnings).
- JavaScript and Worker: **554 passed**, no failures or skips.
- Full browser harness: **498/498 passed**, five viewports; local Turnstile configuration errors were expected and recorded.
- Focused new UI browser checks: **44/44 passed**; old-chart baseline failed the targeted readability/detail checks.
- Both workflow YAML files parsed; all 23 shell blocks passed `bash -n` after the final workflow edits. This checks syntax, not remote execution.
- The compact-input rebuild matches the previous validated rebuild except `as_of`. Five-day rebuild: 6.96 seconds, 851,768 KiB peak RSS (about 832 MiB). Full 35-day method-4 resource use is **not benchmarked**.

See [runbook](RUNBOOK.md) for release/replay steps and [limits](../LIMITS.md) for storage safeguards. Temporary logs/screenshots and derivative outputs are not durable release artifacts; the small saved validation record is the handoff evidence.

## Lessons from the Open Innovations reference

The follow-through adopts the useful principles identified in the pinned review: journey-specific timetable comparison, declared trip identity where verifiable, interpolation visibly distinct from measurement, linked map/details, explicit timestamps and reproducible share state. It adds production-specific immutable publication and replay safeguards that the reference prototype did not establish for this project.

It does not adopt equal arrival/departure timestamps as an observation, browser-local time semantics, silent speed filtering, or interpolation as evidence of where congestion occurred. Reconstructed GTFS, isochrones and accessibility analysis remain later extensions. The reference does not resolve this project's fare rules, coverage bias or road-level causal claims.

## Release prerequisites and remaining limitations

No commits, pushes, deployments, timetable rebuilds or raw private-feed retrieval were performed. No deployed data has been repaired by this local work. Credentials for remote replay/publication were not available in the inspected environment.

Deploy the compatible Worker, API and frontend before running the new processor workflow. Resolve timetable cohort coverage, test a recoverable recording day and its full replay chain, inspect the uploaded manifest/objects, and test rollback before exposing the preview broadly. Historical days need the original reports and exact timetable; unavailable days must retain their legacy/unavailable status. Monitor archive size, published storage growth and 35-day memory before expanding the operational window. The public-storage cap stops publication rather than deleting old shared builds automatically.
