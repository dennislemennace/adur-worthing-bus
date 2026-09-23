# Feasibility: recurring delay locations and peak-time patterns

Assessed 22 September 2026 against application `ec9f12c7940ca0ba4bd452cb33ebf67ba8e0c28a`, using the five reviewed observation assets for **16–20 September 2026**. This is a local diagnostic and feature proposal, not a published finding about road congestion. See the [Claude Code handoff](../CLAUDE_HANDOFF_DATA_UI_2026-09-22.md) for implementation priorities and the [full review](../REVIEW_DATA_UI_2026-09-22.md) for unresolved measurement defects.

**Feasible:** identify directional sections between stops where a particular service repeatedly gains delay, then compare times of day and similar operating days. Much of the required data and the core calculation already exist. **Not established from the present evidence:** which precise road/junction caused a recurring ten-minute delay, whether traffic rather than dwell/holding/matching error caused it, or how representative five days are.

## What the question requires

For a traversal from A to B:

```text
extra section time = observed duration A→B − scheduled duration A→B
                   = lateness at B − lateness at A

A: 2 minutes late; B: 12 minutes late → 10 minutes gained on A→B
A: 12 minutes late; B: 12 minutes late → 0 minutes gained on A→B
```

Ranking stops by total lateness would blame downstream stops for delay inherited from elsewhere. Use the change in lateness to locate the section where time was lost, and show inherited lateness separately.

Also offer **actual peak versus comparable off-peak section duration**. A busy road may take longer at rush hour while the timetable already allows for it; such a section can have little extra time against schedule. These answer different questions. Compare matched operator, direction, route pattern, stop pair and day type, with visible differences in schedule allowance and coverage. Do not subtract two independently computed medians and label that the median of paired per-trip delay gains.

For “regularly ten minutes”, show the number of eligible traversals gaining at least 600 seconds divided by all eligible observed traversals, plus the number of contributing days and daily recurrence. Missing buses must not enter that denominator as successful trips. A percentile or isolated worst trip cannot establish frequency.

## Existing foundations and gaps

| Component | Existing support | Required work |
|---|---|---|
| Collection/observations | Positions, report-derived stop times, service/operator, trip/day, ordered calls, scheduled/observed seconds, matching label, timing-point and estimate flags | Correct midnight, visit matching and RT-only processing; retain source reports, service-instance identity and uncertainty. |
| Section statistics | [`segment_stats()`](../../scripts/reliability_stats.py) already computes median/p90 lateness gained between consecutive **observed** timing points, excluding interpolated observations by default | Use operator, ordered stop IDs, route pattern, comparable days, timetable/method versions and quality cohorts; add recurrence, coverage and retained evidence. |
| Query/rollup | [`query_reliability.py`](../../scripts/query_reliability.py) supports segment/hour queries; the inspected monthly rollup contains 261 segment entries | Do not confuse existing output with validated road-level hotspot findings. Keep thin cells visible as insufficient evidence, with consistent JSON/text suppression. |
| Visual tool | Stop-pair journey duration/difference chart and map selection | There is no integrated hotspot map/ranking consuming these section statistics. Add linked section selection, time/day controls and an evidence detail view. |
| Location | Ordered stop IDs; timetable stops and some route shapes | Resolve exact historical route geometry. Distinguish verified path from indicative geometry; do not highlight a short road more precisely than observations justify. |

Two method details need correction before reuse:

- Existing segment keys use **stop names and broad direction**, combining services/operators/patterns where these coincide. They do not establish that each comparison follows the same road. The current diagnostic keeps ordered ATCO codes and separates operator, service, headsign, call count/indices and versions; that is still a pattern proxy, not a shape validation.
- `--from-hour`/`--to-hour` filter individual rows by scheduled hour **before** pairing. `--segment-hours` subsequently buckets by the downstream observed hour. This can discard endpoints and mix time conventions. Form whole traversals first, then apply an explicit exposure-time rule. The pilot uses observed entry time; show entry/exit times for sections spanning windows. A scheduled-departure view can be a separate passenger-oriented filter.

The current times approximate **departures**, using the last report within a stop radius. A departure-to-departure interval includes intermediate dwell and downstream-stop dwell. It is not a clean measurement of traffic-only running time. Where arrival and departure cannot be distinguished, describe it as section time and do not infer a dwell-free road speed.

## What the actual five-day diagnostic found

The [saved result](delay-hotspot-feasibility-2026-09-22.json) carries computation time, full input hashes, input timetable/method versions, script/helper hashes, denominators, exclusions and caveats. Its [script](delay_hotspot_feasibility.py) refuses inputs that differ from the [review manifest](review-2026-09-22-manifest.sha256).

The five inputs contain **357,859 stop rows**, of which **66,114 are non-interpolated timing-point rows**. Their composition changes substantially:

| File date | Non-interpolated timing-point rows | Of those, labelled declared |
|---|---:|---:|
| 16 September | 6,591 | 0 |
| 17 September | 16,263 | 0 |
| 18 September | 17,355 | 10,804 |
| 19 September | 15,572 | 15,451 |
| 20 September | 10,333 | 10,287 |

These are row counts, not independent buses or evidence of accuracy. “Declared” is currently a **journey-level** tag: one declared match can label the journey; it is not per-stop proof. Wednesday is partial, and the window contains only three weekdays. Method version is 3 in all five files; the input timetable identifier changes on Sunday. The exact metadata is retained in the result.

Running the existing measured-only westbound **700** segment/hour calculation gives **198 cells, all suppressed** by its existing floor of 30 traversals and five journeys. The current data does not support a detailed hourly ranking for that service even under that relatively modest test; those five journeys are not five days.

The stricter diagnostic forms same-file, same-service-day measured timing-point pairs, requires increasing call order and positive scheduled/observed duration, and limits both endpoints to scheduled and observed **06:00–22:00**. It partitions timetable/method, operator/service, direction, headsign, match label, call count, ordered ATCO/call indices, weekday/weekend and observed entry window. Windows are 07:00–10:00, 10:00–16:00, 16:00–19:00, and other daytime; these are trial settings, not an official definition of peak hours.

It retains **51,079 traversals in 9,298 cells**. No cell meets a proposed exploratory minimum of 30 distinct journeys across five comparable days. This is expected with only three weekdays and two weekend days; it is evidence about sample sufficiency, not evidence that there are no hotspots. It separately reports 93 candidate pairs with nonpositive time/sequence, 5,042 outside the daytime scope, and 213 whose service day differs from the file day. Excluding these bounds the exercise; it does not repair upstream defects or validate the remaining times.

### A concrete investigation candidate, not a congestion conclusion

For **46 / BHBC / westbound, Holmbush Centre (`4400AD0222`) → Southwick Square (`4400AD0259`)**, consider weekday entries between 10:00 and 16:00, headsign Southwick Square, 67-call trips, indices 58→66, method 3 and input timetable identifier `f1daaae3ed886a18`:

| Current match label | Dates and measured traversals | Median extra section time | Traversals gaining ≥10 minutes |
|---|---|---:|---:|
| Inferred | 17–18 September; 23 journeys | 725 seconds (12m 05s) | 16 / 23 |
| Declared | 18 September; 6 journeys | 70 seconds (1m 10s) | 0 / 6 |

The figures are computed per traversal, with medians over those values; the JSON includes daily numerators/denominators and all other cells for this pair. These groups do **not** represent the same buses or identical operating times/dates. The difference could reflect traffic, matching error, selection effects or several factors. It neither proves that declared matches are correct nor that the road is congested. This example was selected for investigation from a high existing aggregate, so it is also subject to selection bias.

Both groups span eight stop-index increments. They cannot identify one road or junction within that interval. Inspect retained reports, the historical route, terminal dwell and trip assignments before interpreting it geographically. Crucially, the 10:00–16:00 example does not demonstrate a peak-specific effect.

## Proposed method and evidence requirements

1. **Stabilise the measurements.** Resolve review findings 1–3, 6–7 and 9; preserve what remains now. Keep every included/excluded call's trip instance, route-call identity, times, source interval, match decision, estimate flag and quality reason. Quarantine ambiguity rather than manufacture precise stop times.
2. **Choose defensible sections.** Start between measured timing points on a verified directional route pattern. Prefer adjacent scheduled timing points; if an intermediate point is missing, either exclude or explicitly label the longer span. Use ordinary stops later only where scheduled times and observed crossing precision support the intended metric. No interpolated points as independent evidence of a hotspot's location.
3. **Build a traversal table.** One row per trip instance and ordered section, including observed/scheduled entry and exit, elapsed times, signed delay gain, inherited lateness, timetable/method/build IDs and quality. Preserve negative gains as recovery. Separate operators, branches and loops before aggregation; do not allocate one uncertain long-span loss to every road it crosses.
4. **Measure recurrence and coverage.** Report median/p90 gain, ≥600-second numerator/denominator, journeys, distinct comparable dates, daily results and scheduled-versus-measured coverage. Distinguish unavailable observations, confirmed cancellations and trips outside the selected scope. Missing very delayed trips can bias results downward; inferred identity errors can create false large gains in either direction.
5. **Compare matched windows.** Start with broad peaks/off-peak and collect roughly four to six weeks of comparable operating days. A starting display floor of 30 journeys on at least five comparable dates is a product guard, not statistical proof. For stronger conclusions, account for dependence within days, assess day-to-day consistency and validate candidates on later dates. Check school days, holidays, works/diversions and timetable revisions. Small samples may need much longer.
6. **Validate location and explanation.** Sample matched traces against independent observations or operator timing records. Corroborate congestion explanations with time-aligned traffic, roadworks or signal evidence and checks for dwell/holding. Disappearance under a better matcher, failure to recur on later comparable days, or unchanged section time despite downstream lateness would weaken/refute the hotspot claim.
7. **Publish a reproducible finding.** Include the selected corridor/cohort, metric and denominators, quality, uncertainty, dates, immutable build/input identity, filter state and downloadable traversal evidence. Use language such as “additional time recorded between A and B”; reserve causal statements for a suitable study with independent corroboration.

An optional feed field is not guaranteed evidence: the [GTFS-RT VehiclePosition reference](https://gtfs.org/documentation/realtime/reference/#message-vehicleposition) defines timestamp, stop-status/sequence and congestion fields, but their availability must be checked in the actual feed. The [local probe](gtfs-rt-probe.md) found no stop sequence/status/stop ID in its 259 sampled vehicles. The existing data does not establish a usable historical congestion feed. No new external traffic integration was tested here.

## A useful first UI

Add a **“Where do buses lose time?”** view alongside the stop-pair explorer. Begin with one chosen operator/service/direction and broad weekday windows, rather than a network-wide ranking that mixes incomparable services.

- Show directional route sections on the map, paired with a sortable table of median extra time, ≥10-minute share, contributing journeys/days and coverage. Thin or unobserved sections have an explicit insufficient-data state, not a zero-delay colour.
- Selecting a section opens the existing journey chart for its endpoints, with matched peak/off-peak views, per-day recurrence and a clear explanation of inherited versus newly gained delay. Keep section names and values available without colour.
- Offer separate metrics for **extra section time**, **actual duration**, and **punctuality at a stop**, with plain labels. A threshold control for five/ten/fifteen minutes changes the frequency calculation and visibly states the denominator.
- Provide tap/keyboard-selected journey details, readable mobile charts, measured-only and match-quality controls, and a concise evidence panel with exclusions and uncertainty.
- Preserve the complete selection and build ID in a share link; export precisely the visible cohort as CSV plus source/provenance JSON. Show geometry as approximate when the historical path cannot be verified.

The [Open Innovations browser code](https://github.com/open-innovations/bus-tracking/blob/4a0e0b5967d3e5698742d88f3cb04c7d46d97987/docs/resources/buses.js) offers useful linked stop selection, schedule/observed detail and shareable-state examples. Its [workflow](https://github.com/open-innovations/bus-tracking/blob/4a0e0b5967d3e5698742d88f3cb04c7d46d97987/WORKFLOW.md) is a reference for later analysis, not proof that this project's proposed hotspot method has been validated. Do not copy silent speed filtering that could remove the very slow journeys of interest.

## Architecture, costs and scope

Precompute traversal summaries in the existing offline processing workflow; publish compact versioned section/cohort JSON. Let the existing static UI load the selected service/build. The basic pilot needs no new live feed and no heavy always-on query endpoint. Precomputation is a natural fit for the repository's current architecture, but production runtime, memory, payload size and storage must be benchmarked against measured volumes.

The diagnostic processed the five local files in approximately **2.2 seconds on this machine**. That demonstrates a small offline analysis is practical; it is not a production benchmark for corrected matching, route-shape alignment or larger archives.

The recorder currently enforces **seven-day retention, a 4 GiB byte cap and 30,000 objects**. `LIMITS.md` estimates about 392 MB written per day. At that estimate, 28 days of the present raw format would be roughly **11 GB**, beyond the current cap. Do not simply extend retention. Measure compression/deduplication and preserve compressed historical source batches and exact timetables within an explicit budget; aggregates alone cannot replay matching decisions. Those are repository settings/estimates, not newly verified provider prices.

Recommended deliverable sequence: correct and preserve evidence → one-service section/time-window diagnostic → validate candidates over more dates → linked map/table/chart preview → publish claims only with the evidence chain and adequate coverage. Use 700 as an initial user-facing corridor; investigate the 46 example as a matching-quality check, without presupposing either contains a real recurring ten-minute road delay.

## Reproduce this assessment

The 44 reviewed public assets have been checksum-verified and preserved locally outside Git at `~/.cache/adur-worthing-bus/review-2026-09-22/`, split into `observations/` and `published/`. They are derived public outputs, not the original feed reports. This cache avoids relying on `/tmp`, but is neither committed nor a remote backup. Public recovery URLs are in the review; exact hashes must match because those URLs can be overwritten.

From the reviewed checkout:

```sh
.venv/bin/python docs/reliability/delay_hotspot_feasibility.py \
  --observations "$HOME/.cache/adur-worthing-bus/review-2026-09-22/observations" \
  > /tmp/delay-hotspot-feasibility-rerun.json

.venv/bin/python scripts/query_reliability.py \
  --observations "$HOME/.cache/adur-worthing-bus/review-2026-09-22/observations" \
  --by segment --segment-hours --service 700 --direction westbound
```

The second command's cell suppression is useful, but its current footer provenance counts rows before all statistical exclusions (review finding 9); do not reuse that footer as the measured traversal denominator. The saved diagnostic records its own included counts. A rerun's `as_of` changes; arithmetic on the same code and input bytes should agree.

Verified locally: all five input hashes; full diagnostic execution; inherited-versus-gained delay arithmetic including recovery; window boundaries; changed-input rejection; script/helper hashes; selected-cell daily numerators/denominators; local document links and whitespace. Application/browser suites were not rerun because rendered and production application code was unchanged. No raw-feed reprocessing, exact historical shape validation, ground-truth congestion validation, new UI, deployment or long-term recurrence study was performed.
