# Claude Code handoff: data, publication, visual tools and delay hotspots

Saved 22 September 2026. Reviewed application HEAD: `ec9f12c7940ca0ba4bd452cb33ebf67ba8e0c28a`. The review covers **56 commits / 47 files since `fb33311`**, following the 19 September review. Reconcile this baseline with the current working tree before implementing anything.

The user's goals are accurate, usable, provable bus data; a powerful, intuitive visual tool; and finding sections where a particular service repeatedly gains delay, especially at peak times. They asked to preserve the review, proposed resolutions and lessons from Open Innovations for Claude Code, and assess the new hotspot idea.

## Read first and current status

Implementation update: [23 September local changes, validation and release prerequisites](IMPLEMENTATION_DATA_UI_2026-09-22.md). The findings below describe the reviewed baseline, not the updated working tree.

- [Full review, findings 1–14 and reproductions](REVIEW_DATA_UI_2026-09-22.md).
- [Earlier review](REVIEW_DATA_UI_2026-09-19.md), for the original concerns and context. The latest review records which were resolved or remain open.
- [Delay-hotspot feasibility, evidence and implementation proposal](reliability/DELAY_HOTSPOT_FEASIBILITY_2026-09-22.md).
- [Exact reviewed input hashes](reliability/review-2026-09-22-manifest.sha256), [offline diagnostic](reliability/delay_hotspot_feasibility.py) and [saved diagnostic result](reliability/delay-hotspot-feasibility-2026-09-22.json).
- [Working instructions](../CLAUDE.md), [operations](RUNBOOK.md), [resource limits](../LIMITS.md), [evidence provenance](../.claude/skills/evidence-provenance/SKILL.md), and [site conventions](../.claude/skills/bus-site-conventions/SKILL.md).

**Completed:** review, proposed resolutions, reference comparison, local feasibility diagnostic and this handoff. **Open:** all 14 findings below at the reviewed HEAD; production implementation of the additional recommendations and hotspot feature. Nothing in this handoff marks a defect fixed or a dataset corrected. Keep the journey-time evidence in preview while its release prerequisites remain unresolved.

Application code, workflows, public data and deployment were not changed for this handoff. A local offline diagnostic was added under `docs/reliability/`; `CLAUDE.md` links here. No commit, push, deployment, full timetable rebuild or public submission was performed. Preserve existing uncommitted review files and unrelated edits. The old `.HANDOFF.md` and `docs/REVIEW_FOLLOWUP.md` concern earlier work and are not substitutes for this review.

## Suggested implementation order

1. Preserve available source evidence immediately, within a measured storage budget (7). Repair time origins, effective timetable coverage, RT processing and visit matching (1, 6, 2, 3). Archive what remains before retention removes it; do not assume expired reports can be recovered.
2. Build a complete validated, immutable publication with truthful provenance and denominators (5, 7, 9). Reprocess affected periods from retained inputs, recording unavailable periods explicitly.
3. Correct misleading journey answers and interactions (4, 8, 10, 11, 12, 13, 14). These UI/API fixes can proceed independently where they do not depend on a new data schema.
4. Add the evidence, sharing and exploration improvements below. Pilot hotspots after the relevant measurement corrections; do not turn the current diagnostic into a public league table.

Choose coherent batches with focused regression checks. A change to matching/time schemas must carry through processing, statistics, published formats and UI rather than stopping at a passing helper test.

## Every review finding: resolution and acceptance

Numbers are the stable identifiers in the full review; source locations and reproduction details are there.

1. **P1 — Midnight time origins.** Use UTC instants plus explicit service day/timezone, or one consistent service-day origin throughout. Keep trip-instance and call-sequence identity. Validate sequence before any sorting; rebuild affected consecutive days and derivatives. **Accept:** a trip spanning two snapshot folders has correct ordered calls and elapsed times, including London DST transitions and GTFS times beyond 24:00. The published 23-hour local-trip example is corrected or explicitly quarantined, not hidden with a duration cap.

2. **P1 — Unprocessed overnight RT.** Process GTFS-RT vehicle positions independently of SIRI XML. Resolve report timestamp, freshness and service instance; merge compatible reports without requiring both feeds. **Accept:** an RT-only night fixture yields a measured journey, stale/repeated reports are handled explicitly, and per-feed/hour counters distinguish fetch, parse, fresh and usable results. Rebuild available nights; report missing ones.

3. **P1 — Wrong repeated visit.** Align candidate visits against the complete ordered route, with loop/parallel-road handling and uncertainty. A globally closest first-stop sample must not anchor the track to a later pass. **Accept:** the A→B→C→near-A reproduction retains the original A/B/C visits; ambiguous tracks have visible rejection reasons and source evidence. Retain monotonic validation without presenting discarded evidence as successful measurement.

4. **P1 — Alternative fare context.** Calculate each alternative using its own operator, full stop path, interchange legs, zones and ticket restrictions. Drive map, itinerary and ticket explanation from the selected alternative. **Accept:** the existing configured-price BHBC fixture yields 630p with either representative option selected, rather than 1,000p under SCSO context; include alternatives crossing different intermediate zones. This checks calculation, not current market prices.

5. **P1 — Publication before validation.** Assemble observations, daily/monthly statistics, index and service documents into one candidate generation. Validate before any public replacement; publish immutable objects, then switch one manifest pointer. Preserve the previous build and rollback target. **Accept:** validation failure changes no public pointer; interrupted upload and cached readers cannot combine generations; incomplete historical downloads cannot silently replace a complete month with a smaller one.

6. **P2 — Weak timetable coverage guard.** Select an exact historical bundle and check effective calendars and relevant operator/route coverage. Distinguish no scheduled service from missing timetable evidence. **Accept:** an old removal exception cannot make a future-only calendar valid for that old day, and an unrelated calendar cannot mask missing target services. Test real no-service days and unexpectedly depleted cohorts.

7. **P2 — Missing durable replay chain.** Preserve compressed raw reports, or complete replayable evidence for each derivation, with exact timetables, full hashes, code commit, schema/method versions and configuration. Link chart → trip/call → observations → reports through immutable revisions. **Accept:** an archived selected journey can be reproduced without today's feed or timetable; corrections create a new revision and disclose the old one. Show storage/object/request costs before expanding retention. A checksum alone does not preserve evidence.

8. **P2 — No recordings presented as no service.** Separate date-specific timetable connectivity from measurement coverage. **Accept:** the service-25 George Street→Moulsecoomb Way reproduction explains the lack of recorded data and offers the timetable; it does not claim a change is required without a connectivity check. Distinguish loading, missing coverage and genuine no-connection states.

9. **P2 — Misleading methods and uncertainty.** Carry source method versions forward; generate truthful descriptions of declared/inferred matching, departure estimates and interpolation. Derive uncertainty from report intervals/visit evidence; show selected-cohort coverage, included denominators, build time and downloads. **Accept:** mixed input methods cannot acquire one false version; measured-only statistics and provenance use the same rows; unresolved zero intervals are distinguished from negative contradictions. Do not apply the inferred matcher’s censoring claim to all declared journeys or promise universal ±30-second accuracy.

10. **P2 — Wrong “Departed” field.** Carry observed/scheduled times at the selected origin and destination through chart, point details, table and export. Label whole-trip start separately. **Accept:** choosing an intermediate origin gives the same observed departure on the axis, tooltip and table, including a trip that crosses midnight.

11. **P2 — Missing fastest alternative.** Preserve candidates best on elapsed time and fare-relevant dimensions before limiting the response. Operator overlap cannot decide whether interchange search is worthwhile. **Accept:** the 50-minute cross-operator fixture survives four slower same-operator options; show search anchor/limits and qualify “quickest/cheapest of options found” until a stronger guarantee is justified. Two operators do not necessarily require two tickets.

12. **P2 — Map-pair errors and races.** Capture selected endpoints and request generation; check view ownership after awaits. Handle failures with a retry preserving selection; ignore obsolete responses. **Accept:** rejected fetch, rapid pair changes, clear and navigation cannot leave a spinner stuck or overwrite the current selection.

13. **P2 — Mobile chart interaction.** Use responsive tick density and effective rendered font sizes; provide expand/time-range controls and deliberate tap/keyboard point details. Preserve the accessible table and non-colour-only markers. **Accept:** inspect actual readability and touch interaction on a 390px phone and larger layouts, in both themes. SVG fitting its container alone is insufficient.

14. **P2 — Deduplication drops operator identity.** Include operator and appropriate route/stop-pattern equivalence when deduplicating departures. **Accept:** simultaneous service-1 departures to the same headsign from BHBC and SCSO both survive, while known duplicated publications of the same coach service still collapse.

## Additional recommendations that also remain in scope

- **Feed joins:** resolve SIRI/RT joins using compatible report timestamps and service instance, not vehicle ID and download minute alone. Retain the matching evidence and test old/stale reports sharing an ID.
- **Collection health:** expose expected, fetched, parsed, fresh, matched and measured counts by feed/hour/cohort. Parse errors and repeated positions cannot count as usable coverage. Missing observations do not prove cancellation or punctuality.
- **Evidence controls:** measured-only default for hotspot evidence; declared/inferred controls with counts; explicit interpolation and uncertainty markers; full build/method/input identity. “Declared” is currently a journey-level label and does not certify every point.
- **Metric clarity:** label elapsed duration, extra section time and departure/arrival punctuality separately. Ten minutes late at both endpoints means zero lateness gained on that section.
- **Exploration:** place/service search, direction, swap/reset, route geometry linked to the chosen stops, selected-point details, and a usable mobile expanded view. Preserve existing operator separation, table, legend and focusability.
- **Sharing and export:** a share link must restore operator/service, ordered endpoints, dates/hours, metric, evidence filters and immutable dataset version. CSV and source JSON must match the visible cohort, with provenance alongside the export.
- **Fair comparisons:** separate weekdays/weekends, timetable changes, route patterns and matching methods. Check coverage and comparable hours before comparing operators or areas. Short samples and method changes cannot establish long-term reliability or a causal effect of a fare/administrative boundary.
- **Pipeline assurance:** add frozen input fixtures spanning recording→processing→statistics→publication→UI, including missing feeds and partial failures, rather than relying only on helpers or a permissive publication validator.

## Lessons from Open Innovations: taken, partial and missing

Reference: [live bus-tracking prototype](https://open-innovations.github.io/bus-tracking/) and [reviewed source at `4a0e0b5`](https://github.com/open-innovations/bus-tracking/tree/4a0e0b5967d3e5698742d88f3cb04c7d46d97987). Status means the capability is present here, not proof it was copied from that project.

| Lesson | Status in this project at the reviewed HEAD | Follow-through |
|---|---|---|
| Explore journeys between selected stops against their own timetable | Present, with remaining defects | Keep per-journey schedule comparisons; correct metric labels and selected-stop departure fields. |
| Use declared trip identity where supplied | Partial | Identity is used, but RT-only periods and timestamp-compatible joins remain incomplete. Its stop sequence/status method cannot be assumed available in our feed. |
| Interpolate only between observed endpoints, respecting schedule proportions | Present in principle | Preserve measured/estimated distinction; interpolation cannot identify where within the gap congestion occurred. |
| Unambiguous timestamps | Missing end to end | Adopt UTC instants with explicit service day and London display semantics; resolve finding 1. |
| Linked map/graph, detailed schedule/observed point information | Partial | Improve chosen-route linking, paired detail, responsive interaction and metric explanations. |
| Shareable route/stop state | Incomplete | Persist all evidence filters and immutable dataset version, not just a landing view. |
| Validated reconstructed GTFS and accessibility/isochrone analysis | Not implemented; later extension | Useful future direction in its workflow, not a prerequisite for the hotspot pilot. |
| Reproducible, atomic, continuously updated public evidence | Still required here | The reference is a prototype with a fixed sample, not a demonstrated production solution to retention or publication guarantees. |

Do not copy its equal arrival/departure timestamps, browser-local timezone handling, or silent straight-line speed exclusions without evaluation. Those choices can conceal dwell or unusually slow journeys—the phenomena the user wants to study. Preserve this project's sample floors, operator identities, accessible table and explicit estimate markers. Fares, alternative ranking and coach deduplication are separate local concerns; the reference does not resolve them. See the full review's pinned notebook/browser links for supporting code.

## Verification and evidence handoff

The earlier full review at this same HEAD recorded **426 Python, 545 JS and 461 browser checks passing**, plus targeted defect reproductions. These are historical results, not checks rerun for this documentation task and not proof that the reported defects are absent.

For this follow-up, the five exact observation files were checksum-verified and analysed locally; diagnostic arithmetic, time-window boundaries, changed-input rejection, script/helper hashes and example denominators were checked. Local document links and whitespace checks passed. All 44 reviewed public inputs were copied out of temporary storage and checksum-verified at:

```text
~/.cache/adur-worthing-bus/review-2026-09-22/observations/
~/.cache/adur-worthing-bus/review-2026-09-22/published/
```

These are local copies of public **derived** assets, not the original raw SIRI/RT reports. They are outside Git and are not a remote backup. On another machine, recover the exact assets from the release/public URLs in the review and require their hashes to match; changed URLs are not equivalent inputs. Earlier reproduction scripts and screenshots remain under `/tmp/adur-review22-*` and should not be assumed durable. Promote useful reproductions into focused regression fixtures during the relevant fixes.

After implementation, report **implemented**, **verified locally**, **verified in deployment**, and **deferred/unavailable** separately. Include affected historical rebuilds and remaining release prerequisites. Follow `CLAUDE.md` for proportionate checks; rendered changes require the browser harness. Do not commit, push, deploy or perform a full timetable rebuild without the user's corresponding instruction.

Suggested next Claude Code task:

> Read this handoff, the full review and the hotspot feasibility report. Reconcile them with current HEAD and preserve unrelated changes. Work through the ordered corrections in coherent batches, reproducing each bug before fixing it. Carry source identity and quality through the entire data path. Keep the additional recommendations and hotspot prerequisites visible in the completion checklist; report local verification, data rebuilds and deployment status separately.
