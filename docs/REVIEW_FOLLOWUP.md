# Review follow-up — Claude's working-tree changes

## Subsequent timetable release — 8 September 2026

The user subsequently authorised a rebuild, commit and push to `main`.
Commits `8580bcb` and `b840e19` publish the timetable pipeline fixes, compatible
service-day departure handling and working CI prerequisites; `3f9a8ed` contains
the regenerated evidence. Unrelated pre-release changes remain uncommitted.

[Rebuild 34241330364](https://github.com/dennislemennace/adur-worthing-bus/actions/runs/34241330364)
succeeded and published `timetable-2026-09-08-1458` plus `timetable-latest`.
The downloaded release passes its SHA-256 and semantic checks: 5,374 stops,
188 routes, 35,433 trips, 1,293,910 stop times, **14,098 times at or beyond
24:00**, and **zero trips with decreasing times along their stop sequence**.
The live API reports the matching new reference counts; public evidence now
carries the release hash
`02109739ad1ba3d988368b1693eb0807a2a51305797eafdf11effc76fbd0927e`.
CI passed 82 Python tests (14 skips) and 182 Node tests on the scoped committed
tree. The new release-path checks exercise JSON writing as well as parsing:
they caught and fixed a three-field/two-field unpacking failure missed by the
earlier tests. Tomorrow's service-day departures and clock-change arithmetic
are also covered. No browser suite was rerun for this nonvisual change.

The old local `data/timetable.sqlite` was not replaced underneath the separate
API process on port 8011. A verified new copy is at
`/tmp/adur-timetable-published-20260908/timetable.sqlite`; the LAN preview uses
the updated deployed API. The local database should be refreshed with that
API stopped before using it for subsequent backend checks.

The findings below describe the **earlier audit state**, not this later release.

## Original follow-up

**Checked:** 8 September 2026, against the uncommitted changes following `docs/PRE_RELEASE_REVIEW.md`.
**Conclusion:** substantial remediation is present, but **“all 26 findings done” is not supported by the current code**. There are incomplete fixes and a new broken station-search action, not just outstanding deployment chores.

This was a focused verification, not a repeat of the full audit. No application files were changed, no timetable was rebuilt, and nothing was committed or deployed during this follow-up.

## Preview

The preview is running at **http://localhost:8765/** using:

```sh
.venv/bin/python scripts/dev_server.py --port 8765
```

It serves the local frontend and proxies API calls to the **deployed Render API**. Consequently, it previews the frontend changes, not a deployment of the modified backend. The root page and proxied station-list endpoint both returned HTTP 200; the latter returned 15 stations. The pre-existing API process on port 8011 was left alone.

## Validation performed once

- Python: **124 passed**, 479 warnings, approximately 1.77 seconds.
- Node frontend/Worker suites: **242 passed**, approximately 0.29 seconds.
- Current timetable semantic gate: **failed**; 1,360,684 stop-time rows, **zero** preserving times at or beyond 24:00.
- Small isolated probes reproduced the station-search error, two remaining departure omissions, Worker fail-open behaviour and insufficient database/readiness checks.
- The full browser suite was **not rerun**. Claude's reported 188 browser checks are not independently recertified here, and the visual adjustments below are described as code changes, not a new visual sign-off.

Local test versions were Python **3.14.6** and Node **24.15.0**. They differ from the newly configured CI versions; this matters below.

## Highest-priority outstanding problems

### 1. New station search calls a function that does not exist

`app.js:5227` calls `selectRailStation(...)`. No such function is defined; the actual board function is `openRailBoard` at `app.js:8937`.

Invoking the real registered search-result listener with a station result produced:

```text
ReferenceError: selectRailStation is not defined
```

The new tests check that stations appear in the result list, but never activate a station result. Fix the binding and test the actual action, not only the generated markup. **R11 remains open.**

### 2. The new CI command is incompatible with its pinned Node version

`.nvmrc:1` selects Node **20**. `.github/workflows/ci.yml:36` runs `node --test --test-isolation=none ...`. That flag is not available in Node 20: the [official CLI history](https://nodejs.org/api/cli.html#--test-isolationmode) records isolation support introduced after Node 20, initially under an experimental name.

The local green result on Node 24 does not validate this workflow. The timetable workflow depends on this CI job, so the mismatch blocks the proposed rebuild/publication path too. Align the runtime and command, then verify on that runtime. **R18/R25 are not complete.**

### 3. Timetable and delayed-departure corrections are only partial

- `scripts/build_timetable.py:504` now preserves service-day seconds, and `scripts/json_to_sqlite.py:186` keeps source ordering. These are real improvements, with passing round-trip tests.
- The existing database still fails `scripts/check_timetable.py`; it must be rebuilt from source, not merely passed through a new reader.
- `api/main.py:2453` checks service-day offsets `(-1, 0)` but not the next service day. At 23:50, a trip belonging to tomorrow and timed **00:05** is still omitted. A local fixture returned zero rows instead of one.
- `api/main.py:1698` still filters TransportAPI-only predictions by the aimed time before examining the expected time. At 12:05, a bus scheduled for 12:00 but expected at 12:10 still disappears in this path; the fixture returned zero rows instead of one.
- `app.js:2796` now ages bus countdowns locally, but does not fetch fresh board data. Station boards still lack the requested ongoing freshness/refresh treatment. Removing old bus rows also does not update the departure count or provide a replacement empty state.

**R03/R07 remain partial even after allowing for the pending database rebuild.**

### 4. Counter-write failures now permit uncounted submissions

`worker/src/index.js:527` uses `Promise.allSettled` for rate-limit writes, logs rejection and returns `ok` regardless. In an isolated probe where **every counter write failed**, the Worker returned **200 / ok: true** and proceeded to the mocked GitHub issue creation.

No real issue was filed. This confirms that the crash has been replaced with a fail-open accounting path. Eight randomly selected global shards also reduce collisions rather than guaranteeing collision-free writes. Agree an explicit safe storage-failure policy and test that no issue is filed when that policy requires rejection. The existing outage test merely checks for a structured response; it does not enforce safe behaviour. **R10 remains partial.**

### 5. Database replacement and readiness still accept unusable states

- `api/timetable_db.py:193` accepts a candidate containing four non-empty tables with the right names but entirely wrong columns and no calendars. A deliberately invalid-schema SQLite fixture returned `True` from `db_is_usable`.
- The downloaded candidate is not compared with its expected sidecar hash before replacement; the added check is not complete schema/integrity verification.
- `api/main.py:394` now reports `ready: false`, which is useful, but still returns **HTTP 200** without a timetable. Render's `healthCheckPath` remains `/` at `render.yaml:50`.
- `_clear_derived` resets the service-span cache, but not `_night_stops_cache` (`api/timetable_db.py:414`).

**R15 is improved, not closed.**

### 6. Interchange fares still do not validate the actual itinerary

The missing singles argument is repaired, and the cross-operator branch now considers Discovery. However, `journeyEndpointOperators` at `app.js:5445` still uses endpoint operator sets for interchange journeys instead of the actual legs. `unifiedTicketOption` at `app.js:5986` checks one service string; the interchange path supplies no direct-service identifier, so exclusions on an interchange leg are not evaluated there.

The two headline examples are improved, but the broader acceptance/route-validity part of **R02 remains open**.

### 7. Backend performance and publication changes are incomplete

- Several expensive operations now run off the event loop, but `get_journey` at `api/main.py:1454` still runs its synchronous database/path search inline after loading the timetable.
- The async single-flight helper is used for the vehicle endpoint, not the rail/NextBuses endpoints. Its presence alone does not coalesce those calls.
- The response cache now has a useful size bound. However, response-cache keys are not tied to a timetable generation, so a reload can coexist with stale cached answers.
- CI has Python and JavaScript jobs but no browser job. The timetable workflow still commits evidence before publishing its matching database, and has no explicit Pages deployment/build request after the bot push. The original [GitHub Pages token-trigger caveat](https://docs.github.com/en/pages/getting-started-with-github-pages/configuring-a-publishing-source-for-your-github-pages-site) still applies to the branch-based publishing mode documented in `docs/RUNBOOK.md`.
- Dated releases and workflow concurrency are worthwhile improvements. Uploading two rolling assets separately is not atomic publication of a matched database/hash pair.

**R13/R14/R18 remain partial.**

### 8. Price and evidence annotations do not cover every public claim

- Fare conflict metadata and a stale-price note renderer were added. But Gold's conflicting fare information is not reliably included when Gold sets the headline: the multi-zone branch passes the zonal tickets, rather than the selected network ticket, to `faresProvenanceHtml` (`app.js:5932`, `app.js:6260`). The network-only branch returns without that provenance footer.
- Four figures in the first objective now have `evidence_claims` and comparison tests. Other objective stop counts remain hardcoded without equivalent claim bindings. The letter renderer still pastes descriptions rather than attaching a complete dated evidence reference.
- The workflow tests run before regenerated evidence exists; there is no post-regeneration test checking whether its changed values still match the objective prose.

**R19/R20 are partly implemented, not comprehensively resolved.**

## Status across the original review

“Present” means the relevant implementation was found, not that production or every edge case is certified.

| Finding | Status in this checkout |
| --- | --- |
| R01 — singles and false savings | Primary correction present; singles passed to the comparison and reform wording distinguishes unlimited travel. |
| R02 — through-ticket validity | Partial: Discovery branch repaired; actual interchange-leg validation remains absent. |
| R03 — GTFS service days/order | Import/conversion corrected; current database unrepaired and next-day board gap remains. |
| R04 — exception-only calendars | Shared predicate implemented; calendar regressions pass locally. |
| R05 — operator identity | Route-level lookup implemented in direct and interchange paths; local route-identity tests pass. |
| R06 — stale async rendering | Shared board/view/journey guards present; councillor lookup still lacks the corresponding ownership guard. |
| R07 — board freshness/delays | Partial: countdown ageing and no-key schedule path added; prediction, next-day and refresh gaps remain. |
| R08 — attribute injection | Popup DOM listeners, attribute escaping and URL filtering present; escaping tests pass. |
| R09 — edited letters | Email action now reads current textarea; draft regressions pass. Real mail-client handoff not exercised. |
| R10 — Worker rate limiting | Partial: controlled errors/sharding added, but write failures permit uncounted submissions. |
| R11 — keyboard interactions | Search/buttons/link handling added; station result activation is broken. |
| R12 — narrow screens | Height caps and filter/chrome layout changes present; no independent visual rerun in this follow-up. |
| R13 — event-loop blocking | Partial offloading; journey computation still inline. |
| R14 — cache and quota behaviour | Bounded cache and vehicle coalescing present; other upstream paths and data-version invalidation remain. |
| R15 — downloads/readiness | Partial, with invalid-schema acceptance and HTTP-200-unready probes failing the original intent. |
| R16 — Worker input/configuration | Null/type/stream caps and missing-origin/KV rejection present; missing salt and Turnstile hostname/action validation remain. |
| R17 — submission handoff | In-flight guard, timeout, persistent proposal receipt and basic proposal validation added; scratch end-to-end sign-off not performed here. |
| R18 — release pipeline | CI/concurrency/dated artifacts added; Node mismatch, Pages trigger and coherent data publication remain. |
| R19 — fare sourcing | Conflict/Flexi/effective-date metadata added; selected-ticket disclosure and source resolution remain partial. |
| R20 — evidence provenance | Four first-objective bindings added; other quantitative prose and post-rebuild validation remain incomplete. |
| R21 — editorial claims | Substantial corrections/qualifications added; not a fresh independent fact-check of every claim. |
| R22 — public trust/privacy | About/privacy/corrections/accessibility page and footer disclosure added; operator/owner promises need owner sign-off. |
| R23 — publication operations | Coverage-loss guard, idea label cleanup and “approved for publication” wording implemented. |
| R24 — sharing metadata | Corrected title, canonical, social tags and preview image present and served locally. |
| R25 — reproducibility | Python/Node files and Pillow dependency added; CI runtime mismatch and dependency refresh/audit remain. |
| R26 — asset rights/licence | Register and third-party notices added; both photo permissions and project licence still unresolved. |

## Design follow-through

The mobile failure-label override and wider desktop reading columns are present in `style.css:2882` and the final reading-view media query. That confirms Claude made those changes; it does not replace visual review.

The new search styles use undefined `--surface`, `--border` and `--accent` variables (`style.css:5326`, `style.css:5365`), instead of the project's `--color-*` tokens. In particular they fall back to white surfaces even in dark mode. Correct these and include the new search states in the next focused visual check.

Stop clustering/culling already existed before this remediation; it is not evidence that vehicle-map visual density was newly addressed. Peak-map/dark-map visual judgement, physical-device checks and module extraction were not completed by the changes inspected here. Deferring the risky module extraction is reasonable; it should remain explicitly deferred.

## Suggested next batch

1. Repair station activation and the CI runtime mismatch first; both are small, concrete defects introduced in the remediation.
2. Complete departure-time, Worker failure-policy, candidate validation and actual-leg fare handling, using focused regressions for the missing paths.
3. Complete evidence/publication ordering, then run the approved timetable rebuild and verify its output before deployment.
4. Resolve photo rights/licensing with the owner, and perform one final browser/release sign-off on the changed UI and deployed stack.

Do not restart a 26-item implementation pass or repeat green suites after every edit. This list identifies the remaining acceptance gaps so the next work can stay targeted.

Temporary evidence: `/tmp/adur-followup-pytest.log`, `/tmp/adur-followup-node.log`, `/tmp/adur-followup-timetable.log`, `/tmp/adur-followup-probes.mjs`, `/tmp/adur-followup-probes.py`. The small probes use local fixtures and mocked upstreams; they never send public submissions.
