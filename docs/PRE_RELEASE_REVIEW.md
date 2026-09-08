# Pre-release review — Worthing Brighton Bus

**Reviewed:** 8 September 2026
**Code baseline:** `89c4497` — `Publish route proposals with a script instead of by hand`
**Starting point:** `CODE_CONTEXT_SNAPSHOT.md`, then source, data, tests, browser behaviour and limited read-only production checks.
**Disposition:** **Do not promote this version as a public-ready release yet.**

This is an audit, not an implementation patch. Application code, curated data, deployment configuration and existing tests are unchanged. The pre-existing, untracked context snapshot is preserved.

## Executive assessment

The project has a distinctive identity, a useful local purpose and substantially more engineering behind it than a typical campaign microsite. It does not need a framework rewrite or a new brand. It needs a correctness and reliability release before a polish release.

The largest problem is trust: the flagship ticket checker can overstate the cost of a journey and claim that an existing through-ticket cannot cover it. Timetable processing loses service-day information; operator lookup can assign a Brighton & Hove journey to Stagecoach; delayed responses can put one service's departures under another service's heading. The latter happened during the production browser check, not just in a synthetic test.

The ordinary desktop/mobile layouts are coherent, but smaller screens, keyboard-only map use, failure states and long-form campaign reading are not consistently professional. Passing the existing suite is not sufficient release evidence: the suite passes while these failures remain.

### Priorities

- **P1 — release blocker:** wrong travel/cost information, lost user edits, unsafe rendering, or a broken essential public interaction. Fix before broad promotion, or explicitly withdraw the affected feature.
- **P2 — pre-release hardening:** resilience, accessible layout, publication controls, provenance and public-facing polish. Complete for the intended high-end launch.
- **P3 — follow-up:** maintainability and refinements that should not delay the correctness work.

There is no demonstrated P0 compromise or evidence that production credentials have leaked. Security findings below distinguish a reproduced code weakness from an observed production exploit.

## Validation and coverage

| Check | Result | Qualification |
| --- | --- | --- |
| Python suite | **85 passed**, 473 warnings | Existing local virtual environment; Python 3.14, not the workflow's Python 3.11 |
| Frontend and Worker Node suites | **182 passed**, 0 failed | 153 frontend/test-loader entries plus 29 Worker tests; isolation disabled to expose individual counts |
| Existing browser harness | **126 checks passed**, **1 skipped** | Departure-board check skipped because the local API has no BODS key |
| Additional local browser probes | Defects reproduced | Fare presets, apostrophes, escaping, response races, mail drafts, keyboard activation and narrow/landscape layouts |
| Additional Python/Worker probes | Defects reproduced | Synthetic GTFS round-trip, calendars, span-cache reload, concurrent cold cache, Worker malformed input and KV failure |
| Public static site | HTTPS **200** | Read-only check; returned GitHub Pages HTML |
| Public API health | **200**, timetable loaded | Reported 5,378 timetable stops, 189 routes, 37,160 trips; not a readiness guarantee |
| Production browser | Stops, bus vehicles and rail data loaded | 1,520 displayed-area stops; subsequently 236 bus markers; one bus board and one rail board requested |

Commands used for the principal suites:

```sh
.venv/bin/python -m pytest -q
node --test --test-isolation=none tests/*.mjs worker/test/*.test.js
SITE_URL='http://127.0.0.1:8765/?api=http://localhost:8011' node scripts/browser_check.mjs --shots /tmp/adur-bus-audit-shots
```

The review covered the five frontend sections, shared navigation/theme/sheet/map behaviour, fare and journey logic, route proposals/editor, submissions and moderation, councillor lookup/drafts, bus/rail integration, both backend modules, GTFS/evidence/representative builders, the remaining authoring and image scripts, all curated dataset categories, tests, dependencies, deployment workflow, configuration, quotas, documentation, branding and attribution.

**Boundaries of assurance:** this is not an exhaustive independent verification of every bus stop, polygon vertex, published councillor address or operator fare. No real submissions, GitHub issues or councillor emails were sent. Local Turnstile produced a domain-validation error; the browser suite's JavaScript-exception check does not establish that submission verification works. No production stress test, fresh regional GTFS download, account-setting change, dependency installation, paid service or deployment was performed. Cloudflare/Render/GitHub account settings, secret scopes/expiry and asset ownership were not independently inspected. Browser coverage is Chromium emulation, not physical iOS/Safari, Firefox or a screen-reader certification.

## P1 findings — release blockers

### R01. The fare checker omits cheaper singles and invents a saving

**Locations:** `app.js:5450`, `app.js:5465`, `app.js:5568`, `app.js:5651`; `data/objectives.json:62`.

**Reproduced:** select **Worthing → Brighton**. The local timetable gives a direct 700, Marine Parade to Old Steine, 12:00–13:00. The page says the cheapest covering ticket is a Gold Dayrider at £8.50 and that merging the zones would save £12.50 per five-day week against a £6 ticket.

The code computes `singlesOption` but does not pass it to `cheapestRealOption` in either multi-zone branch. Under the site's own current capped-single model, one bus each way costs at most £6, so the claimed saving is not established for this journey. The objective's prose independently repeats the same false two-option framing and is copied into constituent letters.

**Required:** compare all eligible real options in one fare-decision path, including singles, network tickets and through-tickets; calculate reforms against the cheapest applicable baseline, not a selected expensive product. Distinguish a fare cap from an exact fare, and a commuter comparison from unlimited daily travel. A date-stamped worked example should disclose the assumed passenger type, number of trips, purchase channel and excluded products.

**Acceptance:** an end-to-end test of the rendered Worthing preset must reject the £12.50 saving under the current single-fare assumptions. Testing the helper with a fifth argument is not enough if production callers omit it.

### R02. Cross-operator journeys incorrectly rule out through-tickets

**Locations:** `app.js:5116`, `app.js:5420`, `app.js:5485`; `data/ticket_zones.json:4045`.

**Reproduced:** **Worthing → Hangleton** produces a 700 plus 5B itinerary, run by Stagecoach and Brighton & Hove. The result says no single ticket can cover it and quotes £12 return in singles. Yet the project explicitly configures and recommends the £10 South Downs Discovery ticket for daytime journeys. The published [Discovery ticket information](https://www.southdowns.gov.uk/travelling-around/south-downs-discovery-ticket/) also describes multi-operator validity and a £10 adult day ticket.

The branch equates “no single operator at both endpoints” with “no ticket accepted by both operators.” Those are different questions. It ignores the already-computed `unifiedOption`. More broadly, interchange fares are decided from endpoint operator sets rather than validating the actual legs and their intermediate coverage.

**Required:** evaluate ticket acceptance over each actual journey leg, including cross-acceptance agreements and route/time exclusions. Never make a universal “no ticket” claim from an empty operator intersection. If a route is not found or coverage is incomplete, return uncertainty, not a confident fare verdict.

**Acceptance:** the Hangleton preset considers Discovery; a mixed Brighton & Hove/Metrobus fixture respects their configured mutual acceptance; a route that leaves and re-enters an endpoint zone does not silently get an invalid ticket.

### R03. GTFS conversion destroys overnight times and authoritative stop order

**Locations:** `scripts/build_timetable.py:491`, `scripts/json_to_sqlite.py:178`, `api/main.py:2222`.

**Reproduced with a small GTFS archive:** a trip `A 23:55 → B 24:05 → C 24:10` is stored as `B 00:05 → C 00:10 → A 23:55`. `_hms_to_secs` applies `% 86400`; the SQLite converter then reconstructs sequence by sorting departure seconds. An A-to-C query loses this overnight trip. A board at B at 23:50 does not show the departure fifteen minutes later.

The importer also discards the original `stop_sequence`, so equal departure times cannot reliably recover the intended order. This is separate from midnight. [The GTFS specification](https://gtfs.org/documentation/schedule/reference/) preserves service-day times beyond 24:00 and defines `stop_sequence` as the ordering field.

**Required:** preserve service-day seconds, arrival/departure fields and source sequence throughout ingestion/storage. Build actual datetimes from the service date plus the offset. Departure windows must include relevant previous/current/next service days and their calendars. Fixing the modulo alone will not fix the board's current-day-only calculation.

**Acceptance:** GTFS-to-SQLite round-trip tests cover midnight in both directions, equal-time stops, unsorted source rows, weekend transitions and UK clock changes. Rebuild the timetable and derived evidence after the correction; otherwise old corrupted records survive a code fix.

### R04. Exception-only services are treated as running every day

**Locations:** `api/main.py:2285`; compare `api/timetable_db.py:718`.

**Reproduced:** a service with no base calendar and a single added date of 7 September is reported as running on 8 September by `_runs_today`. Missing base calendar returns `True`. There are **1,030 trips without a base-calendar row** in the inspected local database; this count identifies potentially affected records, not 1,030 confirmed erroneous departures today.

GTFS permits service definitions using `calendar_dates` without `calendar`. An unlisted date is not permission to run. The separate `Timetable.runs_on` path already takes a stricter approach, so API answers and evidence/service-span answers can disagree.

**Required:** share one service-day predicate; honour added/removed dates and base-calendar validity consistently. Missing or malformed calendar data must not invent service.

**Acceptance:** addition-only, removal, expired-calendar and no-calendar fixtures produce the same answer in journeys, departures and evidence calculations.

### R05. Route number is used as a unique operator identity

**Locations:** `api/main.py:1423`, `api/timetable_db.py:534`.

The direct-journey response calls `noc_for_short_name` rather than resolving the selected trip's route. The helper is last-row-wins across operators.

**Verified against the local database:** Brighton & Hove route `3829`, short name `1`, resolves to `SCSO`; Brighton & Hove `5` resolves to `METR`; Compass `47` resolves to `SCSO`. There are 23 short names shared by multiple NOCs. These errors feed ticket eligibility, not just badge colour. The interchange path already demonstrates a safer route-level lookup.

**Required:** retain route/operator identity end to end. Use the selected trip's route record, with explicitly scoped corrections where necessary. Audit route-number-only keys in colours, vehicle selection and route filters too; do not expand a night-service alias across operators.

**Acceptance:** API-level fixtures for two operators sharing the same number return their actual operator and cannot recommend the other operator's exclusive ticket.

### R06. Late responses overwrite the user's current view or selection

**Locations:** `app.js:1859`, `app.js:3330`, `app.js:3826`, `app.js:5072`, `app.js:8387`.

**Reproduced locally:** requests for stop A then B, resolved B then A, leave stop B selected with A's departures. Delaying the Route view load, switching to Network Objectives and then releasing the first load leaves **35 route layers visible in Network Objectives**.

**Reproduced in production:** open Marine Parade, then Worthing rail station while bus departures are loading. The delayed bus response replaces the rail board with bus departures beneath **“Worthing / CRS: WRH.”** At that point `selectedStop` is null and `selectedRailStation` is Worthing. Bus departures took about 19.2 seconds in this single observation, making the race quite plausible for real users.

**Required:** scope every async completion to a request generation, selected entity and owning view. Cancel superseded work where useful, but retain guards because cancellation alone is insufficient. Guard success, failure and `finally` rendering. Apply the pattern to journeys and councillor lookups as well as boards.

**Acceptance:** delayed-response tests cover stop A→B, bus→rail, rail→bus, journey A→B, leaving a section during its first load, and errors from an obsolete request.

### R07. Live boards can become stale and hide buses that are still due

**Locations:** `app.js:1219`, `app.js:1859`, `app.js:1901`, `app.js:8387`, `app.js:8590`; `api/main.py:1234`, `api/main.py:1566`, `api/main.py:2250`.

Vehicle polling updates map positions, not the open departure board. Bus due-time labels are rendered once: the probe retained “5 mins” after advancing the clock without a rerender. Rail boards also load on selection; the rail interval updates selected service details, not the station board. The normal bus refresh control requires a selected bus stop and does not provide a rail-board refresh route.

Separately, backend filtering discards trips whose **scheduled** departure is past before applying the later expected time. A delayed bus can disappear while the passenger should still be waiting. And `/api/departures` checks for a BODS key before it can serve the local timetable fallback: the local no-key server returned 503 despite having a usable database.

**Required:** retain timestamped board data and update countdowns locally; refresh only the visible selected board at a quota-safe cadence; show an explicit as-of/stale state. Apply predictions before filtering on effective departure time. Make schedule-only operation independent of the live-vehicle credential.

**Acceptance:** fake-clock tests age and remove rows correctly; a scheduled 12:00 / expected 12:10 bus remains visible at 12:05; no-key/quota/upstream cases still render an honestly labelled timetable. Budget any additional upstream polling under `LIMITS.md` before enabling it.

### R08. Attribute escaping breaks legitimate names and permits script injection

**Locations:** `app.js:1207`, `app.js:4912`, `app.js:8988`.

`escapeAttr` mixes JavaScript-string escaping with HTML-attribute escaping and leaves ampersands untouched.

**Reproduced with real stop data:** the datalist offers `Church Place St Mary\'s Hall` for `Church Place St Mary's Hall`; choosing that option does not resolve to an ATCO code.

**Reproduced with a harmless local malicious-data fixture:** the stop name `&#39;);window.__auditEscaping=true;//` becomes executable code inside the popup's inline `onclick`; clicking sets the test marker. HTML entity decoding happens before the handler is interpreted.

This demonstrates an injection sink reachable through manipulated upstream stop data. It does **not** demonstrate that an anonymous submission automatically reaches that sink or that production has been exploited: submissions have a manual publication boundary.

**Required:** remove interpolated inline handlers. Attach listeners to elements and pass values through properties/data, using text nodes for text. Where HTML serialization remains necessary, use context-specific escaping, including ampersands. Validate published link protocols separately; HTML escaping does not make `javascript:` safe.

**Acceptance:** test apostrophes, quotes, ampersands and entity payloads in stop pickers, popups and the proposal editor. Verify actual DOM behaviour, not just the returned string.

### R09. The councillor email action discards the user's edits

**Locations:** `app.js:7167`, `app.js:7341`.

**Reproduced:** edit the draft textarea to a personal letter, then inspect the email-app link. Its body still contains the original generated text. `councillorResultHtml` calculates the `mailto:` once; textarea changes never update it. Copy uses the edited text, so the two primary actions disagree.

This undermines the explicit instruction to personalise the letter and can send wording the resident deliberately removed.

**Required:** derive the email link from the current text at activation, and recalculate the long-mailto warning. Preserve the draft across non-destructive UI changes; prevent stale postcode lookups overwriting a newer choice. Keep sending under the user's explicit control.

**Acceptance:** edited text survives both Copy and email-app paths, including non-ASCII characters and long content. Use fixtures/test mail clients; do not send test messages to councillors.

### R10. The submission rate limiter can fail under an ordinary burst

**Locations:** `worker/src/index.js:126`, `worker/src/index.js:409`.

Every accepted submission writes the same global daily KV key. Cloudflare documents a [one-write-per-second limit for the same key](https://developers.cloudflare.com/kv/api/write-key-value-pairs/). Consequently, different genuine users submitting close together can hit a storage error, not merely the acknowledged read-modify-write undercount.

**Reproduced with mocked KV:** make `put` throw; the whole Worker `fetch` rejects because `checkRateLimit` is outside the error boundary. The user loses the structured retry response. No production burst was sent.

**Required:** design rate limiting for the documented storage semantics and free-tier budgets; handle binding/storage failure explicitly and fail safely. Use an appropriate supported limiter or serialized design only after checking availability and cost. Do not simply retry a hot global KV key or silently remove abuse protection.

**Acceptance:** HTTP-handler tests simulate simultaneous clients, storage rejection, missing bindings and exhausted limits; every path returns controlled JSON with the intended CORS/status behaviour.

## P2 findings — public-release hardening

### R11. Keyboard access does not cover the core map interactions

**Locations:** `app.js:1946`, `app.js:4536`, `app.js:8364`; `style.css:4951`.

Departure rows are clickable `<tr>` elements without a focusable action or keyboard activation. Stop clusters have no keyboard entry in the inspected DOM; rail markers explicitly use `keyboard: false`. There is no equivalent live-view stop/station search that removes this dependency on pointing at the map.

The ticket-zone card also intercepts Enter before checking whether the event came from its official-map link. A browser probe confirmed **Enter is prevented on “View official zone map.”** Map cluster targets measured 34 px and rail station targets are 22 px, outside the project's 44 px target convention. The browser harness excludes the map from its target scan; a passing scan is not whole-application accessibility assurance.

**Required:** provide a keyboard-operable stop/station list or search, real buttons for row actions, and correct nested-link handling. Preserve focus across list rerenders. Review map hit areas separately without creating thousands of sequential tab stops. Include keyboard-only and screen-reader task testing before sign-off.

### R12. Small-screen sheets exceed their available space

**Locations:** `style.css:161`, `style.css:691`, `style.css:2845`, `style.css:4904`.

**Reproduced at 320×568:** Route view's default sheet is about 273 px tall but has 402 px of content before meaningful scrolling can expose the list. Its tab strip begins at y≈581, below the viewport; the footer begins at y=497. In Network Objectives and Updates the full sheet begins above the viewport (y≈−3), with tabs partly behind the fixed header. The Network Objectives title also compresses the wordmark beyond its available width.

These are vertical clipping/occlusion failures, not horizontal overflow, so the existing overflow checks miss them. Short landscape viewports also require explicit coverage.

**Required:** cap sheet height to the available area after header, footer and safe-area insets. Ensure filters belong to an appropriate scroller or collapse by default when space is insufficient. Keep the current section and restore/close controls visible at every detent. Test 320 px width, short landscape and enlarged text, not just 390/768/1440 presets.

### R13. Async API routes perform synchronous heavy work on the event loop

**Locations:** `api/main.py:361`, `api/main.py:784`, `api/main.py:874`, `api/main.py:2210`; `api/timetable_db.py:170`.

The snapshot's shared-threadpool diagnosis is not the actual request model: these routes are `async def`. Their direct synchronous helpers, SQLite queries, preload work, hashing and urllib downloads run inline. `_fetch_db` uses `urlretrieve` without a download timeout; hourly refresh can therefore block unrelated requests.

In one warm-production observation, stops took ~0.28 s, vehicles ~13.0 s, route lines ~6.2 s, stop span ~6.2 s, bus departures ~19.2 s and rail departures ~7.4 s. These are individual end-to-end samples, not p95 figures or proof that every delay has the same cause. The blocking code path itself is directly visible. [FastAPI's concurrency documentation](https://fastapi.tiangolo.com/async/) distinguishes async endpoints from synchronous functions it offloads automatically.

**Required:** isolate blocking DB/download work appropriately, with an explicit connection/concurrency strategy; preload and refresh outside latency-sensitive requests; bound upstream waits. Do not merely switch everything to threads while retaining an unexamined shared SQLite lifecycle. Load-test locally with mocked upstreams and representative data.

### R14. Cache expiry is not memory eviction, and misses are not coalesced

**Locations:** `api/main.py:171`, `api/main.py:182`, `api/main.py:686`, `api/main.py:730`, `api/main.py:874`.

Expired entries are removed only when the same key is read again. One-off journey combinations and rail-service keys accumulate in `_cache`. There is no capacity bound. Concurrent misses also duplicate work: **12 simultaneous local vehicle requests produced 12 mocked upstream fetches**.

**Required:** bounded TTL/LRU storage, single-flight work per key, response reuse, and request limits for expensive/high-cardinality endpoints. Include rail budgets and concurrency in `LIMITS.md`; do not depend on browser polling discipline to protect a publicly callable API. Account for restarts and failed requests when claiming daily quota protection.

### R15. Download replacement, cache invalidation and readiness are incomplete

**Locations:** `api/timetable_db.py:170`, `api/timetable_db.py:250`, `api/timetable_db.py:974`; `api/main.py:283`; `render.yaml:46`.

- A downloaded database replaces the working file before its expected hash, integrity and required schema/data are verified. Sidecar comparison detects a possible change; it does not validate the replacement. Mutable release assets also permit a manifest/file race.
- `_span_cache` survives `reload()`. The local probe confirmed the same cache object remained, despite a newly opened database. Derived service-span answers can outlive timetable changes and their sampled week.
- `/` always returns HTTP 200 with `status: ok`, even if the timetable is absent. Render uses that endpoint as its health check. It exposes an internal path but no reliable data-version/age readiness condition.

**Required:** verify a candidate fully before an atomic last-known-good swap; reset all derived caches with the data version; publish build/service coverage age. Separate liveness from dependency readiness without making transient optional live-feed failures restart a healthy schedule service. Test corrupt download, missing sidecar, release rollover and refresh failure.

### R16. Worker validation and configuration do not fail safely everywhere

**Locations:** `worker/src/index.js:88`, `worker/src/index.js:102`, `worker/src/index.js:412`, `worker/src/index.js:536`, `worker/src/index.js:567`.

**Reproduced locally with all external calls mocked:** JSON `null` throws at `payload.botcheck`; a body with no declared length is fully buffered before the 8 KB check (a 64 KB fixture was consumed in full); missing origin configuration and KV allow a valid mocked submission from an arbitrary origin.

The committed Wrangler file does configure origins and KV, so the last result is a deployment-safety defect, not proof those protections are absent in production. Origin restrictions are useful browser controls, not authentication against clients that can supply headers. Turnstile verification should also enforce the intended hostname/action where applicable, not only success.

**Required:** validate a non-null plain-object body before property access, enforce the byte limit while streaming, catch request-read failures, and reject missing production security configuration. Keep permissive test behaviour behind an explicit development setting. Allowlist published URL schemes at moderation boundaries.

### R17. Successful submissions need a durable, comprehensible handoff

**Locations:** `app.js:6736`, `app.js:7594`; `index.html:876`; `worker/src/index.js:155`.

The proposal editor has no submission-in-flight lock, relies on a brief status/toast, and opens the resulting URL after awaiting the network, which browsers can block as a popup. Worker validation and publication validation do not enforce the same proposal contract: payloads can be accepted into the tracker but be unpublishable by the moderation script. Forms also lack a bounded client request timeout.

**Required:** one in-flight submission, persistent confirmation/reference link, explicit retry state and retained draft. Explain that submission means “received for review,” not “published.” Align the basic field/geometry contract while retaining the moderator's authority. Show the public-tracker/privacy notice at the editor's submit action, not only in help.

**Acceptance:** a scratch-repository/test-key flow covers all three kinds, duplicate clicks, slow/failed Worker calls, an expired challenge, popup blocking and moderation rejection. Do not use the public tracker for routine automated checks.

### R18. The release workflow does not gate quality or safely publish data

**Location:** `.github/workflows/update-timetable.yml:1`.

This is the only committed workflow. It compiles three scripts and checks that `stop_times` is non-empty, but runs none of the Python, Node, Worker or browser regression suites. Non-empty data can still have corrupt overnight ordering, invalid calendars or lost regional coverage.

It commits refreshed evidence using the checkout token, then deletes the rolling release before creating its replacement. A failure after deletion leaves no current asset; overlapping runs and rollback are not controlled. Evidence can land before its matching timetable release.

The comment that a bot commit refreshes the Pages data needs deployment verification: for branch-based Pages publishing, [GitHub documents that pushes using `GITHUB_TOKEN` do not trigger a Pages build](https://docs.github.com/en/pages/getting-started-with-github-pages/configuring-a-publishing-source-for-your-github-pages-site). Repository Pages settings were not inspected, and no explicit Pages deployment is present in this workflow.

**Required:** add PR/release regression gates and semantic GTFS/data checks. Publish versioned verified artifacts, retain rollback, serialize relevant runs, and deploy matching evidence/site data explicitly. Confirm the actual Pages publishing mode. Keep automation within the free-tier budgets; no bundler is required.

### R19. Fare sources conflict, and freshness labels do not resolve the conflict

**Locations:** `data/ticket_zones.json:3990`, `data/ticket_zones.json:4013`, `data/ticket_zones.json:4088`.

The Gold fare is £8.50, described as checked locally in the app on 1 September. Its link is an August 2023 coverage map, explicitly not price evidence. The official [Stagecoach South fare table effective 1 June 2026](https://tiscon-maps-stagecoachbus.s3.amazonaws.com/Miscellaneous/South/Stagecoach%20South%20Fares%20Table%20June%202026.pdf) shows **£9 Gold adult day**, **£36 Gold week/Flexi5**, and **£4 Nightrider after 19:00**, whereas the project restricts Nightrider to 19:30.

This is a **source conflict to reconcile**, not proof that the reported app purchase or a channel-specific promotion is impossible. It does mean the site cannot present one unqualified current price with an independently unverifiable check date. The published weekly/Flexi options also matter before framing five separately purchased day tickets as what a commuter must spend.

**Required:** retain inspectable fare evidence, effective dates, app/onboard distinctions and reviewer ownership. Reconcile the official table with any app-specific fare; expose the comparison's limits. Automatically warn or withhold absolute price claims after `review_by` expires.

The £2 cap announcement for January 2027 is **not** a false headline: current [Department for Transport guidance](https://www.gov.uk/guidance/3-national-bus-fare-cap) confirms it and says the £3 cap continues through the end of 2026. The data's note about funding to March 2027 should nevertheless be reconciled with that guidance; do not let a prose reminder stand in for effective-date handling.

### R20. Evidence has a strong generated path but an ungoverned prose path

**Locations:** `scripts/build_evidence.py:157`, `data/boundary_evidence.json:1`, `data/objectives.json:7`, `data/objectives.json:44`, `app.js:7167`.

The generated boundary evidence is a strength: it includes dates, a database hash, denominators, method and limitations. However, objectives separately hardcode 32.8/116.8, operator stop counts and fare savings. Weekly automation only rebuilds `boundary_evidence.json`; it does not update or invalidate those claims. Today the 32.8/116.8 numbers match the generated place comparison, but they are not guaranteed to remain so.

These descriptions are copied verbatim into resident letters without carrying the full provenance. Some figures lack an equivalent maintained query/version beside the claim. The comparison is scheduled service, not observed reliability; proximity to a council boundary is not by itself proof that the authority caused the gap.

**Required:** bind quantitative prose to named generated claims, with as-of date, unit/denominator, reproducible method and caveats as required by the project's evidence-provenance conventions. Preserve dated historical claims deliberately rather than silently leaving stale “current” prose. Recompute and editorially review affected evidence after R03/R04/R05.

### R21. Campaign copy needs an adversarial factual edit

**Locations:** `data/journey_presets.json:5`, `data/updates.json:16`, `data/updates.json:29`, `data/objectives.json:44`, `data/objectives.json:131`.

Concrete contradictions and overstatements:

- The Lancing preset says one ticket will not do it and suggests two operators; the actual example is Stagecoach 9 plus Stagecoach 700 and the result offers a network ticket.
- The fare-cap update says crossing the Worthing/Brighton boundary needs two buses. The site's own direct 700 example disproves this; a ticket-zone boundary is not a required interchange.
- The Ticketer article moves from a national rollout beginning in September to a claim that both local fleets will then have the same equipment and the technical problem is gone. A rollout start is not local completion or proof of interoperable products/settlement. Present the latter as a proposal/inference, not a verified implementation fact.
- The express-service objective asserts no extra drivers/vehicles are needed without a worked scheduling/cycle-time case. Frame that as something to evaluate, not an established zero-cost result.
- The accessible-information objective says the rest of the fleet, anything before September 2014, must comply. Qualify this against the actual scope and [published exemptions](https://www.gov.uk/government/publications/providing-accessible-information-onboard-local-bus-and-coach-services/current-exemptions-from-the-public-service-vehicles-accessible-information-regulations-2023), rather than a blanket assertion about every old vehicle.

**Required:** maintain a claim register with primary source, event/effective date and a clear separation between verified fact, inference and campaign ask. This is particularly important because the prose is published in the project's name and offered for emailing to elected representatives.

### R22. Public trust, privacy and correction routes are incomplete

**Locations:** `index.html:876`, `app.js:6736`, `worker/README.md:1`, `data/representatives.json:1`.

There are useful inline submission notices, but no complete public-facing explanation of who runs the project, how to contact them about an error, how to request removal of a public submission, or how the external services and local storage are used. A submission becomes a public GitHub issue **before** site moderation. “Moderated before appearing on this site” is not equivalent to “private until approved.”

**Required:** provide concise About, privacy/data-use, corrections/contact and accessibility information. Explain public tracker visibility, name/location disclosure, local drafts/preferences, postcode lookup, map/font/challenge providers and retention responsibilities. Identify the independent campaign status so operator/council branding is not mistaken for official endorsement. Review the final privacy wording against the actual deployment; this audit is not a legal compliance opinion.

### R23. Representative data and publication scripts need release-safe operations

**Locations:** `scripts/build_representatives.py:255`, `scripts/add_suggestion.py:148`, `scripts/add_proposal.py:159`, `scripts/add_proposal.py:321`.

The representative builder reads published official addresses rather than guessing them, and its handling of ambiguous names is appropriately cautious. However, it can overwrite the dataset after partial unmatched/missing results as long as the overall result is non-empty. There is no scheduled recheck or coverage-loss gate. The inspected file is dated 7 September and contains 239 published representatives; their individual current appointments were not independently recertified.

Moderation also has a status mismatch: `add_suggestion.py` leaves `unverified` on a published idea, unlike `add_proposal.py`. Both can close the issue with a “published” message after writing a local file but before commit/deployment, which can be misleading if release never happens.

**Required:** review area joins/coverage diffs before replacement, fail on unexpected regressions, and maintain a refresh owner. Make publication acknowledgement follow actual publication or clearly say “approved for publication.” Align label transitions and schema/URL validation across authoring scripts; retain the good category and geometry restrictions on proposals.

### R24. Discoverability and sharing do not match the current product

**Location:** `index.html:10`.

The title and description still describe an Adur & Worthing live tracker, while the visible brand is Worthing Brighton Bus and the site also presents tickets, route proposals, objectives and updates. There is no canonical link, Open Graph or social preview metadata. This is a material omission for a campaign intended to circulate in messages and social posts.

**Required:** align product naming and description, add canonical/social metadata and an appropriate preview image, and verify shared links restore a useful context. Review crawlable campaign content: critical advocacy currently depends on JavaScript. A small static explanatory page or progressive content can help without introducing a frontend framework.

### R25. Dependency and development builds are not fully reproducible

**Locations:** `requirements.txt:1`, `requirements-dev.txt:1`, `render.yaml:44`, `.github/workflows/update-timetable.yml:1`; image preparation scripts.

The inspected environment resolves FastAPI 0.111.0 and Starlette 0.37.2; the workflow installs unpinned `httpx`, and Render does not pin a Python version in the committed configuration. The audit environment and CI Python versions differ. Image-build scripts require Pillow, which is absent from the committed Python requirements.

**Required:** choose supported compatible runtime/dependency versions, record the complete reproducible development toolchain, and run an advisory/dependency audit on the resolved deployment set before upgrades. For example, Starlette has a published [multipart-processing advisory](https://github.com/encode/starlette/security/advisories/GHSA-2c2j-9gv5-cj73); the reviewed API exposes GET routes rather than a multipart upload surface, so that advisory is **not evidence of an exploitable upload endpoint here**. Assess actual reachability instead of treating version age as a vulnerability proof.

Keep dependency updates separate from behavioural fixes and rerun API/Worker/browser tests. Do not add a bundler merely to obtain a lockfile or formatter.

### R26. Asset rights and third-party delivery need explicit sign-off

**Locations:** `data/updates.json:10`, `data/updates.json:31`, `index.html:24`, `scripts/build_icons.py:1`.

The optimised served bus icons, separate masters and deterministic favicon/brand pipeline are good. No redesign is needed simply because the mark is raster. However, both update-image credits are blank, there is no asset-rights register, and no project licence file accompanies the open-source description. Inline Lucide attribution identifies MIT but should be supported by the appropriate licence notice.

**Required:** record ownership/permission for photos, illustrations, liveries and logo use; distinguish original/generated artwork from licensed third-party assets. Do not invent attribution. Provide the intended project licence and third-party notices after the owner confirms them. Review external script/style integrity and content-security policy; replacing inline handlers under R08 makes a useful CSP substantially easier. Keep existing map/data attribution visible.

## Design and product direction

### Preserve

- The coastal palette, typography hierarchy, recognisable mark and operator-specific vehicle art.
- The distinction between live operation, current routes/tickets and campaign proposals.
- Existing theme support, native dialogs, reduced-motion accommodations, sheet controls and source links.
- The worked journey examples and generated evidence: both are unusually effective ways to explain the campaign once their claims are correct.
- The simple static frontend, Python/SQLite backend and moderated Worker relay. Complexity is not the missing ingredient.

### Refine before presenting it as high-end

1. **Make the first useful action obvious.** Live view needs stop/station search and a clear explanation of scheduled versus live information. A moving map alone should not be the only route to an answer.
2. **Make failure as polished as success.** The live-update failure label is hidden below 880 px (`style.css:2881`); mobile can look merely empty while updates are failing. Show compact visible loading/stale/offline states with a meaningful next action. “Check API configuration” is not passenger-facing recovery copy.
3. **Use space according to the task.** At 1440 px, Network Objectives/Updates retain a large map alongside a roughly 360 px prose column, even when the map has no relevant content. Offer a comfortable reading layout rather than forcing articles and letters into a tracking sidebar. Preserve the map-first layout for routes and fares where it earns the space.
4. **Reduce competing visual emphasis.** Peak live-map screenshots contain dense vehicle art, stop clusters, route labels, boundaries and controls. Prefer viewport culling/selective detail and a clear selected-service focus over adding more decoration. Dark-map readability deserves manual review separately from text contrast checks.
5. **Keep controls reachable, not merely large.** The 44 px convention is worthwhile, but a large control beneath a fixed footer or inside a collapsed non-scrolling region is still unusable. Treat usable viewport height and keyboard focus as layout inputs.
6. **Give every action a dependable conclusion.** Persistent journey results, editable drafts that remain edited, submission receipts, explicit source dates and clear back/close behaviour create the premium feel more reliably than additional animation.

## P3 maintainability work

- `app.js` is about 8,990 lines; `style.css` about 5,280. This is not itself a launch bug, but the repeated late CSS overrides and cross-view state make accidental coupling expensive. After the release blockers, extract cohesive dependency-light modules and consolidate obsolete style rules without changing the no-bundler architecture by default.
- Formalise async ownership, data freshness and selected-entity state instead of adding separate ad hoc flags for each new pane.
- Several loaders swallow errors into empty arrays and retain the successful promise/state, preventing retry for that page session (`app.js:3889`, `app.js:6789`, `app.js:8342`). Distinguish unavailable, loading, empty and loaded data consistently.
- Keep `docs/ROADMAP.md` as historical planning but add a short authoritative current runbook. The snapshot is valuable context, not ground truth: stop clustering, optimised icons, hidden-tab polling and frontend vehicle in-flight guarding already exist; the API-threadpool claim needed correction.
- Document the one-time migration/image tools' prerequisites and destructive outputs. Do not run route snapping, source-image transforms or full data rebuilds merely as a routine test; these alter release assets and need reviewed diffs.

## Release sequence and exit criteria

### 1. Restore trustworthy answers

Fix R01–R05 and R07's time/filtering semantics. Add end-to-end fare verdict tests and synthetic GTFS round-trip tests first; prove each regression test fails on the current implementation. Rebuild timetable/evidence and reconcile public fare sources and prose. Do not try to repair fare logic by editing the examples to avoid problematic journeys.

### 2. Make interaction and input handling safe

Fix R06, R08–R10, Worker validation, and the draft/submission handoff. Exercise races and failures with deterministic fixtures. Provide a keyboard path to core live tasks and fix narrow-sheet clipping. Use a scratch submission environment only.

### 3. Make delivery recoverable

Add quality gates, bounded/coalesced caching, non-blocking refresh, verified last-known-good database replacement and versioned publication. Verify Pages refresh behaviour, production debug gating/CORS, secret scope/expiry, quotas, and rollback with the actual account configuration. Keep the agreed free-tier posture.

### 4. Finish the public presentation

Complete editorial/source review, About/privacy/corrections/accessibility information, asset-rights sign-off, metadata and reading layouts. Test physical mobile Safari and Android Chrome, keyboard-only operation, a screen reader, enlarged text, slow network, offline return and returning from a background tab.

### Sign-off checklist

- [ ] Every P1 finding is fixed and has a regression test, or the affected feature is intentionally withheld.
- [ ] All five journey presets have a reviewed itinerary, defensible fare baseline and non-contradictory campaign copy.
- [ ] Overnight, exception-only, delayed and schedule-only services behave correctly.
- [ ] Switching selection/view cannot display obsolete results; countdowns and freshness states remain honest.
- [ ] Keyboard users can select stops/stations and activate board/ticket actions; small/short screens expose all controls.
- [ ] Edited councillor text survives both handoff paths; representative coverage and sources are checked.
- [ ] All three submission types succeed in a test environment and fail safely under invalid input, storage outage, quota and duplicate-click scenarios.
- [ ] A clean supported environment runs the complete suites; the browser departure test is exercised rather than silently skipped for release.
- [ ] Timetable, evidence and deployed frontend have matching version provenance, quality gates and a tested rollback path.
- [ ] Current price conflicts, article assertions, privacy/contact information, sharing metadata and asset permissions are resolved.

## Session evidence

These temporary files are available in the audit environment; they are not shipped site assets or a portable test suite:

- `/tmp/adur-bus-audit-shots/` — existing harness screenshots plus narrow, landscape, desktop and production captures.
- `/tmp/adur-bus-audit-shots/production-rail-overwritten-by-bus.png` — production rail heading with the late bus-board replacement.
- `/tmp/adur-bus-ui-results.json` — measured DOM bounds, apostrophe failure, harmless injection fixture, mailto edit loss and local departure race.
- `/tmp/adur-bus-python-results.txt` — GTFS round-trip, exception calendar, cache lifecycle, local database inventory and 12-client cache-miss results.
- `/tmp/adur-bus-ui-probes.js`, `/tmp/adur-bus-fare-probes.js`, `/tmp/adur-bus-final-ui-probes.js` — local browser reproduction scenarios.
- `/tmp/adur-bus-worker-probes.mjs`, `/tmp/adur-bus-python-probes.py` — isolated backend/Worker reproduction scenarios.

**Bottom line:** preserve the identity and architecture. Correct the travel advice, async ownership and publication safeguards first. That is the shortest defensible route from an impressive prototype to a professional public service and campaign site.
