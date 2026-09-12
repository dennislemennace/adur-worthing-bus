# UI/UX and mobile performance review — 10 September 2026

## Verdict

**The visual identity is strong, but I would not sign off the mobile experience for a broad public launch yet.** The main problems are not a lack of styling: primary actions are initially hidden, selecting a stop can reveal no departures, station search throws an exception, and the route editor leaves almost no room to edit. These are fixable within the current architecture; a framework migration or wholesale redesign is not warranted.

This is a review, not an implementation or deployment. Findings below remain open unless explicitly described as existing strengths. Prior review completion claims and green checks are not treated as proof that these journeys work.

## Scope and method

- Reviewed repository `b9ad422` (`Give route 27 the Coaster livery`) and the public site at `https://worthingbrightonbus.co.uk/`. Public `app.js` matched the local file byte-for-byte during this audit: SHA-256 `4a3aae43a829adb425682bfee7ecd9818186ef9df52b66df65e62ba7f110abd1`.
- Started with `CODE_CONTEXT_SNAPSHOT.md`, project conventions, `CLAUDE.md`, `LIMITS.md` and existing review material; checked current implementation rather than assuming the snapshot was current.
- Inspected rendered pages, screenshots, browser exceptions, computed styles, layout rectangles, accessibility-tree names, keyboard interactions, request timings and relevant HTML/CSS/JavaScript.
- Ran the existing browser suite **once**, against the local preview: **196/196 checks passed**. Its five viewport sizes were 390×844, 320×568, 740×360, 768×1024 and 1440×900. Theme and dialog checks were included. Follow-up probes targeted uncovered behaviour rather than repeating that suite.
- Used Chrome 153 headless with touch/device emulation. Detailed public-site probes used 390×844 CSS pixels, DPR 2; desktop inspection used 1440×900. Browser cache was disabled.
- Took one unthrottled and one throttled public-site loading sample. Throttling: 4× CPU slowdown, 1.6 Mbps download, 750 kbps upload, 150 ms latency. These are diagnostic lab samples, not representative field percentiles or a Lighthouse score.
- Delayed/failed API responses were injected in the audit browser. Board ageing used a synthetic clock. No server failure or production data was created. No public forms were submitted, councillor emails sent, or production configuration changed.
- No Python/Node application suites were rerun: this review does not change application code.

### Coverage and limits

| Surface | Reviewed | Limit |
| --- | --- | --- |
| Live map and navigation | Initial view, filters, clustering, themes, layout, loading, keyboard section selection | Evening sample; not peak-service stress or a long battery/thermal soak |
| Stop/station search | Real query results, actual station-result activation, contrast and visibility | Not every stop, direction or station |
| Bus departures | Loaded board, collapsed/expanded layout, ageing, controlled API failure | Not independent validation of timetable/prediction accuracy |
| Rail | Broken search entry point; board loading/rendering/refresh implementation | Successful end-to-end rail tracking not re-certified |
| Routes and proposals | Service list, About/Proposals tabs, draft editor, mobile controls | No route submitted; moderation not exercised |
| Tickets/fares | Form placement, presets, input sizing, desktop/mobile layout, reduced-height viewport | Fare-result correctness and real mobile keyboards not re-certified |
| Network and Ideas | Reading layout, ideas form, stalled/failed deep link | No public idea or email sent |
| Updates/community | Reading layout, tabbed navigation, image presentation | No news submission |
| Shared accessibility | Existing dialog checks, focus/tab probes, populated dark search, map names, text-enlargement stress | Not a WCAG conformance audit or VoiceOver/TalkBack session |

Physical iPhone Safari and Android Chrome remain necessary for keyboard, browser-toolbar, safe-area, touch precision and assistive-technology validation. The reduced-height viewport and injected 200% root font size are stress tests, not substitutes for those devices. Local Turnstile error `110200` appeared in the browser-suite log; localhost domain restrictions are not evidence that production submissions fail.

## What already works well

- The coastal identity, warm light palette, navy dark palette, service branding and consistent typography give the site a recognisable character. Preserve this rather than adding decorative complexity.
- The recently improved mobile Routes layout exposes the service list on entry. The existing browser check confirms this at the tested sizes.
- Network and Updates use a full mobile sheet and a wider desktop reading column: 736 px at 1440 px, versus the normal 360 px panel.
- Stop clustering and viewport culling already exist. Stops have transparent **44×44 px** CSS hit areas around smaller visible dots; the dots should not be incorrectly reported as 12 px touch targets.
- Source links, dated evidence, scheduled/live notices, attribution and explicit public-submission warnings support trust. Keep their meaning while improving their placement.
- The existing suite checks useful basics: viewport overflow, common target sizes, selected text contrast, dialogs and visible mobile feed failure status. These are worth keeping.
- Warmed navigation remained responsive under CPU throttling. The measured heap/DOM sample alone gives no reason to claim a memory leak or mandate a rewrite.

## Prioritised findings

Priority means **P1: fix before promoting to the general public; P2: important usability/performance follow-up; P3: refinement**. Observed defects, source-derived risks and design judgements are distinguished below.

### U01 — P1: Initial mobile search is below the visible panel

**Observed:** On a fresh 390×844 Live view, the bottom sheet occupies y=532–780 and the footer occupies y=780–844. The search input starts at approximately **y=863**, outside the visible sheet. A large placeholder icon and instruction consume space before the first useful action. A visitor must discover sheet expansion or scrolling before discovering search.

**Source:** `app.js:2656` sets Live to the `peek` detent; `index.html:384` and `style.css:1879` place search after the large prompt.

**Fix direction:** Put a compact, labelled “Search stops and stations” field in the initial visible sheet. Make decorative guidance secondary. Do not automatically focus it and summon the keyboard on page load.

**Acceptance:** At 320 and 390 px widths, search is visible without scrolling or dragging; a result can be selected with touch or keyboard without first learning the panel mechanism.

### U02 — P1: Selecting a stop can show zero departure rows

**Observed:** Opening Town Hall (`149000007954`) from Live leaves the panel at `peek`. The first departure row begins at **y=780**, exactly where the panel ends and the fixed footer begins. Ten rows exist in the DOM, but none is visible. Expanding the sheet moves the first row to y=300.

**Source:** `app.js:1722` updates the panel and calls `scrollIntoView`, but does not expand the fixed sheet. The handle, tabs, stop heading and supporting information consume its initial height.

**Fix direction:** Selecting a stop should open enough panel space to reveal the next departures automatically. Put the next bus and destination before optional identifiers, span information and reporting controls. Apply the same principle to station selection.

**Acceptance:** An actual stop tap, not just a render-function test, reveals at least the next two departures at 390×844. Define and test an equally useful compact layout at 320×568 and in landscape. Empty/error states must occupy that same visible area.

### U03 — P1: Selecting a railway search result throws an exception

**Observed:** Searching “Southwick” and activating “Southwick station” on the public site throws `ReferenceError: selectRailStation is not defined`. No railway station is selected.

**Source:** `app.js:5276` calls `selectRailStation`; the implemented entry point is `openRailBoard` at `app.js:9210`.

**Fix direction:** Connect search to the existing rail-board entry point and ensure the panel opens visibly.

**Acceptance:** Activate a real station search-result button and assert selected station, visible board/loading state and absence of exceptions. Cover both a successful response and a controlled failure; do not stop at asserting that a result button exists.

### U04 — P1: The mobile proposal editor is largely consumed by its own controls

**Observed:** At 390×844, the editor has about 348 px below the Day/Night and About/Proposals controls. Its header takes 47 px and its action/verification/privacy area about **233 px**, leaving only **68 px of scrolling form space** for 544 px of content. Even the Name field is partly clipped. Exact dimensions can vary with verification-widget state, but the layout problem is visible without opening a keyboard.

**Source:** `app.js:6612`; `style.css:4438`, `style.css:4487`, `style.css:4707`.

**Fix direction:** Use a full-height editing mode or short “Route / Details / Review” sequence. Remove unrelated view controls while editing. Keep one compact primary action; make JSON copy/download secondary advanced options. Keep the public-submission warning clear before submission, but out of a permanently oversized action area. Offer a searchable/list-based stop selection path alongside map editing.

**Acceptance:** On a normal phone, multiple fields are visible together; the focused field and primary action remain reachable with a keyboard open. Draft persistence, back/cancel, validation and disclosure remain intact. Never submit a real proposal merely to test layout.

### U05 — P1: A slow stop feed delays unrelated deep-linked content

**Observed with controlled network delay:** Opening `#view=n` while holding `/api/stops` for 12 seconds leaves `state.viewMode` at Live and shows “Loading live bus data…”. Network content is not selected until the stop request resolves/fails. On a mocked 503, the toast says “Timetables are still available in Ticket view”, although Ticket view is a fare/zone interface and its stop picker has no stop data in this state.

**Source:** `app.js:455` awaits `loadStops()` before `applyUrlState()` and vehicle loading. `apiFetch` at `app.js:8565` has no application-level timeout. The stop failure path at `app.js:606` ends in a short toast, without a stop-loader retry control.

**Fix direction:** Apply the requested section immediately and initialise independent data separately. Static/campaign content must not depend on the live-stop service. Add bounded request deadlines, persistent useful failure states and scoped retries. Do not promise a fallback that depends on the failed data.

**Acceptance:** With `/api/stops` pending or failed, Network/Updates links still open their requested content, navigation works, unavailable search is explained, and a retry can restore stops without a reload.

### U06 — P2: Departure ageing leaves a misleading count and blank board

**Observed:** A board initially showed ten rows and “16 departures · as of 21:07”. Advancing the ageing function by three hours removed all ten rows but left the same count and an empty table body. The initial count also describes more departures than the ten displayed, without exposing the remainder.

**Source:** `app.js:1967` counts the filtered response but renders only `CONFIG.DEPARTURES_COUNT`. `app.js:2859` removes expired rows without updating the count or transitioning to an empty/stale state. The 30-second ticker recalculates labels, not predictions. Source inspection also found no refresh/as-of affordance in the rail-board renderer at `app.js:9254`.

**Fix direction:** Keep counts consistent with visible/available rows and show an explicit expired/no-upcoming state. Clearly distinguish ageing a cached prediction from fetching a fresh one. Retain manual refresh; consider bounded visible-board refresh only within `LIMITS.md` and backend cache/quota policies. Add freshness/retry controls to rail without aggressive polling.

**Acceptance:** A fixture clock can age every row out without leaving a positive count or unexplained blank table. A board with more results than its display limit labels that limit accurately. Refresh failure must not silently make old predictions look current.

### U07 — P2: Bus-first search ranking hides important railway results

**Observed:** “Worthing” and “Hove” each fill all 12 search slots with bus-stop matches, excluding their railway results. “Hove” also matches the district suffix “Brighton & Hove” on less relevant names. “Lancing Station” and “Lancing station” represent different modes but appear almost identical.

**Source:** `app.js:5194` returns as soon as the bus-only list reaches the limit, before considering railway stations. Results display names without a visible mode distinction.

**Fix direction:** Combine candidates before ranking. Prefer exact/prefix names over district substrings; distinguish Bus stop / Railway station and direction where available. Ensure a relevant railway result cannot be crowded out by weak bus matches.

**Acceptance:** Queries for Worthing, Hove, Lancing and Southwick produce understandable, relevant mixed-mode results. Test ordering and actual selection separately.

### U08 — P2: Populated search does not follow the dark theme and has insufficient text contrast

**Observed:** Expanded dark-mode search renders white inputs/result cards with `rgb(116,139,169)` text. Computed contrast is **3.495:1**, below 4.5:1 for this normal-sized text. This is not merely a preference for dark rather than white cards. The relevant criterion includes input/placeholder text. See [W3C contrast minimum](https://www.w3.org/WAI/WCAG22/Understanding/contrast-minimum.html).

**Source:** `style.css:5320` and `style.css:5358` use undefined `--surface`, `--border` and `--accent` names, falling back to unrelated hard-coded colours while inheriting the theme's muted text.

**Fix direction:** Use the existing `--color-*` surface, border, text and action tokens. Check populated, hovered and focused results, not only the empty panel.

**Acceptance:** Typed input and all result states meet the applicable contrast threshold in both themes and remain visually consistent with surrounding controls.

### U09 — P2: The fare-checking action is buried below introductory material

**Observed:** On entry at 390×844, the Tickets sheet ends at y=780. The origin field begins around **y=1066**, destination around y=1137, and “Check this journey” around y=1195. Introductory copy, warnings and six presets all come first. Input text is 14.4 px. A reduced-height viewport makes the map/form competition worse.

**Source:** Ticket form at `index.html:748`; panel defaults at `app.js:2656`.

**Fix direction:** Lead with origin and destination, followed by a concise scope/caveat statement. Make examples optional and place explanation after the useful action. Expand the sheet on input focus, using 16 px or larger mobile input text. Smaller text is a potential iOS focus-zoom issue, not an iOS failure demonstrated by this audit.

**Acceptance:** The journey fields are discoverable on entry; keyboard focus does not leave the user editing a tiny strip. Check results remain readable and clearly distinguish fares, route possibilities and timetable/live information.

### U10 — P2: API failure copy exposes implementation details

**Observed with a mocked 503:** The departure error displayed `API error 503: {"detail":"Service temporarily unavailable"}` directly to the passenger. The rail error renderer also displays the caught API message.

**Source:** `app.js:8565`, `app.js:1943`, `app.js:9254`.

**Fix direction:** Map failures to concise passenger-facing messages, preserve an obvious retry and keep diagnostics in logs or optional details. Do not replace the existing scheduled/live distinction with a generic error. A useful message identifies what failed and what still works.

**Acceptance:** Timeout, offline, 503, empty and quota-degraded responses produce distinct, understandable states without raw JSON or deployment instructions. Existing mobile feed-failure visibility should remain intact.

### U11 — P2: Initial loading does unnecessary work before Live is useful

**Observed:** The throttled run painted content at 1.06 seconds, dismissed the stop-loading overlay at 3.70 seconds, and first inserted bus-marker DOM at **10.09 seconds**. The vehicle request itself took 6.36 seconds, almost all before response delivery. This sample does not establish a backend cold start or identify a specific upstream bottleneck.

**Source-derived optimisation opportunities:** `app.js:455` starts vehicles only after stops are fetched/rendered. `app.js:513` eagerly warms Routes data during initial Live use. Route lines and proposals together transferred about **304 KiB**, approximately **35%** of the observed landing-page transfer, even without visiting Routes. `app.js:626` creates/adds all 1,520 stop markers before applying culling; `app.js:1509` adds the returned vehicle set without equivalent viewport culling.

**Fix direction:** Start independent live requests independently. Delay discretionary route prefetch until useful Live content is ready or the user signals intent; account for constrained connections. Create visible/clustered stop objects first instead of mounting the entire dataset and removing most of it. Consider vehicle culling when profiling peak load. Preserve cached route-switch speed and selected markers.

**Acceptance:** Record both paint metrics and time to useful stop/vehicle data under a repeatable mobile profile. The Live critical path should not wait for route/proposal work. Compare before/after requests, bytes and traces on the same fixture/profile before claiming improvement.

### U12 — P2: Throttled initial layout is not sufficiently stable

**Observed:** The largest layout-shift session was **0.151**, versus 0.007 in the unthrottled sample. This exceeds the “good” CLS boundary of 0.1 in the diagnostic run. Most shift accumulated at 1.67–2.01 seconds. The probe recorded values/times but not shifted source nodes, so it does not prove whether fonts, panel sizing, status changes or another element caused them.

**Fix direction:** Capture shift-source attribution in a targeted trace, then reserve the required initial space and avoid layout-affecting entrance transitions. Do not blindly remove fonts or animation on the basis of this metric alone.

**Acceptance:** Repeatable slow-profile first loads show no unexplained layout movement, with CLS at or below 0.1. Use real-user monitoring before claiming a field Core Web Vitals pass. The published good thresholds are LCP ≤2.5 seconds, INP ≤200 ms and CLS ≤0.1, assessed at the 75th percentile. See [Web Vitals](https://web.dev/articles/vitals).

### U13 — P2: Keyboard semantics and text enlargement need a focused accessibility pass

**Observed:** ArrowRight on the Routes About tab leaves focus there and does not select Proposals. The inspected elements with `role="tab"` all have `tabIndex=0`, rather than a single tab stop per tablist. The section chooser, by contrast, has working keyboard navigation. The editor colour input has no associated label (`labels.length === 0`). Accessibility-tree button names for many buses are only route numbers, without distinguishing direction/operator.

**Stress-test observation:** Setting the root font size to 200% at 390 px produces clipped/overlapping header branding and grows the fixed footer from about 64 to 153 px. There was no document-wide horizontal overflow, but that alone does not make the enlarged layout usable. This is a source/layout stress test, not certification of browser zoom behaviour.

**Source:** Tab markup/event handling in `index.html` and `app.js`; colour control at `app.js:6654`; fixed header/footer and mobile rules in `style.css`.

**Fix direction:** Implement roving tab focus and arrow-key behaviour appropriate to the declared tab pattern; label the colour field; give map controls useful distinguishing names where data permits. Keep a list/search route to map functionality. Adapt the header and footer to enlarged text without hiding required attribution. See the [W3C tabs pattern](https://www.w3.org/WAI/ARIA/apg/patterns/tabs/).

**Acceptance:** Keyboard-only navigation can select every tab, enter/exit dialogs and return focus coherently. VoiceOver/TalkBack can distinguish map actions. Real text enlargement and 320 CSS px reflow do not obscure actions or content. Maps have a reflow exception, not their surrounding forms/navigation: [W3C reflow](https://www.w3.org/WAI/WCAG22/Understanding/reflow.html).

**Target-size nuance:** Retain the existing 44 px stop hit areas (`style.css:2374`). Some editor controls are smaller than the project's preferred 44 px target; do not call every such case a WCAG failure. WCAG 2.2's minimum criterion uses 24 px with exceptions/spacing provisions: [W3C target size minimum](https://www.w3.org/WAI/WCAG22/Understanding/target-size-minimum.html).

### U14 — P3: Reduce competing map emphasis and make sections more task-oriented

**Design judgement:** Vehicle illustrations, count clusters, stop dots, boundaries, route lines, floating controls and a translucent sheet all compete in a small viewport. On the inspected dark map, street names are less prominent than overlays. Cluster counts are not self-explanatory to a first-time visitor asked to “click any bus stop”. Desktop Network/Updates retain a 704 px map alongside a 736 px article column, even while reading rather than navigating; some visible stops are inert outside Live.

**Fix direction:** Keep the distinctive vehicle art, but prioritise the selected route/stop and reduce emphasis of unrelated overlays. Explain cluster counts and preserve street readability. Consider article-first layouts for non-map sections, with a map only where it supports the story. Prefer public-facing task labels such as Routes and Fares over repeated “view” terminology. Make operational travel tools and advocacy/editorial content clearly related but not interchangeable.

**Acceptance:** A first-time visitor can identify how to find their next bus, understand a map cluster, distinguish live from scheduled information and recognise campaign content. Review peak-service screenshots and real phones in both themes before changing the whole map style. Optional location/favourites features are later enhancements, not prerequisites for fixing existing search.

## Performance measurements

Public-site samples used a cold browser cache but an already exercised backend; they were taken in the evening with 119 returned vehicles. They do not measure first-ever DNS/TLS setup, a guaranteed Render cold start, peak traffic or a typical user's handset.

| Measurement | Unthrottled mobile layout | Throttled mobile layout |
| --- | ---: | ---: |
| First contentful paint | 0.308 s | 1.064 s |
| Largest contentful paint | 0.428 s | 2.868 s |
| Stop-loading overlay dismissed | 0.743 s | 3.702 s |
| First bus-marker DOM inserted | 0.856 s | 10.092 s |
| Largest layout-shift session | 0.007 | 0.151 |
| Requests during observation | 39 | 39 |
| Encoded transfer, approximately | 863 KiB | 866 KiB |
| Observed long tasks | 0 | 6 |
| Longest observed task | None above 50 ms | 78 ms |

Observation windows were approximately 13.2 and 15.7 seconds, not identical-length long-running sessions. LCP was a map tile, not a usable departure answer. “First bus-marker DOM” is an application-readiness proxy, not proof that a particular passenger's bus was visible.

The six throttled long tasks lasted 51–78 ms, 345 ms in total; only 45 ms was above the 50 ms threshold. Do not misreport 345 ms as Total Blocking Time. A separate warmed-data navigation probe under 4× CPU slowdown took approximately 49–92 ms from section-menu interaction to two subsequent animation frames. Its largest recorded nonzero-interaction event duration was 80 ms. These are encouraging samples, **not a field INP measurement**.

The landing transfer included approximately 264 KiB of route lines, 40 KiB of proposals, 119 KiB of compressed application JavaScript, 40 KiB of compressed application CSS and 62 KiB of two downloaded font files. Route prefetch is a clearer immediate opportunity than speculative image replacement. The final main document had approximately 1,373 DOM elements; 1,520 retained stop-marker objects does not mean all 1,520 remained in the rendered DOM after culling.

## Recommended delivery order

1. **Restore useful mobile journeys:** U01–U03. Reveal search and departures, fix actual rail activation. Validate these together with user-level taps rather than only DOM presence.
2. **Make complex tasks and failure states usable:** U04–U06, U09–U10. Give editing/form entry space, decouple static content from live loading, correct stale boards and error copy.
3. **Close search/accessibility gaps:** U07–U08 and U13. Mixed-mode ranking, real contrast, tab semantics, labels and physical-phone checks.
4. **Optimise measured loading work:** U11–U12. Keep a reproducible baseline, fix request sequencing/prefetch, trace layout shifts, then compare. Do not begin by extracting all of `app.js` or introducing a build system.
5. **Polish with representative users:** U14. Check first-time comprehension, daylight readability and peak-service map density. Preserve the existing brand and source transparency.

## Efficient verification plan for the fixes

The green suite did not contradict these findings: it checked rendered row existence, not whether a row was visible above the footer; generic contrast did not include populated search; it did not activate a railway search result or measure editor field space. Add a few user-outcome checks rather than hundreds of broad assertions.

- For U01–U04, add focused browser checks for visible initial search, actual result/stop activation, first-row visibility and usable editor scroll area. Check 390 px and the smallest supported size.
- For U06, use a deterministic clock fixture for the count, expiry and empty-state transition. Keep this out of repeated live API testing.
- For U05/U10, mock delayed, failed and recovered requests. Do not wait for a real outage or repeatedly consume live quotas.
- For U07/U08/U13, cover mixed-mode search ordering, populated search contrast, real keyboard tab movement and label associations.
- Run the existing wider browser suite once per coherent UI change batch, not after every edit. Run only affected application tests during iteration; broader suites are justified when shared behaviour changes.
- Repeat the throttled loading profile after relevant performance changes. A small set of comparable samples is more useful than repeatedly running unrelated suites. Collect field data later if making public performance claims.
- Finish with a short physical iPhone/Android pass: initial search → next departure, station search, fare entry with keyboard, editor draft, failed/offline recovery and larger text. Successful public submissions need a separate owner-approved test or non-public fixture environment.

## Evidence and handoff

Application code, tests, data, deployment and existing review documents were not changed by this audit. This document is the review deliverable; the findings are not claimed fixed.

Local diagnostic artifacts are under `/tmp/adur-ui-review-20260910/` and are temporary rather than committed release assets:

- `tour.json`: initial/search/board/form geometry, real station exception and synthetic board-ageing result.
- `focused.json`: populated dark-search inspection, tab/AX probes, navigation timings and controlled network failures.
- `editor-targets.json`: correct editor-region measurements, colour-label association and 44 px stop pseudo-element dimensions.
- `mobile-unthrottled.json`, `mobile-slow4g-cpu4.json`: raw browser timing, request and layout samples.
- Key screenshots: `mobile-initial.png`, `mobile-departures-default.png`, `mobile-departures-full.png`, `search-dark-expanded.png`, `mobile-route-editor.png`, `mobile-tickets-fresh.png`, `mobile-text-size-200.png`, `network-deeplink-stalled-stops.png`, `mobile-departures-error.png`.
- Existing browser-suite log: `/tmp/adur-ui-review-20260910-browser.log`; its screenshot set: `/tmp/adur-ui-review-20260910-shots/`.

Reproduction helpers are `/tmp/adur-ui-review-cdp.mjs`, `/tmp/adur-ui-review-tour.mjs`, `/tmp/adur-ui-review-perf.mjs` and `/tmp/adur-ui-review-focused.mjs`. These are audit probes, not additions to the project's maintained test suite. Their raw output should be interpreted with this report: for example, the first focused editor probe used unmatched selectors; the final editor measurement is in `editor-targets.json`. Retain artifacts separately if needed beyond this machine's temporary-file lifetime.
