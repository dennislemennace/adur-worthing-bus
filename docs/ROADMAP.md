# Roadmap — endpoints, submissions, restyle

**Written:** 2026-08-29 · **Against:** `main` @ `f498865`

**Status:** Phases 1, 1b and 2 are **done, merged and live**. Phase 3 packages
P3.0–P3.5 are **built and committed, not yet pushed or deployed**.
Test count 121 → 150, plus 50 browser checks (was 12).

Submissions went live 2026-08-30 and are proven end to end: issue #1 arrived
through the Worker with the right labels and was published to the site with
`scripts/add_suggestion.py`.

**Two things bit during Phase 2 that the plan had not anticipated** — both now
fixed in `worker/README.md` and in code:

1. `wrangler secret put` fails until the Worker exists, so the first deploy has
   to come *before* the secrets, not after.
2. The first deploy prints a `workers.dev` URL that does not work until a
   subdomain is registered on the account. The symptom is
   `curl: (35) TLS connect error`, which suggests nothing about the cause.

**And one self-inflicted outage worth remembering.** Phase 1 made the API
honour `ALLOWED_ORIGIN`, which `render.yaml` had documented from the beginning
while nothing read it. The variable was already set in Render to a stale value,
so the first deploy that respected it dropped the real Pages origin and the
live site lost access to its own API. `curl` kept returning 200 throughout,
because curl does not enforce CORS — the same shape of trap as the CARTO
basemap returning 200 while serving "API KEY REQUIRED" tiles. `ALLOWED_ORIGIN`
now adds to the canonical origin instead of replacing it.

The final failure was a `403 Resource not accessible by personal access token`:
GitHub's current fine-grained-PAT UI starts with **zero** repository
permissions and requires an explicit "Add permissions" step, which is easy to
miss because there is no list of permissions to scroll through.

Three phases, in this order: **secure the endpoints → switch submissions on →
restyle for mobile**. Each phase is independently shippable; nothing here needs
a flag or a migration.

Every claim below was checked against the code or the live deployment. File and
line references are to `f498865` — re-verify if the tree has moved.

---

## Phase 0 — where things stand

Working, live at <https://dennislemennace.github.io/adur-worthing-bus/>:

- **Live Bus Tracking** — BODS SIRI-VM vehicles (20 s poll), TransportAPI
  predictions over the GTFS timetable with a timetable-only fallback when the
  daily quota is spent, Realtime Trains rail boards proxied server-side.
- **Route view** — 4 curated proposals, map overlay, `localStorage` draft editor.
- **Ticket view** — indicative zone outlines plus the A-to-B boundary penalty
  calculator (`GET /api/journey`), verified end-to-end against production.
- **Network Objectives** — 8 objectives grouped by responsible body, 4 campaign
  asks pinned open, 2 published community ideas.

Built but dormant: the whole community-submission path (Worker + three forms).
See Phase 2.

Assessed and dropped: reliability heatmaps — BODS publishes no punctuality data
for this area, so there is nothing to render.

---

## Phase 1 — endpoints  ✅ DONE

### Findings

Probed against production, worst first.

**1. `/api/debug/live-raw` is an unauthenticated quota drain.**
`api/main.py:925` calls TransportAPI directly with **no cache and no quota
accounting** — `_check_api_key()` only validates the BODS key, and the path
never calls `_nb_quota_bump()`. It therefore bypasses the local
`NEXTBUSES_DAILY_LIMIT=300` cap *and* spends the real upstream allowance. A
trivial loop takes real-time departures down site-wide until midnight rolls the
counter. This is the only finding with a direct operational cost.

**2. `/docs` and `/openapi.json` return 200 in production.**
Confirmed by request. Every debug route, with parameter names and types, is
publicly browsable — discovery for (1) is free.

**3. Raw feed republication.** `/api/debug/siri-sample` and
`/api/debug/vehicles-raw` re-expose raw BODS SIRI-VM. Beyond the leak,
redistributing the raw feed sits badly with BODS fair-use.

**4. `/api/debug/nb-quota`** reports exactly how much budget remains —
reconnaissance for (1).

**5. CORS is `allow_origins=["*"]`** (`api/main.py:190`) while `render.yaml`
documents an `ALLOWED_ORIGIN` env var that nothing reads. The dead-but-documented
variable is worse than either choice on its own, because the setup instructions
actively mislead.

Scope: 7 of 10 `/api/debug/*` endpoints are ungated. Only the two rail-raw routes
check `RTT_DEBUG_ENABLED`. `/api/debug/rail-token` does **not** leak the token
itself — verified, it returns only the validity window.

### Design

**Gate structurally, not per-endpoint.** Move every debug route onto an
`APIRouter(prefix="/api/debug", dependencies=[Depends(require_debug)])`. One
dependency at router level means a *future* debug endpoint is gated by
construction. That is the difference between fixing seven endpoints and fixing
the class.

- `require_debug` reads `DEBUG_ENABLED` (default off) and raises **404, not 403**
  — do not confirm the endpoint exists.
- Keep `RTT_DEBUG_ENABLED` as a second, narrower gate on the rail-raw routes, so
  enabling general debug does not also re-expose the RTT feed.
- `FastAPI(docs_url=None, redoc_url=None, openapi_url=None)` in production,
  re-enabled by the same flag — they are genuinely useful locally.
- CORS reads `ALLOWED_ORIGIN` (comma-split), falling back to `*` only when unset
  **and** debug is on. Then fix `render.yaml` so it stops promising something
  that never happens.
- Independently of gating: route `live-raw` through the same cache and
  `_nb_quota_bump()` path as `/api/departures`, so it cannot drain the quota even
  when debug is deliberately on.

### Tests

- Each debug route 404s with `DEBUG_ENABLED` unset, 200 with it set.
- `/openapi.json` 404s under the production config.
- CORS header reflects the allowlist, not `*`.
- A route-walker over `app.routes` asserting every `/api/debug` path carries the
  dependency. **This is the test that stops the class coming back** — without it,
  the next debug endpoint is ungated again.

### Effort and risk

Half a day: roughly 60 lines in `api/main.py`, 40 lines of tests, one
`render.yaml` edit. **No frontend change** — confirmed that `app.js` and
`index.html` never reference `/api/debug`.

**Built 2026-08-29.** All nine debug routes moved onto `debug_router`;
`require_debug` gates them at router level and returns 404. `/docs`, `/redoc`
and `/openapi.json` gated by the same flag. CORS now reads `ALLOWED_ORIGIN`,
defaults to the Pages origin, and allows any localhost port so the preview flow
keeps working. `live-raw` routed through the cache and `_nb_quota_bump()`.
13 new tests in `tests/test_debug_gating.py`, including the route-walker.
Verified on a real uvicorn server in both flag states.

Risk is low; the only downside is losing your own diagnostics, mitigated by the
env flag.

> **Deploy note.** Render redeploys from `main`, so this goes live on push. Set
> `DEBUG_ENABLED` in the Render dashboard *before* merging if you want to keep
> the endpoints reachable for a while.

---

## Phase 1b — the panel-collapse bug  ✅ DONE

Recommended to ship alongside Phase 1. It is 1–2 hours and it currently makes the
Ticket view a dead end.

### Diagnosis

`togglePanelCollapsed()` (`app.js:1611`) sets a class on **`<body>`** — global —
but the control lives inside each panel mode, and `applyViewMode`
(`app.js:2079`) never clears it. The rule at `style.css:620` matches every view:

```css
body.panel-collapsed .panel-mode > .panel-tab-content { display: none !important; }
```

**The trap: Ticket view has no `.panel-tabs-bar`.** The collapse chevron exists in
live (`index.html:257`), improvements (`466`) and network (`635`) — but not
tickets. So:

> Collapse the panel in Route view, switch to Ticket view, and the entire view is
> `display: none !important` **with no on-screen control to restore it.**

The boundary calculator — the headline advocacy feature — becomes unreachable.
Only a page reload recovers, because the state is not persisted to
`localStorage`. That is the one mercy.

**Second trap:** the chevron is `display: none` above 700px (`style.css:611`).
Collapse on a phone, rotate to landscape past the breakpoint, and the content
stays hidden with the affordance gone.

### Fix

Three options considered:

- **A. Clear on view change.** One line in `applyViewMode`. Loses user intent —
  someone who collapsed to see the map gets the panel back on every switch.
- **B. Scope collapse per view.** `state.collapsed = {live: false, …}`. Preserves
  intent, more moving parts.
- **C. (recommended) A, plus make the dead end unrepresentable:** clear on view
  change, give Ticket view a tabs bar so it has the same affordance as every other
  view, and clear the class when crossing the 700px breakpoint.

C is the right size given Phase 3 rebuilds the mobile panel anyway — cheap now,
and the bottom sheet inherits a sane model.

**Built 2026-08-29 (option C).** `setPanelCollapsed()` extracted;
`applyViewMode` resets it as its first statement; `syncPanelCollapsedToWidth()`
clears it on a resize above 700px; Ticket view gained a single-view strip so it
has the same collapse affordance as every other view. 6 tests in
`tests/test_panel_collapse.mjs` — the DOM stub uses a **real** classList,
because a stubbed `contains: () => false` would let every assertion pass while
the bug survived. Confirmed the view-switch test fails without the fix.
Also verified in a real browser: see below.

Add a regression test covering: collapse in one view, switch, assert content
visible.

> Worth running the `click-path-audit` skill across all view and panel
> transitions once. Its description matches this bug class exactly, and there is
> no reason to think this is the only instance.

---

## Phase 2 — switching submissions on  ✅ DONE, LIVE 2026-08-30

Mostly account work that cannot be done on your behalf. The Worker itself is
built, tested (26 tests) and deployed-ready.

### Gap in the existing runbook

`worker/README.md` **never tells you to create the GitHub labels**, and the
GitHub API returns **422** for an unknown label. The first real submission would
fail as a 502 and look like a broken Worker. Fix the README as part of this
phase.

The exact 11 labels, from `worker/src/index.js:210,247,294` and
`STOP_ISSUE_CATEGORIES` at `:51`:

```
community-submission
idea
proposal
stop-issue
unverified
stop-issue:shelter
stop-issue:timetable-case
stop-issue:rtpi-display
stop-issue:accessibility
stop-issue:lighting
stop-issue:other
```

### Steps (~45 minutes)

| # | Step | Who |
|---|------|-----|
| 0 | ~~Create the 11 labels above~~ **DONE 2026-08-30** — all 11 exist on the live repo | Claude |
| 1 | Cloudflare account — free, no card required | **you** |
| 2 | `cd worker && npx wrangler kv namespace create RATE_LIMIT`, paste the id over `REPLACE_WITH_KV_NAMESPACE_ID` at `wrangler.toml:28` | Claude |
| 3 | Turnstile → Add site → **Managed** widget. Domains: `dennislemennace.github.io`, plus `127.0.0.1` only while testing | **you** |
| 4 | Fine-grained PAT: this repo only, **Issues: Read and write**, nothing else. Set an expiry you will notice; diary the rotation | **you** |
| 5 | `npx wrangler secret put GITHUB_TOKEN / TURNSTILE_SECRET / IP_SALT` (`openssl rand -hex 32` for the salt) | Claude |
| 6 | `npx wrangler deploy`, note the URL | Claude |
| 7 | Point `GITHUB_REPO` at a **scratch repo**, submit one of each kind, verify labels / dedupe / sanitiser, revert | Claude |
| 8 | Set `CONFIG.SUBMIT_ENDPOINT` (replacing the `YOUR-WORKER` sentinel) and `CONFIG.TURNSTILE_SITE_KEY` in `app.js` | Claude |
| 9 | Remove `127.0.0.1` from `ALLOWED_ORIGINS`, redeploy | Claude |
| 10 | Live smoke test; approve the test issue with `scripts/add_suggestion.py` | Claude |

**Step 2 note:** `kv namespace` (with a space) requires Wrangler ≥ 3.60.0; below
that the form is `kv:namespace`. The README already uses the modern form — add
the version note.

**Do not skip step 7.** The Worker writes to a public tracker; test somewhere
disposable first.

### Worth adding while in there — both DONE 2026-08-30

- ~~README labels step~~ **DONE.** `worker/README.md` now opens with the label
  step, the eleven `gh label create` commands, and a note that the set derives
  from `buildIssue` + `STOP_ISSUE_CATEGORIES`. Steps renumbered 1–6, and the
  Wrangler 3.60.0 `kv namespace` vs `kv:namespace` caveat added.
- ~~422 retry-without-labels fallback~~ **DONE.** `fileIssue` retries once
  without labels on a 422, so a renamed label costs the labelling rather than
  the submission. `ghFetch` attaches the HTTP status so the retry is scoped to
  422 only — a 401 is a wrong token and retrying would double the damage.
  3 new tests (worker suite 26 → 29).

**What is left is only what needs your accounts:** the Cloudflare account,
Turnstile keys, and the fine-grained PAT (steps 1, 3, 4). Everything after
those — KV namespace, secrets, deploy, scratch-repo test, wiring
`CONFIG.SUBMIT_ENDPOINT` and `CONFIG.TURNSTILE_SITE_KEY`, tightening
`ALLOWED_ORIGINS` — can be driven once the credentials exist.

Turnstile's always-passing local test keys (`1x00000000000000000000AA` /
`1x0000000000000000000000000000000AA`) are already documented in the README for
local end-to-end runs.

---

## Phase 3 — restyle and mobile

> ### ✅ Built 2026-08-30 — and decision (a) went the other way
>
> The recommendation below was the civic-transit re-theme. **That was made
> without ever looking at the site.** Driving it at 390 / 768 / 1440 in both
> themes, with a departure board actually loaded, changed the answer: the
> identity is already distinctive and already half-civic — mono numerals on a
> live board *is* departure-board language. What failed was structural.
>
> **Decision taken: refine in place.** Fraunces, Outfit, JetBrains Mono, navy
> and amber, sand and ocean, and the wave all stay. No hue moved except where
> contrast required it.
>
> Measured before, at 390px:
>
> | Defect | Evidence |
> |---|---|
> | Status chip clipped on **every** departure row | `right: 396px` vs a 390px viewport |
> | Header title rendering 38% of itself | `clientWidth 110` / `scrollWidth 292` |
> | 1,520 stop markers, 1,604 marker elements | route lines in Route/Network view invisible beneath them |
> | Panel locked at 45vh — 5 of 10 departures, no way to see more | `max-height: 379.8px` of 844 |
> | Zero `svh`, zero safe-area insets, no `viewport-fit=cover` | 0 occurrences |
> | Targets 24–42px | status-pill toggles were 24×24, not the 38 assumed |
> | 768px tablet on the desktop split | squeezed map beside a mostly-empty panel |
> | Every `:hover` unguarded | 0 `any-hover` queries — hover stuck after every tap |
>
> After: **50/50 browser checks**, four views × two themes × three viewports.
> The board fits, the title is 137/137, the default view carries 124 marker
> elements instead of 1,604, and the sheet's full detent shows all ten
> departures at once — which the old slab could never do.
>
> Two bugs found that predate this work:
>
> - **`el.hidden` did nothing.** The UA stylesheet's `[hidden]` is
>   author-level-zero, so `.btn-icon { display: flex }` beat it. The bus and
>   rail toggles had been sitting in the header in all four views, including
>   the three where they do nothing.
> - **`pickTextOn` was not measuring contrast.** It weighed backgrounds with
>   the YIQ brightness formula against a 0.62 threshold, and `getLineColour`
>   handed it `hsl()` strings that its six-digit-hex check silently rejected,
>   returning "light" as a shrug. Unknown routes got white on mid-tone fills
>   at 3.0:1 for that reason alone.
>
> **Still open in Phase 3:** the systematic state-coverage pass in P3.3
> (disabled, loading and error states were not audited — focus and hover were),
> and the large "Click any bus stop" empty state itself. Everything under
> "Not in scope" below is still deferred.
>
> **A note for whoever runs this next:** on 2026-08-30 a commit left seven
> zero-length files in `.git/objects` and pointed `refs/heads/main` at one of
> them, which broke `HEAD`. Nothing was lost — the last good commit was intact
> and the working tree untouched — but seven empty objects appearing at once
> is a filesystem or power event, not a git bug. Worth watching for.

### The good news, measured

`style.css` is **~95% tokenised**: 809 `var(--…)` usages against 43 hardcoded hex
literals outside the token blocks, on top of a real semantic layer
(`--color-*`, `--space-*`, `--radius-*`, `--shadow-*`, plus type and spacing
scales at `style.css:10-68`).

**A re-theme is therefore mostly a token swap, not a rewrite.** This materially
lowers the cost of the phase. Hunt the 43 strays first.

### Mobile audit

| Finding | Evidence | Impact |
|---|---|---|
| Panel is a fixed `max-height: 45vh` slab below the map | `style.css` | `vh` is the classic mobile lie — mis-sizes under the address bar |
| Zero `dvh` / `svh` usage | 0 occurrences | as above |
| Zero safe-area insets | 0 occurrences | content under the home indicator and notch |
| No PWA manifest, no `theme-color` | 0 in `index.html` | no add-to-home-screen, no branded status bar |
| `apple-touch-icon` points at `favicon.ico` | `index.html:10` | iOS wants PNG; home-screen icon renders wrong |
| Touch targets 42px | `style.css:575` | passes WCAG AA (24×24) but under Apple 44 / Material 48 |
| 13 media queries across 3,688 lines | breakpoints 1024/880/700/480 | mobile is a patch layer, not a design |
| 3 font families, 9 weights, render-blocking third-party | `index.html:18` | LCP cost plus GDPR exposure |
| Leaflet CSS from unpkg | `index.html:13` | render-blocking third-party |
| No drag gesture on the panel | — | not the pattern users expect over a map |

Asset weights: `app.js` 74 KB gzip, `style.css` 21 KB, `index.html` 10 KB.

### Decision (a) — aesthetic direction

What exists — warm paper, navy, amber, Fraunces/Outfit, the wave motif — is
genuinely distinctive and not generic. Three options:

1. **Refine in place.** Keep the palette identity, fix rhythm, hierarchy and
   density. Lowest risk.
2. **Civic-transit re-theme (recommended).** Lean into departure-board language:
   high contrast, mono numerals, strong per-operator colour coding.
3. **Fresh direction** chosen via the `frontend-design` / `ui-ux-pro-max` skills.

Recommendation is 2. The site exists to make a political case to councils, and
looking like *infrastructure* rather than a hobby project is worth more than
novelty — while still building on the identity already there.

### Decision (b) — mobile architecture

Replace the 45vh slab with a **draggable bottom sheet** with three detents:
peek (stop name plus next departure), half, full.

This is the expected pattern for map-based apps and the current standard for
secondary content over a map. It also **retires the Phase 1b bug structurally**:
a sheet always carries a handle, so "hidden with no affordance" becomes
unrepresentable.

Implementation notes:

- Use `svh` for containers that must not overflow. Prefer it over `dvh`, which
  recalculates as browser chrome appears and disappears and can cause visible
  layout shifts mid-scroll. Keep a `vh` fallback.
- `env(safe-area-inset-bottom)` on the sheet, `viewport-fit=cover` on the meta.
- CSS scroll-snap or a small pointer-events drag handler. No library.

### Work packages

| ID | Package | Effort | Contents |
|----|---------|--------|----------|
| P3.1 | Tokens and theme | 1 d | Hunt the 43 strays; add elevation, focus-ring and motion tokens; new palette across light and dark; contrast-check every pair to AA (4.5:1 text, 3:1 UI); re-tune the dark tile filter |
| P3.2 | Mobile shell | 2–3 d | Bottom sheet with detents; `svh` and safe areas; header condensation; 44px minimum targets; landscape handling; Phase 1b bug retired by design |
| P3.3 | Components | 2 d | Departure board first (it is what people stare at), ticket accordion, objective groups, forms. Full state coverage: hover, active, focus-visible, disabled, loading, empty, error |
| P3.4 | Performance | 0.5–1 d | Self-host and subset fonts, drop unused weights; self-host or preload Leaflet CSS; `content-visibility` on offscreen panels; optionally begin the ESM split of `app.js` (already the top item in `.HANDOFF.md`) |
| P3.5 | Motion | 0.5 d | View Transitions API on the four-view switch, progressive-enhanced behind `if (document.startViewTransition)`. Respect the three existing `prefers-reduced-motion` blocks |
| P3.6 | Public-ready | 1–2 d | WCAG 2.2 AA audit plus accessibility statement; Open Graph, Twitter card, JSON-LD; PWA manifest and maskable icons |

Total 7–10 days of focused work. Every package is independently deployable — it
is a static site.

Notes on P3.4 and P3.6:

- Self-hosting fonts also removes the Google Fonts GDPR question, which matters
  for a site being shown to local authorities.
- Same-document View Transitions reached Baseline Newly Available in October 2025
  across all three engines (Chrome 111+, Safari 18+, Firefox 144+). Cross-document
  is still Chromium-and-Safari only, so stick to same-document.
- On accessibility, the argument is sharper than "it is nice to have": UK public
  sector bodies are legally bound to WCAG 2.2 AA by the 2018 regulations. This
  site is not one, but it is *lobbying* ones. Arriving AA-compliant is
  credibility; arriving non-compliant hands them a free reason to dismiss you.
  AA's target-size floor is 24×24 CSS px; Apple's 44 and Material's 48 are the
  usability numbers, and 44 is the AAA criterion.

**Service worker: deferred deliberately.** On a site whose entire value is live
data, a botched service worker serves stale JS forever. Revisit as a separate
decision once the restyle has settled.

### The missing verification loop

No one can currently see the rendered page from the agent side. Every visual bug
so far survived for that reason — the CARTO "API KEY REQUIRED" watermark
(HTTP 200, so nothing caught it) and the Phase 1b collapse trap.

**Built 2026-08-29 — and Playwright turned out to be unnecessary.**
`scripts/browser_check.mjs` speaks CDP over node's built-in WebSocket to any
Chrome you already have, so there is no 150 MB download and no new dependency.
It asserts on the live page and can write screenshots at 390 / 768 / 1440 px:

```sh
python -m http.server 8765 &
uvicorn api.main:app --port 8000 &
flatpak run --share=network com.google.Chrome \
  --headless=new --remote-debugging-port=9222 about:blank &
node scripts/browser_check.mjs --shots ./shots
```

12 checks currently pass, covering the collapse regression end to end, the
basemap actually rendering tiles (not merely returning 200 — the CARTO lesson),
and all four views activating. Exits non-zero, so it can gate a release.

Chrome on this machine is a flatpak, which the chrome-devtools MCP cannot see —
it looks for `/opt/google/chrome/chrome`. The script sidesteps that entirely by
talking to whatever is listening on the debug port.

Extend this in Phase 3 with light/dark baselines per view.

---

## Skills

### Already installed

| Skill | Where | Use |
|-------|-------|-----|
| `ui-ux-pro-max` | plugin, enabled | 161 palettes, 57 font pairings, 99 UX guidelines, searchable → P3.1 |
| `frontend-design` | `~/.claude/skills` | Aesthetic direction, explicitly anti-generic → decision (a) |

### Worth enabling from the `ecc` marketplace

Already registered in `~/.claude/settings.json` under `extraKnownMarketplaces`;
enable via `/plugin`.

| Skill | Phase | Why |
|-------|-------|-----|
| `click-path-audit` | 1b | Scoped exactly to this bug class — "functions individually work but cancel each other out, leave the UI in an inconsistent state" |
| `browser-qa` | all | Playwright visual verification; closes the loop described above |
| `accessibility` | 3.6 | WCAG 2.2 AA, semantic ARIA |
| `make-interfaces-feel-better` | 3.3 | Spacing, typography, hit areas, interaction states |
| `design-system` | 3.1 | Token audit and styling-PR review |
| `security-review` | 1 | Endpoint gating checklist |
| `seo` | 3.6 | OG tags, structured data, Core Web Vitals |
| `production-audit` | pre-launch | Local-evidence readiness check |

Skip `motion-ui` and `frontend-patterns` — both React-oriented, and the plain-JS
plus View Transitions route suits this codebase better.

---

## Open decisions

1. **Phase 1 scope** — gate debug, fix CORS, fix `render.yaml`. Any debug endpoint
   that should stay reachable?
2. **Pull Phase 1b forward?** 1–2 hours, and the Ticket view is currently a dead
   end for mobile users who collapse the panel.
3. **Aesthetic direction** — recommendation is the civic-transit re-theme.
4. **Playwright** — set up the screenshot harness before Phase 3 so the restyle is
   verifiable rather than hopeful.

---

## Carried over from earlier work

Not part of the three phases, but still open:

- **Delete the `fix-dark-basemap` branch.** Same fix as `main`, different SHA, so
  not an ancestor — merging it later would conflict for no reason.
  `git push origin --delete fix-dark-basemap && git branch -D fix-dark-basemap`
- **GTFS midnight wraparound.** The builder wraps a 24:06 stop time to 00:06
  mod 86400, so it sorts to the front of the trip — 460 of 39,528 trips (1.2%).
  Currently *guarded* in `api/timetable_db.py` (`path_has_time_gap`,
  `MAX_SECS_PER_STOP`), so affected journeys are excluded rather than shown
  wrong. The real fix belongs in `scripts/build_timetable.py`.
- **`_noc_by_short_name` is last-row-wins** (`api/timetable_db.py:712`), so routes
  2/47/60 are misattributed to a Chichester Stagecoach service. Papered over with
  `_OPERATOR_OVERRIDES`; the TODO is still open.
- **`app.js` is 6,539 lines** in one file.
- **Env vars are still named `NEXTBUSES_*`** while pointing at TransportAPI.

---

## Sources

- [Wrangler KV commands](https://developers.cloudflare.com/kv/reference/kv-commands/) — `kv namespace` vs `kv:namespace` and the 3.60.0 cutover
- [Same-document view transitions are Baseline Newly available](https://web.dev/blog/same-document-view-transitions-are-now-baseline-newly-available)
- [CSS dvh, svh, lvh guide](https://csstoolkit.net/blog/css-dvh-svh-lvh-guide/) — why `svh` is the safer default
- [Bottom sheet UI patterns](https://mobbin.com/glossary/bottom-sheet)
- [WCAG 2.5.8 Target Size (Minimum), Level AA](https://silktide.com/accessibility-guide/the-wcag-standard/2-5/input-modalities/2-5-8-target-size-minimum/)
- [WCAG 2.5.5 Target Size (Enhanced), Level AAA](https://silktide.com/accessibility-guide/the-wcag-standard/2-5/input-modalities/2-5-5-target-size-enhanced/)

See also [`LIMITS.md`](../LIMITS.md) for the free-tier envelope every change here
has to fit inside.
