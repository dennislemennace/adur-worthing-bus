# Code context snapshot — briefing for an adversarial review

Written 2026-09-08 against `89c4497`. This is not documentation for
contributors; `README.md` and `docs/` do that. It exists to give an external
reviewer the things that are *not* recoverable by reading the code: why the odd
decisions are odd, where the load-bearing assumptions are, and which parts I am
least confident in.

**Read `.claude/skills/bus-site-conventions/SKILL.md` first.** It records house
rules with the incident that produced each one. A review proposing a bundler, or
link text in `--color-primary`, is re-litigating settled ground.

**Bias warning about this document.** I (Claude) wrote or touched a minority of
this repository across a handful of sessions; the history is 214 commits. Where
I say "I introduced this", it is attributable. Where I describe older code I am
reading it like you are, and my account of *why* it is that way is inference
unless a comment says otherwise. Trust §4 on provenance; trust §2 less on it.

---

## 1. Architectural blueprint

Three independently deployed things, no shared build:

```
  Browser ──────────────────────────────────────────────┐
    │                                                   │
    │ (a) static assets + curated JSON                  │ (c) POST /submit
    │     GitHub Pages, worthingbrightonbus.co.uk       │     Cloudflare Worker
    │     (branch build from main, root CNAME file)     │       ↓
    │                                                   │     GitHub Issues API
    │ (b) live data                                     │     (labelled unverified)
    ↓                                                   ↓
  FastAPI on Render free tier                     Human moderation:
  adur-worthing-bus.onrender.com                  scripts/add_*.py
    │                                                   ↓
    ├── BODS SIRI-VM (vehicles)                   data/*.json → (a)
    ├── TransportAPI (departure predictions)
    ├── RealtimeTrains (rail)
    └── SQLite timetable, fetched at startup from a GitHub Release
```

**The frontend is one 8,990-line `app.js` in global scope**, plus a 933-line
`index.html` and 5,280 lines of CSS. No modules, bundler, framework or
transpile. Deliberate, and defended in the conventions skill. Consequences for
review: every function is a global, load order in `index.html` *is* the
dependency graph, and one mutable `state` object (~80 keys) is read and written
by everything.

**Two data planes that look alike and are not.** `data/*.json` is *curated* —
hand-written or script-published, fetched by the browser straight from Pages,
never touched by the API. `/api/*` is *live*. Keep them apart when reviewing:
the failure modes differ completely (stale-but-consistent versus
fresh-but-absent), and several UI paths degrade one while trusting the other.

**The timetable bootstrap is the least obvious flow.** `_ensure_fresh()`
(`api/timetable_db.py:243`) downloads `timetable.sqlite` from a GitHub Release
on startup, comparing a `.sha256` sidecar against a locally cached hash. It
exists because Render has no persistent disk and the repo must stay small.
Failure is deliberately non-fatal — "a brittle release shouldn't take the API
down" — so **the API can serve a stale timetable indefinitely and only says so
in the log.**

**Curated-data contract.** Every published fact carries `source_url` and
`checked_on`, enforced by `tests/test_curated_data.py`. Derived statistics carry
five fields (`value` / `method` / `data_version` / `as_of` / `caveats`) per
`.claude/skills/evidence-provenance`. This is an advocacy site: a number without
its method is a defect, not a style preference.

---

## 2. State and logic quirks

Ordered roughly by how much I want a second opinion.

### 2.1 SQLite on the event loop — corrected, then fixed

**This section was wrong, and the correction matters more than the original
claim.** It described a single connection shared "across the threadpool",
reasoning that FastAPI runs sync endpoints in a threadpool. Every route in
`api/main.py` is `async def`, so FastAPI ran them **on the event loop and
offloaded nothing**: the SQLite queries, the SHA-256 hashing and the
timetable download all executed there, and one slow call blocked every other
request in the process. A warm production sample measured vehicles at 13.0s,
route lines at 6.2s and bus departures at 19.2s.

That was the real defect, and it was worse than the one described. It is now
fixed in both halves:

* `off_loop()` in `api/main.py` is the single route by which work leaves the
  event loop, applied to the departure board, service spans, route polylines,
  vehicle-to-trip matching and the timetable open/reload.
* `api/timetable_db.py` gives **each thread its own read-only connection**
  (`threading.local`, invalidated by a generation counter bumped on every
  reload) rather than passing one connection round a threadpool. Threading
  the work while keeping an unexamined shared connection would have replaced
  one unstated lifetime with another.

The lesson for a reviewer reading the rest of this file: I inferred the
concurrency model from what FastAPI usually does rather than from what these
handlers are declared as. Treat every "probably fine" below as an unverified
claim of the same kind.

### 2.2 The daily API quota counter is not daily

`_nb_quota` (`api/main.py:169`) throttles TransportAPI to
`NEXTBUSES_DAILY_LIMIT=300`. It persists to `data/.nb_quota.json`, which is
**gitignored and on Render's ephemeral disk.** The free tier sleeps after ~15
minutes idle and loses the filesystem on redeploy.

So the counter resets on every cold start: the effective cap is "300 per
instance lifetime", not per day. On a quiet site that never approaches the real
1,000/day upstream allowance, so it has not bitten — but the name and the
comment both promise a guarantee the mechanism cannot make. Same reasoning for
`_cache` (`api/main.py:129`): a plain dict with TTLs and **no lock**, cold on
every start, with concurrent writers racing benignly because entries are
idempotent.

### 2.3 The Worker fails *open* when unconfigured

`worker/src/index.js:536`:

```js
function isAllowedOrigin(origin, allowed) {
  if (allowed.length === 0) return true;   // unset in dev
  return allowed.includes(origin);
}
```

An empty `ALLOWED_ORIGINS` accepts every origin. A deliberate dev convenience —
but a typo that blanks the variable turns a public form endpoint into an open
relay to the GitHub Issues API, with Turnstile as the only remaining gate.
`api/main.py` was deliberately built to fail *closed* after an incident (§2.4).
**The two services disagree about this, and I think the Worker is on the wrong
side.**

### 2.4 CORS adds rather than replaces, on purpose

`api/main.py:203` `DEFAULT_ALLOWED_ORIGINS` is a tuple the `ALLOWED_ORIGIN` env
var *appends to*. The comment records why: the var sat in Render's dashboard
long before any code read it, and the moment the code honoured it, a stale value
took the live site off its own API within a minute. A reviewer proposing "the
env var should be authoritative" is proposing to reintroduce a known outage.

Both `worthingbrightonbus.co.uk` and the old `dennislemennace.github.io` are
canonical, so rolling the domain back is deleting one file rather than a tour of
three dashboards.

### 2.5 GTFS time is not clock time

GTFS writes past-midnight departures as `24:30`, `25:15`. Take `% 86400` before
placing one on a clock. This has been walked into before. Project notes also
record a **suspected midnight-wraparound bug in the timetable builder**
(`scripts/build_timetable.py`) that I have not verified — worth a targeted look,
because service-span figures feed published evidence.

### 2.6 `noc_for_short_name` is last-row-wins

Two operators can run routes sharing a short name. Resolving Brighton & Hove
routes at Portslade to Stagecoach reintroduced exactly the false positive the
fare logic exists to prevent. Read `routes.noc` per route wherever the operator
matters. An unresolved `TODO` at `api/timetable_db.py:1142` notes route "2" has
two rows.

### 2.7 Leaflet aliasing

`L.latLngBounds(x)` returns `x` itself, not a copy — aliasing two accumulators
this way made adding to one grow the other. `fitBounds` animates by default, so
a corrective `panBy` straight after is overwritten; pass `animate: false`.

### 2.8 Frontend timers and view teardown

Three `setInterval`s: vehicles (20s, `app.js:1219`), a 1-second "X ago" ticker,
and rail service polling. `fetchVehicles` has both an in-flight guard
(`state.vehicleFetchInFlight`) and a *post-hoc* staleness check. **They solve
different problems and both are needed**: the guard stops overlapping requests,
the staleness check stops a request already in flight from re-adding markers
after the user has left the live view.

`applyViewMode` must tear down layers in *every* non-owning branch. Route view
once had no teardown of its own while every other branch did; that asymmetry is
the easiest thing here to get wrong.

### 2.9 Rate-limit counters undercount

The Worker's KV counters use read-modify-write and undercount under concurrency.
Accepted and documented — coarse abuse bounds, not accounting. The binding
constraint is the **KV write budget** (1,000/day free, 3 writes per submission →
~300/day), not the 100k request limit; the Worker caps itself at 200/day
deliberately under that.

---

## 3. Implicit dependencies and non-obvious side effects

- **Leaflet from a CDN as a global `L`.** No import statement anywhere. Tests
  stub it with a Proxy (`tests/load_app.mjs`).
- **`app.js` has no exports.** Node tests evaluate the whole file in a `vm`
  context with a hand-built DOM stub. Two consequences that bite: `const`
  declarations never land on the context object, so `state`, `BODY_AREA` and
  friends need `vm.runInContext("name", ctx)`; and values built inside the vm
  come from another realm, so `deepStrictEqual` rejects them on prototype
  identity — hence the `plain()` JSON round-trip helper.
- **`stop_times` is `WITHOUT ROWID`.** Trip-path lookups are primary-key seeks;
  query plans assuming a rowid table will mislead.
- **`Image.FASTOCTREE` is the only PIL quantiser that preserves alpha.** Both
  icon scripts depend on it. Switching quantiser silently flattens transparency.
- **PNG decode cost is `width × height × 4` regardless of palette.** An earlier
  46% palette reduction did nothing for memory; the fix was smaller images (§4.6).
- **`postcodes.io`** — third-party, keyless, called *from the browser* in the
  councillor flow. Deliberately not proxied: it keeps the reader's postcode off
  servers this project controls and off the Render quota. An availability
  dependency with no fallback.
- **Council ModernGov feeds and the ONS Open Geography Portal** are build-time
  dependencies of `scripts/build_representatives.py`, not runtime ones.
- **Turnstile** loads from Cloudflare and fails *silently* on a domain mismatch
  (error 110200 — "Domain not authorized"): the widget renders and never
  validates.

---

## 4. Caveats — things I introduced, and what I am unsure about

This section is why the document exists. Argue with it.

### 4.1 `data/representatives.json` is 239 real people's email addresses

Built by `scripts/build_representatives.py` from four councils' own ModernGov
directories. Every address is read from the published feed; **none is derived
from a `firstname.lastname@` pattern**, because a guessed address that looks
right is worse than none on a site whose value is being checkable. Only official
council domains are accepted — East Sussex's feed carries personal `gmail.com`
addresses that the filter drops.

Where I am not confident:

- **Staleness.** These go wrong at every election. `checked_on` makes it visible;
  it does not fix it. There is no re-check schedule.
- **The name→ONS-code join is heuristic.** `PLACE_DISTRICTS` is a hand-written
  table mapping settlement suffixes ("Shoreham-by-Sea") to districts, needed
  because Adur and Worthing both have a "Marine" ward inside one combined feed.
  Correct for today's data; it will need extending if a feed changes shape. A
  wrong join sends a resident's email to the wrong councillor and **looks like it
  worked**, which is the worst failure class here.
- **Nothing has ever been sent.** The `mailto:` construction is unit-tested; no
  real message has gone to a real councillor from this site.

### 4.2 The objective texts are advocacy prose I wrote

All eight `description` fields in `data/objectives.json` were rewritten by me and
are pasted verbatim into the letters the councillor flow generates. Every
*figure* is either already measured in this repo (`data/boundary_evidence.json`)
or verified against primary sources — the accessibility deadlines are SI
2023/715, checked against legislation.gov.uk. The *rhetoric* is mine and has had
no adversarial read. Treat these as claims made in the project's name to elected
officials.

### 4.3 Asymmetry I knowingly left in the moderation scripts

`scripts/add_proposal.py` (written 2026-09-07) clears the `unverified` label when
it closes an issue. **`scripts/add_suggestion.py` does not** — it closes the
issue and leaves the label, so a published idea still reads as "nobody has looked
at this". I fixed the new script and did not backport it: known debt, one-line
fix.

`add_proposal.py` has two properties I want challenged. It refuses to let a
submission set its own `category` (which would let anyone with the form make the
site draw their route as an official project proposal), and it bounds-checks
every coordinate because a transposed `[lon, lat]` pair parses cleanly and draws
a line into the Indian Ocean. 19 tests cover the refusals. I believe the threat
model is right; I would like someone to try to get past it.

### 4.4 A check I wrote, shipped, then deleted

`scripts/browser_check.mjs` carries a comment where a test used to be. I had
asserted "the contact button is not nested inside the card button" — and it
passed with the bug deliberately restored, because the HTML parser closes an open
`<button>` on meeting another, so `innerHTML` can never produce a nested one in
the DOM. The check could not fail. I removed it and left the reasoning in place.
**If you find other checks in that file that cannot fail, that is the pattern to
hunt.** The repo's rule is that a new check must be proven red before it is
trusted green; I do not know that every existing check was.

### 4.5 The brand mark has no vector source

`brand/mark-source.png` is a 1254px raster. Everything — header mark at 1×/2×/3×,
both favicons, the `.ico` — is generated from it by
`scripts/prepare_brand_mark.py`. At the 40px it renders at, that is a 27:1
downsample. It looks right (checked at true size), but there is no path to
sharper output without new artwork, and the hand-drawn SVG that used to serve
this purpose was deleted because it was *a different drawing wearing the same
name*.

### 4.6 Stale context that would waste your time

My own planning notes describe a "Live tracking performance" package as
approved-but-unbuilt: icon oversampling, baked shadows, hidden-tab polling, an
in-flight guard. **I checked while writing this: all of it is built.** Icons are
168×112, the shadow is baked by `scripts/build_icons.py`, polling pauses via
`syncVehicleRefreshToVisibility()`, and `fetchVehicles` guards itself. Do not go
looking for that debt.

Deliberately *not* built and still open: bus-marker culling/clustering at high
vehicle counts (~200 markers at the daytime peak, most off-viewport), ~1,400 stop
markers built at load, uncached rail markers, and an undebounced `moveend`
handler.

### 4.7 Things a reviewer might reasonably call wrong

- `index.html` has **no `canonical`, `og:` or `twitter:` tags**, on a site whose
  purpose is being pasted into emails and social posts. Known gap, not yet done.
- The night-service livery fallback strips a leading `N` and retries
  (`iconForService`, `getRouteColour`, `getLineColour`). It cannot invent a
  livery — it only resolves to one already asserted for that route on that
  operator — but it **assumes a night route always wears its day route's
  identity**. True for every current case; not guaranteed.
- `--target-min: 44px` targets WCAG 2.5.5 **AAA**, well above the 24px AA floor,
  and `browser_check.mjs` enforces it. This constrains layout more than most
  sites accept. Deliberate: the site lobbies bodies themselves legally bound
  to AA.
- Dialogs need `margin: auto` re-added because the universal reset zeroes the
  UA's centring for `<dialog>`. Fixed — but the reset will keep doing this to
  any new element relying on a UA margin.

---

## 5. Where to point the sharpest tools

If review time is limited, in order:

1. **`worker/src/index.js`** — the only endpoint accepting unauthenticated public
   input. Start at §2.3.
2. **`scripts/add_proposal.py`** — the gate between that input and the live site.
3. **`api/main.py` CORS and quota** — §2.1, §2.2, §2.4.
4. **`api/timetable_db.py` service-span and midnight handling** — §2.5; its
   output is published as evidence to councillors.
5. **`data/representatives.json` and its builder** — §4.1; wrong here means a
   real person's email reaches the wrong elected official.

Test baseline at `89c4497`: **85 pytest, 153 node, 29 worker, 126 browser
checks**, all green. `browser_check.mjs` needs a local static server, a running
API and headless Chrome on `:9222`; the invocation is in the conventions skill.
