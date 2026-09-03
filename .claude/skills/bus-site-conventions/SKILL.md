---
name: bus-site-conventions
description: Conventions and known traps for the adur-worthing-bus repo. Use when editing app.js, style.css, index.html, api/, scripts/ or data/ in this project — before writing CSS that sets a colour, before adding a panel or tab, before adding a curated data file, and before claiming any visual change is verified.
---

# adur-worthing-bus — house rules

Hard-won, mostly from shipping the bug first. Each rule names the failure that
produced it, because a rule without its incident gets argued away.

## Verification

**`scripts/browser_check.mjs` is the arbiter for anything rendered.** Unit tests
cannot see layout, contrast or reachability. Run it before saying a visual change
works:

```sh
python -m http.server 8765 --bind 127.0.0.1 &
uvicorn api.main:app --port 8011 &
flatpak run --share=network com.google.Chrome --headless=new --remote-debugging-port=9222 about:blank &
SITE_URL="http://127.0.0.1:8765/?api=http://localhost:8011" node scripts/browser_check.mjs
```

**A new check must be proven to fail before it is trusted to pass.** Revert the
fix, watch the check go red, restore the fix. A check written against a bug you
have already fixed is untested code asserting a tautology.

> Incident: a reachability check was added, ran green at 59/59, and still passed
> with the bug restored — it only ran at 390px, where the panel is a bottom sheet
> that does not clip. It would have certified the bug as fixed. Viewport-dependent
> defects need the check at every viewport.

**A 200 is not a pass.** The CARTO basemap returned HTTP 200 while stamping
"API KEY REQUIRED" across every tile. Assert on the rendered result.

**Disable the cache when verifying.** `Network.setCacheDisabled` is in the harness
because a run once validated the *previous* stylesheet — every check passed and
none of them had looked at the new code.

## CSS

**Link text uses `--color-link`, never `--color-primary`.** The primary is a fill
colour; as text it measures ~3.4–3.7:1 on the dark surfaces and fails AA. There is
a comment on `.proposal-link` saying exactly this, and it has been walked into
twice since.

**Define every colour on bare `:root` first.** A colour whose only definition sits
inside a media query or `[data-theme]` block does not apply in the un-stamped
state, and the page renders one theme's text on the other theme's ground.

**Any new `.panel-tab-content` must be reachable.** The container scrolls itself.
Do not reintroduce `overflow: hidden` on it in the belief that an inner element
will absorb the overflow — three views forgot to nominate one, and a view that
forgets does not scroll awkwardly, it silently clips.

**`[hidden]` loses to an author `display`.** `style.css` carries
`[hidden] { display: none !important; }` for this reason. Setting `el.hidden` on
something styled `display: flex` otherwise does nothing at all.

## Frontend architecture

**No build step, no framework, no bundler.** `index.html` + `app.js` + `style.css`,
Leaflet from a CDN. Do not introduce React, a bundler, or a package manager for
the frontend.

**Curated data is fetched at runtime from `data/*.json`.** Five files already work
this way. A sixth follows the same pattern — a memoised loader, a reconcile
function, and teardown in every view that does not show it.

**Add teardown to every non-owning branch of `applyViewMode`.** Route view once had
no teardown of its own while every other branch tore its layers down; that
asymmetry is the easiest thing here to get wrong.

**Naming: "boundary" means the *fare* seam.** Anything administrative is spelled
out as `councilBoundary` / `councilBoundaryLabels`. The two must never be confused.

## Curated data

**Every published fact carries `source_url` and `checked_on`, and a pytest schema
test.** `tests/test_curated_data.py` validates schema, not values, which is why it
survives data edits untouched. New curated files get the same treatment in the same
file. For *derived* statistics, see the `evidence-provenance` skill.

**Prefer schema tests to value tests.** `tests/test_boundary_calc.mjs` pins 36
specific ids and is the most brittle file in the repo. Do not add to that pattern.

**Check any file path referenced from data actually exists.** `add_update.py` and
`pytest` both verify update images are in the tree, because a hero that 404s is
only visible once the page is live.

## API and pipeline

**`LIMITS.md` governs anything that costs quota.** Before adding an external call,
a polling loop, or a scheduled job, check it fits the free-tier envelope. Prefer
precomputing at build time over an endpoint: the timetable only changes weekly, and
Render's free tier sleeps.

**GTFS writes past-midnight times as `24:xx` and beyond.** Take `% 86400` before
placing a departure on a clock. A trip at `24:30` is half past midnight, not half
past eight in the evening.

**`noc_for_short_name` is last-row-wins.** Two routes can share a short name across
operators. Read `routes.noc` per route when the operator matters — resolving B&H
routes at Portslade to Stagecoach reintroduced the exact false positive the fare
logic exists to prevent.

**`L.latLngBounds(x)` returns `x` itself, not a copy.** Aliasing two accumulators
this way made adding to one grow the other.

**`fitBounds` animates by default.** A corrective `panBy` applied straight after is
overwritten. Pass `animate: false`.

## Git

**No `Co-Authored-By` trailers.** They were stripped from this repo's history
deliberately; do not reintroduce them. Do not add `Claude-Session` trailers either.

**Stage explicitly, never `git add -A`.** The working tree routinely carries
unrelated in-progress work.

**Commit and push only when asked.**
