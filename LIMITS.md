# Free-tier limits

This site runs **entirely on free tiers**. Every external service it depends on
has a usage cap that, if exceeded, will silently degrade the site or push us
onto a paid plan. Before adding a new external call, scheduled job, polling
loop, or build step, check the relevant section below and confirm the change
fits within the existing envelope.

> **Numbers below are accurate as of 2026-05-20.** Free-tier policies change.
> When in doubt, verify on the provider's dashboard or pricing page before
> committing.

---

## Render.com — backend host

- Plan: free web service (see `render.yaml`, `plan: free`).
- **750 instance-hours / month**, per workspace — not per service. A second
  free service running alongside this one shares the same allowance.
- Sleeps after ~15 min idle → cold start on the next request. **Measured
  2026-09-11: 22.4 s to first byte after a 17-minute idle, against 0.15 s
  warm.** The frontend handles cold starts; do not regress that behavior.

**The keep-warm budget.** A cron trigger on the Cloudflare Worker
(`worker/wrangler.toml`, `keepWarm()` in `worker/src/index.js`) pings the service
every 10 minutes between 07:30 and 23:30 Europe/London. The last ping, at 23:20,
keeps it up until about 23:35, so the instance is warm roughly 16 hours a day:
**about 500 hours in a 31-day month, two thirds of the allowance.** The third
left over is deliberate. Every overnight visit wakes the service for at least
15 minutes, redeploys count, and a second free service in the workspace would
draw on the same 750. (The first version slept only 01:30 to 06:30 and used
about 597 hours, 80%; it was narrowed on 13 September 2026 to leave more room.)

The window is checked as *inside the warm hours*, not *inside the quiet ones*.
The quiet period crosses midnight, and a `from < now < to` test across midnight
is never true, so written the other way round the job pings around the clock.

**Why a Worker and not GitHub Actions.** It started as a GitHub schedule set to
every 10 minutes. GitHub treats schedules as best effort, and between 12 and 13
September 2026 it started that workflow 13 times in 38 hours instead of about
228, with a median gap of 163 minutes. Every gap was longer than Render's
15-minute idle limit, so the service slept regardless. Cloudflare cron triggers
fire on time. The GitHub workflow is kept only as a backup and should not be
relied on. The idle limit is 15 minutes, so the cadence stays at 10.

The static stop list (`data/stops.json`, see below) is what makes the overnight
window cheap: the map draws without the API, so someone checking a night bus
after 23:30 still gets the map and its stops while the container wakes. Route
lines and timetables are API calls too, and arrive with it.
- 512 MB RAM, ~0.1 vCPU (shared).
- **No persistent disk.** Anything written to disk is lost on redeploy. The
  GTFS SQLite (`data/timetable.sqlite`) is fetched from a GitHub Release on
  startup (`api/timetable_db.py`) — keep that flow intact.
- 100 GB egress / month.
- Build time: ~15 min hard ceiling.

**Implications**

- No long-running background workers; no cron-style processes inside the
  Render service. Use GitHub Actions for scheduled work instead.
- No in-memory caches that assume the process stays warm overnight. The
  process will be killed between idle periods.
- Don't ship large model files or datasets into the repo or build.

## GitHub — source, Actions, Releases, Pages

- Public repos: **unlimited Actions minutes**. Going private would re-introduce
  a 2,000 min / month cap.
- Release asset size: up to 2 GB per asset (the weekly `timetable-latest`
  release sits well under this).
- GitHub Pages hosts the frontend at **worthingbrightonbus.co.uk** (custom
  domain via the root `CNAME` file; the `dennislemennace.github.io` URL 301s
  across): 100 GB / month soft bandwidth cap, 1 GB site size, 10 builds / hour.
  Cloudflare serves DNS only — it is not proxying, so it absorbs none of that
  bandwidth and none of these limits are shared with it.
- `.github/workflows/update-timetable.yml` runs weekly — keep that cadence.
  Increasing the schedule eats into the (currently unlimited) public-repo
  budget needlessly.
- `.github/workflows/keep-warm.yml` is a backup for the Worker's cron. It is
  scheduled every 10 minutes but GitHub runs it every few hours in practice
  (see the Render section). Free on a public repo; if the repo went private it
  should be deleted rather than budgeted for.
- **GitHub disables scheduled workflows in a repository with no activity for 60
  days**, and the nightly processor no longer commits. `keepalive.yml` re-enables
  every scheduled workflow each Monday, and makes an empty commit if there has
  been none for 45 days. The Worker's crons are Cloudflare's and unaffected.
- Pages also serves `data/stops.json` (~290 KB, ~41 KB gzipped), downloaded on
  a visitor's first load and then cached. At the 100 GB monthly Pages
  allowance that is not a constraint; it is listed so a future change that
  inflates it is a deliberate one. The last one was: from 24 September 2026
  every stop carries its `services` and `locality`, adding 3.4 KB gzipped
  (0.5 KB for the services on 189 unique-name stops, 2.9 KB for localities),
  so the journey-time view can offer every stop and find one by its town.

## BODS — Bus Open Data Service (SIRI-VM vehicles)

- Free, requires `BODS_API_KEY` (Render env var).
- No published hard rate limit; "fair use" applies.
- Current usage: a single bounding-box poll of `/datafeed/` every
  `VEHICLE_REFRESH_MS` (default 20 s) per active browser tab.
- Every caller of the feed goes through one single-flight cache
  (`_live_vehicles` in `api/main.py`): the map, a bus panel and the gap monitor
  share one fetch per 15 s whoever asks. `/api/vehicle` and the monitor once
  had their own path that fetched on a cache miss and matched trips on the
  event loop, which stalled the API for seconds on the free instance.
- The A259 gap monitor (`/api/corridor-gaps`, `api/corridor_gaps.py`) covers
  both directions in one answer, cached for 30 s, from that shared cache. The
  page asks once a minute, only in Live view, and not at all from 23:30 to
  04:30 London time, so it never wakes the service overnight. It does not use
  TransportAPI: six stops polled every minute would have spent the 300-a-day
  cap by mid-morning.

**Implications**

- Do not add per-stop or per-bus BODS calls — the bounding-box poll is the
  supported pattern.
- Don't drop `VEHICLE_REFRESH_MS` below ~15 s without a strong reason; many
  concurrent users multiplied by a fast poll will trigger fair-use throttling.

## TransportAPI — real-time departure predictions

(Env vars are named `NEXTBUSES_*` for legacy reasons — they point at
`transportapi.com`. Do not rename without updating Render env config.)

- Free developer plan: **1,000 hits / day** (historical; verify on dashboard).
- Current cap: `NEXTBUSES_DAILY_LIMIT=300` (conservative, env-configurable).
- Per-stop response cache: `nb:{stop_id}` for `NEXTBUSES_CACHE_TTL=90` s.
  **Hardcoded constant** at `api/main.py:67` — not env-configurable; change
  requires a code edit.
- Skip threshold: don't query if the next scheduled departure is more than
  `NEXTBUSES_SKIP_THRESHOLD_MINUTES=30` away.

**Implications**

- Don't raise the daily cap without confirming the current plan still allows
  it.
- Per-stop caching is load-bearing — don't remove it. The site must continue
  to work when the daily quota is exhausted (timetable-only fallback exists).
- **Every path that calls upstream must go through the quota gate.**
  `/api/debug/live-raw` did not: it called TransportAPI directly with no cache
  and no counter, so it bypassed `NEXTBUSES_DAILY_LIMIT` *and* spent the real
  upstream allowance, unauthenticated. It now shares the cache and gate. A
  diagnostic is still a caller.

## GoatCounter — visit counting

(`loadAnalytics` and `track` in `app.js`, and `analytics-page.js` for the
static pages. Off without a site code, and for readers who switch it off on
`privacy.html`.)

- Hosted service, free for "reasonable public usage". There is no published
  hard cap, only a note that millions of page views a day is beyond it. This
  site is several orders of magnitude below that.
- Each page load is one page view. Events (`track()` names: views, journey
  checks, councillor letters, submissions, gap alerts, cold starts,
  accessibility changes) are counted the same way, so a busy session can be a
  dozen or so. The Live view's polling never counts.
- Not loaded for Do Not Track, Global Privacy Control, localhost or LAN
  previews, and usually stopped by content blockers, so every figure is an
  undercount. Say so beside any number quoted from it (evidence-provenance).

**Implications**

- Do not add a `track()` call inside a timer or a poll. Count things a person
  did, once.
- If the free service ever becomes unsuitable, GoatCounter can be self-hosted,
  and Umami Cloud's free tier (100K events a month) takes the same event model.
- `privacy.html` lists what is counted. A new event name is a change to that
  list, and `tests/test_privacy_notice.py` fails until the list says so.

## Cloudflare Workers + KV — community submission relay

(`worker/` — takes idea / proposal / stop-issue submissions and files them as
GitHub issues. Replaced the Web3Forms email relay.)

- Workers free plan: **100,000 requests / day**, 10 ms CPU per request, and up
  to 5 cron triggers per account.
- The keep-warm cron runs about 108 times a day (every 10 minutes, 06:00 to
  23:50 UTC); runs outside the London window return without a request. Waiting
  on Render's reply is wall time, not CPU time.
- Workers KV free plan: **100,000 reads / day, 1,000 writes / day**, shared with
  the snapshot recorder below (~1,170 reads and ~24 writes a day).
- Turnstile: free, unlimited.
- Each submission costs 3 KV reads + 3 KV writes (hour, day and global
  counters), so **the KV write budget caps out around 300 submissions/day** —
  the binding constraint, well before the request limit.

**Implications**

- The Worker enforces its own global cap of **200 submissions/day**, deliberately
  under the ~300/day KV write ceiling. Don't raise it without recalculating
  against the write budget.
- Per-client limits are 5/hour and 20/day, keyed on a salted hash of the IP.
- Rate-limit counters use read-modify-write and can undercount under
  concurrency. That's accepted: these are coarse abuse bounds, not accounting.
- If the Worker is unreachable the forms fail with a visible message and nothing
  is lost silently — but submissions are simply not accepted until it returns.
  There is no queue and no fallback path.

## Cloudflare R2 — the reliability record

(`worker/src/recorder.js` — two feeds, a minute apiece, for the nightly
processor: BODS SIRI-VM positions 05:00 to 00:30 London as
`raw/YYYY-MM-DD/HHMM-<UTC-epoch-seconds>.xml`, and BODS GTFS-RT **around the clock** as
`rt/YYYY-MM-DD/HHMM-<UTC-epoch-seconds>.pb`.

The hours differ because the costs do. SIRI-VM is 285 KB a minute and says only
where a bus is; GTFS-RT is 34 KB and names the scheduled journey it is running,
which is what makes an arrival measurable. Recording the cheap feed all night
is what puts the night services — the ones this site is arguing about — into the
evidence at all; they were simply absent while both feeds shared one window.)

**R2 charges automatically past the free allowances and has no "stop at the
free tier" switch.** The only cut-off is the one in our own code, so this
section is the arithmetic that cut-off is set from.

| Free each month | Then | What we use |
|---|---|---|
| 10 GB stored | $0.015 / GB | ~392 MB a day written, pruned to ≤ 7 days → 2.74 GB held |
| 1M Class A (write, list, delete) | $4.50 / M | ~81k writes, ~14k lists, ~81k deletes — 176k, 17.6% |
| 10M Class B (read) | $0.36 / M | ~81k, the processor reading each object once |

- **Operations cannot breach the free tier by construction.** 2,610 writes a
  day — 1,170 SIRI plus 1,440 GTFS-RT — is 80,910 in the longest month; lists
  and deletes are bounded by those writes. The total is 17.6% of the Class A
  allowance. A test in `worker/test/recorder.test.js` asserts this arithmetic
  *from the constants themselves*, so neither a faster cadence nor a third feed
  can be introduced without failing it. It used to assert a hard-coded 44,640
  and said nothing when the second feed doubled it.
- **Storage is the only real risk**, and only if the nightly processor stops
  deleting what it has used. A month of unnoticed failure would reach 10 GB.

**The hard limits, all enforced in the recorder before anything is written**

- **Retention: 7 days, and only once a day is safe.** Once an hour the Worker
  lists the bucket and deletes whole days older than that *if the published
  catalogue (`journey-times/index.json`) names them*: the nightly run archives a
  day's raw objects in an immutable release before it publishes the day. A day
  no run has processed is kept, and still counts against the budget below; if
  the catalogue cannot be read, nothing is deleted. Until 26 September 2026
  days went at seven regardless, so a week of failed runs lost them for good.
  Unattended failure now fills the 4 GB budget in about 12 days and recording
  pauses: new minutes are lost, the unprocessed days are not.
- **Storage budget: 4 GB and 30,000 objects.** The 4 GB is 40% of the free
  allowance and is the limit that matters, because bytes are what R2 charges
  for. Past either, the recorder stops writing and logs `snapshots paused`
  rather than spending. The margin absorbs an hour of writing on a stale
  measurement.
- **The object ceiling is a runaway guard, not a cost control** — objects are
  billed as operations, which are nowhere near their limit. It was 15,000, sized
  when one feed was recorded, and a week of two feeds is 18,270: the recorder
  would have refused itself partway through every week, which is a scheduled
  outage written into a constant. A test now derives the window from
  window × feeds × retention and fails if it does not fit, with headroom.
- **The measurement is cached in KV** (`r2-usage`), read each minute and written
  only when it is taken — once an hour, ~24 writes a day against the 1,000 the
  submission counters share. Measuring every minute would spend 1,440 of them,
  which is what the hourly test exists to prevent.
- A failed measurement or an unreadable budget never stops recording: losing a
  minute of evidence is worse than acting on an hour-old measurement.

**Implications**

- Raising the cadence, the retention window or either budget means redoing the
  arithmetic above. The tests fail on the arithmetic, not on the constants.
- R2 has no per-account spending cap to fall back on, so treat "the code stops
  it" as the whole control. A billing alert in the Cloudflare dashboard is worth
  adding as a second pair of eyes, not as the limit.

## Diagnostics — `/api/debug/*`

Gated behind `DEBUG_ENABLED` (off unless set) via a router-level dependency in
`api/main.py`, so a diagnostic added later is gated by construction rather than
by remembering. `/docs`, `/redoc` and `/openapi.json` are gated by the same
flag — the schema is a map of the surface.

**Implications**

- Add new diagnostics to `debug_router`, never to `app`. A test walks the
  routes and fails if one is missing the gate.
- The gate raises **404, not 403** — a 403 confirms the route exists.
- Leave `DEBUG_ENABLED` unset in Render. Turn it on to diagnose, then off.

## GitHub REST API — issue creation

- 5,000 authenticated requests / hour for the fine-grained PAT.
- One submission = 1 call, or 2 when stop-issue dedupe search runs.
- At the Worker's own 200/day cap this is ~400 calls/day against a 120,000/day
  allowance — not a constraint, but don't add per-submission API chatter
  (label lookups, project-board moves) without rechecking.

## postcodes.io — postcode to councillor lookup

- Free, no API key, no registration, CORS-enabled. Run by Ideal Postcodes as a
  public service over ONS ONSPD data.
- No published hard rate limit; fair use applies. Bulk lookups are explicitly
  supported via `POST /postcodes` (100 at a time) if that is ever needed.
- **Called from the browser, not the backend.** One request per postcode the
  reader actually types, in the "Email your councillor" flow. Nothing is
  cached, because nothing is repeated: a reader looks up their own postcode
  once.
- This costs no Render, Worker or BODS quota, and the reader's postcode never
  reaches a server this project controls. Keep it that way — proxying it
  through the API would add a cold-start-prone hop and turn a lookup nobody
  stores into one we would have to promise not to.

**Implications**

- Do not pre-fetch, warm, or batch-validate postcodes. The only acceptable
  volume here is one call per deliberate user action.
- `data/representatives.json` is committed, so the councillor half of the
  lookup costs no request at all. It is rebuilt by
  `scripts/build_representatives.py`, which hits each council's ModernGov
  directory and the ONS Open Geography Portal — a build-time cost, not a
  runtime one, and one that should be re-run after an election rather than on
  a schedule.

## OpenStreetMap — `tile.openstreetmap.org` (both themes)

- Tile usage policy: https://operations.osmfoundation.org/policies/tiles/
- No per-key limit, but expects moderate non-commercial use, correct
  attribution, and no hot-linking from very high-traffic sites.
- If traffic ever grows past hobbyist scale, move to a self-hosted /
  CDN-fronted tile source rather than hammering the OSMF tile servers.

## Dark theme — no second tile provider

Dark mode is a CSS filter over the same OpenStreetMap tiles the light theme
loads (`html.dark-mode .leaflet-tile-pane` in `style.css`). It is **not** a
separate basemap, so it adds no requests, no key and no third-party cap.

**This replaced CARTO in August 2026, and the reason is worth keeping.**
`basemaps.cartocdn.com` was free-for-non-commercial-use with attribution, and
then started requiring an API key: every tile came back stamped "API KEY
REQUIRED" — but with **HTTP 200**, so no error handling anywhere caught it and
dark mode broke silently for every user until someone happened to look.

**Implications**

- Prefer rendering over fetching. A filter over tiles already being requested
  cannot be withdrawn, rate-limited or keyed by a third party.
- If a dark basemap provider is ever adopted again, monitor the *content* of a
  tile, not just its status code. A 200 proves nothing.
- Switching theme no longer refetches tiles, which is also kinder to the OSM
  tile policy below.

---

## Guidance for future edits

1. **Any new external HTTP call** — from the API layer or the frontend —
   must declare:
   - The cap and pricing tier you're on.
   - A caching strategy (TTL, key) that keeps expected traffic safely under
     the cap.
   - A graceful-degradation path when the cap is hit.

2. **Reducing a poll interval** (e.g. `VEHICLE_REFRESH_MS`) or **raising a
   cache TTL** needs a back-of-envelope calc against the relevant cap.
   Document the calc in the PR.

3. **Scheduled GitHub Actions:** cron frequency should fit the unlimited
   public-repo budget. If the repo ever goes private, audit Actions usage
   before merging anything that runs on a schedule.

4. **The site must continue to work when:**
   - Render is cold-starting (~30 s of unavailability).
   - TransportAPI's daily quota is exhausted.
   - BODS returns an empty / stale feed.

   Both fallback code paths exist today (`api/main.py` quota check, frontend
   stale-vehicle filtering). Don't regress them.

5. **Don't switch to a paid tier of any service** without a separate decision —
   this doc exists to keep that pressure off, not to enable it.

## Reliability evidence and immutable publication — implementation budget

Added 23 September 2026. These are application safeguards, not a new verification
of provider pricing. No paid tier or increased raw retention is introduced.

- Recorder keys now include UTC minute identity:
  `raw/YYYY-MM-DD/HHMM-<UTC-epoch-seconds>.xml` and
  `rt/YYYY-MM-DD/HHMM-<UTC-epoch-seconds>.pb`. Readers accept old HHMM-only keys too.
  This prevents overwriting the repeated autumn clock hour without increasing
  poll cadence. The existing 7-day / 4 GiB / 30,000-object raw limits remain.
- Replay evidence goes into immutable GitHub releases. A compressed daily
  archive over **250 MiB** stops publication before upload. Each includes exact
  raw objects, timetable and code, so repeated timetable bytes are deliberate.
  Actual daily compression/storage growth must be measured on live inputs;
  this is a maximum, not a forecast or a cumulative retention policy.
- The published R2 bucket has a separate **4 GiB** pre-upload budget, including
  old generations. Together the nominal raw and published budgets are 8 GiB;
  other account usage and raw measurement headroom still need monitoring.
  No automatic deletion invalidates old share links. At saturation the
  workflow stops and keeps the previous public generation; an explicit
  retention/migration decision is required before capacity can be reclaimed.
- Every new generation adds per-service documents, summaries and observations,
  then reads each uploaded object back once before switching the pointer.
  The reviewed five-day derivative has 38 service files plus an index, about
  36 MB uncompressed before observation/summary assets. Full 35-day method-4
  bytes, read-back traffic and release growth have not been measured. Each
  rebuild repeats those bytes, even when much of the input window overlaps.
- Heavy processing stays in Actions. The five-day local builder used about
  **832 MiB peak RSS** and 6.96 seconds on the review machine. This does not fit
  the Render free instance and is not a 35-day capacity test. Compact consumer
  projections avoid retaining full raw-report payloads in aggregate jobs;
  benchmark the complete method-4 window before expanding collection or UI
  retention.
- Historical restore downloads are bounded by the current 35-day window and
  the requested month, while the small source catalog retains the complete
  known history. A missing archived day is an error, never silently omitted.
  Timetable archive searches paginate because daily evidence releases can
  push older timetable tags beyond the first page.

## Recorded timetable and delay map — measured sizes

Added 23 September 2026, measured on that day's timetable rather than estimated.

- **The day's timetable now travels inside each observations file**
  (`observations-DAY.json.gz`, key `schedule`), so it is archived wherever the
  observations are: the immutable evidence release, the monthly release and the
  publication bundle. No new upload path, no new request. On 23 September it held
  2,824 scheduled trips over 197 route patterns and 1,135 run-time profiles:
  **about 236 KB gzipped a day**, roughly 15–20% on top of the observation file
  itself. (The plan estimated 30–50 KB; run times vary far more per trip than
  assumed, so profiles barely deduplicate. Delta encoding would save about 20%
  and was declined: plain offsets are easier to audit.) At that rate a year of
  recorded timetables is under 90 MB of release storage. Publication refuses a
  day without one.
- **Service documents** fold the window's timetables in as `schedule`, with each
  day pointing at a deduplicated set of trips. Every weekday under one timetable
  is identical, so a 35-day window stores a handful of sets per service.
- **Delay-map files** (`hotspot-map-*.json`, one per service, plus an index):
  48 files and **675 KB in total** for 23 September's timetable before any
  current-method journeys were in them, the largest 66 KB. They grow with the
  cells measured: on 24 September one day of journeys took the 2's file to
  507 KB, over the 500 KB raw cap then in force, and the whole night's
  publication stopped. Map cells now carry only what the map uses (no route
  pattern hash or status; builds named by a 16-character hash), about a third
  smaller, and `check_published.py` judges a file by what a phone downloads:
  at most **200 KB compressed** (gzip, as a cautious stand-in for the Worker's
  Brotli), with a 4 MB raw ceiling. A simulated 2,100-cell file, weekdays and
  weekends for a route like the 2, is about 640 KB raw and 32 KB compressed.
  They are rebuilt into every publication generation, so budget about 1 to 2 MB
  of each generation's share of the 4 GiB published total for them.
- Building the delay map with its geometry took 3.4 seconds and 430 MB peak RSS
  on the review machine. Heavy, but in Actions, and nowhere near Render.
- No new external calls. Stretch geometry comes from the timetable's own shapes;
  where the feed has none (about half of trips), the line runs through the stops
  and is labelled approximate. Filling those from OSRM would mean hundreds of
  requests a night against a public demo server's fair-use terms, so it is not
  done.
