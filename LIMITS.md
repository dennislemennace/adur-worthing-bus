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
- ~750 instance-hours / month (one always-on free service fits comfortably).
- Sleeps after ~15 min idle → ~30 s cold start on the next request.
  The frontend handles cold starts; do not regress that behavior.
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
- GitHub Pages (likely the frontend host based on `ALLOWED_ORIGIN` in
  `render.yaml`): 100 GB / month soft bandwidth cap, 1 GB site size,
  10 builds / hour.
- `.github/workflows/update-timetable.yml` runs weekly — keep that cadence.
  Increasing the schedule eats into the (currently unlimited) public-repo
  budget needlessly.

## BODS — Bus Open Data Service (SIRI-VM vehicles)

- Free, requires `BODS_API_KEY` (Render env var).
- No published hard rate limit; "fair use" applies.
- Current usage: a single bounding-box poll of `/datafeed/` every
  `VEHICLE_REFRESH_MS` (default 20 s) per active browser tab.

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

## Cloudflare Workers + KV — community submission relay

(`worker/` — takes idea / proposal / stop-issue submissions and files them as
GitHub issues. Replaced the Web3Forms email relay.)

- Workers free plan: **100,000 requests / day**, 10 ms CPU per request.
- Workers KV free plan: **100,000 reads / day, 1,000 writes / day.**
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

## GitHub REST API — issue creation

- 5,000 authenticated requests / hour for the fine-grained PAT.
- One submission = 1 call, or 2 when stop-issue dedupe search runs.
- At the Worker's own 200/day cap this is ~400 calls/day against a 120,000/day
  allowance — not a constraint, but don't add per-submission API chatter
  (label lookups, project-board moves) without rechecking.

## OpenStreetMap — `tile.openstreetmap.org` (light theme)

- Tile usage policy: https://operations.osmfoundation.org/policies/tiles/
- No per-key limit, but expects moderate non-commercial use, correct
  attribution, and no hot-linking from very high-traffic sites.
- If traffic ever grows past hobbyist scale, move to a self-hosted /
  CDN-fronted tile source rather than hammering the OSMF tile servers.

## CARTO — `basemaps.cartocdn.com` (dark theme)

- Free for non-commercial use with attribution (preserved in `app.js`).
- Soft fair-use; no hard cap published.

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
