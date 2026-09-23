# Runbook

The short, current, authoritative description of how this thing runs.
`docs/ROADMAP.md` is kept as historical planning — it records what was
intended at various points and is not a description of what exists.

## What runs where

| Piece | Where | Notes |
|---|---|---|
| Static site | GitHub Pages, legacy branch build from `main` at `/` | `CNAME` claims `worthingbrightonbus.co.uk`; the `dennislemennace.github.io` URL 301s to it. |
| API | Render (free plan), `api/main.py` | Sleeps when idle; the first request after a sleep is slow. Health check is `/`. |
| Submissions | Cloudflare Worker, `worker/src/index.js` | Files issues in the private `adur-worthing-bus-inbox` repo; nothing is public until published with `scripts/add_*.py`. Secrets set via `wrangler secret put`, never in the repo. |
| Timetable | SQLite, published as a GitHub release asset | The API downloads it at startup and re-checks hourly. |

## Routine operations

**Rebuild the timetable.** Run the *Update Timetable Data* workflow. It runs
the test suites first, builds, checks the result semantically
(`scripts/check_timetable.py`), commits refreshed evidence, publishes a dated
release, then moves the `timetable-latest` tag onto it.

**Rebuild the static stop list.** `python scripts/build_stops_json.py` writes
`data/stops.json` from `data/timetable.sqlite`. The *Update Timetable Data*
workflow already runs it and commits the result, so this is only for when you
have rebuilt the database by hand. It refuses to write a list of fewer than 800
stops — a short file is worse than none, because the frontend falls back to the
API only when the *fetch* fails, and a successful download of a near-empty list
would give every visitor a blank map.

**If cold starts come back.** The Cloudflare Worker's cron trigger pings the
API every 10 minutes between 07:30 and 23:30 Europe/London. Outside those hours
a cold start is expected, not a fault. Inside them, check the Worker first: in
the Cloudflare dashboard, Workers & Pages → `adur-worthing-submissions` →
Settings → Triggers should list `*/10 6-23 * * *`, and its Logs should show a
`keep-warm HH:MM: HTTP 200` line every 10 minutes. `npx wrangler tail` from
`worker/` shows the same live. The GitHub `keep-warm.yml` workflow is only a
backup; GitHub runs it every few hours, so its run history proves nothing.
The arithmetic behind the window is in `LIMITS.md`: the service is up about
500 hours a month against a 750-hour allowance, so widening the window is not
free.

**Roll back a bad timetable.** Dated releases are retained for historical measurement and replay (no five-release pruning). Copy the
assets from a good one onto the rolling tag:

```sh
gh release download timetable-2026-09-01-0300 -D /tmp/rollback
gh release upload timetable-latest /tmp/rollback/* --clobber
```

Then redeploy the API, or wait an hour for its own refresh. The API verifies a
download before replacing its working copy (`db_is_usable`), so a corrupt
asset will be refused rather than installed.

**Publish a community submission.** `scripts/add_proposal.py --from-issue N`
or `scripts/add_suggestion.py --from-issue N`. Both refuse geometry they
cannot place on this coast, write the category themselves rather than reading
it from the submission, close the issue and clear its `unverified` label.
Both say "approved for publication", not "published": committing and
deploying is still a separate step you have to take.

**Rebuild the councillor data.** `python scripts/build_representatives.py`.
It refuses to write if coverage falls more than 10% below the existing file;
`--force` if the loss is genuine, which it will be after boundary changes.

**Rebuild the brand mark.** `python scripts/prepare_brand_mark.py` regenerates
every size, the favicons and the share-preview image from
`brand/mark-source.png`. Deterministic — re-running it produces byte-identical
files.

## Tools that change release assets

Do not run these as a routine check. They rewrite committed artefacts and
their diffs need reading.

| Script | Needs | Writes |
|---|---|---|
| `scripts/build_timetable.py` | network (BODS, ~hundreds of MB; OSRM unless `SKIP_OSRM=1`) | `data/timetable.json` |
| `scripts/json_to_sqlite.py` | `data/timetable.json` | `data/timetable.sqlite` |
| `scripts/build_evidence.py` | `data/timetable.sqlite` | `data/boundary_evidence.json` |
| `scripts/build_representatives.py` | network (council directories, ONS) | `data/representatives.json` |
| `scripts/build_icons.py`, `scripts/prepare_bus_icon.py`, `scripts/prepare_brand_mark.py` | Pillow (`requirements-dev.txt`) | `icons/`, `brand/`, favicons |
| `scripts/migrate_proposals_to_atco.py` | — | `data/proposals.json` — one-time migration, already run |

## Tests

```sh
.venv/bin/python -m pytest -q
node --test --test-isolation=none "tests/*.mjs" "worker/test/*.js"
SITE_URL='http://127.0.0.1:8765/' node scripts/browser_check.mjs
```

The browser check needs Chrome on `--remote-debugging-port=9222` and
`scripts/dev_server.py` running. It is timing-sensitive across five viewports;
a lone failure is worth re-running before believing.

## Latest verified timetable release

On 8 September 2026, workflow run `34241330364` published
`timetable-2026-09-08-1458` and updated `timetable-latest`. The downloaded
database passes checksum and semantic validation: 14,098 stop times retain
their service-day offset past midnight, with no decreasing trip sequences.
The live API reports the new 5,374 stops, 188 routes and 35,433 trips; the
public evidence hash matches the release. See `docs/REVIEW_FOLLOWUP.md` for
the verification record and scoped commits.

## Known open items

- The deployed timetable is rebuilt, but the old local `data/timetable.sqlite`
  has not been replaced underneath the running API on port 8011. Stop that
  local API before replacing its database with the verified release copy at
  `/tmp/adur-timetable-published-20260908/timetable.sqlite`. The preview on
  port 8765 proxies the deployed API and already uses the rebuilt timetable.
- GitHub Pages domain verification (`_github-pages-challenge-*` TXT record) has
  not been added.
- Two update photographs have no recorded provenance — see
  `docs/ASSET_RIGHTS.md`.
- The project has no `LICENSE` file while describing itself as open source.

## Reliability publication and replay (23 September implementation)

This workflow is implemented locally; deployment is not established by this
runbook. See [implementation status](IMPLEMENTATION_DATA_UI_2026-09-22.md) for
validation and the current timetable cohort blocker.

Deploy the compatible Worker routes/recorder, API and frontend before enabling
new processing. The Worker must serve `journey-times/builds/<sha256>/...` as
well as the root index. Leave `JOURNEY_TIMES_PUBLIC` false until recording
coverage, independent timing checks and replay have been verified.

The nightly *Process Snapshots* workflow now:

1. Selects a checksum-verified timetable describing the day and every relevant
   local operator/service cohort, searching all pages of archived releases.
   A global date range is insufficient. The local 21 September build cannot
   validate 23 September for BHBC 25X; do not bypass this as a routine fix.
2. Downloads SIRI and RT independently, accepting either feed on its own.
   The combined object floor is a download sanity check, not a coverage score.
3. Generates method-4 observations and a daily summary; restores every known
   input for the rolling 35-day view and the requested calendar month. Failed
   downloads, altered hashes and wrong-day files stop the run. Old-month reruns
   do not move or widen the latest rolling window.
4. Creates a replay archive of exact recorded objects, timetable, code,
   configuration and runtime-version evidence. Validates all candidate
   derivatives before any release/publication upload.
5. Publishes a unique `reliability-run-<run-id>-<attempt>` release containing
   replay evidence, observations and the complete candidate archive. Assets
   are not overwritten. The historical source catalog remains in the index.
6. Enforces the published bucket's separate 4 GiB budget, uploads the immutable
   generation, reads back and hashes every object, then switches the single
   public `journey-times/index.json` pointer. Old generations remain available.

The manifest is authoritative for observation, summary and rollup versions;
there is no daily Git commit of refreshed summaries. Known legacy inputs are
migrated to unique release assets when restored; that freezes their bytes but
cannot supply missing raw evidence or upgrade their method.

### Local preparation

Use an isolated output directory and the original historical timetable. This
example writes only local files; replace the example date with the input day.
`raw/<day>/` and `rt/<day>/` contain the original recorded objects.

```sh
DAY=2026-09-22
python scripts/timetable_covers.py --timetable replay/timetable.sqlite --day "$DAY"
python scripts/process_snapshots.py --day "$DAY" \
  --snapshots "replay/raw/$DAY" --gtfs-rt "replay/rt/$DAY" \
  --timetable replay/timetable.sqlite --out observations.json \
  --summary-out summary.json
python scripts/archive_reliability_evidence.py --observations observations.json \
  --snapshots replay/raw --rt replay/rt --timetable replay/timetable.sqlite \
  --out evidence.tar.gz
python scripts/build_journey_times.py --observations observations.json \
  --out candidate-journeys --timetable replay/timetable.sqlite
python scripts/build_delay_hotspots.py --observations observations.json \
  --out hotspot-preview.json
python scripts/check_published.py --journey-times candidate-journeys \
  --summaries summary.json
```

Use the workflow's `prepare_reliability_inputs.py` and `publication_bundle.py`
steps for a complete multi-day candidate. A single-day local check does not
validate replacement of the rolling public view.

### Replay and rollback

Download the source catalog's day-specific replay archive and observation
asset; verify their SHA-256 values against the immutable index before use.
Extract into an isolated directory. `manifest.json` hashes every archived raw
object, code file and timetable; verify all of them. `provenance.json` records
configuration and runtime/package versions. Run the archived code with those
inputs and settings in an isolated environment, and compare observation
identities, epochs, quality flags, intervals and downstream statistics.
Generation timestamps may differ; do not treat timestamp-only differences as
changed measurements. Legacy days without archives cannot pass this replay.

For rollback, obtain the current immutable index's `rollback_index`, verify
that its referenced generation and objects are available and their hashes
match, then replace only the root `journey-times/index.json` with that previous
index using the existing publication credentials. No service file needs an
in-place overwrite. A failed upload/read-back leaves the old pointer active;
its unused release/generation is not proof of successful publication. Check
both fresh and build-pinned links after rollback. These remote recovery steps
have not yet been exercised by this implementation.
