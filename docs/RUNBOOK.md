# Runbook

The short, current, authoritative description of how this thing runs.
`docs/ROADMAP.md` is kept as historical planning — it records what was
intended at various points and is not a description of what exists.

## What runs where

| Piece | Where | Notes |
|---|---|---|
| Static site | GitHub Pages, legacy branch build from `main` at `/` | `CNAME` claims `worthingbrightonbus.co.uk`; the `dennislemennace.github.io` URL 301s to it. |
| API | Render (free plan), `api/main.py` | Sleeps when idle; the first request after a sleep is slow. Health check is `/`. |
| Submissions | Cloudflare Worker, `worker/src/index.js` | Files public GitHub issues. Secrets set via `wrangler secret put`, never in the repo. |
| Timetable | SQLite, published as a GitHub release asset | The API downloads it at startup and re-checks hourly. |

## Routine operations

**Rebuild the timetable.** Run the *Update Timetable Data* workflow. It runs
the test suites first, builds, checks the result semantically
(`scripts/check_timetable.py`), commits refreshed evidence, publishes a dated
release, then moves the `timetable-latest` tag onto it.

**Roll back a bad timetable.** Dated releases are kept (five deep). Copy the
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
