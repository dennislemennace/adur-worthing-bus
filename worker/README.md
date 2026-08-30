# Submission relay (Cloudflare Worker)

Takes a submission from the site and files it as a GitHub issue. Replaces the
old Web3Forms email relay — submissions are now public and trackable, and the
sender gets a link to their own issue instead of nothing.

Handles three kinds on one endpoint (`POST /submit`): `idea`, `proposal`,
`stop_issue`.

**Nothing here publishes to the site.** Issues arrive labelled `unverified`; a
maintainer still runs `scripts/add_suggestion.py` to publish. That moderation
gate is deliberate — this is a public endpoint writing to a public tracker.

## First-time setup

You need a Cloudflare account (free) and the `wrangler` CLI (`npx wrangler`
works without installing anything globally).

### 1. Create the GitHub labels — do this first

**GitHub does not create labels on demand.** Posting an issue with a label that
doesn't exist fails the whole request with **422**, which the Worker surfaces as
a 502. The first real submission would look like a broken Worker.

These eleven are already created on `dennislemennace/adur-worthing-bus`. Run
this against any other repo, including the scratch repo you test with before
pointing `GITHUB_REPO` at the real one:

```sh
gh label create community-submission --color 1a4b82 --force \
  --description "Filed by the site's submission Worker, not by a maintainer"
gh label create unverified           --color 6b7280 --force \
  --description "Not yet reviewed. Nothing reaches the site until a maintainer publishes it"
gh label create idea                 --color c07808 --force \
  --description "A network idea from the Ideas form"
gh label create proposal             --color 0e7490 --force \
  --description "A route proposal from the map editor, carrying a JSON block"
gh label create stop-issue           --color b45309 --force \
  --description "A fault reported at a specific stop, keyed by ATCO code"
gh label create stop-issue:shelter        --color fed7aa --force --description "Shelter damaged or dirty"
gh label create stop-issue:timetable-case --color fde68a --force --description "Timetable missing or out of date"
gh label create stop-issue:rtpi-display   --color bfdbfe --force --description "Real-time display broken or wrong"
gh label create stop-issue:accessibility  --color ddd6fe --force --description "Kerb, ramp, or step-free access problem"
gh label create stop-issue:lighting       --color fef08a --force --description "Lighting out or inadequate"
gh label create stop-issue:other          --color e5e7eb --force --description "Anything else about this stop"
```

The set is derived from `buildIssue` and `STOP_ISSUE_CATEGORIES` in
`src/index.js` — if you add a stop-issue category there, add its label here too.

If a label is renamed or deleted later the Worker no longer breaks: a 422 on
issue creation is retried once without labels, so the submission still lands and
only the labelling is lost. Don't rely on that — it's a safety net, not a plan.

### 2. Log wrangler in

```sh
cd worker && npx wrangler login
```

### 3. Create the KV namespace for rate-limit counters

```sh
cd worker
npx wrangler kv namespace create RATE_LIMIT
```

(`kv namespace` with a space needs Wrangler 3.60.0 or later; older
versions use the deprecated `kv:namespace` form.)

Paste the returned id into `wrangler.toml` under `[[kv_namespaces]]`.

### 4. Deploy once, before touching secrets

**Order matters here and it is not obvious.** `wrangler secret put` fails with
*"Worker not found"* until the Worker exists, so the first deploy has to come
first. This is safe: with no `TURNSTILE_SECRET` the Worker **fails closed** and
accepts nothing, and with no `GITHUB_TOKEN` it cannot file anything either.

```sh
npx wrangler deploy
```

### 5. Register a workers.dev subdomain

The first deploy warns *"You need to register a workers.dev subdomain"* and then
prints a URL anyway. **That URL will not work** — DNS resolves but TLS fails,
because Cloudflare has no certificate for an unregistered subdomain. The
symptom is `curl: (35) TLS connect error`, which looks nothing like the actual
cause.

Register one at **Workers & Pages → your account → Subdomain** in the
dashboard. Picking `yourname` gives you:

```
https://adur-worthing-submissions.yourname.workers.dev
```

That is the URL for `CONFIG.SUBMIT_ENDPOINT` in step 9.

### 6. Create the GitHub token

Use a **fine-grained** personal access token, not a classic one:

- GitHub → Settings → Developer settings → Personal access tokens → Fine-grained
- **Repository access:** only `dennislemennace/adur-worthing-bus`
- **Permissions:** `Issues: Read and write`. Nothing else — not Contents, not
  Workflows, not Metadata beyond what GitHub forces.
- Set an expiry you'll actually notice, and diary the rotation.

> A **GitHub App** installation token is the upgrade path if this outgrows a
> PAT: tokens rotate automatically and there's no expiry cliff. Worth doing if
> the endpoint ever handles more than hobby volume.

### 7. Create the Turnstile keys

Cloudflare dashboard → Turnstile → Add site. Use the **Managed** widget.
Keep the *site key* (public, goes in `app.js`) and the *secret key* (goes in
`wrangler secret`).

### 8. Push the secrets

```sh
npx wrangler secret put GITHUB_TOKEN       # the fine-grained PAT
npx wrangler secret put TURNSTILE_SECRET   # Turnstile secret key
npx wrangler secret put IP_SALT            # any long random string
```

`IP_SALT` salts the hashed client IP used for rate limiting, so the KV store
never holds a raw address. Generate one with `openssl rand -hex 32`.

### 9. Redeploy and wire up the frontend

Secrets take effect immediately, but redeploy after any `wrangler.toml`
change:

```sh
npx wrangler deploy
```

Then put the step-5 URL into `CONFIG.SUBMIT_ENDPOINT` in `app.js` — replacing
the `YOUR-WORKER` sentinel, which is what makes the forms say "not switched on
yet" — and the Turnstile **site key** into `CONFIG.TURNSTILE_SITE_KEY`.

## Local development

```sh
npx wrangler dev
```

With no `RATE_LIMIT` binding and an empty `ALLOWED_ORIGINS`, the Worker skips
rate limiting and accepts any origin — convenient locally, which is exactly why
`ALLOWED_ORIGINS` must be set in production.

Turnstile has no such escape hatch: an unset `TURNSTILE_SECRET` **fails closed**.
For local end-to-end testing use Cloudflare's always-passing test keys
(site `1x00000000000000000000AA`, secret `1x0000000000000000000000000000000AA`).

**Test against a scratch repo before pointing `GITHUB_REPO` at the real one.**

## Tests

```sh
cd worker && npm test
```

No dependencies — Node's built-in runner. Covers the sanitizer and the issue
builders, which is where the security-relevant logic lives.

## Request shape

```jsonc
POST /submit
{
  "kind": "idea",              // idea | proposal | stop_issue
  "turnstileToken": "…",       // from the Turnstile widget
  "botcheck": false,           // honeypot; truthy = silently dropped

  // idea
  "title": "…", "details": "…", "area": "…", "objective": "…", "name": "…",

  // proposal
  "proposalJson": "{…}",

  // stop_issue
  "stopName": "…", "atco": "…", "category": "shelter", "lat": 50.8, "lon": -0.37
}
```

Responses: `200 {ok, number, url}` · `400` validation/Turnstile ·
`403` origin · `413` too large · `429` client rate limit ·
`503` global daily cap · `502` GitHub error.

## Defences, in the order they run

| Check | Why |
|---|---|
| Origin allowlist | Stops the endpoint being driven from anywhere but the site |
| 8 KB body cap | Applied before parsing |
| Schema validation | The form has `novalidate`; this is the first real enforcement |
| Honeypot | Returns a fake success so the bot learns nothing |
| Turnstile | Fails closed if unconfigured |
| KV rate limits | 5/hour and 20/day per client, plus a 200/day global cap |
| Sanitizer | Defuses `@mentions`, breaks code fences, strips headings and HTML |

The sanitizer's `@mention` handling is the non-obvious one: an issue body is a
broadcast, so without it a single submission could notify an entire org.

## Free-tier caps

Cloudflare Workers free: 100,000 requests/day, 10 ms CPU per request. KV free:
100,000 reads and 1,000 writes/day — each submission costs 3 reads and 3 writes,
so the KV write budget caps out around 300 submissions/day, well above the
200/day global limit the Worker enforces itself. GitHub's REST API allows 5,000
authenticated requests/hour; a submission costs 1 call, or 2 when stop-issue
dedupe runs.

See `../LIMITS.md` for how this fits the rest of the free-tier envelope.
