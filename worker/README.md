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

### 1. Create the KV namespace for rate-limit counters

```sh
cd worker
npx wrangler kv namespace create RATE_LIMIT
```

Paste the returned id into `wrangler.toml` under `[[kv_namespaces]]`.

### 2. Create the GitHub token

Use a **fine-grained** personal access token, not a classic one:

- GitHub → Settings → Developer settings → Personal access tokens → Fine-grained
- **Repository access:** only `dennislemennace/adur-worthing-bus`
- **Permissions:** `Issues: Read and write`. Nothing else — not Contents, not
  Workflows, not Metadata beyond what GitHub forces.
- Set an expiry you'll actually notice, and diary the rotation.

> A **GitHub App** installation token is the upgrade path if this outgrows a
> PAT: tokens rotate automatically and there's no expiry cliff. Worth doing if
> the endpoint ever handles more than hobby volume.

### 3. Create the Turnstile keys

Cloudflare dashboard → Turnstile → Add site. Use the **Managed** widget.
Keep the *site key* (public, goes in `app.js`) and the *secret key* (goes in
`wrangler secret`).

### 4. Push the secrets

```sh
npx wrangler secret put GITHUB_TOKEN       # the fine-grained PAT
npx wrangler secret put TURNSTILE_SECRET   # Turnstile secret key
npx wrangler secret put IP_SALT            # any long random string
```

`IP_SALT` salts the hashed client IP used for rate limiting, so the KV store
never holds a raw address. Generate one with `openssl rand -hex 32`.

### 5. Deploy

```sh
npx wrangler deploy
```

Then put the deployed URL into `CONFIG.SUBMIT_ENDPOINT` in `app.js`, and the
Turnstile **site key** into `CONFIG.TURNSTILE_SITE_KEY`.

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
