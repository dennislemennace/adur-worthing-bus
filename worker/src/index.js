/**
 * Community submission relay — Adur & Worthing Live Bus Tracker
 *
 * Accepts a JSON submission from the site (no account needed by the sender)
 * and files it as a GitHub issue in the project repo. Replaces the old
 * Web3Forms email relay: submissions are now public, trackable, and carry a
 * link the sender can follow, instead of landing in a private inbox.
 *
 * Nothing here publishes to the site. Issues are filed with an `unverified`
 * label and a maintainer still runs scripts/add_suggestion.py to publish —
 * the moderation gate that existed before is deliberately preserved, because
 * a public endpoint that writes to a public tracker is a spam target.
 *
 * Three submission kinds share this one endpoint:
 *   idea       — Network Objectives → Ideas form
 *   proposal   — the in-app route proposal editor
 *   stop_issue — "Report an issue" on a stop's departure board
 *
 * Secrets (wrangler secret put):
 *   GITHUB_TOKEN     fine-grained PAT, THIS REPO ONLY, Issues: Read & Write
 *   TURNSTILE_SECRET Cloudflare Turnstile secret key
 *   IP_SALT          random string; salts the hashed IP used for rate limits
 *
 * Bindings (wrangler.toml):
 *   RATE_LIMIT       KV namespace for the rate-limit counters
 */

// ── Tunables ────────────────────────────────────────────────
const MAX_BODY_BYTES = 8 * 1024;   // proposal drafts are the large case
const PER_HOUR_LIMIT = 5;          // per client
const PER_DAY_LIMIT  = 20;         // per client
const GLOBAL_DAY_LIMIT = 200;      // bounds the damage from a distributed flood
const GITHUB_API = "https://api.github.com";
const USER_AGENT = "adur-worthing-bus-submissions";

// Field caps mirror the form's maxlength attributes. The form carries
// `novalidate` and validates only in JS, so this is the first place these
// limits are actually enforced rather than merely suggested.
const LIMITS = {
  title:   120,
  details: 1000,
  name:    60,
  area:    40,
  objective: 120,
  category: 40,
  stopName: 120,
  atco:    40,
  json:    6000,   // proposal payload
};

const STOP_ISSUE_CATEGORIES = new Set([
  "shelter", "timetable-case", "rtpi-display", "accessibility", "lighting", "other",
]);

// What can be wrong with the bus itself, as distinct from the stop it is
// standing at. `accessibility` and `onboard-info` are the two the Public
// Service Vehicles (Accessible Information) Regulations 2023 bear on — a
// resident's report of a missing next-stop announcement is evidence about
// compliance, and there is nowhere else on this site to put it.
const BUS_ISSUE_CATEGORIES = new Set([
  "full", "cancelled", "accessibility", "onboard-info", "late", "other",
]);

export default {
  async fetch(request, env, ctx) {
    const origin = request.headers.get("Origin") || "";
    const allowed = allowedOrigins(env);

    if (request.method === "OPTIONS") {
      return preflight(origin, allowed);
    }
    if (request.method !== "POST") {
      return json({ ok: false, error: "Method not allowed" }, 405, origin, allowed);
    }
    if (!isAllowedOrigin(origin, allowed, env)) {
      // Not a CORS nicety — this is the check that stops the endpoint being
      // driven from anywhere but the site itself.
      return json({ ok: false, error: "Forbidden" }, 403, origin, allowed);
    }

    const url = new URL(request.url);
    if (url.pathname !== "/submit") {
      return json({ ok: false, error: "Not found" }, 404, origin, allowed);
    }

    // ── Body size cap, before we parse anything ───────────────
    const raw = await readCapped(request, MAX_BODY_BYTES);
    if (raw === null) {
      return json({ ok: false, error: "Submission too large" }, 413, origin, allowed);
    }

    let payload;
    try {
      payload = JSON.parse(raw);
    } catch {
      return json({ ok: false, error: "Malformed submission" }, 400, origin, allowed);
    }
    // `null`, `42` and `[1,2]` are all valid JSON. Only an object has the
    // fields everything below reads, and `null.botcheck` threw the whole
    // request rather than answering the sender.
    if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
      return json({ ok: false, error: "Malformed submission" }, 400, origin, allowed);
    }

    // ── Honeypot: pretend it worked, do nothing ───────────────
    // Mirrors the existing client-side behaviour and tells the bot nothing.
    if (payload.botcheck) {
      return json({ ok: true, skipped: true }, 200, origin, allowed);
    }

    // ── Validate per kind ─────────────────────────────────────
    let built;
    try {
      built = buildIssue(payload);
    } catch (err) {
      return json({ ok: false, error: err.message }, 400, origin, allowed);
    }

    // ── Turnstile ─────────────────────────────────────────────
    const turnstileOk = await verifyTurnstile(
      env.TURNSTILE_SECRET,
      payload.turnstileToken,
      request.headers.get("CF-Connecting-IP"),
    );
    if (!turnstileOk) {
      return json(
        { ok: false, error: "Could not verify you're human — please try again." },
        400, origin, allowed,
      );
    }

    // ── Rate limits ───────────────────────────────────────────
    // Inside the error boundary. This sat above it, so a KV write failure
    // rejected the whole fetch and the sender lost their submission with no
    // usable answer — and a KV write failure is not exotic: Cloudflare allows
    // one write per second to the same key.
    let verdict;
    try {
      verdict = await checkRateLimit(env, request);
    } catch (err) {
      console.error("Rate limit check failed:", err && err.message);
      return json(
        { ok: false, error: "We couldn't check your submission just now — please try again shortly." },
        503, origin, allowed,
      );
    }
    if (verdict === "client") {
      return json(
        { ok: false, error: "You've sent a few already — please try again later." },
        429, origin, allowed,
      );
    }
    if (verdict === "global") {
      return json(
        { ok: false, error: "We're receiving an unusual number of submissions — please try later." },
        503, origin, allowed,
      );
    }

    // ── File it ───────────────────────────────────────────────
    try {
      const result = await fileIssue(env, built);
      return json({ ok: true, number: result.number, url: result.html_url }, 200, origin, allowed);
    } catch (err) {
      // Real reason to Worker logs; the sender gets something generic.
      console.error("GitHub issue creation failed:", err && err.message);
      return json(
        { ok: false, error: "Couldn't file your submission — please try again shortly." },
        502, origin, allowed,
      );
    }
  },
};

// ============================================================
// VALIDATION + ISSUE CONSTRUCTION
// ============================================================

/** Build {title, body, labels, dedupeKey} for a submission, or throw. */
function buildIssue(p) {
  const kind = String(p.kind || "");
  switch (kind) {
    case "idea":       return buildIdea(p);
    case "proposal":   return buildProposal(p);
    case "stop_issue": return buildStopIssue(p);
    case "bus_issue":  return buildBusIssue(p);
    case "news":       return buildNews(p);
    default:           throw new Error("Unknown submission type");
  }
}

function buildIdea(p) {
  const title   = required(p.title, "title", LIMITS.title, "a one-line summary");
  const details = required(p.details, "details", LIMITS.details, "a few more details");
  const area      = optional(p.area, LIMITS.area);
  const objective = optional(p.objective, LIMITS.objective);
  const name      = optional(p.name, LIMITS.name);

  // suggestion_json is what scripts/add_suggestion.py --from-issue reads.
  // Built server-side rather than trusting a client-supplied blob.
  const publish = {
    id:     `${slugify(title)}-${shortId()}`,
    title,
    body:   details,
    area:   area || "Other",
    name:   name || "",
    date:   today(),
    status: "published",
  };

  const body = [
    "**Community idea** submitted from the site's Ideas form.",
    "",
    `**What it's about:** ${area || "—"}`,
    objective ? `**Related objective:** ${objective}` : null,
    `**From:** ${name || "anonymous"}`,
    "",
    "### The idea",
    "",
    blockquote(details),
    "",
    "---",
    "",
    "<details><summary>Ready-to-publish JSON (for <code>scripts/add_suggestion.py</code>)</summary>",
    "",
    "```json",
    JSON.stringify(publish, null, 2),
    "```",
    "",
    "</details>",
    "",
    autoNote(),
  // Drop only the conditional (null) rows — empty strings are deliberate
  // blank lines, and markdown needs them to separate block elements.
  ].filter(line => line !== null).join("\n");

  return {
    title: `Idea: ${title}`,
    body,
    labels: ["community-submission", "idea", "unverified"],
  };
}

// The coast this site covers. Mirrors LAT_RANGE/LON_RANGE in
// scripts/add_proposal.py — a point outside it is almost always a
// transposed [lon, lat] pair, which parses perfectly and draws a line into
// the Indian Ocean.
const PROPOSAL_LAT_RANGE = [50.5, 51.2];
const PROPOSAL_LON_RANGE = [-1.2, 0.4];

function assertPublishableProposal(obj) {
  if (obj === null || typeof obj !== "object" || Array.isArray(obj)) {
    throw new Error("Proposal data must be an object");
  }
  if (!String(obj.name || "").trim()) {
    throw new Error("The proposal needs a name");
  }
  if (!String(obj.summary || "").trim()) {
    throw new Error("The proposal needs a one-line summary");
  }
  const line = obj.polyline;
  if (!Array.isArray(line) || line.length < 2) {
    throw new Error("The proposal needs a route line with at least two points");
  }
  for (const point of line) {
    if (!Array.isArray(point) || point.length < 2
        || typeof point[0] !== "number" || typeof point[1] !== "number") {
      throw new Error("Every route point must be a [latitude, longitude] pair");
    }
    const [lat, lon] = point;
    if (lat < PROPOSAL_LAT_RANGE[0] || lat > PROPOSAL_LAT_RANGE[1]
        || lon < PROPOSAL_LON_RANGE[0] || lon > PROPOSAL_LON_RANGE[1]) {
      const swapped = lat >= PROPOSAL_LON_RANGE[0] && lat <= PROPOSAL_LON_RANGE[1]
                   && lon >= PROPOSAL_LAT_RANGE[0] && lon <= PROPOSAL_LAT_RANGE[1];
      throw new Error(
        `Route point [${lat}, ${lon}] is not in this area`
        + (swapped ? " — that reads as [lon, lat]; this format is [lat, lon]" : ""));
    }
  }
  if (obj.color != null && !/^#[0-9a-fA-F]{6}$/.test(String(obj.color))) {
    throw new Error("Colour must be a #rrggbb value");
  }
}

function buildProposal(p) {
  const title = required(p.title, "title", LIMITS.title, "a name for the proposal");
  const name  = optional(p.name, LIMITS.name);
  const blob  = String(p.proposalJson == null ? "" : p.proposalJson);
  if (!blob.trim()) throw new Error("Proposal data is missing");
  if (blob.length > LIMITS.json) throw new Error("Proposal is too large to submit");

  // Must be valid JSON — a proposal that won't parse is useless to a
  // maintainer and we'd rather reject it here than file a broken issue.
  let parsed;
  try {
    parsed = JSON.parse(blob);
  } catch {
    throw new Error("Proposal data was not valid JSON");
  }
  // ...and must be a proposal the moderation script could actually publish.
  // These two ends enforced different contracts, so a submission could be
  // accepted onto the public tracker and then be unpublishable — which wastes
  // the submitter's effort and leaves a maintainer to explain why. The rules
  // here are the subset scripts/add_proposal.py refuses on; the script stays
  // the authority, and keeps its own checks.
  assertPublishableProposal(parsed);

  const body = [
    "**Route proposal** submitted from the in-app proposal editor.",
    "",
    `**From:** ${name || "anonymous"}`,
    "",
    "To publish: paste the JSON below into `data/proposals.json` and open a PR.",
    "",
    "```json",
    JSON.stringify(parsed, null, 2),
    "```",
    "",
    autoNote(),
  ].join("\n");

  return {
    title: `Proposal: ${title}`,
    body,
    labels: ["community-submission", "proposal", "unverified"],
  };
}

/** A report about a bus, not about a stop.
 *
 * Deliberately not folded into buildStopIssue. A stop fault belongs to the
 * council and stays put; a bus fault belongs to the operator and moves, so it
 * dedupes by vehicle and service rather than by ATCO, and it says something
 * different about who can act on it.
 */
/** A short article submitted by a reader, for Community News.
 *
 * Built server-side into the same ````json` shape `add_update.py` reads, so
 * publishing is `--from-issue N` like every other submission rather than a
 * copy-and-paste. The blob is composed here from validated fields, never
 * taken from the browser: `status` and `date` are the site's to set, and a
 * submission that could set its own would be publishing itself.
 */
function buildNews(p) {
  const title   = required(p.title, "title", LIMITS.title, "a headline");
  const summary = required(p.summary, "summary", LIMITS.title, "a one-line summary");
  const details = required(p.details, "details", LIMITS.details, "the article itself");
  const name    = optional(p.name, LIMITS.name);
  const topic   = optional(p.topic, LIMITS.area);
  const link    = optional(p.sourceUrl, LIMITS.objective);

  // A source link is optional, but one that is not http(s) is not a source.
  let sourceUrl = "";
  if (link) {
    if (!/^https?:\/\//i.test(link)) throw new Error("A source link must start with http:// or https://");
    sourceUrl = link;
  }

  const publish = {
    title,
    summary,
    body: details,
    topic: topic || "Community",
    name: name || "",
  };
  if (sourceUrl) publish.links = [{ label: "Source", url: sourceUrl }];

  const body = [
    "**Community news** submitted from the Updates tab.",
    "",
    `**From:** ${name || "anonymous"}`,
    topic ? `**Topic:** ${topic}` : null,
    sourceUrl ? `**Source:** ${sourceUrl}` : null,
    "",
    "### Summary",
    "",
    blockquote(summary),
    "",
    "### Article",
    "",
    blockquote(details),
    "",
    "---",
    "",
    "<details><summary>Ready to publish</summary>",
    "",
    "```json",
    JSON.stringify(publish, null, 2),
    "```",
    "",
    "</details>",
    "",
    "To publish: `python scripts/add_update.py --from-issue <this issue>`",
    "",
    "> Nothing here has been checked. A community article is somebody's account",
    "> of something, and it is published under this site's name — read it, check",
    "> what it asserts, and edit before publishing.",
    "",
    autoNote(),
  ].filter(line => line !== null).join("\n");

  return {
    title: `News: ${title}`,
    body,
    labels: ["community-submission", "news", "unverified"],
  };
}

function buildBusIssue(p) {
  const service = required(p.service, "service", LIMITS.stopName, "a service");
  const details = required(p.details, "details", LIMITS.details,
                           "a description of the problem");
  const name     = optional(p.name, LIMITS.name);
  const operator = optional(p.operator, LIMITS.atco);
  const vehicle  = optional(p.vehicleRef, LIMITS.atco);

  const category = String(p.category || "other");
  if (!BUS_ISSUE_CATEGORIES.has(category)) throw new Error("Unknown issue category");

  // `when` is the reporter's own clock, and it is the field that makes a
  // report checkable against the timetable later. Kept only if it parses.
  const when = String(p.when || "").trim();
  const whenOk = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(when) ? when : "";

  const lat = num(p.lat), lon = num(p.lon);
  const hasGeo = lat !== null && lon !== null;

  const accessible = category === "accessibility" || category === "onboard-info";

  const body = [
    "**Bus issue** reported from the live bus panel.",
    "",
    `**Service:** ${service}`,
    operator ? `**Operator:** \`${operator}\`` : null,
    vehicle  ? `**Vehicle:** \`${vehicle}\`` : null,
    `**Category:** ${category}`,
    whenOk ? `**Reported for:** ${whenOk}` : null,
    hasGeo ? `**Seen near:** ${lat.toFixed(6)}, ${lon.toFixed(6)}` : null,
    hasGeo
      ? `**Map:** [OpenStreetMap](https://www.openstreetmap.org/?mlat=${lat}&mlon=${lon}#map=17/${lat}/${lon})`
      : null,
    `**From:** ${name || "anonymous"}`,
    "",
    "### Reported problem",
    "",
    blockquote(details),
    "",
    "---",
    "",
    "> A bus that is full, cancelled or inaccessible is the operator's to answer",
    "> for, not the council's. This issue records the report so it can be counted",
    "> and quoted; it is not a complaint lodged with the operator, and it does not",
    "> reach the driver.",
    accessible ? "" : null,
    accessible
      ? "> Reports of missing or broken audible and visible next-stop information"
      : null,
    accessible
      ? "> are evidence about compliance with the Public Service Vehicles"
      : null,
    accessible
      ? "> (Accessible Information) Regulations 2023, whose final deadline is"
      : null,
    accessible
      ? "> 1 October 2026. Some vehicles and services are exempt — see the DfT's"
      : null,
    accessible
      ? "> published exemptions before treating any one bus as non-compliant."
      : null,
    "",
    autoNote(),
  ].filter(line => line !== null).join("\n");

  // One thread per vehicle where we know it, per service otherwise: repeat
  // reports about the same bus belong together, and "the 700 is always full"
  // is a pattern worth seeing in one place.
  const dedupeKey = vehicle ? `bus:${operator || "?"}:${vehicle}` : `svc:${service}`;

  return {
    title: vehicle
      ? `Bus issue: ${service} (vehicle ${vehicle})`
      : `Bus issue: ${service}`,
    body,
    labels: ["community-submission", "bus-issue", `bus-issue:${category}`, "unverified"],
    dedupeKey,
  };
}

function buildStopIssue(p) {
  const stopName = required(p.stopName, "stopName", LIMITS.stopName, "a stop");
  const atco     = required(p.atco, "atco", LIMITS.atco, "a stop");
  const details  = required(p.details, "details", LIMITS.details, "a description of the problem");
  const name     = optional(p.name, LIMITS.name);

  const category = String(p.category || "other");
  if (!STOP_ISSUE_CATEGORIES.has(category)) throw new Error("Unknown issue category");

  const lat = num(p.lat), lon = num(p.lon);
  const hasGeo = lat !== null && lon !== null;

  const body = [
    "**Stop issue** reported from the live departure board.",
    "",
    `**Stop:** ${stopName}`,
    `**ATCO:** \`${atco}\``,
    `**Category:** ${category}`,
    hasGeo ? `**Location:** ${lat.toFixed(6)}, ${lon.toFixed(6)}` : null,
    hasGeo
      ? `**Map:** [OpenStreetMap](https://www.openstreetmap.org/?mlat=${lat}&mlon=${lon}#map=19/${lat}/${lon})`
      : null,
    `**From:** ${name || "anonymous"}`,
    "",
    "### Reported problem",
    "",
    blockquote(details),
    "",
    "---",
    "",
    "> Stop infrastructure (shelters, timetable cases, RTPI displays) is usually the",
    "> responsibility of West Sussex County Council or Adur & Worthing Councils rather",
    "> than the bus operator. This issue records the report; it does not raise a works",
    "> order with the authority.",
    "",
    autoNote(),
  // Drop only the conditional (null) rows — empty strings are deliberate
  // blank lines, and markdown needs them to separate block elements.
  ].filter(line => line !== null).join("\n");

  return {
    title: `Stop issue: ${stopName} (${atco})`,
    body,
    labels: ["community-submission", "stop-issue", `stop-issue:${category}`, "unverified"],
    // Used to fold repeat reports about one stop into a single thread.
    dedupeKey: atco,
  };
}

function autoNote() {
  return "_Filed automatically from the site. Nothing is published to the site until a maintainer reviews it._";
}

// ── Field helpers ───────────────────────────────────────────

function required(value, field, max, humanName) {
  const s = sanitize(value, max);
  if (!s) throw new Error(`Please include ${humanName}.`);
  return s;
}

function optional(value, max) {
  return sanitize(value, max);
}

function num(v) {
  const n = typeof v === "number" ? v : parseFloat(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * Clean a user string that is about to land in a public GitHub issue.
 *
 * Three distinct jobs:
 *  1. strip control characters and normalise whitespace
 *  2. defuse @mentions — an issue body is a broadcast, and without this a
 *     submission could ping every member of an org
 *  3. neutralise markdown structure so a submission can't forge headings,
 *     fenced blocks or HTML that impersonate maintainer text
 */
function sanitize(value, max) {
  if (value == null) return "";
  const ZWSP = "\u200b";
  let s = String(value);
  // Strip C0/C1 control characters, but keep \t, \n and \r.
  s = s.replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, "");
  s = s.replace(/\r\n?/g, "\n");
  s = s.replace(/\n{3,}/g, "\n\n");
  // @mention / #issue-reference defusing: a zero-width space after the sigil
  // reads identically but doesn't resolve to a notification or a backlink.
  // Without this, one submission could ping every member of an org.
  s = s.replace(/@(?=[A-Za-z0-9-])/g, "@" + ZWSP);
  s = s.replace(/#(?=\d)/g, "#" + ZWSP);
  // Markdown structure the sender shouldn't control.
  s = s.replace(/^[ \t]*#{1,6}[ \t]/gm, "");        // ATX headings
  s = s.replace(/```/g, "``" + ZWSP + "`");         // fence escapes
  s = s.replace(/</g, "&lt;").replace(/>/g, "&gt;");
  s = s.trim();
  if (s.length > max) s = s.slice(0, max).trim();
  return s;
}

/** Render text as a markdown blockquote, so it reads as quoted, not authored. */
function blockquote(text) {
  return text.split("\n").map(line => `> ${line}`).join("\n");
}

function slugify(s) {
  return String(s).toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48) || "idea";
}

function shortId() {
  return Math.random().toString(36).slice(-4);
}

function today() {
  return new Date().toISOString().slice(0, 10);
}

// ============================================================
// TURNSTILE
// ============================================================

async function verifyTurnstile(secret, token, ip) {
  // Unconfigured secret => fail closed. An open submission endpoint with no
  // human check is exactly the thing we're trying not to ship.
  if (!secret) return false;
  if (!token) return false;

  const form = new FormData();
  form.append("secret", secret);
  form.append("response", token);
  if (ip) form.append("remoteip", ip);

  try {
    const res = await fetch("https://challenges.cloudflare.com/turnstile/v0/siteverify", {
      method: "POST",
      body: form,
    });
    const data = await res.json();
    return data.success === true;
  } catch (err) {
    console.error("Turnstile verify failed:", err && err.message);
    return false;
  }
}

// ============================================================
// RATE LIMITING (KV)
// ============================================================

/** How many keys the site-wide daily counter is spread across.
 *
 *  Cloudflare allows one write per second to a *single* key. Every accepted
 *  submission used to write one shared `rl:global:<date>`, so two people
 *  submitting in the same second made each other's writes fail — an ordinary
 *  event dressed up as an outage. Spreading the count over several keys and
 *  summing them on read gives that many writes per second instead of one,
 *  which is far more headroom than this site will ever need.
 */
const GLOBAL_SHARDS = 8;

/** Returns "ok" | "client" | "global". Throws only if reads fail. */
async function checkRateLimit(env, request) {
  const kv = env.RATE_LIMIT;
  if (!kv) {
    // An unbound namespace used to mean "no limits", which is the wrong
    // default for the one control standing between a public form and the
    // GitHub API. In production it is a misconfiguration, not a dev
    // convenience, and the sender is told to try later rather than waved past.
    if (isDevUnsafe(env)) return "ok";
    console.error("RATE_LIMIT namespace is not bound — refusing submissions");
    return "global";
  }

  const ip = request.headers.get("CF-Connecting-IP") || "unknown";
  const id = await hashIp(ip, env.IP_SALT || "");
  const now = new Date();
  const hourKey = `rl:${id}:${now.toISOString().slice(0, 13)}`;
  const dayKey  = `rl:${id}:${now.toISOString().slice(0, 10)}`;
  const day     = now.toISOString().slice(0, 10);
  const shardKeys = Array.from(
    { length: GLOBAL_SHARDS }, (_, i) => `rl:global:${day}:${i}`);

  // Reads decide the verdict, so a read failure is a real failure and
  // propagates to the caller, which answers 503.
  const [h, d, ...shards] = await Promise.all([
    kv.get(hourKey), kv.get(dayKey), ...shardKeys.map(k => kv.get(k)),
  ]);
  const globalCount = shards.reduce((sum, v) => sum + toInt(v), 0);

  if (toInt(h) >= PER_HOUR_LIMIT) return "client";
  if (toInt(d) >= PER_DAY_LIMIT)  return "client";
  if (globalCount >= GLOBAL_DAY_LIMIT) return "global";

  // Writes are best effort. Read-modify-write races can undercount under
  // concurrency, and so can a dropped write; both are acceptable here because
  // these are coarse abuse bounds, not accounting. What is *not* acceptable is
  // losing someone's submission because a counter could not be incremented.
  const shard = Math.floor(Math.random() * GLOBAL_SHARDS);
  const writes = [
    kv.put(hourKey, String(toInt(h) + 1), { expirationTtl: 7200 }),
    kv.put(dayKey,  String(toInt(d) + 1), { expirationTtl: 172800 }),
    kv.put(shardKeys[shard], String(toInt(shards[shard]) + 1),
           { expirationTtl: 172800 }),
  ];
  const results = await Promise.allSettled(writes);
  for (const r of results) {
    if (r.status === "rejected") {
      console.error("Rate limit counter write failed:", r.reason && r.reason.message);
    }
  }
  return "ok";
}

function toInt(v) {
  const n = parseInt(v || "0", 10);
  return Number.isFinite(n) ? n : 0;
}

/** Hash the IP so the KV store never holds a raw address. */
async function hashIp(ip, salt) {
  const data = new TextEncoder().encode(`${salt}:${ip}`);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return [...new Uint8Array(digest)].slice(0, 12)
    .map(b => b.toString(16).padStart(2, "0")).join("");
}

// ============================================================
// GITHUB
// ============================================================

async function fileIssue(env, built) {
  const repo = env.GITHUB_REPO;
  if (!repo) throw new Error("GITHUB_REPO is not configured");
  if (!env.GITHUB_TOKEN) throw new Error("GITHUB_TOKEN is not configured");

  // Repeat reports about one stop belong on one thread, not forty issues.
  if (built.dedupeKey) {
    const existing = await findOpenIssueFor(env, repo, built.dedupeKey);
    if (existing) {
      await ghFetch(env, `${GITHUB_API}/repos/${repo}/issues/${existing.number}/comments`, {
        method: "POST",
        body: JSON.stringify({ body: built.body }),
      });
      return existing;
    }
  }

  const url     = `${GITHUB_API}/repos/${repo}/issues`;
  const payload = { title: built.title, body: built.body, labels: built.labels };

  try {
    return await ghFetch(env, url, { method: "POST", body: JSON.stringify(payload) });
  } catch (err) {
    // GitHub rejects the whole request with 422 if any label doesn't exist —
    // it does not create them. So a label renamed or deleted months from now
    // would turn every submission into a 502 with no obvious cause. Losing the
    // labels is recoverable by hand; losing someone's submission is not.
    if (err.status !== 422) throw err;
    console.error("Issue create returned 422, retrying without labels:", err.message);
    const { labels, ...unlabelled } = payload;
    const res = await ghFetch(env, url, { method: "POST", body: JSON.stringify(unlabelled) });
    res.labelsDropped = true;
    return res;
  }
}

/** Find an open stop-issue thread already covering this ATCO code. */
async function findOpenIssueFor(env, repo, atco) {
  try {
    const q = encodeURIComponent(`repo:${repo} is:issue is:open label:stop-issue in:title ${atco}`);
    const res = await ghFetch(env, `${GITHUB_API}/search/issues?q=${q}&per_page=1`);
    if (res && Array.isArray(res.items) && res.items.length > 0) {
      const hit = res.items[0];
      // Search is fuzzy; only fold in on an exact ATCO match in the title.
      if (typeof hit.title === "string" && hit.title.includes(atco)) return hit;
    }
  } catch (err) {
    // A failed dedupe lookup shouldn't lose the report — fall through and
    // file a fresh issue instead.
    console.error("Dedupe search failed:", err && err.message);
  }
  return null;
}

async function ghFetch(env, url, init = {}) {
  const res = await fetch(url, {
    ...init,
    headers: {
      "Authorization": `Bearer ${env.GITHUB_TOKEN}`,
      "Accept":        "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "Content-Type":  "application/json",
      "User-Agent":    USER_AGENT,
      ...(init.headers || {}),
    },
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    const err = new Error(`GitHub ${res.status}: ${text.slice(0, 300)}`);
    err.status = res.status;   // callers branch on this; see fileIssue's 422 path
    throw err;
  }
  return res.json();
}

// ============================================================
// HTTP HELPERS
// ============================================================

function allowedOrigins(env) {
  return String(env.ALLOWED_ORIGINS || "")
    .split(",").map(s => s.trim()).filter(Boolean);
}

function isAllowedOrigin(origin, allowed, env) {
  // This used to return true when the list was empty — "unset in dev". But a
  // typo blanking ALLOWED_ORIGINS in production has the same shape as an
  // unset one, and the result is a public endpoint relaying anything to the
  // GitHub Issues API. Missing configuration is now a refusal, and the
  // development escape hatch has to be asked for by name.
  if (allowed.length === 0) return isDevUnsafe(env);
  return allowed.includes(origin);
}

/** The one explicit opt-out of the production safety checks.
 *
 *  Deliberately not inferable from anything else: no "if localhost", no "if
 *  the config looks empty". Set DEV_UNSAFE=1 in a local wrangler config and
 *  nowhere near a deployment.
 */
function isDevUnsafe(env) {
  return String((env && env.DEV_UNSAFE) || "") === "1";
}

function corsHeaders(origin, allowed) {
  const h = {
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
    "Vary": "Origin",
  };
  if (isAllowedOrigin(origin, allowed) && origin) {
    h["Access-Control-Allow-Origin"] = origin;
  }
  return h;
}

function preflight(origin, allowed) {
  return new Response(null, { status: 204, headers: corsHeaders(origin, allowed) });
}

function json(body, status, origin, allowed) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...corsHeaders(origin, allowed),
    },
  });
}

/** Read the body, refusing anything over `max` bytes. Returns null if over. */
async function readCapped(request, max) {
  const declared = request.headers.get("Content-Length");
  if (declared && parseInt(declared, 10) > max) return null;

  // Read incrementally and stop at the cap. `arrayBuffer()` buffered the whole
  // body first and only then measured it, so a sender who simply omitted
  // Content-Length could make the Worker hold as much as it liked before the
  // limit was consulted — the check ran, but after the cost was paid.
  const body = request.body;
  if (!body) return "";

  const reader = body.getReader();
  const chunks = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > max) {
        await reader.cancel();
        return null;
      }
      chunks.push(value);
    }
  } catch (err) {
    // A truncated or aborted upload is the sender's problem to retry, not a
    // reason for the request to end without an answer.
    console.error("Reading request body failed:", err && err.message);
    return null;
  }

  const joined = new Uint8Array(total);
  let at = 0;
  for (const c of chunks) { joined.set(c, at); at += c.byteLength; }
  return new TextDecoder().decode(joined);
}

// Exported for tests.
export const _internals = {
  sanitize, slugify, buildIssue, blockquote, isAllowedOrigin, fileIssue,
};
