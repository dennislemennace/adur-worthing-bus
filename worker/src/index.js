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
    if (!isAllowedOrigin(origin, allowed)) {
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
    const verdict = await checkRateLimit(env, request);
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

/** Returns "ok" | "client" | "global". */
async function checkRateLimit(env, request) {
  const kv = env.RATE_LIMIT;
  if (!kv) return "ok";   // unbound in dev — don't block local testing

  const ip = request.headers.get("CF-Connecting-IP") || "unknown";
  const id = await hashIp(ip, env.IP_SALT || "");
  const now = new Date();
  const hourKey = `rl:${id}:${now.toISOString().slice(0, 13)}`;
  const dayKey  = `rl:${id}:${now.toISOString().slice(0, 10)}`;
  const globalKey = `rl:global:${now.toISOString().slice(0, 10)}`;

  const [h, d, g] = await Promise.all([
    kv.get(hourKey), kv.get(dayKey), kv.get(globalKey),
  ]);

  if (toInt(h) >= PER_HOUR_LIMIT) return "client";
  if (toInt(d) >= PER_DAY_LIMIT)  return "client";
  if (toInt(g) >= GLOBAL_DAY_LIMIT) return "global";

  // Read-modify-write races can undercount under concurrency. That's an
  // acceptable trade here: these are coarse abuse bounds, not accounting.
  await Promise.all([
    kv.put(hourKey, String(toInt(h) + 1), { expirationTtl: 7200 }),
    kv.put(dayKey,  String(toInt(d) + 1), { expirationTtl: 172800 }),
    kv.put(globalKey, String(toInt(g) + 1), { expirationTtl: 172800 }),
  ]);
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

  const res = await ghFetch(env, `${GITHUB_API}/repos/${repo}/issues`, {
    method: "POST",
    body: JSON.stringify({
      title:  built.title,
      body:   built.body,
      labels: built.labels,
    }),
  });
  return res;
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
    throw new Error(`GitHub ${res.status}: ${text.slice(0, 300)}`);
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

function isAllowedOrigin(origin, allowed) {
  if (allowed.length === 0) return true;   // unset in dev
  return allowed.includes(origin);
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

  const buf = await request.arrayBuffer();
  if (buf.byteLength > max) return null;
  return new TextDecoder().decode(buf);
}

// Exported for tests.
export const _internals = {
  sanitize, slugify, buildIssue, blockquote, isAllowedOrigin,
};
