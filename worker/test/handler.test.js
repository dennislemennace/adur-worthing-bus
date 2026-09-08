/**
 * Tests for the Worker's HTTP handler — the paths a submission actually takes.
 *
 * `sanitize.test.js` covers the pure builders. Everything here is about what
 * happens when something goes wrong around them: a KV outage, a missing
 * binding, a body that is valid JSON but not an object, a configuration that
 * was never set. Each of these used to end the request in a way the sender
 * could not act on, or in a way that let them through.
 *
 * Run with:  node --test worker/test/
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import worker from "../src/index.js";

const ORIGIN = "https://worthingbrightonbus.co.uk";

/** A KV namespace that behaves, unless told to break. */
function fakeKV({ throwOnPut = false } = {}) {
  const store = new Map();
  return {
    async get(k) { return store.get(k) ?? null; },
    async put(k, v) {
      if (throwOnPut) throw new Error("KV PUT failed: rate limited");
      store.set(k, v);
    },
    _store: store,
  };
}

function env(over = {}) {
  return {
    ALLOWED_ORIGINS: ORIGIN,
    TURNSTILE_SECRET: "secret",
    GITHUB_TOKEN: "token",
    GITHUB_REPO: "example/repo",
    IP_SALT: "salt",
    RATE_LIMIT: fakeKV(),
    ...over,
  };
}

/** Stand in for Turnstile and the GitHub API so no test leaves the machine.
 *  Returns the calls made, so a test can assert what was *not* attempted. */
function stubNetwork({ issue = { number: 7, html_url: "https://example/7" } } = {}) {
  const calls = [];
  const real = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    const href = String(url);
    calls.push(href);
    if (href.includes("challenges.cloudflare.com")) {
      return new Response(JSON.stringify({ success: true }),
        { headers: { "Content-Type": "application/json" } });
    }
    if (href.includes("api.github.com")) {
      return new Response(JSON.stringify(issue),
        { status: 201, headers: { "Content-Type": "application/json" } });
    }
    throw new Error(`unexpected outbound request: ${href}`);
  };
  return { calls, restore() { globalThis.fetch = real; } };
}

function post(body, { origin = ORIGIN, headers = {} } = {}) {
  return new Request("https://worker.example/submit", {
    method: "POST",
    headers: { "Content-Type": "application/json", Origin: origin, ...headers },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
}

const IDEA = {
  kind: "idea",
  title: "A test idea",
  details: "Enough detail to pass validation.",
  turnstileToken: "x",
};

// ── The handler always answers ──────────────────────────────

test("a KV outage returns a controlled response, not a rejected request", async () => {
  // checkRateLimit sat above the try that wraps issue filing, so a KV write
  // failure rejected the whole fetch. Cloudflare documents one write per
  // second to the same key, and every accepted submission writes the same
  // global daily key — so ordinary concurrent users can cause this.
  const net = stubNetwork();
  try {
    const res = await worker.fetch(
      post(IDEA), env({ RATE_LIMIT: fakeKV({ throwOnPut: true }) }), {});
    assert.ok(res instanceof Response, "the handler did not return a Response");
    const body = await res.json();
    assert.equal(typeof body.ok, "boolean", "no structured answer for the sender");
  } finally {
    net.restore();
  }
});

test("a submission with everything in order is filed", async () => {
  const net = stubNetwork();
  try {
    const res = await worker.fetch(post(IDEA), env(), {});
    const body = await res.json();
    assert.equal(res.status, 200, JSON.stringify(body));
    assert.equal(body.ok, true);
    assert.equal(body.number, 7);
  } finally {
    net.restore();
  }
});

test("a JSON body that is not an object is refused, not dereferenced", async () => {
  // `null` parses fine, and `payload.botcheck` then throws.
  for (const raw of ["null", "42", '"a string"', "[1,2,3]"]) {
    const res = await worker.fetch(post(raw), env(), {});
    assert.equal(res.status, 400, `${raw} should be a bad request`);
    const body = await res.json();
    assert.equal(body.ok, false);
  }
});

// ── Configuration must fail closed ──────────────────────────

test("an unset origin allowlist refuses the request rather than allowing everything", async () => {
  // isAllowedOrigin returned true when the list was empty. A typo blanking
  // one variable turns a public endpoint into an open relay to the GitHub
  // Issues API.
  const res = await worker.fetch(
    post(IDEA, { origin: "https://not-our-site.example" }),
    env({ ALLOWED_ORIGINS: "" }), {});
  assert.equal(res.status, 403,
    "a blank allowlist accepted a submission from an arbitrary origin");
});

test("development can still opt out, but only deliberately", async () => {
  const res = await worker.fetch(
    post(IDEA, { origin: "http://localhost:8765" }),
    env({ ALLOWED_ORIGINS: "", DEV_UNSAFE: "1" }), {});
  assert.notEqual(res.status, 403,
    "the explicit development escape hatch did not work");
});

test("a request from another origin is refused when the allowlist is set", async () => {
  const res = await worker.fetch(
    post(IDEA, { origin: "https://not-our-site.example" }), env(), {});
  assert.equal(res.status, 403);
});

// ── Body size ───────────────────────────────────────────────

test("an oversized body with no declared length is refused", async () => {
  // Without Content-Length the whole body was buffered before the check.
  const huge = JSON.stringify({ ...IDEA, details: "x".repeat(64 * 1024) });
  const req = new Request("https://worker.example/submit", {
    method: "POST",
    headers: { "Content-Type": "application/json", Origin: ORIGIN },
    body: huge,
  });
  req.headers.delete("Content-Length");
  const res = await worker.fetch(req, env(), {});
  assert.equal(res.status, 413);
});

// ── Ordinary paths still work ───────────────────────────────

test("the honeypot still answers as though it succeeded", async () => {
  const res = await worker.fetch(
    post({ ...IDEA, botcheck: "filled in" }), env(), {});
  assert.equal(res.status, 200);
  assert.deepEqual(await res.json(), { ok: true, skipped: true });
});

test("a non-POST method is refused", async () => {
  const res = await worker.fetch(
    new Request("https://worker.example/submit", {
      method: "GET", headers: { Origin: ORIGIN } }), env(), {});
  assert.equal(res.status, 405);
});

test("an oversized stream is abandoned rather than fully read", async () => {
  // The cap used to be applied after the whole body had been buffered. This
  // counts how much the Worker actually pulls: with a 8 KB cap and a 1 MB
  // body it should stop early, not read to the end.
  let pulled = 0;
  const chunk = new TextEncoder().encode("x".repeat(4096));
  const stream = new ReadableStream({
    pull(controller) {
      pulled += chunk.byteLength;
      if (pulled > 1024 * 1024) { controller.close(); return; }
      controller.enqueue(chunk);
    },
  });
  const req = new Request("https://worker.example/submit", {
    method: "POST",
    headers: { "Content-Type": "application/json", Origin: ORIGIN },
    body: stream,
    duplex: "half",
  });
  const res = await worker.fetch(req, env(), {});
  assert.equal(res.status, 413);
  assert.ok(pulled < 1024 * 1024,
    `read ${pulled} bytes of a 1 MB body before applying an 8 KB cap`);
});

test("a body that fails mid-read is answered, not dropped", async () => {
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode('{"kind":'));
      controller.error(new Error("connection reset"));
    },
  });
  const req = new Request("https://worker.example/submit", {
    method: "POST",
    headers: { "Content-Type": "application/json", Origin: ORIGIN },
    body: stream,
    duplex: "half",
  });
  const res = await worker.fetch(req, env(), {});
  assert.ok(res instanceof Response);
  assert.equal((await res.json()).ok, false);
});
