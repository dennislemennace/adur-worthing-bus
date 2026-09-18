/**
 * Serving published measurements from R2.
 *
 * These are the evidence the project exists to produce, so the route is
 * deliberately open to any origin — a councillor's analyst should be able to
 * fetch the numbers and check our arithmetic without asking us. That makes it
 * the one public-read surface on a Worker whose other endpoint is fenced to
 * our own site, so the tests below are mostly about what it refuses.
 *
 * Run with:  node --test worker/test/published.test.js
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import worker from "../src/index.js";

const BODY = JSON.stringify({ service: "700", journeys: [] });

function env(over = {}) {
  return {
    PUBLISHED: {
      async get(key) {
        return key === "journey-times/700.json"
          ? { body: BODY, key }
          : null;
      },
    },
    ALLOWED_ORIGINS: "https://worthingbrightonbus.co.uk",
    ...over,
  };
}

const get = (path, method = "GET", e = env()) =>
  worker.fetch(new Request(`https://w.example${path}`, { method }), e, { waitUntil() {} });

test("a published file is served to anyone", async () => {
  const res = await get("/journey-times/700.json");
  assert.equal(res.status, 200);
  assert.equal(res.headers.get("access-control-allow-origin"), "*",
    "the evidence cannot be checked by anyone who cannot fetch it");
  assert.match(res.headers.get("content-type"), /application\/json/);
  assert.equal(await res.text(), BODY);
});

test("a file that does not exist says so, as JSON", async () => {
  const res = await get("/journey-times/999.json");
  assert.equal(res.status, 404);
  assert.match(res.headers.get("content-type"), /application\/json/);
});

test("the path cannot escape the prefix", async () => {
  // Asserting on the *key asked of R2*, not merely on the status: a bucket
  // that happens to hold nothing at the escaped key would let a broken route
  // pass. Getting this wrong on a bucket that also held the raw record would
  // be a bad day.
  const asked = [];
  const e = env({ PUBLISHED: { async get(key) { asked.push(key); return null; } } });
  for (const path of ["/journey-times/../snapshots.json",
                      "/journey-times/%2e%2e%2fraw.json",
                      "/journey-times/sub/dir.json",
                      "/journey-times/700.json.bak",
                      "/journey-times/"]) {
    const res = await get(path, "GET", e);
    assert.notEqual(res.status, 200, `${path} was served`);
  }
  assert.deepEqual(asked, [],
    `a request escaped the prefix and reached R2: ${asked.join(", ")}`);
});

test("only reading is allowed", async () => {
  for (const method of ["POST", "PUT", "DELETE"]) {
    const res = await get("/journey-times/700.json", method);
    assert.equal(res.status, 405, `${method} was not refused`);
  }
});

test("a preflight is answered without touching the bucket", async () => {
  let asked = false;
  const e = env({ PUBLISHED: { async get() { asked = true; return null; } } });
  const res = await get("/journey-times/700.json", "OPTIONS", e);
  assert.equal(res.status, 204);
  assert.equal(res.headers.get("access-control-allow-origin"), "*");
  assert.equal(asked, false);
});

test("the submission endpoint is untouched by any of this", async () => {
  // The other route is fenced to our own origin, and must stay that way: it
  // files issues in a private repository.
  const res = await worker.fetch(
    new Request("https://w.example/submit", { method: "POST", body: "{}" }),
    env(), { waitUntil() {} });
  assert.equal(res.status, 403, "the submission endpoint stopped checking origins");
});

test("an unconfigured bucket says so rather than pretending", async () => {
  const res = await get("/journey-times/700.json", "GET", env({ PUBLISHED: undefined }));
  assert.equal(res.status, 503);
});
