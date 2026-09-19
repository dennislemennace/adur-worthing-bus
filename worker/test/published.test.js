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
        return ["journey-times/700.json", "journey-times/index.json",
                "journey-times/1-BHBC.json"].includes(key)
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


// ── What is cached, and for how long ────────────────────────

test("the index is not cached as long as the files it names", async () => {
  // Everything here was served with an hour's cache, the index included. The
  // night the documents were split by operator, 1.json became 1-BHBC.json and
  // 1-SCSO.json and the old name was deleted from the bucket — so every reader
  // holding an hour-old index was asking for a file that no longer existed.
  //
  // A cross-origin fetch is not reliably re-fetched by a hard reload either,
  // which is why "I have pressed Ctrl+Shift+R several times" did not clear it.
  const index = await get("/journey-times/index.json");
  const doc = await get("/journey-times/700.json");
  const indexAge = Number(/max-age=(\d+)/.exec(
    index.headers.get("cache-control"))?.[1]);
  const docAge = Number(/max-age=(\d+)/.exec(
    doc.headers.get("cache-control"))?.[1]);

  assert.ok(indexAge <= 300,
    `the index is cached ${indexAge}s, long enough to name a deleted file`);
  assert.ok(docAge > indexAge,
    "a document should outlast the index, not the other way round");
});

test("a document may still be cached for an hour", async () => {
  // A stale chart is yesterday's data, which is honest. A stale index is a
  // broken view, which is not. Only the second is worth a request a minute.
  const doc = await get("/journey-times/700.json");
  assert.match(doc.headers.get("cache-control"), /max-age=3600/);
});

test("the split filenames are servable at all", async () => {
  // The path is matched by a regex, and the new names carry a hyphen and are
  // longer than the old ones. A name the builder writes and the server refuses
  // is a 404 that only appears in production.
  const res = await get("/journey-times/1-BHBC.json");
  assert.equal(res.status, 200, "an operator-split filename was rejected");
});
