/**
 * Tests for the prediction log route (src/predlog.js).
 *
 * It takes writes from outside, so it must not exist until switched on, must
 * refuse anyone without the secret, must store only the fields the scorer
 * reads, and must stop at the recorder's storage budget like everything else.
 *
 * Run with:  node --test worker/test/
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import worker from "../src/index.js";
import { cleanRow, logKey, predictionLog } from "../src/predlog.js";

const WHEN = new Date("2026-10-26T12:00:00Z");
const ROW = { fetched_at: 1792843200, stop: "4400AD0329", trip_id: "VJ123", aimed: "2026-10-26T12:05:00+00:00",
              expected: "2026-10-26T12:07:00+00:00", cancelled: false };

function env(over = {}) {
  const puts = [];
  return {
    puts,
    PREDICTION_LOG_TOKEN: "s3cret",
    SNAPSHOTS: { put: async (key, body) => puts.push({ key, body }) },
    RATE_LIMIT: { get: async () => JSON.stringify({ bytes: 1000, objects: 10, measured_at: WHEN.toISOString() }) },
    ...over,
  };
}

const post = (body, token = "s3cret") => new Request("https://w.example/prediction-log", {
  method: "POST", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

test("the route does not exist until its secret is set", async () => {
  const res = await predictionLog(post({ rows: [ROW] }), env({ PREDICTION_LOG_TOKEN: "" }), WHEN);
  assert.equal(res.status, 404);
});

test("without the secret, nothing is stored", async () => {
  const e = env();
  const res = await predictionLog(post({ rows: [ROW] }, "guess"), e, WHEN);
  assert.equal(res.status, 403);
  assert.equal(e.puts.length, 0);
});

test("rows are stored by day, and only the fields the scorer reads", async () => {
  const e = env();
  const res = await predictionLog(post({ rows: [{ ...ROW, extra: "x" }, { ...ROW, stop: "<script>" }] }), e, WHEN);
  assert.equal(res.status, 200);
  assert.deepEqual(await res.json(), { ok: true, stored: 1, rejected: 1 });
  assert.match(e.puts[0].key, /^predlog\/2026-10-26\/\d+-[a-z0-9]+\.json$/);
  assert.deepEqual(JSON.parse(e.puts[0].body.trim()), ROW, "a field the scorer does not read was kept");
});

test("the recorder's storage budget stops the log too", async () => {
  const e = env({ RATE_LIMIT: { get: async () => null } });
  const res = await predictionLog(post({ rows: [ROW] }), e, WHEN);
  assert.equal(res.status, 507);
  assert.equal(e.puts.length, 0);
});

test("a cancelled journey needs no expected time; anything else malformed is dropped", () => {
  assert.ok(cleanRow({ ...ROW, expected: null, cancelled: true }));
  assert.equal(cleanRow({ ...ROW, fetched_at: "now" }), null);
  assert.equal(cleanRow({ ...ROW, aimed: "12:05" }), null);
  assert.match(logKey(WHEN, "abc"), /^predlog\/2026-10-26\/\d+-abc\.json$/);
});

test("the Worker routes it before the origin check", async () => {
  // The Worker uses the real clock, so the budget is measured now.
  const fresh = { get: async () => JSON.stringify({ bytes: 1000, objects: 10, measured_at: new Date().toISOString() }) };
  const res = await worker.fetch(post({ rows: [ROW] }), env({ RATE_LIMIT: fresh }), {});
  assert.equal(res.status, 200, "a server-to-server call with no Origin was refused");
});
