/**
 * Tests for keeping the Render API awake from 07:30 to 23:30 London time.
 *
 * This used to be a GitHub Actions schedule set to every ten minutes. Over 38
 * hours GitHub started it 13 times instead of about 228, with a median gap of
 * 163 minutes, so the free-tier container slept between every pair of runs
 * and the keep-warm kept nothing warm. Cloudflare cron triggers fire on time,
 * and this Worker was already deployed.
 *
 * Two things here are easy to get wrong and silent when wrong: the window has
 * to follow British Summer Time, and the cron (which Cloudflare reads in UTC)
 * has to cover the whole window in both summer and winter.
 *
 * Run with:  node --test worker/test/
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import worker, { _internals } from "../src/index.js";

const { isWarmTime, keepWarm } = _internals;

const at = (iso) => new Date(iso);

// ── The window follows London, not UTC ──────────────────────

test("in summer the window opens at 07:30 BST, which is 06:30 UTC", () => {
  assert.equal(isWarmTime(at("2026-09-14T06:29:00Z")), false, "07:29 BST should still be asleep");
  assert.equal(isWarmTime(at("2026-09-14T06:30:00Z")), true, "07:30 BST should be warm");
});

test("in summer the window closes at 23:30 BST, which is 22:30 UTC", () => {
  assert.equal(isWarmTime(at("2026-09-14T22:29:00Z")), true, "23:29 BST should be warm");
  assert.equal(isWarmTime(at("2026-09-14T22:30:00Z")), false, "23:30 BST should be asleep");
});

test("in winter the same window is 07:30 to 23:30 UTC", () => {
  assert.equal(isWarmTime(at("2027-01-14T07:29:00Z")), false);
  assert.equal(isWarmTime(at("2027-01-14T07:30:00Z")), true);
  assert.equal(isWarmTime(at("2027-01-14T23:29:00Z")), true);
  assert.equal(isWarmTime(at("2027-01-14T23:30:00Z")), false);
});

test("the small hours are asleep whichever side of midnight", () => {
  // The quiet period crosses midnight. A "from < now < to" test written for it
  // is never true, which is how the GitHub version nearly came to ping all night.
  for (const iso of ["2026-09-14T23:10:00Z", "2026-09-15T01:00:00Z",
                     "2027-01-14T23:45:00Z", "2027-01-15T04:00:00Z"]) {
    assert.equal(isWarmTime(at(iso)), false, `${iso} was treated as warm`);
  }
});

// ── The cron actually covers the window ─────────────────────

/** The crons from wrangler.toml, parsed just enough for this schedule. */
function cronSlots() {
  const toml = readFileSync(new URL("../wrangler.toml", import.meta.url), "utf8");
  const m = /crons\s*=\s*\[\s*"([^"]+)"/.exec(toml);
  assert.ok(m, "wrangler.toml has no cron trigger, so nothing will ever ping");
  const [min, hour] = m[1].split(/\s+/);
  const step = /^\*\/(\d+)$/.exec(min);
  assert.ok(step, `unexpected minute field ${min}`);
  const range = /^(\d+)-(\d+)$/.exec(hour);
  const hours = range ? [Number(range[1]), Number(range[2])] : [0, 23];
  return { every: Number(step[1]), fromHour: hours[0], toHour: hours[1] };
}

function cronFiresAt(date, c) {
  return date.getUTCMinutes() % c.every === 0
      && date.getUTCHours() >= c.fromHour && date.getUTCHours() <= c.toHour;
}

test("the cron fires often enough to beat the 15-minute idle limit", () => {
  const c = cronSlots();
  assert.ok(c.every <= 10, `a ping every ${c.every} minutes lets the container sleep`);
});

for (const [season, day] of [["summer", "2026-09-14"], ["winter", "2027-01-14"]]) {
  test(`every ten minutes of the warm window has a cron run (${season})`, () => {
    // Cloudflare reads crons in UTC. An hour range that fits BST misses the
    // last hour of the evening in GMT, or the reverse, and nothing would say so.
    const c = cronSlots();
    const missed = [];
    const start = at(`${day}T00:00:00Z`).getTime();
    for (let t = start; t < start + 86_400_000; t += 600_000) {
      const d = new Date(t);
      if (isWarmTime(d) && !cronFiresAt(d, c)) missed.push(d.toISOString().slice(11, 16));
    }
    assert.deepEqual(missed, [], `warm slots with no cron run (UTC): ${missed.join(", ")}`);
  });
}

// ── The handler ─────────────────────────────────────────────

function fakeCtx() {
  const pending = [];
  return { waitUntil: (p) => pending.push(p), pending };
}

test("a cron run inside the window pings the API once", async () => {
  const calls = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url) => { calls.push(String(url)); return new Response("ok", { status: 200 }); };
  try {
    const ctx = fakeCtx();
    await worker.scheduled({ scheduledTime: at("2026-09-14T12:00:00Z").getTime() },
                           { KEEP_WARM_URL: "https://api.example/" }, ctx);
    await Promise.all(ctx.pending);
    assert.deepEqual(calls, ["https://api.example/"]);
  } finally {
    globalThis.fetch = realFetch;
  }
});

test("a cron run outside the window does nothing", async () => {
  const calls = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url) => { calls.push(String(url)); return new Response("ok"); };
  try {
    const ctx = fakeCtx();
    await worker.scheduled({ scheduledTime: at("2026-09-14T02:00:00Z").getTime() },
                           { KEEP_WARM_URL: "https://api.example/" }, ctx);
    await Promise.all(ctx.pending);
    assert.deepEqual(calls, [], "the API was woken at 03:00 BST");
  } finally {
    globalThis.fetch = realFetch;
  }
});

test("the window is judged by when the run was scheduled, not when it started", async () => {
  // A run scheduled for 23:20 that starts a few seconds late must still ping.
  const result = await keepWarm({}, at("2026-09-14T22:20:00Z"),
                                async () => new Response("ok", { status: 200 }));
  assert.equal(result.pinged, true);
});

test("an API that is down is logged, not thrown", async () => {
  // A throw from a scheduled handler shows as a failed invocation and nothing
  // else; a returned result with the error in it is at least in the logs.
  const result = await keepWarm({}, at("2026-09-14T12:00:00Z"),
                                async () => { throw new Error("connect ECONNREFUSED"); });
  assert.equal(result.pinged, true);
  assert.match(result.error, /ECONNREFUSED/);
});

test("the submission endpoint still answers after adding a scheduled handler", async () => {
  assert.equal(typeof worker.fetch, "function");
  const res = await worker.fetch(new Request("https://w.example/", { method: "OPTIONS",
    headers: { Origin: "https://worthingbrightonbus.co.uk" } }),
    { ALLOWED_ORIGINS: "https://worthingbrightonbus.co.uk" }, fakeCtx());
  assert.ok(res.status < 500, `preflight returned ${res.status}`);
});
