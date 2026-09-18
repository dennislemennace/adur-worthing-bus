/**
 * Tests for the snapshot recorder.
 *
 * The record is the foundation of every reliability figure the site will
 * publish, so the ways it can quietly fail all matter:
 *
 *   * recording through the night would spend storage on an empty road, and
 *     the window crosses midnight, which is where such tests usually go wrong;
 *   * a missing bucket or key must stop it, not half-write something;
 *   * a scheduled handler that throws shows as a failed invocation and nothing
 *     else, so a feed outage has to be logged and swallowed;
 *   * the key has to sort by day and minute, because a day is processed and
 *     deleted by prefix; and
 *   * the body must be streamed, never read, or the Worker's CPU budget is
 *     spent on copying XML.
 *
 * Run with:  node --test worker/test/
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import worker from "../src/index.js";
import {
  recordSnapshot, isRecordingTime, snapshotKey, gtfsRtKey, keyDay,
  pruneAndMeasure, withinBudget, readUsage, _internals,
} from "../src/recorder.js";

/** Both feeds are fetched each minute, so a fixture must answer twice. */
const bothFeeds = (body = "<Siri/>") => async () => new Response(body, { status: 200 });

/** The stored object for one feed, by prefix. */
const put = (e, prefix) => e.SNAPSHOTS.puts.find(p => p.key.startsWith(prefix));

const at = (iso) => new Date(iso);

/** An R2 bucket that remembers what it was given, and can be seeded with
 *  objects so pruning and measuring have something to work on. */
function fakeBucket(objects = []) {
  const puts = [], deleted = [], lists = [];
  const held = [...objects];
  return {
    puts, deleted, lists, held,
    async put(key, body, opts) {
      // R2's own rule, and the one this recorder was written against wrongly:
      // a stream it cannot measure is refused outright.
      if (body instanceof ReadableStream) {
        throw new TypeError("Provided readable stream must have a known length");
      }
      puts.push({ key, body, opts });
    },
    async list({ prefix, cursor, limit = 1000 } = {}) {
      lists.push({ prefix, cursor });
      const all = held.filter((o) => o.key.startsWith(prefix || ""));
      const start = cursor ? Number(cursor) : 0;
      const page = all.slice(start, start + limit);
      const end = start + page.length;
      return { objects: page, truncated: end < all.length, cursor: String(end) };
    },
    async delete(keys) {
      for (const k of [].concat(keys)) {
        deleted.push(k);
        const i = held.findIndex((o) => o.key === k);
        if (i >= 0) held.splice(i, 1);
      }
    },
  };
}

/** A KV namespace, which the recorder uses to remember what it measured. */
function fakeKv(seed = {}) {
  const store = new Map(Object.entries(seed));
  return {
    store,
    async get(k) { return store.has(k) ? store.get(k) : null; },
    async put(k, v) { store.set(k, v); },
  };
}

const MB = 1024 * 1024;

/** A day of snapshots, as objects of a given size. */
function dayOf(date, count, size = 300 * 1024, prefix = "raw", ext = "xml") {
  return Array.from({ length: count }, (_, i) => ({
    key: `${prefix}/${date}/${String(500 + i).padStart(4, "0")}.${ext}`, size,
  }));
}

function env(over = {}) {
  return { SNAPSHOTS: fakeBucket(), BODS_API_KEY: "test-key", ...over };
}

// A real Response, because the recorder depends on how one behaves: the feed
// sends no content-length, so `body` is a stream of unknown length.
const okFeed = (body = "<Siri/>") => async () => new Response(body, { status: 200 });
const decode = (buf) => new TextDecoder().decode(buf);

// ── When it records ─────────────────────────────────────────

for (const [iso, recording, label] of [
  ["2026-09-16T03:59:00Z", false, "04:59 BST, a minute before the first buses"],
  ["2026-09-16T04:00:00Z", true,  "05:00 BST, the first buses"],
  ["2026-09-16T12:00:00Z", true,  "13:00 BST"],
  ["2026-09-16T22:45:00Z", true,  "23:45 BST"],
  ["2026-09-16T23:29:00Z", true,  "00:29 BST, the last night buses"],
  ["2026-09-16T23:30:00Z", false, "00:30 BST, the road is empty"],
  ["2026-09-17T02:00:00Z", false, "03:00 BST"],
  ["2027-01-14T03:30:00Z", false, "03:30 GMT"],
  ["2027-01-14T05:00:00Z", true,  "05:00 GMT"],
]) {
  test(`${label} is ${recording ? "recorded" : "skipped"}`, () => {
    assert.equal(isRecordingTime(at(iso)), recording);
  });
}

test("nothing is fetched or stored during the quiet hours", async () => {
  let called = false;
  const e = env();
  const r = await recordSnapshot(e, at("2026-09-17T02:00:00Z"), async () => { called = true; });
  assert.equal(r.recorded, false);
  assert.equal(r.reason, "quiet_hours");
  assert.equal(called, false, "the feed was fetched at 03:00");
  assert.deepEqual(e.SNAPSHOTS.puts, []);
});

// ── The key ─────────────────────────────────────────────────

test("a key sorts by day and minute, in London time", () => {
  assert.equal(snapshotKey(at("2026-09-16T11:45:00Z")), "raw/2026-09-16/1245.xml");
  assert.equal(snapshotKey(at("2027-01-14T11:45:00Z")), "raw/2027-01-14/1145.xml");
  // Just before midnight London, the day is still the 16th, so an evening and
  // the night buses after it land in one folder to process together.
  assert.equal(snapshotKey(at("2026-09-16T22:05:00Z")), "raw/2026-09-16/2305.xml");
});

// ── Storing ─────────────────────────────────────────────────

test("a snapshot is stored once, as the feed sent it", async () => {
  const e = env();
  const body = "<Siri><VehicleActivity/></Siri>";
  const r = await recordSnapshot(e, at("2026-09-16T11:45:00Z"), bothFeeds(body));
  assert.equal(r.recorded, true);
  const siri = put(e, "raw/");
  assert.equal(siri.key, "raw/2026-09-16/1245.xml");
  assert.equal(decode(siri.body), body, "the body was not passed through untouched");
  assert.equal(siri.opts.customMetadata.recordedAt, "2026-09-16T11:45:00.000Z");
  assert.equal(r.bytes, body.length);
});

test("the same minute's GTFS-RT is stored beside it", async () => {
  // SIRI-VM says where a bus is; GTFS-RT says which journey it is running,
  // which is the one thing our matching has to infer. Both are kept until the
  // second is proven to carry what it claims.
  const e = env();
  const r = await recordSnapshot(e, at("2026-09-16T11:45:00Z"), bothFeeds());
  assert.equal(e.SNAPSHOTS.puts.length, 2, "only one feed was recorded");
  const rt = put(e, "rt/");
  assert.equal(rt.key, "rt/2026-09-16/1245.pb");
  assert.equal(rt.opts.httpMetadata.contentType, "application/x-protobuf");
  assert.equal(r.rt.stored, true);
});

test("a GTFS-RT outage does not cost us the positions", async () => {
  // The second feed is the experiment; the first is the record. An error from
  // the new one must not take the working one down with it.
  const e = env();
  let call = 0;
  const r = await recordSnapshot(e, at("2026-09-16T11:45:00Z"), async () => {
    call += 1;
    if (call === 1) throw new Error("gtfs-rt unavailable");
    return new Response("<Siri/>", { status: 200 });
  });
  assert.equal(r.recorded, true, "the SIRI snapshot was lost with the GTFS-RT one");
  assert.equal(r.rt.stored, false);
  assert.equal(e.SNAPSHOTS.puts.length, 1);
  assert.ok(put(e, "raw/"), "the positions were not stored");
});

test("a feed answering without a content length is still stored", async () => {
  // Incident, 16 September 2026: the first deploy recorded nothing at all. The
  // recorder piped res.body into R2, which refuses a stream of unknown length,
  // and BODS answers chunked — so every minute failed with "Provided readable
  // stream must have a known length" while the test suite stayed green, because
  // the test asserted the intended design instead of R2's behaviour.
  const e = env();
  // A fresh reply each call: both feeds are fetched, and a Response body can
  // only be read once.
  const chunked = () => new Response(new ReadableStream({
    start(c) { c.enqueue(new TextEncoder().encode("<Siri><Vehicle/></Siri>")); c.close(); },
  }), { status: 200 });
  assert.equal(chunked().headers.get("content-length"), null, "the fixture is not a chunked reply");
  const r = await recordSnapshot(e, at("2026-09-16T11:45:00Z"), async () => chunked());
  assert.equal(r.recorded, true, `a chunked feed was not stored: ${r.error || r.reason}`);
  assert.equal(decode(put(e, "raw/").body), "<Siri><Vehicle/></Siri>");
});

test("the body is copied, never decoded or parsed", () => {
  // An ArrayBuffer is a copy; text() would decode ~300 KB of XML and JSON would
  // parse it, and the Worker has 10 ms of CPU an invocation.
  const src = readFileSync(new URL("../src/recorder.js", import.meta.url), "utf8");
  assert.match(src, /await res\.arrayBuffer\(\)/);
  assert.doesNotMatch(src, /await res\.(text|json)\(\)/,
    "the recorder decodes the feed instead of copying it");
});

test("the key never reaches the stored object or the log", () => {
  const src = readFileSync(new URL("../src/recorder.js", import.meta.url), "utf8");
  assert.match(src, /encodeURIComponent\(env\.BODS_API_KEY\)/);
  // The URL carries the key, so it must not be what gets logged.
  assert.doesNotMatch(src, /console\.(log|error)\([^)]*\burl\b/);
});

// ── When it cannot ──────────────────────────────────────────

for (const [label, over, reason] of [
  ["no bucket is bound", { SNAPSHOTS: undefined }, "no_bucket"],
  ["no feed key is set", { BODS_API_KEY: "" }, "no_key"],
]) {
  test(`recording stops when ${label}`, async () => {
    let called = false;
    const r = await recordSnapshot(env(over), at("2026-09-16T11:45:00Z"),
                                   async () => { called = true; });
    assert.equal(r.recorded, false);
    assert.equal(r.reason, reason);
    assert.equal(called, false);
  });
}

test("a feed outage is logged and swallowed, not thrown", async () => {
  const e = env();
  const r = await recordSnapshot(e, at("2026-09-16T11:45:00Z"),
                                 async () => { throw new Error("connect ETIMEDOUT"); });
  assert.equal(r.recorded, false);
  assert.match(r.error, /ETIMEDOUT/);
  assert.deepEqual(e.SNAPSHOTS.puts, [], "a failed fetch still wrote something");
});

test("an error response is not stored as if it were a snapshot", async () => {
  const e = env();
  const r = await recordSnapshot(e, at("2026-09-16T11:45:00Z"),
                                 async () => new Response("slow down", { status: 429 }));
  assert.equal(r.recorded, false);
  assert.equal(r.status, 429);
  assert.deepEqual(e.SNAPSHOTS.puts, [], "an HTTP error was stored as data");
});

// ── The two schedules ───────────────────────────────────────

test("each cron runs its own job and not the other", async () => {
  const pending = [];
  const ctx = { waitUntil: (p) => pending.push(p) };
  const calls = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    calls.push(String(url));
    return new Response("<Siri/>", { status: 200 });
  };
  try {
    // The minute trigger records; it must not ping Render.
    await worker.scheduled({ cron: "* * * * *", scheduledTime: at("2026-09-16T11:45:00Z").getTime() },
                           env({ KEEP_WARM_URL: "https://api.example/" }), ctx);
    await Promise.all(pending.splice(0));
    assert.ok(calls.every(u => u.includes("bus-data.dft.gov.uk")),
      `the recorder cron called something else: ${calls.join(", ")}`);

    // The ten-minute trigger keeps Render warm; it must not record.
    calls.length = 0;
    const e = env({ KEEP_WARM_URL: "https://api.example/" });
    await worker.scheduled({ cron: "*/10 6-23 * * *", scheduledTime: at("2026-09-16T11:45:00Z").getTime() },
                           e, ctx);
    await Promise.all(pending.splice(0));
    assert.deepEqual(calls, ["https://api.example/"]);
    assert.deepEqual(e.SNAPSHOTS.puts, [], "the keep-warm cron recorded a snapshot");
  } finally {
    globalThis.fetch = realFetch;
  }
});

test("wrangler.toml has both triggers and the bucket the recorder needs", () => {
  const toml = readFileSync(new URL("../wrangler.toml", import.meta.url), "utf8");
  const crons = /crons\s*=\s*\[([^\]]+)\]/.exec(toml);
  assert.ok(crons, "no cron triggers are configured");
  assert.match(crons[1], /"\* \* \* \* \*"/, "nothing records every minute");
  assert.match(toml, /binding\s*=\s*"SNAPSHOTS"/, "no R2 bucket is bound for snapshots");
});

// ── Staying inside R2's free tier ───────────────────────────
//
// R2 charges automatically past its free allowances and has no cut-off switch,
// so these are the tests that stand between the project and a bill. The danger
// is not the writes (a minute apart, they cannot reach a million a month) but
// storage, if the nightly processor stops deleting what it has used.

test("days past the retention window are deleted, recent ones kept", async () => {
  const e = env({
    SNAPSHOTS: fakeBucket([
      ...dayOf("2026-09-01", 3),   // fifteen days old
      ...dayOf("2026-09-08", 3),   // eight days old, outside the week
      ...dayOf("2026-09-09", 3),   // seven days old: the oldest day still kept
      ...dayOf("2026-09-14", 3),   // two days old
      ...dayOf("2026-09-16", 3),   // today
    ]),
    RATE_LIMIT: fakeKv(),
  });
  const r = await pruneAndMeasure(e, at("2026-09-16T11:07:00Z"));
  assert.equal(r.deleted, 6, "the wrong number of old snapshots was dropped");
  assert.equal(r.objects, 9, "what was kept is not what was measured");
  assert.ok(e.SNAPSHOTS.deleted.every((k) => keyDay(k) < "2026-09-09"),
    `a snapshot inside the window was deleted: ${e.SNAPSHOTS.deleted.join(", ")}`);
  assert.ok(e.SNAPSHOTS.held.every((o) => keyDay(o.key) >= "2026-09-09"),
    "an expired snapshot survived the prune");
});

test("what is stored is measured and remembered for the next minute", async () => {
  const kv = fakeKv();
  const e = env({ SNAPSHOTS: fakeBucket(dayOf("2026-09-16", 4, 250 * 1024)), RATE_LIMIT: kv });
  const r = await pruneAndMeasure(e, at("2026-09-16T11:07:00Z"));
  assert.equal(r.bytes, 4 * 250 * 1024);
  assert.equal(r.objects, 4);
  assert.deepEqual(JSON.parse(kv.store.get("r2-usage")),
    { bytes: 4 * 250 * 1024, objects: 4, deleted: 0, measured_at: "2026-09-16T11:07:00.000Z" });
  assert.deepEqual(await readUsage(e), r && { bytes: r.bytes, objects: r.objects, deleted: 0,
    measured_at: "2026-09-16T11:07:00.000Z" });
});

test("a bucket larger than a page is measured whole", async () => {
  // A day is ~1,170 objects and a list returns at most 1,000, so a prune that
  // stops at the first page would under-measure and never notice a full bucket.
  const e = env({ SNAPSHOTS: fakeBucket(dayOf("2026-09-16", 2_400, MB)), RATE_LIMIT: fakeKv() });
  const r = await pruneAndMeasure(e, at("2026-09-16T11:07:00Z"));
  assert.equal(r.objects, 2_400, "measuring stopped at the first page");
  assert.equal(e.SNAPSHOTS.lists.length, 3);
});

// The budget refuses a write before it happens, which is the only moment that
// costs anything.
for (const [label, usage, allowed] of [
  ["a bucket inside its budget", { bytes: 2 * 1024 * MB, objects: 7_000 }, true],
  ["nothing measured yet", null, true],
  ["storage past the budget", { bytes: 5 * 1024 * MB, objects: 7_000 }, false],
  ["more objects than budgeted", { bytes: 100 * MB, objects: 20_000 }, false],
]) {
  test(`recording ${allowed ? "continues with" : "stops on"} ${label}`, async () => {
    let fetched = false;
    const e = env({ RATE_LIMIT: fakeKv(usage ? { "r2-usage": JSON.stringify(usage) } : {}) });
    const r = await recordSnapshot(e, at("2026-09-16T11:45:00Z"),
                                   async () => { fetched = true; return new Response("<Siri/>", { status: 200 }); });
    assert.equal(r.recorded, allowed);
    assert.equal(e.SNAPSHOTS.puts.length, allowed ? 2 : 0,
      allowed ? "a snapshot inside budget was refused" : "a snapshot was stored past the budget");
    assert.equal(fetched, allowed, "the feed was called for a snapshot that could not be stored");
    if (!allowed) assert.match(r.reason, /budget/);
  });
}

test("the budget holds even at four fifths of the free tier", () => {
  // The free allowance is 10 GB. The point of the budget is that it stops well
  // short of it, so a measurement an hour stale cannot cross the line.
  const freeTier = 10 * 1024 * MB;
  assert.ok(_internals.MAX_STORED_BYTES <= freeTier / 2,
    "the byte budget is too close to the free tier to absorb an hour of writing");
  assert.equal(withinBudget({ bytes: 8 * 1024 * MB, objects: 1 }).ok, false);
  // A day of snapshots is ~350 MB, so the retention window must stay inside it.
  assert.ok(_internals.RETENTION_DAYS * 400 * MB < _internals.MAX_STORED_BYTES,
    "the retention window alone can exceed the byte budget");
});

test("a month of writing cannot reach the free operation limit", () => {
  // Class A operations are the writes, plus the lists and deletes that follow
  // them. One write a minute for the longest possible month, each attended by a
  // prune's handful of lists and its deletes, stays far below a million.
  const writes = 44_640;                        // 31 days of minutes
  const prunes = 31 * 24;                       // one an hour
  const classA = writes + prunes * 4 + writes;  // writes + lists + one delete each
  assert.ok(classA < 1_000_000 / 2, `Class A operations reach ${classA} a month`);
});

test("the bucket is measured once an hour, not once a minute", async () => {
  // Each measurement writes to KV, and KV allows 1,000 writes a day across the
  // whole Worker; measuring every minute would spend 1,440 of them.
  const kv = fakeKv();
  const e = env({ SNAPSHOTS: fakeBucket(dayOf("2026-09-16", 2)), RATE_LIMIT: kv });
  for (const minute of ["11:45", "11:46", "11:59", "12:00"]) {
    await recordSnapshot(e, at(`2026-09-16T${minute}:00Z`), okFeed());
  }
  assert.equal(e.SNAPSHOTS.lists.length, 0, "the bucket was measured on an ordinary minute");
  await recordSnapshot(e, at("2026-09-16T12:07:00Z"), okFeed());
  assert.ok(e.SNAPSHOTS.lists.length > 0, "the bucket is never measured");
  assert.equal(kv.store.size, 1);
});

test("a failed measurement does not stop the recording", async () => {
  // Losing a minute of record because a list call failed would be a worse
  // outcome than the stale measurement it was trying to refresh.
  const bucket = fakeBucket();
  bucket.list = async () => { throw new Error("R2 unavailable"); };
  const e = env({ SNAPSHOTS: bucket, RATE_LIMIT: fakeKv() });
  const r = await recordSnapshot(e, at("2026-09-16T11:07:00Z"), bothFeeds());
  assert.equal(r.recorded, true);
  assert.equal(bucket.puts.length, 2);
});

test("an unreadable budget does not stop the recording either", async () => {
  const kv = fakeKv();
  kv.get = async () => { throw new Error("KV unavailable"); };
  const e = env({ RATE_LIMIT: kv });
  const r = await recordSnapshot(e, at("2026-09-16T11:45:00Z"), okFeed());
  assert.equal(r.recorded, true);
});

test("the quiet hours are still free, budget or not", async () => {
  const e = env({ RATE_LIMIT: fakeKv() });
  const r = await recordSnapshot(e, at("2026-09-17T02:00:00Z"), okFeed());
  assert.equal(r.reason, "quiet_hours");
  assert.equal(e.SNAPSHOTS.lists.length, 0);
});


// ── Both feeds, one budget ──────────────────────────────────
//
// The recorder writes two objects a minute now. A prune that knew only about
// the first prefix would leave the second growing unmeasured — invisible to
// the budget that exists to keep R2 free, and visible only on a bill.

test("both feeds are pruned when they expire", async () => {
  const e = env({
    SNAPSHOTS: fakeBucket([
      ...dayOf("2026-09-01", 3),                       // old positions
      ...dayOf("2026-09-01", 3, 150 * 1024, "rt", "pb"),   // old journeys
      ...dayOf("2026-09-16", 3),                       // today's positions
      ...dayOf("2026-09-16", 3, 150 * 1024, "rt", "pb"),   // today's journeys
    ]),
    RATE_LIMIT: fakeKv(),
  });
  const r = await pruneAndMeasure(e, at("2026-09-16T11:07:00Z"));
  assert.equal(r.deleted, 6, "an expired feed survived the prune");
  assert.ok(e.SNAPSHOTS.deleted.some(k => k.startsWith("rt/")),
    "the GTFS-RT objects were left behind to accumulate");
  assert.ok(e.SNAPSHOTS.held.every(o => keyDay(o.key) === "2026-09-16"));
});

test("both feeds count against the storage budget", async () => {
  const e = env({
    SNAPSHOTS: fakeBucket([
      ...dayOf("2026-09-16", 2, 300 * 1024),
      ...dayOf("2026-09-16", 2, 150 * 1024, "rt", "pb"),
    ]),
    RATE_LIMIT: fakeKv(),
  });
  const r = await pruneAndMeasure(e, at("2026-09-16T11:07:00Z"));
  assert.equal(r.objects, 4, "a feed was left out of the count");
  assert.equal(r.bytes, 2 * 300 * 1024 + 2 * 150 * 1024,
    "the bytes the budget is judged on do not include both feeds");
});

test("a week of two feeds still fits the byte budget", () => {
  // Measured: 285 KB of SIRI XML and 34 KB of GTFS-RT a minute, over the
  // 19.5-hour recording window, is about 330 MB a day. The budget has to hold
  // the whole retention window with room to spare.
  const dailyBytes = 330 * 1024 * 1024;
  assert.ok(_internals.RETENTION_DAYS * dailyBytes < _internals.MAX_STORED_BYTES * 0.8,
    `${_internals.RETENTION_DAYS} days at 540 MB does not fit the budget`);
});

test("a key says which day it belongs to, whichever feed it is", () => {
  assert.equal(keyDay("raw/2026-09-18/0740.xml"), "2026-09-18");
  assert.equal(keyDay("rt/2026-09-18/0740.pb"), "2026-09-18");
  assert.equal(keyDay("stray-object"), "", "an unrecognised key must not read as a date");
});


test("an error page from the second feed is not stored as journeys", async () => {
  // BODS answers 403 for a bad key and 429 when throttled, both with a body.
  // Storing either as a snapshot would put an HTML error page in the record
  // and leave the processor to discover it a day later.
  const e = env();
  let call = 0;
  const r = await recordSnapshot(e, at("2026-09-16T11:45:00Z"), async () => {
    call += 1;
    return call === 1
      ? new Response("<html>Forbidden</html>", { status: 403 })
      : new Response("<Siri/>", { status: 200 });
  });
  assert.equal(r.rt.stored, false);
  assert.equal(r.rt.reason, "http_403");
  assert.equal(e.SNAPSHOTS.puts.length, 1, "an error page was stored");
  assert.ok(put(e, "raw/"), "the positions were lost with it");
});
