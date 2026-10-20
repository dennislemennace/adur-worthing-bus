/**
 * The feed relay: where the recorder's requests leave from.
 *
 * From the evening of 17 October 2026 BODS (behind Amazon CloudFront) refused
 * every request the scheduled recorder made, HTTP 403 with CloudFront's
 * "request could not be satisfied" page, because Cloudflare ran the cron in
 * Singapore. The same key fetched the same URL from London. A Durable Object
 * can be placed by region and a cron cannot, so the requests go through one.
 *
 * Run with:  node --test worker/test/
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import worker from "../src/index.js";
import { FeedRelay, feedFetch, RELAY_LOCATION } from "../src/relay.js";

function fakeNamespace(calls) {
  return {
    idFromName: (name) => ({ name }),
    get: (id, options) => {
      calls.push({ id, options });
      return { fetch: async (url) => { calls.push({ url: String(url) }); return new Response("<Siri/>", { status: 200 }); } };
    },
  };
}

test("the recorder's requests go through the relay, placed in Western Europe", async () => {
  const relayCalls = [], direct = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url) => { direct.push(String(url)); return new Response("<Siri/>"); };
  const puts = [];
  const env = {
    BODS_API_KEY: "k", FEED_RELAY: fakeNamespace(relayCalls),
    SNAPSHOTS: { put: async (key) => { puts.push(key); }, list: async () => ({ objects: [], truncated: false }), delete: async () => {} },
    RATE_LIMIT: { get: async () => JSON.stringify({ bytes: 0, objects: 0, measured_at: new Date("2026-10-20T11:44:00Z").toISOString() }), put: async () => {} },
  };
  const pending = [];
  try {
    await worker.scheduled({ cron: "* * * * *", scheduledTime: new Date("2026-10-20T11:45:00Z").getTime() },
                           env, { waitUntil: (p) => pending.push(p) });
    await Promise.all(pending);
  } finally {
    globalThis.fetch = realFetch;
  }
  assert.equal(RELAY_LOCATION, "weur");
  assert.ok(relayCalls.some(c => c.options && c.options.locationHint === "weur"), "the relay was not placed");
  const fetched = relayCalls.filter(c => c.url).map(c => c.url);
  assert.equal(fetched.length, 2, "both feeds go through the relay");
  assert.ok(fetched.every(u => u.startsWith("https://data.bus-data.dft.gov.uk/")));
  assert.deepEqual(direct.filter(u => u.includes("bus-data")), [], "a feed was fetched from wherever the cron ran");
  assert.equal(puts.length, 2, "both snapshots were stored");
});

test("without a relay bound, the recorder fetches directly as before", async () => {
  const seen = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url) => { seen.push(String(url)); return new Response("x"); };
  try {
    await feedFetch({})("https://data.bus-data.dft.gov.uk/api/v1/datafeed/");
  } finally {
    globalThis.fetch = realFetch;
  }
  assert.deepEqual(seen, ["https://data.bus-data.dft.gov.uk/api/v1/datafeed/"]);
});

test("the relay fetches the feed and nothing else", async () => {
  const seen = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url) => { seen.push(String(url)); return new Response("<Siri/>"); };
  const relay = new FeedRelay({}, {});
  try {
    for (const url of ["https://example.com/", "http://data.bus-data.dft.gov.uk/api/v1/datafeed/",
                       "https://data.bus-data.dft.gov.uk.evil.example/"]) {
      assert.equal((await relay.fetch(new Request(url))).status, 400, `${url} was relayed`);
    }
    const ok = await relay.fetch(new Request("https://data.bus-data.dft.gov.uk/api/v1/datafeed/?api_key=k"));
    assert.equal(ok.status, 200);
  } finally {
    globalThis.fetch = realFetch;
  }
  assert.deepEqual(seen, ["https://data.bus-data.dft.gov.uk/api/v1/datafeed/?api_key=k"]);
});

test("wrangler.toml binds the relay as a free-plan (SQLite) Durable Object", () => {
  const toml = readFileSync(new URL("../wrangler.toml", import.meta.url), "utf8");
  assert.match(toml, /\[\[durable_objects\.bindings\]\][^[]*name\s*=\s*"FEED_RELAY"[^[]*class_name\s*=\s*"FeedRelay"/);
  assert.match(toml, /new_sqlite_classes\s*=\s*\[\s*"FeedRelay"\s*\]/);
});
