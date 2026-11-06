/** Discover (discover.js): the places a bus reaches directly, nearest door to
 *  door first, each from its best stop; and everything escaped. */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { loadApp, ROOT } from "./load_app.mjs";

const app = loadApp();
vm.runInContext(readFileSync(join(ROOT, "discover.js"), "utf8"), app);
const plain = (v) => JSON.parse(JSON.stringify(v));

const PLACES = [
  { id: "far", name: "Far Fort", category: "History", line: "A fort.", source_url: "https://far.example/",
    stops: [{ atco: "S1", name: "Fort Road", metres: 160 }] },
  { id: "near", name: "Near Museum", category: "Museum", line: "A museum.", source_url: "https://near.example/",
    stops: [{ atco: "S2", name: "High Street", metres: 800 }, { atco: "S3", name: "Museum Gate", metres: 80 }] },
  { id: "none", name: "Unreached", category: "Park", line: "A park.", source_url: "https://none.example/",
    stops: [{ atco: "S9", name: "Lane", metres: 50 }] },
];

test("places a bus reaches directly come nearest door to door first, each from its best stop", () => {
  const reach = { S1: { ride_mins: 30, service: "700" }, S2: { ride_mins: 5, service: "2" }, S3: { ride_mins: 12, service: "2" } };
  const rows = plain(app.discoverReachable(PLACES, reach));
  assert.deepEqual(rows.map(r => r.place.id), ["near", "far"], "a place no bus reaches was listed, or the order is wrong");
  // Museum Gate: 12 min ride and a 1 min walk beats High Street: 5 min and a 10 min walk.
  assert.equal(rows[0].stop.atco, "S3");
  assert.equal(rows[0].total, 13);
});

test("the results say the bus, the time, the ride and the walk, and escape what they quote", () => {
  const evil = [{ ...PLACES[0], name: "<img src=x onerror=alert(1)>", source_url: "javascript:alert(1)" }];
  const rows = app.discoverReachable(evil, { S1: { ride_mins: 30, service: "700", next: "2026-11-06T10:05:00+00:00",
    trips: 12, from: "X" } });
  const html = app.discoverResultsHtml(rows, "where you are");
  assert.doesNotMatch(html, /<img src=x|href="javascript:/i);
  assert.match(html, /<strong>700<\/strong>/);
  assert.match(html, /10:05/);
  assert.match(html, /30 min to Fort Road,\s+then a 2 min walk/);
  assert.match(html, /not live times/);
  assert.match(app.discoverResultsHtml([], "Ham Road"), /No place on this list is a direct bus ride from Ham Road/);
});
