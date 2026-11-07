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

test("a walk given by hand beats the straight line (Highdown's hill)", () => {
  const places = [{ ...PLACES[0], stops: [{ atco: "S1", name: "The Strand", metres: 875, walk_mins: 20 }] }];
  const rows = plain(app.discoverReachable(places, { S1: { ride_mins: 10, service: "700" } }));
  assert.equal(rows[0].walk, 20, "the 11 minutes a straight line gives was used");
  assert.equal(rows[0].total, 30);
});

test("with a start chosen, each place is listed once: reached first, then the rest", () => {
  const html = vm.runInContext(`
    disc.data = { places: ${JSON.stringify(PLACES)} };
    disc.reach = { S1: { ride_mins: 30, service: "700", next: "2026-11-06T10:05:00+00:00", trips: 3, from: "X" } };
    disc.from = { atcos: ["X"], label: "Ham Road" };
    disc.open = null;
    discoverPanelHtml();`, app);
  for (const id of ["far", "near", "none"]) {
    assert.equal(html.split(`id="disc-${id}"`).length - 1, 1, `${id} is not listed exactly once`);
  }
  assert.ok(html.indexOf('id="disc-far"') < html.indexOf("A change of bus or more from Ham Road"),
    "the place a bus reaches is not listed first");
  assert.ok(html.indexOf("A change of bus or more") < html.indexOf('id="disc-none"'));
  // Folded until opened, and says so to a screen reader.
  assert.match(html, /aria-expanded="false" aria-controls="disc-far-more"/);
  assert.match(html, /id="disc-far-more" hidden/);
});

test("a place's photo carries its credit, escaped", () => {
  const photo = { src: "images/discover/x.jpg", thumb: "images/discover/x-thumb.jpg", alt: 'A "pier"',
    author: "<b>Ann</b>", licence: "CC BY-SA 4.0", licence_url: "javascript:alert(1)",
    source_url: "https://commons.wikimedia.org/wiki/File:X.jpg" };
  const html = app.discoverPlaceHtml({ ...PLACES[0], about: "More.", photo });
  assert.match(html, /alt="A &quot;pier&quot;"/);
  assert.match(html, /Photo: &lt;b&gt;Ann&lt;\/b&gt;/);
  assert.doesNotMatch(html, /href="javascript:/i);
  assert.match(html, /via <a href="https:\/\/commons\.wikimedia\.org\/wiki\/File:X\.jpg"/);
  assert.match(html, /class="disc-thumb" src="images\/discover\/x-thumb\.jpg" alt=""/);
});
