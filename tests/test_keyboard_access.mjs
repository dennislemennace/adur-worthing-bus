/**
 * Tests for the routes that do not involve pointing at a map.
 *
 * The site's primary task — see when the next bus leaves — could only be
 * started by clicking a marker. Markers are not focusable, cluster targets
 * measured 34px, and rail markers were created with `keyboard: false`, so a
 * keyboard user had no way in at all. The browser harness's target-size scan
 * excludes the map, so it passed throughout.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { loadApp, ROOT } from "./load_app.mjs";

const app = loadApp();
const state = vm.runInContext("state", app);
const INDEX = readFileSync(join(ROOT, "index.html"), "utf8");

state.stopData = {
  "14900000001": { name: "Marine Parade", lat: 50.8095, lon: -0.3730 },
  "14900000002": { name: "Church Place St Mary's Hall", lat: 50.8100, lon: -0.3740 },
};
state.railStations = [{ crs: "WRH", name: "Worthing" }];

// ── There is a way in that is not the map ───────────────────

test("the live view offers a search, not only a map", () => {
  assert.match(INDEX, /id="stop-search-input"/,
    "no stop search exists, so the map is still the only route in");
  assert.match(INDEX, /<label[^>]*for="stop-search-input"/,
    "the search box has no label");
});

test("searching finds a stop by name", () => {
  const hits = app.stopSearchMatches("marine");
  assert.ok(hits.length >= 1, "no match for a stop that exists");
  assert.equal(hits[0].kind, "stop");
  assert.ok(hits[0].atco, "a result with no stop to open");
});

test("searching finds a rail station too", () => {
  const hits = app.stopSearchMatches("worthing");
  assert.ok(hits.some(h => h.kind === "rail" && h.crs === "WRH"),
    "rail stations are unreachable by keyboard for the same reason stops were");
});

/** Arrays built inside the vm come from another realm, so deepStrictEqual
 *  rejects them on prototype identity alone. Same helper the other suites use. */
const plain = (v) => JSON.parse(JSON.stringify(v));

test("a single letter does not dump the whole network into the list", () => {
  assert.deepEqual(plain(app.stopSearchMatches("m")), []);
  assert.deepEqual(plain(app.stopSearchMatches("")), []);
});

test("results are buttons, so Tab reaches them and Enter activates them", () => {
  let html = "";
  const listEl = { set innerHTML(v) { html = v; }, get innerHTML() { return html; } };
  app.renderStopSearchResults(listEl, app.stopSearchMatches("marine"), "marine");
  assert.match(html, /<button[^>]*type="button"[^>]*class="stop-search-result"/,
    "results are not focusable controls");
});

test("a name with an apostrophe survives into the results", () => {
  let html = "";
  const listEl = { set innerHTML(v) { html = v; }, get innerHTML() { return html; } };
  app.renderStopSearchResults(listEl, app.stopSearchMatches("mary"), "mary");
  assert.ok(!html.includes("\\'"), "a JavaScript escape reached the markup: " + html);
});

// ── Row actions ─────────────────────────────────────────────

test("a departure row's action is a real button", () => {
  // It was a click listener on a <tr>: not focusable, and Enter does nothing.
  const html = app.buildDepartureRow({
    service: "700", destination: "Brighton",
    aimed_departure: "2026-09-08T12:05:00+01:00",
    expected_departure: null, status: "Scheduled", delay_seconds: null,
  });
  assert.match(html, /<button[^>]*class="service-badge-btn"/,
    "the row's only action has no keyboard route");
  assert.match(html, /aria-label="Show service 700 on the map"/,
    "the button is a coloured badge with no accessible name");
});
