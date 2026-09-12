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

// ── Activating a result, not merely rendering one ───────────
//
// The original tests here asserted that `stopSearchMatches` returned a rail
// result and that `renderStopSearchResults` emitted a button. Both were true
// while pressing that button threw `ReferenceError: selectRailStation is not
// defined` on the live site, because the handler called a function that has
// never existed. A control is not tested until it has been activated.

test("the rail search handler calls a function that exists", () => {
  const src = readFileSync(join(ROOT, "app.js"), "utf8");
  const handler = src.slice(src.indexOf('btn.dataset.kind === "rail"'));
  const called = /\b([A-Za-z_$][\w$]*)\s*\(btn\.dataset\.crs/.exec(handler);
  assert.ok(called, "no rail activation call found");
  // Plain string search, not a built regex: escaping a name into a pattern is
  // its own source of false results, and this test exists to catch a missing
  // function, not to exercise the regex engine.
  assert.ok(src.includes(`function ${called[1]}(`),
    `the rail result calls ${called[1]}(), which is not defined anywhere`);
});

test("every function the search handlers call is defined", () => {
  // The general form of the same bug: a handler naming something plausible.
  const src = readFileSync(join(ROOT, "app.js"), "utf8");
  const start = src.indexOf("function bindStopSearch()");
  const body = src.slice(start, src.indexOf("\n}", src.indexOf("list.addEventListener")));
  const ignore = new Set(["if", "for", "while", "switch", "catch", "return",
                          "function", "typeof", "closest", "querySelector",
                          "getElementById", "addEventListener", "preventDefault",
                          "focus", "click", "setAttribute", "replace"]);
  for (const m of body.matchAll(/\b([a-z][A-Za-z0-9_$]*)\s*\(/g)) {
    const name = m[1];
    if (ignore.has(name)) continue;
    // The three ways this file defines something callable. `openDepartures`
    // is the third — assigned to `window` so Leaflet popups could reach it.
    const defined = src.includes(`function ${name}(`)
                 || src.includes(`const ${name} =`)
                 || src.includes(`window.${name} =`);
    if (!defined) {
      assert.fail(`bindStopSearch calls ${name}(), which is not defined in app.js`);
    }
  }
});

// ── Mixed-mode ranking ──────────────────────────────────────

test("a railway station is not crowded out by bus stops", () => {
  // "Worthing" and "Hove" each filled all twelve slots with bus stops, so the
  // station of that name — the thing most people mean — never appeared.
  state._stopIndex = Array.from({ length: 20 }, (_, i) => ({
    label: `Some Road ${i}, Worthing`, name: `Some Road ${i}`,
    atcos: [`44000000${i}`],
  }));
  state.railStations = [{ crs: "WRH", name: "Worthing" }];

  const hits = plain(app.stopSearchMatches("worthing"));
  assert.ok(hits.some(h => h.kind === "rail"),
    "the railway station was pushed out by bus stops");
});

test("an exact name outranks a district suffix", () => {
  state._stopIndex = [
    { label: "Marine Parade, Worthing", name: "Marine Parade", atcos: ["A"] },
    { label: "Worthing Pier", name: "Worthing Pier", atcos: ["B"] },
  ];
  state.railStations = [{ crs: "WRH", name: "Worthing" }];

  const hits = plain(app.stopSearchMatches("worthing"));
  assert.equal(hits[0].name, "Worthing",
    `expected the exact match first, got ${hits[0].name}`);
  assert.ok(hits.findIndex(h => h.name === "Worthing Pier")
            < hits.findIndex(h => h.name === "Marine Parade"),
    "a district suffix outranked a name that starts with the query");
});

test("results say which mode they are", () => {
  state._stopIndex = [{ label: "Lancing Station", name: "Lancing Station", atcos: ["A"] }];
  state.railStations = [{ crs: "LAC", name: "Lancing" }];
  let html = "";
  const listEl = { set innerHTML(v) { html = v; }, get innerHTML() { return html; } };
  app.renderStopSearchResults(listEl, app.stopSearchMatches("lancing"), "lancing");
  assert.match(html, /Railway station/, "a station result does not say it is one");
  assert.match(html, /Bus stop/, "a stop result does not say it is one");
});

// ── Two poles, two directions ───────────────────────────────

test("a stop with two poles offers both, told apart by direction", () => {
  // Colebrook Road has two poles ~100m apart. The journey checker clusters
  // them on purpose — you pick a place to travel from, not a kerb — but a
  // live departure board belongs to one pole, and showing a single entry sent
  // half the people asking to the opposite side of the road.
  state.stopData = {
    "4400AD0331": { name: "Colebrook Road", lat: 50.831535, lon: -0.232766,
                    towards: "Old Steine", services: ["700", "N700"] },
    "4400AD0332": { name: "Colebrook Road", lat: 50.831372, lon: -0.231659,
                    towards: "Durrington Tesco", services: ["700", "N700"] },
  };
  state._stopIndex = null;          // rebuild from the stopData above
  state._liveSearchIndex = null;
  state.railStations = [];

  const hits = plain(app.stopSearchMatches("colebrook"));
  assert.equal(hits.length, 2, `expected both poles, got ${hits.length}`);
  const modes = hits.map(h => h.mode).sort();
  assert.match(modes[0], /towards/, "a result does not say which way it goes");
  assert.notEqual(modes[0], modes[1],
    "both poles describe themselves identically, so they cannot be told apart");
  assert.ok(modes.some(m => /Old Steine/.test(m)));
  assert.ok(modes.some(m => /Durrington Tesco/.test(m)));
});

test("a stop with one pole is still a single result", () => {
  state.stopData = {
    "4400WO0253": { name: "Marine Parade", lat: 50.8095, lon: -0.3730 },
  };
  state._stopIndex = null;
  state._liveSearchIndex = null;
  state.railStations = [];
  assert.equal(plain(app.stopSearchMatches("marine")).length, 1);
});

test("results name the services calling at the stop", () => {
  state.stopData = {
    "4400AD0331": { name: "Colebrook Road", lat: 50.831535, lon: -0.232766,
                    towards: "Old Steine", services: ["700", "N700"] },
    "4400AD0332": { name: "Colebrook Road", lat: 50.831372, lon: -0.231659,
                    towards: "Durrington Tesco", services: ["700", "N700"] },
  };
  state._stopIndex = null;
  state._liveSearchIndex = null;
  state.railStations = [];
  const hits = plain(app.stopSearchMatches("colebrook"));
  assert.ok(hits.every(h => /700/.test(h.mode)),
    "a result does not say what stops there");
});
