/**
 * Tests for the Route view's service control and the map's stop bubbles.
 *
 * Both are about the same thing: the map is the point of this view, and two
 * separate pieces of the interface were covering it — a chip list that took
 * the whole tab, and a curtain of cluster bubbles at the zooms where
 * clustering first applies.
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
const INDEX = readFileSync(join(ROOT, "index.html"), "utf8");

// ── The service list lives in the control that filters it ───

test("the service chips are inside the disclosure, not above it", () => {
  // Three stacked blocks — a chip list, a checkbox and a separate "Filter
  // services" disclosure — took the whole tab on a phone and pushed the prose
  // the tab is about below the fold.
  const disc = INDEX.slice(INDEX.indexOf('id="route-filters-disclosure"'));
  const body = disc.slice(0, disc.indexOf("</details>"));
  for (const id of ["route-filter-chips", "show-limited-services",
                    "service-operator-toggle", "routes-all-btn"]) {
    // The full attribute, not the bare id: "route-filter-chips" is a prefix of
    // "route-filter-chipsX", so a substring search called a renamed element
    // present and this test passed with the list moved back out.
    assert.ok(body.includes(`id="${id}"`) || body.includes(`for="${id}"`),
      `${id} is still outside the service control`);
  }
});

test("the closed control can say how many services are on the map", () => {
  assert.match(INDEX, /id="route-filters-count"/,
    "collapsed, nothing says how much of the network is drawn");
});

test("the expander is a real button with a described state", () => {
  const m = /<button[^>]*id="route-chips-more"[^>]*>/.exec(INDEX);
  assert.ok(m, "no expander for the clipped services");
  assert.match(m[0], /aria-expanded="false"/, "the expander declares no state");
  assert.match(m[0], /aria-controls="route-filter-chips"/,
    "the expander is not tied to the list it expands");
});

// ── Cluster density ─────────────────────────────────────────

const cellPx  = (z) => vm.runInContext(`clusterCellPixels(${z})`, app);
const cellDeg = (z) => vm.runInContext(`clusterCellDegrees(${z})`, app);
const ZOOM    = vm.runInContext("STOP_ZOOM_INDIVIDUAL", app);

test("bubbles are spread furthest apart where clustering first applies", () => {
  // A flat 60px cell put a bubble every 60px across the coast. The two zooms
  // just below the individual-stop threshold are where that was worst, and
  // they are the ones a reader actually looks at.
  assert.ok(cellPx(ZOOM - 1) > 60 * 2,
    `the first clustered zoom uses a ${cellPx(ZOOM - 1)}px cell — barely wider than the 60px it replaced`);
  assert.ok(cellPx(ZOOM - 2) > 60 * 1.5,
    `the second clustered zoom uses a ${cellPx(ZOOM - 2)}px cell`);
  assert.ok(cellPx(ZOOM - 1) >= cellPx(ZOOM - 2),
    "the closest clustered zoom is not the most spread out, which is backwards");
});

test("a cell still shrinks in degrees as you zoom in", () => {
  // Whatever the pixel size, the geographic cell must fall with zoom or the
  // bubbles stop tracking the map underneath them.
  for (let z = 9; z < ZOOM; z++) {
    assert.ok(cellDeg(z) > cellDeg(z + 1),
      `the cell at zoom ${z} is not larger than at ${z + 1}`);
  }
});

test("a bubble's size says roughly how much it is standing for", () => {
  const d = (n) => vm.runInContext(`clusterDiameter(${n})`, app);
  assert.ok(d(200) > d(4), "a bubble of 200 stops looks the same as one of 4");
  // The bounds are the point of this one: below 12px the disc stops reading as
  // a control at all, and above 22px it is back to being the loudest thing on
  // the map, which is what the resize was for.
  // The floor is a guard on the scale factor, not on the counts: at 0.35 the
  // formula already returns 14px for a single stop. Asserting d(1) >= 12 was
  // therefore blind — it passed with the clamp deleted. This binds on it.
  assert.ok(d(0) >= 12, `the clamp does not hold: a degenerate bucket is ${d(0)}px`);
  assert.ok(d(1) >= 14, `the smallest real bubble is ${d(1)}px — too small to see`);
  assert.ok(d(5000) <= 22,
    `an unbounded bubble reached ${d(5000)}px and becomes an obstacle itself`);
  // Monotonic, or the size is noise rather than information.
  let prev = 0;
  for (const n of [1, 5, 20, 100, 400]) {
    const cur = d(n);
    assert.ok(cur >= prev, `size fell from ${prev} to ${cur} as the count rose`);
    prev = cur;
  }
});

// ── One prioritised list ────────────────────────────────────
//
// Every service is eligible for the chip list. Limited services go after the
// frequent ones on the default view, so they only reach the visible rows when a
// filter frees them up; ticking "Show limited services" makes it one numeric
// order. A limited variant stays with its frequent base.

function routeState(over = {}) {
  const st = vm.runInContext("state", app);
  const routes = [
    ["1", "BHBC", true], ["2", "BHBC", true], ["2B", "BHBC", false],
    ["10", "SCSO", true], ["13", "SCSO", false], ["106", "SCSO", false],
    ["700", "SCSO", true], ["60", "BHBC", false], ["N700", "SCSO", false],
  ];
  st.routeLines = routes.map(([service]) => ({ service }));
  st.routeOperatorByService = Object.fromEntries(routes.map(([s, op]) => [s, op]));
  st.routeFrequencyByService = Object.fromEntries(
    routes.map(([s, , f]) => [s, { is_frequent_all_day: f }]));
  st.serviceMode = "day";
  st.visibleCategories = new Set(["all"]);
  st.visibleOperators = new Set(["BHBC", "SCSO"]);
  st.showLimitedServices = false;
  Object.assign(st, over);
  return st;
}

const bases = () => JSON.parse(JSON.stringify(
  vm.runInContext("orderedRouteGroups()", app))).map(g => g.base);

test("on the default view, every limited service sorts after every frequent one", () => {
  routeState();
  assert.deepEqual(bases(), ["1", "2", "10", "700", "13", "60", "106"],
    "limited services are mixed in with frequent ones, so they take the visible rows");
});

test("a limited variant stays with its frequent base", () => {
  routeState();
  const groups = JSON.parse(JSON.stringify(vm.runInContext("orderedRouteGroups()", app)));
  const two = groups.find(g => g.base === "2");
  assert.deepEqual(two.variants, ["2", "2B"], "2B was split off from the 2");
  assert.equal(two.frequent, true, "2/2B was filed as limited because 2B is");
});

test("limited services are still eligible, not filtered out", () => {
  // They used to be excluded from the list entirely unless the box was ticked,
  // so narrowing the filters could never bring them into view.
  routeState({ visibleOperators: new Set(["SCSO"]) });
  assert.deepEqual(bases(), ["10", "700", "13", "106"],
    "filtering to one operator did not bring its limited services into the list");
});

test("with limited services shown, the list is one numeric order", () => {
  routeState({ showLimitedServices: true });
  assert.deepEqual(bases(), ["1", "2", "10", "13", "60", "106", "700"]);
});

test("night services are not treated as limited", () => {
  routeState({ serviceMode: "night" });
  const groups = JSON.parse(JSON.stringify(vm.runInContext("orderedRouteGroups()", app)));
  assert.deepEqual(groups.map(g => [g.base, g.frequent]), [["N700", true]]);
});
