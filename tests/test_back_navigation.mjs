/**
 * Back walks back through what the reader opened.
 *
 * The bug (27 October 2026): from the live map, tap a stop, then a bus on its
 * board, then Back. Both openings rewrote the address in place, so the site
 * was one page in the history and Back left it. Back should return to the
 * stop's board, and Back again to the map with nothing open.
 *
 * The history and location here are a real stack, so a replaceState where a
 * pushState was needed shows up as a missing page rather than passing quietly.
 * Back is applied the way the popstate handler applies it:
 * applyUrlState(parseUrlState()).
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import vm from "node:vm";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

function makeHistory() {
  const location = { pathname: "/", search: "", hash: "" };
  const stack = [""];
  let at = 0;
  const hashOf = (url) => (String(url).includes("#") ? String(url).slice(String(url).indexOf("#")) : "");
  const history = {
    pushes: 0,
    pushState(_s, _t, url) { stack.splice(at + 1); stack.push(hashOf(url)); at++; location.hash = stack[at]; this.pushes++; },
    replaceState(_s, _t, url) { stack[at] = hashOf(url); location.hash = stack[at]; },
    back() { if (at > 0) { at--; location.hash = stack[at]; } },
    get pages() { return stack.slice(0, at + 1); },
  };
  return { history, location };
}

function loadApp() {
  const noop = () => {};
  const stubEl = new Proxy({}, {
    get(_t, prop) {
      if (prop === "classList") return { add: noop, remove: noop, toggle: noop, contains: () => false };
      if (prop === "dataset" || prop === "style") return {};
      if (prop === "querySelector" || prop === "closest") return () => stubEl;
      if (prop === "querySelectorAll") return () => [];
      if (prop === "value" || prop === "textContent" || prop === "innerHTML") return "";
      if (typeof prop === "string" && /^(add|remove)EventListener$|^(set|remove|get)Attribute$|^(append|remove|focus|reset|scroll\w*|replaceChildren|insertAdjacentHTML)/.test(prop)) return noop;
      return undefined;
    },
    set() { return true; },
  });
  const { history, location } = makeHistory();
  const sandbox = {
    document: {
      getElementById: () => stubEl, createElement: () => stubEl, addEventListener: noop,
      querySelector: () => stubEl, querySelectorAll: () => [], head: stubEl,
      body: { classList: { add: noop, remove: noop, toggle: noop, contains: () => false }, dataset: {}, addEventListener: noop, appendChild: noop, setAttribute: noop },
    },
    addEventListener: noop, innerWidth: 390, innerHeight: 844,
    navigator: { userAgent: "node" }, location, history,
    localStorage: { getItem: () => null, setItem: noop, removeItem: noop },
    console, URLSearchParams, AbortController,
    fetch: async () => ({ ok: false, status: 500, json: async () => ({}) }),
    // A board and a Bus tab start tickers; unref'd, they cannot hold the
    // test run open after the assertions are done.
    setTimeout: (...a) => { const t = setTimeout(...a); t.unref?.(); return t; },
    setInterval: (...a) => { const t = setInterval(...a); t.unref?.(); return t; },
    clearTimeout, clearInterval,
    requestAnimationFrame: (fn) => { const t = setTimeout(fn, 0); t.unref?.(); return t; },
    getComputedStyle: () => ({}),
    L: new Proxy({}, { get: () => () => stubEl }),
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  const ctx = vm.createContext(sandbox);
  vm.runInContext(readFileSync(join(ROOT, "app.js"), "utf8"), ctx, { filename: "app.js" });
  vm.runInContext(`
    state.stopData = {
      "1490A": { name: "Shoreham High Street", lat: 50.83, lon: -0.27 },
      "1490B": { name: "Ham Road", lat: 50.83, lon: -0.30 },
    };
    state.map = { closePopup() {}, removeLayer() {}, addLayer() {}, panTo() {}, setView() {}, invalidateSize() {} };
  `, ctx);
  return { ctx, history, location };
}

const read = (ctx, expr) => vm.runInContext(expr, ctx);
// The stub DOM cannot carry a whole board or Bus tab. What matters here
// happens before the first thing it cannot draw.
const tolerate = async (fn) => { try { await fn(); } catch { /* stub DOM */ } };
const BUS = { vehicle_ref: "V1", line_name: "700", operator_ref: "SCSO", latitude: 50.83, longitude: -0.28 };

async function back(app) {
  app.history.back();
  await tolerate(() => app.ctx.applyUrlState(app.ctx.parseUrlState()));
}

test("stop, then a bus from its board, is two pages Back walks through", async () => {
  const app = loadApp();
  await tolerate(() => app.ctx.openDepartures("1490A", "Shoreham High Street"));
  assert.equal(app.location.hash, "#stop=1490A");
  await tolerate(() => app.ctx.openBusInfo(BUS, { fromBoard: true }));
  assert.deepEqual(app.history.pages, ["", "#stop=1490A", "#stop=1490A&bus=V1"],
    "each opening needs a page of its own, or Back leaves the site");

  await back(app);
  assert.equal(app.location.hash, "#stop=1490A", "Back lands on the stop's board");
  assert.equal(read(app.ctx, "state.selectedVehicleRef"), null, "and puts the bus down");
  assert.equal(read(app.ctx, "state.selectedStop && state.selectedStop.atcoCode"), "1490A");

  await back(app);
  assert.equal(app.location.hash, "", "Back again lands on the map");
  assert.equal(read(app.ctx, "state.selectedStop"), null, "with nothing open");
});

test("a favourite stop opens as a page over the map, so Back shows the map first", async () => {
  const app = loadApp();
  app.ctx.localStorage.getItem = (k) =>
    k === "favourite-stop" ? JSON.stringify({ atco: "1490A", name: "Shoreham High Street" }) : null;
  await tolerate(() => app.ctx.applyStartupAddress({}));
  assert.deepEqual(app.history.pages, ["", "#stop=1490A"],
    "the plain map is the first page and the favourite stop the second");

  await back(app);
  assert.equal(app.location.hash, "", "Back from the favourite lands on the map, not off the site");
  assert.equal(read(app.ctx, "state.selectedStop"), null);
});

test("a bus opened from the map is one page, and Back closes it", async () => {
  const app = loadApp();
  await tolerate(() => app.ctx.openBusInfo(BUS));
  assert.deepEqual(app.history.pages, ["", "#bus=V1"]);
  await back(app);
  assert.equal(read(app.ctx, "state.selectedVehicleRef"), null);
});

test("one stop to another (a closure notice's link) is a page; Back returns", async () => {
  const app = loadApp();
  await tolerate(() => app.ctx.openDepartures("1490A", "Shoreham High Street"));
  await tolerate(() => app.ctx.openDepartures("1490B", "Ham Road"));
  assert.deepEqual(app.history.pages, ["", "#stop=1490A", "#stop=1490B"]);
  await back(app);
  assert.equal(read(app.ctx, "state.selectedStop.atcoCode"), "1490A");
});

test("refreshing the same stop does not add a page", async () => {
  const app = loadApp();
  await tolerate(() => app.ctx.openDepartures("1490A", "Shoreham High Street"));
  await tolerate(() => app.ctx.openDepartures("1490A", "Shoreham High Street"));
  assert.equal(app.history.pushes, 1);
});

test("one action makes one page, even when it changes view and opens a stop", async () => {
  // Journey times' "next buses" and Route view's live board switch to the
  // live view and open a stop in the same breath. Two pages would make Back
  // stop at an empty map on the way.
  const app = loadApp();
  vm.runInContext(`state.selectedStop = { atcoCode: "1490A", stopName: "x" };`, app.ctx);
  app.ctx.pushUrlState({ major: true });
  vm.runInContext(`state.selectedStop = { atcoCode: "1490B", stopName: "y" };`, app.ctx);
  app.ctx.pushUrlState({ major: true });
  assert.equal(app.history.pushes, 1, "two pushes in one turn coalesce");
  await Promise.resolve();
  vm.runInContext(`state.selectedStop = { atcoCode: "1490A", stopName: "x" };`, app.ctx);
  app.ctx.pushUrlState({ major: true });
  assert.equal(app.history.pushes, 2, "the next action makes its own page");
});

test("the tour makes its own pages, so a board it opens adds none", async () => {
  const app = loadApp();
  vm.runInContext(`state.tourDemo = { step: 2, tryIndex: 0 };`, app.ctx);
  await tolerate(() => app.ctx.openDepartures("1490A", "Shoreham High Street"));
  assert.equal(app.history.pushes, 0);
});

test("closing the panel is a page, so Back reopens what was closed", async () => {
  const app = loadApp();
  await tolerate(() => app.ctx.openDepartures("1490A", "Shoreham High Street"));
  await tolerate(() => app.ctx.closePanel({ major: true }));
  assert.deepEqual(app.history.pages, ["", "#stop=1490A", ""],
    "a replace here leaves a Back that does nothing");
  await back(app);
  assert.equal(read(app.ctx, "state.selectedStop && state.selectedStop.atcoCode"), "1490A");
});
