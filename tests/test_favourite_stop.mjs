/**
 * Saved stops (up to eight per device) and recent stops, shown on arrival in
 * the home sheet with their next buses. They replaced one favourite that
 * opened by itself (6 November 2026); that favourite moves across once.
 *
 * Storage is a real Map here, and one variant throws on every call, because
 * a private window or blocked site data is a normal way to visit and the
 * site has to work the same without it.
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
const KEY = "favourite-stop";

function loadApp({ blocked = false, saved = null, extra = {} } = {}) {
  const noop = () => {};
  const stubEl = new Proxy({}, {
    get(_t, prop) {
      if (prop === "classList") return { add: noop, remove: noop, toggle: noop, contains: () => false };
      if (prop === "dataset" || prop === "style") return {};
      if (prop === "querySelector" || prop === "closest") return () => stubEl;
      if (prop === "querySelectorAll") return () => [];
      if (prop === "value" || prop === "textContent" || prop === "innerHTML") return "";
      if (typeof prop === "string" && /^(add|remove)EventListener$|^(set|remove|get)Attribute$|^(append|remove|focus|reset|scroll\w*)/.test(prop)) return noop;
      return undefined;
    },
    set() { return true; },
  });
  const store = new Map(saved ? [[KEY, JSON.stringify(saved)]] : []);
  for (const [k, v] of Object.entries(extra)) store.set(k, JSON.stringify(v));
  const boom = () => { throw new Error("SecurityError: storage is blocked"); };
  const localStorage = blocked
    ? { getItem: boom, setItem: boom, removeItem: boom }
    : { getItem: (k) => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)), removeItem: (k) => store.delete(k) };
  const button = { hidden: true, attrs: {}, title: "", setAttribute(k, v) { this.attrs[k] = String(v); }, addEventListener: noop };
  const toasts = [];
  const sandbox = {
    document: {
      getElementById: (id) => (id === "favourite-stop-btn" ? button : stubEl),
      createElement: () => stubEl, addEventListener: noop, querySelector: () => stubEl,
      querySelectorAll: () => [], head: stubEl,
      body: { classList: { add: noop, remove: noop, toggle: noop, contains: () => false }, dataset: {}, addEventListener: noop, appendChild: noop, setAttribute: noop },
    },
    addEventListener: noop, innerWidth: 390, innerHeight: 844,
    navigator: { userAgent: "node" },
    location: { pathname: "/", search: "", hash: "" },
    history: { pushState: noop, replaceState: noop },
    localStorage, console, URLSearchParams,
    fetch: async () => ({ ok: false, status: 500, json: async () => ({}) }),
    setTimeout: (...a) => { const t = setTimeout(...a); t.unref?.(); return t; },
    setInterval: (...a) => { const t = setInterval(...a); t.unref?.(); return t; },
    clearTimeout, clearInterval,
    L: new Proxy({}, { get: () => () => stubEl }),
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  const ctx = vm.createContext(sandbox);
  vm.runInContext(readFileSync(join(ROOT, "app.js"), "utf8"), ctx, { filename: "app.js" });
  vm.runInContext(`showToast = (msg) => globalThis.__toasts.push(msg);`, Object.assign(ctx, { __toasts: toasts }));
  return { ctx, store, button, toasts };
}

const select = (ctx, atco, name) =>
  vm.runInContext(`state.selectedStop = { atcoCode: ${JSON.stringify(atco)}, stopName: ${JSON.stringify(name)} };`, ctx);
const plain = (v) => JSON.parse(JSON.stringify(v));

test("nothing saved: nothing opens by itself, and the home sheet says how to save a stop", () => {
  const { ctx } = loadApp();
  assert.deepEqual(plain(ctx.readSavedStops()), []);
  assert.equal(typeof ctx.startupFavouriteStop, "undefined", "a saved stop still opens by itself");
  assert.match(ctx.homeSheetHtml([], []), /press the star to save it/);
});

test("the one favourite from before moves into the saved stops, once", () => {
  const app = loadApp({ saved: { atco: "1490A", name: "Shoreham High Street" } });
  assert.deepEqual(plain(app.ctx.readSavedStops()), [{ atco: "1490A", name: "Shoreham High Street" }]);
  assert.equal(app.store.has(KEY), false, "the old key was left behind");
  assert.deepEqual(JSON.parse(app.store.get("saved-stops")), [{ atco: "1490A", name: "Shoreham High Street" }]);
});

test("the star saves the open stop beside the others, and pressing it again removes it", () => {
  const app = loadApp({ extra: { "saved-stops": [{ atco: "1490B", name: "Ham Road" }] } });
  select(app.ctx, "1490A", "Shoreham High Street");
  app.ctx.toggleFavouriteStop();
  assert.deepEqual(JSON.parse(app.store.get("saved-stops")).map(s => s.atco), ["1490A", "1490B"]);
  assert.equal(app.button.attrs["aria-pressed"], "true");
  app.ctx.toggleFavouriteStop();
  assert.deepEqual(JSON.parse(app.store.get("saved-stops")).map(s => s.atco), ["1490B"]);
  assert.equal(app.button.attrs["aria-pressed"], "false");
});

test("no more than eight saved stops, and the reader is told", () => {
  const eight = Array.from({ length: 8 }, (_, i) => ({ atco: `S${i}`, name: `Stop ${i}` }));
  const app = loadApp({ extra: { "saved-stops": eight } });
  select(app.ctx, "1490A", "Shoreham High Street");
  app.ctx.toggleFavouriteStop();
  assert.equal(JSON.parse(app.store.get("saved-stops")).length, 8);
  assert.match(app.toasts.at(-1), /8 saved stops/);
});

test("recent stops: newest first, no repeats, five at most", () => {
  const app = loadApp();
  for (const a of ["A", "B", "C", "A", "D", "E", "F"]) app.ctx.rememberRecentStop(a, `Stop ${a}`);
  assert.deepEqual(plain(app.ctx.readRecentStops()).map(s => s.atco), ["F", "E", "D", "A", "C"]);
});

test("the home sheet gives each saved stop its next buses, escaped, and leaves saved stops out of Recent", () => {
  const { ctx } = loadApp();
  const html = ctx.homeSheetHtml(
    [{ atco: "1490A", name: "High <b>Street</b>" }],
    [{ atco: "1490A", name: "High Street" }, { atco: "1490B", name: "Ham Road" }],
    { "1490A": [{ service: "700", destination: "brighton", aimed_departure: "2026-11-06T12:05:00+00:00" }] });
  assert.doesNotMatch(html, /<b>Street/);
  assert.match(html, /<strong>700<\/strong> 12:05 to Brighton/);
  assert.match(html, /timetable/);
  assert.equal((html.match(/data-home-stop="1490A"/g) || []).length, 1, "a saved stop is listed again under Recent");
  assert.match(html, /data-home-stop="1490B"/);
});

test("blocked storage: nothing throws, nothing is claimed, and the reader is told", () => {
  const app = loadApp({ blocked: true });
  assert.deepEqual(plain(app.ctx.readSavedStops()), []);
  select(app.ctx, "1490A", "Shoreham High Street");
  assert.doesNotThrow(() => app.ctx.toggleFavouriteStop());
  assert.doesNotThrow(() => app.ctx.rememberRecentStop("1490A", "x"));
  assert.equal(app.button.attrs["aria-pressed"], "false", "the star must not claim a save that failed");
  assert.match(app.toasts.at(-1), /can't be kept/);
});
