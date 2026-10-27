/**
 * A favourite stop: one per device, opened on arrival unless the address
 * asks for something else.
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

function loadApp({ blocked = false, saved = null } = {}) {
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

const STOPS = { "1490A": { name: "Shoreham High Street" }, "1490B": { name: "Ham Road" } };
const select = (ctx, atco, name) =>
  vm.runInContext(`state.selectedStop = { atcoCode: ${JSON.stringify(atco)}, stopName: ${JSON.stringify(name)} };`, ctx);

test("nothing saved: the site opens on the map as it always has", () => {
  const { ctx } = loadApp();
  assert.equal(ctx.startupFavouriteStop({}, STOPS), null);
});

test("a saved favourite opens when the address asks for nothing", () => {
  const { ctx } = loadApp({ saved: { atco: "1490A", name: "Shoreham High Street" } });
  assert.equal(ctx.startupFavouriteStop({}, STOPS), "1490A");
});

test("a link always wins over the favourite", () => {
  const { ctx } = loadApp({ saved: { atco: "1490A", name: "Shoreham High Street" } });
  assert.equal(ctx.startupFavouriteStop({ stop: "1490B" }, STOPS), null, "a shared stop");
  assert.equal(ctx.startupFavouriteStop({ view: "u" }, STOPS), null, "another section");
  assert.equal(ctx.startupFavouriteStop({ bus: "V1" }, STOPS), null, "a shared bus");
});

test("a favourite stop that no longer exists is ignored, not opened", () => {
  const { ctx } = loadApp({ saved: { atco: "1490Z", name: "Gone" } });
  assert.equal(ctx.startupFavouriteStop({}, STOPS), null);
});

test("the star saves the open stop, and pressing it again clears it", () => {
  const app = loadApp();
  select(app.ctx, "1490A", "Shoreham High Street");
  app.ctx.toggleFavouriteStop();
  assert.deepEqual(JSON.parse(app.store.get(KEY)), { atco: "1490A", name: "Shoreham High Street" });
  assert.equal(app.button.attrs["aria-pressed"], "true");
  app.ctx.toggleFavouriteStop();
  assert.equal(app.store.has(KEY), false);
  assert.equal(app.button.attrs["aria-pressed"], "false");
});

test("starring another stop replaces the favourite: one per device", () => {
  const app = loadApp({ saved: { atco: "1490A", name: "Shoreham High Street" } });
  select(app.ctx, "1490B", "Ham Road");
  app.ctx.syncFavouriteStopButton();
  assert.equal(app.button.attrs["aria-pressed"], "false", "the star is for the open stop only");
  app.ctx.toggleFavouriteStop();
  assert.equal(JSON.parse(app.store.get(KEY)).atco, "1490B");
});

test("the star is hidden when no stop is open", () => {
  const app = loadApp();
  vm.runInContext(`state.selectedStop = null;`, app.ctx);
  app.ctx.syncFavouriteStopButton();
  assert.equal(app.button.hidden, true);
  select(app.ctx, "1490A", "Shoreham High Street");
  app.ctx.syncFavouriteStopButton();
  assert.equal(app.button.hidden, false);
});

test("blocked storage: nothing throws, nothing opens, and the reader is told", () => {
  const app = loadApp({ blocked: true });
  assert.equal(app.ctx.startupFavouriteStop({}, STOPS), null);
  select(app.ctx, "1490A", "Shoreham High Street");
  assert.doesNotThrow(() => app.ctx.toggleFavouriteStop());
  assert.equal(app.button.attrs["aria-pressed"], "false", "the star must not claim a save that failed");
  assert.ok(app.toasts.some(t => /save/i.test(t)), "a save that failed says so");
});

test("the button is a real toggle with a name and a 44px target", () => {
  const html = readFileSync(join(ROOT, "index.html"), "utf8");
  const btn = html.match(/<button[^>]*id="favourite-stop-btn"[^>]*>[\s\S]*?<\/button>/);
  assert.ok(btn, "index.html needs #favourite-stop-btn in the stop header");
  assert.match(btn[0], /type="button"/);
  assert.match(btn[0], /aria-pressed="false"/);
  assert.match(btn[0], /aria-label="[^"]+"|class="visually-hidden"[^>]*>[^<]+</, "an icon alone has no name");
  const css = readFileSync(join(ROOT, "style.css"), "utf8");
  const rule = css.match(/\.btn-favourite-stop\s*\{[^}]*\}/);
  assert.ok(rule, "style.css needs a .btn-favourite-stop rule");
  assert.match(rule[0], /min-(width|height):\s*var\(--target-min\)/);
});
