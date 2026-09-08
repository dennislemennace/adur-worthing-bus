/**
 * Loads app.js into a vm context with just enough DOM stubbed to survive the
 * `dom` bag it builds at load time.
 *
 * app.js is a browser script with no module exports — no build step, by design
 * — so this is the only way to get at its pure functions from node. Extracted
 * here rather than copied into each test file: the stub is fiddly enough that
 * two divergent copies would eventually disagree about what a DOM is.
 */

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import vm from "node:vm";

export const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

export function loadApp() {
  const noop = () => {};
  const stubEl = new Proxy({}, {
    get(_t, prop) {
      if (prop === "classList") return { add: noop, remove: noop, toggle: noop, contains: () => false };
      if (prop === "dataset") return {};
      if (prop === "style") return {};
      if (prop === "addEventListener") return noop;
      if (prop === "querySelector" || prop === "closest") return () => stubEl;
      if (prop === "querySelectorAll") return () => [];
      if (prop === "appendChild") return noop;
      if (prop === "setAttribute" || prop === "removeAttribute") return noop;
      if (prop === "focus" || prop === "reset") return noop;
      if (prop === "value" || prop === "textContent" || prop === "innerHTML") return "";
      return undefined;
    },
    set() { return true; },
  });

  const sandbox = {
    document: {
      getElementById: () => stubEl,
      createElement: () => stubEl,
      addEventListener: noop,
      querySelector: () => stubEl,
      querySelectorAll: () => [],
      head: stubEl,
      body: stubEl,
    },
    window: { addEventListener: noop },
    navigator: { userAgent: "node" },
    location: { hash: "", search: "" },
    localStorage: { getItem: () => null, setItem: noop, removeItem: noop },
    console,
    // Browsers always have these; app.js uses URL to validate link schemes.
    URL, URLSearchParams,
    fetch: async () => ({ ok: false, status: 500, json: async () => ({}) }),
    setTimeout, clearTimeout, setInterval, clearInterval,
    requestAnimationFrame: (fn) => setTimeout(fn, 0),
    // Leaflet is only touched inside map functions we don't call.
    L: new Proxy({}, { get: () => () => stubEl }),
  };
  sandbox.globalThis = sandbox;

  const code = readFileSync(join(ROOT, "app.js"), "utf8");
  const ctx = vm.createContext(sandbox);
  vm.runInContext(code, ctx, { filename: "app.js" });
  return ctx;
}
