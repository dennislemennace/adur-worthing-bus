/**
 * Regression tests for the panel-collapse state.
 *
 * The bug: `panel-collapsed` is a class on <body>, so it outlived the view
 * that set it. Collapsing the panel in Route view and switching to Ticket view
 * hid the incoming view's content too — and Ticket view had no collapse
 * control, so nothing on screen could undo it. The boundary calculator became
 * unreachable until a page reload. Crossing the 700px breakpoint while
 * collapsed stranded it the same way, because the control is mobile-only.
 *
 * Unlike test_boundary_calc.mjs, the DOM stub here gives <body> a REAL
 * classList backed by a Set. A stubbed `contains: () => false` would make
 * every assertion below pass while the bug survived.
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

function makeClassList(set) {
  return {
    add:      (c) => set.add(c),
    remove:   (c) => set.delete(c),
    contains: (c) => set.has(c),
    toggle:   (c, force) => {
      const on = force === undefined ? !set.has(c) : !!force;
      if (on) set.add(c); else set.delete(c);
      return on;
    },
  };
}

function loadApp() {
  const noop = () => {};
  const bodyClasses = new Set();

  // Stand-ins for the collapse buttons; they record what was set on them.
  const buttons = [0, 1, 2, 3].map(() => ({
    attrs: {},
    setAttribute(k, v) { this.attrs[k] = v; },
    getAttribute(k) { return this.attrs[k]; },
    addEventListener: noop,
  }));

  const stubEl = new Proxy({}, {
    get(_t, prop) {
      if (prop === "classList") return makeClassList(new Set());
      if (prop === "dataset") return {};
      if (prop === "style") return {};
      if (prop === "addEventListener") return noop;
      if (prop === "querySelector" || prop === "closest") return () => stubEl;
      if (prop === "querySelectorAll") return () => [];
      if (prop === "appendChild" || prop === "remove") return noop;
      if (prop === "setAttribute" || prop === "removeAttribute") return noop;
      if (prop === "focus" || prop === "reset") return noop;
      if (prop === "value" || prop === "textContent" || prop === "innerHTML") return "";
      return undefined;
    },
    set() { return true; },
  });

  const body = {
    classList: makeClassList(bodyClasses),
    dataset: {},
    addEventListener: noop,
    appendChild: noop,
    setAttribute: noop,
  };

  const sandbox = {
    document: {
      getElementById: () => stubEl,
      createElement: () => stubEl,
      addEventListener: noop,
      querySelector: () => stubEl,
      querySelectorAll: (sel) =>
        sel === ".btn-collapse-panel" ? buttons : [],
      head: stubEl,
      body,
    },
    window: { addEventListener: noop, innerWidth: 390 },
    navigator: { userAgent: "node" },
    location: { hash: "", search: "" },
    localStorage: { getItem: () => null, setItem: noop, removeItem: noop },
    console,
    fetch: async () => ({ ok: false, status: 500, json: async () => ({}) }),
    setTimeout, clearTimeout, setInterval, clearInterval,
    requestAnimationFrame: (fn) => setTimeout(fn, 0),
    L: new Proxy({}, { get: () => () => stubEl }),
  };
  sandbox.globalThis = sandbox;

  const code = readFileSync(join(ROOT, "app.js"), "utf8");
  const ctx = vm.createContext(sandbox);
  vm.runInContext(code, ctx, { filename: "app.js" });
  return { ctx, bodyClasses, buttons };
}

const COLLAPSED = "panel-collapsed";

// ── The primitive ───────────────────────────────────────────

test("toggling sets and clears the body class", () => {
  const { ctx, bodyClasses } = loadApp();
  ctx.togglePanelCollapsed();
  assert.ok(bodyClasses.has(COLLAPSED), "should collapse");
  ctx.togglePanelCollapsed();
  assert.ok(!bodyClasses.has(COLLAPSED), "should expand again");
});

test("every collapse button follows the state for screen readers", () => {
  const { ctx, buttons } = loadApp();
  ctx.togglePanelCollapsed();
  for (const b of buttons) {
    assert.equal(b.getAttribute("aria-pressed"), "true");
    assert.equal(b.getAttribute("aria-label"), "Show panel");
  }
  ctx.togglePanelCollapsed();
  for (const b of buttons) {
    assert.equal(b.getAttribute("aria-pressed"), "false");
    assert.equal(b.getAttribute("aria-label"), "Hide panel");
  }
});

test("setPanelCollapsed is idempotent", () => {
  const { ctx, bodyClasses } = loadApp();
  ctx.setPanelCollapsed(true);
  ctx.setPanelCollapsed(true);
  assert.ok(bodyClasses.has(COLLAPSED));
  ctx.setPanelCollapsed(false);
  ctx.setPanelCollapsed(false);
  assert.ok(!bodyClasses.has(COLLAPSED));
});

// ── The bug ─────────────────────────────────────────────────

test("switching view clears a collapsed panel", async () => {
  // This is the reported bug: collapse in Route view, switch to Ticket view,
  // and the whole view was display:none with no control to bring it back.
  const { ctx, bodyClasses } = loadApp();
  ctx.setPanelCollapsed(true);
  assert.ok(bodyClasses.has(COLLAPSED), "precondition: collapsed");

  vm.runInContext('state.viewMode = "tickets"', ctx);
  vm.runInContext('state.map = { closePopup() {}, removeLayer() {}, addLayer() {} }', ctx);

  // The reset is the first statement in applyViewMode, so it has already run
  // by the time the sandbox runs out of Leaflet. What happens after is this
  // test's business only insofar as it must not undo the reset.
  try {
    await ctx.applyViewMode();
  } catch (err) {
    // Expected: the stub DOM/map cannot satisfy the whole view switch.
  }

  assert.ok(
    !bodyClasses.has(COLLAPSED),
    "the incoming view must not inherit the previous view's collapsed state",
  );
});

test("crossing above the mobile breakpoint clears a collapsed panel", () => {
  // The control is display:none above the breakpoint, so staying collapsed
  // past it strands the panel with nothing on screen to reopen it.
  const { ctx, bodyClasses } = loadApp();
  ctx.setPanelCollapsed(true);

  ctx.window.innerWidth = 1200;
  ctx.syncPanelCollapsedToWidth();

  assert.ok(!bodyClasses.has(COLLAPSED), "should expand when the control vanishes");
});

test("staying under the breakpoint leaves the panel collapsed", () => {
  // The gesture must still work on the phones it exists for.
  const { ctx, bodyClasses } = loadApp();
  ctx.setPanelCollapsed(true);

  ctx.window.innerWidth = 390;
  ctx.syncPanelCollapsedToWidth();

  assert.ok(bodyClasses.has(COLLAPSED), "resizing on a phone must not undo the user");
});


/* `state` is a const in app.js, so it never becomes a property of the vm
   global — it has to be read from inside the context, not off `ctx`. */
const detentOf = (ctx) => vm.runInContext("state.sheetDetent", ctx);

// ── The bottom sheet ────────────────────────────────────────
//
// The panel is now a sheet with three detents rather than a fixed slab, and
// "collapsed" is the name of its smallest one. The point of the change is
// that the smallest state is still a sheet: content stays in the layout, so
// no state can leave a view unreachable the way display:none did.

test("a detent publishes itself to the body and to state", () => {
  const { ctx } = loadApp();
  ctx.setSheetDetent("full");
  assert.equal(detentOf(ctx), "full");
  assert.equal(ctx.document.body.dataset.sheet, "full");
});

test("peek is the collapsed detent, and nothing else is", () => {
  const { ctx, bodyClasses } = loadApp();
  ctx.setSheetDetent("peek");
  assert.ok(bodyClasses.has(COLLAPSED), "peek must read as collapsed");
  for (const d of ["half", "full"]) {
    ctx.setSheetDetent(d);
    assert.ok(!bodyClasses.has(COLLAPSED), `${d} must not read as collapsed`);
  }
});

test("collapsing and expanding map onto detents", () => {
  const { ctx } = loadApp();
  ctx.setPanelCollapsed(true);
  assert.equal(detentOf(ctx), "peek");
  ctx.setPanelCollapsed(false);
  assert.equal(detentOf(ctx), "half");
});

test("cycling walks peek -> half -> full and wraps", () => {
  // This is the keyboard path: the handle is a button precisely so the sheet
  // is not pointer-only, and cycling is what Enter and Space do.
  const { ctx } = loadApp();
  ctx.setSheetDetent("peek");
  const seen = [];
  for (let i = 0; i < 4; i++) {
    ctx.cycleSheetDetent();
    seen.push(detentOf(ctx));
  }
  assert.deepEqual(seen, ["half", "full", "peek", "half"]);
});

test("an unknown detent is ignored rather than applied", () => {
  const { ctx } = loadApp();
  ctx.setSheetDetent("half");
  ctx.setSheetDetent("enormous");
  assert.equal(detentOf(ctx), "half", "must not accept a name it cannot render");
});

test("the collapse buttons still describe the state to a screen reader", () => {
  // Previously driven from setPanelCollapsed; now from the detent, so the
  // handle and the chevron cannot disagree about what the sheet is doing.
  const { ctx, buttons } = loadApp();
  ctx.setSheetDetent("peek");
  for (const b of buttons) assert.equal(b.getAttribute("aria-label"), "Show panel");
  ctx.setSheetDetent("full");
  for (const b of buttons) assert.equal(b.getAttribute("aria-label"), "Hide panel");
});
