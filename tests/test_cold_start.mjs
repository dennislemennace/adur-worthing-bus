/**
 * Tests for the two halves of a first visit after a quiet spell.
 *
 * The backend runs on a free instance that sleeps after fifteen minutes with
 * no traffic. A measured cold start was 22.4 seconds to first byte against
 * 0.15 warm — and the client's deadline was 15 seconds, so the first person to
 * arrive was guaranteed an error, and the error said "you may be offline".
 * They were not offline. The service was asleep.
 *
 * Two changes, tested here:
 *
 *   1. the map is drawn from a stop list published with the site, so nothing
 *      a visitor sees first depends on the container being awake, and
 *   2. while it does wake, the site says so.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

/** A banner stub that remembers whether it is hidden. The shared harness
 *  returns a Proxy that swallows every write, which would make any assertion
 *  about visibility pass. */
function fakeBanner() {
  const el = {
    hidden: true,
    text: "",
    classList: {
      add(c)      { if (c === "hidden") el.hidden = true; },
      remove(c)   { if (c === "hidden") el.hidden = false; },
      contains(c) { return c === "hidden" && el.hidden; },
    },
  };
  return el;
}

/** app.js in a fresh context, with the map-drawing and banner bits replaced
 *  by something observable. Returns the context plus the doubles. */
function freshApp() {
  const app = loadApp();
  const dom   = vm.runInContext("dom", app);     // `const`: never on the context
  const state = vm.runInContext("state", app);

  const banner = fakeBanner();
  const textEl = { textContent: "" };
  dom.wakingBanner = banner;
  dom.wakingText   = textEl;

  const rendered = [];
  // Leaflet is a Proxy in the harness and marker.addTo() would throw; what
  // these tests care about is which stops arrived, not how they were drawn.
  app.renderStopsInChunks = async (stops) => { rendered.push(...stops); };
  app.applyStopVisibility = () => {};

  const toasts = [];
  app.showToast = (m) => toasts.push(m);

  return { app, state, banner, textEl, rendered, toasts };
}

const okJson = (body) => ({ ok: true, status: 200, json: async () => body });
const STATIC = { stops: [{ atco_code: "4400A", name: "Marine Parade",
                           latitude: 50.8, longitude: -0.37,
                           night_serving: false }],
                 count: 1, generated_on: "2026-09-11" };

// ── The map does not wait for a container to start ──────────

test("the stop list comes from the published file, not the live service", async () => {
  const { app, rendered } = freshApp();
  const asked = [];
  app.fetch = async (url) => { asked.push(url); return okJson(STATIC); };
  app.apiFetch = async (p) => { throw new Error(`the API was asked for ${p}`); };

  await app.loadStops();

  assert.equal(rendered.length, 1, "no stops were drawn");
  assert.equal(rendered[0].atco_code, "4400A");
  assert.deepEqual(asked.map(String), ["data/stops.json"],
    "the stop list was fetched from somewhere other than the published file");
});

test("the published file is read from the page's own origin", () => {
  // An absolute URL, or one built from CONFIG.API_BASE_URL, would send the
  // request straight back to the service being avoided.
  const url = vm.runInContext("STATIC_STOPS_URL", loadApp());
  assert.doesNotMatch(url, /^https?:\/\//,
    `the static stop list is fetched from ${url}, not from the site itself`);
});

test("the file's date stamp is kept", async () => {
  const { app, state } = freshApp();
  app.fetch = async () => okJson(STATIC);
  await app.loadStops();
  assert.equal(state.stopsGeneratedOn, "2026-09-11");
});

test("a stale stop list says so, and a current one stays quiet", async () => {
  // The weekly rebuild stopping is silent by nature — GitHub disables
  // scheduled workflows in a quiet repository — so the page has to notice.
  const days = (n) =>
    new Date(Date.now() - n * 86_400_000).toISOString().slice(0, 10);

  const fresh = freshApp();
  fresh.app.fetch = async () => okJson({ ...STATIC, generated_on: days(3) });
  await fresh.app.loadStops();
  assert.deepEqual(fresh.toasts, [],
    "a stop list three days old was reported as stale");

  const old = freshApp();
  old.app.fetch = async () => okJson({ ...STATIC, generated_on: days(40) });
  await old.app.loadStops();
  assert.equal(old.toasts.length, 1, "a 40-day-old stop list passed unremarked");
  assert.match(old.toasts[0], /rebuilt/);
  assert.match(old.toasts[0], /Live times are unaffected/,
    "the warning does not say what it is not about");

  // A file without the field — an older build — must not produce "NaN days".
  const undated = freshApp();
  undated.app.fetch = async () => okJson({ stops: STATIC.stops, count: 1 });
  await undated.app.loadStops();
  assert.deepEqual(undated.toasts, [],
    "a stop list with no date stamp was guessed at");
});

// ── …but it still works when the file is not there ──────────

test("a missing published file falls back to the API", async () => {
  const { app, rendered } = freshApp();
  app.fetch = async () => ({ ok: false, status: 404, json: async () => ({}) });
  let apiCalls = 0;
  app.apiFetch = async () => { apiCalls++; return { stops: STATIC.stops }; };

  await app.loadStops();

  assert.equal(apiCalls, 1, "a 404 on the static file left the map empty");
  assert.equal(rendered.length, 1);
});

test("a truncated or malformed published file falls back to the API", async () => {
  // A half-written deploy is the realistic case, and it arrives as a 200.
  for (const body of [{}, { stops: [] }, { stops: "nope" }]) {
    const { app, rendered } = freshApp();
    app.fetch = async () => okJson(body);
    app.apiFetch = async () => ({ stops: STATIC.stops });
    await app.loadStops();
    assert.equal(rendered.length, 1,
      `${JSON.stringify(body)} was accepted as a stop list`);
  }
});

test("when both are unavailable the reader is told what still works", async () => {
  const { app, toasts } = freshApp();
  app.fetch = async () => { throw new Error("network down"); };
  app.apiFetch = async () => { throw new Error("also down"); };
  await app.loadStops();
  assert.equal(toasts.length, 1, "the failure was silent");
  assert.match(toasts[0], /Route, Network and Updates still work/);
});

// ── Saying what is actually happening ───────────────────────

test("a cold start that never finishes is not reported as being offline", async () => {
  const { app, state } = freshApp();
  state.apiEverResponded = false;
  // What a deadline looks like from inside apiRequest.
  app.fetch = async () => { const e = new Error("aborted"); e.name = "AbortError"; throw e; };

  await assert.rejects(() => app.apiFetch("/api/departures/X"), (err) => {
    assert.equal(err.kind, "waking",
      `a cold start was reported as "${err.kind}"`);
    assert.doesNotMatch(err.message, /offline/i,
      "a sleeping container was blamed on the reader's connection");
    assert.match(err.message, /timetables/i,
      "the message does not say what still works");
    return true;
  });
});

test("once the service has answered, a later failure is a failure again", async () => {
  const { app, state } = freshApp();
  state.apiEverResponded = true;
  app.fetch = async () => { const e = new Error("aborted"); e.name = "AbortError"; throw e; };
  await assert.rejects(() => app.apiFetch("/api/vehicles"), (err) => {
    assert.equal(err.kind, "timeout",
      "a warm service that stopped answering was excused as still waking up");
    return true;
  });
});

test("a first call gets longer than the ordinary deadline", () => {
  // 22.4 s measured; a 15 s budget guaranteed the first visitor an error.
  const app  = loadApp();
  const cold = vm.runInContext("COLD_START_TIMEOUT_MS", app);
  const warm = vm.runInContext("API_TIMEOUT_MS", app);
  assert.ok(cold > 25_000, `${cold} ms is not long enough for a measured 22.4 s start`);
  assert.ok(cold > warm, "the cold-start budget is no longer than the ordinary one");
});

test("the banner explains the wait and does not blame the reader", () => {
  const { app, banner, textEl } = freshApp();
  app.showWakingBanner();
  assert.equal(banner.hidden, false, "the banner never became visible");
  assert.match(textEl.textContent, /waking/i);
  assert.match(textEl.textContent, /seconds/,
    "the wait is described without saying roughly how long it is");
  app.hideWakingBanner();
  assert.equal(banner.hidden, true, "the banner cannot be dismissed");
});

test("a warm service never shows the banner", async () => {
  const { app, state, banner } = freshApp();
  state.apiEverResponded = true;
  app.fetch = async () => okJson({ fine: true });
  await app.apiFetch("/api/vehicles");
  assert.equal(banner.hidden, true,
    "a service answering in milliseconds announced that it was waking up");
});

test("the banner appears while waiting and clears only when the last call is done",
  async () => {
  // Startup fires several calls at once. The bug this guards is the first one
  // to finish hiding the banner while the others are still waiting.
  const { app, state, banner } = freshApp();
  state.apiEverResponded = false;

  const releases = [];
  app.fetch = () => new Promise((resolve) => {
    releases.push(() => resolve(okJson({ ok: 1 })));
  });

  const a = app.apiFetch("/api/vehicles");
  const b = app.apiFetch("/api/stops");

  const announceMs = vm.runInContext("WAKING_ANNOUNCE_MS", app);
  await new Promise((r) => setTimeout(r, announceMs + 150));
  assert.equal(banner.hidden, false,
    `nothing was said after ${announceMs} ms of waiting`);

  releases[0]();
  await a;
  assert.equal(banner.hidden, false,
    "the banner vanished while a second first-call was still waiting");

  releases[1]();
  await b;
  assert.equal(banner.hidden, true,
    "the banner stayed up after the service answered");
});
