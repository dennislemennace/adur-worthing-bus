/**
 * Tests for who owns the panel when a slow response finally arrives.
 *
 * Reproduced in production during the pre-release audit: open a bus stop,
 * then a rail station while the bus board is still loading. The bus response
 * lands 19 seconds later and paints bus departures underneath the heading
 * "Worthing / CRS: WRH". At that moment `selectedStop` is null and
 * `selectedRailStation` is Worthing — the data on screen belongs to neither.
 *
 * The panel is one place, and two different loaders write to it. These tests
 * pin the rule that a response may only render if it is still the most recent
 * request for the slot it is writing into.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

/** A fresh app with `apiFetch` under our control.
 *
 *  Returns `release(url, value)` so a test can decide the order responses
 *  come back in, which is the whole subject here. */
function appWithControlledFetch() {
  const app = loadApp();
  const state = vm.runInContext("state", app);
  const pending = new Map();

  app.apiFetch = (url) => new Promise((resolve, reject) => {
    pending.set(url, { resolve, reject });
  });

  const findPending = (fragment) => {
    for (const [url, ctl] of pending) if (url.includes(fragment)) return { url, ctl };
    throw new Error(`no in-flight request matching ${fragment}; `
                    + `have: ${[...pending.keys()].join(", ") || "(none)"}`);
  };

  return {
    app, state,
    pendingCount: () => pending.size,
    resolve(fragment, value) {
      const { url, ctl } = findPending(fragment);
      pending.delete(url);
      ctl.resolve(value);
      return new Promise(r => setTimeout(r, 0));   // let continuations run
    },
    reject(fragment, err) {
      const { url, ctl } = findPending(fragment);
      pending.delete(url);
      ctl.reject(err);
      return new Promise(r => setTimeout(r, 0));
    },
  };
}

const BUS_BOARD = { stop_name: "Marine Parade", departures: [{ service: "700" }] };

// ── The panel belongs to one selection at a time ────────────

test("a late bus board does not paint over a rail station", async () => {
  const h = appWithControlledFetch();
  let rendered = 0;
  h.app.renderDepartures = () => { rendered += 1; };
  h.app.renderRailBoard = () => {};

  h.state.viewMode = "live";
  h.state.selectedStop = { atcoCode: "A", stopName: "Marine Parade" };
  const busLoad = h.app.fetchDepartures("A");

  // The reader gives up and opens a rail station instead.
  h.state.selectedStop = null;
  h.state.selectedRailStation = { crs: "WRH", name: "Worthing" };

  await h.resolve("/api/departures", BUS_BOARD);
  await busLoad;

  assert.equal(rendered, 0,
    "bus departures were rendered while a rail station was selected");
});

test("a late board for the previous stop does not replace the current one", async () => {
  const h = appWithControlledFetch();
  const painted = [];
  h.app.renderDepartures = (d) => { painted.push(d.stop_name); };

  h.state.viewMode = "live";
  h.state.selectedStop = { atcoCode: "A", stopName: "A" };
  const first = h.app.fetchDepartures("A");

  h.state.selectedStop = { atcoCode: "B", stopName: "B" };
  const second = h.app.fetchDepartures("B");

  // B answers first, then A — the order the audit reproduced.
  await h.resolve("stopId=B", { stop_name: "B", departures: [] });
  await h.resolve("stopId=A", { stop_name: "A", departures: [] });
  await Promise.all([first, second]);

  assert.deepEqual(painted, ["B"],
    `the panel ended up showing ${painted[painted.length - 1]} while B was selected`);
});

test("an error from a superseded request does not replace a good board", async () => {
  const h = appWithControlledFetch();
  const states = [];
  h.app.renderDepartures = () => { states.push("ok"); };
  h.app.showPanelState = (k) => { if (k === "error") states.push("error"); };

  h.state.viewMode = "live";
  h.state.selectedStop = { atcoCode: "A", stopName: "A" };
  const first = h.app.fetchDepartures("A");
  h.state.selectedStop = { atcoCode: "B", stopName: "B" };
  const second = h.app.fetchDepartures("B");

  await h.resolve("stopId=B", { stop_name: "B", departures: [] });
  await h.reject("stopId=A", new Error("network"));
  await Promise.all([first, second]);

  assert.ok(!states.includes("error"),
    "an obsolete request's failure put the panel into an error state");
});

// ── Leaving a section during its first load ─────────────────

test("a journey result does not appear after the reader has left the view", async () => {
  const h = appWithControlledFetch();
  let rendered = 0;
  h.app.renderJourneyResult = () => { rendered += 1; };
  h.app.loadTicketZones = async () => [];
  h.app.clearJourneyLayers = () => {};
  h.app.setJourneyStatus = () => {};
  h.app.atcoFromPickerValue = (v) => v;

  h.state.viewMode = "tickets";
  const dom = vm.runInContext("dom", h.app);
  dom.jcFrom = { value: "A" };
  dom.jcTo   = { value: "B" };
  dom.jcResult = { innerHTML: "" };

  const check = h.app.checkJourney();
  // Let the request actually go out before the reader moves on, so this
  // exercises the late-response window rather than the earlier guard that
  // stops the request being made at all.
  await new Promise(r => setTimeout(r, 0));
  h.state.viewMode = "network";          // reader moves to Network Objectives

  await h.resolve("/api/journey", { from: {}, to: {}, options: [] });
  await check;

  assert.equal(rendered, 0,
    "a journey result was rendered into a view the reader had left");
});


test("a journey the reader has already left is not even requested", async () => {
  // The cheaper half of the same rule: if the view changes while the ticket
  // zones are still loading, there is no reason to ask the API at all.
  // Asserted separately so the test above cannot be passing for this reason.
  const h = appWithControlledFetch();
  let releaseZones;
  h.app.loadTicketZones = () => new Promise(r => { releaseZones = r; });
  h.app.clearJourneyLayers = () => {};
  h.app.setJourneyStatus = () => {};
  h.app.atcoFromPickerValue = (v) => v;
  h.app.renderJourneyResult = () => { throw new Error("should not render"); };

  h.state.viewMode = "tickets";
  const dom = vm.runInContext("dom", h.app);
  dom.jcFrom = { value: "A" };
  dom.jcTo   = { value: "B" };
  dom.jcResult = { innerHTML: "" };

  const check = h.app.checkJourney();
  h.state.viewMode = "network";     // reader leaves while zones are loading
  releaseZones([]);
  await check;

  assert.equal(h.pendingCount(), 0,
    "an API request was sent for a view the reader had already left");
});

// ── Leaving a view while its layers are loading ─────────────

test("route layers do not appear on the map of a view the reader has left", async () => {
  // Reproduced by the audit: delay the Route view load, switch to Network
  // Objectives, then release it — and 35 route layers appear over a section
  // that has no map content of its own.
  const h = appWithControlledFetch();
  let shown = 0;
  let releaseLines;
  h.app.loadRouteLines = () => new Promise(r => { releaseLines = r; });
  h.app.loadProposals = async () => [];
  h.app.loadCouncilBoundaries = async () => [];
  h.app.showRouteLines = () => { shown += 1; };
  h.app.reconcileCouncilBoundaries = () => {};
  h.app.reconcileProposalLayers = () => {};
  h.app.hideTicketZones = () => {};
  h.app.clearJourneyLayers = () => {};
  h.app.ensureMapOverlayControls = () => {};
  h.app.setSheetDetent = () => {};
  h.app.isSheetLayout = () => false;
  h.app.stopVehicleRefresh = () => {};
  h.app.hideVehicleMarkers = () => {};
  h.app.hideRailStations = () => {};
  h.app.clearAllSelectedRailServices = () => {};
  h.app.closePanel = () => {};

  // applyViewMode touches the map directly before any await.
  h.state.map = { closePopup() {}, hasLayer: () => false, getContainer: () => ({}) };

  h.state.viewMode = "improvements";
  const applying = h.app.applyViewMode();
  await new Promise(r => setTimeout(r, 0));

  h.state.viewMode = "network";        // reader moves on
  releaseLines([]);
  await applying;

  assert.equal(shown, 0,
    "route lines were added to the map after the reader left Route view");
});

// ── A board that has been open a while ──────────────────────

test("a due-time label is re-derived from the row, not frozen at render", async () => {
  // The audit advanced the clock and the board still read "5 mins". The label
  // has to be recomputable from something the row carries, or it can only
  // ever describe the moment it was drawn.
  const app = loadApp();
  const at = "2026-09-08T12:05:00+01:00";
  const fiveToGo = new Date("2026-09-08T12:00:00+01:00");
  const gone     = new Date("2026-09-08T12:10:00+01:00");

  assert.equal(app.formatDueTime(at, fiveToGo), "5 mins");
  assert.equal(app.formatDueTime(at, gone), "Departed",
    "the same row still read as upcoming after its bus had left");
});

test("a rendered departure row carries the time its label was derived from", () => {
  const app = loadApp();
  const html = app.buildDepartureRow({
    service: "700", destination: "Brighton",
    aimed_departure: "2026-09-08T12:05:00+01:00",
    expected_departure: null, status: "Scheduled", delay_seconds: null,
  });
  assert.match(html, /data-due-at="2026-09-08T12:05:00\+01:00"/,
    "the row has no time to re-derive its label from");
});

// ── A failed load can be tried again ────────────────────────

test("a failed objectives load does not become a permanent empty list", async () => {
  // The catch set `state.objectives = []`, and the guard at the top of the
  // loader is `if (state.objectives) return`. An empty array is truthy, so one
  // transient failure showed "No objectives published yet" for the rest of the
  // page's life — a wrong answer, presented as a fact about the campaign.
  const app = loadApp();
  const state = vm.runInContext("state", app);
  let attempts = 0;
  app.fetch = async () => {
    attempts += 1;
    if (attempts === 1) throw new Error("network");
    return { ok: true, json: async () => ({ objectives: [{ id: "a", title: "A" }] }) };
  };
  app.renderObjectivesList = () => {};
  app.populateObjectiveSelect = () => {};

  await app.loadObjectives();
  assert.equal(state.objectives, null,
    "a failure was recorded as an empty list, which reads as an answer");
  assert.ok(state.objectivesError, "nothing records that the load failed");

  await app.loadObjectives();
  assert.equal(attempts, 2, "the second attempt never happened");
  assert.equal(state.objectives.length, 1);
});
