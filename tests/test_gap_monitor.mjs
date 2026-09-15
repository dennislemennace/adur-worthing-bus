/**
 * Tests for the A259 gap monitor in the Live panel.
 *
 * Two promises to the reader, and both are about what is *not* shown:
 *
 *   1. From 23:30 to 04:30 London time it neither appears nor asks the server,
 *      whatever time zone the visitor's device is in. The night crosses
 *      midnight, which is where a window test usually goes wrong.
 *   2. "Running to timetable" is a claim, so it only appears when the server
 *      has actually judged the service normal. No data, low coverage, an error
 *      or paused positions all show nothing at all.
 *
 * And one about honesty inside an alert: a bus not sending its position is
 * indistinguishable from one not running, so the alert says so when it applies.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

const at = (iso) => new Date(iso);

const NORMAL = {
  active: true, status: "normal", reason: null, as_of: "2026-09-16T12:30:05+01:00",
  alert: null,
  stops: [
    { atco: "4400AD0330", name: "Shoreham Port",
      next: [{ service: "700", due: "12:34", minutes: 4, source: "live" },
             { service: "700", due: "12:44", minutes: 14, source: "scheduled" }] },
    { atco: "4400AD0203", name: "Shoreham High Street",
      next: [{ service: "2", due: "12:30", minutes: 0, source: "live" }] },
    { atco: "4400AD0063", name: "Beach Green Hotel, Lancing", next: [] },
  ],
};

function alertPayload(over = {}) {
  const alert = { atco: "4400AD0203", name: "Shoreham High Street", minutes: 32,
                  from: "12:31", to: "13:03", from_now: false, to_horizon: false,
                  timetable_minutes: 10, not_reporting: 2, alert: true, ...over };
  return { ...NORMAL, status: "alert", alert };
}

function fakeHost() {
  return { hidden: true, innerHTML: "", querySelector: () => null };
}

function freshApp() {
  const app = loadApp();
  const dom = vm.runInContext("dom", app);
  const state = vm.runInContext("state", app);
  const host = fakeHost();
  const said = [];
  const button = { hidden: true, title: "", attrs: {},
                   setAttribute(k, v) { this.attrs[k] = v; } };
  dom.gapMonitor = host;
  dom.gapMonitorLive = { set textContent(v) { said.push(v); }, get textContent() { return said.at(-1) || ""; } };
  dom.gapAlertBtn = button;
  state.viewMode = "live";
  return { app, state, host, said, button };
}

// ── Quiet hours ─────────────────────────────────────────────

for (const [iso, quiet, label] of [
  ["2026-09-16T22:29:00Z", false, "23:29 BST"],
  ["2026-09-16T22:30:00Z", true,  "23:30 BST"],
  ["2026-09-16T23:59:00Z", true,  "00:59 BST"],
  ["2026-09-17T03:29:00Z", true,  "04:29 BST"],
  ["2026-09-17T03:30:00Z", false, "04:30 BST"],
  ["2027-01-14T23:29:00Z", false, "23:29 GMT"],
  ["2027-01-14T23:30:00Z", true,  "23:30 GMT"],
  ["2027-01-15T04:30:00Z", false, "04:30 GMT"],
]) {
  test(`quiet hours follow London time: ${label} is ${quiet ? "quiet" : "shown"}`, () => {
    const { app } = freshApp();
    assert.equal(app.isGapQuietHours(at(iso)), quiet);
  });
}

test("in quiet hours the monitor is hidden and the server is not asked", async () => {
  const { app, host } = freshApp();
  host.hidden = false;
  const asked = [];
  app.isGapQuietHours = () => true;
  app.apiFetch = async (p) => { asked.push(p); return NORMAL; };
  await app.fetchGapMonitor();
  assert.deepEqual(asked, [], "the API was woken during quiet hours");
  assert.equal(host.hidden, true);
});

test("in the day it asks for the corridor and shows the answer", async () => {
  const { app, host } = freshApp();
  const asked = [];
  app.isGapQuietHours = () => false;
  app.apiFetch = async (p) => { asked.push(p); return NORMAL; };
  await app.fetchGapMonitor();
  assert.deepEqual(asked, ["/api/corridor-gaps"]);
  assert.equal(host.hidden, false);
  assert.match(host.innerHTML, /running to timetable/);
});

test("outside Live view it does not ask", async () => {
  const { app, state, host } = freshApp();
  state.viewMode = "tickets";
  const asked = [];
  app.isGapQuietHours = () => false;
  app.apiFetch = async (p) => { asked.push(p); return NORMAL; };
  await app.fetchGapMonitor();
  assert.deepEqual(asked, []);
  assert.equal(host.hidden, true);
});

// ── Nothing is shown that the server did not judge ──────────

for (const [label, data] of [
  ["no response", null],
  ["quiet hours from the server", { active: false, status: "quiet", reason: "quiet_hours" }],
  ["no live data", { active: true, status: "unknown", reason: "no_live_data" }],
  ["too few buses reporting", { active: true, status: "unknown", reason: "low_coverage", stops: NORMAL.stops }],
  ["an alert with no detail", { ...NORMAL, status: "alert", alert: null }],
  ["no stops", { ...NORMAL, stops: [] }],
]) {
  test(`nothing is shown for ${label}`, () => {
    const { app } = freshApp();
    assert.equal(app.gapMonitorHtml(data), "");
  });
}

test("an error hides the monitor rather than leaving the last answer up", async () => {
  const { app, host } = freshApp();
  app.isGapQuietHours = () => false;
  app.apiFetch = async () => NORMAL;
  await app.fetchGapMonitor();
  assert.equal(host.hidden, false);
  app.apiFetch = async () => { throw new Error("502"); };
  await app.fetchGapMonitor();
  assert.equal(host.hidden, true, "a stale 'running to timetable' stayed on screen");
});

test("pausing live positions hides the monitor", async () => {
  const { app, host } = freshApp();
  app.isGapQuietHours = () => false;
  app.apiFetch = async () => NORMAL;
  await app.fetchGapMonitor();
  app.stopGapMonitor();
  assert.equal(host.hidden, true);
});

// ── What it says ────────────────────────────────────────────

test("a normal service is one line with the detail folded away", () => {
  const { app } = freshApp();
  const html = app.gapMonitorHtml(NORMAL);
  assert.match(html, /<summary>[\s\S]*A259 westbound: buses running to timetable[\s\S]*<\/summary>/);
  assert.doesNotMatch(html, /gap-monitor--alert/);
  assert.doesNotMatch(html, /<details[^>]*\bopen\b/, "the detail should start closed");
  assert.match(html, /700 in 4 min/);
  assert.match(html, /700 in 14 min \(timetable\)/);
  assert.match(html, /2 due now/);
  assert.match(html, /Beach Green Hotel, Lancing<\/span> no bus in the next hour/);
  assert.match(html, /Estimated at 12:30/);
  assert.doesNotMatch(html, /NaN|undefined|null/);
});

test("an alert names the stop, the times, the timetable's gap and the missing buses", () => {
  const { app } = freshApp();
  const html = app.gapMonitorHtml(alertPayload());
  assert.match(html, /gap-monitor--alert/);
  assert.match(html, /<summary>[\s\S]*Long gap in westbound buses at Shoreham High Street/);
  assert.match(html, /between 12:31 and 13:03, a gap of 32 minutes/);
  assert.match(html, /timetable's own gap there is 10 minutes/);
  assert.match(html, /2 scheduled buses in that time are not sending their positions/);
});

test("one missing bus is described in the singular, and none is not mentioned", () => {
  const { app } = freshApp();
  assert.match(app.gapMonitorHtml(alertPayload({ not_reporting: 1 })),
    /One scheduled bus in that time is not sending its position/);
  assert.doesNotMatch(app.gapMonitorHtml(alertPayload({ not_reporting: 0 })),
    /not sending/);
});

test("a gap cut off by the hour ahead is 'at least', and a gap from now says until", () => {
  const { app } = freshApp();
  assert.match(app.gapMonitorHtml(alertPayload({ to_horizon: true })), /a gap of at least 32 minutes/);
  assert.match(app.gapMonitorHtml(alertPayload({ from_now: true, from: "12:30" })),
    /until 13:03, 32 minutes from now/);
  assert.match(app.gapMonitorHtml(alertPayload({ from_now: true, to_horizon: true })),
    /in the next hour/);
});

test("stop and service names are escaped", () => {
  const { app } = freshApp();
  const html = app.gapMonitorHtml(alertPayload({ name: "<img src=x onerror=alert(1)>" }));
  assert.doesNotMatch(html, /<img/);
});

test("an alert is announced once, not on every refresh", () => {
  const { app, said } = freshApp();
  app.renderGapMonitor(alertPayload());
  app.renderGapMonitor(alertPayload({ minutes: 31 }));
  app.renderGapMonitor(alertPayload({ minutes: 30 }));
  assert.deepEqual(said, ["Long gap in westbound buses at Shoreham High Street."]);
  app.renderGapMonitor(NORMAL);
  assert.equal(said.at(-1), "Westbound buses on the A259 are running to timetable again.");
  assert.equal(said.length, 2);
});

test("an open detail stays open across a refresh", () => {
  const { app, host } = freshApp();
  app.renderGapMonitor(NORMAL);
  host.querySelector = (sel) => sel === "details" ? { open: true, contains: () => false } : null;
  app.renderGapMonitor(NORMAL);
  assert.match(host.innerHTML, /<details[^>]*\bopen\b/);
});

// ── The way in on a phone ───────────────────────────────────
// At the resting sheet height the monitor is below the fold, so an alert with
// no other sign would go unseen. The status-pill button is that sign, and it
// must exist only while there is an alert: in normal service it costs the map
// nothing.

test("the status-pill alert button appears only during an alert, naming the stop", () => {
  const { app, button } = freshApp();
  app.renderGapMonitor(NORMAL);
  assert.equal(button.hidden, true, "a normal service put a button over the map");
  app.renderGapMonitor(alertPayload());
  assert.equal(button.hidden, false);
  assert.match(button.attrs["aria-label"], /Long gap in westbound buses at Shoreham High Street/);
  app.renderGapMonitor(null);
  assert.equal(button.hidden, true, "the alert button outlived the alert");
});
