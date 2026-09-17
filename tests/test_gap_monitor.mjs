/**
 * Tests for the A259 gap monitor in the Live panel: one row for the corridor,
 * with a line for each stop in each direction.
 *
 * Two promises to the reader, and both are about what is *not* shown:
 *
 *   1. From 23:30 to 04:30 London time it neither appears nor asks the server,
 *      whatever time zone the visitor's device is in. The night crosses
 *      midnight, which is where a window test usually goes wrong.
 *   2. "Running to timetable" is a claim, so a row only appears when the
 *      server has actually judged that direction. No data, low coverage, an
 *      error, paused positions or an older API all show nothing.
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

const WORTHING_STOPS = [
  { atco: "4400AD0330", name: "Shoreham Port",
    next: [{ service: "700", due: "12:34", minutes: 4, source: "live" },
           { service: "700", due: "12:44", minutes: 14, source: "scheduled" }] },
  { atco: "4400AD0203", name: "Shoreham High Street",
    next: [{ service: "2", due: "12:30", minutes: 0, source: "live" }] },
  { atco: "4400AD0063", name: "Beach Green Hotel, Lancing", next: [] },
];
const BRIGHTON_STOPS = [
  { atco: "4400AD0064", name: "Beach Green Hotel, Lancing",
    next: [{ service: "700", due: "12:38", minutes: 8, source: "live" }] },
  { atco: "4400AD0204", name: "Shoreham High Street",
    next: [{ service: "700", due: "12:47", minutes: 17, source: "live" }] },
  { atco: "4400AD0329", name: "Shoreham Port",
    next: [{ service: "700", due: "12:52", minutes: 22, source: "live" }] },
];

function direction(id, over = {}) {
  const towards = id === "brighton" ? "towards Brighton" : "towards Worthing";
  return { id, label: `A259 Coast Rd ${towards}`, towards, status: "normal", reason: null,
           alert: null, stops: id === "brighton" ? BRIGHTON_STOPS : WORTHING_STOPS, ...over };
}

const NORMAL = {
  active: true, as_of: "2026-09-16T12:30:05+01:00",
  directions: [direction("worthing"), direction("brighton")],
};

function brightonAlert(over = {}) {
  const alert = { atco: "4400AD0204", name: "Shoreham High Street", minutes: 32,
                  from: "12:31", to: "13:03", from_now: false, to_horizon: false,
                  timetable_minutes: 10, not_reporting: 2, alert: true, ...over };
  return { ...NORMAL, directions: [direction("worthing"),
                                   direction("brighton", { status: "alert", alert })] };
}

function fakeHost(rows = []) {
  return { hidden: true, innerHTML: "", querySelector: () => null,
           querySelectorAll: () => rows };
}

function freshApp() {
  const app = loadApp();
  const dom = vm.runInContext("dom", app);
  const state = vm.runInContext("state", app);
  const host = fakeHost();
  const said = [];
  const button = { hidden: true, title: "", attrs: {}, dataset: {},
                   setAttribute(k, v) { this.attrs[k] = v; } };
  dom.gapMonitor = host;
  dom.gapMonitorLive = { set textContent(v) { said.push(v); }, get textContent() { return said.at(-1) || ""; } };
  dom.gapAlertBtn = button;
  state.viewMode = "live";
  return { app, dom, state, host, said, button };
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
  assert.match(host.innerHTML, /no confirmed long gaps/);
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
  ["quiet hours from the server", { active: false, reason: "quiet_hours", directions: [] }],
  ["an older API with no directions", { active: true, status: "normal", stops: WORTHING_STOPS }],
  ["no live data either way", { ...NORMAL, directions: [
    direction("worthing", { status: "unknown", reason: "no_live_data", stops: undefined }),
    direction("brighton", { status: "unknown", reason: "no_live_data", stops: undefined })] }],
  ["an alert with no detail", { ...NORMAL, directions: [direction("worthing", { status: "alert", alert: null })] }],
  ["no stops", { ...NORMAL, directions: [direction("worthing", { stops: [] })] }],
]) {
  test(`nothing is shown for ${label}`, () => {
    const { app } = freshApp();
    assert.equal(app.gapMonitorHtml(data), "");
  });
}

test("a direction the server could not judge is left out, and the other still shows", () => {
  const { app } = freshApp();
  const html = app.gapMonitorHtml({ ...NORMAL, directions: [
    direction("worthing", { status: "unknown", reason: "low_coverage" }), direction("brighton")] });
  assert.doesNotMatch(html, /towards Worthing/);
  assert.match(html, /A259 Coast Rd towards Brighton: no confirmed long gaps/);
});

test("an error hides the monitor rather than leaving the last answer up", async () => {
  const { app, host } = freshApp();
  app.isGapQuietHours = () => false;
  app.apiFetch = async () => NORMAL;
  await app.fetchGapMonitor();
  assert.equal(host.hidden, false);
  app.apiFetch = async () => { throw new Error("502"); };
  await app.fetchGapMonitor();
  assert.equal(host.hidden, true, "a stale 'no confirmed long gaps' stayed on screen");
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

test("a normal service is one line for the corridor, with the detail folded away", () => {
  const { app } = freshApp();
  const html = app.gapMonitorHtml(NORMAL);
  const summaries = [...html.matchAll(/<summary>([\s\S]*?)<\/summary>/g)]
    .map(m => m[1].replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim());
  assert.deepEqual(summaries, ["A259 Coast Rd: no confirmed long gaps"]);
  assert.doesNotMatch(html, /gap-monitor--alert/);
  assert.doesNotMatch(html, /<details[^>]*\bopen\b/, "the detail should start closed");
  assert.match(html, /700 in 4 min/);
  assert.match(html, /700 in 14 min \(timetable\)/);
  assert.match(html, /2 due now/);
  assert.match(html, /Estimated at 12:30/);
  assert.doesNotMatch(html, /NaN|undefined|null/);
});

test("each line reads stop, direction, then times, with a stop's directions together", () => {
  const { app } = freshApp();
  const lines = [...app.gapMonitorHtml(NORMAL).matchAll(/<li>([\s\S]*?)<\/li>/g)]
    .map(m => m[1].replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim());
  assert.deepEqual(lines, [
    "Shoreham Port towards Worthing: 700 in 4 min, 700 in 14 min (timetable)",
    "Shoreham Port towards Brighton: 700 in 22 min",
    "Shoreham High Street towards Worthing: 2 due now",
    "Shoreham High Street towards Brighton: 700 in 17 min",
    "Beach Green Hotel, Lancing towards Worthing: no bus in the next hour",
    "Beach Green Hotel, Lancing towards Brighton: 700 in 8 min",
  ]);
});

test("an alert names the direction, the stop, the times, the timetable's gap and the missing buses", () => {
  const { app } = freshApp();
  const html = app.gapMonitorHtml(brightonAlert());
  assert.match(html, /<summary>[\s\S]*A259 Coast Rd: long gap towards Brighton/);
  assert.match(html, /No bus towards Brighton is expected at Shoreham High Street between 12:31 and 13:03, a gap of 32 minutes/);
  assert.match(html, /timetable's own gap there is 10 minutes/);
  assert.match(html, /2 scheduled buses in that time are not sending their positions/);
  assert.equal((html.match(/gap-monitor--alert/g) || []).length, 1, "only the alerting row is marked");
});

test("one missing bus is described in the singular, and none is not mentioned", () => {
  const { app } = freshApp();
  assert.match(app.gapMonitorHtml(brightonAlert({ not_reporting: 1 })),
    /One scheduled bus in that time is not sending its position/);
  assert.doesNotMatch(app.gapMonitorHtml(brightonAlert({ not_reporting: 0 })),
    /not sending/);
});

test("a gap cut off by the hour ahead is 'at least', and a gap from now says until", () => {
  const { app } = freshApp();
  assert.match(app.gapMonitorHtml(brightonAlert({ to_horizon: true })), /a gap of at least 32 minutes/);
  assert.match(app.gapMonitorHtml(brightonAlert({ from_now: true, from: "12:30" })),
    /until 13:03, 32 minutes from now/);
  assert.match(app.gapMonitorHtml(brightonAlert({ from_now: true, to_horizon: true })),
    /towards Brighton is expected at Shoreham High Street in the next hour/);
});

test("names from the server are escaped", () => {
  const { app } = freshApp();
  const data = brightonAlert({ name: "<img src=x onerror=alert(1)>" });
  data.directions[0].towards = "<b>towards</b>";
  const html = app.gapMonitorHtml(data);
  assert.doesNotMatch(html, /<img|<b>/);
});

test("a new alert is announced once, not on every refresh", () => {
  const { app, said } = freshApp();
  app.renderGapMonitor(NORMAL);
  app.renderGapMonitor(brightonAlert());
  app.renderGapMonitor(brightonAlert({ minutes: 31 }));
  assert.deepEqual(said, ["A259 Coast Rd: long gap at Shoreham High Street towards Brighton."]);
  app.renderGapMonitor(NORMAL);
  assert.equal(said.at(-1), "No confirmed long gaps on the A259 Coast Rd now.");
  assert.equal(said.length, 2);
});

test("an open tracker stays open across a refresh", () => {
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

test("the status-pill alert button appears only during an alert, naming the direction", () => {
  const { app, button } = freshApp();
  app.renderGapMonitor(NORMAL);
  assert.equal(button.hidden, true, "a normal service put a button over the map");
  app.renderGapMonitor(brightonAlert());
  assert.equal(button.hidden, false);
  assert.match(button.attrs["aria-label"],
    /A259 Coast Rd: long gap at Shoreham High Street towards Brighton/);
  app.renderGapMonitor(null);
  assert.equal(button.hidden, true, "the alert button outlived the alert");
});

// ── The waking banner ───────────────────────────────────────

test("while the service wakes, the banner promises only what is already there", () => {
  // The stop list ships with the site. Route lines and timetables come from
  // the service that is still starting, so Route view had no routes to show.
  const { app, dom } = freshApp();
  const text = { textContent: "" };
  dom.wakingText = text;
  dom.wakingBanner = { classList: { remove() {}, add() {} } };
  app.showWakingBanner();
  assert.match(text.textContent, /stops are ready/i);
  assert.doesNotMatch(text.textContent, /(routes|timetables)[^.]*\bready now\b/i);
});


// ── Saying only what was measured ───────────────────────────

test("a gap awaiting confirmation is not called running to timetable", () => {
  // A 30-minute gap over the threshold is held back until a second look, at
  // least 45 seconds later, still finds it. During that hold there is no
  // confirmed alert — but the service is not known to be fine either, and the
  // old wording claimed it was.
  const { app } = freshApp();
  const held = direction("worthing");
  held.stops[0].longest_gap = { minutes: 30, from: "12:00", to: "12:30",
                                timetable_minutes: 12, not_reporting: 0,
                                alert: false, pending: true };
  const html = app.gapMonitorHtml({ ...NORMAL, directions: [held] });
  assert.match(html, /checking a possible long gap/);
  assert.doesNotMatch(html, /no confirmed long gaps/,
    "a 30-minute gap under confirmation was reported as a normal service");
});

test("a gap containing an untracked bus is not called running to timetable either", () => {
  // A bus that is not reporting cannot be told from one that never ran, so the
  // gap is never called a wait — and must not be called fine.
  const { app } = freshApp();
  const quiet = direction("worthing");
  quiet.stops[0].longest_gap = { minutes: 28, from: "12:00", to: "12:28",
                                 timetable_minutes: 10, not_reporting: 2,
                                 alert: false };
  const html = app.gapMonitorHtml({ ...NORMAL, directions: [quiet] });
  assert.doesNotMatch(html, /no confirmed long gaps/);
});

test("an answer already in the air does not revive a stopped monitor", async () => {
  // Turning buses off stops the monitor and clears the panel. A request sent a
  // moment earlier used to land afterwards and redraw it, leaving an answer on
  // screen that nothing would ever refresh.
  const { app, host } = freshApp();
  app.isGapQuietHours = () => false;
  let release;
  app.apiFetch = () => new Promise(resolve => { release = () => resolve(NORMAL); });
  const inFlight = app.fetchGapMonitor();
  app.stopGapMonitor();
  assert.equal(host.hidden, true, "stopping did not clear the panel");
  release();
  await inFlight;
  assert.equal(host.hidden, true, "a stale answer brought the monitor back");
});

test("a restarted monitor still shows the answer it asked for", async () => {
  // The guard must not throw away the answer to the current request.
  const { app, host } = freshApp();
  app.isGapQuietHours = () => false;
  app.apiFetch = async () => NORMAL;
  app.startGapMonitor();
  await app.fetchGapMonitor();
  assert.equal(host.hidden, false, "the monitor stopped showing anything at all");
  app.stopGapMonitor();
});
