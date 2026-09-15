/**
 * Tests for visit counting (GoatCounter).
 *
 * The privacy section of about.html makes promises, and these are those
 * promises checked against the code:
 *
 *   * nothing loads without a site code, for a browser that sends Do Not Track
 *     or Global Privacy Control, or on a local or LAN preview,
 *   * what is sent is a fixed name for an action, never a postcode, a letter,
 *     a stop someone searched for or anything else a reader typed,
 *   * a blocked or broken counter never breaks the feature it sits in, and
 *   * things that repeat on their own (a poll, a cold start firing for several
 *     calls) are counted once, not once a tick.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";
import { loadApp } from "./load_app.mjs";

function fresh() {
  const app = loadApp();
  const dom = vm.runInContext("dom", app);
  const state = vm.runInContext("state", app);
  const CONFIG = vm.runInContext("CONFIG", app);
  const analytics = vm.runInContext("analytics", app);
  return { app, dom, state, CONFIG, analytics };
}

/** Counting switched on, with a stand-in GoatCounter that records calls. */
function counting() {
  const ctx = fresh();
  ctx.analytics.enabled = true;
  ctx.sent = [];
  ctx.app.window.goatcounter = { count: (o) => ctx.sent.push(o) };
  ctx.names = () => ctx.sent.map(o => o.path);
  return ctx;
}

const LIVE_SITE = { hostname: "worthingbrightonbus.co.uk" };

// ── When it may load ────────────────────────────────────────

test("with no site code nothing is allowed to load", () => {
  const { app, CONFIG } = fresh();
  CONFIG.GOATCOUNTER_CODE = "";
  assert.equal(app.analyticsAllowed({}, LIVE_SITE, {}), false);
});

test("on the live site with a code, it may load", () => {
  const { app, CONFIG } = fresh();
  CONFIG.GOATCOUNTER_CODE = "worthingbrightonbus";
  assert.equal(app.analyticsAllowed({}, LIVE_SITE, {}), true);
});

for (const [label, nav, win] of [
  ["Global Privacy Control", { globalPrivacyControl: true }, {}],
  ["Do Not Track", { doNotTrack: "1" }, {}],
  ["Do Not Track, the old Safari spelling", {}, { doNotTrack: "1" }],
]) {
  test(`a browser sending ${label} is never counted`, () => {
    const { app, CONFIG } = fresh();
    CONFIG.GOATCOUNTER_CODE = "worthingbrightonbus";
    assert.equal(app.analyticsAllowed(nav, LIVE_SITE, win), false);
  });
}

for (const host of ["localhost", "127.0.0.1", "192.168.0.32", "10.0.0.5", "172.20.1.1", "pi.local", ""]) {
  test(`a preview on ${host || "a file"} is never counted`, () => {
    const { app, CONFIG } = fresh();
    CONFIG.GOATCOUNTER_CODE = "worthingbrightonbus";
    assert.equal(app.analyticsAllowed({}, { hostname: host }, {}), false);
  });
}

test("a code that is not a plain GoatCounter code is refused, not put in a URL", () => {
  const { app, CONFIG } = fresh();
  CONFIG.GOATCOUNTER_CODE = "evil.example/x?";
  assert.equal(app.analyticsAllowed({}, LIVE_SITE, {}), false);
});

test("when allowed, one script is added pointing at the site's own counter", () => {
  const { app, CONFIG, analytics } = fresh();
  CONFIG.GOATCOUNTER_CODE = "worthingbrightonbus";
  app.navigator = {};
  app.location = LIVE_SITE;
  const added = [];
  const script = { attrs: {}, setAttribute(k, v) { this.attrs[k] = v; }, addEventListener() {} };
  app.document = { createElement: () => script, head: { appendChild: (el) => added.push(el) } };
  app.loadAnalytics();
  app.loadAnalytics();
  assert.equal(added.length, 1, "the counter was added twice");
  assert.equal(script.src, "https://gc.zgo.at/count.js");
  assert.equal(script.attrs["data-goatcounter"], "https://worthingbrightonbus.goatcounter.com/count");
  assert.equal(analytics.enabled, true);
});

test("when not allowed, nothing is added and track does nothing", () => {
  const { app, CONFIG } = fresh();
  CONFIG.GOATCOUNTER_CODE = "worthingbrightonbus";
  app.navigator = { globalPrivacyControl: true };
  app.location = LIVE_SITE;
  const added = [];
  app.document = { createElement: () => ({ setAttribute() {}, addEventListener() {} }),
                   head: { appendChild: (el) => added.push(el) } };
  const sent = [];
  app.window.goatcounter = { count: (o) => sent.push(o) };
  app.loadAnalytics();
  app.track("view-live");
  assert.deepEqual(added, []);
  assert.deepEqual(sent, [], "an opted-out visit was counted");
});

// ── track itself ────────────────────────────────────────────

test("an action is sent as an event under its name, and nothing else", () => {
  const ctx = counting();
  ctx.app.track("view-tickets");
  // Compared as JSON: the object was made inside app.js's own context.
  assert.equal(JSON.stringify(ctx.sent),
    JSON.stringify([{ path: "view-tickets", title: "view-tickets", event: true }]));
});

test("names are reduced to lower-case words and hyphens", () => {
  const ctx = counting();
  ctx.app.track("Submission bus_issue/<b>");
  assert.deepEqual(ctx.names(), ["submission-bus-issue-b"]);
});

test("calls made before the counter arrives are sent when it does", () => {
  const ctx = counting();
  ctx.app.window.goatcounter = undefined;
  ctx.app.track("view-live");
  ctx.app.track("journey-check");
  assert.deepEqual(ctx.sent, []);
  ctx.app.window.goatcounter = { count: (o) => ctx.sent.push(o) };
  ctx.app.flushAnalytics();
  assert.deepEqual(ctx.names(), ["view-live", "journey-check"]);
});

test("a counter that throws does not throw into the feature", () => {
  const ctx = counting();
  ctx.app.window.goatcounter = { count: () => { throw new Error("blocked"); } };
  assert.doesNotThrow(() => ctx.app.track("view-live"));
});

// ── What each feature counts ────────────────────────────────

test("switching view counts the view under the name a reader knows it by", () => {
  const ctx = counting();
  ctx.app.applyViewMode = () => {};
  ctx.app.syncSectionNavToViewMode = () => {};
  ctx.app.pushUrlState = () => {};
  ctx.state.viewMode = "live";
  ctx.app.setViewMode("tickets");
  ctx.app.setViewMode("network");
  ctx.app.setViewMode("network");          // no change, no count
  ctx.app.setViewMode("improvements");
  assert.deepEqual(ctx.names(), ["view-tickets", "view-better-buses", "view-route"]);
});

test("a journey check counts that it happened, never which stops", async () => {
  const ctx = counting();
  ctx.dom.jcFrom = { value: "4400AD0203" };
  ctx.dom.jcTo = { value: "149000007954" };
  ctx.dom.jcResult = { innerHTML: "" };
  ctx.app.atcoFromPickerValue = (v) => v;
  ctx.app.setJourneyStatus = () => {};
  ctx.app.claimRender = () => () => true;
  ctx.app.clearJourneyLayers = () => {};
  ctx.app.loadTicketZones = async () => {};
  ctx.app.apiFetch = async () => ({});
  ctx.app.renderJourneyResult = () => {};
  await ctx.app.checkJourney();
  assert.deepEqual(ctx.names(), ["journey-check"]);
  assert.doesNotMatch(JSON.stringify(ctx.sent), /4400|1490/);
});

test("a failed journey lookup is not counted as a check", async () => {
  const ctx = counting();
  ctx.dom.jcFrom = { value: "A" };
  ctx.dom.jcTo = { value: "B" };
  ctx.dom.jcResult = { innerHTML: "" };
  ctx.app.atcoFromPickerValue = (v) => v;
  ctx.app.setJourneyStatus = () => {};
  ctx.app.claimRender = () => () => true;
  ctx.app.clearJourneyLayers = () => {};
  ctx.app.loadTicketZones = async () => {};
  ctx.app.apiFetch = async () => { throw new Error("502"); };
  await ctx.app.checkJourney();
  assert.deepEqual(ctx.names(), []);
});

test("an example journey counts its preset id", async () => {
  const ctx = counting();
  ctx.dom.jcFrom = { value: "" };
  ctx.dom.jcTo = { value: "" };
  ctx.dom.jcResult = { innerHTML: "" };
  ctx.dom.jcStatus = null;
  ctx.app.journeyPickerValue = (name, atco) => atco;
  ctx.app.checkJourney = async () => {};
  ctx.app.scrollPanelTo = () => {};
  await ctx.app.runJourneyPreset({ id: "lancing-to-brighton", from: "4400AD0064", to: "1" });
  assert.deepEqual(ctx.names(), ["journey-preset-lancing-to-brighton"]);
});

test("a councillor letter counts the objective, never the postcode or the letter", () => {
  const ctx = counting();
  ctx.state.objectives = [{ id: "night-service-west" }];
  ctx.app.objectiveAuthorities = () => [];
  ctx.app.renderCouncillorDialog = () => {};
  const dialog = { open: false, showModal() { this.open = true; } };
  ctx.app.document = { getElementById: (id) => id === "councillor-dialog" ? dialog : { focus() {} } };
  ctx.app.openCouncillorDialog("night-service-west");

  const letter = "Dear Councillor, I live at BN43 5AB and the last bus is too early.";
  ctx.app.currentCouncillorDraft = () => letter;
  ctx.app.councillorMailtoFromDraft = (t) => ({ url: `mailto:a@b?body=${encodeURIComponent(t)}` });
  ctx.app.window.location = {};
  ctx.app.onCouncillorOpenMail();

  assert.deepEqual(ctx.names(), ["councillor-open-night-service-west", "councillor-email-night-service-west"]);
  assert.doesNotMatch(JSON.stringify(ctx.sent), /bn43|5ab|dear|councillor,/i);
});

test("a submission counts its kind once it is accepted, and nothing from the form", async () => {
  const ctx = counting();
  ctx.CONFIG.SUBMIT_ENDPOINT = "https://submit.example/";
  ctx.app.turnstileToken = () => "";
  ctx.app.fetch = async () => ({ ok: true, status: 200, json: async () => ({ ok: true, url: "u", number: 1 }) });
  await ctx.app.postSubmission("bus_issue", { details: "call me on 07700 900123", publishAck: true });
  assert.deepEqual(ctx.names(), ["submission-bus-issue"]);
  assert.doesNotMatch(JSON.stringify(ctx.sent), /07700/);
});

test("a rejected submission is not counted", async () => {
  const ctx = counting();
  ctx.CONFIG.SUBMIT_ENDPOINT = "https://submit.example/";
  ctx.app.turnstileToken = () => "";
  ctx.app.fetch = async () => ({ ok: false, status: 429, json: async () => ({ error: "rate" }) });
  await ctx.app.postSubmission("idea", { publishAck: true });
  assert.deepEqual(ctx.names(), []);
});

test("a gap alert is counted when it first appears, not on every refresh", () => {
  const ctx = counting();
  ctx.dom.gapMonitor = { hidden: true, innerHTML: "", querySelector: () => null, querySelectorAll: () => [] };
  ctx.dom.gapMonitorLive = { textContent: "" };
  ctx.dom.gapAlertBtn = null;
  const stops = [{ atco: "a", name: "Shoreham High Street", next: [] }];
  const alert = { atco: "a", name: "Shoreham High Street", minutes: 30, from: "12:30", to: "13:00",
                  timetable_minutes: 10, not_reporting: 0, alert: true };
  const data = { active: true, as_of: "2026-09-16T12:30:00+01:00", directions: [
    { id: "brighton", label: "A259 Coast Rd towards Brighton", towards: "towards Brighton",
      status: "alert", alert, stops }] };
  ctx.app.renderGapMonitor(data);
  ctx.app.renderGapMonitor({ ...data });
  ctx.app.renderGapMonitor({ ...data });
  assert.deepEqual(ctx.names(), ["gap-alert-brighton"]);
});

test("a normal service is never counted, however often it refreshes", () => {
  const ctx = counting();
  ctx.dom.gapMonitor = { hidden: true, innerHTML: "", querySelector: () => null, querySelectorAll: () => [] };
  ctx.dom.gapMonitorLive = { textContent: "" };
  ctx.dom.gapAlertBtn = null;
  const data = { active: true, as_of: "x", directions: [
    { id: "worthing", label: "L", towards: "t", status: "normal", alert: null,
      stops: [{ name: "S", next: [] }] }] };
  for (let i = 0; i < 5; i++) ctx.app.renderGapMonitor(data);
  assert.deepEqual(ctx.names(), []);
});

test("a cold start is counted once a page load, however many calls wait on it", () => {
  const ctx = counting();
  ctx.dom.wakingBanner = { classList: { remove() {}, add() {} } };
  ctx.dom.wakingText = { textContent: "" };
  ctx.app.showWakingBanner();
  ctx.app.showWakingBanner();
  ctx.app.showWakingBanner();
  assert.deepEqual(ctx.names(), ["api-waking"]);
});

test("an accessibility change counts which setting, not the reader", () => {
  const ctx = counting();
  ctx.app.applyA11ySettings = () => {};
  ctx.app.saveA11ySettings = () => {};
  ctx.state.a11y = ctx.app.normaliseA11y({});
  ctx.app.updateA11y({ textScale: 1.4 });
  ctx.app.updateA11y({ cvd: true });
  ctx.app.updateA11y({ reduceMotion: false });
  assert.deepEqual(ctx.names(), ["a11y-text-largest", "a11y-colour-blind-on", "a11y-reduce-motion-off"]);
});

// ── The switch on privacy.html ──────────────────────────────
// PECR's analytics exemption, as amended in 2026, rests on readers having a
// simple, free way to object. Not every browser sends a privacy signal, so the
// privacy page has a switch, and every loader has to obey it.

test("a reader who switched counting off on the privacy page is never counted", () => {
  const { app, CONFIG } = fresh();
  CONFIG.GOATCOUNTER_CODE = "worthingbrightonbus";
  const optedOut = { getItem: (k) => (k === "analytics-opt-out" ? "1" : null) };
  const notOptedOut = { getItem: () => null };
  assert.equal(app.analyticsAllowed({}, LIVE_SITE, {}, optedOut), false);
  assert.equal(app.analyticsAllowed({}, LIVE_SITE, {}, notOptedOut), true);
});

test("storage that throws is not taken as an opt-out, and does not break the check", () => {
  const { app, CONFIG } = fresh();
  CONFIG.GOATCOUNTER_CODE = "worthingbrightonbus";
  const blocked = { getItem: () => { throw new Error("SecurityError"); } };
  assert.doesNotThrow(() => app.analyticsAllowed({}, LIVE_SITE, {}, blocked));
});

test("the switch on the privacy page uses the key the loaders check", () => {
  const { app } = fresh();
  const key = vm.runInContext("ANALYTICS_OPT_OUT_KEY", app);
  const privacy = readFileSync(new URL("../privacy.html", import.meta.url), "utf8");
  const pageLoader = readFileSync(new URL("../analytics-page.js", import.meta.url), "utf8");
  assert.match(privacy, new RegExp(`var KEY = "${key}"`));
  assert.match(pageLoader, new RegExp(`getItem\\("${key}"\\) === "1"`));
});
