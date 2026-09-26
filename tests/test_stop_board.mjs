/**
 * The stop board, the Bus tab's stops ahead, and the help a visitor needs to
 * read them — polished for a launch where most readers will not know the area.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { loadApp, ROOT } from "./load_app.mjs";

const app = loadApp();
const state = vm.runInContext("state", app);
const INDEX = readFileSync(join(ROOT, "index.html"), "utf8");
const APP = readFileSync(join(ROOT, "app.js"), "utf8");

const inMinutes = (m) => new Date(Date.now() + m * 60_000).toISOString();
const row = (over = {}) => app.buildDepartureRow({
  service: "700", operator: "SCSO", destination: "Brighton Pier",
  aimed_departure: inMinutes(10), expected_departure: null,
  status: "Scheduled", delay_seconds: null, ...over,
});

// ── Live or timetable, on every row ─────────────────────────

test("a tracked bus carries the live dot, with words behind it", () => {
  const html = row({ expected_departure: inMinutes(12), delay_seconds: 120, status: "Late" });
  assert.match(html, /class="live-dot"/);
  assert.match(html, /visually-hidden">, live</);
});

test("a timetable-only row has no dot and says Timetable", () => {
  const html = row();
  assert.doesNotMatch(html, /class="live-dot"/,
    "a timetable time was marked as one we can see");
  assert.match(html, />Timetable</);
  assert.match(html, /visually-hidden">, timetable</);
});

test("a late prediction says by how much, not just Delayed", () => {
  // TransportAPI rows arrive with status "Late" and the delay beside it. The
  // status word used to win, so eleven minutes late read as "Delayed".
  const chip = app.buildStatusChip({ status: "Late", delay_seconds: 660 });
  assert.equal(chip.label, "11 min late");
  assert.equal(app.buildStatusChip({ status: "Early", delay_seconds: -120 }).label, "2 min early");
  assert.equal(app.buildStatusChip({ status: "Late" }).label, "Delayed",
    "with no minutes known, the operator's word is all there is");
});

test("the badge takes its colour from the operator the API names", () => {
  // Departures carry `operator`; this read `operator_ref`, which only
  // vehicles have, so every route without a colour of its own was default.
  const seen = [];
  const original = app.getRouteColour;
  app.getRouteColour = (svc, op) => { seen.push(op); return "#123456"; };
  try { row({ operator: "BHBC", service: "2" }); } finally { app.getRouteColour = original; }
  assert.deepEqual(seen, ["BHBC"]);
});

test("route buttons run in the order people read them", () => {
  const sorted = ["N700", "700", "10", "9", "2A", "2"].sort(app.compareServiceLabels);
  assert.deepEqual(Array.from(sorted), ["2", "2A", "9", "10", "700", "N700"]);
});

test("the board has a key, a filter and a way to learn to read it", () => {
  assert.match(INDEX, /id="board-filter"[^>]*role="group"/);
  assert.match(INDEX, /class="board-key"/, "the header key is hidden on a phone; nothing replaced it");
  assert.match(INDEX, /<details class="board-help"/);
  assert.match(INDEX, /TransportAPI and the buses' own location reports/,
    "the footer still credits one source for times that come from three");
});

test("Try again asks once", () => {
  const binds = APP.match(/dom\.panelRetryBtn\.addEventListener/g) || [];
  assert.equal(binds.length, 1, "each press sent two requests");
});

// ── Stops ahead on the Bus tab ──────────────────────────────

const at = (m) => inMinutes(m);
const stopRow = (i, over = {}) => ({
  stop_id: `S${i}`, stop_name: `Stop ${i}`, seq: i,
  scheduled: at(i * 2), expected: at(i * 2 + 3), lateness_secs: 180,
  timing_point: null, passed: false, is_next: false, is_terminus: false, ...over,
});

function upcoming(details, expanded = false) {
  state.busDetailsLoading = false;
  state.selectedVehicleRef = "BUS-1";
  state.upcomingExpanded = expanded ? { "BUS-1": true } : {};
  state.busDetails = details;
  return app.buildUpcomingStopsHtml();
}

test("the list shows where the bus is: the stop passed, then the next", () => {
  const stops = [stopRow(1, { passed: true, expected: null, lateness_secs: null }),
                 stopRow(2, { is_next: true }), stopRow(3), stopRow(4, { is_terminus: true })];
  const html = upcoming({ source: "trip", upcoming_stops: stops,
                          vehicle: { trip_source: "feed", recorded_at: at(-2) } });
  assert.match(html, /upcoming-stop--passed[\s\S]*Passed/);
  assert.match(html, /upcoming-stop--next[\s\S]*Next stop/);
  assert.match(html, /3 min late/, "the stops ahead do not say how late the bus will be");
  assert.match(html, /was \d\d:\d\d/, "the timetabled time was not shown beside the estimate");
  assert.match(html, /be at your stop by the timetabled time/,
    "an estimate that errs late went out without the advice that makes it safe");
});

test("a long route folds, and the end of the journey stays in view", () => {
  const stops = Array.from({ length: 20 }, (_, i) =>
    stopRow(i + 1, { is_next: i === 0, is_terminus: i === 19 }));
  const folded = upcoming({ source: "trip", upcoming_stops: stops, vehicle: { trip_source: "feed" } });
  assert.match(folded, /Show all 20 stops \(13 more\)/);
  assert.match(folded, /Stop 20/, "the terminus disappeared behind the fold");
  assert.doesNotMatch(folded, /Stop 10</);
  const open = upcoming({ source: "trip", upcoming_stops: stops, vehicle: { trip_source: "feed" } }, true);
  assert.match(open, /Stop 10</);
  assert.match(open, /Show fewer stops/);
});

test("a bus that doesn't name its journey gets timetable times, labelled", () => {
  const stops = [stopRow(1, { is_next: true, expected: null, lateness_secs: null }),
                 stopRow(2, { expected: null, lateness_secs: null })];
  const html = upcoming({ source: "trip", upcoming_stops: stops, vehicle: { trip_source: "inferred" } });
  assert.match(html, /timetable times/);
  assert.match(html, /doesn't say which journey it's running/);
  assert.doesNotMatch(html, /min late|On time/, "an inferred journey was given a punctuality");
});

test("an unmatched bus says so instead of losing the section", () => {
  const html = upcoming({ source: "none", upcoming_stops: [], vehicle: {} });
  assert.match(html, /couldn't match this bus to a timetable/);
});

test("every stop in the list opens its own board", () => {
  const html = upcoming({ source: "trip", upcoming_stops: [stopRow(1, { is_next: true })],
                          vehicle: { trip_source: "feed" } });
  assert.match(html, /<button type="button" class="upcoming-stop-open"[\s\S]*data-atco="S1"/);
});

// ── Stops near me ───────────────────────────────────────────

test("near me lists the closest stops first, with distance and direction", () => {
  state.stopData = {
    A: { name: "Far", lat: 50.8300, lon: -0.3700, towards: "Worthing", services: ["700"] },
    B: { name: "Near", lat: 50.8101, lon: -0.3731, towards: "Brighton", services: ["700", "9"] },
    C: { name: "Middle", lat: 50.8150, lon: -0.3731 },
  };
  const found = app.nearestStops(50.8100, -0.3730);
  assert.deepEqual(Array.from(found, f => f.name), ["Near", "Middle", "Far"]);
  assert.match(found[0].mode, /^\d+ m · towards Brighton · 700, 9$/);
});

test("near me does not pretend a stop 30 km away is near", () => {
  state.stopData = { A: { name: "Worthing", lat: 50.81, lon: -0.37 } };
  assert.equal(app.nearestStops(51.5, -0.12).length, 0);
});

// ── Tickets on the Bus tab come from the checked file ───────

test("the ticket box reads prices from ticket_zones.json, not a second list", () => {
  const data = JSON.parse(readFileSync(join(ROOT, "data/ticket_zones.json"), "utf8"));
  state.ticketZones = data.zones;
  state.ticketFaresMeta = data.fares_meta;
  state.ticketOperatorNotes = data.operator_notes;
  const sc = app.buildTicketInfoHtml("SCSO", null, "700");
  assert.match(sc, /£6\.00 a day/);
  assert.doesNotMatch(sc, /£5\.50/, "the hand-typed DayRider price came back");
  assert.match(sc, /Prices checked/);
  const compass = app.buildTicketInfoHtml("COMT", null, "1");
  assert.doesNotMatch(compass, /Day tickets available on bus/,
    "Compass publishes no day ticket, and the box said it did");
  assert.match(compass, /£30\.00 for 7 days/);
  assert.match(app.buildTicketInfoHtml("METR", null, "270"), /Metrovoyager/,
    "Metrobus had no ticket box at all");
  assert.match(app.buildTicketInfoHtml("SCSO", null, "N700"), /N700 night bus/);
});

// ── Where a stop is (NaPTAN) ────────────────────────────────

test("a stop reads as the flag and the street say it", () => {
  state.stopData = {
    D: { name: "Chapel Road", towards: "Brighton", locality: "Worthing" },
    O: { name: "Cuthbert Road", locality: "Brighton" },
  };
  state.stopDetails = {
    D: ["Stop D", "Chapel Road", "Boots", "S"],
    O: ["opp", "Freshfield Road", "The Cuthbert", "NE"],
  };
  assert.equal(app.stopContextLine("D"), "Stop D · Towards Brighton · Worthing");
  assert.equal(app.stopWhereLine("O"), "Opposite The Cuthbert, Freshfield Road");
  // No "towards" from the timetable: the bearing stands in.
  assert.equal(app.stopContextLine("O"), "Buses heading north-east · Brighton");
  assert.equal(app.stopWhereLine("D"), "On Chapel Road");
  state.stopDetails = null;
  assert.equal(app.stopContextLine("O"), "Brighton", "without NaPTAN the line must still read");
});

// ── Published disruptions ───────────────────────────────────

test("a disruption shows its own words, scope, dates and author", () => {
  const html = app.buildDisruptionsHtml([{
    id: "D1", summary: "700 diverted via Church Road", description: "Eastbound only.",
    advice: "Use the Church Road stop.", publisher: "WestSussexCC",
    ends: "2026-10-03T18:00:00+00:00", starts: null, link: "https://example.org/x",
    lines: [{ operator: "SCSO", line: "700" }], stops: [], operators: [],
  }]);
  assert.match(html, /<details class="disruption">/);
  assert.match(html, /700 diverted via Church Road/);
  assert.match(html, /Service 700 · Until Sat 3 Oct/);
  assert.match(html, /Advice:<\/strong> Use the Church Road stop/);
  assert.match(html, /Published by WestSussexCC through the Bus Open Data Service/);
  assert.equal(app.buildDisruptionsHtml([]), "");
});

test("an affected row points at the notice, and a cancelled one says so", () => {
  const tagged = row({ disruption_ids: ["D1"] });
  assert.match(tagged, /row-disruption[\s\S]*Disruption, see notice above/);
  const cancelled = row({ status: "Cancelled", vehicle_ref: "15621" });
  assert.match(cancelled, /departure-row--cancelled/);
  assert.match(cancelled, />Cancelled</);
  assert.match(cancelled, /data-vehicle="15621"/, "the named bus was not kept for the row's click");
});
