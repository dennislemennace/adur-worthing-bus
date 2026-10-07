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
  assert.match(html, /<s class="upcoming-stop-was">\d\d:\d\d<\/s>/,
    "the timetabled time was not struck through beside the estimate");
  assert.match(html, /upcoming-stop-time--live"><span class="live-dot"><\/span>\d\d:\d\d/,
    "the estimate is not the main time, or lost its live dot");
  assert.match(html, /timetabled \d\d:\d\d\. Show departures/,
    "a screen reader never hears the time the estimate replaced");
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
  assert.doesNotMatch(html, /upcoming-stop-time--live|<s class/,
    "a timetable time was dressed as a live one");
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

test("an on-time stop keeps its one time, uncrossed", () => {
  const stops = [stopRow(1, { is_next: true, expected: at(2), lateness_secs: 0 })];
  const html = upcoming({ source: "trip", upcoming_stops: stops, vehicle: { trip_source: "feed" } });
  assert.match(html, /upcoming-stop-time--live/);
  assert.doesNotMatch(html, /<s class/, "an on-time bus had its own time crossed out");
});

test("the Bus tab heading is the destination, and the icon is the route's livery", () => {
  const shell = APP.slice(APP.indexOf("function buildBusTabShell"), APP.indexOf("function patchHtml"));
  assert.match(shell, /iconForService\(v\.operator_ref, service\)/,
    "the panel drew the operator's generic bus, not the livery on the map");
  assert.doesNotMatch(shell, /<dt>(Destination|Journey)<\/dt>/,
    "the destination and journey rows came back");
  assert.doesNotMatch(shell, /bus-info-journey/, "the journey start time came back under the operator");
});

// ── Closed stops and diversions, recorded by hand ───────────

const closure = {
  id: "c1", summary: "Western Road closed between Holland Road and Montpelier Road",
  until: "2026-11-06T23:59:00+00:00", publisher: "Brighton & Hove Buses",
  source_url: "https://www.buses.co.uk/service-updates", checked_on: "2026-09-28",
  diversions: [{ towards: "Hove", routes: "All routes except 1X, 2 and 46",
                 via: ["Montpelier Road", "Davigdor Road", "Holland Road"], rejoins: "Palmeira Square" }],
  still_served: [{ atco: "149000007948", name: "Palmeira Square", metres: 340, direction: "west",
                   routes: ["1", "2", "5"], side: "after" }],
};

test("a closed stop says so, until when, and where to go instead", () => {
  const html = app.buildStopClosureHtml(closure);
  assert.match(html, /No buses stop here until Fri 6 Nov/);
  assert.match(html, /class="stop-closure-alt" data-atco="149000007948"/,
    "the nearest stop still served is not a way into its own board");
  assert.match(html, /340 m west · 1, 2, 5/);
  assert.match(html, /Towards Hove<\/strong>: All routes except 1X, 2 and 46 go via Montpelier Road, Davigdor Road and Holland Road, back on the usual route at Palmeira Square/);
  assert.match(html, /From Brighton &amp; Hove Buses&#39; own notice, checked 28 Sept? 2026|From Brighton &amp; Hove Buses' own notice, checked 28 Sept? 2026/);
  assert.equal(app.buildStopClosureHtml(null), "");
});

test("a row at a closed stop is not served, and promises no live time", () => {
  const html = row({ not_served: true, status: "Not served" });
  assert.match(html, /departure-row--cancelled/);
  assert.match(html, />Not served</);
  assert.match(html, /Not stopping here, see above/);
  assert.doesNotMatch(html, /class="live-dot"/);
});

test("a notice we recorded says whose words and when we checked", () => {
  const html = app.buildDisruptionsHtml([{ id: "c1", summary: "Western Road closed", source: "curated",
    publisher: "Brighton & Hove Buses", checked_on: "2026-09-28", link: "https://www.buses.co.uk/service-updates",
    lines: [{ operator: "BHBC", line: "1" }], operators: [], diversions: closure.diversions }]);
  assert.match(html, /own notice, checked/);
  assert.doesNotMatch(html, /through the Bus Open Data Service/, "a hand-copied notice claimed to come from BODS");
  assert.match(html, /disruption-diversions/);
});

test("a stop the bus goes round is marked, with no time promised", () => {
  const html = upcoming({ source: "trip", vehicle: { trip_source: "feed" }, upcoming_stops: [
    stopRow(1, { not_served: true, expected: null, lateness_secs: null }),
    stopRow(2, { is_next: true })] });
  assert.match(html, /upcoming-stop--not-served[\s\S]*Not served: on diversion/);
  const closedRow = html.split("upcoming-stop--next")[0];
  assert.match(closedRow, /aria-label="Stop 1, not served, the bus is on a diversion/,
    "a screen reader was read the closed stop's timetabled time as if the bus would call");
  assert.doesNotMatch(closedRow, /upcoming-stop-time[^>]*>\d\d:\d\d/,
    "the closed stop still shows a time, which reads as a promise");
  assert.doesNotMatch(html.split("upcoming-stop--next")[0], /upcoming-stop-time--live/,
    "the stop not served was given a live estimate");
});


// ── Order ───────────────────────────────────────────────────

test("the board runs soonest first, by live time where there is one", () => {
  const rows = app.sortBySoonest([
    { service: "5", aimed_departure: inMinutes(-10), expected_departure: inMinutes(8) },
    { service: "1", aimed_departure: inMinutes(0), expected_departure: inMinutes(0) },
    { service: "12", aimed_departure: inMinutes(3), expected_departure: null },
    { service: "18", aimed_departure: inMinutes(5) },
  ]);
  assert.deepEqual(Array.from(rows, r => r.service), ["1", "12", "18", "5"],
    "a bus running late sat above ones that will come before it");
});

// ── The route's registration ────────────────────────────────

test("the Bus tab says who pays for a route and when it last changed", () => {
  state.registrations = { as_of: "2026-09-29", source_url: "https://www.data.gov.uk/dataset/x",
    services: [
      { operator: "BHBC", service: "6", registered_as: "1", reg_no: "PK0001213/13",
        start: "Brighton Station", finish: "Downs Park", subsidy: "No", subsidised_by: [],
        changes: [{ variation: 66, effective: "2026-04-13", change: "Vary route <b>", short_notice: false },
                  { variation: 65, effective: "2025-06-08", change: "Timetable", short_notice: true }] },
      { operator: "SCSO", service: "1", registered_as: null, reg_no: "PK0002571/45",
        start: "Worthing", finish: "Midhurst", subsidy: "In Part",
        subsidised_by: ["West Sussex County Council"], changes: [] }] };
  // Metrobus runs under Brighton & Hove's licence.
  const html = app.buildRegistrationHtml("METR", "6");
  assert.match(html, /About the 6/);
  assert.match(html, /registered with the 1/, "a bundled registration did not say so");
  assert.match(html, /None: the operator runs it commercially/);
  assert.match(html, /13 Apr 2026: Vary route &lt;b&gt;/, "register text was not escaped");
  assert.match(html, /1 earlier change since 2019/);
  assert.match(html, /short notice/);
  assert.match(app.buildRegistrationHtml("SCSO", "1"),
    /Partly paid for by West Sussex County Council/);
  assert.equal(app.buildRegistrationHtml("BHBC", "1"), "",
    "Stagecoach's registration was shown on a Brighton & Hove bus of the same number");
  state.registrations = null;
});

// ── A device clock that is out ──────────────────────────────

test("a device clock minutes out is corrected from the server's Date", () => {
  const sent = Date.parse("2026-09-29T12:00:00Z");
  // The device says 12:00; the server said 12:03 in the middle of the request.
  const skew = app.recordClockSkew(sent, sent + 400, "Tue, 29 Sep 2026 12:03:00 GMT");
  assert.ok(Math.abs(skew - 180_300) < 1000, `skew ${skew}`);
  assert.ok(Math.abs(app.nowMs() - (Date.now() + skew)) < 50);
  // A few seconds is latency and rounding, and is left alone.
  assert.equal(app.recordClockSkew(sent, sent + 400, "Tue, 29 Sep 2026 12:00:05 GMT"), 0);
  assert.equal(app.recordClockSkew(sent, sent + 400, "not a date"), 0);
});

// ── A tap that could mean two markers ───────────────────────

test("a tap within reach of a bus and a stop finds both, nearest first", () => {
  const near = app.markersWithinReach({ x: 100, y: 100 }, [
    { kind: "stop", id: "S", x: 110, y: 104 },
    { kind: "bus", id: "B", x: 103, y: 99 },
    { kind: "stop", id: "FAR", x: 140, y: 100 },
  ]);
  assert.deepEqual(Array.from(near, c => c.id), ["B", "S"],
    "the far stop was offered, or the nearest was not first");
  assert.equal(app.markersWithinReach({ x: 0, y: 0 }, [{ x: 30, y: 0 }]).length, 0);
});

test("a National Express coach shows no local fares", () => {
  assert.equal(app.buildTicketInfoHtml("NATX", null, "025"), "",
    "local tickets and the bus fare cap were offered for a coach");
});

// ── A row opens its own journey ────────────────────────────────

test("a board row opens the bus running its journey, never the nearest with the same number", () => {
  // The 700 in 14 min and the 700 in 35: the later row used to open the
  // earlier bus, the one nearest the stop.
  const app = loadApp();
  const bus = (ref, trip, extra = {}) => ({ _vehicle: { vehicle_ref: ref, service_ref: "700",
    declared_trip_id: trip, trip_source: "feed", ...extra } });
  vm.runInContext("state", app).busMarkers = {
    "BUS-A": bus("BUS-A", "VJ_A"), "BUS-B": bus("BUS-B", "VJ_B"),
    "BUS-X": bus("BUS-X", "VJ_X", { trip_source: undefined }),   // declaration contradicted
  };
  const pick = vm.runInContext("departureBus", app);
  const tr = data => ({ dataset: data });
  assert.equal(pick(tr({ service: "700", trip: "VJ_B" })).vehicle_ref, "BUS-B");
  assert.equal(pick(tr({ service: "700", trip: "VJ_A", vehicle: "BUS-B" })).vehicle_ref, "BUS-B",
    "a bus the board names comes first");
  assert.equal(pick(tr({ service: "700", trip: "VJ_C" })), null, "a journey with no bus on it is not guessed");
  assert.equal(pick(tr({ service: "700" })), null, "a row with no journey is not guessed");
  assert.equal(pick(tr({ service: "700", trip: "VJ_X" })), null, "a contradicted declaration is not trusted");
});

test("a journey not yet on the road is shown from the timetable, and says so", () => {
  const app = loadApp();
  const html = vm.runInContext("buildDeparturePlanHtml", app)({
    service: "700", headsign: "Worthing Pier", journey_start: "15:05",
    calls: Array.from({ length: 15 }, (_, i) => ({ atco: `S${i}`, name: `Stop ${i}`, time: `15:${String(10 + i).padStart(2, "0")}` })),
  }, { onRoad: false, service: "700" }, 14 * 60 + 50);
  assert.match(html, /This 700 to Worthing Pier has not set off yet/);
  assert.match(html, /It starts at 15:05\. No bus is running it yet/);
  assert.equal((html.match(/<li>/g) || []).length, 12);
  assert.match(html, /And 3 more stops to\s+Stop 14/);
  // Past its start with no bus declaring it, it may be running untracked.
  const late = vm.runInContext("buildDeparturePlanHtml", app)({ service: "700", journey_start: "15:05", calls: [] },
    { onRoad: false, service: "700" }, 15 * 60 + 8);
  assert.match(late, /This 700 is not being tracked[\s\S]*was due to start at 15:05/);
  assert.doesNotMatch(late, /has not set off/);
  // Just after midnight, a 23:50 start is past; a 00:20 start at 23:55 is not.
  const build = vm.runInContext("buildDeparturePlanHtml", app);
  assert.match(build({ service: "N7", journey_start: "23:50", calls: [] }, { service: "N7" }, 5), /not being tracked/);
  assert.match(build({ service: "N7", journey_start: "00:20", calls: [] }, { service: "N7" }, 23 * 60 + 55), /has not set off yet/);
  const hidden = vm.runInContext("buildDeparturePlanHtml", app)({ service: "700", calls: [] }, { onRoad: true, service: "700" });
  assert.match(hidden, /is not on the map[\s\S]*bus filter may be hiding it/);
});
