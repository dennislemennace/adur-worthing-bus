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

// ── RTIG wording (RTIGT035-1.0, RTIGT041-1.0 section 10) ─────

test("Due is a live bus under a minute away, never a timetable time", () => {
  const now = new Date("2026-10-26T12:00:00Z");
  const at = (s) => new Date(now.getTime() + s * 1000).toISOString();
  assert.equal(app.formatDueTime(at(50), now), "Due");
  assert.equal(app.formatDueTime(at(75), now), "1 min", "75 s is not Due");
  assert.match(app.formatDueTime(at(50), now, false), /^\d\d:\d\d$/,
    "a timetable time a minute away was called Due");
  assert.equal(app.formatDueTime(at(300), now, false), "5 mins", "timetable rows still count down");
});

test("under the countdown, the live and scheduled clock times, one a line", () => {
  const live = row({ aimed_departure: "2026-10-26T12:05:00Z", expected_departure: "2026-10-26T12:07:00Z",
    delay_seconds: 120 });
  assert.match(live, /class="due-clock-live">Live 12:07</);
  assert.match(live, /class="due-clock-sched">Scheduled 12:05</);
  assert.ok(live.indexOf("due-time") < live.indexOf("due-clock-live")
    && live.indexOf("due-clock-live") < live.indexOf("due-clock-sched"), "countdown, then Live, then Scheduled");
  const timetable = row({ aimed_departure: "2026-10-26T12:16:00Z" });
  assert.match(timetable, /class="due-clock-sched">Scheduled 12:16</);
  assert.doesNotMatch(timetable, /due-clock-live/, "a timetable time was called live");
  assert.doesNotMatch(live + timetable, /Sched\b(?!uled)/, "Scheduled was shortened");
  assert.match(live, /data-live="1"/);
  assert.match(timetable, /data-live="0"/);
});

test("on time at a stop is up to 2 minutes late; on the Bus tab, a minute either way", () => {
  assert.equal(app.buildStatusChip({ delay_seconds: 110 }, true).label, "On time");
  assert.equal(app.buildStatusChip({ delay_seconds: 130 }, true).label, "+2 mins");
  assert.equal(app.buildStatusChip({ delay_seconds: -70 }, true).label, "1 min early");
  assert.equal(app.buildStatusChip({ delay_seconds: 110 }).label, "2 min late",
    "the Bus tab says where the bus is against its timetable, within a minute");
  assert.match(row({ expected_departure: inMinutes(12), delay_seconds: 100 }), />On time</);
});

test("the board help says what its times are, in RTIG's terms", () => {
  const help = INDEX.slice(INDEX.indexOf('id="board-help"'), INDEX.indexOf("</details>", INDEX.indexOf('id="board-help"')));
  assert.match(help, /leaves? this stop/i, "the help no longer says these are departure times");
  assert.match(help, /up to 2 minutes late/);
  assert.doesNotMatch(help, /real[- ]time|headway/i);
  assert.match(help, /<strong>Live<\/strong>[\s\S]*<strong>Scheduled<\/strong>/,
    "the help does not name the clock times the rows show");
  assert.match(help, /<strong>\+3 mins<\/strong>/, "the help does not say what +3 mins means");
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
  assert.match(html, /class="upcoming-stop-line">Passed Stop 1/);
  assert.match(html, /upcoming-stop--next[\s\S]*Next stop/);
  assert.match(html, /3 min late/, "the stops ahead do not say how late the bus will be");
  assert.match(html, /class="upcoming-stop-sched">Scheduled \d\d:\d\d</,
    "the timetabled time is not under the estimate");
  assert.match(html, /upcoming-stop-time--live"><span class="live-dot"><\/span>\d\d:\d\d/,
    "the estimate is not the main time, or lost its live dot");
  assert.match(html, /timetabled \d\d:\d\d\. Show departures/,
    "a screen reader never hears the time the estimate replaced");
  assert.match(html, /be at your stop by the timetabled time/,
    "an estimate that errs late went out without the advice that makes it safe");
});

test("a time that allows for a diversion says so, on the Bus tab and the board", () => {
  const stops = [stopRow(1, { is_next: true }),
                 stopRow(2, { allowance_secs: 180, allowance_reason: "the Western Road diversion" }),
                 stopRow(3, { allowance_secs: 300, allowance_reason: "the Western Road diversion" })];
  const html = upcoming({ source: "trip", upcoming_stops: stops,
                          vehicle: { trip_source: "feed", recorded_at: at(-2) } });
  assert.match(html, /Times past the Western Road diversion\s+include up to 5 min for it/);
  const plain = upcoming({ source: "trip", upcoming_stops: [stopRow(1, { is_next: true })],
                           vehicle: { trip_source: "feed" } });
  assert.doesNotMatch(plain, /diversion/);
  const board = row({ expected_departure: inMinutes(14), delay_seconds: 240, status: "Late",
                      allowance_secs: 170, allowance_reason: "the Western Road diversion",
                      disruption_ids: ["x"] });
  assert.match(board, /Time allows 3 min for the Western Road diversion, see notice above/);
  assert.doesNotMatch(row({ allowance_secs: 170 }), /Time allows/,
    "a timetable-only row has no estimate for an allowance to be in");
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

test("a stop says what OpenStreetMap records, credits it, and asks for what is missing", () => {
  state.stopOsmMeta = { osm_base: "2026-10-13T19:33:34Z", licence_url: "https://opendatacommons.org/licenses/odbl/1-0/" };
  state.stopOsm = {
    FULL: { id: 42, shelter: "yes", bench: "no", lit: "yes", wheelchair: "limited", kerb: "raised",
            tactile_paving: "yes", check_date: "2025-03" },
    SOME: { id: 7, shelter: "yes" },
    BARE: { id: 8 },
  };
  const full = app.stopAccessHtml("FULL");
  for (const words of ["Shelter", "No seating", "Lit at night", "Partly wheelchair accessible",
                       "Raised kerb", "Tactile paving"]) {
    assert.match(full, new RegExp(words), words);
  }
  assert.doesNotMatch(full, /OpenStop/, "nothing to ask for when access is all recorded");
  assert.match(full, /© OpenStreetMap contributors/);
  assert.match(full, /openstreetmap\.org\/node\/42/);
  assert.match(full, /checked Mar 2025/);

  const some = app.stopAccessHtml("SOME");
  assert.match(some, /Wheelchair access, kerb height and tactile paving aren't recorded/);
  assert.match(some, /openstop\.app/);
  assert.match(app.stopAccessHtml("BARE"), /Nothing is recorded about this stop/);
  assert.match(app.stopAccessHtml("ABSENT"), /not in OpenStreetMap yet/);
  assert.doesNotMatch(app.stopAccessHtml("ABSENT"), /openstop\.app/,
    "OpenStop can only edit stops already on the map");
  state.stopOsm = null;
  assert.equal(app.stopAccessHtml("FULL"), "", "nothing before the file has loaded");
});

test("the cashless expresses say so, and only they do", () => {
  const data = JSON.parse(readFileSync(join(ROOT, "data/ticket_zones.json"), "utf8"));
  state.ticketZones = data.zones;
  state.ticketFaresMeta = data.fares_meta;
  state.ticketOperatorNotes = data.operator_notes;
  for (const svc of ["1X", "3X", "25X"]) {
    const html = app.buildTicketInfoHtml("BHBC", null, svc);
    assert.match(html, /Cashless/, `${svc} should say it takes no cash`);
    assert.doesNotMatch(html, /\(£[\d.]+ cash\)/, `${svc} should not quote a cash price`);
  }
  // The Coaster expresses and the ordinary routes still take cash.
  for (const svc of ["12X", "13X", "1", "25"]) {
    assert.doesNotMatch(app.buildTicketInfoHtml("BHBC", null, svc), /Cashless/, svc);
  }
  assert.doesNotMatch(app.buildTicketInfoHtml("SCSO", null, "1X"), /Cashless/,
    "another operator's route sharing the number is not cashless");
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

test("a disruption shows its own words, dates and author", () => {
  const html = app.compactDisruptionsHtml([{
    id: "D1", summary: "700 diverted via Church Road", description: "Eastbound only.",
    advice: "Use the Church Road stop.", publisher: "WestSussexCC",
    ends: "2026-10-03T18:00:00+00:00", starts: null, link: "https://example.org/x",
    lines: [{ operator: "SCSO", line: "700" }], stops: [], operators: [],
  }]);
  assert.match(html, /<details class="disruption disruption--compact">/);
  assert.match(html, /class="disruption-summary">700 diverted via Church Road</);
  assert.match(html, /class="disruption-when">Until Sat 3 Oct</);
  assert.match(html, /Advice:<\/strong> Use the Church Road stop/);
  assert.match(html, /Published by WestSussexCC through the Bus Open Data Service/);
  assert.equal(app.compactDisruptionsHtml([]), "");
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

test("the Bus tab heading is the destination, with the route's tile in its colour", () => {
  const shell = APP.slice(APP.indexOf("function buildBusTabShell"), APP.indexOf("function patchHtml"));
  assert.match(shell, /getRouteColour\(service, v\.operator_ref\)/,
    "the heading's tile is not in the route's colour, as on the map");
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
  assert.match(html, /class="stop-closure-alt" href="#stop=149000007948" data-atco="149000007948"/,
    "the nearest stop still served is not a way into its own board");
  assert.match(html, /340 m west · 1, 2, 5/);
  assert.match(html, /Towards Hove<\/strong>: All routes except 1X, 2 and 46 go via Montpelier Road, Davigdor Road and Holland Road, back on the usual route at Palmeira Square/);
  assert.match(html, /From Brighton &amp; Hove Buses&#39; own notice, checked 28 Sept? 2026|From Brighton &amp; Hove Buses' own notice, checked 28 Sept? 2026/);
  assert.equal(app.buildStopClosureHtml(null), "");
  assert.doesNotMatch(html, /all stops on the diversion are served/,
    "the board claimed what this notice never said");
  assert.match(app.buildStopClosureHtml({ ...closure, diversion_stops_served: true }),
    /The operator says all stops on the diversion are served/);
});

test("routes that skip a stop others still serve are named, with today's end time", () => {
  const html = app.buildStopClosureHtml({ ...closure, id: "c1-25", lines: ["25", "25X", "78"],
    until: "2026-10-24T17:00:00+00:00", daily: { from: "07:00", to: "18:00" },
    final_ends: "2026-10-27T23:59:00+00:00" });
  assert.match(html, /Routes 25, 25X and 78 don(&#39;|')t stop here until 18:00/);
  assert.doesNotMatch(html, /No buses stop here/, "a stop other buses still serve was called closed");
  assert.match(app.buildStopClosureHtml({ ...closure, lines: ["700"] }),
    /Route 700 doesn(&#39;|')t stop here until Fri 6 Nov/);
});

test("a notice seen on the road says nobody published it, not whose words it is", () => {
  const seen = { id: "s1", summary: "700 turning at Brighton Station", source: "curated", observed: true,
    publisher: "Stagecoach", checked_on: "2026-10-24", link: "https://www.stagecoachbus.com/service-updates",
    lines: [{ operator: "SCSO", line: "700" }], operators: [], ends: "2026-10-27T23:59:00+00:00" };
  const html = app.compactDisruptionsHtml([seen]);
  assert.match(html, /Seen on the road: Stagecoach has posted no notice \(checked 24 Oct/);
  assert.doesNotMatch(html, /own notice/, "our own words were put in Stagecoach's mouth");
  assert.match(app.buildStopClosureHtml({ ...closure, ...seen, source_url: seen.link }), /Their updates page/);
  const daily = app.compactDisruptionsHtml([{ ...seen, observed: false, daily: { from: "07:00", to: "18:00" },
    final_ends: "2026-10-27T23:59:00+00:00", ends: "2026-10-24T17:00:00+00:00" }]);
  assert.match(daily, /07:00 to 18:00 daily, until Tue 27 Oct/);
});

test("notices about the same works are one notice, each route's own words inside", () => {
  const curated = { source: "curated", operators: [], checked_on: "2026-10-24" };
  const western = { ...curated, id: "w", summary: "Western Road closed", publisher: "Brighton & Hove Buses",
    lines: [{ operator: "BHBC", line: "1" }], ends: "2026-11-06T23:59:00+00:00" };
  const council = { ...curated, id: "w700", same_event_as: "w", summary: "Western Road closed",
    description: "Brunswick Place is not served.", publisher: "Brighton & Hove City Council",
    lines: [{ operator: "SCSO", line: "700" }], ends: "2026-11-12T23:59:00+00:00" };
  const steine = { ...curated, id: "o", summary: "Old Steine roadworks", publisher: "Brighton & Hove Buses",
    lines: [{ operator: "BHBC", line: "7" }], ends: "2026-10-27T23:59:00+00:00" };
  const turn = { ...curated, id: "o700", parent: "o", summary: "700 turning at Brighton Station",
    publisher: "Stagecoach", observed: true, lines: [{ operator: "SCSO", line: "700" }],
    ends: "2026-10-27T23:59:00+00:00" };
  const western700 = { ...western, lines: [...western.lines, { operator: "SCSO", line: "700" }] };
  const html = app.compactDisruptionsHtml([western700, steine, council, turn], [{ service: "700", operator: "SCSO" }]);
  assert.equal(html.match(/<details /g).length, 2, "one works showed as two notices");
  assert.ok(html.indexOf("Western Road closed") < html.indexOf("700 turning"), "the order changed");
  assert.match(html, /class="disruption-when">Until Fri 6 Nov</);
  assert.match(html, /Service 700<\/strong> · Until Thu 12 Nov/, "the 700's own end date was lost");
  assert.match(html, /Brunswick Place is not served[\s\S]*Brighton &amp; Hove City Council/);
  assert.match(html, /700 turning at Brighton Station[\s\S]*Seen on the road: Stagecoach/);
  assert.equal(app.compactDisruptionsHtml([turn]).match(/<details /g).length, 1,
    "a route's notice with its main one not here vanished");
});

// A busy Brighton stop can have five notices across thirty routes: split by
// service, each route read the same works over again; now one per works.
const busy = (() => {
  const curated = { source: "curated", operators: [], checked_on: "2026-10-24" };
  const lines = (op, ...ls) => ls.map(line => ({ operator: op, line }));
  return {
    western: { ...curated, id: "w", summary: "Western Road closed", publisher: "Brighton & Hove Buses",
      lines: lines("BHBC", "1", "25", "49", "60"), ends: "2026-11-06T23:59:00+00:00" },
    steine: { ...curated, id: "o", summary: "Old Steine roadworks", publisher: "Brighton & Hove Buses",
      lines: lines("BHBC", "25", "25X", "49", "60", "78"), ends: "2026-10-27T23:59:00+00:00" },
    council: { ...curated, id: "w700", same_event_as: "w", summary: "Western Road closed for the 700",
      publisher: "Brighton & Hove City Council", lines: lines("SCSO", "700", "N700"),
      ends: "2026-11-12T23:59:00+00:00" },
    here: ["25", "25X", "49", "60", "78", "270", "700", "N25", "N700"]
      .map(service => ({ service, operator: /700/.test(service) ? "SCSO" : "BHBC" })),
  };
})();

test("a notice for no route that calls here is still shown, and never filtered away", () => {
  const stray = { ...busy.steine, id: "x", summary: "Marina works", lines: [{ operator: "BHBC", line: "7" }] };
  const html = app.compactDisruptionsHtml([busy.western, busy.steine, stray], busy.here);
  const card = html.split("<details").find(c => c.includes("Marina works"));
  assert.ok(card, "a notice the API sent for this stop vanished");
  assert.doesNotMatch(card.split(">")[0], /data-services/, "a route chip could hide it");
});

test("a row at a closed stop is not served, and promises no live time", () => {
  const html = row({ not_served: true, status: "Not calling" });
  assert.match(html, /departure-row--cancelled/);
  assert.match(html, />Not calling here</, "RTIGT035's term for a stop the bus skips");
  assert.doesNotMatch(html, /class="live-dot"/);
});

test("a notice we recorded says whose words and when we checked", () => {
  const html = app.compactDisruptionsHtml([{ id: "c1", summary: "Western Road closed", source: "curated",
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
  assert.match(html, /class="upcoming-stop-line upcoming-stop-line--skipped">Not calling at Stop 1: diversion</);
  const closedRow = html.split("upcoming-stop--next")[0];
  assert.doesNotMatch(closedRow, /\d\d:\d\d/,
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

test("the chooser says which way each stop's buses go and which side of the road it is on", () => {
  // Two poles called Grand Avenue, either side of the road and a finger apart
  // on the map: the chooser listed both as "Stop Grand Avenue".
  state.stopData = {
    E: { name: "Grand Avenue", towards: "Brighton", locality: "Hove" },
    W: { name: "Grand Avenue", towards: "Worthing", locality: "Hove" },
    N: { name: "Lancing Station", locality: "Lancing" },
  };
  state.stopDetails = {
    E: ["opp", "Kingsway", "King Alfred", "E"],
    W: ["Stop D", "Kingsway", "", "W"],
    N: ["o/s", "Station Approach", "Lancing Station", "N"],
  };
  const stop = (id) => ({ kind: "stop", id, label: state.stopData[id].name });
  const row = (id) => app.tapChoiceHtml(stop(id));
  const east = row("E"), west = row("W");
  assert.match(east, /Towards Brighton · heading east/);
  assert.match(east, /--heading:\s*90deg/);
  assert.match(west, /Stop D · Towards Worthing · heading west/);
  assert.match(west, /--heading:\s*270deg/);
  assert.match(west, /class="tap-choice-arrow" aria-hidden="true"/, "the arrow is decoration; the words carry it");
  // No "towards" from the timetable: the bearing alone still says which way.
  assert.match(row("N"), /Heading north/);
  // Directions that differ tell the poles apart; the side of the road would
  // only be a third line to read.
  const apart = app.tapChoicesHtml([stop("E"), stop("W")]);
  assert.doesNotMatch(apart, /Opposite King Alfred/, "a side shown where the directions already differ");
  // Two poles the same way, a few metres apart: only the side tells them apart.
  state.stopData.T1 = { name: "Tarring Road", towards: "Worthing", locality: "Worthing" };
  state.stopData.T2 = { name: "Tarring Road", towards: "Worthing", locality: "Worthing" };
  state.stopDetails.T1 = ["opp", "Tarring Road", "The Vine", "W"];
  state.stopDetails.T2 = ["o/s", "Tarring Road", "Station Parade", "W"];
  const alike = app.tapChoicesHtml([stop("T1"), stop("T2"), { kind: "bus", id: "B", label: "6 to Worthing" }]);
  assert.match(alike, /Opposite The Vine, Tarring Road/);
  assert.match(alike, /Outside Station Parade, Tarring Road/);
  assert.doesNotMatch(app.tapChoicesHtml([stop("T1"), stop("E")]), /Opposite The Vine/,
    "one stop of a direction has nothing to be told apart from");
  // A different letter is not a different direction: the side still shows.
  state.stopDetails.T1 = ["Stop A", "Tarring Road", "", "W"];
  state.stopDetails.T2 = ["Stop B", "Tarring Road", "", "W"];
  assert.doesNotMatch(app.tapChoicesHtml([stop("T1"), stop("T2")]), /On Tarring Road/,
    "a street both poles share tells them apart from nothing");
  // Before NaPTAN has loaded, the timetable's "towards" still tells them apart.
  state.stopDetails = null;
  const early = row("E");
  assert.match(early, /Towards Brighton/);
  assert.doesNotMatch(early, /heading|--heading/);
  assert.match(early, /data-atco="E"/, "the row must be findable to fill in once NaPTAN arrives");
  // A bus row is as it was, and every name is escaped.
  const bus = app.tapChoiceHtml({ kind: "bus", id: "B<1", label: "700 to <Brighton>" });
  assert.match(bus, /700 to &lt;Brighton&gt;/);
  assert.doesNotMatch(bus, /Towards|heading|tap-choice-detail/);
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
    // Named by line, first stop and start (Stagecoach, from 7 October 2026).
    "BUS-S": bus("BUS-S", "VJ_S", { trip_source: "feed_start" }),
  };
  const pick = vm.runInContext("departureBus", app);
  assert.equal(pick({ dataset: { service: "700", trip: "VJ_S" } })?.vehicle_ref, "BUS-S",
    "a bus that named its journey by its start was not found");
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

// ── Every service at the stop, and the next times of one with none ──

const plainText = (html) => html.replace(/<[^>]+>/g, " ").replace(/&amp;/g, "&").replace(/&#39;/g, "'")
  .replace(/\s+/g, " ").trim();

test("a route with nothing on the board shows its next times, dated the day they run", () => {
  // Saturday 17 October 2026, 21:00 in London. The 60 runs on weekdays only,
  // so its next bus is Monday's, and must say Monday.
  const now = new Date("2026-10-17T20:00:00Z");
  const data = { days_searched: 8, departures: [
    { service: "60", operator: "BHBC", destination: "Old Steine", aimed_departure: "2026-10-19T09:16:00+01:00" },
    { service: "60", operator: "BHBC", destination: "Old Steine", aimed_departure: "2026-10-20T09:16:00+01:00" },
  ] };
  const text = plainText(app.boardNextHtml(data, "60", now));
  assert.match(text, /^Next 60 from this stop, from the timetable: Monday 19 October 60 09:16 to Old Steine Tuesday 20 October 60 09:16/);
  assert.match(text, /from two hours before it is due/);
  assert.equal(app.boardNextDayLabel("2026-10-17T22:10:00+01:00", now), "Today");
  assert.equal(app.boardNextDayLabel("2026-10-18T06:12:00+01:00", now), "Tomorrow, Sunday 18 October");
  // Half past midnight is tomorrow, whatever service day GTFS files it under.
  assert.equal(app.boardNextDayLabel("2026-10-18T00:30:00+01:00", now), "Tomorrow, Sunday 18 October");
  assert.match(plainText(app.boardNextHtml({ days_searched: 8, departures: [] }, "60", now)),
    /The timetable has no 60 from this stop in the next 8 days/);
});

test("the board's route buttons come from every service at the stop", () => {
  assert.match(APP, /renderBoardFilter\(\[\.\.\.departures\.map\(d => d\.service \|\| "\?"\), \.\.\.here\.map\(h => h\.service\)\]/,
    "the buttons are built from the departures alone");
  assert.match(APP, /renderBoardFilter\(here\.map\(h => h\.service\), hereOperators\)/,
    "an empty board offers no buttons");
  assert.match(INDEX, /id="board-next"[^>]*aria-live="polite"/);
});

// ── The last bus home, from Brighton and from Worthing ──────

const lastDays = (dayBus, nightBus = null) => Object.fromEntries(
  ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    .map(d => [d, { day_bus: dayBus, night_bus: nightBus }]));

test("the last bus home names Brighton and Worthing, never the stops", () => {
  const bus = (depart, service) => ({ depart, service, after_midnight: false });
  const html = app.lastBusesHomeHtml({
    last_from_brighton: { inside_hub: false, week_of: "2026-10-19", days: lastDays(bus("23:58", "700"), bus("03:36", "N700")) },
    last_from_worthing: { inside_hub: false, week_of: "2026-10-19", days: lastDays(bus("23:45", "700")) },
  });
  const text = plainText(html);
  assert.match(text, /Last bus home from Brighton/);
  assert.match(text, /Last bus home from Worthing/);
  assert.doesNotMatch(text, /central|Marine Parade/i);
  assert.match(text, /Mon–Sun 23:58 700 03:36 N700/);
  // Worthing has no night bus here, so no column of "None".
  assert.match(text, /Last bus home from Worthing Days Last bus Mon–Sun 23:45 700/);
  assert.match(text, /or a coach, isn't counted/);
});

test("a stop in one town shows only the other; a stop neither serves says so once", () => {
  const bus = { depart: "23:45", service: "700" };
  const inBrighton = plainText(app.lastBusesHomeHtml({
    last_from_brighton: { inside_hub: true, days: {} },
    last_from_worthing: { inside_hub: false, week_of: "2026-10-19", days: lastDays(bus) } }));
  assert.match(inBrighton, /Last bus home from Worthing/);
  assert.doesNotMatch(inBrighton, /from Brighton/);
  const none = Object.fromEntries(["monday"].map(d => [d, null]));
  const neither = plainText(app.lastBusesHomeHtml({
    last_from_brighton: { inside_hub: false, days: none }, last_from_worthing: { inside_hub: false, days: none } }));
  assert.equal(neither, "Last bus home No direct bus from Brighton or Worthing calls here on any day.");
  assert.equal(app.lastBusesHomeHtml({}), "", "an older API sends neither");
});

// ── Route view: a stop chosen on the map ────────────────────

test("Route view's stop lists each service with when and how often it calls", () => {
  const svc = { service: "700", operator: "SCSO", towards: ["Old Steine", "Worthing"], days: {
    weekday: { first: "05:27", last: "23:56", count: 85, night: 2, every_min: 12 },
    saturday: { first: "05:57", last: "23:56", count: 79, night: 0, every_min: 15 },
    sunday: null } };
  const text = plainText(app.routeStopServiceHtml(svc));
  assert.match(text, /700 Stagecoach South to Old Steine or Worthing/);
  assert.match(text, /Mon–Fri 05:27–23:56, every 12 min, and 2 at night/);
  assert.match(text, /Sat 05:57–23:56, every 15 min/);
  assert.match(text, /Sun No buses/);
  assert.equal(app.routeStopFrequency({ first: "", last: "", count: 4, night: 4 }), "Night only, 4 buses");
  assert.equal(app.routeStopFrequency({ first: "07:48", last: "17:07", count: 7, night: 0, every_min: 70 }),
    "07:48–17:07, about hourly");
  assert.equal(app.routeStopFrequency({ first: "09:16", last: "09:16", count: 1, night: 0, every_min: null }),
    "09:16, 1 bus");
});

test("a stop clicked in Route view opens its panel, and leaving the view closes it", () => {
  assert.match(APP, /if \(state\.viewMode === "improvements"\) \{\s*state\.map\.closePopup\(\);\s*openRouteStop\(atcoCode, stopName\);/);
  assert.match(APP, /if \(state\.viewMode !== "improvements"\) closeRouteStop\(\);/);
  assert.match(INDEX, /id="tab-content-about"[^>]*>\s*<!--[\s\S]*?-->\s*<section class="route-stop" id="route-stop" hidden/);
});

test("a time from the bus's previous journey is marked apart from a bus on the road", () => {
  const html = row({ expected_departure: inMinutes(14), delay_seconds: 240, live_source: "previous_journey" });
  assert.match(html, /live-dot--previous/);
  assert.match(html, /visually-hidden">, estimated from the bus's previous journey</);
  assert.doesNotMatch(html, /visually-hidden">, live</, "it was read out as a bus we can see");
});

// ── The board after the user's mockup (26 October 2026) ─────

test("a late bus on the board is +7 mins, and a screen reader hears 7 min late", () => {
  const html = row({ expected_departure: inMinutes(12), delay_seconds: 420 });
  assert.match(html, /aria-hidden="true">\+7 mins</);
  assert.match(html, /visually-hidden">7 min late</);
  assert.equal(app.buildStatusChip({ delay_seconds: 130 }, true).label, "+2 mins");
  assert.equal(app.buildStatusChip({ delay_seconds: 420, report_age_secs: 300 }, true).label, "about +7 mins");
  assert.equal(app.buildStatusChip({ delay_seconds: -70 }, true).label, "1 min early", "early changed too");
  assert.equal(app.buildStatusChip({ delay_seconds: 130 }).label, "2 min late", "the Bus tab changed too");
});

test("a row the bus will not call at has a dash, and says so once", () => {
  const html = row({ not_served: true, status: "Not calling", aimed_departure: "2026-10-26T12:16:00Z" });
  assert.match(html, /class="due-time[^"]*"[^>]*data-blank="1"\s*>–</);
  assert.match(html, /Scheduled 12:16/);
  assert.match(html, /aria-hidden="true">Not calling here</);
  assert.match(html, /visually-hidden">Not calling here: the notice above says where to catch it</);
  assert.doesNotMatch(html, /row-disruption/, "it was said under the destination too");
});

test("a stop still served is a link to its board, not a button", () => {
  const html = app.buildStopClosureHtml(closure);
  assert.match(html, /<a class="stop-closure-alt" href="#stop=149000007948" data-atco="149000007948"/);
  assert.doesNotMatch(html, /<button/, "a stop still served is a button, too big for the board");
  assert.match(html, /340 m west · 1, 2, 5/);
});

test("routes that skip the stop are told inside the works' notice, not a box of their own", () => {
  // The 25, 25X and 78 at the Old Steine (24 October 2026): their own box
  // stood over the board and pushed the buses out of sight.
  const sub = { ...busy.steine, id: "o25", parent: "o", summary: "Routes 25, 25X and 78 not serving the Old Steine",
    lines: [{ operator: "BHBC", line: "25" }, { operator: "BHBC", line: "25X" }, { operator: "BHBC", line: "78" }],
    daily: { from: "07:00", to: "18:00" }, final_ends: "2026-10-27T23:59:00+00:00" };
  const here = ["25", "25X", "78", "49"].map(service => ({ service, operator: "BHBC" }));
  const note = { ...closure, id: "o25", parent: "o", lines: ["25", "25X", "78"], daily: sub.daily,
    until: "2026-10-26T18:00:00Z", still_served: [{ atco: "149000006999", name: "Valley Gardens",
      metres: 250, direction: "north", routes: ["25", "25X", "78"], side: "both" }] };
  // With the sub-entry's own notice on the board, and without it (no 25 in
  // the next two hours): either way, inside the works' notice.
  for (const disruptions of [[busy.steine, sub], [busy.steine]]) {
    const html = app.boardNoticesHtml({ services_here: here, disruptions, line_closures: [note] });
    assert.doesNotMatch(html, /class="stop-closure"/, "the routes that skip the stop got a box of their own");
    assert.match(html, /25, 25X and 78 not stopping here until 18:00/, "the notice does not say they skip this stop");
    assert.match(html, /<a class="stop-closure-alt" href="#stop=149000006999"[^>]*>Valley Gardens/,
      "where to catch them instead is not a link to that stop's board");
    assert.doesNotMatch(html, /<button/);
    assert.equal((html.match(/<details/g) || []).length, 1, "not one notice for the works");
  }
});

// Royal Pavilion R1 on 26 October 2026 showed five notices, each repeating
// the same two works: one notice per works, and the route chips filter them.
const steineSub = { ...busy.steine, id: "o25", parent: "o", summary: "Routes 25, 25X and 78 not serving the Old Steine",
  lines: [{ operator: "BHBC", line: "25" }, { operator: "BHBC", line: "25X" }, { operator: "BHBC", line: "78" }],
  daily: { from: "07:00", to: "18:00" }, final_ends: "2026-10-27T23:59:00+00:00" };
const steine700 = { ...busy.steine, id: "o700", parent: "o", publisher: "Stagecoach",
  summary: "Old Steine roadworks: after Churchill Square, the 700 diverts to Brighton Station",
  lines: [{ operator: "SCSO", line: "700" }] };
const steineNote = { ...closure, id: "o25", parent: "o", lines: ["25", "25X", "78"], daily: steineSub.daily,
  until: "2026-10-26T18:00:00Z", still_served: [{ atco: "149000006999", name: "Valley Gardens",
    metres: 250, direction: "north", routes: ["25", "25X", "78"], side: "both" }] };
const summaryOf = (card) => card.split("</summary>")[0];

test("one notice per works, so two at the busiest stop, and no box", () => {
  const html = app.boardNoticesHtml({ services_here: busy.here,
    disruptions: [busy.western, busy.steine, busy.council, steineSub, steine700], line_closures: [steineNote] });
  const cards = html.split("<details").slice(1);
  assert.equal(cards.length, 2, "more than one notice per works");
  assert.doesNotMatch(html, /disruption-find/, "the find box is back; the route chips do its job");
  assert.doesNotMatch(html, /disruption-badges/, "a notice spent a row on route badges");
  assert.match(summaryOf(cards[0]), /class="disruption-summary">Western Road closed</);
  assert.match(summaryOf(cards[1]), /class="disruption-summary">Old Steine roadworks</);
  assert.match(summaryOf(cards[1]), /25, 25X and 78 not stopping here until 18:00/);
  // The routes here each touches, for the chips to filter by.
  assert.deepEqual([...html.matchAll(/<details[^>]*data-services="([^"]*)"/g)].map(m => m[1]),
    ["25 49 60 700 N700", "25 25X 49 60 78 700"]);
});

test("a notice's title is its words before the colon; a route's own detail stays in view", () => {
  const many = { ...busy.steine, summary: "Old Steine roadworks: southbound buses diverted via Edward Street" };
  const one = app.boardNoticesHtml({ services_here: busy.here, disruptions: [many] });
  assert.match(summaryOf(one), /class="disruption-summary">Old Steine roadworks</);
  assert.doesNotMatch(summaryOf(one), /Edward Street/, "a five-route notice's detail took a line before it was opened");
  assert.match(one.split("</summary>")[1], /southbound buses diverted via Edward Street/, "the full summary is lost");
  const own = app.boardNoticesHtml({ services_here: [{ service: "700", operator: "SCSO" }],
    disruptions: [{ ...steine700, parent: undefined }] });
  assert.match(summaryOf(own), /class="disruption-detail">after Churchill Square, the 700 diverts to Brighton Station</,
    "the 700's own detail was hidden behind a tap");
});

test("the board line is the count and when, no 'of 15'", () => {
  const at = new Date("2026-10-26T15:45:00Z");
  assert.equal(app.departureCountText(10, 15, null, at), "10 departures · as of 15:45");
  assert.equal(app.departureCountText(2, 4, "700", at), "2 departures on the 700 · as of 15:45");
});

test("the stop heading is one line, the street or the town, never the name twice", () => {
  vm.runInContext(`state.stopData = {
      R1: { name: "Royal Pavilion", towards: "Hollingbury Asda", locality: "Brighton" },
      HS: { name: "High Street", towards: "Shoreham High Street", locality: "Shoreham-by-Sea" } };
    state.stopDetails = { R1: ["Stop R1", "Old Steine", "", "N"], HS: ["", "High Street", "", "E"] };`, app);
  assert.equal(app.stopHeadingLine("R1"), "Stop R1 · Towards Hollingbury Asda · On Old Steine");
  assert.equal(app.stopHeadingLine("HS"), "Towards Shoreham High Street · Shoreham-by-Sea");
});

test("a one-line notice is one line tall, not padded to two", () => {
  // .disruption summary's 44px floor came later in style.css at the same
  // specificity, so the compact card's own rule lost and Old Steine at
  // Waitrose showed an empty second line (26 October 2026).
  const css = readFileSync(join(ROOT, "style.css"), "utf8");
  assert.match(css, /\.disruption\.disruption--compact summary\s*\{[^}]*min-height:\s*0/);
});

// ── The Bus tab, in the board's style (26 October 2026) ─────

const t = (hhmm) => `2026-10-26T${hhmm}:00Z`;
const route46 = () => upcoming({ source: "trip", vehicle: { trip_source: "feed", recorded_at: t("16:20") },
  upcoming_stops: [
    stopRow(1, { stop_name: "Palmeira Square", passed: true, scheduled: t("16:19"), expected: null, lateness_secs: null }),
    stopRow(2, { stop_name: "Brunswick Place", not_served: true, scheduled: t("16:20"), expected: null, lateness_secs: null }),
    stopRow(3, { stop_name: "Norfolk Square", not_served: true, scheduled: t("16:21"), expected: null, lateness_secs: null }),
    stopRow(4, { stop_name: "Waitrose", is_next: true, scheduled: t("16:22"), expected: t("16:23"), lateness_secs: 60 }),
    stopRow(5, { stop_name: "Churchill Square", timing_point: true, scheduled: t("16:27"), expected: t("16:28"), lateness_secs: 60 }),
  ] });

test("the stops ahead read like the board, with passed and skipped stops a line each", () => {
  const html = route46();
  assert.match(html, /class="upcoming-stop-line">Passed Palmeira Square · 16:19</, "the passed stop took a full row");
  assert.match(html, /class="upcoming-stop-line upcoming-stop-line--skipped">Not calling at Brunswick Place or Norfolk Square: diversion</,
    "the stops it goes round took a row each");
  assert.doesNotMatch(html, /upcoming-stop--not-served/);
  assert.match(html, /class="upcoming-stop-time upcoming-stop-time--live"><span class="live-dot"><\/span>16:23</);
  assert.match(html, /class="upcoming-stop-sched">Scheduled 16:22</, "the timetabled time is not under the live one");
  assert.doesNotMatch(html, /upcoming-stop-was/, "a time is still crossed out");
});

test("the Bus tab's status is the board's, +3 mins and where", () => {
  const html = app.busStatusLineHtml({ lateness_secs: 200, nearest_stop_name: "Palmeira Square" });
  assert.match(html, /status-late"><span aria-hidden="true">\+3 mins<\/span><span class="visually-hidden">3 min late</);
  assert.match(html, /near Palmeira Square/);
  assert.match(app.busStatusLineHtml({ lateness_secs: 0, on_diversion: true }), />On time<[\s\S]*on diversion/);
});

test("the Bus tab is a row, two toggles and the stops; the rest folds away", () => {
  const v = { vehicle_ref: "512", service_ref: "46", operator_ref: "BHBC" };
  const html = app.busTabShellHtml(v);
  assert.doesNotMatch(html, /bus-info-hero/, "the identity block came back");
  assert.equal((html.match(/class="bus-pill"/g) || []).length, 2);
  assert.match(html, /id="follow-bus-checkbox"/);
  assert.match(html, /id="notify-move-checkbox"/);
  assert.ok(html.indexOf('id="bus-disruptions"') < html.indexOf('id="bus-upcoming"'), "the notices are not above the stops");
  assert.match(html, /<details class="bus-more"[^>]*>\s*<summary>Fares and tickets<\/summary>\s*<div id="bus-tickets">/);
  assert.match(html, /<summary>About the 46 · Brighton &amp; Hove Buses, fleet 512<\/summary>/);
  assert.match(html, /Report a problem with this bus/);
  assert.doesNotMatch(html, /refreshes every 20s/);
});

test("the bus's next journey is one line", () => {
  const html = app.buildNextJourneyHtml({ depart: "16:58", service: "46", headsign: "Hangleton" });
  assert.match(html, /Then it's planned to run the <strong>16:58 46<\/strong> to Hangleton\./);
});
