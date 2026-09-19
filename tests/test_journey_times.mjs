/**
 * "How long it really takes" — the arithmetic behind the chart.
 *
 * A reader picks two stops and sees every journey we watched make that trip.
 * The claim is strong and personal — "your 08:15 takes 52 minutes, not the 48
 * advertised" — so the ways it could quietly mislead are what these tests pin:
 *
 *   * a journey that called at only one of the two stops is not a journey time;
 *   * the same pair of stop names exists on both sides of a road, and a bus
 *     going the other way must not appear as one travelling backwards;
 *   * an interpolated observation and a scheduled time GTFS invented are
 *     different admissions, and the chart owes the reader both;
 *   * one stuck bus must not become the headline.
 *
 * Run with:  node --test tests/test_journey_times.mjs
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

const app = loadApp();
const between = vm.runInContext("journeyTimesBetween", app);
const forDays = vm.runInContext("journeyTimesForDays", app);
const summarise = vm.runInContext("journeyTimesSummary", app);

const MIN = 60;

/** calls: [stop index, observed, scheduled, flags] */
function journey(day, start, calls, direction = "westbound") {
  return { day, start, direction, calls };
}

/** A journey from stop 0 to stop 3, taking `tookMins` against `promisedMins`. */
function trip(day, startMin, tookMins, promisedMins, flags = [0, 0]) {
  const depart = startMin * MIN;
  const label = `${String(Math.floor(startMin / 60)).padStart(2, "0")}:` +
                `${String(startMin % 60).padStart(2, "0")}`;
  return journey(day, label, [
    [0, depart, depart, flags[0]],
    [3, depart + tookMins * MIN, depart + promisedMins * MIN, flags[1]],
  ]);
}

// ── Which journeys count ────────────────────────────────────

test("a journey that missed one of the two stops is not a journey time", () => {
  const doc = { journeys: [
    trip("2026-09-17", 8 * 60 + 15, 52, 48),
    journey("2026-09-17", "08:25", [[0, 30300, 30300, 0]]),          // never reached 3
    journey("2026-09-17", "08:35", [[3, 33000, 33000, 0]]),          // never seen at 0
  ] };
  const times = between(doc, 0, 3);
  assert.equal(times.length, 1, "a partial journey was counted as a journey time");
  assert.equal(times[0].observedSecs, 52 * MIN);
  assert.equal(times[0].scheduledSecs, 48 * MIN);
});

test("a bus going the other way is not a journey backwards in time", () => {
  // The two sides of a road share stop names, and an eastbound journey calls
  // at "to" before "from". Subtracting would give a negative journey time —
  // or worse, a plausible small positive one after a day wrap.
  const doc = { journeys: [
    journey("2026-09-17", "09:00", [[3, 32400, 32400, 0], [0, 34200, 34200, 0]],
            "eastbound"),
  ] };
  // Compared by length, not deepEqual: arrays built inside the app's realm
  // are never deepEqual to ones built here, which has caught this suite out
  // more than once.
  assert.equal(between(doc, 0, 3).length, 0);
});

test("journeys come back in departure order", () => {
  const doc = { journeys: [
    trip("2026-09-17", 17 * 60, 40, 35),
    trip("2026-09-17", 8 * 60, 52, 48),
    trip("2026-09-17", 12 * 60, 36, 35),
  ] };
  const departs = [...between(doc, 0, 3).map(t => t.departSecs)];
  assert.equal(JSON.stringify(departs),
               JSON.stringify([...departs].sort((a, b) => a - b)));
  assert.equal(departs.length, 3);
});

test("a journey recorded arriving before it left is not published", () => {
  // The scheduled order is right and the observed order is not, so the
  // direction test above passes it and the subtraction produces a negative
  // duration. Sixty-nine of these reached readers, the worst -12 minutes,
  // because a stop was timed from a later pass of the same bus.
  const doc = { journeys: [
    journey("2026-09-17", "08:15", [[0, 30600, 30600, 0], [3, 29700, 33480, 0]]),
    trip("2026-09-17", 9 * 60, 52, 48),
  ] };
  const times = between(doc, 0, 3);
  assert.equal(times.length, 1, "a negative journey time was published");
  assert.equal(times[0].observedSecs, 52 * MIN);
  assert.ok(times.every(t => t.observedSecs > 0));
});

test("the journeys refused as contradictory are counted, not hidden", () => {
  // "14 journeys, 3 excluded" says something about the evidence. "14
  // journeys" alone quietly overstates it.
  const doc = { journeys: [
    journey("2026-09-17", "08:15", [[0, 30600, 30600, 0], [3, 29700, 33480, 0]]),
    journey("2026-09-17", "08:45", [[0, 32400, 32400, 0], [3, 32400, 35280, 0]]),
    trip("2026-09-17", 9 * 60, 52, 48),
    // The other direction is not a fault and must not be counted as one.
    journey("2026-09-17", "09:30", [[3, 34200, 34200, 0], [0, 36000, 36000, 0]],
            "eastbound"),
  ] };
  assert.equal(vm.runInContext("journeyTimesContradictions", app)(doc, 0, 3), 2);
  assert.equal(between(doc, 0, 3).length, 1);
});

test("a journey that calls at a stop twice is not subtracted from itself", () => {
  // A circular service calls at a stop on the way out and again on the way
  // back. Taking the first match for both ends pairs the outward visit with
  // the return one and reports a loop of the town as the time between two
  // adjacent stops — a figure that looks entirely plausible on a chart.
  //
  // No route in the recorded area does this today, so this guards a timetable
  // change rather than fixing something observed.
  const doc = { journeys: [
    journey("2026-09-17", "08:15", [
      [0, 30600, 30600, 0],       // out
      [3, 31800, 31800, 0],
      [0, 34200, 34200, 0],       // and back past stop 0
    ]),
    trip("2026-09-17", 9 * 60, 52, 48),
  ] };
  const times = between(doc, 0, 3);
  assert.equal(times.length, 1, "an ambiguous pair of calls was subtracted");
  assert.equal(times[0].observedSecs, 52 * MIN);
});

// ── What the chart must admit ───────────────────────────────

test("an interpolated observation is marked as an estimate", () => {
  const doc = { journeys: [trip("2026-09-17", 8 * 60, 52, 48, [0, 1])] };
  assert.equal(between(doc, 0, 3)[0].estimated, true);
  const seen = { journeys: [trip("2026-09-17", 8 * 60, 52, 48, [0, 0])] };
  assert.equal(between(seen, 0, 3)[0].estimated, false);
});

test("a scheduled time GTFS invented is not a promise", () => {
  // Flag 2 means the stop is not a timing point, so "the timetable says 48
  // minutes" compares against an interpolation rather than anything the
  // operator committed to. The chart may draw the dots, not the line.
  const doc = { journeys: [trip("2026-09-17", 8 * 60, 52, 48, [0, 2])] };
  assert.equal(between(doc, 0, 3)[0].promised, false);
  const timed = { journeys: [trip("2026-09-17", 8 * 60, 52, 48, [0, 0])] };
  assert.equal(between(timed, 0, 3)[0].promised, true);
});

// ── Weekday and weekend ─────────────────────────────────────

test("days are split on the service day, not on the clock", () => {
  // 2026-09-17 is a Thursday, 2026-09-19 a Saturday.
  const times = between({ journeys: [
    trip("2026-09-17", 8 * 60, 52, 48),
    trip("2026-09-19", 8 * 60, 40, 48),
  ] }, 0, 3);
  assert.equal(forDays(times, "weekday").length, 1);
  assert.equal(forDays(times, "weekday")[0].day, "2026-09-17");
  assert.equal(forDays(times, "weekend")[0].day, "2026-09-19");
  assert.equal(forDays(times, "all").length, 2);
});

// ── The numbers under the chart ─────────────────────────────

test("one stuck bus does not become the headline", () => {
  // Nine journeys near the timetable and one that took an hour longer. The
  // median is what a passenger can expect; the slowest is worth showing
  // beside it, not instead of it.
  const journeys = [];
  for (let i = 0; i < 9; i++) journeys.push(trip("2026-09-17", 8 * 60 + i, 48, 48));
  journeys.push(trip("2026-09-17", 9 * 60, 108, 48));
  const s = summarise(between({ journeys }, 0, 3));
  assert.equal(s.journeys, 10);
  assert.equal(s.medianSecs, 48 * MIN, "the median followed the outlier");
  assert.equal(s.slowestSecs, 108 * MIN, "the worst case vanished from the summary");
});

test("the summary counts how often it beat the promise, with its denominator", () => {
  const journeys = [
    trip("2026-09-17", 8 * 60, 52, 48),        // over
    trip("2026-09-17", 9 * 60, 60, 48),        // over
    trip("2026-09-17", 10 * 60, 47, 48),       // under
    trip("2026-09-17", 11 * 60, 48, 48),       // on it
  ];
  const s = summarise(between({ journeys }, 0, 3));
  assert.equal(s.overPromised, 2);
  assert.equal(s.promisedJourneys, 4, "the denominator for that count is missing");
  assert.equal(s.scheduledSecs, 48 * MIN);
});

test("the promised-time denominator counts only promised journeys", () => {
  // A mixed pair of stops: some journeys have a timing point at both ends,
  // some do not. Counting all of them as the denominator would understate
  // how often the service missed a promise it actually made.
  const journeys = [
    trip("2026-09-17", 8 * 60, 60, 48),            // promised, over
    trip("2026-09-17", 9 * 60, 60, 48, [2, 2]),    // no promise at all
    trip("2026-09-17", 10 * 60, 60, 48, [0, 2]),   // no promise at one end
  ];
  const s = summarise(between({ journeys }, 0, 3));
  assert.equal(s.journeys, 3, "every observed journey still counts as observed");
  assert.equal(s.promisedJourneys, 1,
    "journeys with no promised time were counted in the promise denominator");
  assert.equal(s.overPromised, 1);
});


test("with no promised times there is no comparison to publish", () => {
  // Both ends interpolated by GTFS: a scheduled figure would compare our
  // arithmetic against theirs.
  const journeys = [trip("2026-09-17", 8 * 60, 52, 48, [2, 2])];
  const s = summarise(between({ journeys }, 0, 3));
  assert.equal(s.scheduledSecs, null);
  assert.equal(s.overPromised, null, "a comparison was published without a promise");
  assert.equal(s.journeys, 1, "the observed times are still worth showing");
});

test("nothing to show is not an empty chart pretending", () => {
  assert.equal(summarise([]), null);
});


// ── Not offered until it is worth offering ──────────────────

test("the view is not in the menu until the data justifies it", () => {
  // Three days, two of them part-days, is not something to put in front of a
  // reader who will read "median 79 minutes" as a fact about their route.
  const fresh = loadApp();
  const CONFIG = vm.runInContext("CONFIG", fresh);
  assert.equal(CONFIG.JOURNEY_TIMES_PUBLIC, false,
    "the view was announced before a week of data existed");
});

test("preview is a deliberate act, not a remembered one", () => {
  // A shared link must show the site as published. Remembering the flag in
  // storage would leak unfinished work to whoever the link reaches.
  const fresh = loadApp();
  const preview = vm.runInContext("previewEnabled", fresh);
  const loc = vm.runInContext("location", fresh);
  loc.search = "?preview=1";
  assert.equal(preview(), true);
  loc.search = "";
  assert.equal(preview(), false, "preview outlived the URL that asked for it");
  loc.search = "?preview=0";
  assert.equal(preview(), false);
});
