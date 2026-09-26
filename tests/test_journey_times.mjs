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
const journeyTimesForDays = forDays;
const summarise = vm.runInContext("journeyTimesSummary", app);
const directions = vm.runInContext("journeyTimesDirections", app);
const defaultPair = vm.runInContext("journeyTimesDefaultPair", app);
const delays = vm.runInContext("journeyTimesDelays", app);
const reachable = vm.runInContext("journeyTimesReachableFrom", app);

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

// ── Clicking two stops ──────────────────────────────────────
//
// The view opens on the map, not on the tool. Two clicks have to answer "which
// buses actually run between these?" without downloading 10 MB of documents to
// find out, and without claiming a service runs a trip it merely touches both
// ends of.

const candidateServices = vm.runInContext("journeyTimesCandidateServices", app);
const pairInDoc = vm.runInContext("journeyTimesPairInDoc", app);

const INDEX = { services: [
  { service: "1", operator: "BHBC", file: "1-BHBC.json" },
  { service: "1", operator: "SCSO", file: "1-SCSO.json" },
  { service: "700", operator: "SCSO", file: "700-SCSO.json" },
] };

test("only services calling at both stops are candidates", () => {
  const got = candidateServices(INDEX, ["1", "700"], ["700", "49"]);
  assert.deepEqual(got.map(e => e.file), ["700-SCSO.json"]);
});

test("a shared number offers both operators' routes as candidates", () => {
  // A number is not a route. The 1 is run by two companies over roads sharing
  // no stop, so both documents have to be asked and neither assumed.
  const got = candidateServices(INDEX, ["1"], ["1"]);
  assert.deepEqual(got.map(e => e.file).sort(), ["1-BHBC.json", "1-SCSO.json"]);
});

test("two stops with nothing in common are not a request at all", () => {
  assert.equal(candidateServices(INDEX, ["1"], ["49"]).length, 0);
  assert.equal(candidateServices(INDEX, null, ["1"]).length, 0);
  assert.equal(candidateServices({}, ["1"], ["1"]).length, 0);
});

const PAIR_DOC = {
  stops: [{ atco: "A1", name: "Pier" }, { atco: "A2", name: "Library" },
          { atco: "A3", name: "Station" },
          // The other side of the same three roads.
          { atco: "B1", name: "Pier" }, { atco: "B2", name: "Library" },
          { atco: "B3", name: "Station" }],
  journeys: [
    journey("2026-09-17", "08:00", [[0, 28800, 28800, 0], [1, 29400, 29400, 0],
                                    [2, 30000, 30000, 0]]),
    journey("2026-09-17", "09:00", [[5, 32400, 32400, 0], [4, 33000, 33000, 0],
                                    [3, 33600, 33600, 0]], "eastbound"),
  ],
};

test("a pair is found in the order the reader clicked", () => {
  const got = pairInDoc(PAIR_DOC, "A1", "A3");
  assert.equal(got.fromIndex, 0);
  assert.equal(got.toIndex, 2);
  assert.equal(got.reversed, false);
});

test("clicking the far end first finds the bus that runs that way", () => {
  // Station to Pier is a real trip here — it is the eastbound working — so the
  // answer is that journey, not the westbound one read backwards.
  const got = pairInDoc(PAIR_DOC, "A3", "A1");
  assert.ok(got, "no journey was found in either direction");
  assert.equal(got.reversed, false);
  assert.equal(got.fromIndex, 5, "the eastbound Station pole");
  assert.equal(got.toIndex, 3, "the eastbound Pier pole");
});

test("a one-way pair is answered the way the buses run", () => {
  // Where nothing runs the way the reader clicked, the corridor is still worth
  // showing — they asked about these two places, and the honest answer is the
  // direction that exists, flagged as such rather than silently swapped.
  const oneWay = {
    stops: [{ atco: "A1", name: "Pier" }, { atco: "A3", name: "Station" }],
    journeys: [journey("2026-09-17", "08:00",
                       [[0, 28800, 28800, 0], [1, 30000, 30000, 0]])],
  };
  const got = pairInDoc(oneWay, "A3", "A1");
  assert.ok(got, "the only direction that runs was refused");
  assert.equal(got.reversed, true);
  assert.equal(got.fromIndex, 0);
  assert.equal(got.toIndex, 1);
});

test("clicking the wrong pole of a stop still finds the journey", () => {
  // Each side of a road is its own ATCO code. A reader clicks a place, not a
  // pole, so picking the eastbound stop at one end and the westbound at the
  // other must not answer "no bus makes that trip".
  const got = pairInDoc(PAIR_DOC, "B1", "A3");
  assert.ok(got, "the pair was refused because the poles did not match");
  assert.equal(got.fromIndex, 0);
  assert.equal(got.toIndex, 2);
});

test("a service that calls at both stops but connects neither is refused", () => {
  // The case the prefilter cannot see: the stop lists say yes and no bus we
  // tracked ever ran from one to the other.
  const doc = {
    stops: [{ atco: "A1", name: "Pier" }, { atco: "A2", name: "Library" }],
    journeys: [journey("2026-09-17", "08:00", [[0, 28800, 28800, 0]]),
               journey("2026-09-17", "08:30", [[1, 30600, 30600, 0]])],
  };
  assert.equal(pairInDoc(doc, "A1", "A2"), null);
});

test("a stop the document has never heard of is refused", () => {
  assert.equal(pairInDoc(PAIR_DOC, "A1", "ZZ9"), null);
});

test("the same stop twice is not a journey", () => {
  assert.equal(pairInDoc(PAIR_DOC, "A1", "A1"), null);
});


// ── Choosing days, and only days we have ────────────────────
//
// The control used to offer All / Weekdays / Weekends regardless of what had
// been recorded. With two days of evidence "Weekends" returns nothing — which
// does not read as "we have not recorded a weekend", it reads as "no buses ran
// at the weekend", which is a claim we would be making by accident.

const dayOptions = vm.runInContext("journeyTimesDayOptions", app);

const onDays = (...days) => days.map(d => ({ day: d, observedSecs: 60 }));

test("a single weekday can be chosen", () => {
  // 2026-09-16 is a Wednesday, the 17th a Thursday, the 23rd a Wednesday.
  const times = onDays("2026-09-16", "2026-09-17", "2026-09-23");
  assert.deepEqual(forDays(times, "dow-3").map(t => t.day),
                   ["2026-09-16", "2026-09-23"]);
  assert.equal(forDays(times, "dow-4").length, 1, "Thursday");
  assert.equal(forDays(times, "dow-1").length, 0, "no Monday was recorded");
});

test("a date range includes both of its ends", () => {
  const times = onDays("2026-09-16", "2026-09-17", "2026-09-18", "2026-09-19");
  const got = forDays(times, "range", { from: "2026-09-17", to: "2026-09-18" });
  assert.deepEqual(got.map(t => t.day), ["2026-09-17", "2026-09-18"]);
});

test("half a range is still a range", () => {
  // A reader who fills in only a start means "from then on". Answering nothing
  // would be a worse reading of that than answering the obvious thing.
  const times = onDays("2026-09-16", "2026-09-17", "2026-09-18");
  assert.equal(forDays(times, "range", { from: "2026-09-17" }).length, 2);
  assert.equal(forDays(times, "range", { to: "2026-09-17" }).length, 2);
  assert.equal(forDays(times, "range", {}).length, 3, "no bounds excludes nothing");
});

test("the weekday is read at midday, so British Summer Time cannot move it", () => {
  // Midnight UTC on a BST date is the previous evening in London. Taken that
  // way a Thursday files itself under Wednesday, which would put a journey in
  // the wrong bucket on roughly half the days of the year.
  assert.equal(forDays(onDays("2026-09-17"), "dow-4").length, 1);
  assert.equal(forDays(onDays("2026-09-17"), "dow-3").length, 0);
});

test("a weekday with no data is never offered", () => {
  const options = dayOptions(["2026-09-16", "2026-09-17"]).map(o => o.value);
  assert.ok(options.includes("all"));
  assert.ok(!options.includes("dow-1"), "Monday was offered without a Monday");
  assert.ok(!options.includes("weekend"), "weekends were offered without one");
});

test("weekday and weekend are offered only when both exist to compare", () => {
  const mixed = dayOptions(["2026-09-17", "2026-09-19"]).map(o => o.value);
  assert.ok(mixed.includes("weekday") && mixed.includes("weekend"));
});

test("one Wednesday is not offered as Wednesdays", () => {
  // It would be the same figure as "all days" wearing a more confident label.
  const one = dayOptions(["2026-09-16", "2026-09-17"]).map(o => o.value);
  assert.ok(!one.includes("dow-3"));
  const two = dayOptions(["2026-09-16", "2026-09-23"]).map(o => o.value);
  assert.ok(two.includes("dow-3"), "two Wednesdays are worth averaging");
});

test("every option offered returns at least one journey", () => {
  // The property that matters, stated directly: a control that can produce an
  // empty chart from a full dataset is a broken tool, whatever its labels say.
  const days = ["2026-09-16", "2026-09-17", "2026-09-19", "2026-09-23"];
  const times = onDays(...days);
  for (const option of dayOptions(days)) {
    if (option.value === "range") continue;   // the reader sets its own bounds
    assert.ok(forDays(times, option.value).length > 0,
      `"${option.label}" was offered and matches no recorded day`);
  }
});

test("a date range is not offered for a single day", () => {
  assert.ok(!dayOptions(["2026-09-17"]).map(o => o.value).includes("range"));
});

test("the counts in the labels are the days behind them", () => {
  const labels = Object.fromEntries(
    dayOptions(["2026-09-16", "2026-09-17", "2026-09-19", "2026-09-23"])
      .map(o => [o.value, o.label]));
  assert.match(labels.all, /\(4\)/);
  assert.match(labels.weekday, /\(3\)/);
  assert.match(labels.weekend, /\(1\)/);
  assert.match(labels["dow-3"], /Wednesdays \(2\)/);
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

test("a nine-in-ten figure is not offered over a handful of journeys", () => {
  // "9 in 10 under 54 minutes" from two journeys is the slower of the two
  // wearing a statistic's clothes, and reads as a far stronger claim.
  const few = [];
  for (let i = 0; i < 4; i++) few.push(trip("2026-09-17", 8 * 60 + i, 48 + i, 48));
  const thin = summarise(between({ journeys: few }, 0, 3));
  assert.equal(thin.p90Secs, null, "a percentile was published over four journeys");
  assert.equal(thin.journeys, 4);
  assert.ok(thin.percentileFloor > 4, "the floor is not stated for the reader");

  const many = [];
  for (let i = 0; i < 12; i++) many.push(trip("2026-09-17", 8 * 60 + i, 48 + i, 48));
  assert.ok(summarise(between({ journeys: many }, 0, 3)).p90Secs > 0,
    "a well-evidenced percentile was suppressed too");
});

test("the days a figure rests on are the days left after filtering", () => {
  // The panel named the days in the file, not the days in the chart. Filter to
  // weekdays and the weekend dates stayed in the caption, so a reader checking
  // one against the other found a day that contributed nothing.
  const journeys = [
    trip("2026-09-17", 8 * 60, 52, 48),      // Thursday
    trip("2026-09-18", 8 * 60, 50, 48),      // Friday
    trip("2026-09-19", 8 * 60, 40, 48),      // Saturday
  ];
  const times = between({ journeys }, 0, 3);
  const weekdays = summarise(journeyTimesForDays(times, "weekday"));
  assert.equal(JSON.stringify(weekdays.days),
    JSON.stringify(["2026-09-17", "2026-09-18"]),
    "a filtered-out day was still named as contributing");
  assert.equal(weekdays.journeys, 2);
  assert.equal(summarise(times).days.length, 3);
});

test("nothing to show is not an empty chart pretending", () => {
  assert.equal(summarise([]), null);
});


// ── Not offered until it is worth offering ──────────────────

test("only the Simple view is announced; the stronger claims stay behind their switches", () => {
  // This gate once held the whole view back: three days, two of them
  // part-days, was not something to put in front of a reader who would read
  // "median 79 minutes" as a fact about their route. Simple went public on the
  // owner's decision on 23 September 2026, answering from the shared default
  // filters and saying what each answer rests on. The gate keeps its purpose
  // for what remains unverified: Detailed exposes every evidence control, and
  // the delay map colours roads, which is a claim about a place that the 22
  // September review asked to pilot rather than publish.
  const fresh = loadApp();
  const CONFIG = vm.runInContext("CONFIG", fresh);
  assert.equal(CONFIG.JOURNEY_TIMES_PUBLIC, true);
  assert.equal(CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC, false,
    "the Detailed view was announced before its evidence was independently checked");
  assert.equal(CONFIG.DELAY_MAP_PUBLIC, false,
    "the delay map was published before any stretch was validated");
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


// ── Which way the bus is going ──────────────────────────────

function headed(headsign, calls, day = "2026-09-17", start = "08:15") {
  return { day, start, direction: "westbound", headsign, calls };
}

test("directions are named as the bus names them, not by compass", () => {
  // The selects offered "towards Worthing" and "towards Brighton" for every
  // service, derived from whether the route's longitude increases — so
  // services that have never been near Worthing were labelled as going there.
  // The operator writes the answer on the front of the bus.
  //
  // The two directions call at *different* stops, because each side of a road
  // is a different ATCO pole. An earlier version of this fixture reused one
  // set of indices for both, which no real service does and which the
  // clustering rightly reads as a single direction.
  const doc = { journeys: [
    headed("Meadowview", [[0, 100, 100, 0], [3, 900, 900, 0]]),
    headed("Meadowview", [[0, 200, 200, 0], [3, 1000, 1000, 0]]),
    headed("Beresford Road", [[5, 300, 300, 0], [8, 1100, 1100, 0]]),
  ] };
  const got = directions(doc);
  assert.equal(JSON.stringify(got.map(d => d.headsign)),
    JSON.stringify(["Meadowview", "Beresford Road"]),
    "directions were not named by destination, busiest first");
  assert.equal(got[0].journeys, 2);
});

test("stops are ordered by where they fall in that direction's journeys", () => {
  // Not by their position in the document, which is a median across every
  // variant the service runs — so the two directions would share one order and
  // one of them would read backwards.
  // Again, two directions means two sets of poles: 0-1-2 out, 5-6-7 back.
  const doc = { journeys: [
    headed("Out", [[0, 100, 100, 0], [1, 200, 200, 0], [2, 300, 300, 0]]),
    headed("Back", [[7, 400, 400, 0], [6, 500, 500, 0], [5, 600, 600, 0]]),
  ] };
  // Selected by name: with equal journey counts the order is alphabetical,
  // and a test that depends on which of two ties wins is testing the tiebreak.
  const byName = Object.fromEntries(directions(doc).map(d => [d.headsign, d]));
  const out = byName["Out"], back = byName["Back"];
  assert.equal(JSON.stringify(out.stops.map(s => s.index)), JSON.stringify([0, 1, 2]));
  assert.equal(JSON.stringify(back.stops.map(s => s.index)), JSON.stringify([7, 6, 5]),
    "the return direction was listed in the outward order");
});

test("a short working that starts mid-route does not jumble the stop list", () => {
  // Stops were ordered by their median position in each journey's call list.
  // A short working starting at the fifth stop puts that stop at position 0 in
  // every one of its journeys, so where short workings outnumber the full run
  // the second half of the route was listed on top of the first. Measured over
  // the published services: 1,332 pairs of stops listed the opposite way round
  // to how buses called at them, 495 on the 21 alone.
  const full = [0, 1, 2, 3, 4, 5, 6, 7];
  const short = [4, 5, 6, 7];
  const doc = { journeys: [
    ...Array.from({ length: 8 }, () =>
      headed("Marina", full.map((i, n) => [i, 100 + n * 60, 100 + n * 60, 0]))),
    ...Array.from({ length: 20 }, () =>
      headed("Marina", short.map((i, n) => [i, 400 + n * 60, 400 + n * 60, 0]))),
  ] };
  const [d] = directions(doc);
  assert.equal(JSON.stringify(d.stops.map(s => s.index)), JSON.stringify(full),
    "the stop list is not in the order the buses call at them");
});

test("stops a bus was not seen at still fall in route order", () => {
  // A journey records only the stops it was seen at, so two journeys along the
  // same road can hold quite different subsets of it. The positions in their
  // call lists do not line up, and the list has to come from the order the
  // stops were called in rather than from where they sat in each list.
  const route = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19];
  const seen = [[10, 11, 12, 13, 14, 15, 16, 17, 18, 19], [10, 14, 15, 16, 17, 18, 19],
                [10, 11, 12, 13, 18, 19], [13, 14, 15, 16], [16, 17, 18, 19],
                [11, 15, 19], [10, 12, 14, 16, 18]];
  const doc = { journeys: seen.flatMap(stops => Array.from({ length: 3 }, () =>
    headed("Hove", stops.map(i => [i, 100 + route.indexOf(i) * 60, 100 + route.indexOf(i) * 60, 0])))) };
  const [d] = directions(doc);
  assert.equal(JSON.stringify(d.stops.map(s => s.index)), JSON.stringify(route));
});

test("a loop run both ways round keeps a stable order rather than failing", () => {
  // Some town services run a loop in both directions under one destination,
  // so the same two stops are called in both orders. No order satisfies every
  // journey; the list must still come back whole and without repeats.
  const doc = { journeys: [
    ...Array.from({ length: 5 }, () => headed("Loop", [[0, 100, 100, 0], [1, 200, 200, 0], [2, 300, 300, 0], [3, 400, 400, 0]])),
    ...Array.from({ length: 5 }, () => headed("Loop", [[0, 100, 100, 0], [3, 200, 200, 0], [2, 300, 300, 0], [1, 400, 400, 0]])),
  ] };
  const [d] = directions(doc);
  const got = d.stops.map(s => s.index);
  assert.equal(got.length, 4);
  assert.equal(new Set(got).size, 4, "a stop was listed twice");
  assert.equal(got[0], 0, "the stop every journey starts from is not first");
});

test("a service that states no headsign still gets a direction", () => {
  // Older documents carry none. Falling back on the compass label is worse
  // than nothing only when it is wrong about the destination; as a last resort
  // it still separates the two sides of the road.
  const doc = { journeys: [
    { day: "2026-09-17", start: "08:15", direction: "eastbound",
      calls: [[0, 100, 100, 0], [1, 200, 200, 0]] },
  ] };
  // Capitalised, because it is shown to a reader: the last-resort label is
  // still a label. NaPTAN place names are left exactly as they come, since
  // prettifyName would render Shoreham-by-Sea as Shoreham-by-sea.
  assert.equal(directions(doc)[0].headsign, "Eastbound");
});

// ── What the view opens on ──────────────────────────────────

test("the default pair is one the timetable actually promises", () => {
  // Service 37 has 444 promised calls and showed no timetable line at all,
  // because the pair it opened on was two stops GTFS had interpolated. A
  // reader sees "no comparison available" and concludes the tool is broken.
  const doc = { journeys: [
    headed("Meadowview", [
      [0, 100, 100, 2],      // interpolated: no promise here
      [1, 200, 200, 0],      // a timing point
      [2, 300, 300, 2],
      [3, 400, 400, 0],      // and another
      [4, 500, 500, 2],
    ]),
  ] };
  const pair = defaultPair(doc, directions(doc)[0]);
  assert.equal(JSON.stringify(pair), JSON.stringify({ from: 1, to: 3 }),
    "the view opened on stops the timetable makes no promise about");
});

test("a service with no promised stops still opens on something", () => {
  // Better an honest observed-only chart than an empty view.
  const doc = { journeys: [
    headed("Meadowview", [[0, 100, 100, 2], [1, 200, 200, 2], [2, 300, 300, 2]]),
  ] };
  assert.equal(JSON.stringify(defaultPair(doc, directions(doc)[0])),
    JSON.stringify({ from: 0, to: 2 }));
});

test("a direction with one stop offers no pair at all", () => {
  const doc = { journeys: [headed("Meadowview", [[0, 100, 100, 0]])] };
  assert.equal(defaultPair(doc, directions(doc)[0]), null);
  assert.equal(defaultPair(doc, undefined), null);
});


// ── Each journey against its own promise ────────────────────

test("a journey is compared with its own scheduled time, not the median", () => {
  // The chart drew one timetable line at the median scheduled duration. The
  // 700's scheduled run between its ends ranges 61 to 85 minutes, so that line
  // is up to twelve minutes wrong about every journey it judges — and wrong in
  // both directions. A bus keeping a slower evening timetable read as late; one
  // missing a tighter morning timetable read as on time.
  const journeys = [
    trip("2026-09-17", 8 * 60, 63, 61),       // 2 min over its own promise
    trip("2026-09-17", 18 * 60, 80, 85),      // 5 min inside its own promise
  ];
  const got = delays(between({ journeys }, 0, 3));
  assert.equal(JSON.stringify(got.map(t => t.delaySecs / 60)),
    JSON.stringify([2, -5]),
    "journeys were judged against an aggregate rather than their own timetable");
});

test("a journey with no promise is not in a delay chart at all", () => {
  // A delay measured against GTFS's own interpolation is a delay against our
  // arithmetic, which is not a finding about a bus.
  const journeys = [
    trip("2026-09-17", 8 * 60, 63, 61),
    trip("2026-09-17", 9 * 60, 70, 61, [2, 2]),     // no promise either end
    trip("2026-09-17", 10 * 60, 70, 61, [0, 2]),    // none at one end
  ];
  const got = delays(between({ journeys }, 0, 3));
  assert.equal(got.length, 1, "an interpolated schedule was published as a delay");
  assert.equal(got[0].delaySecs, 2 * 60);
});

test("the summary's delay figures use each journey's own promise", () => {
  const journeys = [
    trip("2026-09-17", 8 * 60, 63, 61),       // +2
    trip("2026-09-17", 12 * 60, 71, 70),      // +1
    trip("2026-09-17", 18 * 60, 97, 85),      // +12
  ];
  const s = summarise(between({ journeys }, 0, 3));
  assert.equal(s.medianDelaySecs, 2 * 60);
  assert.equal(s.worstDelaySecs, 12 * 60);
  // And the aggregate scheduled figure is still the median of the promises,
  // which is a different and much weaker statement — kept, but not the basis
  // of the comparison.
  assert.equal(s.scheduledSecs, 70 * 60);
});

test("with nothing promised there are no delay figures to state", () => {
  const journeys = [trip("2026-09-17", 8 * 60, 63, 61, [2, 2])];
  const s = summarise(between({ journeys }, 0, 3));
  assert.equal(s.medianDelaySecs, null);
  assert.equal(s.worstDelaySecs, null);
  assert.equal(s.journeys, 1, "the observed time is still worth showing");
});


// ── Several destinations, one direction ─────────────────────

/** A journey heading for `place`, calling at `stops` in order. */
function toward(place, headsign, stops, day = "2026-09-17", start = "08:15") {
  return { day, start, direction: "westbound", headsign, place,
           calls: stops.map((i, n) => [i, 100 + n * 60, 100 + n * 60, 0]) };
}

test("a short working joins the direction it is a short working of", () => {
  // The 700's Worthing journeys are the Durrington run stopping early, not a
  // third direction. They share most of their stops, so they cluster.
  const doc = { journeys: [
    ...Array.from({ length: 8 }, () =>
      toward("Durrington", "Durrington Tesco", [0, 1, 2, 3, 4, 5])),
    ...Array.from({ length: 2 }, () =>
      toward("Worthing", "Worthing Marine Parade", [0, 1, 2, 3])),
    ...Array.from({ length: 6 }, () =>
      toward("Brighton", "Old Steine", [10, 11, 12, 13, 14])),
  ] };
  const got = directions(doc);
  assert.equal(got.length, 2, `${got.length} directions where the service has 2`);
  assert.equal(got[0].journeys, 10, "the short working founded its own direction");
});

test("the two destinations are chosen by journeys and shown nearest first", () => {
  // Chosen by how many journeys go there; ordered as a passenger passes them,
  // so the 700 reads "Worthing / Durrington" and not the other way about.
  const doc = { journeys: [
    ...Array.from({ length: 8 }, () =>
      toward("Durrington", "Durrington Tesco", [0, 1, 2, 3, 4, 5])),
    ...Array.from({ length: 2 }, () =>
      toward("Worthing", "Worthing Marine Parade", [0, 1, 2, 3])),
  ] };
  assert.equal(directions(doc)[0].headsign, "Worthing / Durrington");
});

test("several destinations in one town are one destination", () => {
  // Service 2 runs west to five stops that are only three places: Shoreham
  // High Street and Red Lion are both Shoreham, Shooting Field and Steyning
  // Clock Tower both Steyning. Pooled by stop it reads "(+3 more)"; pooled by
  // place the whole direction fits in its own label.
  const doc = { journeys: [
    ...Array.from({ length: 5 }, () =>
      toward("Shoreham-by-Sea", "Shoreham High Street", [0, 1, 2, 3])),
    ...Array.from({ length: 2 }, () =>
      toward("Shoreham-by-Sea", "Red Lion", [0, 1, 2])),
    ...Array.from({ length: 3 }, () =>
      toward("Steyning", "Shooting Field", [0, 1, 2, 3, 4, 5])),
    toward("Steyning", "Steyning Clock Tower", [0, 1, 2, 3, 4]),
  ] };
  const [west] = directions(doc);
  assert.equal(west.headsign, "Shoreham-by-Sea / Steyning",
    "destinations were counted by stop rather than by town");
  assert.equal(west.places.length, 2, "four destinations are two places");
  assert.equal(west.places[0].headsigns.length, 2,
    "the town does not list the destinations it pools");
});

test("a destination almost nobody goes to is not half the label", () => {
  // Service 2 west runs 127 journeys to Shoreham and 2 to Hove. Billing them
  // equally would be a label 98% wrong about where the bus goes.
  const doc = { journeys: [
    ...Array.from({ length: 30 }, () =>
      toward("Shoreham-by-Sea", "Shoreham High Street", [0, 1, 2, 3])),
    toward("Hove", "Palmeira Square", [0, 1]),
  ] };
  const [west] = directions(doc);
  assert.equal(west.headsign, "Shoreham-by-Sea (or 1 other place)");
  assert.equal(west.places.length, 2, "the rare destination was dropped, not counted");
  assert.equal(west.places[1].journeys, 1);
});

test("a destination is named without the stand letter the feed adds", () => {
  // The 25 was offered as "towards Old Steine S3" and the 7 as "George Street
  // (stop J)": the stand at the terminus, which helps nobody pick a direction.
  const clean = vm.runInContext("jtCleanHeadsign", app);
  assert.equal(clean("Old Steine S3"), "Old Steine");
  assert.equal(clean("Imperial Arcade C7"), "Imperial Arcade");
  assert.equal(clean("George Street (stop J)"), "George Street");
  assert.equal(clean("Portslade Station (stop G)"), "Portslade Station");
  assert.equal(clean("Brighton University Falmer"), "Brighton University Falmer");
  assert.equal(clean("Worthing"), "Worthing");
  // Where no town tells the two directions apart, the bus's own words are
  // used, and two stands at one terminus are one destination.
  const doc = { journeys: [
    ...Array.from({ length: 5 }, () => headed("Northfield Crescent", [[0, 100, 100, 0], [1, 200, 200, 0]])),
    ...Array.from({ length: 5 }, () => headed("Old Steine S3", [[5, 100, 100, 0], [6, 200, 200, 0]])),
    ...Array.from({ length: 2 }, () => headed("Old Steine S1", [[5, 100, 100, 0], [6, 200, 200, 0]])),
  ].map(j => ({ ...j, place: "Brighton" })) };
  const got = directions(doc).map(d => d.headsign);
  assert.ok(got.includes("Old Steine"), `stand letters reached the label: ${got.join(" | ")}`);
  const label = vm.runInContext("jtDirectionLabel", app);
  assert.equal(label({ headsign: "Hove" }), "Towards Hove");
  assert.equal(label({ headsign: "Eastbound" }), "Eastbound", "the compass fallback was given a destination");
});

test("with no place published the destination text is used", () => {
  // An older document, or a build where NaPTAN was unreachable.
  const doc = { journeys: [
    { day: "2026-09-17", start: "08:15", direction: "westbound",
      headsign: "Meadowview", calls: [[0, 100, 100, 0], [1, 200, 200, 0]] },
  ] };
  assert.equal(directions(doc)[0].headsign, "Meadowview");
});


// ── Only pairs a bus actually runs ──────────────────────────

test("a stop no journey reaches from here is not offered", () => {
  // The direction pools variants, so its stop list is the union of them. Stop
  // 9 belongs to the other working; offering it answers "no bus we tracked
  // made that trip", which reads as a broken tool.
  const doc = { journeys: [
    toward("Durrington", "Durrington Tesco", [0, 1, 2, 3]),
    toward("Worthing", "Worthing Marine Parade", [0, 1, 9]),
  ] };
  const from0 = reachable(doc, 0);
  assert.equal(from0.get(3), 1, "a stop one working reaches was not offered");
  assert.equal(from0.get(9), 1);
  const from2 = reachable(doc, 2);
  assert.equal(from2.get(9), undefined,
    "a stop on the other working was offered from a stop it never follows");
  assert.equal(from2.get(3), 1);
});

test("a stop behind you is not somewhere you can get to", () => {
  const doc = { journeys: [toward("X", "X", [0, 1, 2, 3])] };
  const from2 = reachable(doc, 2);
  assert.equal(from2.get(1), undefined, "the tool offered travelling backwards");
  assert.equal(from2.get(3), 1);
});

test("the count says how many journeys make each trip", () => {
  // "Durrington High School — 191 journeys" is the difference between a pair
  // a reader can trust and one served twice a week.
  const doc = { journeys: [
    ...Array.from({ length: 5 }, () => toward("A", "A", [0, 1, 2])),
    toward("B", "B", [0, 1, 2, 7]),
  ] };
  const from0 = reachable(doc, 0);
  assert.equal(from0.get(2), 6);
  assert.equal(from0.get(7), 1, "a rarely served stop lost its true count");
});

test("a journey calling twice at the chosen stop is left out", () => {
  // It cannot say which visit is meant, and journeyTimesBetween drops it for
  // that reason — the two must agree or the count promises journeys the chart
  // will not draw.
  const doc = { journeys: [
    { day: "2026-09-17", start: "08:15", direction: "westbound", headsign: "Loop",
      place: "Town", calls: [[0, 100, 100, 0], [1, 200, 200, 0], [0, 300, 300, 0]] },
    toward("X", "X", [0, 1]),
  ] };
  assert.equal(reachable(doc, 0).get(1), 1,
    "a looping journey was counted as reaching a stop from an ambiguous start");
});

test("a stop nothing departs from reaches nowhere", () => {
  assert.equal(reachable({ journeys: [toward("X", "X", [0, 1])] }, 5).size, 0);
  assert.equal(reachable({}, 0).size, 0);
});


test("every stop offered has a journey the chart can draw", () => {
  // The property that matters, stated directly: what the To list promises and
  // what the chart draws must be the same set. They were not — reachability
  // counted every later call, while the chart needs the observed times to
  // advance and the stops to be called at once. Across the 35 published
  // services that offered 95 pairs answering with an empty chart.
  // Stops 5 and 3 are each reached by one journey only, and by a journey the
  // chart must refuse — so nothing else can supply the pair and mask the bug.
  const doc = { journeys: [
    // Stop 5 shares stop 0's observed second: no duration to draw.
    { day: "2026-09-17", start: "08:00", direction: "westbound", headsign: "A",
      place: "Town", calls: [[0, 100, 100, 0], [5, 100, 160, 0], [2, 400, 400, 0]] },
    // Stop 3 sits on a loop, which cannot say which visit is meant.
    { day: "2026-09-17", start: "09:00", direction: "westbound", headsign: "A",
      place: "Town", calls: [[0, 100, 100, 0], [3, 200, 200, 0], [0, 300, 300, 0]] },
    toward("Town", "A", [0, 1, 2]),
  ] };
  for (const from of [0, 1, 2, 3, 5]) {
    for (const [to, count] of reachable(doc, from)) {
      const drawn = between(doc, from, to).length;
      assert.ok(drawn > 0,
        `${from}→${to} was offered with ${count} journeys and drew nothing`);
    }
  }
});


// ── When a town cannot tell the two directions apart ────────

/** A direction's worth of journeys heading for `place`. */
function runs(n, place, headsign, stops) {
  return Array.from({ length: n }, () => toward(place, headsign, stops));
}

test("a service inside one town is named by what the bus says", () => {
  // Service 37 runs Meadowview to Bristol Estate entirely inside Brighton, so
  // both directions read "Towards Brighton" and the control said nothing at
  // all. The words on the front of the bus are more specific than the map is.
  const doc = { journeys: [
    ...runs(8, "Brighton", "Meadowview", [0, 1, 2, 3]),
    ...runs(7, "Brighton", "Beresford Road", [10, 11, 12, 13]),
  ] };
  const got = directions(doc).map(d => d.headsign);
  assert.equal(JSON.stringify(got), JSON.stringify(["Meadowview", "Beresford Road"]),
    "both directions were named after the town they both sit in");
});

test("the town that contains the other end is the useless one", () => {
  // Bristol Estate is inside Brighton. "Towards Brighton" against "Towards
  // Bristol Estate" reads as a place and the place containing it, not as two
  // ends of a route — so Brighton gives way to the destination on the bus
  // while Bristol Estate, being the specific half, stays.
  const doc = {
    place_parents: { "Bristol Estate": "Brighton" },
    journeys: [
      ...runs(8, "Brighton", "Meadowview", [0, 1, 2, 3]),
      ...runs(7, "Bristol Estate", "Beresford Road", [10, 11, 12, 13]),
    ],
  };
  assert.equal(JSON.stringify(directions(doc).map(d => d.headsign)),
    JSON.stringify(["Meadowview", "Bristol Estate"]),
    "the containing town survived, or the contained one was thrown away");
});

test("a handful of journeys does not spoil the other direction's name", () => {
  // Service 1 sends 3 journeys of 305 to Portslade Village and 284 the other
  // way. Those 3 make the name useless for the first direction and say nothing
  // about the second, which genuinely is the Portslade Village one — a single
  // shared set let the 3 disqualify the 284.
  const doc = { journeys: [
    ...runs(30, "Whitehawk", "Swanborough Drive", [0, 1, 2, 3]),
    ...runs(1, "Portslade Village", "Community Centre", [0, 1]),
    ...runs(28, "Portslade Village", "Portslade Academy", [10, 11, 12, 13]),
  ] };
  const got = directions(doc).map(d => d.headsign);
  assert.ok(got.includes("Portslade Village"),
    `a 1% overlap cost the other direction its name: ${got.join(" | ")}`);
});

test("a direction is not named after somewhere almost nobody goes", () => {
  // Service 21 has Whitehawk to itself, but 9 journeys of 143. Naming a
  // direction after 6% of it is the same mistake as naming it after a town
  // both directions share — so the destination on the bus is used instead.
  const doc = { journeys: [
    ...runs(30, "Brighton", "Marina Cinema", [0, 1, 2, 3]),
    ...runs(2, "Whitehawk", "Whitehawk Bus Garage", [0, 1, 2, 3, 4]),
    ...runs(28, "Brighton", "Imperial Arcade", [10, 11, 12, 13]),
  ] };
  const [first] = directions(doc);
  assert.equal(first.headsign.split(" (or ")[0], "Marina Cinema",
    `named after 6% of the direction: ${first.headsign}`);
});

test("two towns that tell the directions apart are left alone", () => {
  // The common case must not be disturbed by any of the above.
  const doc = { journeys: [
    ...runs(8, "Durrington", "Durrington Tesco", [0, 1, 2, 3, 4, 5]),
    ...runs(2, "Worthing", "Worthing Marine Parade", [0, 1, 2, 3]),
    ...runs(6, "Brighton", "Old Steine", [10, 11, 12, 13, 14]),
  ] };
  assert.equal(JSON.stringify(directions(doc).map(d => d.headsign)),
    JSON.stringify(["Worthing / Durrington", "Brighton"]));
});

// ── Opening on the two ends of the line ─────────────────────

test("the view opens on the ends of the longest run", () => {
  // It used to take the first and last of the direction's stop list, ordered
  // by each stop's median position. On a route that doubles back those are not
  // the ends of the line: service 23X opened on two stops 2.0 km apart when
  // 6.1 km was available, which looks like the map has drawn the route wrongly.
  const doc = { journeys: [
    toward("Town", "A", [0, 1, 2, 3, 4, 5]),
    toward("Town", "A", [2, 3]),
  ] };
  assert.equal(JSON.stringify(defaultPair(doc, directions(doc)[0])),
    JSON.stringify({ from: 0, to: 5 }));
});

test("the view never opens on one stop twice", () => {
  // A service passing a stop twice can have the same stop at both ends of a
  // run, and "Church to Church" is not a journey time. Service 5 opened on
  // exactly that.
  const doc = { journeys: [
    { day: "2026-09-17", start: "08:15", direction: "westbound", headsign: "A",
      place: "Town",
      calls: [[0, 100, 100, 0], [1, 200, 200, 0], [0, 300, 300, 0]] },
    toward("Town", "A", [0, 1]),
  ] };
  const pair = defaultPair(doc, directions(doc)[0]);
  assert.notEqual(pair.from, pair.to, "the view opened on a stop and itself");
});

test("the view never opens on a pair with no journeys", () => {
  // The worst possible first impression. A pair taken from one journey can
  // still be a pair that journey is not counted for — journeyTimesBetween
  // drops a run calling twice at either end — and on the 5 no other journey
  // served it, so the view opened on "no bus we tracked made that trip".
  const doc = { journeys: [
    { day: "2026-09-17", start: "08:15", direction: "westbound", headsign: "A",
      place: "Town",
      calls: [[9, 100, 100, 0], [1, 200, 200, 0], [2, 300, 300, 0],
              [9, 400, 400, 0]] },
    toward("Town", "A", [1, 2]),
  ] };
  const pair = defaultPair(doc, directions(doc)[0]);
  assert.ok(between(doc, pair.from, pair.to).length > 0,
    `opened on ${pair.from}→${pair.to}, which no journey makes`);
});


test("the opening pair is the one the default names, not the next stop along", () => {
  // Not a unit of journeyTimesDefaultPair but of how the view applies it. The
  // To list is rebuilt by replacing the select's innerHTML, which makes the
  // browser select the first option — so asking afterwards whether the
  // reader's choice survived always answered yes, and the view opened on the
  // stop *after* the start rather than the end of the line. Two adjacent
  // interpolated stops a minute apart, with no timetable to compare against.
  //
  // Stated here as the property the view must satisfy: whatever the default
  // pair says, the chart must be drawn for that pair.
  const doc = { journeys: [
    ...Array.from({ length: 4 }, () =>
      toward("Town", "A", [0, 1, 2, 3, 4, 5])),
  ] };
  const d = directions(doc)[0];
  const pair = defaultPair(doc, d);
  const reach = reachable(doc, pair.from);
  assert.ok(reach.has(pair.to),
    "the default To is not among the stops reachable from the default From, "
    + "so the view cannot open on it");
  assert.ok(between(doc, pair.from, pair.to).length > 0);
  // And it is not merely the first stop along, which is what the bug produced.
  const onward = d.stops.filter(s => reach.has(s.index));
  assert.notEqual(pair.to, onward[0].index,
    "the default opened on the very next stop rather than the far end");
});


// ── Simple and Detailed share one answer ─────────────────────
//
// Two views of one selection. The same trip must never show two different
// headline numbers, the timetable line must be the timetable's promise and not
// a guess, and a delay colour must never be painted on thin evidence.

import { readFileSync } from "node:fs";
import { ROOT } from "./load_app.mjs";
import { join } from "node:path";

const DEFAULTS = vm.runInContext("JT_DEFAULT_FILTERS", app);
const inSlot = vm.runInContext("journeyTimesInSlot", app);
const insight = vm.runInContext("journeyTimesPeriodInsight", app);
const scheduleLine = vm.runInContext("journeyTimesScheduleLine", app);
const coverage = vm.runInContext("journeyTimesCoverage", app);
const band = vm.runInContext("delayBand", app);
const stepPath = vm.runInContext("jtStepPath", app);
const chart = vm.runInContext("journeyTimesChart", app);
// Arrays made inside the vm have that realm's prototype; compare contents.
const plain = v => JSON.parse(JSON.stringify(v));

test("the Detailed controls open on exactly the filters Simple answers with", () => {
  // If they drifted, one trip would read "usually 34 min" in Simple and
  // "median 36 min" in Detailed — the site contradicting itself.
  const html = readFileSync(join(ROOT, "index.html"), "utf8");
  const selected = id => {
    const block = html.match(new RegExp(`<select id="journey-times-${id}"[^>]*>([\\s\\S]*?)</select>`));
    assert.ok(block, `no ${id} select`);
    const options = [...block[1].matchAll(/<option value="([^"]*)"([^>]*)>/g)];
    const chosen = options.find(o => /\bselected\b/.test(o[2])) || options[0];
    return chosen[1];
  };
  const value = id => html.match(new RegExp(`value="([^"]*)" id="journey-times-${id}"`))?.[1];
  assert.equal(selected("evidence"), DEFAULTS.evidence);
  assert.equal(selected("identity"), DEFAULTS.identity);
  assert.equal(selected("quality"), DEFAULTS.quality);
  assert.equal(selected("cohort"), DEFAULTS.cohort);
  assert.equal(value("time-from"), DEFAULTS.start);
  assert.equal(value("time-to"), DEFAULTS.end);
});

/** A tracked journey from stop 0 to stop 1 on `day`, leaving at `h:m`. */
function tracked(day, h, m, tookMins, promisedMins = 20, tripId = `T${h}${m}`) {
  const dep = h * 3600 + m * 60;
  return { day, start: `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`,
           trip_id: tripId, direction: "westbound",
           calls: [[0, dep, dep, 0], [1, dep + tookMins * MIN, dep + promisedMins * MIN, 0]] };
}

test("the early-and-late period wraps midnight, and a day type combines with any time", () => {
  const doc = { stops: [{}, {}], journeys: [
    tracked("2026-09-22", 23, 0, 20), tracked("2026-09-22", 5, 0, 20),
    tracked("2026-09-22", 12, 0, 20), tracked("2026-09-26", 12, 0, 20),
    tracked("2026-09-27", 8, 0, 20)] };
  const all = between(doc, 0, 1);
  assert.deepEqual(plain(inSlot(all, "weekday", "other").map(t => t.start).sort()), ["05:00", "23:00"]);
  assert.deepEqual(plain(inSlot(all, "weekend", "any").map(t => t.day).sort()), ["2026-09-26", "2026-09-27"]);
  // Weekends were once a time of day of their own, so a weekend morning
  // could not be asked for at all.
  assert.deepEqual(plain(inSlot(all, "weekend", "08-10").map(t => t.day)), ["2026-09-27"]);
  assert.equal(inSlot(all, "weekday", "any").length, 3);
  assert.equal(inSlot(all, "all", "any").length, 5);
  // An old link's "weekend" period still lands somewhere sensible.
  const fromLink = vm.runInContext("jtSlotFromLink", app);
  assert.deepEqual(plain(fromLink(null, "weekend")), { day: "weekend", period: "any" });
  assert.deepEqual(plain(fromLink("weekday", "nonsense")), { day: "weekday", period: null });
  // The rush hours were 07-10 and 16-19; an old link opens on the new ones.
  assert.deepEqual(plain(fromLink("weekday", "07-10")), { day: "weekday", period: "08-10" });
  assert.deepEqual(plain(fromLink("weekday", "16-19")), { day: "weekday", period: "16-18" });
});

test("the slowest time of day is only named from enough journeys", () => {
  const journeys = [];
  for (let i = 0; i < 12; i++) journeys.push(tracked("2026-09-22", 8, i, 30, 20, `M${i}`));
  for (let i = 0; i < 12; i++) journeys.push(tracked("2026-09-22", 12, i, 22, 20, `D${i}`));
  // Nine evening buses, one short of the floor: not enough to call it anything.
  for (let i = 0; i < 9; i++) journeys.push(tracked("2026-09-22", 17, i, 60, 20, `E${i}`));
  const found = insight(between({ stops: [{}, {}], journeys }, 0, 1), "weekday");
  assert.equal(found.slowest.period.key, "08-10");
  assert.equal(found.quickest.period.key, "10-16");
  assert.equal(found.compared, 2, "a period below the floor was compared");
});

function recorded(days) {
  // Stops 0 → 1 → 2. Weekday trips leave at 07:00 and 08:00; the Saturday one
  // takes longer. Profile 1 calls at 2 before 0, so it goes the other way.
  return {
    profiles: [[[0, 0, 1], [1, 300, 0], [2, 900, 1]],
               [[0, 0, 1], [1, 300, 1], [2, 1200, 1]],
               [[2, 0, 1], [0, 600, 1]]],
    sets: [[["W1", 0, 25200, "x"], ["W2", 0, 28800, "x"], ["BACK", 2, 30000, "y"]],
           [["S1", 1, 32400, "x"]]],
    days,
  };
}

test("the timetable line is every scheduled departure, by day type, in route order", () => {
  const doc = { schedule: recorded({ "2026-09-22": 0, "2026-09-26": 1 }) };
  const line = scheduleLine(doc, 0, 2, ["2026-09-22", "2026-09-26"]);
  assert.equal(line.source, "recorded");
  assert.deepEqual(plain(line.series.map(s => s.dayType)), ["weekday", "saturday"]);
  assert.deepEqual(plain(line.series[0].points.map(p => [p.departSecs, p.scheduledSecs])),
                   [[25200, 900], [28800, 900]]);
  assert.deepEqual(plain(line.series[1].points.map(p => p.scheduledSecs)), [1200]);
  // The trip calling at 2 before 0 is going the other way, not promising a
  // negative journey.
  assert.ok(!line.series.flatMap(s => s.points).some(p => p.tripId === "BACK"));
  // A pair through an interpolated stop is drawn, but not as a promise.
  const partial = scheduleLine(doc, 0, 1, ["2026-09-22"]);
  assert.equal(partial.series[0].points[0].promised, false);
});

test("without a recorded timetable the line says it came from tracked journeys", () => {
  const doc = { stops: [{}, {}], journeys: [tracked("2026-09-22", 9, 0, 25, 20)] };
  const line = scheduleLine(doc, 0, 1, ["2026-09-22"], between(doc, 0, 1));
  assert.equal(line.source, "journeys");
  assert.deepEqual(plain(line.series[0].points.map(p => p.scheduledSecs)), [20 * MIN]);
});

test("coverage counts scheduled journeys in the period against the ones tracked", () => {
  const doc = { stops: [{}, {}, {}], schedule: recorded({ "2026-09-22": 0 }),
                journeys: [{ day: "2026-09-22", start: "07:00", trip_id: "W1", direction: "westbound",
                             calls: [[0, 25200, 25200, 0], [2, 26200, 26100, 0]] }] };
  const all = between(doc, 0, 2);
  assert.deepEqual({ ...coverage(doc, 0, 2, ["2026-09-22"], all) },
                   { scheduled: 2, tracked: 1, days: 1 });
  // The morning peak is 08:00 to 10:00: the 08:00 is in it, the 07:00 is
  // early, and the daytime period holds neither.
  assert.equal(coverage(doc, 0, 2, ["2026-09-22"], all, "08-10").scheduled, 1);
  assert.equal(coverage(doc, 0, 2, ["2026-09-22"], all, "other").scheduled, 1);
  assert.equal(coverage(doc, 0, 2, ["2026-09-22"], all, "10-16").scheduled, 0);
  assert.equal(coverage({ stops: [] }, 0, 2, ["2026-09-22"], all), null);
});

test("delay colours follow the agreed bands and never paint thin evidence", () => {
  const floor = { journeys: 30, distinct_days: 5 };
  const cell = secs => ({ sample_sufficient: true, median_gained_secs: secs,
                          journeys: 40, distinct_days: 6 });
  assert.equal(band(cell(-90), floor), "green", "making up time is not a delay");
  assert.equal(band(cell(59), floor), "green");
  assert.equal(band(cell(60), floor), "yellow");
  assert.equal(band(cell(119), floor), "yellow");
  assert.equal(band(cell(120), floor), "amber");
  assert.equal(band(cell(149), floor), "amber");
  assert.equal(band(cell(150), floor), "red");
  assert.equal(band({ ...cell(600), sample_sufficient: false }, floor), "none");
  // Marked sufficient but under the floor it states: trust the floor.
  assert.equal(band({ ...cell(600), journeys: 12 }, floor), "none");
  assert.equal(band(null, floor), "none");
});

test("the timetable step line breaks across the night instead of promising 3 a.m.", () => {
  const x = s => s / 60, y = s => s;
  const d = stepPath([{ departSecs: 23 * 3600, scheduledSecs: 600 },
                      { departSecs: 23 * 3600 + 1800, scheduledSecs: 600 },
                      { departSecs: 6 * 3600 + 86400 * 0, scheduledSecs: 700 }].sort(
                        (a, b) => a.departSecs - b.departSecs), x, y);
  assert.equal((d.match(/M/g) || []).length, 2, `expected two runs: ${d}`);
});

test("in journey-time mode the chart draws the timetable, not one flat median", () => {
  const doc = { stops: [{}, {}, {}], schedule: recorded({ "2026-09-22": 0 }),
                journeys: [tracked("2026-09-22", 7, 0, 16, 15)] };
  const all = between({ ...doc, journeys: [{ day: "2026-09-22", start: "07:00", trip_id: "W1",
    direction: "westbound", calls: [[0, 25200, 25200, 0], [2, 26200, 26100, 0]] }] }, 0, 2);
  const summary = summarise(all);
  const withLine = chart(all, summary, "duration", 640,
                         { schedule: scheduleLine(doc, 0, 2, ["2026-09-22"]), variant: "simple" });
  assert.match(withLine, /class="jt-timetable jt-timetable--weekday"/);
  assert.doesNotMatch(withLine, /median timetable/);
  assert.match(withLine, /Timetable, weekdays/);
  // A step line, not a circle on every departure: on a ten-minute route that
  // was hundreds of marks saying nothing the line does not.
  assert.doesNotMatch(withLine, /jt-timetable-stop/);
  const withoutLine = chart(all, summary, "duration", 640);
  assert.match(withoutLine, /median timetable/);
});

// ── Charts a reader can read ──────────────────────────────────

const niceStep = vm.runInContext("jtNiceStep", app);
const chartSecs = vm.runInContext("jtChartSecs", app);
const chartSpan = vm.runInContext("jtChartSpan", app);
const hourly = vm.runInContext("jtHourlyTypical", app);
const dayChart = vm.runInContext("journeyTimesDayChart", app);
const defaultHour = vm.runInContext("jtDefaultHour", app);

/** `n` journeys from stop 0 to 1 on `day`, one every `gap` minutes from `h`:00. */
function many(n, day = "2026-09-22", h = 7, gap = 10, took = 30) {
  return Array.from({ length: n }, (_, i) => {
    const mins = h * 60 + i * gap;
    return tracked(day, Math.floor(mins / 60) % 24, mins % 60, took, 25, `N${i}`);
  });
}

test("axis ticks come in steps a reader can count in", () => {
  // The old ticks were the range divided by five: 0, 17, 34, 51, 68.
  for (const range of [3, 9, 17, 34, 68, 75, 140]) {
    const step = niceStep(range);
    assert.ok([1, 2, 5, 10, 15, 20, 30, 60, 120].includes(step), `step ${step} for ${range} min`);
    assert.ok(range / step <= 6, `${range / step} ticks for ${range} min`);
  }
  const doc = { stops: [{}, {}], journeys: many(12, "2026-09-22", 7, 20, 61) };
  const all = between(doc, 0, 1);
  const svg = chart(all, summarise(all), "duration", 640);
  const ticks = [...svg.matchAll(/text-anchor="end">(\d+)<\/text>/g)].map(m => Number(m[1]));
  assert.ok(ticks.length >= 3 && ticks.every(v => v % 5 === 0), `ticks ${ticks.join(", ")}`);
});

test("the chart's day starts at four, so the last buses of the night come last", () => {
  assert.equal(chartSecs(4 * 3600), 0);
  assert.equal(chartSecs(30 * 60), 20.5 * 3600, "00:30 belongs at the end of the day, not the start");
  const span = chartSpan([chartSecs(5.5 * 3600), chartSecs(0.5 * 3600)]);
  assert.equal(span.from, 1, "the chart should open at 05:00");
  assert.equal(span.to, 21, "and run past half past midnight");
  // Never narrower than six hours, so one busy hour is not a chart of one hour.
  const narrow = chartSpan([chartSecs(8 * 3600)]);
  assert.ok(narrow.to - narrow.from >= 6);
});

test("the typical time is only drawn through hours with at least three journeys", () => {
  const journeys = [...many(4, "2026-09-22", 8, 10), ...many(2, "2026-09-22", 14, 10)];
  const rows = hourly(between({ stops: [{}, {}], journeys }, 0, 1));
  assert.equal(rows[8].journeys, 4);
  assert.equal(rows[8].medianSecs, 30 * 60);
  assert.equal(rows[14].journeys, 2);
  assert.equal(rows[14].medianSecs, null, "two buses made a typical time");
  assert.equal(rows[14].fastestSecs, 30 * 60, "the hour still says what it saw");
});

test("the passenger's chart shades the chosen time rather than hiding the rest of the day", () => {
  const journeys = [...many(20, "2026-09-22", 6, 30, 30), ...many(4, "2026-09-22", 8, 5, 45)];
  const doc = { stops: [{}, {}, {}], schedule: recorded({ "2026-09-22": 0 }), journeys };
  const all = between(doc, 0, 1);
  const got = dayChart(all, { width: 390, periodKey: "08-10", dayKey: "weekday", hour: 8,
                              schedule: scheduleLine(doc, 0, 2, ["2026-09-22"]) });
  assert.match(got.svg, /class="jt-band"/, "the morning peak is not shaded");
  assert.equal((got.svg.match(/<circle class="jt-dot/g) || []).length, all.length,
    "journeys outside the chosen time were dropped instead of shaded around");
  assert.match(got.svg, /class="jt-typical"/);
  assert.match(got.svg, /class="jt-hour-sel"/);
  assert.match(got.svg, /class="jt-timetable jt-timetable--weekday"/);
  // Nothing in the drawing takes focus: the hour slider is the way in.
  assert.doesNotMatch(got.svg, /tabindex/);
  assert.match(got.key, /Typical time/);
  assert.match(got.key, /Timetable/);
  const none = dayChart(all, { width: 390, periodKey: "any", dayKey: "weekday" });
  assert.doesNotMatch(none.svg, /jt-band/, "all day shaded something");
});

test("a busy route's dots shrink and fade instead of becoming a blob", () => {
  const all = between({ stops: [{}, {}], journeys: many(400, "2026-09-22", 5, 2, 12) }, 0, 1);
  const busy = dayChart(all, { width: 390, dayKey: "weekday" });
  assert.match(busy.svg, /jt-dot jt-dot--dense/);
  assert.match(busy.svg, /r="2\.4"/);
  // Detailed keeps every dot reachable, through one tab stop.
  const detailed = chart(all, summarise(all), "duration", 640);
  assert.equal((detailed.match(/tabindex="0"/g) || []).length, 1, "every dot was its own tab stop");
  assert.equal((detailed.match(/tabindex="-1"/g) || []).length, all.length - 1);
});

test("the chart opens on the first busy hour of the chosen time", () => {
  const all = between({ stops: [{}, {}], journeys: [...many(3, "2026-09-22", 6, 10), ...many(6, "2026-09-22", 8, 10), ...many(9, "2026-09-22", 12, 5)] }, 0, 1);
  const rows = hourly(all);
  const span = chartSpan(all.map(t => chartSecs(Number(t.start.slice(0, 2)) * 3600)));
  assert.equal(defaultHour(rows, span, "08-10", null), 8, "the first busy hour of the morning peak");
  assert.equal(defaultHour(rows, span, "10-16", null), 12, "10:00 and 11:00 had no buses; 12:00 did");
  assert.equal(defaultHour(rows, span, "any", 6), 6, "the current hour, when it has buses");
  assert.equal(defaultHour(rows, span, "any", 3), 12, "otherwise the busiest hour");
});

test("an hour with ten journeys says how long to allow, on the headline's own rule", () => {
  const journeys = Array.from({ length: 12 }, (_, i) => tracked("2026-09-22", 8, i * 4, 10 + i, 10, `P${i}`));
  const rows = hourly(between({ stops: [{}, {}], journeys }, 0, 1));
  // Twelve journeys taking 10..21 min: the ninth-in-ten is index 10, 20 min.
  assert.equal(rows[8].p90Secs, 20 * MIN);
  assert.equal(rows[8].longerThanP90, 1);
  const few = hourly(between({ stops: [{}, {}], journeys: journeys.slice(0, 9) }, 0, 1));
  assert.equal(few[8].p90Secs, null, "nine journeys gave an hour its own 'allow'");
  const words = vm.runInContext("jtLongerWords", app);
  assert.equal(words(12, 1), "Only 1 of the 12 buses we timed took longer.");
  assert.equal(words(10, 0), "None of the 10 buses we timed took longer.");
  assert.equal(words(80, 8), "Only 1 in 10 buses we timed took longer.");
});

test("how long to allow is said for each hour, not drawn over the chart", () => {
  // The band made the chart harder to read, so the figure moved into the words
  // the hour slider gives and the table under it.
  const busy = Array.from({ length: 24 }, (_, i) => tracked("2026-09-22", 8 + Math.floor(i / 12), (i % 12) * 5, 10 + (i % 7), 10, `B${i}`));
  const all = between({ stops: [{}, {}], journeys: busy }, 0, 1);
  const got = dayChart(all, { width: 640, dayKey: "weekday" });
  assert.doesNotMatch(got.svg, /jt-spread/);
  assert.doesNotMatch(got.key, /Time to allow/);
  const detail = vm.runInContext("jtHourDetailText", app);
  const rows = hourly(all);
  // Twelve journeys taking 10 to 16 min: the middle is 12.5, the ninth in ten
  // is 15, and one (the 16) took longer.
  assert.match(detail(rows[8], "weekday", null), /usually 13 min, from 12 buses timed\. Allow 15 min: only 1 of the 12 buses we timed took longer\./);
});

test("every hour of the chart is also a row in a table", () => {
  const table = vm.runInContext("jtHoursTableHtml", app);
  const journeys = [...many(12, "2026-09-22", 8, 4, 20), ...many(2, "2026-09-22", 14, 10, 30)];
  const all = between({ stops: [{}, {}], journeys }, 0, 1);
  const rows = hourly(all);
  const span = chartSpan(all.map(t => chartSecs(Number(t.start.slice(0, 2)) * 3600)));
  const html = table(rows, span, { series: [] }, all, "weekday");
  assert.match(html, /<th scope="row">08:00<\/th>\s*<td>20 min<\/td>\s*<td>20 min<\/td>/);
  assert.match(html, /<th scope="row">14:00<\/th>\s*<td>too few<\/td>/, "two buses gave a usual time");
  assert.doesNotMatch(html, /<th scope="row">03:00/, "an hour with nothing timed has a row");
});

// ── Words a passenger can act on ───────────────────────────────

const allowHtml = vm.runInContext("jtAllowHtml", app);
const versus = vm.runInContext("jtVersusTimetable", app);
const chips = vm.runInContext("jtWhenChipsHtml", app);
const slotSummaries = vm.runInContext("journeyTimesSlotSummaries", app);

test("how long to allow is said as how many buses took longer, counted", () => {
  const took = mins => mins.map((m, i) => tracked("2026-09-22", 8, i, m, 20, `A${i}`));
  const ten = summarise(between({ stops: [{}, {}], journeys: took([20, 21, 22, 23, 24, 25, 26, 27, 28, 29]) }, 0, 1));
  assert.match(allowHtml(ten), /Allow <strong>29 min<\/strong>/);
  assert.match(allowHtml(ten), /None of the 10 buses we timed took longer/,
    "with ten journeys the ninth-in-ten is the slowest, so none took longer");
  const twenty = summarise(between({ stops: [{}, {}], journeys: took(Array.from({ length: 20 }, (_, i) => 20 + i)) }, 0, 1));
  assert.match(allowHtml(twenty), /Only 1 of the 20 buses we timed took longer/);
  const hundred = summarise(between({ stops: [{}, {}], journeys: Array.from({ length: 100 },
    (_, i) => tracked("2026-09-22", 6 + Math.floor(i / 10), (i % 10) * 5, 20 + (i % 25), 20, `H${i}`)) }, 0, 1));
  assert.match(allowHtml(hundred), /Only 1 in 10 buses we timed took longer/);
  const few = summarise(between({ stops: [{}, {}], journeys: took([20, 21, 22]) }, 0, 1));
  assert.match(allowHtml(few), /We need 10 timed journeys[\s\S]*We have 3 so far/);
  assert.doesNotMatch(allowHtml(few), /Allow/);
});

test("the timetable comparison agrees with the two numbers on screen", () => {
  assert.equal(versus({ medianSecs: 61 * 60, scheduledSecs: 60 * 60 }), "About 1 min more than the timetable's 60 min.");
  assert.equal(versus({ medianSecs: 34 * 60 + 20, scheduledSecs: 28 * 60 }), "About 6 min more than the timetable's 28 min.");
  assert.equal(versus({ medianSecs: 26 * 60, scheduledSecs: 28 * 60 }), "About 2 min less than the timetable's 28 min.");
  assert.equal(versus({ medianSecs: 28 * 60 + 20, scheduledSecs: 28 * 60 }), "About the same as the timetable's 28 min.");
  assert.match(versus({ medianSecs: 1500, scheduledSecs: null }), /nothing to compare with/);
});

const frequency = vm.runInContext("jtFrequency", app);
const frequencyLabel = vm.runInContext("jtFrequencyLabel", app);

/** A schedule series with departures every `gap` minutes from `from` to `to`. */
function everyFew(dayType, gap, from = 6 * 60, to = 23 * 60) {
  const points = [];
  for (let m = from; m <= to; m += gap) points.push({ departSecs: m * 60, scheduledSecs: 1200, promised: true });
  return { dayType, points };
}

test("the badge says how often buses run, and the 700 has plenty all through a weekday", () => {
  // The 700 as the week's departures show it: every 12 min from 08:00 to
  // 18:00, every 20 either side of that.
  const seven = { source: "recorded", series: [{ dayType: "weekday", points: [
    ...everyFew("weekday", 20, 6 * 60, 8 * 60 - 20).points,
    ...everyFew("weekday", 12, 8 * 60, 18 * 60).points,
    ...everyFew("weekday", 20, 18 * 60 + 20, 23 * 60).points] }] };
  for (const period of ["any", "08-10", "10-16", "16-18"]) {
    assert.equal(frequency(seven, period).level, "plenty", `the 700 ${period} was not plenty`);
  }
  assert.equal(frequency(seven, "other").level, "some", "the 700 early and late is every 20 min");
  assert.equal(frequencyLabel(frequency(seven, "10-16")), "Plenty of buses · every 12 min");
  assert.equal(frequencyLabel(frequency({ source: "recorded", series: [everyFew("weekday", 30)] }, "any")),
    "Some buses · every 30 min");
  assert.equal(frequencyLabel(frequency({ source: "recorded", series: [everyFew("weekday", 60)] }, "any")),
    "Only a few buses · about hourly");
  const one = { source: "recorded", series: [{ dayType: "weekday", points: [{ departSecs: 9 * 3600, scheduledSecs: 900 }] }] };
  assert.equal(frequencyLabel(frequency(one, "08-10")), "Only a few buses · 1 bus a day");
  assert.equal(frequency(one, "16-18"), null, "no buses at all is no badge, not 'only a few'");
});

test("Saturday and Sunday are measured apart, not interleaved into a service twice as frequent", () => {
  // Every 20 min on each day, offset by ten: pooled, they look every 10.
  const weekend = { source: "journeys", series: [everyFew("saturday", 20, 8 * 60, 20 * 60),
                                                 everyFew("sunday", 20, 8 * 60 + 10, 20 * 60)] };
  assert.equal(frequency(weekend, "any").minutes, 20);
  assert.equal(frequency(weekend, "any").level, "some");
});

test("before the timetable is recorded, frequency comes from several days of tracked buses", () => {
  const fromSchedule = vm.runInContext("jtFrequencySchedule", app);
  // Stops the timetable gives no time at still have buses leaving them.
  const unpromised = (day, h, m) => ({ ...tracked(day, h, m, 20, 20, `U${day}${h}${m}`),
    calls: [[0, h * 3600 + m * 60, h * 3600 + m * 60, 2], [1, h * 3600 + m * 60 + 1200, h * 3600 + m * 60 + 1200, 2]] });
  const weekdays = ["2026-09-21", "2026-09-22", "2026-09-23"];
  const journeys = weekdays.flatMap((day, d) => [0, 10, 20, 30, 40, 50].map(m => unpromised(day, 9, m)))
    .concat([0, 30].map(m => unpromised("2026-09-26", 9, m)));          // one Saturday
  const doc = { stops: [{}, {}], journeys };
  const all = between(doc, 0, 1);
  const got = fromSchedule(doc, 0, 1, [...weekdays, "2026-09-26"], all);
  assert.equal(got.source, "journeys");
  assert.deepEqual(plain(got.series.map(s => s.dayType)), ["weekday"],
    "one Saturday of tracked buses was used to say how often buses run");
  assert.equal(frequency(got, "08-10").minutes, 10, "unpromised departures were left out");
  // Once the timetable is recorded it is the source, exact.
  const recordedDoc = { stops: [{}, {}, {}], schedule: recorded({ "2026-09-22": 0 }), journeys: [] };
  assert.equal(fromSchedule(recordedDoc, 0, 2, ["2026-09-22"], []).source, "recorded");
});

test("the night between the last bus and the first is not a gap in the service", () => {
  // Early and late wraps midnight: the last evening bus and the first morning
  // one are hours apart, and that is the night, not the frequency.
  const s = { source: "recorded", series: [everyFew("weekday", 15, 5 * 60, 24 * 60 - 15)] };
  assert.equal(frequency(s, "other").minutes, 15);
});

test("each time of day says how long it usually takes, and one with nothing is not offered", () => {
  const journeys = [...many(12, "2026-09-22", 8, 5, 41), ...many(3, "2026-09-22", 11, 10, 30)];
  const all = between({ stops: [{}, {}], journeys }, 0, 1);
  const html = chips("weekday", "any", { summaries: slotSummaries(all, "weekday"),
    dayCounts: new Map([["weekday", 15], ["weekend", 0]]) });
  assert.match(html, /data-jt-period="08-10"[^>]*aria-label="Morning peak, 08:00 to 10:00, usually 41 min"/);
  assert.match(html, /08–10 · 41 min/);
  assert.match(html, /data-jt-period="10-16"[^>]*aria-label="Daytime, 10:00 to 16:00, only 3 buses timed"/);
  assert.match(html, /data-jt-period="16-18"[^>]*disabled/, "an evening with no buses timed is offered");
  assert.match(html, /data-jt-day="weekend"[^>]*disabled/, "weekends with nothing timed are offered");
  assert.doesNotMatch(chips("weekday", "08-10", { showAll: false }), /data-jt-period="any"/,
    "the delay map has no all-day figure to offer");
});

// ── Typing a stop ──────────────────────────────────────────────

const matches = vm.runInContext("jtStopMatches", app);

test("typed stops are found by name, and To offers only a direct bus from From", () => {
  const place = (label, services, atco = label) => ({ label, name: label.split(",")[0], atco, atcos: [atco], services });
  const choices = [
    place("Old Steine", ["1", "2", "7", "700"]),
    place("Steine Gardens", ["7"]),
    place("Marine Parade, Worthing", ["700", "10"]),
    place("Marine Parade, Brighton", ["12", "14"]),
    place("Hollingbury Asda", ["5B"]),
  ];
  assert.deepEqual(plain(matches("steine", choices).map(c => c.label)), ["Steine Gardens", "Old Steine"].sort((a, b) =>
    a === "Steine Gardens" ? -1 : 1), "a name that starts with what was typed comes first");
  assert.deepEqual(plain(matches("s", choices)), [], "one letter is not a search");
  const fromOldSteine = choices[0];
  const to = plain(matches("marine", choices, fromOldSteine).map(c => c.label));
  assert.deepEqual(to, ["Marine Parade, Worthing"], "a stop no bus from Old Steine reaches was offered");
  assert.deepEqual(plain(matches("old", choices, fromOldSteine)), [], "From was offered as its own destination");
  assert.deepEqual(plain(matches("asda", choices, fromOldSteine)), []);
});

test("a stop can be found by the place it is in, and by words in any order", () => {
  // NaPTAN calls Shoreham's main stop "High Street", and the Hollingbury Asda
  // stop "Asda Crowhurst Road". Nobody types either.
  const place = (label, locality, services) => ({ label, name: label, atco: label, atcos: [label], locality, services });
  const choices = [
    place("High Street", "Shoreham-by-Sea", ["2", "700"]),
    place("Asda Crowhurst Road", "Hollingbury", ["5B"]),
    place("Hove Station", "Hove", ["7"]),
    place("Church Road", "Hove", ["1", "6"]),
  ];
  assert.deepEqual(plain(matches("shoreham high", choices).map(c => c.label)), ["High Street"]);
  assert.deepEqual(plain(matches("hollingbury", choices).map(c => c.label)), ["Asda Crowhurst Road"]);
  assert.deepEqual(plain(matches("hove", choices).map(c => c.label)), ["Hove Station", "Church Road"],
    "a stop named for the place should come before one merely in it");
});

// ── Names for the evidence behind a figure ─────────────────────

const cohortLabels = vm.runInContext("jtCohortLabels", app);

test("timetable versions are named by when they were in use, not by hash", () => {
  const row = (day, pattern, version, method = 4) => ({ day, routePattern: pattern, dataVersion: version, methodVersion: method });
  const rows = [
    ...Array.from({ length: 5 }, () => row("2026-09-16", "main", "timetable.sqlite sha256:aaaa1111")),
    ...Array.from({ length: 3 }, () => row("2026-09-18", "main", "timetable.sqlite sha256:aaaa1111")),
    ...Array.from({ length: 2 }, () => row("2026-09-21", "short", "timetable.sqlite sha256:bbbb2222", 3)),
  ];
  const labels = [...cohortLabels(rows).values()];
  assert.equal(labels.length, 2);
  assert.match(labels[0], /^Timetable in use 16 Sept? to 18 Sept? · main route · 8 journeys$/);
  assert.match(labels[1], /route variant 2 · earlier matching · 2 journeys$/);
  assert.ok(labels.every(l => !/sha256|Pattern/.test(l)), labels.join(" | "));
  // Two versions that read the same are told apart.
  const twins = [...cohortLabels([row("2026-09-16", "p", "v sha256:cccc3333"), row("2026-09-16", "p", "v sha256:dddd4444")]).values()];
  assert.notEqual(twins[0], twins[1]);
});


// ── The delay map says only what its evidence can ────────────

const cellFor = vm.runInContext("delayCellFor", app);
const stretchHtml = vm.runInContext("delayStretchHtml", app);
const amount = vm.runInContext("delayAmount", app);
const oneIn = vm.runInContext("delayOneIn", app);
const slotOf = vm.runInContext("delaySlot", app);

const FLOOR = { journeys: 30, distinct_days: 5 };
const cellAt = over => ({ stretch: "s", latest: true, day_type: "weekday", resolution: "period",
  period: "08-10", median_gained_secs: 150, journeys: 40, distinct_days: 6, traversals: 40,
  at_least_600s: 10, sample_sufficient: true, ...over });

test("a stretch is coloured by its own time slot, latest timetable, best evidence", () => {
  const doc = { cells: [
    // Busier but on too few days: thin evidence, however many buses it holds.
    cellAt({ journeys: 60, distinct_days: 2, sample_sufficient: false, median_gained_secs: 900 }),
    cellAt({ journeys: 35 }),
    cellAt({ journeys: 99, latest: false }),                  // an older timetable's promise
    cellAt({ journeys: 50, period: "16-18" }),                // another time of day
    cellAt({ journeys: 50, day_type: "weekend" }),
    cellAt({ journeys: 45, resolution: "hour", hour: 8, period: undefined }),
  ] };
  const period = { dayType: "weekday", resolution: "period", period: "08-10" };
  assert.equal(cellFor(doc, "s", period).journeys, 35,
    "the sufficient cell must speak for the stretch, not the busier stale or thin one");
  assert.equal(cellFor(doc, "s", { dayType: "weekday", resolution: "hour", hour: 8 }).journeys, 45);
  assert.equal(cellFor(doc, "s", { dayType: "weekday", resolution: "hour", hour: 9 }), null);
  assert.equal(cellFor(doc, "other", period), null);
});

test("delay amounts are finer than whole minutes, because the bands are", () => {
  assert.equal(amount(40), "40 seconds");
  assert.equal(amount(-40), "40 seconds");
  assert.equal(amount(60), "1 min");
  assert.equal(amount(150), "2.5 min");
  assert.equal(oneIn({ traversals: 40, at_least_600s: 0 }), "None of them lost 10 minutes or more here.");
  assert.equal(oneIn({ traversals: 40, at_least_600s: 10 }), "About 1 in 4 lost 10 minutes or more here.");
});

test("a stretch's words match its evidence, and never colour thin evidence as a finding", () => {
  const slot = { hint: "07:00–10:00 on weekdays" };
  const stretch = { id: "s", from_name: "Shoreham High St", to_name: "Kingston Bay Rd",
                    direction: "westbound", approximate: false };
  assert.match(stretchHtml(stretch, cellAt({}), FLOOR, slot), /usually lose\s+<strong>2\.5 min<\/strong>/);
  assert.match(stretchHtml(stretch, cellAt({}), FLOOR, slot), /middle of 40 journeys over 6\s+days/);
  assert.match(stretchHtml(stretch, cellAt({ median_gained_secs: -70 }), FLOOR, slot), /usually make up/);
  const thin = stretchHtml(stretch, cellAt({ sample_sufficient: false, journeys: 12, distinct_days: 2 }), FLOOR, slot);
  assert.match(thin, /Not enough journeys yet: 12 of the 30 needed/);
  assert.doesNotMatch(thin, /usually lose/);
  assert.match(stretchHtml(stretch, null, FLOOR, slot, false, "Towards Worthing"), /Towards Worthing, 07:00/);
  assert.match(stretchHtml(stretch, cellAt({ sample_sufficient: false, journeys: 12 }), FLOOR, slot, true),
               /Early evidence only/);
  assert.match(stretchHtml(stretch, null, FLOOR, slot), /No journeys tracked on this stretch/);
  assert.match(stretchHtml({ ...stretch, approximate: true }, cellAt({}), FLOOR, slot), /Drawn roughly/);
});

test("stop names from the feed are escaped before they reach a popup", () => {
  const html = stretchHtml({ id: "s", from_name: '<img src=x onerror="alert(1)">', to_name: "B",
                             direction: "eastbound" }, null, FLOOR, { hint: "x" });
  // Case-insensitive: prettifyName writes it as "<Img".
  assert.doesNotMatch(html, /<img/i);
});

test("the passenger's time buttons and the expert's hour slider name the same groups", () => {
  const was = vm.runInContext("JSON.stringify({ slot: jtDelay.slot, day: jtDelay.day, time: jtDelay.time, hour: jtDelay.hour })", app);
  // The same day and time chips as My journey: a weekend morning can now be
  // asked for, where the old row had one "weekend daytime" chip.
  vm.runInContext('jtDelay.day = "weekend"; jtDelay.slot = "08-10"', app);
  assert.deepEqual(plain(slotOf("simple")), { dayType: "weekend", resolution: "period", period: "08-10",
    label: "Morning peak", hint: "08:00–10:00 on weekends" });
  // The map has no all-day figure, so "All day" from My journey opens a peak.
  vm.runInContext('jtDelay.day = "weekday"; jtDelay.slot = "any"', app);
  assert.equal(plain(slotOf("simple")).period, "08-10");
  vm.runInContext('jtDelay.time = "hour"; jtDelay.hour = 8; jtDelay.dayType = "weekday"', app);
  const hour = plain(slotOf("detailed"));
  assert.equal(hour.resolution, "hour");
  assert.equal(hour.hour, 8);
  assert.equal(hour.label, "08:00–09:00");
  vm.runInContext(`Object.assign(jtDelay, ${was})`, app);
});

test("delay-map directions are named where the buses go, not by compass", () => {
  const labels = vm.runInContext("delayDirectionLabels", app);
  const got = plain([...labels([
    { direction: "westbound", headsign: "Hardwick Road" }, { direction: "westbound", headsign: "Hardwick Road" },
    { direction: "eastbound", headsign: "Hollingbury Asda" }, { direction: "eastbound", headsign: "Imperial Arcade C7" },
    { direction: "eastbound", headsign: "Hollingbury Asda" },
  ]).entries()]);
  assert.deepEqual(got, [["westbound", "Towards Hardwick Road"], ["eastbound", "Towards Hollingbury Asda"]]);
  // Both directions to one place tells the reader nothing: the compass stays.
  const same = plain([...labels([{ direction: "westbound", headsign: "Loop" }, { direction: "eastbound", headsign: "Loop" }]).values()]);
  assert.deepEqual(same, ["Westbound", "Eastbound"]);
});

test("a destination from the feed cannot write into the delay map's direction chips", () => {
  const labels = vm.runInContext("delayDirectionLabels", app);
  const chipsHtml = vm.runInContext("delayDirectionChipsHtml", app);
  const ways = labels([{ direction: "westbound", headsign: '<img src=x onerror="alert(1)">' },
                       { direction: "eastbound", headsign: "Marina" }]);
  const html = chipsHtml(["westbound", "eastbound"], d => ways.get(d), "westbound");
  // Case-insensitive: prettifyName title-cases it to "<Img", which a
  // case-sensitive pattern passed straight over.
  assert.doesNotMatch(html, /<img/i, "a headsign reached the page as markup");
  assert.match(html, /Towards Marina/);
  assert.equal(chipsHtml(["westbound"], d => d, "westbound"), "", "one direction needs no chips");
});

test("the view follows a link over memory, and Detailed stays shut without preview", () => {
  const view = () => vm.runInContext("jtView()", app);
  const set = search => vm.runInContext(`location.search = ${JSON.stringify(search)}; jtEntry.view = undefined`, app);
  try {
    set("?jt-view=detailed");
    assert.equal(view(), "simple", "Detailed opened without its switch or preview");
    set("?preview=1&jt-view=detailed");
    assert.equal(view(), "detailed");
    set("?preview=1&jt-service=700-SCSO.json");
    assert.equal(view(), "detailed", "a link from before the two views existed was made in Detailed");
    set("?preview=1");
    assert.equal(view(), "simple");
  } finally {
    set("");
  }
});

// ── The compact wire form ───────────────────────────────────
// The nightly build writes each service compact (scripts/journey_times_codec.py
// says why). The browser has to expand it to exactly what the Python codec
// started from: one fixture, read by both, so the two cannot drift apart.

const CODEC = JSON.parse(readFileSync(join(ROOT, "tests/fixtures/journey_times_codec.json"), "utf8"));
const expandDocument = vm.runInContext("jtExpandDocument", app);

test("a compact document expands to exactly the document it was written from", () => {
  const compact = structuredClone(CODEC.compact);
  assert.deepEqual(plain(expandDocument(compact)), CODEC.expanded);
  assert.deepEqual(compact, CODEC.compact, "expanding changed the document it was handed");
});

test("a document that is not compact is read as it is", () => {
  const doc = structuredClone(CODEC.expanded);
  assert.equal(expandDocument(doc), doc);
});

test("the loader expands a compact file before anything reads a journey", async () => {
  const before = app.fetch;
  app.fetch = async () => ({ ok: true, status: 200, json: async () => structuredClone(CODEC.compact) });
  try {
    const doc = await vm.runInContext("loadJourneyTimes", app)("700-SCSO.json");
    assert.deepEqual(plain(doc.journeys), CODEC.expanded.journeys);
    assert.equal(doc.encoding, undefined);
  } finally {
    app.fetch = before;
  }
});
