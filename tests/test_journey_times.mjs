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
  assert.equal(west.headsign, "Shoreham-by-Sea  (+1 more)");
  assert.equal(west.places.length, 2, "the rare destination was dropped, not counted");
  assert.equal(west.places[1].journeys, 1);
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
  assert.equal(first.headsign.split("  ")[0], "Marina Cinema",
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
