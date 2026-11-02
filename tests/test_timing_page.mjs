/**
 * timing.html's script: logging buses by hand at a stop, to check our own stop
 * times. The taps must pair "doors open" with "left" for the same bus, the CSV
 * must be what scripts/validate_hand_timings.py reads, and nothing it holds
 * may run as a spreadsheet formula.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { ROOT } from "./load_app.mjs";

const context = vm.createContext({ console, Intl, Date, Math });
vm.runInContext(readFileSync(join(ROOT, "timing.js"), "utf8"), context);
const fn = name => vm.runInContext(name, context);
const STOP = { atco_code: "4400AD0204", name: "High Street", towards: "Old Steine", latitude: 50.832, longitude: -0.275 };

test("doors open then left make one row; a departure alone makes its own", () => {
  const tap = fn("timingTap");
  let log = tap([], "arrived", { stop: STOP, route: "700", vehicle: "10950", now: new Date("2026-10-20T09:00:00Z") });
  log = tap(log, "departed", { stop: STOP, route: "", vehicle: "", now: new Date("2026-10-20T09:00:40Z") });
  assert.equal(log.length, 1);
  assert.deepEqual([log[0].arrived_at, log[0].departed_at, log[0].route, log[0].vehicle],
    ["2026-10-20T09:00:00.000Z", "2026-10-20T09:00:40.000Z", "700", "10950"]);
  log = tap(log, "departed", { stop: STOP, route: "2", vehicle: "832", now: new Date("2026-10-20T09:02:00Z") });
  assert.equal(log.length, 2);
  assert.deepEqual([log[0].arrived_at, log[0].route], ["", "2"], "a bus with no doors-open tap is its own row");
});

test("the CSV is what the validator reads, oldest first, formulas kept as text", () => {
  const tap = fn("timingTap");
  let log = tap([], "departed", { stop: STOP, route: "700", vehicle: "10950", offsetMs: 2500,
    now: new Date("2026-10-19T23:30:00Z") });
  log = tap(log, "departed", { stop: STOP, route: "=1+1", vehicle: "", now: new Date("2026-10-20T08:00:00Z") });
  const lines = fn("timingCsv")(log).trim().split("\n");
  assert.equal(lines[0], "id,day,atco,stop_name,towards,route,vehicle,arrived_at,departed_at,clock_offset_ms,note");
  assert.match(lines[1], /,2026-10-20,4400AD0204,High Street,Old Steine,700,10950,,2026-10-19T23:30:00.000Z,2500,$/,
    "half past midnight in London is 20 October, and the clock offset rides along");
  assert.match(lines[2], /,'=1\+1,/);
});

test("fleet numbers, stops near me and stop search", () => {
  assert.equal(fn("timingVehicleKey")("scso-10950"), "SCSO10950");
  const stops = [STOP, { atco_code: "B", name: "Station Road", locality: "Portslade", latitude: 50.835, longitude: -0.21 }];
  const near = fn("timingNearest")(stops, 50.8321, -0.2751, 1);
  assert.equal(near[0].atco_code, "4400AD0204");
  assert.ok(near[0].metres < 30);
  assert.deepEqual([...fn("timingSearch")(stops, "station portslade")].map(s => s.atco_code), ["B"]);
  const html = readFileSync(join(ROOT, "timing.html"), "utf8");
  assert.match(html, /<meta name="robots" content="noindex"/);
});

test("the stop's routes are buttons, the chosen one pressed, and nothing typed runs as markup", () => {
  const html = fn("timingRouteChipsHtml")(["2", "700", "N700", "<b>"], "n700");
  const pressed = [...html.matchAll(/data-route="([^"]+)"\s+aria-pressed="(true|false)"/g)]
    .map(m => [m[1], m[2]]);
  assert.deepEqual(pressed, [["2", "false"], ["700", "false"], ["N700", "true"], ["&lt;b&gt;", "false"]]);
  assert.ok(!html.includes("<b>"), "a route name is text, not markup");
  assert.equal(fn("timingRouteChipsHtml")(undefined, ""), "", "no stop chosen, no buttons");
});

test("a stop letter typed finds that pole, and a stop's own name beats buses heading there", () => {
  const letter = fn("timingLetter");
  assert.deepEqual(["Stop B6", "stand 3", "Bay 12", "opp", "o/s 14", ""].map(letter), ["B6", "3", "12", "", "", ""]);
  const pole = (atco_code, name, towards, l = "") => ({ atco_code, name, towards, letter: l, locality: "Brighton" });
  const stops = [
    pole("q1", "Queens Road", "Brighton Station"),
    pole("b4", "Brighton Station", "Bottom of Davey Drive", "B4"),
    pole("b6", "Brighton Station", "George Street (stop J)", "B6"),
    pole("t1", "Trafalgar Street", "Brighton Station B6"),
  ];
  const search = q => [...fn("timingSearch")(stops, q)].map(s => s.atco_code);
  for (const q of ["Brighton Station B6", "brighton station stop b6", "b6 brighton station"]) {
    assert.equal(search(q)[0], "b6", q);
  }
  assert.ok(!search("brighton station b6").includes("b4"), "another pole's letter is not a match");
  assert.deepEqual(search("brighton station").slice(0, 2).sort(), ["b4", "b6"],
    "the station's own poles before stops whose buses go there");
  assert.match(fn("timingStopLabel")(stops[2]), /Brighton Station, stop B6, towards George Street/);
});

const plain = (v) => JSON.parse(JSON.stringify(v));
/* Two poles of one stop either side of the road, and a neighbour on the
   same side further along. */
const POLES = [
  { atco_code: "N1", name: "Elm Grove", towards: "Northfield Crescent", latitude: 50.832874, longitude: -0.129013, bearing: "NE", services: ["25"] },
  { atco_code: "S1", name: "Elm Grove", towards: "Old Steine", latitude: 50.832702, longitude: -0.128949, bearing: "SW", services: ["25"] },
  { atco_code: "N2", name: "St Paul's Street", towards: "Northfield Crescent", latitude: 50.834177, longitude: -0.127285, bearing: "NE", services: ["25"] },
];

test("the pole across the road is found: same stop, the other way", () => {
  const other = fn("timingOtherSide");
  assert.equal(other(POLES, POLES[0]).atco_code, "S1");
  assert.equal(other(POLES, POLES[1]).atco_code, "N1");
  assert.equal(other(POLES, POLES[2]), null, "no pole of its own across the road");
});

test("an L and R pole each record their own stop, and switching keeps both", () => {
  const set = fn("timingSetPole");
  let sides = plain(set({ L: null, R: null, current: "L" }, "L", POLES[0], POLES));
  assert.deepEqual([sides.L.atco_code, sides.R.atco_code, sides.current], ["N1", "S1", "L"],
    "choosing one side fills the other with the pole across the road");
  sides = plain(set(sides, "R", POLES[2], POLES));
  assert.deepEqual([sides.L.atco_code, sides.R.atco_code, sides.current], ["N1", "N2", "R"],
    "a side chosen by hand is kept");
  const tap = fn("timingTap");
  let log = tap([], "arrived", { stop: POLES[0], route: "25", vehicle: "", now: new Date("2026-11-01T10:00:00Z") });
  log = tap(log, "arrived", { stop: POLES[1], route: "25", vehicle: "", now: new Date("2026-11-01T10:00:10Z") });
  log = tap(log, "departed", { stop: POLES[0], route: "", vehicle: "", now: new Date("2026-11-01T10:00:30Z") });
  const n1 = log.find(e => e.atco === "N1"), s1 = log.find(e => e.atco === "S1");
  assert.deepEqual([n1.departed_at, n1.towards, s1.departed_at, s1.towards],
    ["2026-11-01T10:00:30.000Z", "Northfield Crescent", "", "Old Steine"]);
});

test("a negative clock offset stays a number in the CSV", () => {
  // A phone 2.5 s slow exported '-2500, which the validator could not read.
  const log = fn("timingTap")([], "departed", { stop: STOP, route: "700", vehicle: "", offsetMs: -2500,
    now: new Date("2026-10-20T08:00:00Z") });
  assert.match(fn("timingCsv")(log), /,-2500,/);
});

test("after an export, the next finished bus asks before the same rows go out twice", () => {
  const tap = fn("timingTap");
  let log = tap([], "departed", { stop: STOP, route: "700", vehicle: "", now: new Date("2026-10-20T08:00:00Z") });
  const exported = { ids: log.map(e => e.id), reminded: false };
  const remind = fn("timingShouldRemind");
  log = tap(log, "arrived", { stop: STOP, route: "700", vehicle: "", now: new Date("2026-10-20T08:05:00Z") });
  assert.equal(remind(log, exported, log[0]), false, "not between doors open and the bus leaving");
  log = tap(log, "departed", { stop: STOP, route: "", vehicle: "", now: new Date("2026-10-20T08:05:30Z") });
  assert.equal(remind(log, exported, log[0]), true);
  assert.deepEqual(plain(fn("timingNewSinceExport")(log, exported)).map(e => e.id), [log[0].id]);
});

test("the timing page has the side toggle, notes with suggestions and a real dialog", () => {
  const html = readFileSync(join(ROOT, "timing.html"), "utf8");
  assert.match(html, /id="sides"/);
  assert.match(html, /<datalist id="timing-notes">[\s\S]*?<option value="[^"]+">/);
  assert.match(html, /<dialog id="dup"/);
  assert.match(readFileSync(join(ROOT, "timing.js"), "utf8"), /list="timing-notes"/);
  assert.doesNotMatch(html, /—/, "no em dashes in the copy");
});
