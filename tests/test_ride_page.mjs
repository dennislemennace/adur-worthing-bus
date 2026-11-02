/**
 * ride.html's script: timing a bus's doors from on board, stop by stop.
 *
 * The sibling of timing.html. There someone stands at one pole and times
 * every bus; here someone rides one bus and taps when its doors open and
 * close at each stop, and the phone's position says which stop it was. The
 * CSV leads with timing.html's columns, so scripts/validate_hand_timings.py
 * reads a ride exactly as it reads a stop session.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { ROOT } from "./load_app.mjs";

const ride = vm.createContext({ console, Intl, Date, Math });
vm.runInContext(readFileSync(join(ROOT, "ride.js"), "utf8"), ride);
const fn = (name) => vm.runInContext(name, ride);
const timing = vm.createContext({ console, Intl, Date, Math });
vm.runInContext(readFileSync(join(ROOT, "timing.js"), "utf8"), timing);

const RIDE = { ride_id: "r1", route: "700", vehicle: "10950" };
const HIGH_ST = { atco_code: "4400AD0204", name: "High Street", towards: "Old Steine", latitude: 50.8320, longitude: -0.2750 };
const AT = { lat: 50.8320, lon: -0.2750, accuracy: 12 };

test("doors open then closed make one stop; the next stop is the next in the ride", () => {
  const tap = fn("rideTap");
  let log = tap([], "open", { ride: RIDE, stop: HIGH_ST, position: AT, now: new Date("2026-11-01T09:00:00Z") });
  log = tap(log, "closed", { ride: RIDE, stop: null, position: AT, now: new Date("2026-11-01T09:00:35Z") });
  assert.equal(log.length, 1);
  assert.deepEqual([log[0].atco, log[0].arrived_at, log[0].departed_at, log[0].seq, log[0].route, log[0].vehicle],
    ["4400AD0204", "2026-11-01T09:00:00.000Z", "2026-11-01T09:00:35.000Z", 1, "700", "10950"]);
  log = tap(log, "open", { ride: RIDE, stop: null, position: { lat: 50.83, lon: -0.26, accuracy: 30 },
    now: new Date("2026-11-01T09:02:00Z") });
  assert.equal(log[0].seq, 2, "a second stop on the same ride");
  assert.equal(log[0].atco, "", "no stop found is left blank to fill in, not guessed");
});

test("doors closed with no open tap still records the stop", () => {
  const log = fn("rideTap")([], "closed", { ride: RIDE, stop: HIGH_ST, position: AT, now: new Date("2026-11-01T09:00:35Z") });
  assert.equal(log.length, 1);
  assert.deepEqual([log[0].arrived_at, log[0].departed_at], ["", "2026-11-01T09:00:35.000Z"]);
});

test("the stop guessed is the nearest one the route serves, within reach", () => {
  const stops = [
    { atco_code: "NEAR", name: "Near, other routes", latitude: 50.83201, longitude: -0.27501, services: ["2"] },
    { atco_code: "ON", name: "On the 700", latitude: 50.83230, longitude: -0.27500, services: ["700", "2"] },
    { atco_code: "FAR", name: "Far, on the 700", latitude: 50.8400, longitude: -0.2750, services: ["700"] },
  ];
  const g = JSON.parse(JSON.stringify(fn("rideStopGuess")(stops, 50.8320, -0.2750, "700")));
  assert.equal(g.best.atco_code, "ON");
  assert.deepEqual(g.choices.map(s => s.atco_code).slice(0, 2), ["NEAR", "ON"], "choices are nearest first");
  const none = fn("rideStopGuess")(stops, 50.9, -0.2750, "700");
  assert.equal(none.best, null, "nothing within reach is no guess at all");
});

test("the CSV leads with timing.html's columns, oldest first, formulas kept as text", () => {
  const tap = fn("rideTap");
  let log = tap([], "open", { ride: { ...RIDE, route: "=1+1" }, stop: HIGH_ST, position: AT, now: new Date("2026-11-01T09:00:00Z") });
  log = tap(log, "closed", { ride: RIDE, stop: null, position: AT, now: new Date("2026-11-01T09:00:35Z") });
  log = tap(log, "open", { ride: RIDE, stop: HIGH_ST, position: AT, now: new Date("2026-11-01T09:05:00Z") });
  const lines = fn("rideCsv")(log).trim().split("\n");
  const head = lines[0].split(",");
  const timingCols = JSON.parse(JSON.stringify(vm.runInContext("TIMING_COLUMNS", timing)));
  assert.deepEqual(head.slice(0, timingCols.length), timingCols,
    "the validator reads these columns by name; they must all be there");
  for (const extra of ["ride_id", "seq", "lat", "lon", "accuracy_m", "departed_basis"]) assert.ok(head.includes(extra), extra);
  assert.equal(lines.length, 3);
  assert.match(lines[1], /'=1\+1/, "a route typed as a formula stays text");
  assert.match(lines[1], /2026-11-01T09:00:00\.000Z/, "oldest first");
  assert.match(lines[1], /doors closed/, "says that departed_at is when the doors closed");
});

/* A line of stops along an east-west street, as the bus meets them heading
   east, with the opposite pole of the next stop across the road. */
const LINE = [
  { atco_code: "W", name: "Behind", latitude: 50.8300, longitude: -0.1300, services: ["37"], bearing: "E" },
  { atco_code: "HERE", name: "Here", latitude: 50.8300, longitude: -0.1280, services: ["37"], bearing: "E" },
  { atco_code: "E1", name: "Next", latitude: 50.8300, longitude: -0.1260, services: ["37"], bearing: "E" },
  { atco_code: "E1X", name: "Next", latitude: 50.83015, longitude: -0.1260, services: ["37"], bearing: "W" },
  { atco_code: "E2", name: "After that", latitude: 50.8300, longitude: -0.1240, services: ["37"], bearing: "E" },
  { atco_code: "OFF", name: "Other route", latitude: 50.8301, longitude: -0.1270, services: ["2"], bearing: "E" },
];
const plain = (v) => JSON.parse(JSON.stringify(v));

test("a tap before the first location fix is filled in when the fix arrives, not left blank", () => {
  // 1 Nov 2026: the first stop was tapped 1.7 s after Start, before the
  // phone had a fix, and the row was never given a stop.
  const tapAt = new Date("2026-11-01T19:24:10Z");
  const [row] = fn("rideTap")([], "open", { ride: { ...RIDE, route: "37" }, stop: null, position: null, now: tapAt });
  assert.equal(row.lat, "");
  const late = fn("rideLateFix");
  const fix = { lat: 50.8300, lon: -0.1280, accuracy: 6, at: tapAt.getTime() + 6000 };
  const filled = plain(late(row, fix, LINE));
  assert.deepEqual([filled.atco, filled.stop_name, filled.lat, filled.lon, filled.accuracy_m, filled.fix_offset_s],
    ["HERE", "Here", 50.83, -0.128, 6, 6], "the stop is found from the fix that came after the tap");
  assert.equal(late(row, { ...fix, at: tapAt.getTime() + 60000 }, LINE), null,
    "a fix a minute later is somewhere else on the route");
  const chosen = plain(late({ ...row, atco: "E2", stop_name: "After that", stop_by: "hand" }, fix, LINE));
  assert.equal(chosen.atco, "E2", "a stop chosen by hand is never overwritten");
  assert.equal(chosen.lat, 50.83, "but the position is still recorded");
});

test("with no position at all the stop list still offers the stops ahead and every stop on the route", () => {
  const c = plain(fn("rideCandidates")(LINE, { route: "37", at: null, after: LINE[1], heading: null }));
  assert.equal(c.near.length, 0);
  assert.equal(c.ahead[0].atco_code, "E1", "the next stop along, after the last one logged");
  assert.ok(!c.ahead.some(s => s.atco_code === "W" || s.atco_code === "E1X"),
    "not the stop behind, nor the pole across the road");
  assert.deepEqual(c.route.map(s => s.atco_code).sort(), ["E1", "E1X", "E2", "HERE", "W"],
    "every stop the route serves, to choose from by hand");
});

test("the stop ahead follows the direction of travel, and the right pole is chosen", () => {
  const ahead = plain(fn("rideAhead")(LINE, { lat: 50.8300, lon: -0.1279 }, 90, "37", { exclude: ["HERE"] }));
  assert.deepEqual(ahead.map(s => s.atco_code), ["E1", "E2"]);
  const west = plain(fn("rideAhead")(LINE, { lat: 50.8300, lon: -0.1279 }, 270, "37", { exclude: ["HERE"] }));
  assert.equal(west.length, 0, "heading west, the only stops behind us face east");
  // Two poles 17 m apart either side of the road: heading east, it is the east-bound one.
  const g = plain(fn("rideStopGuess")(LINE, 50.83012, -0.1260, "37", 6, 90));
  assert.equal(g.best.atco_code, "E1");
  assert.equal(fn("rideCompass")("SW"), 225);
});

test("a stop skipped and an unscheduled stop are their own rows, never door times", () => {
  const tap = fn("rideTap");
  const r = { ...RIDE, route: "37" };
  let log = tap([], "open", { ride: r, stop: LINE[1], position: AT, now: new Date("2026-11-01T19:30:00Z") });
  log = tap(log, "closed", { ride: r, stop: null, position: AT, now: new Date("2026-11-01T19:30:20Z") });
  log = tap(log, "skipped", { ride: r, stop: LINE[2], position: AT, now: new Date("2026-11-01T19:31:00Z") });
  assert.deepEqual([log[0].event, log[0].atco, log[0].arrived_at, log[0].departed_at, log[0].passed_at, log[0].seq],
    ["skipped", "E1", "", "", "2026-11-01T19:31:00.000Z", 2]);
  log = tap(log, "unscheduled", { ride: r, stop: LINE[4], position: AT, now: new Date("2026-11-01T19:32:00Z") });
  assert.deepEqual([log[0].event, log[0].atco, log[0].arrived_at], ["unscheduled", "", "2026-11-01T19:32:00.000Z"],
    "an unscheduled stop is not at a stop, whatever is nearby");
  log = tap(log, "closed", { ride: r, stop: LINE[4], position: AT, now: new Date("2026-11-01T19:32:30Z") });
  assert.equal(log[1].departed_at, "", "doors closing never completes an unscheduled stop");
  assert.equal(log[0].event, "stop");
  log = tap(log, "moving", { ride: r, stop: null, position: AT, now: new Date("2026-11-01T19:32:40Z") });
  assert.equal(log[1].departed_at, "2026-11-01T19:32:40.000Z", "moving again ends it");
  const lines = fn("rideCsv")(log).trim().split("\n");
  const head = lines[0].split(",");
  assert.deepEqual(head.slice(-3), ["event", "passed_at", "fix_offset_s"], "new columns go at the end");
  const at = (line, col) => line.split(",")[head.indexOf(col)];
  assert.deepEqual([at(lines[2], "event"), at(lines[2], "departed_basis"), at(lines[2], "day")],
    ["skipped", "", "2026-11-01"]);
  assert.deepEqual([at(lines[3], "event"), at(lines[3], "departed_basis")], ["unscheduled", "moving again"]);
  assert.equal(at(lines[1], "event"), "stop");
});

test("a negative number in the CSV stays a number; a formula still does not", () => {
  const log = fn("rideTap")([], "open", { ride: RIDE, stop: HIGH_ST, position: { lat: 50.83, lon: -0.134714, accuracy: 4 },
    offsetMs: -2500, now: new Date("2026-11-01T09:00:00Z") });
  log[0].note = "-tired";
  const line = fn("rideCsv")(log).trim().split("\n")[1];
  assert.match(line, /,-0\.134714,/, "a longitude west of Greenwich is not a formula");
  assert.match(line, /,-2500,/);
  assert.match(line, /,'-tired,/);
});

test("after an export, the next finished entry asks before the same rows go out twice", () => {
  const tap = fn("rideTap");
  let log = tap([], "open", { ride: RIDE, stop: HIGH_ST, position: AT, now: new Date("2026-11-01T09:00:00Z") });
  log = tap(log, "closed", { ride: RIDE, stop: null, position: AT, now: new Date("2026-11-01T09:00:30Z") });
  const exported = { ids: log.map(e => e.id), reminded: false };
  const remind = fn("rideShouldRemind");
  assert.equal(remind(log, exported, log[0]), false, "nothing new yet");
  log = tap(log, "open", { ride: RIDE, stop: HIGH_ST, position: AT, now: new Date("2026-11-01T09:03:00Z") });
  assert.equal(remind(log, exported, log[0]), false, "not while the doors are open: the next tap comes in seconds");
  log = tap(log, "closed", { ride: RIDE, stop: null, position: AT, now: new Date("2026-11-01T09:03:20Z") });
  assert.equal(remind(log, exported, log[0]), true);
  assert.equal(remind(log, { ...exported, reminded: true }, log[0]), false, "once per export");
  assert.deepEqual(plain(fn("rideNewSinceExport")(log, exported)).map(e => e.id), [log[0].id]);
  assert.equal(remind(log, { ids: [], reminded: false }, log[0]), false, "never exported, nothing to repeat");
});

test("the ride page has notes with suggestions, the two new buttons and a real dialog", () => {
  const html = readFileSync(join(ROOT, "ride.html"), "utf8");
  assert.match(html, /<datalist id="ride-notes">[\s\S]*?<option value="[^"]+">/);
  assert.match(html, /id="skip"/);
  assert.match(html, /id="unscheduled"/);
  assert.match(html, /<dialog id="dup"/);
  assert.match(html, /id="where"/);
  assert.match(readFileSync(join(ROOT, "ride.js"), "utf8"), /list="ride-notes"/);
  assert.doesNotMatch(html, /—/, "no em dashes in the copy");
});
