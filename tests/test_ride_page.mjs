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
