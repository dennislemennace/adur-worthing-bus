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
  assert.deepEqual(fn("timingSearch")(stops, "station portslade").map(s => s.atco_code), ["B"]);
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
