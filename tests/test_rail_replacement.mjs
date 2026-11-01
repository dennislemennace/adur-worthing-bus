/**
 * Rail replacement buses, on a station's board and on the map (asked for
 * 1 November 2026).
 *
 * The board marks a replacement bus as a bus, where a platform would be, and
 * says where the buses leave from, from the operator's poster
 * (data/rail_replacement_stops.json). A bus on the live map whose route is
 * "Rail" is said to be one: that is how Metrobus 6986 reported itself while
 * running Brighton's buses on 1 November 2026, to a destination of
 * "Destination".
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
const run = (code) => vm.runInContext(code, app);
const STOPS = JSON.parse(readFileSync(join(ROOT, "data", "rail_replacement_stops.json"), "utf8")).stations;

const BUS = { mode: "bus", uid: "U1", departureDate: "2026-11-01", atocCode: "SN", headcode: "0B00",
  destination: [{ description: "Lewes" }], departure: { scheduled: "2026-11-01T18:31:00" } };
const TRAIN = { mode: "train", uid: "U2", departureDate: "2026-11-01", atocCode: "SN", headcode: "1A23",
  platform: "4", destination: [{ description: "Lewes" }], departure: { scheduled: "2026-11-01T18:40:00" } };

test("a replacement bus on the board is a bus, not a train with no platform", () => {
  const row = run(`renderRailBoardRow(${JSON.stringify(BUS)})`);
  assert.match(row, /rail-plat--bus/);
  assert.match(row, />Bus</);
  assert.match(row, /Replacement bus/);
  assert.doesNotMatch(row, /data-rail-action="select"/, "a bus has no train position to track");
  const train = run(`renderRailBoardRow(${JSON.stringify(TRAIN)})`);
  assert.doesNotMatch(train, /Replacement bus/);
  assert.match(train, /data-rail-action="select"/);
});

test("the board says where the buses leave from, when any are due", () => {
  const note = run(`railReplacementNoteHtml("BTN", [${JSON.stringify(BUS)}], ${JSON.stringify(STOPS)})`);
  assert.match(note, /Turning circle on Stroudley Road/);
  assert.match(note, /platform 8/);
  assert.match(note, /data-rail-action="rrb-stop"/, "and can show it on the map");
  assert.equal(run(`railReplacementNoteHtml("BTN", [${JSON.stringify(TRAIN)}], ${JSON.stringify(STOPS)})`), "");
});

test("a station with no known stop says to follow the signs, rather than guessing", () => {
  const note = run(`railReplacementNoteHtml("SSE", [${JSON.stringify(BUS)}], ${JSON.stringify(STOPS)})`);
  assert.match(note, /signs/);
  assert.doesNotMatch(note, /rrb-stop/);
});

test("a live bus whose route is Rail is a rail replacement bus", () => {
  assert.equal(run(`isRailReplacementVehicle({ service_ref: "Rail" })`), true);
  assert.equal(run(`isRailReplacementVehicle({ service_ref: "RAIL" })`), true);
  assert.equal(run(`isRailReplacementVehicle({ service_ref: "700" })`), false);
  assert.equal(run(`isRailReplacementVehicle(null)`), false);
});
