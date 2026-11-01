/**
 * A bus opened on the map draws its whole journey as a faint line, split
 * where the bus is: fainter behind it, where it has come from, and stronger
 * ahead, where it is going (asked for 1 November 2026). The split is the
 * point of the journey's line nearest the bus.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

const app = loadApp();
const run = (code) => vm.runInContext(code, app);

// West to east along the coast road, then a turn north.
const LINE = [[50.83, -0.30], [50.83, -0.29], [50.83, -0.28], [50.83, -0.27], [50.84, -0.27]];

test("the journey splits at the point nearest the bus", () => {
  assert.equal(run(`busJourneySplit(${JSON.stringify(LINE)}, [50.8302, -0.2805])`), 2);
  assert.equal(run(`busJourneySplit(${JSON.stringify(LINE)}, [50.839, -0.2701])`), 4);
});

test("a bus with no position has the whole journey still ahead", () => {
  assert.equal(run(`busJourneySplit(${JSON.stringify(LINE)}, [NaN, NaN])`), 0);
  assert.equal(run(`busJourneySplit(${JSON.stringify(LINE)}, null)`), 0);
});

test("the split is measured on the ground, not in raw degrees", () => {
  // A degree of longitude here is about 0.63 of a degree of latitude. The
  // bus is 0.02 degrees south of the second point and 0.025 west of the
  // third: nearer the second in raw degrees (2.2 km), the third on the
  // ground (1.8 km).
  const line = [[50.83, -0.33], [50.85, -0.28], [50.83, -0.255]];
  assert.equal(run(`busJourneySplit(${JSON.stringify(line)}, [50.83, -0.28])`), 2);
});
