/**
 * A night service is the same route after dark: same vehicles, same livery,
 * same colour on the map. These tests pin that, because the bug they were
 * written against was invisible from the code — ROUTE_ICONS and ROUTE_COLOURS
 * each carried night entries for *some* routes, so the N1 looked right and
 * nobody noticed the N14 and N29 falling through to Brighton & Hove's generic
 * red bus.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { loadApp } from "./load_app.mjs";

const app = loadApp();

// The night services Brighton & Hove publish that have a day route wearing a
// dedicated livery. N5, N25 and N48 are deliberately absent: routes 5, 25 and
// 48 have no icon of their own, so there is nothing for them to inherit.
const NIGHT_TO_DAY = [
  ["N1",  "1"],    // pink
  ["N7",  "7"],    // purple
  ["N12", "12"],   // Coaster
  ["N14", "14"],   // Coaster
  ["N29", "29"],   // Regency
];

for (const [night, day] of NIGHT_TO_DAY) {
  test(`${night} wears the ${day}'s livery, not the operator's generic bus`, () => {
    const dayIcon   = app.iconForService("BHBC", day);
    const nightIcon = app.iconForService("BHBC", night);
    assert.ok(dayIcon, `route ${day} has no livery, so this pairing proves nothing`);
    assert.equal(nightIcon, dayIcon,
      `${night} resolved to ${nightIcon} — a night service falling back to the ` +
      `operator's generic bus is the exact defect this guards`);
  });

  test(`${night} draws in the ${day}'s colour`, () => {
    assert.equal(app.getRouteColour(night, "BHBC"), app.getRouteColour(day, "BHBC"));
    assert.equal(app.getLineColour(night, "BHBC"), app.getLineColour(day, "BHBC"));
  });
}

test("an unbranded night service still falls back to its operator", () => {
  // Route 48 has no livery, so N48 should land on the generic B&H bus rather
  // than borrowing one. The fallback must not invent an identity.
  assert.equal(app.iconForService("BHBC", "N48"), app.iconForService("BHBC", "48"));
  assert.equal(app.iconForService("BHBC", "N48"), app.iconForService("BHBC", "999"));
});

test("the night fallback does not cross operators", () => {
  // Stagecoach's N700 must not pick up a Brighton & Hove livery just because
  // the number matches. An icon is a stronger claim than a colour.
  assert.notEqual(app.iconForService("SCSO", "N1"), app.iconForService("BHBC", "1"));
});

test("stripNightPrefix only strips N followed by digits", () => {
  // "N1" is a night route; "NX" or "N" alone are not, and a route legitimately
  // named with a leading N would otherwise be rewritten into something else.
  assert.equal(app.stripNightPrefix("N29"), "29");
  assert.equal(app.stripNightPrefix("29"),  "29");
  assert.equal(app.stripNightPrefix("N29X"), "N29X");
  assert.equal(app.stripNightPrefix(""), "");
});
