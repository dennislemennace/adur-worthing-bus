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

// ── Coaster family ──────────────────────────────────────────

test("route 27 and its variants wear the Coaster livery", () => {
  // 27 appears only in the live vehicle feed — there is no 27 anywhere in the
  // GTFS timetable — so a missing entry here shows up as a generic red bus on
  // the map rather than as an absent route, which is easy to miss. 27B and
  // 27C are listed ahead of ever being seen for the same reason.
  const coaster = app.iconForService("BHBC", "12");
  for (const svc of ["27", "27B", "27C"]) {
    assert.equal(app.iconForService("BHBC", svc), coaster,
      `${svc} is not wearing the Coaster livery`);
  }
});

test("the 27 family's colour matches the livery it wears", () => {
  // A Coaster-painted bus with a differently-coloured badge and route line
  // reads as a bug. These moved together, so they are asserted together.
  const coasterColour = app.getRouteColour("12", "BHBC");
  for (const svc of ["27", "27B", "27C"]) {
    assert.equal(app.getRouteColour(svc, "BHBC"), coasterColour,
      `${svc}'s colour has drifted from its livery`);
  }
});

test("the Coaster livery did not leak onto neighbouring routes", () => {
  // 26 and 28 sit either side of 27 in the colour table and are not Coaster.
  const coaster = app.iconForService("BHBC", "12");
  assert.notEqual(app.iconForService("BHBC", "26"), coaster);
  assert.notEqual(app.iconForService("BHBC", "28"), coaster);
  assert.notEqual(app.getRouteColour("28", "BHBC"), app.getRouteColour("27", "BHBC"));
});

test("a night 27 keeps the Coaster livery", () => {
  // The night fallback strips the N and retries, so N27 should land on 27.
  assert.equal(app.iconForService("BHBC", "N27"), app.iconForService("BHBC", "27"));
});
