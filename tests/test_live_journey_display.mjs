/**
 * What the map may say about a bus, now the feed names its journey.
 *
 * Until GTFS-RT arrived there was nothing honest to put in the status chip:
 * this feed publishes no Delay field at all — 0 of 235 vehicles carried one —
 * and the journey was inferred from position and service, right about four
 * times in five. "Probably a 700" is fine for drawing a dot on a map and not
 * good enough to tell someone which departure they are looking at.
 *
 * So the rule these tests pin is: say it when the operator said it, and say
 * nothing when we are guessing.
 *
 * Run with:  node --test tests/test_live_journey_display.mjs
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

function chip(vehicle) {
  const app = loadApp();
  return vm.runInContext("buildStatusChip", app)(vehicle);
}

// ── The status chip ─────────────────────────────────────────

test("lateness against the declared journey becomes the status", () => {
  // Compared as JSON: objects built inside the app's realm are never
  // deepEqual to ones built here, which has caught this suite out before.
  assert.equal(JSON.stringify(chip({ lateness_secs: 240 })),
    JSON.stringify({ label: "4 min late", cssClass: "status-late" }));
  assert.equal(JSON.stringify(chip({ lateness_secs: -180 })),
    JSON.stringify({ label: "3 min early", cssClass: "status-early" }));
  assert.equal(chip({ lateness_secs: 20 }).label, "On time");
});

test("a bus with nothing to go on still says nothing", () => {
  // No declared journey and no Delay field: "Scheduled" is the honest label,
  // not "On time". The feed has told us nothing about this bus's punctuality.
  assert.equal(JSON.stringify(chip({})),
    JSON.stringify({ label: "Scheduled", cssClass: "status-scheduled" }));
});

test("the operator's own status still wins where it exists", () => {
  // Rail boards and some bus feeds send a status string. Deriving one from
  // lateness when the operator has stated theirs would overrule them.
  assert.equal(chip({ status: "cancelled", lateness_secs: 0 }).label, "Cancelled");
  assert.equal(chip({ status: "on time", lateness_secs: 600 }).label, "On time");
});

test("a minute either way is on time, and the boundary is not fudged", () => {
  // The label is for a reader, so rounding to the nearest minute is right —
  // but 90 seconds late must not read as on time.
  assert.equal(chip({ lateness_secs: 59 }).label, "On time");
  assert.equal(chip({ lateness_secs: 90 }).label, "2 min late");
  assert.equal(chip({ lateness_secs: -90 }).label, "2 min early");
});

// The rendered panel — that the journey line appears only for a declared
// journey — is checked in scripts/browser_check.mjs against the real page.
// A stubbed DOM here would assert that our stub renders, which is not the
// question anyone is asking.
