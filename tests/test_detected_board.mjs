/** Diversions detected in the buses' own positions, on the site: always said
 *  to be unconfirmed, on Travel Updates and on the boards they touch. */
import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

const app = loadApp();
const state = vm.runInContext("state", app);
const D = { id: "abc", operator: "SCSO", service: "700", direction: "eastbound", first_seen: "2026-11-02",
            journeys: 40, usual_share: 0.9, recent_share: 0,
            stops: [{ atco: "S1", name: "BRUNSWICK PLACE" }, { atco: "S2", name: "<b>Norfolk</b> Square" }] };

test("a detection says where, since when, how sure, and that the operator has not confirmed it", () => {
  const html = app.detectedDiversionHtml(D);
  assert.match(html, /Brunswick Place to/);
  assert.doesNotMatch(html, /<b>Norfolk/);
  assert.match(html, /seen on 0% of\s+40 journeys/);
  assert.match(html, /90% usually/);
  assert.match(html, /not confirmed by the operator/);
});

test("a board says so only at a stop the detection names", () => {
  state.detectedDiversions = { detections: [D] };
  assert.match(app.boardDetectedHtml("S2"), /The 700 has not been passing this stop/);
  assert.match(app.boardDetectedHtml("S2"), /Not confirmed by the operator/);
  assert.equal(app.boardDetectedHtml("S9"), "");
  state.detectedDiversions = null;
});
