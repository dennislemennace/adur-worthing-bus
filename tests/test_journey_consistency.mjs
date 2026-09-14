/**
 * Tests that the ticket answer and the journey it describes are the same journey.
 *
 * Three disagreements, each found by pressing a preset:
 *
 *  - Sompting to the Marina had no itinerary, yet the fare panel said the
 *    Compass Rover "is valid on the whole of it" and priced "2 buses each way".
 *    Nothing knew which buses it was, so neither claim had anything behind it.
 *  - The itinerary only ever described two buses, so a three-bus journey could
 *    not be written down at all.
 *  - A weekend itinerary read "This is a Monday At weekends these two buses...",
 *    a sentence the em-dash pass left without its full stop.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

const app = loadApp();
const call = (expr) => vm.runInContext(expr, app);

const leg = (service, operator, depart, arrive, from, to) => ({
  service, operator, depart, arrive,
  stops: [{ name: from, lat: 50.83, lon: -0.35 }, { name: to, lat: 50.82, lon: -0.2 }],
});
const place = (atco, name) => ({ atco, name, lat: 50.82, lon: -0.2 });

const THREE = {
  legs: [leg("16", "COMT", "12:26", "12:44", "Upton Farm House", "Old Salts Farm Road"),
         leg("700", "SCSO", "12:49", "13:31", "Old Salts Farm Road", "North Street"),
         leg("47", "COMT", "13:48", "14:10", "Clock Tower", "Marina")],
  changes: [
    { change_at: place("A", "Old Salts Farm Road"), board_at: place("A", "Old Salts Farm Road"), walk_metres: 0, wait_minutes: 5 },
    { change_at: place("B", "North Street"), board_at: place("C", "Clock Tower"), walk_metres: 72, wait_minutes: 17 },
  ],
  change_at: place("A", "Old Salts Farm Road"), board_at: place("A", "Old Salts Farm Road"),
  walk_metres: 0, wait_minutes: 5, total_minutes: 104,
};

test("a three-bus itinerary names all three buses and both changes", () => {
  const html = call(`itineraryHtml(${JSON.stringify(THREE)}, "")`);
  const text = html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");
  for (const svc of ["16", "700", "47"]) assert.match(text, new RegExp(`\\b${svc}\\b`), `bus ${svc} is missing`);
  assert.match(text, /Old Salts Farm Road/);
  assert.match(text, /North Street/);
  assert.match(text, /72 m walk/, "the walk at the second change is not mentioned");
  assert.match(text, /three buses/, `the total does not say how many buses: ${text}`);
});

test("the journey's operators are all named", () => {
  const text = call(`itineraryHtml(${JSON.stringify(THREE)}, "")`).replace(/<[^>]+>/g, " ");
  assert.match(text, /Compass/);
  assert.match(text, /Stagecoach/);
});

test("a journey from another day reads as sentences", () => {
  const two = { ...THREE, legs: THREE.legs.slice(0, 2), changes: THREE.changes.slice(0, 1) };
  const text = call(`itineraryHtml(${JSON.stringify(two)}, "Monday")`)
    .replace(/<[^>]+>/g, "").replace(/\s+/g, " ");
  assert.doesNotMatch(text, /Monday At/, `a missing full stop: ${text}`);
  assert.match(text, /Monday\./);
});

test("a weekly ticket is only claimed valid for buses we know the journey takes", () => {
  // No operators known means no itinerary: the answer is to say nothing.
  const zones = [{ id: "rover", name: "Compass Rover", operator: "COMT", coverage_rule: "operator_network",
                   valid_on_operators: ["COMT"], fares: { adult_week: { price_pence: 3000 } } }];
  assert.equal(call(`weeklyOptionHtml(${JSON.stringify(zones)}, [], {}, null)`), "");
  const src = vm.runInContext("renderJourneyResult.toString()", app);
  assert.doesNotMatch(src, /weeklyOptionHtml\(allZones,\s*legOperators \|\| shared \|\|/,
    "the weekly claim still falls back to the operators at the two ends");
});

test("the singles count follows the buses in the itinerary", () => {
  const src = vm.runInContext("renderJourneyResult.toString()", app);
  assert.doesNotMatch(src, /const legs = option \? 1 : 2;/,
    "a three-bus journey is still costed as two buses each way");
});

test("a company running two legs is named once", () => {
  const phrase = call(`operatorPhrase(["COMT", "SCSO", "COMT"])`);
  assert.equal((phrase.match(/Compass/g) || []).length, 1, `repeated: ${phrase}`);
  assert.match(phrase, /Stagecoach/);
});
