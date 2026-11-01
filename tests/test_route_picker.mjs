/**
 * Route view's route picker (asked for 1 November 2026): choose an operator,
 * then a number, and the tab shows that route alone, with its main places,
 * every stop a tap away, when it runs, its tickets and its disruptions. On a
 * computer its main places also run in a strip along the foot of the map.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

const app = loadApp();
// Plain values: arrays made inside the app's context have its prototypes, and
// strict deepEqual tells those from this file's.
const run = (code) => { const v = vm.runInContext(code, app); return typeof v === "object" && v ? JSON.parse(JSON.stringify(v)) : v; };
const J = (v) => JSON.stringify(v);

const stop = (n, timing, locality = "Shoreham-by-Sea") =>
  ({ atco: `S${n}`, name: `Stop ${n}`, locality, lat: 50.83, lon: -0.3 + n / 100, timing });
const STOPS = Array.from({ length: 12 }, (_, i) => stop(i, [0, 2, 3, 5, 6, 8, 9, 10, 11].includes(i)));
const DETAIL = {
  service: "19", operator: "COMT", days: ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat"], journeys: 75,
  first: "07:00", last: "19:05", after_midnight: 1, every_min: 20,
  directions: [{ from: "Stop 0", to: "The Longshore", stops: STOPS, line: [[50.83, -0.3], [50.83, -0.19]] },
               { from: "Stop 11", to: "Holmbush Centre", stops: [...STOPS].reverse(), line: [] }],
  disruptions: [],
};

test("a route's main places are its timing points, both ends kept, at most seven", () => {
  const picked = run(`routeMainPlaces(${J(STOPS)})`);
  assert.equal(picked.length, 7);
  assert.equal(picked[0], 0);
  assert.equal(picked[picked.length - 1], 11);
  assert.ok(picked.every(i => STOPS[i].timing), "only timing points");
  assert.deepEqual([...picked].sort((a, b) => a - b), picked, "in route order");
});

test("a route with no timing points still has main places, spread along it", () => {
  const plain = STOPS.map(s => ({ ...s, timing: false }));
  const picked = run(`routeMainPlaces(${J(plain)})`);
  assert.equal(picked[0], 0);
  assert.equal(picked[picked.length - 1], 11);
  assert.ok(picked.length >= 3 && picked.length <= 7);
});

test("the days a route runs, as a person would say them", () => {
  assert.equal(run(`routeDaysText(${J(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])})`), "every day");
  assert.equal(run(`routeDaysText(${J(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat"])})`), "Monday to Saturday");
  assert.equal(run(`routeDaysText(${J(["Mon", "Tue", "Wed", "Thu", "Fri"])})`), "Monday to Friday");
  assert.equal(run(`routeDaysText(${J(["Sat", "Sun"])})`), "Saturday and Sunday");
  assert.equal(run(`routeDaysText(${J(["Mon", "Wed", "Fri"])})`), "Monday, Wednesday and Friday");
});

test("when it runs: how often, the day's first and last, and after midnight", () => {
  const text = run(`routeWhenText(${J(DETAIL)})`);
  assert.match(text, /every 20 minutes/i);
  assert.match(text, /07:00/);
  assert.match(text, /19:05/);
  assert.match(text, /after midnight/);
  assert.match(text, /Monday to Saturday/);
  const sunday = run(`routeWhenText(${J({ ...DETAIL, journeys: 0, first: null, last: null, after_midnight: 0 })})`);
  assert.match(sunday, /No journeys today/);
  assert.match(sunday, /Monday to Saturday/);
});

test("the detail shows one way at a time, its main places, and every stop on request", () => {
  const html = run(`routeDetailHtml(${J(DETAIL)}, 0, false)`);
  assert.match(html, /data-route-dir="0"[^>]*aria-pressed="true"/);
  assert.match(html, /data-route-dir="1"[^>]*aria-pressed="false"/);
  assert.match(html, /To The Longshore/);
  assert.equal((html.match(/class="route-line-stop[^"]*"/g) || []).length, 7, "the main places only");
  assert.match(html, /Show all 12 stops/);
  const all = run(`routeDetailHtml(${J(DETAIL)}, 0, true)`);
  assert.equal((all.match(/class="route-line-stop[^"]*"/g) || []).length, 12);
  assert.match(all, /Show the main places only/);
  assert.match(html, /data-route-clear/, "and a way back to every route");
});

test("the strip along the map shows the main places of the way chosen", () => {
  const html = run(`routeStripHtml(${J(DETAIL)}, 1)`);
  assert.match(html, />19</);
  assert.equal((html.match(/class="route-strip-place"/g) || []).length, 7);
  assert.ok(html.indexOf("Stop 11") < html.indexOf("Stop 0"), "the way chosen, in its own order");
});

test("the picker lists each operator's routes, by number", () => {
  const groups = run(`routePickerGroups(${J([
    { operator: "COMT", service: "47" }, { operator: "BHBC", service: "12" }, { operator: "COMT", service: "19" },
    { operator: "BHBC", service: "2" }, { operator: "BHBC", service: "N1" }])})`);
  const comt = groups.find(g => g.operator === "COMT");
  assert.deepEqual(comt.services, ["19", "47"]);
  assert.deepEqual(groups.find(g => g.operator === "BHBC").services, ["2", "12", "N1"]);
});
