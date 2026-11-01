/**
 * Two things on the Live board, both asked for on 1 November 2026.
 *
 * An empty board offers the nearest stops with a bus coming, a button each
 * (the API works out which: api/main.py _nearby_active_stops), and keeps the
 * timetable's next times below them.
 *
 * Typing in the stop search on a phone puts the box halfway up the space the
 * keyboard leaves, with the matching stops under it, rather than wherever the
 * browser's own nudge leaves it: just above the keys, with the list under them.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

const app = loadApp();
const run = (code) => vm.runInContext(code, app);

const STOPS = [
  { atco: "4400NB100", name: "East Road", metres: 540, direction: "east", routes: ["1", "23"], next: "2026-11-01T19:35:00+00:00",
    destination: "Durrington" },
  { atco: "4400NB300", name: "North <Street>", metres: 560, direction: "north-east", routes: ["1"], next: "2026-11-01T19:34:00+00:00" },
  { atco: "4400NB400", name: "South Street", metres: 890, direction: "north", routes: ["700"], next: "2026-11-01T18:55:00+00:00" },
];

test("an empty board offers a button for each nearby stop with a bus coming", () => {
  const html = run(`boardNearbyHtml(${JSON.stringify(STOPS)})`);
  const buttons = html.match(/<button type="button" class="board-nearby-stop"/g) || [];
  assert.equal(buttons.length, 3);
  assert.match(html, /data-atco="4400NB100"/);
  assert.match(html, /East Road/);
  assert.match(html, /540 m east/, "how far, and which way to walk");
  assert.match(html, /1, 23/, "the routes due there");
  assert.match(html, /next 19:35 to Durrington/,
    "the next bus there, on the London clock, and where it goes: what tells two poles of one name apart");
  assert.doesNotMatch(html, /<Street>/, "a stop name is text, not markup");
  assert.doesNotMatch(html, /Stop ID/, "with no stop details loaded, nothing reads as a code");
});

test("no nearby stops, no block", () => {
  assert.equal(run("boardNearbyHtml([])"), "");
  assert.equal(run("boardNearbyHtml(undefined)"), "");
});

test("the sheet rises until the search box is halfway up what the keyboard leaves", () => {
  // A 390x844 phone at peek: the box's middle at 702, a keyboard leaving 480px.
  const h = run(`searchSheetHeight({ sheetH: 248, boxTop: 680, boxHeight: 44,
                                     visibleTop: 0, visibleHeight: 480, minH: 248, maxH: 742 })`);
  assert.equal(h, 248 + (702 - 240), "the box's middle moves up to the middle of the visible 480px");
  // Already panned by the browser: the middle of what shows is lower on the page.
  const panned = run(`searchSheetHeight({ sheetH: 248, boxTop: 680, boxHeight: 44,
                                          visibleTop: 200, visibleHeight: 480, minH: 248, maxH: 742 })`);
  assert.equal(panned, 248 + (702 - 440));
});

test("the sheet never goes past its own top or bottom for the search", () => {
  const args = (top, visible) => `{ sheetH: 400, boxTop: ${top}, boxHeight: 44,
    visibleTop: 0, visibleHeight: ${visible}, minH: 248, maxH: 742 }`;
  assert.equal(run(`searchSheetHeight(${args(800, 200)})`), 742, "a landscape keyboard leaves almost nothing");
  assert.equal(run(`searchSheetHeight(${args(100, 800)})`), 248);
});

test("before the keyboard is up, the space it will leave is estimated", () => {
  // Focus comes before the keyboard. Lifting then is what keeps the browser
  // from nudging the page itself, so the estimate has to be a phone keyboard's.
  const visible = run("searchVisibleHeight(844, 844)");
  assert.ok(visible > 844 * 0.5 && visible < 844 * 0.7, `estimated ${visible}px above the keyboard`);
  assert.equal(run("searchVisibleHeight(480, 844)"), 480, "once it is up, the real space is used");
});
