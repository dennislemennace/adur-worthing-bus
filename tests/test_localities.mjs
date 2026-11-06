/** Route view's Areas: each ward shaded by buses per stop on fixed bands, so
 *  a Sunday reads as fewer buses than a weekday; and each ward's card says
 *  what was counted, with its councillors, escaped. */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { loadApp, ROOT } from "./load_app.mjs";

const app = loadApp();
const DOC = JSON.parse(readFileSync(join(ROOT, "data/localities.json"), "utf8"));

test("the shades are fixed bands of buses per stop, and no buses is no shade", () => {
  assert.deepEqual([0, -3, null, 1, 14.9, 15, 59, 60, 119.9, 120, 300].map(v => app.areaBand(v)),
    [-1, -1, -1, 0, 0, 1, 2, 3, 3, 4, 4]);
  assert.deepEqual([0, 1, 2, 3, 4].map(i => app.areaBandLabel(i)),
    ["Under 15", "15 to 29", "30 to 59", "60 to 119", "120 or more"]);
});

test("a ward's card gives each day's figures, its caveat and its councillors, escaped", () => {
  const w = JSON.parse(JSON.stringify(DOC.wards.find(x => !x.inside) || DOC.wards[0]));
  w.name = "<img src=x onerror=alert(1)>";
  w.councillors = { council: "A council", members: [{ name: "A <b>Person</b>", party: "Ind", url: "javascript:alert(1)" }] };
  const html = app.areaCardHtml(w, DOC);
  assert.doesNotMatch(html, /<img src=x|<b>Person|href="javascript:/i);
  for (const label of ["Weekday", "Saturday", "Sunday", "Buses per stop", "First and last", "Through the night"]) {
    assert.match(html, new RegExp(label), label);
  }
  if (!w.inside) assert.match(html, /outside the area this site covers/);
  assert.match(html, /Scheduled departures, not buses that ran/);
});
