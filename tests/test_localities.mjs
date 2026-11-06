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

import vm from "node:vm";
const CENSUS = JSON.parse(readFileSync(join(ROOT, "data/ward_census.json"), "utf8"));

test("a ward's card adds who lives there, the time lost on its roads, and a comparison", () => {
  const state = vm.runInContext("state", app);
  const [a, b] = DOC.wards;
  state.wardCensus = CENSUS;
  const census = app.areaCensusHtml(a.code);
  const c = CENSUS.wards[a.code];
  assert.match(census, new RegExp(`${c.households_no_car.toLocaleString("en-GB")} of`));
  assert.match(census, /Census 2021/);
  state.wardDelays = { window: { days: 35, to: "2026-11-01" }, wards: {
    [a.code]: { "weekday:10-16": { stretches: 2, median_lost_secs: 126, worst: { service: "700", from: "A <b>", to: "B", lost_secs: 300 } },
                "weekday:16-18": { stretches: 0 } } } };
  const delays = app.areaDelaysHtml(a.code);
  assert.match(delays, /2\.1 min/);
  assert.match(delays, /too few journeys/);
  assert.doesNotMatch(delays, /<b>/, "a stop name was not escaped");
  assert.equal(app.areaDelaysHtml("E05999999"), "");
  const cmp = app.areaCompareHtml(a, b);
  assert.match(cmp, new RegExp(app.escapeHtml(a.name)));
  assert.match(cmp, new RegExp(app.escapeHtml(b.name)));
  assert.match(cmp, /Households with no car/);
  state.wardCensus = null; state.wardDelays = null;
});

test("the letter to a ward's councillors goes to every one of them and quotes the figures, encoded", () => {
  const w = JSON.parse(JSON.stringify(DOC.wards[0]));
  w.name = "Hove & <Brunswick>";
  const href = app.wardLetterHref(w, ["a@x.gov.uk", "b@x.gov.uk"], DOC, CENSUS);
  assert.match(href, /^mailto:a%40x\.gov\.uk,b%40x\.gov\.uk\?subject=/);
  const body = decodeURIComponent(href.split("body=")[1]);
  assert.match(body, /Hove & <Brunswick>/, "the body did not round-trip");
  assert.match(body, new RegExp(`has ${w.days.monday.departures_per_stop} buses on average`));
  assert.match(body, new RegExp(`week of ${DOC.measured_week.monday}`));
  assert.doesNotMatch(href, /[\s<>]/, "the address was not encoded");
  assert.equal(app.wardLetterHref(w, [], DOC, CENSUS), "", "a letter to nobody");
});
