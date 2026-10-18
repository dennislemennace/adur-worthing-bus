/**
 * figures.html's script: the headline figures, built from the evidence file
 * the weekly rebuild publishes. Each claim is a sentence written by hand
 * ("about two thirds", "a quarter"), so this checks the sentence still fits
 * the number every time the evidence changes, the way the objectives' quoted
 * figures are checked (tests/test_curated_data.py).
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { ROOT } from "./load_app.mjs";

const context = vm.createContext({ console });
vm.runInContext(readFileSync(join(ROOT, "figures.js"), "utf8"), context);
const fn = name => vm.runInContext(name, context);
const EVIDENCE = JSON.parse(readFileSync(join(ROOT, "data/boundary_evidence.json"), "utf8"));
const text = html => html.replace(/<[^>]+>/g, " ").replace(/&amp;/g, "&").replace(/\s+/g, " ");

test("the hand-written claims still fit the generated evidence", () => {
  const [band, places, night] = fn("boundaryFigures")(EVIDENCE);
  const m = EVIDENCE.days.monday, p = EVIDENCE.places.days.monday;
  const bandShare = m.west.departures_per_stop / m.east.departures_per_stop;
  assert.ok(bandShare > 0.6 && bandShare < 0.72, `"about two thirds" no longer fits ${bandShare.toFixed(2)}`);
  const placeShare = p.west.departures_per_stop / p.east.departures_per_stop;
  assert.ok(placeShare > 0.2 && placeShare < 0.31, `"a quarter" no longer fits ${placeShare.toFixed(2)}`);
  const n = EVIDENCE.night.nights.weeknight.by_area;
  const nightShare = n.lancing.departures_per_stop / n["south-portslade"].departures_per_stop;
  assert.ok(nightShare > 0.18 && nightShare < 0.32, `"about a quarter" at night no longer fits ${nightShare.toFixed(2)}`);
  assert.match(text(band.value), new RegExp(`${m.west.departures_per_stop} weekday departures per stop`));
  assert.match(text(places.value), new RegExp(`${p.east.departures_per_stop} weekday departures per stop`));
  assert.ok(night, "the night figure is missing");
});

test("every figure carries its method, date, caveat and source", () => {
  const figures = [...fn("boundaryFigures")(EVIDENCE), ...fn("REPLAY_FIGURES"),
    fn("identificationFigure")({ days: ["2026-09-25", "2026-10-14"], operators: {
      BHBC: { scheduled: 10, tracked: 9, by: { journey_id: 8, start: 0, position: 1 } },
      SCSO: { scheduled: 10, tracked: 10, by: { journey_id: 2, start: 8, position: 0 } } } }, "https://example.org/i.json")];
  for (const f of figures) {
    for (const key of ["claim", "value", "method", "asOf", "caveat", "sourceUrl", "sourceLabel"]) {
      assert.ok(String(f[key] || "").trim(), `${f.id} has no ${key}`);
    }
  }
  const ident = figures[figures.length - 1];
  assert.match(text(ident.value), /18 of 19 tracked journeys \( ?95% ?\)/);
  const html = fn("figureHtml")({ ...ident, claim: "<script>x</script>" });
  assert.doesNotMatch(html, /<script>/, "a claim is escaped");
});
