/**
 * report.html's script: one route's report laid out with every figure's count
 * beside it, a thin row saying it is thin, and a CSV a spreadsheet cannot be
 * tricked into running.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { ROOT } from "./load_app.mjs";

const context = vm.createContext({ console, URLSearchParams });
vm.runInContext(readFileSync(join(ROOT, "report.js"), "utf8"), context);
const fn = name => vm.runInContext(name, context);
const text = html => html.replace(/<[^>]+>/g, " ").replace(/&amp;/g, "&").replace(/&#39;/g, "'").replace(/\s+/g, " ");

const counts = (judged, on_time, days, sufficient = true) => ({
  judged, on_time, early: 0, late: judged - on_time, very_late: 0, days, sample_sufficient: sufficient });

const DOC = {
  service: "700", operator: "SCSO", window: ["2026-09-25", "2026-10-14"], days: ["2026-09-25"],
  method_version: 1, method_versions: [5, 7], data_versions: ["abc"], as_of: "2026-10-15T03:30:00+00:00",
  method: "Punctuality counts departures from GTFS timing points.", caveats: ["Not yet checked against times someone saw."],
  floor: { judged: 30, distinct_days: 5 },
  identification: { scheduled: 3548, tracked: 3370, by: { journey_id: 1302, start: 1370, position: 698 } },
  punctuality: { definition: { standard: 0.95 }, timings: 100, disrupted_timings: 50, rows: [
    { direction: "westbound", day_type: "weekday", ...counts(5871, 4297, 12),
      excluding_disruptions: counts(791, 600, 12), daily: { days: 11, min: 0.62, median: 0.714, max: 0.846 } },
    { direction: "eastbound", day_type: "weekend", ...counts(12, 10, 2, false), daily: null }],
    by_period: [] },
  trend: [{ week: "2026-W41", ...counts(6284, 4823, 7), median_lateness_secs: 90 }],
  waits: [], bottlenecks: [], disruptions: [{ label: "the Western Road diversion", from: "2026-09-28T07:00:00+01:00",
    to: null, journeys_marked: 1259 }],
  sources: [{ day: "2026-09-25", sha256: "f00d" }],
};

test("the report gives every share with its count, and says when a row is thin", () => {
  const html = text(fn("reportHtml")(DOC, { build_id: "b".repeat(64), observation_sources: [
    { sha256: "f00d", day: "2026-09-25", url: "https://example.org/obs.json.gz" }] }));
  assert.match(html, /Route report: 700, Stagecoach South/);
  assert.match(html, /Draft\. Stop times come from the buses' own location reports/);
  assert.match(html, /3,370 of 3,548 \(95%\) journeys timetabled while we were recording were tracked/);
  assert.match(html, /Westbound, weekdays 4,297 of 5,871 \(73%\) 62% to 85%, typically 71% \(11 days\)/);
  assert.match(html, /600 of 791 \(76%\)/, "the figure without disrupted journeys is missing");
  assert.match(html, /Eastbound, weekends 12 timed over 2 days; needs 30 over 5/);
  assert.match(html, /50% of these timings were on journeys through a recorded disruption/);
  assert.match(html, /the Western Road diversion, from 28 September 2026: 1259 journeys marked/);
  assert.match(fn("reportHtml")(DOC, { observation_sources: [{ sha256: "f00d", url: "https://example.org/obs.json.gz" }] }),
    /href="https:\/\/example\.org\/obs\.json\.gz"/, "the evidence is not linked");
});

test("the CSV holds the counts, and keeps a formula as text", () => {
  const csv = fn("reportCsv")({ ...DOC, trend: [{ week: "=HYPERLINK(1)", ...counts(1, 1, 1, false) }] });
  const lines = csv.trim().split("\n");
  assert.equal(lines[0], "section,direction,day_type,period_or_week,judged,on_time,early,late,very_late,days,sample_sufficient");
  assert.equal(lines[1], "punctuality,westbound,weekday,,5871,4297,0,1574,0,12,true");
  assert.match(csv, /punctuality_excluding_disruptions,westbound,weekday,,791,600/);
  assert.match(csv, /by_week,,,'=HYPERLINK\(1\)/, "a formula reached the spreadsheet");
});

test("the page asks for the same file the nightly build writes", () => {
  assert.equal(fn("reportFileName")("700", "SCSO"), "route-report-700-SCSO.json");
  assert.equal(fn("reportFileName")("N700", "SCSO"), "route-report-N700-SCSO.json");
  assert.equal(fn("reportFileName")("<x>", ""), "route-report-x-unknown.json");
  const html = readFileSync(join(ROOT, "report.html"), "utf8");
  assert.match(html, /<script src="report\.js" defer><\/script>/);
  assert.match(html, /@media print/);
});

test("the report says how often a bus left behind a later one, as a floor", () => {
  const doc = { ...DOC, overtaking: { directions: [{ direction: "eastbound", day_type: "weekday", checked: 117, overtaken: 12 }],
    worst: [{ direction: "eastbound", day_type: "weekday", period: "16-18", atco: "A", stop_name: "High Street",
      checked: 97, overtaken: 7, days: 14 }] } };
  const html = text(fn("reportHtml")(doc, {}));
  assert.match(html, /Buses leaving behind a later one/);
  assert.match(html, /Eastbound, weekdays 12 of 117 \(10%\)/);
  assert.match(html, /High Street, eastbound, weekdays, evening peak, 16:00–18:00: 7 of 97 \(7%\) over 14 days/);
  assert.match(html, /so these are floors/);
  assert.doesNotMatch(text(fn("reportHtml")(DOC, {})), /Buses leaving behind/, "no section without the counts");
});
