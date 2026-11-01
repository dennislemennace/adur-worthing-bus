/**
 * The news section's two tabs: Travel Updates and News and Analysis.
 *
 * Travel Updates is what affects a journey now: diversions and disruptions in
 * force or about to start (data/disruptions.json, in their publishers' words),
 * the project's own pieces about them, and what passengers report. News and
 * Analysis is everything else the project publishes: Freedom of Information
 * replies, data releases and analysis. Each article says which it is with
 * `section` in data/updates.json (checked in tests/test_curated_data.py).
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { loadApp, ROOT } from "./load_app.mjs";

const app = loadApp();
const run = (code) => vm.runInContext(code, app);
const plain = (v) => JSON.parse(JSON.stringify(v));
const INDEX = readFileSync(join(ROOT, "index.html"), "utf8");

const OFFICIAL = [
  { id: "foi", title: "FOI reply", section: "news", date: "2026-10-27" },
  { id: "works", title: "Western Road closed", section: "travel", date: "2026-09-29" },
  { id: "fares", title: "Fares drop", section: "news", date: "2026-08-31" },
];
const COMMUNITY = [{ id: "seen", title: "Reported: 700 diverted", date: "2026-09-25" }];

test("each article goes to its own tab, and passenger reports to Travel Updates", () => {
  const pick = (section) => plain(run(`updatesForSection(${JSON.stringify(OFFICIAL)}, ${JSON.stringify(COMMUNITY)}, "${section}")`));
  assert.deepEqual(pick("travel").map(x => [x.u.id, x.community]), [["works", false], ["seen", true]]);
  assert.deepEqual(pick("news").map(x => [x.u.id, x.community]), [["foi", false], ["fares", false]]);
});

test("only disruptions in force, or starting within a fortnight, are listed", () => {
  const now = "2026-11-01T12:00:00Z";
  const list = [
    { id: "over", summary: "Ended", starts: "2026-09-01T07:00:00Z", ends: "2026-10-01T00:00:00Z" },
    { id: "now", summary: "In force", starts: "2026-09-28T07:00:00Z", ends: "2026-11-06T23:59:00Z" },
    { id: "soon", summary: "Next week", starts: "2026-11-08T07:00:00Z", ends: "2026-11-09T00:00:00Z" },
    { id: "later", summary: "Next year", starts: "2027-01-08T07:00:00Z" },
    { id: "open", summary: "No end given", starts: "2026-10-01T07:00:00Z" },
  ];
  const ids = plain(run(`currentDisruptions(${JSON.stringify(list)}, new Date("${now}"))`)).map(d => d.id);
  assert.deepEqual(ids.sort(), ["now", "open", "soon"]);
});

test("a disruption card names who published it, links their notice and says when", () => {
  const html = run(`travelDisruptionsHtml([{ id: "w", summary: "Western Road closed", publisher: "Brighton & Hove Buses",
    source_url: "https://www.buses.co.uk/service-updates", checked_on: "2026-09-28",
    starts: "2026-09-28T07:00:00+01:00", ends: "2026-11-06T23:59:00+00:00",
    lines: [{ operator: "BHBC", line: "1" }] }], new Date("2026-11-01T12:00:00Z"))`);
  assert.match(html, /Western Road closed/);
  assert.match(html, /Brighton &amp; Hove Buses/);
  assert.match(html, /href="https:\/\/www\.buses\.co\.uk\/service-updates"/);
  assert.match(html, /Until Fri 6 Nov/);
});

test("no current disruptions says so, rather than showing an empty box", () => {
  assert.match(run(`travelDisruptionsHtml([], new Date())`), /No diversions or disruptions/);
});

test("the tabs are named as the section is", () => {
  const tabs = INDEX.slice(INDEX.indexOf('aria-label="News and notes panels"'), INDEX.indexOf("</div>", INDEX.indexOf('aria-label="News and notes panels"')));
  const names = [...tabs.matchAll(/<span class="panel-tab-label">([^<]+)<\/span>/g)].map(m => m[1]);
  assert.deepEqual(names, ["Travel Updates", "News and Analysis"]);
  assert.match(tabs, /data-updates-tab="travel"[^>]*aria-selected="true"/, "Travel Updates should open first");
});
