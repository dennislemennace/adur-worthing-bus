import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import fs from "node:fs";
import { loadApp } from "./load_app.mjs";

test("an alternative uses its own operator and stop path for the fare", () => {
  const app = loadApp(), zones = JSON.parse(fs.readFileSync("data/ticket_zones.json"));
  const stops = [{ lat: 50.823, lon: -.16 }, { lat: 50.823, lon: -.15 }];
  const route = app.journeyRoutes({ itineraries: [{ total_minutes: 20, legs: [
    { service: "2", operator: "BHBC", depart: "12:00", stops },
    { service: "25", operator: "BHBC", depart: "12:12", stops },
  ] }] })[0];
  const ctx = { allStandardZones: zones.zones.filter(app.isStandardFareZone),
    endpointOperators: stops.map(() => ["SCSO"]), operatorsKnown: true,
    usable: [{lat: 50.9, lon: -.4}, {lat: 50.9, lon: -.3}],
    byId: Object.fromEntries(zones.zones.map(z => [z.id, z])), meta: zones.fares_meta };
  assert.equal(app.costRoute(route, ctx).total, 630);
});

test("the selected origin's observed departure is the displayed departure", () => {
  const app = loadApp();
  const doc = { journeys: [{ day: "2026-09-17", start: "08:00", calls: [
    [0, 28800, 28800, 0, 0], [1, 30120, 30000, 0, 1], [2, 31020, 30600, 0, 2],
  ] }] };
  const row = app.journeyTimesBetween(doc, 1, 2)[0];
  assert.equal(row.start, "08:22");
  assert.equal(row.scheduledDepartSecs, 30000);
  assert.equal(row.arrivalLatenessSecs, 420);
});

test("route-call sequence overrides accidentally increasing midnight clock values", () => {
  const app = loadApp();
  const doc = { journeys: [{ day: "2026-09-17", calls: [
    [0, 500, 400, 0, 106], [1, 84000, 83900, 0, 0],
  ] }] };
  assert.equal(app.journeyTimesBetween(doc, 0, 1).length, 0);
});

test("failed pair lookup provides retry and does not reject or remain loading", async () => {
  const app = loadApp();
  const host = { innerHTML: "", querySelector: () => null };
  app.document.getElementById = () => host;
  vm.runInContext('state.viewMode="journeytimes"; jtEntry.a="A"; jtEntry.b="B";', app);
  app.loadJourneyTimesIndex = async () => { throw new Error("offline fixture"); };
  await app.journeyTimesResolvePair();
  assert.match(host.innerHTML, /retry/i);
  assert.doesNotMatch(host.innerHTML, /Looking for buses/);
});

test("immutable document paths are preserved and unexpected paths rejected", async () => {
  const app = loadApp(), file = `builds/${"b".repeat(64)}/700-SCSO.json`, asked = [];
  app.fetch = async url => { asked.push(url); return {ok: true, json: async () => ({build_id: "b".repeat(64)})}; };
  await app.loadJourneyTimes(file);
  assert.ok(asked[0].endsWith(`/${file}`));
  await assert.rejects(app.loadJourneyTimes("../private.json"));
  assert.equal(asked.length, 1);
});

test("evidence filters and midnight time windows keep the same exported cohort", () => {
  const app = loadApp();
  const rows = [
    {day: "2026-09-21", start: "23:30", estimated: false, match: "declared", qualityFlags: []},
    {day: "2026-09-21", start: "00:30", estimated: true, match: "declared", qualityFlags: []},
    {day: "2026-09-21", start: "08:30", estimated: false, match: "inferred", qualityFlags: []},
  ];
  const kept = app.journeyTimesFilter(rows, {evidence: "measured", identity: "declared", quality: "clear", start: "22:00", end: "02:00"});
  assert.equal(kept.length, 1);
  assert.equal(kept[0].start, "23:30");
  const csv = app.journeyTimesCsv(kept, {build_id: "build", service: "700", operator: "SCSO"});
  assert.match(csv, /build_id/);
  assert.match(csv, /23:30/);
  assert.doesNotMatch(csv, /00:30/);
});

test("chart supports a legible narrow viewport and selectable points", () => {
  const app = loadApp();
  const rows = [{day: "2026-09-21", start: "08:00", departSecs: 28800,
    observedSecs: 900, scheduledSecs: 600, promised: true}];
  const svg = app.journeyTimesChart(rows, app.journeyTimesSummary(rows), "delay", 340);
  assert.match(svg, /viewBox="0 0 340 300"/);
  assert.match(svg, /data-jt-point="0"/);
  assert.match(svg, /role="button"/);
});
