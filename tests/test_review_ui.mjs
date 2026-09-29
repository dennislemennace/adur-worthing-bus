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

// The delay map's files are published byte for byte, with their SHA-256 in
// the manifest and no build_id inside. Asking them for one refused every map
// from the day they moved under builds/ (found 28 Sep 2026: "Measurement
// build mismatch" on the live site, while the browser check, which filled
// the cache directly, stayed green).
async function artifactApp(bytes) {
  const { createHash, webcrypto } = await import("node:crypto");
  const app = loadApp();
  app.crypto = webcrypto;
  app.TextDecoder = TextDecoder;
  app.fetch = async () => ({ ok: true, arrayBuffer: async () => bytes.buffer.slice(0),
                             json: async () => JSON.parse(new TextDecoder().decode(bytes)) });
  return { app, sha: createHash("sha256").update(bytes).digest("hex") };
}

test("a delay-map artifact with no build_id loads when its bytes match the manifest", async () => {
  const bytes = new TextEncoder().encode(JSON.stringify({ schema_version: 1, services: [{ service: "700" }] }));
  const { app, sha } = await artifactApp(bytes);
  const file = `builds/${"c".repeat(64)}/hotspot-map-index.json`;
  const doc = await app.loadJourneyTimes(file, sha);
  assert.equal(doc.services[0].service, "700");
});

test("a delay-map artifact whose bytes differ from the manifest is refused", async () => {
  const bytes = new TextEncoder().encode(JSON.stringify({ schema_version: 1, services: [] }));
  const { app } = await artifactApp(bytes);
  await assert.rejects(app.loadJourneyTimes(`builds/${"c".repeat(64)}/hotspot-map-index.json`, "0".repeat(64)),
    /hash mismatch/);
});

test("a journey-time document still has to name its own build", async () => {
  const app = loadApp();
  app.fetch = async () => ({ ok: true, json: async () => ({ build_id: "a".repeat(64) }) });
  await assert.rejects(app.loadJourneyTimes(`builds/${"b".repeat(64)}/700-SCSO.json`), /build mismatch/);
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

// ── Photo credits ───────────────────────────────────────────

test("a Creative Commons photo credit links its source and licence", () => {
  const app = loadApp();
  const html = app.updateCreditHtml({
    credit: "88-93, Western Road, Brighton © Simon Carey",
    credit_url: "https://www.geograph.org.uk/photo/4884023",
    license: "CC BY-SA 2.0", license_url: "https://creativecommons.org/licenses/by-sa/2.0/" });
  assert.match(html, /href="https:\/\/www\.geograph\.org\.uk\/photo\/4884023"[^>]*>88-93, Western Road, Brighton © Simon Carey<\/a>/);
  assert.match(html, /href="https:\/\/creativecommons\.org\/licenses\/by-sa\/2\.0\/"[^>]*>CC BY-SA 2\.0<\/a>/);
  assert.equal(app.updateCreditHtml({ credit: "A <b>name</b>" }), "A &lt;b&gt;name&lt;/b&gt;",
    "a credit with no links is still escaped plain text");
});
