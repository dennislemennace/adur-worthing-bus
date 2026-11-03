/**
 * Which picture a bus gets on the map.
 *
 * An icon is a strong claim about what the bus looks like, so a route livery
 * is keyed by operator and route together: route numbers repeat across
 * operators, and only Stagecoach's 17 wears the 17's colours.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

const app = loadApp();
const run = (code) => vm.runInContext(code, app);
const ROOT = new URL("../", import.meta.url);

test("Stagecoach's 17 wears its own livery", () => {
  assert.equal(app.iconForService("SCSO", "17"), "icons/SCSO-17.png");
  assert.equal(app.iconForService("SCSC", "17"), "icons/SCSO-17.png");
});

test("other Stagecoach routes keep the generic Stagecoach bus", () => {
  assert.equal(app.iconForService("SCSO", "700"), "icons/SCSO.png");
  assert.equal(app.iconForService("SCSO", "1"), "icons/SCSO.png");
});

test("another operator's 17 does not borrow Stagecoach's livery", () => {
  assert.notEqual(app.iconForService("BHBC", "17"), "icons/SCSO-17.png");
});

// ── Each bus in its own livery (asked for 2 November 2026; for everyone from 3 November) ──

const FLEET = {
  operators: {
    BHBC: { by: "fleet", vehicles: { "212": { i: "liveries/bh-teal-dd.png", l: "Brighton & Hove Buses", d: "dd" },
                                     "201": { l: "Brighton & Hove", d: "dd" } } },
    SCSO: { by: "fleet", vehicles: { "10941": { i: "liveries/sc-coastliner-dd.png", l: "Coastliner", d: "dd" } } },
    COMT: { by: "reg", vehicles: { "YW25DFC": { i: "liveries/compass-sd.png", l: "Compass Travel", d: "sd", f: "4201" } } },
  },
};
const bus = (operator_ref, vehicle_ref, service_ref = "1") => ({ operator_ref, vehicle_ref, service_ref });

test("a bus wears its own livery, found by fleet number or plate", () => {
  assert.equal(app.iconForVehicle(bus("BHBC", "212", "20"), "20", FLEET), "icons/liveries/bh-teal-dd.png");
  assert.equal(app.iconForVehicle(bus("SCSO", "SCSO-10941", "700"), "700", FLEET), "icons/liveries/sc-coastliner-dd.png");
  assert.equal(app.iconForVehicle(bus("SCSC", "SCSO-10941", "700"), "700", FLEET), "icons/liveries/sc-coastliner-dd.png");
  assert.equal(app.iconForVehicle(bus("COMT", "YW25_DFC", "37"), "37", FLEET), "icons/liveries/compass-sd.png");
});

test("liveries need no preview flag: everyone sees them", () => {
  app.location.search = "";
  assert.equal(app.iconForVehicle(bus("BHBC", "212", "7"), "7", FLEET), "icons/liveries/bh-teal-dd.png");
});

test("a bus with no livery icon, or not in the list, falls back to the route icon", () => {
  assert.equal(app.iconForVehicle(bus("BHBC", "201", "7"), "7", FLEET), app.iconForService("BHBC", "7"));
  assert.equal(app.iconForVehicle(bus("BHBC", "999", "1"), "1", FLEET), app.iconForService("BHBC", "1"));
  assert.equal(app.iconForVehicle(bus("BHBC", "212", "7"), "7", null), app.iconForService("BHBC", "7"));
});

test("every livery icon in data/fleet.json is a file that exists", () => {
  const fleet = JSON.parse(readFileSync(new URL("data/fleet.json", ROOT), "utf8"));
  const paths = new Set(Object.values(fleet.operators)
    .flatMap((o) => Object.values(o.vehicles)).map((v) => v.i).filter(Boolean));
  assert.ok(paths.size > 20, "the fleet list names hardly any liveries");
  const missing = [...paths].filter((p) => !existsSync(new URL(`icons/${p}`, ROOT)));
  assert.deepEqual(missing, [], "these markers would 404 on the map");
});

test("every icon the map can ask for is a file that exists", () => {
  const paths = new Set([
    ...Object.values(run("ROUTE_ICONS")),
    ...Object.values(run("OPERATOR_ICONS")),
  ]);
  const missing = [...paths].filter((p) => !existsSync(new URL(p, ROOT)));
  assert.deepEqual(missing, [], "these markers would 404 on the map");
});
