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
import { existsSync } from "node:fs";
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

test("every icon the map can ask for is a file that exists", () => {
  const paths = new Set([
    ...Object.values(run("ROUTE_ICONS")),
    ...Object.values(run("OPERATOR_ICONS")),
  ]);
  const missing = [...paths].filter((p) => !existsSync(new URL(p, ROOT)));
  assert.deepEqual(missing, [], "these markers would 404 on the map");
});
