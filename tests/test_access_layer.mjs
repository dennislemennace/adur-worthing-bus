/** Accessibility, phase B (6 November 2026): the Stop access layer colours
 *  each stop by whether it is step-free; a reader who needs step-free access
 *  sees it tagged on journeys; a station's board says its category; the Bus
 *  tab says what the law requires; the health page says how much is known. */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { loadApp, ROOT } from "./load_app.mjs";

const app = loadApp();
const state = vm.runInContext("state", app);
const STATIONS = JSON.parse(readFileSync(join(ROOT, "data/station_access.json"), "utf8"));
const ACCESS = JSON.parse(readFileSync(join(ROOT, "data/operator_access.json"), "utf8"));

function fakeMarker() {
  const classes = new Set(["stop-marker-icon"]);
  const el = { classList: { add: (...c) => c.forEach(x => classes.add(x)), remove: (...c) => c.forEach(x => classes.delete(x)) } };
  return { classes, marker: { getElement: () => el } };
}

test("each stop dot is coloured by its recorded step-free access, only while the layer is on", () => {
  state.stopOsm = { Y: { wheelchair: "yes" }, P: { wheelchair: "limited" }, N: { wheelchair: "no" }, U: { shelter: "yes" } };
  state.stopData = { Y: {}, P: {}, N: {}, U: {}, X: {} };
  const seen = {};
  for (const atco of ["Y", "P", "N", "U", "X"]) {
    const { classes, marker } = fakeMarker();
    state.stopAccessLayer = true;
    app.applyStopAccessClass(atco, marker);
    seen[atco] = [...classes].filter(c => c.startsWith("stop-access-")).join();
    state.stopAccessLayer = false;
    app.applyStopAccessClass(atco, marker);
    assert.equal([...classes].filter(c => c.startsWith("stop-access-")).length, 0, "the layer stayed on the dot");
  }
  assert.deepEqual(seen, { Y: "stop-access-yes", P: "stop-access-part", N: "stop-access-no",
                           U: "stop-access-unknown", X: "stop-access-unknown" });
  assert.deepEqual(JSON.parse(JSON.stringify(app.stopAccessCounts())), { yes: 1, part: 1, no: 1, unknown: 2, total: 5 });
});

test("a reader who needs step-free access sees each end of a journey tagged; nobody else does", () => {
  state.stopOsm = { A: { wheelchair: "yes" } };
  state.stopData = { A: { name: "High Street" }, B: { name: "Marine Parade" } };
  state.a11y = app.normaliseA11y({});
  assert.equal(app.journeyAccessHtml("A", "B"), "");
  state.a11y = app.normaliseA11y({ stepFree: true });
  const html = app.journeyAccessHtml("A", "B");
  assert.match(html, /High Street.*Step-free.*Marine Parade.*Step-free not recorded/s);
  const leg = app.planLegHtml({ mode: "BUS", route: "700", agency: "Stagecoach", from_name: "High Street",
    to_name: "Marine Parade", from_stop_id: "A", to_stop_id: "B", departure_time: 0, arrival_time: 600, duration_seconds: 600 });
  assert.match(leg, /access-chip--yes/);
  assert.match(leg, /access-chip--unknown/);
  state.a11y = app.normaliseA11y({});
});

test("a station's board gives its National Rail category in words, with the page", () => {
  state.stationAccess = STATIONS;
  const brighton = app.stationAccessHtml("BTN");
  assert.match(brighton, /Step-free/);
  assert.match(brighton, /category A/);
  assert.match(brighton, /nationalrail\.co\.uk\/stations\/brighton\//);
  assert.match(app.stationAccessHtml("AGT"), /Not step-free.*no step-free access/s);
  assert.equal(app.stationAccessHtml("XXX"), "");
});

test("the Bus tab says what the law requires of a local bus, as the law and not a check of the bus", () => {
  state.operatorAccess = { ...ACCESS.operators, _buses: ACCESS.buses };
  const html = app.localBusAccessHtml();
  assert.match(html, /1 January 2017/);
  assert.match(html, /1 October 2026/);
  assert.match(html, /not a check of this bus/);
  assert.match(html, /gov\.uk/);
  state.operatorAccess = null;
});

test("the health page counts how much of each stop's access is recorded", () => {
  const context = vm.createContext({ console, Date, Math, Intl, Number, String, JSON, Object, Array, isNaN });
  vm.runInContext(readFileSync(join(ROOT, "health.js"), "utf8").replace(/^if \(typeof document[^\n]*$/m, ""), context);
  const html = vm.runInContext("accessCoverageHtml", context)(
    { osm_base: "2026-11-06T00:00:00Z", stops: { A: { wheelchair: "yes", shelter: "yes" }, B: { shelter: "no" } } },
    { stops: [{ atco_code: "A" }, { atco_code: "B" }, { atco_code: "C" }, { atco_code: "D" }] });
  assert.match(html, /4 stops on this site/);
  // The sentence and the table count the same thing (7 November 2026): the
  // table's row is stops that say either way, the sentence how many say yes.
  assert.match(html, /Of the 1 that say\s+whether they are step-free, 1 are/);
  assert.match(html, /Whether it is step-free<\/th><td>1<\/td><td>25%/);
  assert.match(html, /Shelter<\/th><td>2<\/td><td>50%/);
});
