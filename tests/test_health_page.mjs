/**
 * health.html's script: shares with their denominators, RTIG's words beside
 * every colour, and "too few" where a route has almost nothing to go on.
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
vm.runInContext(readFileSync(join(ROOT, "health.js"), "utf8"), context);
const fn = name => vm.runInContext(name, context);
const text = html => html.replace(/<[^>]+>/g, " ").replace(/&amp;/g, "&").replace(/\s+/g, " ");

const IDENT = {
  as_of: "2026-10-25T03:00:00+00:00", method: "Matched by journey ID, start or position.", caveats: ["Counts only."],
  operators: { BHBC: { scheduled: 100, tracked: 90, by: { journey_id: 80, start: 5, position: 5 } } },
  services: { "7|BHBC": { scheduled: 50, tracked: 45, by: { journey_id: 40, start: 0, position: 5 }, operator: "BHBC", service: "7" },
              "N7|BHBC": { scheduled: 4, tracked: 3, by: { journey_id: 3, start: 0, position: 0 }, operator: "BHBC", service: "N7" } },
};
const SERVED = {
  days: ["2026-10-20", "2026-10-24"], as_of: "2026-10-25T03:00:00+00:00", method: "Rebuilt minute by minute.",
  caveats: ["Departures, not arrivals."],
  accuracy: { all: { ok: 83, early: 8, late: 9 }, "operator|BHBC": { ok: 86, early: 6, late: 8 },
              "range|0-3 min": { ok: 7, early: 1, late: 2 }, "service|7|BHBC": { ok: 60, early: 30, late: 10 },
              "service|N7|BHBC": { ok: 9, early: 0, late: 0 } },
  coverage: { all: { calls: 200, with_estimate: 160, no_bus_seen: 20, seen_not_named: 5, named_no_estimate: 15 },
              "operator|BHBC": { calls: 200, with_estimate: 160 }, "service|7|BHBC": { calls: 100, with_estimate: 90 } },
  journeys: { "service|7|BHBC": { journeys: 40, with_estimate: 38 }, "service|N7|BHBC": { journeys: 3, with_estimate: 3 } },
  cleardown: { all: { calls: 100, listed_1_min_after: 10, listed_3_min_after: 4, listed_5_min_after: 2, left_before_timetable: 20 } },
};

test("RTIG's rating bands, with 70% read as satisfactory", () => {
  const r = fn("rtigShares");
  assert.equal(r({ ok: 9, early: 1 }).rating, "excellent");
  assert.equal(r({ ok: 7, late: 3 }).rating, "satisfactory");
  assert.equal(r({ ok: 69, late: 31 }).rating, "unsatisfactory");
  assert.equal(r({ unscored: 5 }), null, "predictions RTIG sets no window for were scored");
});

test("every share on the page gives what it was divided by", () => {
  const html = text(fn("healthHtml")(IDENT, SERVED, {}));
  assert.match(html, /83% of the live times the board showed were within RTIG's accuracy window: good on RTIG's scale/);
  assert.match(html, /90% of 100/, "journeys seen has no denominator");
  assert.match(html, /Of 200 timetabled departures in the area, 80% had a live time/);
  assert.match(html, /10% no bus was seen/);
  assert.match(html, /RTIGT041-1\.0/, "the measure is not named");
  assert.match(html, /recorded roadworks and diversions are counted apart/);
});

test("a route with too few journeys says so, never a share", () => {
  const rows = fn("routeRows")(IDENT, SERVED);
  const n7 = rows.find(r => r.svc === "N7");
  assert.ok(n7.tooFew && n7.acc === null, "three journeys were given a rating");
  assert.match(fn("routeTableHtml")(rows), /too few/);
  assert.equal(rows[0].svc, "7", "routes are not in the order people read them");
});

test("colour is never the only signal", () => {
  const html = fn("ratingHtml")(fn("rtigShares")({ ok: 5, early: 5 }));
  assert.match(html, /rating--unsatisfactory">unsatisfactory</);
});

test("text from the files is escaped", () => {
  const html = fn("healthHtml")({ ...IDENT, method: "<script>x</script>" }, SERVED, {});
  assert.doesNotMatch(html, /<script>/);
});

test("the comparison with TransportAPI can be switched off, and waits for enough departures", () => {
  const v = { versus: { "both|nextbuses|all": { ok: 500, early: 100 }, "both|ours|all": { ok: 550, early: 50 } } };
  assert.equal(fn("versusHtml")(v, false), "", "switched off, it still showed");
  assert.match(fn("versusHtml")(v), /Too few departures compared so far \(600/,
    "a few hundred departures were published as a comparison");
  const big = { versus: { "both|nextbuses|all": { ok: 1500, early: 300 }, "both|ours|all": { ok: 1600, early: 200 } } };
  assert.match(text(fn("versusHtml")(big, true)), /TransportAPI \(NextBuses\) 1,800 83% good/);
});
