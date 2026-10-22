/** The guided tour (tour.js): its numbers come from the evidence files and
 *  never from typing, every placeholder in data/tour.json resolves, it is
 *  offered only once approved (or with ?preview=1), and what it renders is
 *  escaped and points at real actions. */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { loadApp, ROOT } from "./load_app.mjs";

const TOUR = JSON.parse(readFileSync(join(ROOT, "data/tour.json"), "utf8"));
const EVIDENCE = JSON.parse(readFileSync(join(ROOT, "data/boundary_evidence.json"), "utf8"));
const INDEX = { days: ["2026-09-16", "2026-09-17", "2026-10-17", "2026-10-20"] };

function loadTour() {
  const app = loadApp();
  vm.runInContext(readFileSync(join(ROOT, "tour-figures.js"), "utf8"), app);
  vm.runInContext(readFileSync(join(ROOT, "tour.js"), "utf8").replace(/^tourInitMenu\(\);$/m, ""), app);
  return app;
}

test("the figures come from the evidence file, not from the tour's text", () => {
  const app = loadTour();
  const v = app.tourFigureValues(EVIDENCE, INDEX);
  const day = EVIDENCE.days.monday;
  assert.equal(v.band_west_dps, String(day.west.departures_per_stop));
  assert.equal(v.band_east_dps, String(day.east.departures_per_stop));
  assert.equal(v.band_share, String(Math.round(day.west.departures_per_stop * 100 / day.east.departures_per_stop)));
  assert.equal(v.routes_west_sat, String(EVIDENCE.days.saturday.west.routes));
  assert.equal(v.late_east, Number(day.east.departures_after_2300).toLocaleString("en-GB"));
  assert.equal(v.lancing_dps, String(EVIDENCE.places.days.monday.west.departures_per_stop));
  assert.equal(v.recorded_days, "4");
  assert.equal(v.recorded_from, "16 September");
  assert.equal(v.recorded_to, "20 October");
});

test("the identification share is worked out from the published counts", () => {
  const app = loadTour();
  const ident = { operators: {
    BHBC: { tracked: 100, by: { journey_id: 90, start: 0, position: 10 } },
    SCSO: { tracked: 100, by: { journey_id: 50, start: 40, position: 10 } } } };
  const v = app.tourFigureValues(EVIDENCE, INDEX, ident);
  assert.equal(v.identified_share, "90", "journey ID and start-time matches both count as named by the feed");
  assert.equal(v.identified_tracked, "200");
  assert.equal(app.tourFigureValues(EVIDENCE, INDEX, null).identified_share, "unavailable");
});

test("days the recorder missed are named, runs of them as one span", () => {
  const app = loadTour();
  assert.equal(app.tourFigureValues(EVIDENCE, INDEX).recorded_gaps, "18 September to 16 October, 18 to 19 October");
  const whole = { days: ["2026-10-01", "2026-10-02", "2026-10-03"] };
  assert.equal(app.tourFigureValues(EVIDENCE, whole).recorded_gaps, "none so far");
  assert.equal(app.tourFigureValues(EVIDENCE, { days: ["2026-10-16", "2026-10-18"] }).recorded_gaps, "17 October");
});

test("every placeholder in data/tour.json is one the tour can fill", () => {
  const app = loadTour();
  const values = app.tourFigureValues(EVIDENCE, INDEX, { operators: {} });
  // The words a reader can see; `_comment` keys are notes for maintainers.
  const strings = JSON.stringify(TOUR, (k, v) => (k.startsWith("_") ? undefined : v));
  const used = [...strings.matchAll(/\{([a-z_]+)\}/g)].map(m => m[1]);
  assert.ok(used.length > 5, "the tour quotes no figures at all");
  const unknown = used.filter(k => !(k in values));
  assert.deepEqual(unknown, [], `placeholders with no value: ${unknown.join(", ")}`);
  assert.equal(app.tourFill("{band_share}% and {nope}", values), `${values.band_share}% and {nope}`,
    "an unknown placeholder is left visible rather than silently blanked");
});

test("offered only once approved, or with ?preview=1", () => {
  const app = loadTour();
  assert.equal(app.tourOffered({ preview_only: true }, false), false, "a draft tour offered to everyone");
  assert.equal(app.tourOffered({ preview_only: true }, true), true);
  assert.equal(app.tourOffered({ preview_only: false }, false), true);
  assert.equal(app.tourOffered(null, true), false);
});

test("the tour's page is a way in, not the tour: a start, the slides, and a contents list", () => {
  const app = loadTour();
  const doc = JSON.parse(JSON.stringify(TOUR));
  doc.steps[0].title = "<img src=x onerror=alert(1)>";
  const html = app.tourPanelHtml(doc, app.tourFigureValues(EVIDENCE, INDEX));
  assert.ok(html.indexOf(app.escapeHtml(TOUR.opening.headline)) >= 0, "headline missing");
  for (const id of ["present", "explore", "pdf"]) assert.match(html, new RegExp(`data-tour-mode="${id}"`), id);
  assert.match(html, /data-tour-mode="explore"[^>]*data-tour-start="0"|data-tour-start="0"[^>]*data-tour-mode="explore"/,
    "the main button does not start the tour at its first step");
  TOUR.steps.forEach((s, i) => {
    if (i) assert.ok(html.includes(app.escapeHtml(s.title)), s.id);
    assert.match(html, new RegExp(`data-tour-start="${i}"`), `no way straight to ${s.id}`);
  });
  assert.doesNotMatch(html, /<img src=x/, "a title was not escaped");
  for (const p of TOUR.steps.flatMap(s => s.points)) {
    assert.ok(!html.includes(app.escapeHtml(app.tourFill(p, {}))), "the page lists the tour's points again, as the old scrolling list did");
  }
  assert.doesNotMatch(html, /\{[a-z_]+\}/, "a placeholder reached the page unfilled");
});

test("the walk: each view a step opens is a stop, and a step that opens none is one stop", () => {
  const app = loadTour();
  const stops = JSON.parse(JSON.stringify(app.tourStops(TOUR, () => true)));
  const views = vm.runInContext("TOUR_VIEWS", app);
  const expected = [];
  TOUR.steps.forEach((s, i) => {
    const js = s.try.map((t, j) => (views.includes(t.action) ? j : -1)).filter(j => j >= 0);
    if (!js.length) expected.push({ step: i, tryIndex: null });
    for (const j of js) expected.push({ step: i, tryIndex: j });
  });
  assert.deepEqual(stops, expected);
  assert.ok(stops.length > TOUR.steps.length, "no step shows the site at all");
  assert.ok(!stops.some(x => x.tryIndex != null && !views.includes(TOUR.steps[x.step].try[x.tryIndex].action)),
    "a link or the evidence became a stop");
  const noDelay = JSON.parse(JSON.stringify(app.tourStops(TOUR, t => t.action !== "delay-map")));
  assert.equal(noDelay.length, stops.length - 1, "a view the site does not offer is still walked to");
  // An old link to a step that opens views lands on its first one.
  const first = stops.findIndex(x => x.step === 1);
  assert.equal(app.tourStopIndex(app.tourStops(TOUR, () => true), { step: 1, tryIndex: null }), first);
  assert.equal(app.tourStopIndex(app.tourStops(TOUR, () => true), { step: 99, tryIndex: null }), -1);
});

test("the card: where you are, what to try, the headline figure, and the way on", () => {
  const app = loadTour();
  const values = app.tourFigureValues(EVIDENCE, INDEX, { operators: {} });
  const stops = app.tourStops(TOUR, () => true);
  const n = TOUR.steps.length;
  const at = stops.findIndex(x => x.tryIndex != null && TOUR.steps[x.step].try[x.tryIndex].action === "board");
  const { step, tryIndex } = stops[at];
  const html = app.tourCoachHtml(TOUR, { step, tryIndex }, values, stops);
  const t = TOUR.steps[step].try[tryIndex];
  assert.match(html, new RegExp(`Step ${step + 1} of ${n}`));
  assert.match(html, new RegExp(`${at + 1} of ${stops.length}`), "the card does not count the stops");
  assert.ok(html.includes(app.escapeHtml(t.label)), "the stop's own name is missing");
  assert.ok(html.includes(app.escapeHtml(app.tourFill(t.hint, values))), "the card does not say what to try");
  assert.match(html, new RegExp(`data-tour-try="${step}:${tryIndex}"[^>]*aria-pressed="true"`), "the open view is not marked");
  assert.match(html, /data-tour-coach="back"/);
  assert.match(html, /data-tour-coach="next"/);
  assert.match(html, /data-tour-coach="end"/);
  assert.doesNotMatch(html, /\{[a-z_]+\}/, "a placeholder reached the card unfilled");

  // One figure: the figure and what it counts.
  const single = TOUR.steps.findIndex(s => s.headline && s.headline.value);
  const singleHtml = app.tourCoachHtml(TOUR, stops[stops.findIndex(x => x.step === single)], values, stops);
  for (const key of ["value", "label"]) {
    assert.ok(singleHtml.includes(app.escapeHtml(app.tourFill(TOUR.steps[single].headline[key], values))), `the headline's ${key} is missing`);
  }
  // A comparison: the claim, what was measured, and each figure with its own
  // label and a bar, so no number stands without saying what it is.
  const compared = TOUR.steps.findIndex(s => s.headline && s.headline.figures);
  const head = TOUR.steps[compared].headline;
  const cmp = app.tourCoachHtml(TOUR, stops[stops.findIndex(x => x.step === compared)], values, stops);
  assert.ok(cmp.includes(app.escapeHtml(app.tourFill(head.claim, values))), "the claim is missing");
  assert.ok(cmp.includes(app.escapeHtml(app.tourFill(head.measure, values))), "what was measured is missing");
  for (const f of head.figures) {
    const row = new RegExp(`tour-compare-label">${app.escapeHtml(app.tourFill(f.label, values))}</span>\\s*`
      + `<strong class="tour-compare-value">${app.escapeHtml(app.tourFill(f.value, values))}</strong>`);
    assert.match(cmp, row, `${f.label}: the figure is not next to its label`);
  }
  const bars = [...cmp.matchAll(/tour-compare-bar[^>]*><span data-pct="(\d+)"/g)].map(m => Number(m[1]));
  assert.equal(bars.length, head.figures.length, "a figure has no bar");
  assert.ok(bars.every(b => b > 0 && b <= 100), `bars ${bars}`);
  // The claim says what the lead would, so the lead is under More, not twice.
  const lead = app.escapeHtml(app.tourFill(TOUR.steps[compared].lead, values));
  assert.equal(cmp.split(lead).length - 1, 1, "the lead is on the card as well as the claim");
  assert.match(cmp, /tour-coach-lead--more/);

  const first = app.tourCoachHtml(TOUR, stops[0], values, stops);
  assert.match(first, /data-tour-coach="back"[^>]*disabled/, "the first stop offers a way back to nothing");
  const last = app.tourCoachHtml(TOUR, stops[stops.length - 1], values, stops);
  assert.match(last, /data-tour-coach="next"[^>]*>[^<]*Finish/, "the last stop does not say it finishes the tour");

  const evil = JSON.parse(JSON.stringify(TOUR));
  evil.steps[step].try[tryIndex].hint = "<img src=x onerror=alert(1)>";
  evil.steps[step].title = "<img src=x onerror=alert(1)>";
  evil.steps[step].sources = [{ label: "evil", url: "javascript:alert(1)", checked_on: "2026-10-20" }];
  const bad = app.tourCoachHtml(evil, { step, tryIndex }, values, app.tourStops(evil, () => true));
  assert.doesNotMatch(bad, /<img src=x/, "the card's text was not escaped");
  assert.doesNotMatch(bad, /href="javascript:/i, "an unsafe source link was rendered");
});

test("every Show me names an action the tour knows how to run", () => {
  const app = loadTour();
  const known = vm.runInContext("TOUR_ACTIONS", app);
  for (const s of TOUR.steps) for (const t of s.try) assert.ok(known.includes(t.action), `${s.id}: ${t.action}`);
});

test("a view opened from the tour keeps its place in the address", () => {
  const app = loadTour();
  const state = vm.runInContext("state", app);
  state.viewMode = "improvements";
  state.tourDemo = { step: 0, tryIndex: 1 };
  assert.equal(app.buildUrlHash(), "#view=i&tour=1-2");
  state.tourDemo = { step: 2, tryIndex: null };
  assert.equal(app.buildUrlHash(), "#view=i&tour=3", "a view reached some other way from inside the tour");
  state.viewMode = "tour";
  assert.equal(app.buildUrlHash(), "#view=tour", "the tour's own page names no step");
  state.viewMode = "improvements";
  state.tourDemo = null;
  assert.equal(app.buildUrlHash(), "#view=i");
});

test("the address names the step a view was opened from, and nothing else", () => {
  const app = loadTour();
  const read = (v, view) => JSON.stringify(app.tourDemoFromAddress(v, view));
  assert.equal(read("2-3", "live"), JSON.stringify({ step: 1, tryIndex: 2 }));
  assert.equal(read("4", "improvements"), JSON.stringify({ step: 3, tryIndex: null }));
  assert.equal(read("2", "tour"), "null", "the tour's own page is not a view opened from it");
  assert.equal(read(undefined, "live"), "null");
  assert.equal(read("0", "live"), "null");
  assert.equal(read("2-0", "live"), "null");
  assert.equal(read("x<script>", "live"), "null");
});

test("only the reader leaves the tour: a view reached from inside it stays inside it", () => {
  const app = loadTour();
  const state = vm.runInContext("state", app);
  vm.runInContext("tour.lastStep = 2", app);
  state.tourDemo = null;
  app.tourCarry("tour", "live", false);
  assert.equal(state.tourDemo && state.tourDemo.step, 2, "the evidence's live-bus button took the reader out of the tour");
  app.tourCarry("live", "tickets", false);
  assert.equal(state.tourDemo && state.tourDemo.step, 2, "a link inside the view took the reader out of the tour");
  app.tourCarry("tickets", "network", true);
  assert.equal(state.tourDemo, null, "choosing a section from the menu left the reader in the tour");
  app.tourCarry("live", "tickets", false);
  assert.equal(state.tourDemo, null, "a view outside the tour was pulled into it");
  state.tourDemo = { step: 1, tryIndex: 0 };
  app.tourCarry("live", "tour", false);
  assert.equal(state.tourDemo, null, "the tour's own page kept a view open");
});

test("every view the tour walks to says what to try there", () => {
  const views = ["board", "live-map", "route-view", "tickets", "planner", "journey-times", "delay-map"];
  for (const s of TOUR.steps) {
    for (const t of s.try) if (views.includes(t.action)) assert.ok(t.hint, `${s.id}: ${t.label} has no hint`);
  }
});
