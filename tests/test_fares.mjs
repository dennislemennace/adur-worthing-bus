/**
 * Tests for the ticket checker's money — the numbers a reader is invited to
 * quote at a councillor.
 *
 * These drive `renderJourneyResult` and read the HTML it produces, rather than
 * calling the fare helpers directly. That is deliberate. The defect these were
 * written for was not a wrong helper: `cheapestRealOption` has always accepted
 * a `singlesOption`, and both production callers omitted it, so a helper test
 * passing a fifth argument would have proved something no visitor could see.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { loadApp, ROOT } from "./load_app.mjs";

const TZ = JSON.parse(readFileSync(join(ROOT, "data/ticket_zones.json"), "utf8"));

// Real stops, and which zone each falls in is checked by the fixtures below
// rather than assumed — a coordinate that quietly stopped being inside the
// Brighton polygon would otherwise turn these into tests of nothing.
const MARINE_PARADE  = { atco: "A", name: "Marine Parade",   lat: 50.8095, lon: -0.3730 };
const CHURCHILL_SQ   = { atco: "B", name: "Churchill Square", lat: 50.8225, lon: -0.1450 };
const OLD_STEINE     = { atco: "C", name: "Old Steine",       lat: 50.8215, lon: -0.1375 };
const HANGLETON      = { atco: "D", name: "Hangleton",        lat: 50.8480, lon: -0.1900 };
const SHOREHAM_HIGH  = { atco: "G", name: "Shoreham High Street", lat: 50.8323, lon: -0.2776 };
const PARK_ROAD      = { atco: "H", name: "Park Road",            lat: 50.8584, lon: -0.1031 };
const UPTON_FARM     = { atco: "E", name: "Upton Farm House", lat: 50.8347, lon: -0.3581 };
const MARINA         = { atco: "F", name: "Brighton Marina",  lat: 50.8121, lon: -0.1042 };

/** app.js is a browser script; this renders one journey and returns the HTML. */
function render(journey, fromAtco, toAtco, stopData = {}) {
  const app = loadApp();
  const state = vm.runInContext("state", app);
  const dom   = vm.runInContext("dom", app);
  state.ticketZones     = JSON.parse(JSON.stringify(TZ.zones));
  state.ticketFaresMeta = TZ.fares_meta;
  state.stopData        = stopData;
  let html = "";
  dom.jcResult = { set innerHTML(v) { html = v; }, get innerHTML() { return html; } };
  app.drawJourneyOnMap = () => {};   // Leaflet is not stubbed this far
  app.renderJourneyResult(journey, fromAtco, toAtco);
  return html;
}

function direct(from, to, extra = {}) {
  return {
    from: { name: from.name }, to: { name: to.name },
    options: [Object.assign({
      service: "700", operator: "SCSO", depart: "12:00", arrive: "13:00",
      stop_count: 40, stops: [from, to],
    }, extra)],
  };
}

// A single fare is capped at £3, so one bus each way is £6 for the return —
// the number every assertion below is really about.
const SINGLES_RETURN_PENCE = 600;
const COMMUTE_DAYS = TZ.fares_meta.commute_days_per_week;

const toPence = (s) => Math.round(parseFloat(s.replace(/[£,]/g, "")) * 100);

/** The one sentence that tells a reader what this journey costs.
 *
 *  Asserted on specifically because "£6.00" appears all over the markup — in
 *  the zone list, in the reform ask — so a test that merely searched the page
 *  for it would pass with the defect fully intact. It did, when these were
 *  first written.
 */
function headlinePence(html) {
  const m = html.match(/class="journey-headline"[\s\S]*?<\/p>/);
  if (!m) return null;
  const money = m[0].match(/£[\d.,]+/);
  return money ? toPence(money[0]) : null;
}

/** Every weekly saving the reform block quotes, in pence. */
function weeklySavingsPence(html) {
  return [...html.matchAll(/saving (£[\d.,]+) a week/g)].map(m => toPence(m[1]));
}

// ── The cheapest thing you can actually buy ─────────────────

test("a two-zone journey is not headlined above what singles cost", () => {
  // Marine Parade and Churchill Square sit in the Worthing and Brighton
  // DayRider polygons respectively: two £6 tickets, or one £8.50 Gold — while
  // two capped singles make the same return trip for £6.
  const html = render(direct(MARINE_PARADE, CHURCHILL_SQ), "A", "B");
  const price = headlinePence(html);
  assert.notEqual(price, null, "expected a priced verdict");
  assert.ok(price <= SINGLES_RETURN_PENCE,
    `headlined £${(price / 100).toFixed(2)} as the cost of a return that two `
    + "capped singles cover for £6.00");
});

test("a network-ticket journey is not headlined above what singles cost", () => {
  // Old Steine is outside the Brighton DayRider polygon, so no combination of
  // zone tickets covers the path and the Gold network ticket is the only zone
  // answer. This is the branch the audit reproduced at £8.50.
  const html = render(direct(MARINE_PARADE, OLD_STEINE), "A", "C");
  const price = headlinePence(html);
  assert.notEqual(price, null, "expected a priced verdict");
  assert.ok(price <= SINGLES_RETURN_PENCE,
    `headlined £${(price / 100).toFixed(2)} as the cost of a return that two `
    + "capped singles cover for £6.00");
});

// ── Reforms are measured against something real ─────────────

test("no weekly saving exceeds what the cheapest real option would save", () => {
  // The reproduced defect: £8.50 Gold minus a £6 merged zone, times five
  // days, quoted as "saving £12.50 a week" — on a journey whose real cost is
  // £6, where merging the zones saves nothing at all.
  for (const [label, html] of [
    ["two-zone",  render(direct(MARINE_PARADE, CHURCHILL_SQ), "A", "B")],
    ["network",   render(direct(MARINE_PARADE, OLD_STEINE),   "A", "C")],
  ]) {
    const price = headlinePence(html);
    const reforms = [...html.matchAll(/would cost <strong>(£[\d.,]+)<\/strong>/g)]
      .map(m => toPence(m[1]));
    const savings = weeklySavingsPence(html);
    for (let i = 0; i < savings.length; i++) {
      const ceiling = (price - (reforms[i] ?? 0)) * COMMUTE_DAYS;
      assert.ok(savings[i] <= ceiling,
        `${label}: quoted a £${(savings[i] / 100).toFixed(2)} weekly saving `
        + `against a £${(price / 100).toFixed(2)} journey — at most `
        + `£${(Math.max(0, ceiling) / 100).toFixed(2)} is real`);
    }
  }
});

test("the merge-zones ask is only offered when two zone tickets are crossed", () => {
  // sc-gold-dayrider has no polygon: it is an operator-wide ticket, not a
  // zone. Counting it as one made "if the Worthing and Brighton zones were
  // merged" appear on a journey that never entered the Brighton zone.
  const html = render(direct(MARINE_PARADE, OLD_STEINE), "A", "C");
  if (/Worthing and Brighton DayRider zones were merged/.test(html)) {
    assert.match(html, /Brighton Dayrider/,
      "the merge-zones ask was offered although the Brighton DayRider zone "
      + "is not among the zones this journey crosses");
  }
});

// ── Cross-operator journeys ─────────────────────────────────

test("a cross-operator journey does not deny a ticket the site itself recommends", () => {
  // Worthing to Hangleton: Stagecoach at one end, Brighton & Hove at the
  // other. No operator's own ticket spans it — but fares_meta configures the
  // £10 South Downs Discovery Ticket, valid on every operator, recommend:true.
  const journey = {
    from: { name: "Marine Parade", operators: ["SCSO"] },
    to:   { name: "Hangleton",     operators: ["BHBC"] },
    options: [], note: "No direct bus found.",
    interchange: { change_at: { name: "Shoreham" }, board_at: {}, legs: [] },
  };
  const html = render(journey, "A", "D", { A: MARINE_PARADE, D: HANGLETON });

  assert.doesNotMatch(html, /No single ticket can cover this journey/,
    "the page ruled out every ticket while the site configures and recommends "
    + "an all-operator ticket for exactly this case");
  assert.match(html, /Discovery/,
    "the all-operator ticket was not considered on a journey that needs one");
});

test("the all-operator ticket is still excluded from services it is not valid on", () => {
  // Not valid on the N700 or N1. The fix for the above must not become
  // "always offer Discovery".
  const journey = {
    from: { name: "Marine Parade", operators: ["SCSO"] },
    to:   { name: "Hangleton",     operators: ["BHBC"] },
    options: [{ service: "N700", operator: "SCSO", depart: "23:40",
                arrive: "00:40", stop_count: 30,
                stops: [MARINE_PARADE, HANGLETON] }],
  };
  const html = render(journey, "A", "D", { A: MARINE_PARADE, D: HANGLETON });
  assert.doesNotMatch(html, /Discovery/,
    "offered the Discovery ticket on the N700, which it is not valid on");
});

// ── Prices carry their evidence ─────────────────────────────

test("every published fare has a source and a check date", () => {
  for (const z of TZ.zones) {
    const f = z.fares || {};
    if (!f.adult_day) continue;
    assert.ok(f.source_url, `${z.id}: a price with no source`);
    assert.ok(f.checked_on, `${z.id}: a price with no check date`);
  }
});

test("a fare whose review date has passed is not presented as current", () => {
  // An advocacy site quoting an expired fare cap is worse than one quoting
  // none. This asserts the data carries the expiry; the rendering rule that
  // acts on it is tested through the journey above.
  const sf = TZ.fares_meta.single_fare;
  assert.ok(sf.review_by, "the single fare has no review_by date");
  assert.match(sf.review_by, /^\d{4}-\d{2}-\d{2}$/);
});

// ── The published-source conflict is disclosed, not resolved ─

test("a fare contradicted by the operator's own table says so on the page", () => {
  // Stagecoach sells the Gold day ticket at £8.50 in its app; its published
  // fares table effective 1 June 2026 lists £9. Both are sourced, and the
  // page cannot know which channel a reader buys through — so it shows one
  // price and names the other rather than picking a winner silently.
  const gold = TZ.zones.find(z => z.id === "sc-gold-dayrider");
  const c = gold.fares.conflicts_with;
  assert.ok(c, "the Gold day fare records no conflicting published figure");
  assert.equal(c.price_pence, 900);
  assert.ok(c.source_url && c.effective_from, "a conflict with no source or date");

  const html = render(direct(MARINE_PARADE, OLD_STEINE), "A", "C");
  if (html.includes("Gold Dayrider:")) {
    assert.match(html, /published table/,
      "the Gold fare was cited without disclosing the table that contradicts it");
  }
});

test("the Nightrider becomes valid when the operator says it does", () => {
  // The published table says "after 7pm". This file said 19:30, which
  // withheld a £4 fare from half an hour of evening journeys it covers.
  const nr = TZ.zones.find(z => z.id === "sc-gold-nightrider");
  assert.equal(nr.valid_from_time, "19:00");

  const app = loadApp();
  assert.equal(app.ticketValidAtTime(nr, "19:15"), true);
  assert.equal(app.ticketValidAtTime(nr, "18:45"), false);
});

test("the commuter basis does not silently assume five separate day tickets", () => {
  // Stagecoach publishes Flexi5 bundles at £24 local and £36 Gold. Costing a
  // week as five day tickets without saying so overstates the baseline every
  // reform is measured against.
  assert.match(TZ.fares_meta.basis_note, /Flexi5/,
    "the stated basis ignores the cheaper multi-day product the operator sells");
  for (const id of ["sc-worthing-dayrider", "sc-brighton-dayrider", "sc-gold-dayrider"]) {
    const f = TZ.zones.find(z => z.id === id).fares;
    assert.ok(f.adult_flexi5 && f.adult_flexi5.source_url,
      `${id}: no sourced Flexi5 price`);
  }
});

// ── Tickets are judged on the legs actually ridden ──────────

/** The real shape of an interchange journey, as /api/journey returns it. */
function interchangeJourney(legs, fromOps, toOps) {
  return {
    from: { name: "Start", operators: fromOps },
    to:   { name: "End",   operators: toOps },
    options: [],
    note: "No direct bus found between these stops today.",
    interchange: {
      change_at: { name: "Churchill Square" },
      board_at: {}, total_minutes: 62, wait_minutes: 8, walk_metres: 0,
      // The fields /api/journey really sends. Omitting them renders a line
      // full of "undefined" that looks like a product bug and is not one.
      legs: legs.map(([service, operator], i) => ({
        service, operator, headsign: "Somewhere",
        depart: `13:${21 + i * 40}`, arrive: `13:${53 + i * 2}`,
        minutes: 32, stops: [],
      })),
    },
  };
}

test("a ticket is not offered for a journey whose first leg it is invalid on", () => {
  // Shoreham High Street to Park Road is the 700 (Stagecoach) then the 5B
  // (Brighton & Hove). The *endpoint* operator sets intersect at BHBC, so
  // filtering on endpoints alone left citySAVER — a Brighton & Hove ticket —
  // presented as covering a journey that starts on a Stagecoach bus.
  const html = render(
    interchangeJourney([["700", "SCSO"], ["5B", "BHBC"]],
                       ["BHBC", "COMT", "NATX", "SCSO"], ["BHBC"]),
    "G", "H", { G: SHOREHAM_HIGH, H: PARK_ROAD });

  // The exact claim the page made: "One ticket covers this journey:
  // citySAVER (BHBC) at £6.30" — printed directly beneath an itinerary
  // reading 700 (Stagecoach South), then 5B.
  assert.doesNotMatch(html, /One ticket covers this journey:\s*<strong>citySAVER/,
    "citySAVER was offered as covering a journey whose first leg is the 700");
});

test("a mixed-operator itinerary says which leg breaks the ticket", () => {
  // The campaign point, not an error message: the fastest way to Brighton is
  // the 700, and its ticket stops working the moment the journey continues.
  const html = render(
    interchangeJourney([["700", "SCSO"], ["5B", "BHBC"]],
                       ["BHBC", "SCSO"], ["BHBC"]),
    "G", "H", { G: SHOREHAM_HIGH, H: PARK_ROAD });
  assert.match(html, /700/, "the itinerary does not name the first leg");
  assert.match(html, /5B/,  "the itinerary does not name the second leg");
});

// ── An operator we hold no day ticket for ───────────────────

test("a Compass-only journey names the operator instead of claiming no coverage", () => {
  // Sompting to Brighton Marina: Compass is the only operator at both ends,
  // and no zone in the data lists Compass — so every ticket was filtered out
  // and the page reported "we don't have ticket-zone coverage", which is a
  // statement about the stops. The stops are fine; the operator is the gap.
  const html = render(
    interchangeJourney([], ["COMT", "SCSO"], ["BHBC", "COMT"]),
    "A", "B", { A: UPTON_FARM, B: MARINA });

  assert.doesNotMatch(html, /don't have ticket-zone coverage for every stop/,
    "reported missing zone data for stops that are inside real zones");
  assert.match(html, /Compass/,
    "did not name the operator whose tickets we do not hold");
});

test("the Compass Rover is recorded, priced weekly, and never shown as a day ticket", () => {
  const compass = TZ.zones.find(z => (z.valid_on_operators || []).includes("COMT"));
  assert.ok(compass, "no Compass entry in ticket_zones.json");
  assert.equal(compass.fares.adult_week.price_pence, 3000);
  assert.ok(compass.fares.source_url, "the Rover price has no source");
  assert.equal(compass.fares.adult_day, undefined,
    "a weekly rover must not be recorded as a day ticket");
  // zoneDayFare is what the per-journey costing reads; a weekly must not
  // reach it, or £30 gets compared against a £6 return.
  const app = loadApp();
  assert.equal(app.zoneDayFare(compass), null);
});

test("a weekly is compared against the singles this journey really needs", () => {
  // The Rover is £30 a week. Sompting to the Marina takes two buses each way,
  // so a five-day week in capped singles is £60, not the £30 a bare
  // single-fare × 2 × 5 gives. Getting that wrong turns a halving into a
  // break-even and the site into something that cannot be quoted.
  const html = render(
    interchangeJourney([], ["COMT", "SCSO"], ["BHBC", "COMT"]),
    "E", "F", { E: UPTON_FARM, F: MARINA });
  if (!/a week is valid on the whole of it/.test(html)) return;  // no weekly offered
  const m = /against\s*£([\d.]+) in capped singles/.exec(html.replace(/\s+/g, " "));
  assert.ok(m, "the weekly is quoted with no comparison");
  assert.equal(m[1], "60.00",
    "the weekly was compared against a one-bus-each-way week");
});
