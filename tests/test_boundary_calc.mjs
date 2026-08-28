/**
 * Tests for the ticket-boundary calculator's pure logic.
 *
 * app.js is a browser script with no module exports, so it's evaluated here in
 * a `vm` context with just enough DOM stubbed to get past the `dom` bag it
 * builds at load time. That lets the geometry and set-cover functions be
 * tested against the REAL data/ticket_zones.json polygons — which is the point,
 * since a point-in-polygon bug is invisible until someone's fare is wrong.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import vm from "node:vm";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const zonesData = JSON.parse(readFileSync(join(ROOT, "data/ticket_zones.json"), "utf8"));

// ── Load app.js under a stub DOM ────────────────────────────

function loadApp() {
  const noop = () => {};
  const stubEl = new Proxy({}, {
    get(_t, prop) {
      if (prop === "classList") return { add: noop, remove: noop, toggle: noop, contains: () => false };
      if (prop === "dataset") return {};
      if (prop === "style") return {};
      if (prop === "addEventListener") return noop;
      if (prop === "querySelector" || prop === "closest") return () => stubEl;
      if (prop === "querySelectorAll") return () => [];
      if (prop === "appendChild") return noop;
      if (prop === "setAttribute" || prop === "removeAttribute") return noop;
      if (prop === "focus" || prop === "reset") return noop;
      if (prop === "value" || prop === "textContent" || prop === "innerHTML") return "";
      return undefined;
    },
    set() { return true; },
  });

  const sandbox = {
    document: {
      getElementById: () => stubEl,
      createElement: () => stubEl,
      addEventListener: noop,
      querySelector: () => stubEl,
      querySelectorAll: () => [],
      head: stubEl,
      body: stubEl,
    },
    window: { addEventListener: noop },
    navigator: { userAgent: "node" },
    location: { hash: "", search: "" },
    localStorage: { getItem: () => null, setItem: noop, removeItem: noop },
    console,
    fetch: async () => ({ ok: false, status: 500, json: async () => ({}) }),
    setTimeout, clearTimeout, setInterval, clearInterval,
    requestAnimationFrame: (fn) => setTimeout(fn, 0),
    // Leaflet is only touched inside map functions we don't call.
    L: new Proxy({}, { get: () => () => stubEl }),
  };
  sandbox.globalThis = sandbox;

  const code = readFileSync(join(ROOT, "app.js"), "utf8");
  const ctx = vm.createContext(sandbox);
  vm.runInContext(code, ctx, { filename: "app.js" });
  return ctx;
}

const app = loadApp();

// `state` is declared with `const`, so it never lands on the context object —
// only `var`s and function declarations do. Reach it by evaluating its name.
const appState = vm.runInContext("state", app);

/**
 * Values built inside the vm come from another realm, so their Array/Object
 * prototypes differ and deepStrictEqual rejects them on identity alone.
 * Round-tripping through JSON brings them into this realm.
 */
const plain = (v) => JSON.parse(JSON.stringify(v));

// A square around Worthing town centre, [lat, lon], deliberately unclosed —
// the real polygons in ticket_zones.json are stored that way too.
const SQUARE = [
  [50.80, -0.40],
  [50.80, -0.36],
  [50.84, -0.36],
  [50.84, -0.40],
];

// ── pointInRing ─────────────────────────────────────────────

test("pointInRing finds a point inside a simple ring", () => {
  assert.equal(app.pointInRing(50.82, -0.38, SQUARE), true);
});

test("pointInRing rejects points outside", () => {
  assert.equal(app.pointInRing(50.90, -0.38, SQUARE), false);  // north
  assert.equal(app.pointInRing(50.82, -0.10, SQUARE), false);  // east
  assert.equal(app.pointInRing(50.70, -0.38, SQUARE), false);  // south
  assert.equal(app.pointInRing(50.82, -0.99, SQUARE), false);  // west
});

test("pointInRing treats the ring as implicitly closed", () => {
  // The closing edge runs from the last vertex back to the first. A point
  // that can only be enclosed by that edge proves it's being applied.
  assert.equal(app.pointInRing(50.82, -0.399, SQUARE), true);
});

test("pointInRing rejects degenerate rings", () => {
  assert.equal(app.pointInRing(50.82, -0.38, []), false);
  assert.equal(app.pointInRing(50.82, -0.38, [[50.8, -0.4], [50.8, -0.3]]), false);
  assert.equal(app.pointInRing(50.82, -0.38, null), false);
});

// ── Against the real zone polygons ──────────────────────────

const zones = zonesData.zones;
const byId = Object.fromEntries(zones.map(z => [z.id, z]));

test("real zone data carries a coverage_rule for every zone", () => {
  for (const z of zones) {
    assert.ok(z.coverage_rule, `${z.id} has no coverage_rule`);
  }
});

test("central Brighton falls inside the citySAVER zone", () => {
  // Old Steine, the main Brighton interchange.
  const covering = app.zonesForStop(
    { lat: 50.8225, lon: -0.1372 }, zones, byId, "BHBC");
  assert.ok(covering.includes("bh-citysaver"),
    `expected bh-citysaver, got ${JSON.stringify(covering)}`);
});

test("central Worthing falls inside the Worthing Dayrider zone", () => {
  const covering = app.zonesForStop(
    { lat: 50.8158, lon: -0.3743 }, zones, byId, "SCSO");
  assert.ok(covering.includes("sc-worthing-dayrider"),
    `expected sc-worthing-dayrider, got ${JSON.stringify(covering)}`);
});

test("Worthing is NOT inside the Brighton citySAVER zone", () => {
  // The whole premise of the feature: these are genuinely different zones.
  const covering = app.zonesForStop(
    { lat: 50.8158, lon: -0.3743 }, zones, byId, "SCSO");
  assert.ok(!covering.includes("bh-citysaver"));
});

test("a far-away point is in no polygon zone", () => {
  const covering = app.zonesForStop({ lat: 53.48, lon: -2.24 }, zones, byId, "");
  const polygonZones = covering.filter(id => byId[id].coverage_rule === "polygon");
  assert.deepEqual(plain(polygonZones), []);
});

test("operator_network zones cover any stop that operator serves", () => {
  // Manchester coordinates — no polygon can match, so only the operator rule
  // can produce a hit. This is what stops the Gold tickets covering nothing.
  const covering = app.zonesForStop({ lat: 53.48, lon: -2.24 }, zones, byId, "SCSO");
  assert.ok(covering.includes("sc-gold-dayrider"),
    "Gold Dayrider should cover Stagecoach stops via the operator rule");
  assert.ok(!covering.includes("bh-networksaver"),
    "a B&H ticket should not cover a Stagecoach leg");
});

test("operator_network zones don't apply when the operator is unknown", () => {
  const covering = app.zonesForStop({ lat: 53.48, lon: -2.24 }, zones, byId, "");
  assert.deepEqual(plain(covering), []);
});

// ── cheapestCover ───────────────────────────────────────────

function fakeZones(spec) {
  // spec: [id, pricePence|null]
  return spec.map(([id, pence]) => ({
    id, name: id, operator: "OP", coverage_rule: "polygon",
    fares: pence === null ? null
      : { adult_day: { price_pence: pence, label: id }, source_url: "x", checked_on: "2026-01-01" },
  }));
}

test("cheapestCover picks a single ticket when one covers everything", () => {
  const z = fakeZones([["a", 600], ["b", 500]]);
  const best = app.cheapestCover([["a"], ["a", "b"], ["a"]], z);
  assert.deepEqual(plain(best.zones), ["a"]);
  assert.equal(best.priced, true);
  assert.equal(best.total, 600);
});

test("cheapestCover needs two tickets when no single zone spans the path", () => {
  const z = fakeZones([["a", 600], ["b", 500]]);
  const best = app.cheapestCover([["a"], ["a"], ["b"]], z);
  assert.equal(best.zones.length, 2);
  assert.equal(best.total, 1100);
  assert.equal(best.priced, true);
});

test("cheapestCover prefers fewer tickets over a cheaper pair", () => {
  // One £9 ticket beats two totalling £3 — passengers buy tickets, not pence.
  const z = fakeZones([["big", 900], ["x", 150], ["y", 150]]);
  const best = app.cheapestCover([["big", "x"], ["big", "y"]], z);
  assert.deepEqual(plain(best.zones), ["big"]);
});

test("cheapestCover reports unpriced when a needed zone has no fare", () => {
  const z = fakeZones([["a", 600], ["b", null]]);
  const best = app.cheapestCover([["a"], ["b"]], z);
  assert.equal(best.zones.length, 2);
  assert.equal(best.priced, false, "must not claim a total it can't source");
});

test("cheapestCover returns null when the path can't be covered at all", () => {
  const z = fakeZones([["a", 600]]);
  assert.equal(app.cheapestCover([["a"], []], z), null);
});

// ── Cross-operator ticket validity ──────────────────────────

test("a Stagecoach ticket is not valid on a Metrobus or B&H journey", () => {
  const gold = byId["sc-gold-dayrider"];
  assert.equal(app.ticketValidOn(gold, "SCSO"), true);
  assert.equal(app.ticketValidOn(gold, "METR"), false);
  assert.equal(app.ticketValidOn(gold, "BHBC"), false);
});

test("Metrovoyager is valid on Brighton & Hove services", () => {
  assert.equal(app.ticketValidOn(byId["mb-metrovoyager"], "BHBC"), true);
});

test("networkSAVER is valid on Metrobus services", () => {
  assert.equal(app.ticketValidOn(byId["bh-networksaver"], "METR"), true);
});

test("validity isn't filtered when the operator is unknown", () => {
  assert.equal(app.ticketValidOn(byId["sc-gold-dayrider"], ""), true);
});

test("a zone with no valid_on_operators falls back to its own operator", () => {
  const legacy = { operator: "SCSO" };
  assert.equal(app.ticketValidOn(legacy, "SCSO"), true);
  assert.equal(app.ticketValidOn(legacy, "BHBC"), false);
});

// ── Cheapest real option + saving claim ─────────────────────

test("cheapestRealOption prefers a cheaper network ticket over zone tickets", () => {
  // The Worthing-to-Brighton shape: two £6 zone tickets vs one £9 Gold.
  const zonal = { zones: ["a", "b"], total: 1200, priced: true };
  const net = { zone: { name: "Gold DayRider" }, pence: 900 };
  const best = app.cheapestRealOption(zonal, net);
  assert.equal(best.kind, "network");
  assert.equal(best.total, 900);
});

test("cheapestRealOption keeps zone tickets when they're cheaper", () => {
  const zonal = { zones: ["a", "b"], total: 1230, priced: true };
  const net = { zone: { name: "Expensive" }, pence: 1500 };
  assert.equal(app.cheapestRealOption(zonal, net).kind, "zonal");
});

test("cheapestRealOption ignores unpriced options", () => {
  const zonal = { zones: ["a"], total: 0, priced: false };
  const net = { zone: { name: "Gold" }, pence: 900 };
  assert.equal(app.cheapestRealOption(zonal, net).kind, "network");
  assert.equal(app.cheapestRealOption(zonal, { zone: {}, pence: null }), null);
});

// ── Time-restricted tickets ─────────────────────────────────

test("hhmmToMinutes parses times and rejects junk", () => {
  assert.equal(app.hhmmToMinutes("00:00"), 0);
  assert.equal(app.hhmmToMinutes("19:30"), 1170);
  assert.equal(app.hhmmToMinutes("9:05"), 545);
  assert.equal(app.hhmmToMinutes("nope"), null);
  assert.equal(app.hhmmToMinutes(""), null);
});

test("an unrestricted ticket is valid at any time, including unknown", () => {
  const gold = byId["sc-gold-dayrider"];
  assert.equal(app.ticketValidAtTime(gold, "12:00"), true);
  assert.equal(app.ticketValidAtTime(gold, ""), true);
});

test("the Gold Nightrider is only valid inside its evening window", () => {
  const night = byId["sc-gold-nightrider"];
  assert.equal(app.ticketValidAtTime(night, "12:00"), false, "midday");
  assert.equal(app.ticketValidAtTime(night, "19:29"), false, "just before 19:30");
  assert.equal(app.ticketValidAtTime(night, "19:30"), true, "start of the window");
  assert.equal(app.ticketValidAtTime(night, "23:59"), true, "late evening");
});

test("the evening window wraps past midnight for night services", () => {
  // The N700 runs in the small hours; 00:25 is still that evening's ticket.
  const night = byId["sc-gold-nightrider"];
  assert.equal(app.ticketValidAtTime(night, "00:25"), true);
  assert.equal(app.ticketValidAtTime(night, "03:59"), true);
  assert.equal(app.ticketValidAtTime(night, "04:00"), false, "window closes at 04:00");
  assert.equal(app.ticketValidAtTime(night, "06:00"), false);
});

test("a time-restricted ticket is excluded when the time is unknown", () => {
  // Quoting £4 for a journey we can't place in time would understate the fare.
  assert.equal(app.ticketValidAtTime(byId["sc-gold-nightrider"], ""), false);
});

// ── Night-service supplements ───────────────────────────────

const META = zonesData.fares_meta;

test("the N700 supplement applies to Stagecoach but not other operators", () => {
  assert.equal(app.serviceSupplement(META, "N700", "SCSO").price_pence, 200);
  assert.equal(app.serviceSupplement(META, "N700", "BHBC"), null);
  assert.equal(app.serviceSupplement(META, "700", "SCSO"), null);
});

test("a supplement is added to every priced option", () => {
  const sup = { price_pence: 200, label: "N700 add-on" };
  const zonal = { zones: ["a", "b"], total: 1200, priced: true };
  const net = { zone: { name: "Gold" }, pence: 900 };
  const best = app.cheapestRealOption(zonal, net, sup);
  // £9 Gold + £2 add-on — not £9.
  assert.equal(best.total, 1100);
  assert.equal(best.kind, "network");
});

test("omitting the supplement leaves totals untouched", () => {
  const zonal = { zones: ["a"], total: 600, priced: true };
  assert.equal(app.cheapestRealOption(zonal, null).total, 600);
});

test("the supplement is disclosed in the rendered output", () => {
  const html = app.penaltyMoneyHtml(
    { kind: "network", total: 1100, tickets: 1, zone: { name: "Gold DayRider" },
      supplement: { price_pence: 200, label: "N700 night-bus add-on" } },
    META, "N700");
  assert.ok(/£11\.00/.test(html));
  assert.ok(/N700 night-bus add-on/.test(html));
  assert.ok(/£2\.00/.test(html));
});

// ── Reforms: which ask helps this journey, and by how much ──


test("merging zones is offered when both tickets are the same operator", () => {
  // Station Road to Marine Parade: Worthing + Brighton DayRider, both SCSO.
  const rs = app.reformsForJourney(
    ["sc-worthing-dayrider", "sc-brighton-dayrider"], byId, META);
  assert.equal(rs.length, 1);
  assert.equal(rs[0].id, "merge-stagecoach-zones");
  assert.equal(rs[0].price_pence, 600, "the ask is one ticket at the £6 each already costs");
});

test("cross-operator acceptance is offered when operators differ", () => {
  const rs = app.reformsForJourney(
    ["sc-worthing-dayrider", "bh-citysaver"], byId, META);
  assert.equal(rs.length, 1);
  assert.equal(rs[0].id, "cross-operator-acceptance");
  // Not an invented figure: the cheapest day ticket already on the route.
  assert.equal(rs[0].price_pence, 600);
});

test("no reform is offered when one ticket already covers the journey", () => {
  assert.deepEqual(plain(app.reformsForJourney(["sc-worthing-dayrider"], byId, META)), []);
  assert.deepEqual(plain(app.reformsForJourney([], byId, META)), []);
});

test("the merge saving is quoted against the Gold DayRider, per week", () => {
  const html = app.reformComparisonHtml(
    { kind: "network", total: 900, tickets: 1, zone: { name: "Gold Dayrider" }, supplement: null },
    ["sc-worthing-dayrider", "sc-brighton-dayrider"], byId, META);
  assert.ok(/would cost <strong>£6\.00<\/strong>/.test(html), `got: ${html}`);
  // (£9.00 - £6.00) x 5 days = £15.00
  assert.ok(/saving £15\.00 a week/.test(html), `got: ${html}`);
});

test("no reform is claimed when it wouldn't be cheaper", () => {
  const html = app.reformComparisonHtml(
    { kind: "network", total: 600, tickets: 1, zone: {}, supplement: null },
    ["sc-worthing-dayrider", "sc-brighton-dayrider"], byId, META);
  assert.equal(html, "", "must not claim a saving at or below break-even");
});

// ── The all-operator ticket as a real option ────────────────

test("the Discovery ticket is offered when it's genuinely the cheapest", () => {
  // Two operators' day tickets come to £12.30; the £10 all-operator ticket
  // undercuts them, so that's what a passenger would actually buy.
  const unified = app.unifiedTicketOption(META, "700");
  const best = app.cheapestRealOption(
    { zones: ["a", "b"], total: 1230, priced: true }, null, null, unified);
  assert.equal(best.kind, "unified");
  assert.equal(best.total, 1000);
});

test("a cheaper operator ticket still wins over Discovery", () => {
  // Station Road to Marine Parade: Gold DayRider £9 beats the £10 Discovery.
  const unified = app.unifiedTicketOption(META, "700");
  const best = app.cheapestRealOption(
    { zones: ["a", "b"], total: 1200, priced: true },
    { zone: { name: "Gold Dayrider" }, pence: 900 }, null, unified);
  assert.equal(best.kind, "network");
  assert.equal(best.total, 900);
});

test("Discovery is never offered on a service it isn't valid for", () => {
  assert.equal(app.unifiedTicketOption(META, "N700"), null);
  assert.equal(app.unifiedTicketOption(META, "N1"), null);
  assert.ok(app.unifiedTicketOption(META, "700"));
});

test("Discovery carries no night supplement — it's excluded, not surcharged", () => {
  const unified = app.unifiedTicketOption(META, "700");
  const best = app.cheapestRealOption(
    { zones: ["a", "b"], total: 1230, priced: true }, null,
    { price_pence: 200, label: "add-on" }, unified);
  assert.equal(best.kind, "unified");
  assert.equal(best.total, 1000, "supplement must not be added to the unified ticket");
});

test("the Discovery headline names and links the ticket", () => {
  const html = app.penaltyMoneyHtml(
    { kind: "unified", total: 1000, tickets: 1,
      zone: { name: "South Downs Discovery Ticket" },
      supplement: null, unified: META.unified_ticket }, META, "700");
  assert.ok(/South Downs Discovery Ticket/.test(html));
  assert.ok(/£10\.00/.test(html));
  assert.ok(/southdowns\.gov\.uk/.test(html), "should link its source");
});

test("merging zones is measured against Discovery when Discovery is cheapest", () => {
  // £10 today vs £6 merged = £4/day x 5 = £20 a week.
  const html = app.reformComparisonHtml(
    { kind: "unified", total: 1000, tickets: 1, zone: {}, supplement: null },
    ["sc-worthing-dayrider", "sc-brighton-dayrider"], byId, META);
  assert.ok(/would cost <strong>£6\.00<\/strong>/.test(html), `got: ${html}`);
  assert.ok(/saving £20\.00 a week/.test(html), `got: ${html}`);
});

// ── Evening-only tickets are out of the costing ─────────────

test("evening-only tickets are excluded from the standard fare comparison", () => {
  assert.equal(app.isStandardFareZone(byId["sc-gold-nightrider"]), false);
  assert.equal(app.isStandardFareZone(byId["sc-gold-dayrider"]), true);
  assert.equal(app.isStandardFareZone(byId["bh-citysaver"]), true);
});

test("zoneDayFare reads the adult day price, or null", () => {
  assert.equal(app.zoneDayFare(byId["sc-worthing-dayrider"]), 600);
  assert.equal(app.zoneDayFare({}), null);
  assert.equal(app.zoneDayFare(null), null);
});

test("the zone breakdown itemises each ticket and the total", () => {
  const best = { zones: ["sc-worthing-dayrider", "sc-brighton-dayrider"],
                 total: 1200, priced: true };
  const html = app.zoneCostHtml(best, byId);
  assert.ok(/Worthing Dayrider/.test(html));
  assert.ok(/Brighton Dayrider/.test(html));
  assert.equal((html.match(/£6\.00/g) || []).length, 2, "both tickets priced");
  assert.ok(/£12\.00/.test(html), "and the combined total");
});

test("the breakdown shows a dash rather than a wrong price when unpriced", () => {
  const best = { zones: ["sc-gold-nightrider"], total: 0, priced: false };
  const nightNoFare = { ...byId, "sc-gold-nightrider": { name: "X", operator: "SCSO" } };
  const html = app.zoneCostHtml(best, nightNoFare);
  assert.ok(/—/.test(html));
  assert.ok(!/Buying all/.test(html), "no total when it can't be summed");
});

// ── Zonal vs operator-wide split ────────────────────────────

test("splitZonesByRule separates operator-wide tickets from zonal ones", () => {
  const { zonal, network } = app.splitZonesByRule(zones);
  const netIds = network.map(z => z.id);
  assert.ok(netIds.includes("sc-gold-dayrider"));
  assert.ok(netIds.includes("bh-networksaver"));
  assert.ok(zonal.map(z => z.id).includes("bh-citysaver"));
  assert.equal(zonal.length + network.length, zones.length);
});

test("a Worthing-to-Brighton run needs two ZONE tickets, not one Gold", () => {
  // The regression that matters. Gold Dayrider covers every all-Stagecoach
  // journey by definition; if it's allowed into the main search, this comes
  // back as "one ticket, no problem" and the feature says nothing.
  const { zonal, network } = app.splitZonesByRule(zones);

  // Real coordinates from the timetable, not guesses — the zone outlines are
  // tight enough that an approximate point lands outside and proves nothing.
  const path = [
    { lat: 50.831919, lon: -0.421517 },  // Tesco, Durrington (4400WO0013)
    { lat: 50.820363, lon: -0.138007 },  // Old Steine, Brighton (149000007828)
  ];
  const cover = path.map(s => app.zonesForStop(s, zones, byId, "SCSO"));

  const best = app.cheapestCover(cover, zonal);
  assert.ok(best, "expected a zonal cover");
  assert.equal(best.zones.length, 2, `got ${JSON.stringify(plain(best.zones))}`);

  // ...and the operator-wide ticket is still offered as the alternative.
  const net = app.bestNetworkTicket(cover, network);
  assert.ok(net, "expected a network alternative");
  assert.equal(net.zone.operator, "SCSO");
});

test("bestNetworkTicket returns null when no single ticket spans the path", () => {
  const { network } = app.splitZonesByRule(zones);
  // A B&H leg and a Stagecoach leg — no one operator covers both.
  const cover = [["bh-networksaver"], ["sc-gold-dayrider"]];
  assert.equal(app.bestNetworkTicket(cover, network), null);
});

// ── Money formatting ────────────────────────────────────────

test("formatGbp renders pence as pounds", () => {
  assert.equal(app.formatGbp(600), "£6.00");
  assert.equal(app.formatGbp(1150), "£11.50");
  assert.equal(app.formatGbp(5), "£0.05");
});

test("formatGbp rounds rather than truncating", () => {
  assert.equal(app.formatGbp(1234.6), "£12.35");
});

// ── Stop picker round-trip ──────────────────────────────────

/** Load stops into the app and clear the memoized picker index. */
function setStops(stops) {
  appState.stopData = stops;
  appState._stopIndex = null;
}

test("opposite poles of one stop collapse to a single entry", () => {
  // The common case: ~1,500 stops, ~800 places. Listing both poles of every
  // road is noise a passenger can't act on.
  setStops({
    "4400WO0013": { name: "Tesco", lat: 50.831919, lon: -0.421517 },
    "4400WO0014": { name: "Tesco", lat: 50.832100, lon: -0.421400 },
  });
  const index = app.stopPickerIndex();
  assert.equal(index.length, 1);
  assert.equal(index[0].label, "Tesco");
  assert.equal(index[0].atcos.length, 2);
});

test("same name in different places stays separate, tagged by district", () => {
  setStops({
    "4400WO0100": { name: "Marine Parade", lat: 50.8100, lon: -0.3700 },
    "149000007830": { name: "Marine Parade", lat: 50.8200, lon: -0.1300 },
  });
  const labels = app.stopPickerIndex().map(c => c.label).sort();
  assert.deepEqual(plain(labels),
    ["Marine Parade, Brighton & Hove", "Marine Parade, Worthing"]);
});

test("districts come from the NaPTAN area code", () => {
  assert.equal(app.districtForAtco("4400WO0013"), "Worthing");
  assert.equal(app.districtForAtco("4400AD1234"), "Adur");
  assert.equal(app.districtForAtco("149000007830"), "Brighton & Hove");
  assert.equal(app.districtForAtco("9999XX0001"), "");
});

test("two same-name places in one district fall back to the ATCO", () => {
  setStops({
    "4400WO0100": { name: "South Street", lat: 50.8100, lon: -0.3700 },
    "4400WO0200": { name: "South Street", lat: 50.8400, lon: -0.4100 },
  });
  const labels = app.stopPickerIndex().map(c => c.label);
  assert.equal(new Set(plain(labels)).size, 2, "labels must be distinguishable");
  assert.ok(labels.every(l => /South Street/.test(l)));
  assert.ok(labels.some(l => /4400WO0100|4400WO0200/.test(l)));
});

test("atcoFromPickerValue resolves a picked label to a stop", () => {
  setStops({ "4400WO0013": { name: "Tesco", lat: 50.8, lon: -0.37 } });
  assert.equal(app.atcoFromPickerValue("Tesco"), "4400WO0013");
});

test("atcoFromPickerValue is case-insensitive and accepts a raw ATCO", () => {
  setStops({ "4400WO0013": { name: "Tesco", lat: 50.8, lon: -0.37 } });
  assert.equal(app.atcoFromPickerValue("tesco"), "4400WO0013");
  assert.equal(app.atcoFromPickerValue("4400WO0013"), "4400WO0013");
});

test("atcoFromPickerValue returns empty for junk", () => {
  setStops({ "4400WO0013": { name: "Tesco", lat: 50.8, lon: -0.37 } });
  assert.equal(app.atcoFromPickerValue("not a stop"), "");
  assert.equal(app.atcoFromPickerValue(""), "");
});

// ── Objectives grouped by responsible body ──────────────────

const objectivesData = JSON.parse(
  readFileSync(join(ROOT, "data/objectives.json"), "utf8"));
const OBJECTIVES = objectivesData.objectives;

test("every objective names at least one body that would have to act", () => {
  for (const o of OBJECTIVES) {
    assert.ok(Array.isArray(o.lead) && o.lead.length, `${o.id} has no lead`);
    assert.ok(Array.isArray(o.shared), `${o.id} has no shared array`);
  }
});

test("a body is never both leading and merely involved", () => {
  for (const o of OBJECTIVES) {
    const both = o.lead.filter(c => o.shared.includes(c));
    assert.deepEqual(plain(both), [], `${o.id} lists ${both} twice`);
  }
});

test("every objective reaches a group, as lead or as shared", () => {
  const groups = app.groupObjectivesByBody(OBJECTIVES);
  const seen = new Set();
  for (const g of groups) for (const o of [...g.lead, ...g.shared]) seen.add(o.id);
  assert.equal(seen.size, OBJECTIVES.length, "an objective fell out of every group");
});

test("a group separates what a body leads from what it only supports", () => {
  // Stagecoach leads the DayRider zone merge; it is only a supporting party on
  // the county's real-time information programme.
  const scso = app.groupObjectivesByBody(OBJECTIVES).find(g => g.key === "SCSO");
  assert.ok(scso, "expected a Stagecoach group");
  assert.ok(scso.lead.some(o => o.id === "fairer-ticketing-zones"),
    "zone merge should be led by Stagecoach");
  assert.ok(scso.shared.some(o => o.id === "real-time-info-everywhere"),
    "RTPI should show as shared, not led, for an operator");
  assert.equal(scso.total, scso.lead.length + scso.shared.length);
});

test("an ask with no single owner leads in every operator's group", () => {
  // Audio-visual announcements are every operator's job; naming one would be
  // arbitrary and would let the others off.
  const groups = app.groupObjectivesByBody(OBJECTIVES);
  for (const code of ["SCSO", "BHBC", "METR", "COMT"]) {
    const g = groups.find(x => x.key === code);
    assert.ok(g && g.lead.some(o => o.id === "accessible-buses-stops"),
      `${code} should lead accessible-buses-stops`);
  }
});

test("councils appear as groups in their own right", () => {
  const keys = app.groupObjectivesByBody(OBJECTIVES).map(g => g.key);
  for (const code of ["WSCC", "BHCC", "ADUR_WORTHING", "ESCC"]) {
    assert.ok(keys.includes(code), `expected a ${code} group`);
  }
});

test("operators are listed before authorities", () => {
  const groups = app.groupObjectivesByBody(OBJECTIVES);
  const kindOf = (k) => (k === "SCSO" || k === "BHBC" || k === "METR" || k === "COMT") ? 0 : 1;
  const kinds = groups.map(g => kindOf(g.key));
  assert.deepEqual(plain(kinds), plain([...kinds].sort((a, b) => a - b)),
    "operator groups should all precede authority groups");
});

test("body codes resolve to the real organisation names", () => {
  assert.equal(app.bodyName("WSCC"), "West Sussex County Council");
  assert.equal(app.bodyName("ESCC"), "East Sussex County Council");
  assert.equal(app.bodyName("BHCC"), "Brighton & Hove City Council");
  assert.equal(app.bodyName("ADUR_WORTHING"), "Adur & Worthing Councils");
  assert.equal(app.bodyName("BHBC"), "Brighton & Hove Buses");
  assert.equal(app.bodyName("SCSO"), "Stagecoach South");
});

test("every body carries a contact link, so an ask is actionable", () => {
  // `const` declarations never land on the vm context object — same reason
  // `state` is reached this way at the top of this file.
  const bodies = vm.runInContext("RESPONSIBLE_BODIES", app);
  for (const [code, b] of Object.entries(bodies)) {
    assert.ok(/^https:\/\//.test(b.url || ""), `${code} has no contact URL`);
    assert.ok(b.name && b.kind, `${code} is missing name or kind`);
  }
});

test("only the lead gets a filled chip", () => {
  // A body that merely has to agree must not look like the one to write to.
  const html = app.objectiveBodyChips({ lead: ["SCSO"], shared: ["WSCC"] });
  const leadChips = (html.match(/objective-chip--lead/g) || []).length;
  assert.equal(leadChips, 1, "exactly one filled chip expected");
  assert.ok(/Stagecoach South/.test(html));
  assert.ok(/West Sussex County Council/.test(html));
});

test("authorities get a palette colour, never an operator's brand colour", () => {
  const operatorColour = app.bodyColour("BHBC");
  for (const code of ["WSCC", "ESCC", "BHCC", "ADUR_WORTHING"]) {
    const c = app.bodyColour(code);
    assert.ok(/^#[0-9a-f]{6}$/i.test(c), `${code} colour looks wrong: ${c}`);
    assert.notEqual(c, operatorColour, `${code} reuses an operator's brand colour`);
  }
});

test("the featured asks are the live campaign ones", () => {
  const featured = OBJECTIVES.filter(o => o.featured).map(o => o.id);
  assert.ok(featured.includes("fairer-ticketing-zones"));
  assert.ok(featured.includes("cross-operator-tickets"));
  assert.ok(featured.includes("connect-adur-to-brighton"));
});
