/**
 * Brighton & Hove's citySAVER and networkSAVER on Compass buses.
 *
 * Brighton & Hove's own SAVER page lists the other companies' routes that take
 * its tickets: every kind of SAVER on Compass's 37, 47 and 52 (and their
 * variants) for the whole route, and some networkSAVERs on parts of other
 * Compass routes. So both tickets are listed under Compass too, with that
 * caveat, and a stop says so only where it is true: a Compass route on the
 * list calls there, and for citySAVER the stop is inside its zone.
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
const DATA = JSON.parse(readFileSync(join(ROOT, "data", "ticket_zones.json"), "utf8"));
const zone = (id) => DATA.zones.find(z => z.id === id);

run(`state.ticketZones = ${JSON.stringify(DATA.zones)};`);
// Two stops: Old Steine in Brighton, inside citySAVER; Worthing, well outside it.
run(`state.stopData = { INSIDE: { lat: 50.8225, lon: -0.1372 }, OUTSIDE: { lat: 50.8119, lon: -0.3714 } };`);

test("both SAVERs say Compass takes them, from Brighton & Hove's own list", () => {
  for (const id of ["bh-citysaver", "bh-networksaver"]) {
    const compass = (zone(id).also_accepted_on || []).filter(a => a.operator === "COMT");
    assert.ok(compass.length, `${id} lists no Compass routes`);
    assert.ok(compass.some(a => ["37", "47", "52"].every(s => a.services.includes(s))),
      `${id} should be accepted on the 37, 47 and 52`);
    for (const a of compass) assert.equal(a.source_url, "https://www.buses.co.uk/saver-ticket");
  }
});

test("a SAVER is accepted on a listed Compass route, and not on any other", () => {
  const accepted = (id, op, svc) => plain(run(`ticketAcceptance(state.ticketZones.find(z => z.id === "${id}"), "${op}", "${svc}")`));
  assert.ok(accepted("bh-citysaver", "COMT", "47"));
  assert.ok(accepted("bh-citysaver", "CMPA", "47"), "Compass's other operator code is the same company");
  assert.equal(accepted("bh-citysaver", "COMT", "12"), null, "the 12 is not on Brighton & Hove's list");
  assert.equal(accepted("bh-citysaver", "SCSO", "47"), null);
});

test("Compass's ticket list shows both SAVERs, after its own ticket", () => {
  const groups = plain(run("ticketOperatorGroups(state.ticketZones)"));
  const compass = groups.find(g => g.op === "COMT");
  assert.deepEqual(compass.own.map(z => z.id), ["compass-rover"]);
  assert.deepEqual(compass.accepted.map(z => z.id), ["bh-citysaver", "bh-networksaver"]);
  const bh = groups.find(g => g.op === "BHBC");
  assert.ok(bh.own.some(z => z.id === "bh-citysaver"), "the SAVERs must still be Brighton & Hove's own tickets");
});

test("the card under Compass carries the caveat: these routes, and citySAVER only inside its zone", () => {
  const html = run(`acceptedTicketCardHtml(state.ticketZones.find(z => z.id === "bh-citysaver"), "COMT")`);
  assert.match(html, /37/);
  assert.match(html, /47/);
  assert.match(html, /52/);
  assert.match(html, /only inside the citySAVER zone/i);
  assert.match(html, /href="https:\/\/www\.buses\.co\.uk\/saver-ticket"/);
  assert.doesNotMatch(html, /Buses's/, "Brighton & Hove Buses' list, not Buses's");
});

test("in Route view a stop names the SAVERs only where they really work", () => {
  const inside = run(`routeStopTicketsHtml("INSIDE", [{ service: "47", operator: "COMT" }])`);
  assert.match(inside, /citySAVER/);
  assert.match(inside, /networkSAVER/);
  const outside = run(`routeStopTicketsHtml("OUTSIDE", [{ service: "47", operator: "COMT" }])`);
  assert.doesNotMatch(outside, /citySAVER/, "citySAVER is only valid inside its zone");
  assert.match(outside, /networkSAVER/, "networkSAVER is accepted on the whole of the 47");
  const other = run(`routeStopTicketsHtml("INSIDE", [{ service: "12", operator: "COMT" }])`);
  assert.doesNotMatch(other, /SAVER/, "a Compass route not on the list takes neither");
});

test("the Bus tab on a Compass 47 mentions the SAVERs, and on a Compass 12 does not", () => {
  run(`state.ticketFaresMeta = ${JSON.stringify(DATA.fares_meta)};`);
  assert.match(run(`buildTicketInfoHtml("COMT", null, "47")`), /citySAVER/);
  assert.doesNotMatch(run(`buildTicketInfoHtml("COMT", null, "12")`), /citySAVER/);
});
