/**
 * Tests for "email your councillor" — the pure logic between a postcode and a
 * draft letter.
 *
 * The thing worth pinning is the tier. Buses are a county-council function
 * here, but bus shelters are a district one, so an objective led by West Sussex
 * has to resolve the reader's *electoral division* while one led by Adur or
 * Worthing has to resolve their *ward*. Getting that backwards does not fail
 * loudly — it produces a real councillor, a real address, and a letter that
 * arrives at someone with no power over the thing it asks about.
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
// Declared with `const`, so like `state` it never lands on the context object.
const BODY_AREA = vm.runInContext("BODY_AREA", app);

const reps = JSON.parse(readFileSync(join(ROOT, "data/representatives.json"), "utf8"));

/** Arrays built inside the vm come from another realm, so their prototypes
 *  differ and deepStrictEqual rejects them on identity alone. Same helper the
 *  boundary tests use. */
const plain = (v) => JSON.parse(JSON.stringify(v));

// What postcodes.io returns, reduced to the fields lookupPostcode keeps.
// Real codes, checked against the live service: BN11 1AA (Worthing town
// centre) and BN41 1DN (Portslade, the other side of the council boundary
// this whole site exists to describe).
const WORTHING  = { district: "Worthing", county: "West Sussex",
                    ced: "E58001667", ward: "E05007696" };
// A unitary authority reports no county at all, which is what separates
// Brighton & Hove from the two-tier counties either side of it.
const PORTSLADE = { district: "Brighton and Hove", county: null,
                    ced: null, ward: "E05015414" };
const ARUN      = { district: "Arun", county: "West Sussex",
                    ced: "E58001621", ward: "E05009809" };
const LEWES     = { district: "Lewes", county: "East Sussex",
                    ced: "E58000374", ward: "E05011589" };

// ── The tier follows the body ───────────────────────────────

test("a West Sussex objective resolves the county electoral division", () => {
  const { area } = app.resolveRepresentatives(reps, "WSCC", WORTHING);
  assert.equal(area.body, "WSCC");
  assert.equal(area.council, "West Sussex County Council");
  // Not the ward: E05007696 is "Central", and resolving it here would be the
  // silent failure this test exists for.
  assert.notEqual(area.name, "Central");
});

test("an Adur & Worthing objective resolves the district ward", () => {
  const { area } = app.resolveRepresentatives(reps, "ADUR_WORTHING", WORTHING);
  assert.equal(area.body, "ADUR_WORTHING");
  assert.equal(area.name, "Central");
});

test("the same postcode gives different people for the two tiers", () => {
  const county = app.resolveRepresentatives(reps, "WSCC", WORTHING).area;
  const ward   = app.resolveRepresentatives(reps, "ADUR_WORTHING", WORTHING).area;
  assert.notEqual(county.name, ward.name);
  assert.notEqual(county.council, ward.council);
});

test("a unitary authority resolves by ward, having no electoral division", () => {
  // Brighton & Hove's codes.ced is E99999999 ("not applicable"), which
  // lookupPostcode maps to null. Asking for a division would find nothing.
  const { area } = app.resolveRepresentatives(reps, "BHCC", PORTSLADE);
  assert.equal(area.body, "BHCC");
  assert.equal(area.council, "Brighton & Hove City Council");
});

// ── Refusing to answer, correctly ───────────────────────────

test("an East Sussex objective resolves an East Sussex division", () => {
  // The corridor does not stop at Brighton, so the integrated-plan objective
  // needs all three authorities. A Lewes reader has a county councillor with a
  // say in it; a Worthing reader does not.
  const { area } = app.resolveRepresentatives(reps, "ESCC", LEWES);
  assert.equal(area.body, "ESCC");
  assert.equal(area.council, "East Sussex County Council");
});

test("the two counties cannot resolve into each other", () => {
  // Both are `ced`-keyed, so without a county check a West Sussex reader would
  // fall through to "we don't hold your area" — true, but the wrong reason,
  // and one division named the same in both counties would resolve outright.
  assert.equal(app.resolveRepresentatives(reps, "ESCC", WORTHING).error, "out-of-area");
  assert.equal(app.resolveRepresentatives(reps, "WSCC", LEWES).error, "out-of-area");
});

test("a postcode outside the body's districts says so rather than guessing", () => {
  // Arun is in West Sussex, so the county objective still works — but Adur &
  // Worthing have no councillor for an Arun address, and offering the nearest
  // one would be worse than saying nothing.
  assert.ok(app.resolveRepresentatives(reps, "WSCC", ARUN).area,
    "the county covers all of West Sussex, including Arun");
  assert.equal(app.resolveRepresentatives(reps, "ADUR_WORTHING", ARUN).error, "out-of-area");
});

test("an area we do not hold is reported, not silently empty", () => {
  const nowhere = { district: "Worthing", county: "West Sussex",
                    ced: "E58999999", ward: "E05999999" };
  assert.equal(app.resolveRepresentatives(reps, "WSCC", nowhere).error, "not-listed");
});

test("an operator has no councillors and is never offered one", () => {
  // Route and timetable decisions are commercial. The existing "Contact"
  // link is the right route for these, not a councillor.
  for (const noc of ["BHBC", "SCSO", "METR", "COMT"]) {
    assert.equal(app.resolveRepresentatives(reps, noc, WORTHING).error, "no-councillors");
  }
});

// ── Which button appears on which objective ─────────────────

test("objectives offer every authority involved, lead before shared", () => {
  const o = { lead: ["SCSO", "WSCC"], shared: ["BHCC", "BHBC"] };
  assert.deepEqual(plain(app.objectiveAuthorities(o)), ["WSCC", "BHCC"]);
});

test("an objective needing no authority offers no councillor at all", () => {
  assert.deepEqual(plain(app.objectiveAuthorities({ lead: ["BHBC"], shared: ["SCSO"] })), []);
});

test("a body named twice is offered once", () => {
  assert.deepEqual(plain(app.objectiveAuthorities({ lead: ["WSCC"], shared: ["WSCC"] })), ["WSCC"]);
});

test("no published objective offers a body that cannot be resolved", () => {
  // Not an assertion that all eight get a button — most are operator-led, and
  // that is the truth about who runs the buses. This checks the weaker, real
  // property: no button is ever shown that leads nowhere.
  const objectives = JSON.parse(
    readFileSync(join(ROOT, "data/objectives.json"), "utf8")).objectives;
  for (const o of objectives) {
    for (const code of app.objectiveAuthorities(o)) {
      assert.ok(BODY_AREA[code], `${o.id} offers ${code}, which cannot be resolved`);
    }
  }
});

// ── Postcodes ───────────────────────────────────────────────

test("postcodes are accepted however they are typed", () => {
  for (const raw of ["BN11 1AA", "bn111aa", " Bn11  1aa ", "BN11-1AA"]) {
    assert.equal(app.normalisePostcode(raw), "BN11 1AA", `rejected ${JSON.stringify(raw)}`);
  }
});

test("something that is not a postcode is rejected before a request is made", () => {
  // Rejecting locally keeps a typo from becoming a request, and keeps the
  // error specific instead of "we couldn't find that".
  for (const raw of ["", "BN11", "hello", "12345", "BN11 1AAA"]) {
    assert.equal(app.normalisePostcode(raw), null, `accepted ${JSON.stringify(raw)}`);
  }
});

// ── The draft ───────────────────────────────────────────────

const OBJECTIVE = {
  id: "test", title: "A joined-up plan", summary: "One plan across Sussex",
  description: "Work with neighbouring authorities on an integrated plan.",
};

test("the draft names the councillors and their area", () => {
  const area = app.resolveRepresentatives(reps, "ADUR_WORTHING", WORTHING).area;
  const draft = app.councillorDraft(OBJECTIVE, area);
  assert.ok(draft.body.startsWith("Dear "));
  for (const m of area.members) assert.ok(draft.body.includes(m.name), `missing ${m.name}`);
  assert.ok(draft.body.includes(area.name), "the letter should say where the writer lives");
  assert.ok(draft.subject.includes(OBJECTIVE.title));
});

test("the draft leaves the personal paragraph for the sender to write", () => {
  // The whole point. A letter that arrives complete gets sent unaltered, and an
  // identical template carries far less weight with a councillor's office than
  // one sentence in someone's own words.
  const area = app.resolveRepresentatives(reps, "WSCC", WORTHING).area;
  const draft = app.councillorDraft(OBJECTIVE, area);
  assert.match(draft.body, /\[Please add/,
    "no prompt for the sender's own words — this is a template, not a letter");
});

test("the mailto carries every councillor for the area", () => {
  const area = app.resolveRepresentatives(reps, "ADUR_WORTHING", WORTHING).area;
  const { url } = app.councillorMailto(area, app.councillorDraft(OBJECTIVE, area));
  assert.ok(url.startsWith("mailto:"));
  const to = decodeURIComponent(url.slice("mailto:".length).split("?")[0]);
  assert.deepEqual(to.split(","), plain(area.members).map(m => m.email));
});

test("the mailto is a valid URL with subject and body", () => {
  const area = app.resolveRepresentatives(reps, "WSCC", WORTHING).area;
  const { url } = app.councillorMailto(area, app.councillorDraft(OBJECTIVE, area));
  const parsed = new URL(url);
  assert.equal(parsed.protocol, "mailto:");
  const params = new URLSearchParams(parsed.search);
  assert.ok(params.get("subject"));
  assert.ok(params.get("body").includes("Yours sincerely"));
});

test("an over-long draft is flagged rather than silently truncated", () => {
  // Some webmail clients cut a long mailto body without saying so. The reader
  // is told to copy instead, which is the only honest option.
  const area = app.resolveRepresentatives(reps, "WSCC", WORTHING).area;
  const short = app.councillorMailto(area, { subject: "x", body: "y" });
  const long  = app.councillorMailto(area, { subject: "x", body: "y".repeat(3000) });
  assert.equal(short.long, false);
  assert.equal(long.long, true);
});
