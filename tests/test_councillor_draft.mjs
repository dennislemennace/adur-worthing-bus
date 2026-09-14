/**
 * Tests for the draft the reader is asked to personalise.
 *
 * The dialog says, in as many words, "Edit this before you send it — a letter
 * in your own words counts for far more than an identical one." The mailto:
 * link was built once when the dialog rendered, so every edit the reader made
 * was thrown away by the button labelled "Open in your email app", while the
 * Copy button next to it used the edited text. The two primary actions
 * disagreed about what the reader had written.
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
const state = vm.runInContext("state", app);
const SOURCE = readFileSync(join(ROOT, "app.js"), "utf8");

const AREA = {
  name: "Marine",
  council: "Adur District Council",
  checked_on: "2026-09-07",
  source_url: "https://www.adur-worthing.gov.uk/councillors/",
  members: [
    { name: "A Councillor", party: "Independent", email: "a.councillor@adur-worthing.gov.uk" },
    { name: "B Councillor", party: "Independent", email: "b.councillor@adur-worthing.gov.uk" },
  ],
};

const SUBJECT = "Bus services in Adur and Worthing: A test objective";

function bodyOf(mailtoUrl) {
  const m = /[?&]body=([^&]*)/.exec(mailtoUrl);
  return m ? decodeURIComponent(m[1]) : "";
}

// ── The reader's words are the ones that get sent ───────────

test("the email link is built from the draft as it stands now", () => {
  state.councillorDraftContext = { area: AREA, subject: SUBJECT };
  const edited = "Dear councillor,\n\nI live on Boundary Road and this is my "
               + "own account of the problem.\n\nYours,\nA resident";
  const { url } = app.councillorMailtoFromDraft(edited);
  assert.equal(bodyOf(url), edited,
    "the link carried text the reader had replaced");
});

test("wording the reader deleted does not reappear in the link", () => {
  state.councillorDraftContext = { area: AREA, subject: SUBJECT };
  const { url } = app.councillorMailtoFromDraft("Just this one line.");
  assert.doesNotMatch(bodyOf(url), /Yours sincerely|bracketed/,
    "generated boilerplate survived into a draft that no longer contains it");
});

test("the long-draft warning is recalculated for the current text", () => {
  state.councillorDraftContext = { area: AREA, subject: SUBJECT };
  assert.equal(app.councillorMailtoFromDraft("Short.").long, false);
  assert.equal(app.councillorMailtoFromDraft("x".repeat(4000)).long, true,
    "a draft grown past the mailto limit was still treated as short");
});

test("non-ASCII characters survive the round trip", () => {
  state.councillorDraftContext = { area: AREA, subject: SUBJECT };
  const text = "Café — naïve — £6.00 — “quoted” — 日本語";
  assert.equal(bodyOf(app.councillorMailtoFromDraft(text).url), text);
});

test("the recipients are still the councillors for the area", () => {
  state.councillorDraftContext = { area: AREA, subject: SUBJECT };
  const { url } = app.councillorMailtoFromDraft("text");
  assert.match(url, /a\.councillor@adur-worthing\.gov\.uk/);
  assert.match(url, /b\.councillor@adur-worthing\.gov\.uk/);
  // Literal @ and , — several clients mishandle them percent-encoded.
  assert.ok(!url.includes("%40") && !url.includes("%2C"));
});

test("nothing is built when there is no draft context", () => {
  state.councillorDraftContext = null;
  assert.equal(app.councillorMailtoFromDraft("text"), null);
});

// ── The structural guard ────────────────────────────────────

test("the rendered dialog does not bake a draft body into a link", () => {
  // The property that keeps the above true. A `mailto:` with a `body=`
  // rendered into markup is a snapshot of the text at render time, and the
  // textarea underneath it can only diverge from that point on.
  const html = app.councillorResultHtml(
    { id: "x", title: "A test objective", description: "Why it matters." }, AREA);
  assert.ok(html.includes("councillor-draft-text"), "fixture: draft is rendered");
  assert.doesNotMatch(html, /href="mailto:[^"]*body=/,
    "a draft body was frozen into an href at render time");
});

// ── A letter a councillor will read to the end ──────────────
//
// The draft pasted each objective's whole description into the letter: four or
// five paragraphs of campaign copy before the reader's own words, which is the
// shape of a template and the reason councillors skim them. It is now a short
// letter written for the purpose, the reader's paragraph, and a link to the
// detail on the site.

const OBJECTIVES = JSON.parse(readFileSync(join(ROOT, "data", "objectives.json"), "utf8")).objectives;
const draftFor = (o) => JSON.parse(JSON.stringify(
  vm.runInContext(`councillorDraft(${JSON.stringify(o)}, ${JSON.stringify(AREA)})`, app)));
const words = (s) => s.split(/\s+/).filter(Boolean).length;
const paragraphs = (s) => s.split(/\n\s*\n/).map(p => p.trim()).filter(Boolean);

test("no objective's letter pastes in its full description", () => {
  for (const o of OBJECTIVES) {
    const body = draftFor(o).body;
    const longest = paragraphs(o.description).sort((a, b) => b.length - a.length)[0];
    assert.ok(!body.includes(longest),
      `${o.id}: the draft still carries the description's longest paragraph`);
  }
});

test("every draft is a few paragraphs plus the reader's own", () => {
  for (const o of OBJECTIVES) {
    const body = draftFor(o).body;
    // Greeting and sign-off are lines, not paragraphs of argument.
    const content = paragraphs(body).filter(p =>
      !/^Dear /.test(p) && !/^Yours sincerely/.test(p) && !/^\[Please add/.test(p));
    assert.ok(content.length <= 4,
      `${o.id}: ${content.length} paragraphs besides the greeting, sign-off and the reader's own`);
    assert.ok(words(body) <= 230, `${o.id}: the draft is ${words(body)} words`);
    assert.match(body, /\[Please add/, `${o.id}: the draft has no place for the reader's own words`);
  }
});

test("every draft links to that objective on the site", () => {
  for (const o of OBJECTIVES) {
    const body = draftFor(o).body;
    assert.ok(body.includes(`https://worthingbrightonbus.co.uk/#view=n&objective=${o.id}`),
      `${o.id}: no link back to the objective`);
  }
});

test("a letter never states a figure the objective itself does not", () => {
  // Letters go to elected representatives, who can check. A number that is not
  // on the objective page is a number with nothing behind it.
  for (const o of OBJECTIVES) {
    assert.ok(typeof o.letter === "string" && o.letter.trim(), `${o.id}: no letter`);
    for (const n of o.letter.match(/\d[\d,.]*/g) || []) {
      assert.ok(o.description.includes(n.replace(/[.,]$/, "")),
        `${o.id}: the letter says ${n}, which the objective's description does not`);
    }
  }
});

test("a link opens Better buses with that objective expanded", () => {
  const parsed = JSON.parse(JSON.stringify(vm.runInContext(
    `(() => { location.hash = "#view=n&objective=night-service-west"; return parseUrlState(); })()`, app)));
  assert.equal(parsed.view, "n");
  assert.equal(parsed.objective, "night-service-west");
  const src = SOURCE.slice(SOURCE.indexOf("async function applyUrlState"));
  assert.match(src.slice(0, 3000), /parsed\.objective/,
    "applyUrlState does not read the objective from the link");
});
