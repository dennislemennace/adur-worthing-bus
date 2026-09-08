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
