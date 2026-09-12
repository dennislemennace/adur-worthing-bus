/**
 * Tests for how stop names reach the page.
 *
 * Stop names come from an upstream feed, and they contain apostrophes:
 * "Church Place St Mary's Hall" is a real stop. `escapeAttr` applied
 * JavaScript string escaping to an HTML attribute — backslash before an
 * apostrophe — because the value was being interpolated into an inline
 * onclick. That broke the name for everyone, and left ampersands untouched,
 * so an HTML entity in a name decoded back into the handler.
 *
 * The fix is structural: no generated markup carries an inline handler, so
 * there is no JavaScript context to escape for. These tests assert that
 * property rather than the shape of any particular escape.
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
// `state` is declared with const, so it never lands on the context object.
const state = vm.runInContext("state", app);
const SOURCE = readFileSync(join(ROOT, "app.js"), "utf8");

// A real stop, and the reason this file exists.
const APOSTROPHE = "Church Place St Mary's Hall";

// ── Attribute escaping is HTML escaping ─────────────────────

test("an apostrophe survives as an apostrophe, not a backslash", () => {
  const out = app.escapeAttr(APOSTROPHE);
  assert.ok(!out.includes("\\'"),
    "the name carries a JavaScript escape into HTML: " + out);
  // Either the character itself or its HTML entity is fine; a backslash is not.
  assert.ok(out.includes("'") || out.includes("&#39;"));
});

test("ampersands are escaped, so an entity cannot be smuggled through", () => {
  // &#39; left intact decodes to an apostrophe when the browser parses the
  // attribute — which is how a name became executable inside a handler.
  assert.match(app.escapeAttr("Fish & Chips"), /&amp;/);
  const out = app.escapeAttr("&#39;);alert(1);//");
  assert.ok(!out.includes("&#39;)"),
    "an entity passed through unescaped: " + out);
});

test("quotes cannot close the attribute they sit in", () => {
  const out = app.escapeAttr('a" onmouseover="alert(1)');
  assert.ok(!/"\s*onmouseover/.test(out), out);
});

// ── There is no JavaScript context to escape for ────────────

test("no generated markup carries an inline event handler", () => {
  // The real fix. An interpolated value inside onclick="" is parsed as
  // JavaScript after HTML entity decoding, so no amount of escaping in the
  // HTML layer makes it safe. Attaching listeners to elements removes the
  // question.
  const offenders = [];
  // A double-quoted attribute may legitimately contain single quotes — which
  // is exactly the shape of the defect — so each quoting style is scanned
  // against its own delimiter rather than against "either quote".
  for (const re of [/\bon[a-z]+\s*=\s*"[^"]*\$\{/g,
                    /\bon[a-z]+\s*=\s*'[^']*\$\{/g]) {
    let m;
    while ((m = re.exec(SOURCE)) !== null) {
      const line = SOURCE.slice(0, m.index).split("\n").length;
      offenders.push(`app.js:${line}  ${m[0]}`);
    }
  }
  assert.deepEqual(offenders, [],
    "interpolated value inside an inline handler:\n" + offenders.join("\n"));
});

test("a stop popup is built as an element, not an HTML string", () => {
  // Leaflet accepts either. An element cannot be built by concatenation, so
  // this is the property that keeps the previous test true as the file grows.
  const popup = app.buildStopPopup("14900000741", APOSTROPHE);
  assert.ok(popup && typeof popup === "object",
    "buildStopPopup should return a DOM node");
});

// ── The datalist the apostrophe bug was reported through ────

test("a name with an apostrophe round-trips through the stop picker", () => {
  const seen = [];
  const listEl = {
    set innerHTML(v) { seen.push(v); },
    get innerHTML() { return seen[seen.length - 1] || ""; },
  };
  state._stopIndex = [{ label: APOSTROPHE, atcos: ["14900000741"] }];
  app.fillStopDatalist(listEl, "mary");

  const html = listEl.innerHTML;
  assert.ok(!html.includes("\\'"),
    "the datalist offered a backslash-escaped name, which then matched "
    + "nothing when the user picked it: " + html);
});

// ── Link schemes ────────────────────────────────────────────

test("a javascript: link is refused, not merely escaped", () => {
  // HTML escaping stops a URL breaking out of its attribute. It does nothing
  // about what the URL does once clicked, and these values come from data
  // files a moderator approves by reading JSON.
  for (const bad of [
    "javascript:alert(1)",
    "JavaScript:alert(1)",
    "  javascript:alert(1)",
    "java\tscript:alert(1)",
    "java\u0000script:alert(1)",
    "data:text/html;base64,PHNjcmlwdD4=",
    "vbscript:msgbox(1)",
  ]) {
    assert.equal(app.safeUrl(bad), "", `allowed: ${JSON.stringify(bad)}`);
  }
});

test("the links the site actually uses are kept", () => {
  for (const good of [
    "https://www.buses.co.uk/saver-ticket",
    "http://example.org/x?a=1&b=2",
    "mailto:councillor@example.gov.uk",
    "media/updates/photo.jpg",
    "/data/objectives.json",
    "#section",
    "//cdn.example.org/x.png",
  ]) {
    assert.equal(app.safeUrl(good), good.trim(), `dropped: ${good}`);
  }
});

test("an empty or missing URL comes back empty, not as the string null", () => {
  assert.equal(app.safeUrl(null), "");
  assert.equal(app.safeUrl(undefined), "");
  assert.equal(app.safeUrl(""), "");
});

// ── What a submission does, said where it is done ───────────

test("the proposal editor says what submitting does, at the submit button", () => {
  // It said so only inside a help popover. "Moderated before appearing on
  // this site" is not the same as "private until approved", and the
  // difference matters to someone about to put their name on it.
  //
  // Asserted on the region between the Submit button and the help popover, so
  // moving the sentence back inside the popover fails this again.
  const src = readFileSync(join(ROOT, "app.js"), "utf8");
  const start = src.indexOf('id="ed-submit"');
  const end = src.indexOf('id="ed-help-popover"');
  assert.ok(start > 0 && end > start, "editor markup not found as expected");
  // Tags stripped: the notice emphasises "public", which otherwise splits the
  // phrase this looks for and fails on the markup rather than the meaning.
  // Tags stripped and whitespace collapsed: the notice emphasises "public",
  // and the source wraps mid-phrase ("straight\n        away"), so a literal
  // match fails on the formatting rather than on the meaning.
  const atTheAction = src.slice(start, end)
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ");
  assert.match(atTheAction, /public issue tracker/,
    "the notice is not next to the submit button");
  // Two separate facts, asserted separately so shortening the wording cannot
  // quietly drop one: it is public *now*, and review happens *before* it is
  // on the site. The notice was condensed when the editor's action area was
  // found to be larger than the form it submits; both facts survived.
  assert.match(atTheAction, /straight away/,
    "it does not say the submission is public immediately");
  assert.match(atTheAction, /before it appears on the site/,
    "it does not distinguish being filed from being published");
});
