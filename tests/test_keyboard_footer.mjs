/**
 * The footer steps aside while the on-screen keyboard is up.
 *
 * On a phone the keyboard takes about half the screen, and the footer's
 * attribution was a fixed strip of what was left. While someone is typing in a
 * field the footer is hidden; it comes back when they leave it. Only fields
 * that open a keyboard count: a checkbox or a button taking focus must not
 * make the footer flicker. Whether it renders that way is checked in
 * scripts/browser_check.mjs.
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
const opens = (el) => vm.runInContext("opensKeyboard", app)(el);
const el = (tagName, attrs = {}, extra = {}) => ({
  nodeType: 1, tagName, readOnly: false, disabled: false, isContentEditable: false,
  getAttribute: (k) => (k in attrs ? attrs[k] : null), ...extra,
});

test("text fields open the keyboard", () => {
  assert.equal(opens(el("INPUT", { type: "search" })), true);
  assert.equal(opens(el("INPUT", { type: "text" })), true);
  assert.equal(opens(el("INPUT")), true, "an input with no type is a text field");
  assert.equal(opens(el("INPUT", { type: "email" })), true);
  assert.equal(opens(el("TEXTAREA")), true);
  assert.equal(opens(el("DIV", {}, { isContentEditable: true })), true);
});

test("controls that take focus without a keyboard do not hide the footer", () => {
  for (const type of ["checkbox", "radio", "button", "submit", "range", "color", "file"]) {
    assert.equal(opens(el("INPUT", { type })), false, type);
  }
  assert.equal(opens(el("BUTTON")), false);
  assert.equal(opens(el("SELECT")), false);
  assert.equal(opens(el("INPUT", { type: "text" }, { readOnly: true })), false, "a read-only field opens no keyboard");
  assert.equal(opens(null), false);
});

test("the stylesheet hides the footer only on phone-sized or touch screens", () => {
  const css = readFileSync(join(ROOT, "style.css"), "utf8");
  const rule = /@media[^{]*\(max-width:\s*900px\)[^{]*\{\s*html\.keyboard-open \.site-footer\s*\{\s*display:\s*none;?\s*\}/;
  assert.match(css, rule, "no rule hides the footer while the keyboard is open");
});
