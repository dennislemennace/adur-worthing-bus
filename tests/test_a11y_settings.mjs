/**
 * Tests for the accessibility settings: text size, reduce motion and a
 * colour-blind-safe palette.
 *
 * The rendering side (does 140% text still fit, does the dialog open from a
 * hold on the theme button) is in scripts/browser_check.mjs. What is here is
 * the part a browser run cannot see clearly: that a stored value from an old
 * version, a hand-edited one or a broken one comes back as something safe.
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
const plain = (v) => JSON.parse(JSON.stringify(v));
const norm = (raw) => plain(vm.runInContext(`normaliseA11y(${JSON.stringify(raw)})`, app));
const INDEX = readFileSync(join(ROOT, "index.html"), "utf8");

test("nothing stored means every setting at its default", () => {
  assert.deepEqual(norm({}), { textScale: 1, reduceMotion: false, cvd: false });
  assert.deepEqual(norm(null), { textScale: 1, reduceMotion: false, cvd: false });
});

test("a text size that is not one of the offered steps is ignored", () => {
  // A hand-edited 3 would make the site unusable, and a stored "1.2" string
  // from an older version is still the Larger step.
  assert.equal(norm({ textScale: 3 }).textScale, 1);
  assert.equal(norm({ textScale: "1.2" }).textScale, 1.2);
  assert.equal(norm({ textScale: 1.4 }).textScale, 1.4);
});

test("the switches only turn on for a real true", () => {
  assert.equal(norm({ reduceMotion: "false" }).reduceMotion, true === "false");
  assert.equal(norm({ reduceMotion: true, cvd: true }).cvd, true);
});

test("the setting reduces motion even when the device does not ask for it", () => {
  const state = vm.runInContext("state", app);
  state.a11y = { textScale: 1, reduceMotion: true, cvd: false };
  assert.equal(vm.runInContext("motionReduced()", app), true);
  state.a11y = { textScale: 1, reduceMotion: false, cvd: false };
  assert.equal(vm.runInContext("motionReduced()", app), false,
    "with no setting and no device preference, motion should be left alone");
});

test("colour-blind mode swaps the two sides of the council boundary to blue and orange", () => {
  // Navy against purple, the normal pair, is close to indistinguishable for
  // the most common kinds of colour blindness.
  const state = vm.runInContext("state", app);
  state.a11y = { textScale: 1, reduceMotion: false, cvd: false };
  const normal = [vm.runInContext('bodyColour("WSCC")', app), vm.runInContext('bodyColour("BHCC")', app)];
  state.a11y = { textScale: 1, reduceMotion: false, cvd: true };
  const safe = [vm.runInContext('bodyColour("WSCC")', app), vm.runInContext('bodyColour("BHCC")', app)];
  assert.notDeepEqual(safe, normal, "colour-blind mode left the boundary colours unchanged");
  assert.deepEqual(safe, ["#0072b2", "#a15c00"]);
});

test("the settings are applied before first paint, in the head", () => {
  // Otherwise a reader at 140% sees the page jump from normal size on every load.
  const head = INDEX.slice(0, INDEX.indexOf("</head>"));
  assert.match(head, /localStorage\.getItem\(['"]a11y['"]\)/,
    "the pre-paint script does not read the accessibility settings");
  assert.match(head, /a11y-reduce-motion/);
  assert.match(head, /a11y-cvd/);
});

test("the menu has a way in that is not a hidden gesture", () => {
  // Holding the theme button is a shortcut. It cannot be done by keyboard,
  // switch access or most screen readers, who are the people the menu is for.
  assert.match(INDEX, /id="a11y-footer-link"/, "no visible link to the settings");
  assert.match(INDEX, /id="a11y-btn"/, "no header button for the settings");
  assert.match(INDEX, /<dialog[^>]*id="a11y-dialog"/, "the settings are not a dialog");
});

test("both footers link to the accessibility settings, and both links work", () => {
  // Phones get a compact footer, not the full one. The full footer carried the
  // link and the compact one did not, so on the devices where the hold gesture
  // is the other way in, the visible route was missing.
  const footer = INDEX.slice(INDEX.indexOf('<footer class="site-footer">'), INDEX.indexOf("</footer>"));
  const compact = footer.slice(footer.indexOf('class="site-footer-compact"'));
  assert.match(compact, /data-open-a11y[^>]*>Accessibility</,
    "the phone footer has no accessibility link");
  const full = footer.slice(0, footer.indexOf('class="site-footer-compact"'));
  assert.match(full, /data-open-a11y[^>]*>Accessibility</, "the full footer lost its link");
  // Bound by attribute, not by one id: a second link with no handler would
  // follow its #accessibility href and do nothing visible.
  const src = readFileSync(join(ROOT, "app.js"), "utf8");
  assert.match(src, /querySelectorAll\("\[data-open-a11y\]"\)/,
    "the links are not all wired to open the dialog");
});
