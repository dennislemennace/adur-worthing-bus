/**
 * FlixBus on the page: named, in its own colour, and never given a fleet
 * number it does not have.
 *
 * FlixBus's live feed carries no vehicle id at all, so the API knows a coach
 * by its journey ("FLIX-UK066-…") and says so with `fleet_known: false`.
 * Printed as "fleet FLIX-UK066-1-0700102026-SRBG#VCS-00" it would be nonsense
 * presented as fact.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

const app = loadApp();
const run = (code) => vm.runInContext(code, app);

test("FlixBus is named, not shown as its operator code", () => {
  assert.equal(run('getOperatorName("FLIX")'), "FlixBus");
});

test("FlixBus routes are drawn in FlixBus green", () => {
  assert.equal(run('getLineColour("UK066", "FLIX")'), "#73D700");
});

test("a coach known only by its journey is given no fleet number", () => {
  const html = run(`busTabShellHtml({ vehicle_ref: "FLIX-UK066-1-0700102026-SRBG#VCS-00",
    operator_ref: "FLIX", service_ref: "UK066", fleet_known: false })`);
  assert.doesNotMatch(html, /fleet/i);
  assert.match(html, /Run by FlixBus\./);
});

test("a bus with a fleet number still shows it", () => {
  const html = run(`busTabShellHtml({ vehicle_ref: "15123", operator_ref: "SCSO", service_ref: "700" })`);
  assert.match(html, /fleet 15123/);
});

test("FlixBus coaches are drawn as the green coach, whatever the line", () => {
  for (const line of ["UK066", "UK900", "UK998"]) {
    assert.equal(run(`iconForService("FLIX", ${JSON.stringify(line)})`), "icons/FLIX.png");
  }
});

test("a complaint about a FlixBus coach is pointed at FlixBus's own help pages", () => {
  // The "Help" link in the header and footer of flixbus.co.uk, checked 30 Oct 2026.
  assert.match(run('operatorComplaintLinkHtml("FLIX")'),
    /href="https:\/\/support\.flixbus\.com\/gb\/en"[^>]*>FlixBus<\/a> directly/);
});
