/**
 * Tests for what happens when someone sends something in.
 *
 * Submissions used to become public GitHub issues the moment they were sent,
 * under this project's account, before anyone had read them. They now go to a
 * private inbox, and every form carries a box the sender ticks to confirm they
 * understand approved submissions are published with the name they give.
 *
 *   * nothing is sent without the box, and the reader is put back on it,
 *   * the box is sent to the Worker, which checks it too, and
 *   * the thank-you message says the submission is private, and no longer
 *     links to an issue a reader could not open.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

function fresh() {
  const app = loadApp();
  const CONFIG = vm.runInContext("CONFIG", app);
  CONFIG.SUBMIT_ENDPOINT = "https://submit.example/";
  const posted = [];
  app.fetch = async (url, init) => {
    posted.push(JSON.parse(init.body));
    return { ok: true, status: 200, json: async () => ({ ok: true, number: 12 }) };
  };
  app.turnstileToken = () => "token";
  return { app, CONFIG, posted, dom: vm.runInContext("dom", app) };
}

/** A form with a publication box, ticked or not, that records focus. */
function formWith(ticked, fields = {}) {
  const box = { checked: ticked, focused: false, focus() { this.focused = true; } };
  const inputs = Object.fromEntries(Object.entries(fields).map(([id, value]) => [id, { value, focus() {} }]));
  return {
    box,
    reset() {},
    querySelector(sel) {
      if (sel === 'input[name="publishAck"]') return box;
      return inputs[sel] || null;
    },
  };
}

test("without the box ticked nothing is sent, and the reader is told why", async () => {
  const { app, posted } = fresh();
  const result = await app.postSubmission("idea", { title: "t", details: "d" });
  assert.equal(result.ok, false);
  assert.equal(result.ack, true);
  assert.match(result.reason, /tick the box/);
  assert.deepEqual(posted, [], "an unacknowledged submission was sent");
});

test("with the box ticked the acknowledgement travels to the Worker", async () => {
  const { app, posted } = fresh();
  const result = await app.postSubmission("idea", { title: "t", details: "d", publishAck: true });
  assert.equal(result.ok, true);
  assert.equal(posted.length, 1);
  assert.equal(posted[0].publishAck, true);
});

test("the idea form sends its box, and an unticked box puts the reader back on it", async () => {
  const { app, dom, posted } = fresh();
  const statuses = [];
  app.setSuggestStatus = (m) => statuses.push(m);
  app.setSuggestBusy = () => {};
  app.resetTurnstile = () => {};
  const unticked = formWith(false, { "#sg-title": "Later buses", "#sg-details": "Please",
    "#sg-area": "", "#sg-objective": "", "#sg-name": "" });
  dom.suggestForm = unticked;
  await app.submitSuggestion();
  assert.deepEqual(posted, []);
  assert.equal(unticked.box.focused, true, "the reader was not taken to the box");
  assert.match(statuses.at(-1), /tick the box/);

  const ticked = formWith(true, { "#sg-title": "Later buses", "#sg-details": "Please",
    "#sg-area": "", "#sg-objective": "", "#sg-name": "" });
  dom.suggestForm = ticked;
  dom.suggestStatus = { classList: { remove() {} }, textContent: "", innerHTML: "" };
  await app.submitSuggestion();
  assert.equal(posted.length, 1);
  assert.equal(posted[0].publishAck, true);
  assert.match(dom.suggestStatus.textContent, /received privately/);
  assert.doesNotMatch(dom.suggestStatus.innerHTML, /<a /, "the receipt still links somewhere");
});

test("the stop report sends its box", async () => {
  const { app, dom, posted } = fresh();
  const state = vm.runInContext("state", app);
  state.selectedStop = { atcoCode: "4400AD0203", stopName: "High Street" };
  state.stopData = { "4400AD0203": { lat: 50.8, lon: -0.27 } };
  app.setReportStopStatus = () => {};
  app.resetTurnstile = () => {};
  dom.reportStopSubmit = null;
  dom.reportStopForm = formWith(true, { "#rs-details": "Shelter glass broken",
    "#rs-category": "shelter", "#rs-name": "" });
  await app.submitStopIssue();
  assert.equal(posted.length, 1);
  assert.equal(posted[0].publishAck, true);
});

test("no thank-you message links to the private issue", () => {
  const { app } = fresh();
  const html = app.submissionReceiptHtml({ ok: true, number: 12, url: "https://github.com/x/inbox/issues/12" });
  assert.doesNotMatch(html, /<a |github\.com/);
  assert.match(html, /received privately for review/);
});
