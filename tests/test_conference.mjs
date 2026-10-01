/** The conference guide (conference.js): offered only while current, and
 *  rendered from its data with every value escaped. */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { loadApp, ROOT } from "./load_app.mjs";

const DATA = JSON.parse(readFileSync(join(ROOT, "data/conference.json"), "utf8"));

function loadGuide() {
  const app = loadApp();
  vm.runInContext(readFileSync(join(ROOT, "conference.js"), "utf8").replace(/^conferenceInitMenu\(\);$/m, ""), app);
  return app;
}

test("the guide is offered until its show_until date, by London's calendar", () => {
  const app = loadGuide();
  const active = (iso) => app.conferenceActive({ show_until: "2026-10-05" }, new Date(iso));
  assert.equal(active("2026-10-05T22:30:00Z"), true, "23:30 in London on the last day");
  assert.equal(active("2026-10-05T23:30:00Z"), false, "after midnight in London");
  assert.equal(app.conferenceActive(null), false);
});

test("the panel covers every section, and escapes what it is given", () => {
  const app = loadGuide();
  const doc = JSON.parse(JSON.stringify(DATA));
  doc.socials[0].event = "<script>x</script>";
  const html = app.conferencePanelHtml(doc);
  for (const heading of ["Nearest bus stops", "Getting here", "Tickets and paying", "From your hotel",
                         "Evenings and socials", "Accessibility"]) {
    assert.match(html, new RegExp(heading), heading);
  }
  assert.doesNotMatch(html, /<script>x/, "event text was not escaped");
  assert.match(html, /data-conf-stop="149000006896"/, "the Clock Tower stop is not a live-board button");
  assert.match(html, /Pool Valley/);
  assert.match(html, /Western Road is closed/);
});

test("the guide is offered only with the preview flag", () => {
  const app = loadGuide();
  const data = { show_until: "2026-10-05" };
  const during = new Date("2026-10-02T09:00:00Z");
  assert.equal(app.conferenceOffered(data, during, true), true);
  assert.equal(app.conferenceOffered(data, during, false), false, "offered without ?preview=1");
});
