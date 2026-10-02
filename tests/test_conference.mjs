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
  doc.social_venues[0].where = "<script>x</script>";
  const html = app.conferencePanelHtml(doc);
  for (const heading of ["Nearest bus stops", "From Brighton Station", "By coach", "Tickets and paying",
                         "From your hotel", "Getting to the socials", "Taxis", "Accessibility"]) {
    assert.match(html, new RegExp(heading), heading);
  }
  assert.doesNotMatch(html, /<script>x/, "venue text was not escaped");
  assert.match(html, /data-conf-locate/, "no find-my-nearest-stop button");
  assert.match(html, /downhill/);
  assert.match(html, /data-conf-stop="149000006896"/, "the Clock Tower stop is not a live-board button");
  assert.match(html, /Pool Valley/);
  assert.match(html, /Western Road is closed/);
});

test("the guide is offered to everyone while current, with no preview flag", () => {
  const app = loadGuide();
  const data = { show_until: "2026-10-05" };
  assert.equal(app.conferenceOffered(data, new Date("2026-10-02T09:00:00Z")), true);
  assert.equal(app.conferenceOffered(data, new Date("2026-10-06T09:00:00Z")), false, "offered after it ended");
});

test("the locator picks the closest stop first", () => {
  const app = loadGuide();
  const stops = [{ atco: "far", lat: 50.83, lon: -0.14 }, { atco: "near", lat: 50.8212, lon: -0.1462 }];
  const got = app.conferenceNearestStops(stops, 50.8211, -0.1462, 2);
  assert.deepEqual(got.map(s => s.atco), ["near", "far"]);
  assert.ok(got[0].metres < 20);
});

test("route numbers are listed lowest first", () => {
  const app = loadGuide();
  assert.deepEqual(["N700", "12X", "7", "1X", "700", "12", "1", "5B"].sort(app.confRouteOrder),
                   ["1", "1X", "5B", "7", "12", "12X", "700", "N700"]);
});
