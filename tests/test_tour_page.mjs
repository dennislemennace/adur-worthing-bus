/** tour.html's slides (tour-page.js): the same words as the tour in the app,
 *  one slide per step after an opening, nothing unescaped, no placeholder left
 *  unfilled, the QR and the contact where a reader leaving with the PDF needs
 *  them, and noindex for as long as the tour is a draft. */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";
import { ROOT } from "./load_app.mjs";

const TOUR = JSON.parse(readFileSync(join(ROOT, "data/tour.json"), "utf8"));
const EVIDENCE = JSON.parse(readFileSync(join(ROOT, "data/boundary_evidence.json"), "utf8"));
const INDEX = { days: ["2026-09-16", "2026-10-17"] };
const IDENT = { operators: { BHBC: { tracked: 10, by: { journey_id: 9, start: 0 } } } };

function loadPage() {
  const context = vm.createContext({ console, Intl, Date, Math, Number, String, Object, Array, JSON, RegExp });
  vm.runInContext(readFileSync(join(ROOT, "tour-figures.js"), "utf8"), context);
  vm.runInContext(readFileSync(join(ROOT, "tour-page.js"), "utf8"), context);
  return context;
}

test("an opening slide, then one slide per step, in order", () => {
  const page = loadPage();
  const values = page.tourFigureValues(EVIDENCE, INDEX, IDENT);
  const html = page.tourSlidesHtml(TOUR, values);
  const ids = [...html.matchAll(/data-slide="([a-z0-9-]+)"/g)].map(m => m[1]);
  assert.deepEqual(ids, ["opening", ...TOUR.steps.map(s => s.id)]);
  assert.ok(html.includes(page.tourEsc(TOUR.opening.headline)));
  for (const s of TOUR.steps) assert.ok(html.includes(page.tourEsc(s.title)), s.id);
  assert.doesNotMatch(html, /\{[a-z_]+\}/, "a placeholder reached a slide unfilled");
});

test("the QR and the way to get in touch are where the reader leaves", () => {
  const page = loadPage();
  const html = page.tourSlidesHtml(TOUR, page.tourFigureValues(EVIDENCE, INDEX, IDENT));
  const slides = html.split(/(?=<section class="slide)/).filter(s => s.startsWith("<section"));
  assert.match(slides[0], /qr-tour\.svg/, "no QR on the opening slide");
  assert.match(slides[slides.length - 1], /qr-tour\.svg/, "no QR on the last slide");
  assert.ok(slides[slides.length - 1].includes(page.tourEsc(TOUR.contact.email)), "no contact on the last slide");
  for (const s of TOUR.steps.filter(x => x.sources.some(src => src.url.startsWith("https://")))) {
    const slide = slides.find(x => x.includes(`data-slide="${s.id}"`));
    assert.match(slide, /class="slide-sources"/, `${s.id}: its sources are not printed`);
  }
});

test("what it renders is escaped, and only safe links are links", () => {
  const page = loadPage();
  const doc = JSON.parse(JSON.stringify(TOUR));
  doc.steps[0].title = "<script>x</script>";
  doc.steps[0].sources = [{ label: "x", url: "javascript:alert(1)", checked_on: "2026-10-20" }];
  const html = page.tourSlidesHtml(doc, page.tourFigureValues(EVIDENCE, INDEX, IDENT));
  assert.doesNotMatch(html, /<script>x/);
  assert.doesNotMatch(html, /href="javascript:/i);
});

test("the slides are kept out of search engines while the tour is a draft", () => {
  const html = readFileSync(join(ROOT, "tour.html"), "utf8");
  const noindex = /<meta name="robots" content="noindex"/.test(html);
  assert.equal(noindex, Boolean(TOUR.preview_only),
    TOUR.preview_only ? "a draft tour is open to search engines" : "the approved tour still says noindex");
});
