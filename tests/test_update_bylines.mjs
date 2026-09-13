/**
 * Tests for who an article says wrote it.
 *
 * A community report carried "Reported by <name>". A piece written by the
 * project carried nothing at all, because the byline was gated on
 * `opts.community`. So the pieces making the site's arguments were the
 * anonymous ones, on a site whose About page is about being independent and
 * accountable — and a reader had no way to tell a maintainer's analysis from
 * an unattributed notice.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { loadApp } from "./load_app.mjs";

const app = loadApp();

const ARTICLE = {
  id: "a-piece", title: "Fares change in January",
  date: "2026-08-31", topic: "Fares",
  summary: "A summary.", body: "One paragraph.\n\nAnother paragraph.",
};

test("a maintainer article can carry a byline", () => {
  const html = app.updateCardHtml({ ...ARTICLE, author: "Connor R" });
  assert.match(html, /Connor R/,
    "an article with an author renders no name, so the project's own pieces "
    + "are the only unattributed ones on the site");
});

test("a community report still says it was reported, not written", () => {
  // The two are different relationships to a piece and the wording says so:
  // somebody reporting what they saw is not somebody writing an analysis.
  const html = app.updateCardHtml({ ...ARTICLE, name: "Connor R" },
                                  { community: true });
  assert.match(html, /Reported by\s+Connor R/);
});

test("an article with no author named is left unattributed", () => {
  const html = app.updateCardHtml(ARTICLE);
  assert.doesNotMatch(html, /By\s*<|Reported by/,
    "an empty byline was rendered rather than omitted");
});

test("a byline is escaped like every other field", () => {
  const html = app.updateCardHtml({ ...ARTICLE, author: 'A<script>"' });
  assert.doesNotMatch(html, /<script>/, "a byline went in unescaped");
  assert.match(html, /&lt;script&gt;/);
});
