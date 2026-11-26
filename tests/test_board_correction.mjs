/**
 * A board whose times today include our correction to BODS's data says so,
 * once, at its foot (asked for on 26 November 2026: "at the bottom of the
 * board; most users don't care about data source just accuracy").
 *
 * The API names the correction (api/main.py _correction_note); the line is
 * one sentence with a link to why, and nothing at all when there is none.
 *
 * Run with:  node --test "tests/*.mjs"
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { loadApp } from "./load_app.mjs";

const app = loadApp();
const run = (code) => vm.runInContext(code, app);

test("a corrected day's board says so in one line, with a link to why", () => {
  const html = run(`boardCorrectionHtml(${JSON.stringify({
    summary: "BODS's timetable data puts New Year's Eve on 1 December 2026.",
    url: "https://github.com/dennislemennace/adur-worthing-bus/blob/main/docs/reliability/bods-2026-holiday-conversion.md",
  })})`);
  assert.match(html, /^<span>Today's times include a correction we made to the national timetable data\./);
  assert.match(html, /<a href="https:\/\/github\.com\/[^"]+bods-2026-holiday-conversion\.md" target="_blank" rel="noopener noreferrer">Why<\/a>/);
  assert.doesNotMatch(html, /New Year's Eve/, "the story stays behind the link");
});

test("no correction, no line", () => {
  assert.equal(run("boardCorrectionHtml(undefined)"), "");
  assert.equal(run("boardCorrectionHtml({})"), "");
});

test("a link that is not a web address is dropped, not followed", () => {
  const html = run(`boardCorrectionHtml({ summary: "x", url: "javascript:alert(1)" })`);
  assert.doesNotMatch(html, /<a /);
  assert.match(html, /national timetable data\./);
});
