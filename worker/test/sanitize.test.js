/**
 * Tests for the Worker's pure logic — the sanitizer and issue builders.
 *
 * These matter more than their size suggests: every string here ends up in a
 * public GitHub issue, so a gap in `sanitize` is a way for a submitter to
 * broadcast notifications or forge maintainer text.
 *
 * Run with:  node --test worker/test/
 * (no dependencies — uses the built-in test runner)
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { _internals } from "../src/index.js";

const { sanitize, slugify, buildIssue, blockquote, isAllowedOrigin } = _internals;

const ZWSP = "​";

// ── sanitize ────────────────────────────────────────────────

test("sanitize defuses @mentions so an issue can't ping people", () => {
  const out = sanitize("cc @everyone and @some-org/team", 500);
  assert.ok(!/@(?=[A-Za-z0-9-])/.test(out), "no bare @mention should survive");
  assert.equal(out, `cc @${ZWSP}everyone and @${ZWSP}some-org/team`);
});

test("sanitize defuses #123 issue cross-references", () => {
  const out = sanitize("see #42", 500);
  assert.equal(out, `see #${ZWSP}42`);
});

test("sanitize strips ATX headings so text can't forge structure", () => {
  const out = sanitize("## Maintainer note\nreal text", 500);
  assert.equal(out, "Maintainer note\nreal text");
});

test("sanitize breaks code fences so a body can't escape its block", () => {
  const out = sanitize("```json\n{}\n```", 500);
  assert.ok(!out.includes("```"), "no intact triple-backtick fence should survive");
});

test("sanitize escapes HTML angle brackets", () => {
  const out = sanitize("<img src=x onerror=alert(1)>", 500);
  assert.ok(!out.includes("<"));
  assert.ok(out.includes("&lt;"));
});

test("sanitize strips control characters but keeps newlines and tabs", () => {
  const out = sanitize("a\u0000b\u001fc\nd\te", 500);
  assert.equal(out, "abc\nd\te");
});

test("sanitize collapses runs of blank lines", () => {
  assert.equal(sanitize("a\n\n\n\n\nb", 500), "a\n\nb");
});

test("sanitize truncates to the cap", () => {
  assert.equal(sanitize("x".repeat(300), 120).length, 120);
});

test("sanitize handles null and undefined", () => {
  assert.equal(sanitize(null, 100), "");
  assert.equal(sanitize(undefined, 100), "");
});

// ── slugify ─────────────────────────────────────────────────

test("slugify produces a safe id and never returns empty", () => {
  assert.equal(slugify("Bring back the 60!"), "bring-back-the-60");
  assert.equal(slugify("!!!"), "idea");
});

// ── blockquote ──────────────────────────────────────────────

test("blockquote prefixes every line", () => {
  assert.equal(blockquote("one\ntwo"), "> one\n> two");
});

// ── buildIssue: ideas ───────────────────────────────────────

test("an idea builds a labelled issue with a publishable JSON blob", () => {
  const built = buildIssue({
    kind: "idea",
    title: "Later buses on the 700",
    details: "Sundays finish too early.",
    area: "More frequency",
    name: "Sam",
  });
  assert.equal(built.title, "Idea: Later buses on the 700");
  assert.deepEqual(built.labels, ["community-submission", "idea", "unverified"]);
  assert.ok(built.body.includes("Sundays finish too early."));
  assert.ok(built.body.includes('"status": "published"'));
  assert.ok(built.body.includes("add_suggestion.py"));
});

test("issue bodies keep the blank lines markdown needs between blocks", () => {
  // filter(Boolean) here would silently strip the deliberate "" separators
  // and render the whole issue as one run-on paragraph.
  const built = buildIssue({
    kind: "idea", title: "T", details: "D", area: "Fares",
  });
  assert.ok(built.body.includes("\n\n"), "body should contain blank lines");
  assert.ok(
    /\n\n### The idea\n\n/.test(built.body),
    "headings should be surrounded by blank lines",
  );
});

test("a null conditional row is dropped without leaving a blank gap", () => {
  // `objective` is omitted here, so its row is null and must vanish entirely.
  const built = buildIssue({ kind: "idea", title: "T", details: "D" });
  assert.ok(!built.body.includes("Related objective"));
  assert.ok(!/\n\n\n/.test(built.body), "no triple newline should survive");
});

test("an idea missing its details is rejected", () => {
  assert.throws(
    () => buildIssue({ kind: "idea", title: "Just a title" }),
    /more details/,
  );
});

test("an idea missing its title is rejected", () => {
  assert.throws(
    () => buildIssue({ kind: "idea", details: "lots of detail" }),
    /one-line summary/,
  );
});

test("a submitted email address never reaches the issue body", () => {
  // The form no longer collects one, but a hand-crafted POST might still
  // send it. Public issue + private address is the mistake to prevent.
  const built = buildIssue({
    kind: "idea",
    title: "Test",
    details: "Body text",
    email: "someone@example.com",
  });
  assert.ok(!built.body.includes("example.com"));
});

// ── buildIssue: proposals ───────────────────────────────────

test("a proposal with valid JSON builds an issue", () => {
  const built = buildIssue({
    kind: "proposal",
    title: "98X",
    proposalJson: JSON.stringify({ id: "98x", name: "98X" }),
  });
  assert.equal(built.title, "Proposal: 98X");
  assert.ok(built.labels.includes("proposal"));
  assert.ok(built.body.includes('"98x"'));
});

test("a proposal with unparseable JSON is rejected", () => {
  assert.throws(
    () => buildIssue({ kind: "proposal", title: "Broken", proposalJson: "{not json" }),
    /not valid JSON/,
  );
});

test("an oversized proposal is rejected", () => {
  assert.throws(
    () => buildIssue({ kind: "proposal", title: "Huge", proposalJson: "x".repeat(7000) }),
    /too large/,
  );
});

// ── buildIssue: stop issues ─────────────────────────────────

test("a stop issue carries the ATCO as its dedupe key", () => {
  const built = buildIssue({
    kind: "stop_issue",
    stopName: "Cuthbert Road",
    atco: "149000007413",
    category: "shelter",
    details: "Shelter glass is smashed.",
    lat: 50.825431,
    lon: -0.121387,
  });
  assert.equal(built.dedupeKey, "149000007413");
  assert.ok(built.title.includes("Cuthbert Road"));
  assert.ok(built.title.includes("149000007413"));
  assert.ok(built.labels.includes("stop-issue:shelter"));
  assert.ok(built.body.includes("openstreetmap.org"));
  // The report is a record, not a works order — say so.
  assert.ok(/West Sussex County Council/.test(built.body));
});

test("a stop issue with an unknown category is rejected", () => {
  assert.throws(
    () => buildIssue({
      kind: "stop_issue", stopName: "X", atco: "1", details: "d", category: "nonsense",
    }),
    /Unknown issue category/,
  );
});

test("a stop issue without coordinates still builds", () => {
  const built = buildIssue({
    kind: "stop_issue", stopName: "X", atco: "440000001", details: "d", category: "other",
  });
  assert.ok(!built.body.includes("openstreetmap.org"));
});

// ── dispatch ────────────────────────────────────────────────

test("an unknown submission kind is rejected", () => {
  assert.throws(() => buildIssue({ kind: "wat" }), /Unknown submission type/);
  assert.throws(() => buildIssue({}), /Unknown submission type/);
});

// ── origin allowlist ────────────────────────────────────────

test("origin allowlist admits only listed origins", () => {
  const allowed = ["https://dennislemennace.github.io"];
  assert.equal(isAllowedOrigin("https://dennislemennace.github.io", allowed), true);
  assert.equal(isAllowedOrigin("https://evil.example", allowed), false);
  assert.equal(isAllowedOrigin("", allowed), false);
});

test("an empty allowlist is permissive, for local dev only", () => {
  assert.equal(isAllowedOrigin("https://anything.example", []), true);
});

// ── fileIssue: 422 fallback ─────────────────────────────────
//
// GitHub 422s the whole request if a label doesn't exist; it does not create
// them. A label renamed later would otherwise turn every submission into a
// 502 with no obvious cause. Dropping the labels is recoverable by hand.

const { fileIssue } = _internals;

/** Stub global fetch with a queue of [status, body] responses, recording calls. */
function stubFetch(responses) {
  const calls = [];
  const original = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    calls.push({ url: String(url), body: init.body ? JSON.parse(init.body) : null });
    const [status, body] = responses.shift() ?? [500, {}];
    return {
      ok: status >= 200 && status < 300,
      status,
      json: async () => body,
      text: async () => JSON.stringify(body),
    };
  };
  return { calls, restore: () => { globalThis.fetch = original; } };
}

const ENV = { GITHUB_TOKEN: "t", GITHUB_REPO: "o/r" };
const BUILT = { title: "T", body: "B", labels: ["idea", "unverified"] };

test("a labelled issue is filed in one call when the labels exist", async () => {
  const { calls, restore } = stubFetch([[201, { number: 7 }]]);
  try {
    const res = await fileIssue(ENV, BUILT);
    assert.equal(res.number, 7);
    assert.equal(calls.length, 1);
    assert.deepEqual(calls[0].body.labels, ["idea", "unverified"]);
  } finally { restore(); }
});

test("a 422 retries without labels rather than losing the submission", async () => {
  const { calls, restore } = stubFetch([
    [422, { message: "Validation Failed" }],
    [201, { number: 9 }],
  ]);
  try {
    const res = await fileIssue(ENV, BUILT);
    assert.equal(res.number, 9, "the submission must still land");
    assert.equal(res.labelsDropped, true);
    assert.equal(calls.length, 2);
    assert.deepEqual(calls[0].body.labels, ["idea", "unverified"]);
    assert.ok(!("labels" in calls[1].body), "the retry must omit labels entirely");
    assert.equal(calls[1].body.title, "T");
  } finally { restore(); }
});

test("a non-422 error is not retried and still fails", async () => {
  // 401 means the token is wrong. Retrying without labels would not help and
  // would double the damage against the rate limit.
  const { calls, restore } = stubFetch([[401, { message: "Bad credentials" }]]);
  try {
    await assert.rejects(() => fileIssue(ENV, BUILT), /401/);
    assert.equal(calls.length, 1, "must not retry");
  } finally { restore(); }
});
