# adur-worthing-bus — working instructions

## Efficient verification

- Preserve correctness and useful regression coverage; reduce repeated work, not assurance.
- Before implementation, briefly identify the affected components and the smallest useful validation plan. Do not launch a full audit for a small task.
- During iteration, run the affected test or test file. For UI changes, use focused browser probes on the affected views, themes and viewport sizes; batch related CSS changes before wider checks.
- Prove each new bug-regression check fails for the intended reason before trusting its passing result. Do this once, preferably before fixing the bug. Do not repeatedly undo and restore a working fix, or disturb unrelated working-tree changes.
- Apply the existing browser-verification convention per coherent change batch: run `scripts/browser_check.mjs` before declaring rendered changes verified, not after every individual edit. It currently has no test-filter option; do not invent one.
- For substantial cross-component work, run the complete relevant suites once before final handoff. For small isolated changes, use proportionate affected checks rather than automatically rerunning every suite.
- After final verification, repeat only the checks invalidated by later changes or needed to diagnose failures. Briefly explain the new risk before an additional full browser run; repeat broader verification when shared behaviour genuinely warrants it.
- Do not run application suites for prose/instructions-only edits unless executable examples or tooling behaviour are affected.
- Reuse audit-owned browser/server sessions. Stop only processes started for this task; avoid broad process-name kills.
- Keep successful test output concise: totals, skips and meaningful warnings. Retain useful failure diagnostics. Do not dump every passing assertion or repeatedly reread unchanged files.
- Check `LIMITS.md` before quota-consuming verification. Prefer local fixtures and mocked upstreams; never make public submissions or send councillor emails as routine tests.

## Accurate completion reporting

- Distinguish **implemented**, **verified locally**, **verified in deployment**, and **blocked/deferred**. Passing tests do not establish production readiness.
- Track all requested scope, including unnumbered design recommendations and release prerequisites. Do not report every finding complete while applicable data rebuilds, permissions, deployment checks or owner decisions remain outstanding.
- End with a concise summary of changes, validation actually performed, skipped checks and remaining release requirements. Do not invent test results or hide unresolved failures.

## Project conventions

- Read `.claude/skills/bus-site-conventions/SKILL.md` when working in its scope, and `.claude/skills/evidence-provenance/SKILL.md` for published evidence. The verification guidance above clarifies cadence, not weaker acceptance criteria.
- Consult `docs/RUNBOOK.md` for current operations. Treat `CODE_CONTEXT_SNAPSHOT.md` and historical plans as context to verify against current code, not authoritative current state.
- Preserve unrelated edits. Do not commit, push, deploy or perform a full timetable rebuild unless requested.
