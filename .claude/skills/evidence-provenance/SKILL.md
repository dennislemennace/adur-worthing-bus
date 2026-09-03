---
name: evidence-provenance
description: How to publish a derived statistic on this advocacy site so it can be checked and defended. Use whenever a number that was computed rather than cited is about to appear in the UI, in data/, in a Network Updates article, or in anything sent to a councillor, officer or journalist.
---

# Publishing a number you worked out yourself

This site already handles *cited* facts well: every fare carries a `source_url`
and a `checked_on`, and `pytest` refuses a price without them. It has no
equivalent discipline for figures the site **derives from its own data** — and
those are the ones a hostile reader will attack, because there is no third party
to blame if they are wrong.

An advocacy site trades on being checkable. A number without its method is worth
less than no number at all, because it invites the reply that ends the
conversation: *where did you get that?*

## The rule

**Every derived statistic ships with five things.** No exceptions, including for
figures that seem obvious.

| Field | What it answers |
|---|---|
| `value` | the number itself, with its unit and denominator named |
| `method` | how it was computed, in enough detail to reproduce |
| `data_version` | which build of the source data it came from |
| `as_of` | the date it was computed |
| `caveats` | what would make it wrong, and which way any bias runs |

Store them together. A number in one file and its method in a comment somewhere
else will come apart the first time either is edited.

## Name the denominator in the value

"Service is 34% worse" is not a claim, it is a mood. "Weekday departures **per
stop** are 189.3 west of the line against 284.8 east" can be checked, argued with,
and defended.

Ratios especially: say what is being divided by what, in the label, every time.

## State which way the bias runs

This is the field that earns trust, and the one that is always omitted.

> `scripts/build_timetable.py` keeps a route only if it touches a West Sussex
> stop or sits on a hand-maintained allowlist, so routes running purely inside
> Brighton & Hove are missing. **The east side of every comparison is therefore
> under-counted, and the real gap is wider than measured.**

A caveat that weakens your own case, stated first and unprompted, is worth more
than the statistic it qualifies. A reader who finds it themselves stops believing
everything else.

Where a bias helps your argument, say so explicitly rather than letting it look
accidental. Where it hurts, say so anyway.

## Compare like with like, and say how you made it so

A raw comparison of two areas is almost always confounded — different population,
different density, different geography. Constrain the comparison and state the
constraint in the `method`:

> Stops between latitude 50.818 and 50.855, within 0.057° longitude (~4 km) of the
> council line at −0.216, bucketed east or west of it.

If the constraint were removed the numbers would change. That is not a weakness,
provided the constraint is published.

## Stale figures are the main failure mode

Derived numbers move whenever the source data is rebuilt — here, weekly, by
`.github/workflows/update-timetable.yml`.

- **Recompute in the pipeline, not by hand.** A figure recomputed by a script that
  runs on every rebuild cannot drift. A figure typed into a JSON file will.
- **Show `as_of` in the UI wherever the number appears**, the same way fares show
  `checked ${checked_on}`.
- **Fail loudly rather than quietly.** If `as_of` is older than the timetable
  build, that is a bug worth surfacing, not a cosmetic detail.

## Charts must not overstate what the number says

If a derived figure gets a chart:

- **Bar charts start at zero.** A truncated axis turns 66% into a visual wipeout,
  and it is the single fastest way to lose an argument you were winning.
- **Encode with more than colour.** A value carried by hue alone fails WCAG 1.4.1
  and is invisible to a good fraction of readers. Add a number, a label or a
  pattern.
- **Show the denominator on the chart**, not only in the caption.
- **No pie charts for ratios between two independent quantities.** West and east
  departures are not parts of one whole.

## Before it goes to a person rather than the web

For anything sent to a councillor, an officer or a journalist, add two things a
public page does not need:

- **The query or script that produced it**, verbatim, in an appendix. Officers
  have analysts, and handing them the method converts a challenge into a check.
- **What would change your mind.** Naming the evidence that would refute you is
  the difference between advocacy and lobbying, and officers can tell.

## Checklist

Before any derived number ships:

- [ ] denominator named in the label, not just the caption
- [ ] method reproducible from what is written down
- [ ] `data_version` and `as_of` recorded and rendered
- [ ] direction of every known bias stated
- [ ] recomputed by the pipeline, not typed in
- [ ] chart, if any, zero-based and not colour-only
- [ ] a schema test in `tests/test_curated_data.py` covering the fields above
