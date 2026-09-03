# The case this site is making

The reference for *why* this project exists. Everything else in the repo — the
objectives, the ticket-boundary calculator, the council border on the map, the
Network Updates articles — argues some part of what is written down here.

Written from the maintainer's brief, 1 September 2026. Where a claim can be
measured against `data/timetable.sqlite`, the measurement is given and the query
described, so a reader can check it rather than take it on trust.

---

## 1. The aim, in one sentence

**Improve bus services westward of Brighton**, along the coast into Adur and
Worthing, where the network thins out at a line that has nothing to do with how
people actually travel.

---

## 2. The structural problem

The **Brighton & Hove City Council / West Sussex County Council boundary runs
through Portslade**. Crossing it westward, there is a considerable — and
measurable — reduction in bus service.

The primary cause is not geography or demand. It is that **WSCC and BHCC do not
co-operate on their Bus Service Improvement Plans**. Each authority plans
independently, so the network is designed twice, up to a line, from both sides.
Nothing is designed *across* it.

Everything in sections 3 to 6 follows from that.

---

## 3. What the timetable actually shows

Measured on the GTFS timetable in this repo, taking a band **4 km either side of
the council line** along the same coastal strip (latitude 50.818–50.855), so the
two sides are genuinely like for like. Figures are for a dated week — Monday
7 September 2026 and the Saturday and Sunday following — because a GTFS feed
describes one real bus with several calendars, and "any Monday" is not a
question the data can answer.

| | West of the line (WSCC) | East (BHCC) | West as % of east |
|---|---:|---:|---:|
| Stops in band | 180 | 320 | |
| Weekday departures per stop | **64.4** | **86.7** | **74 %** |
| Weekday routes | 15 | 18 | 83 % |
| Saturday routes | **11** | **18** | 61 % |
| Sunday departures per stop | 36.7 | 50.5 | 73 % |
| Sunday routes | **9** | **16** | **56 %** |
| Weekday departures after 23:00 | 382 | 929 | 41 % |

**Service is about a quarter thinner west of the line on every day of the week.
Route choice holds up on a weekday and then collapses at the weekend** — from 15
routes to 11 on a Saturday and 9 on a Sunday, against 18 and 16 in Brighton.

The Sunday list is the clearest statement of the problem. West of the line:

> 025, 1, 1X, 2, 2B, 46, 700, N1, N700

Six of those nine are Brighton & Hove services passing through, one is the
Coastliner, and two are night buses. East of the line there are sixteen,
including 5, 5B, 6, 7, 21, 47 and 3X — a network rather than a corridor.

Local Shoreham and Southwick services drop out entirely at the weekend, which is
what the route counts above are measuring.

### An important caveat, and which way it cuts

`scripts/build_timetable.py` keeps a route if it touches a West Sussex (ATCO
`4400`) stop, or if it appears on a hand-maintained allowlist of cross-boundary
services. A route that runs **only** within Brighton & Hove and is not on that
list is therefore absent from this database.

So the east side of every figure above is **under-counted**. The real gap is
wider than the table shows, not narrower. The bias runs against the finding.

### A correction worth recording

The first version of these figures reported a 34 % gap and 11 weekday routes
west of the line. Both were wrong. They counted a trip once for every calendar
that listed its weekday, and this feed carries 112 calendars over 14 date
ranges — term time, holidays, seasonal variants — so one bus was counted several
times. The inflation was **not** uniform between the two sides, so it did not
cancel in the ratio: one Portslade stop fell by 20 % once corrected while
Lancing did not move at all.

Figures are now measured against dated days, with `calendar_dates` exceptions
applied. `Timetable.runs_on` is the shared test.

---

## 4. Operators and ticketing

### Only a handful of Brighton & Hove services cross

West of the boundary, Brighton & Hove runs a small number of routes. The other
main operator is **Stagecoach**.

### Stagecoach splits its ticketing zone at Shoreham

The DayRider zone boundary falls in the middle of a continuous built-up coastal
strip, **dividing communities that are otherwise one place**.

### The Lancing trap

This is the sharpest illustration of the whole problem.

A passenger in **Lancing** travelling to **Brighton** buys a Stagecoach day
ticket. In Brighton that ticket is **essentially useless**: Stagecoach has only
one other service going north from the city, so the ticket buys them the journey
in and almost nothing once they arrive.

They cannot solve it by switching operator either — **no Brighton & Hove buses
serve Lancing** or points west of it. So unless the destination is the very city
centre, the passenger is stuck: buy a second operator's ticket, or don't go.

That is not a fares problem. It is what happens when two networks are planned to
a line instead of across it.

---

## 5. Service quality west of the line

In the West Sussex areas Brighton & Hove *does* serve, the service is
significantly reduced against the same operator's Brighton provision:

- **No night bus service.**
- **Only one route that is not very slow** for a commuter.

The services covering the niche areas around **Shoreham and Southwick**:

- run **infrequently**
- run **at peak hours only**
- have **limited or no weekend service** — confirmed above: 60 gone on
  Saturdays, 9 and 19 gone on Sundays

---

## 6. The missing connections

There is **no service, or no simple route**, connecting Worthing with:

- North Southwick
- Mile Oak
- Hangleton
- North Portslade
- Moulsecoomb
- The universities
- Falmer

Commuters on these corridors are left with slow options, or buying tickets from
more than one operator. **There are no express buses that cross the WSCC/BHCC
boundary at all.**

---

## 7. Infrastructure

Bus stop and shelter quality is **patchy**.

The specific ask: install **low-cost, low-power real-time information displays**
using technology such as **e-ink**, so live departure information does not depend
on a mains-powered screen at every stop.

---

## 8. How this maps onto `data/objectives.json`

Eight objectives, four featured. Most of the case above is already represented.

**Covered:**

| Objective | Covers |
|---|---|
| `integrated-sussex-plan` | §2 — the BSIP non-co-operation, the root cause |
| `express-commuter-routes` | §6 — no express services across the boundary |
| `fairer-ticketing-zones` | §4 — the Shoreham DayRider split |
| `cross-operator-tickets` | §4 — the Lancing trap's other half |
| `extend-existing-routes` | §5 — peak-only and infrequent services |
| `night-service-west` | §5 — no night service west of the line |
| `real-time-info-everywhere` | §7 — e-ink displays, already named explicitly |
| `accessible-buses-stops` | audio-visual announcements |

**Gaps — in the brief but not yet an objective:**

1. **The named missing corridors** (§6). `express-commuter-routes` covers
   Worthing–Brighton, but not North Southwick, Mile Oak, Hangleton, North
   Portslade, Moulsecoomb, the universities or Falmer. These are orbital and
   northward links, not a faster coastal run, and they are a different ask.
2. **Weekend service specifically** (§5). `extend-existing-routes` is framed
   around earlier mornings and later evenings. The measured Saturday and Sunday
   route losses are a distinct and more concrete claim.
3. **Shelter condition** (§7). Covered for *information*, not for the physical
   state of stops and shelters.
4. **The Lancing trap as written evidence** (§4). It is the single clearest
   story the campaign has, and it currently lives in neither the objectives nor
   the Network Updates feed.

---

## 9. Reproducing the figures

The measurements in §3 come from `data/timetable.sqlite`, built by
`scripts/build_timetable.py` from the BODS South East GTFS bundle.

Method: take stops between latitude 50.818 and 50.855 within 0.057° longitude
(~4 km) of the council line at longitude −0.216; bucket them east or west of that
line; join `stop_times` to `trips`, keeping a trip only when its service actually
runs on the dated day being measured; count departures and distinct
`routes.short_name` per bucket; divide by stop count.

Recomputed by `scripts/build_evidence.py` on every timetable rebuild, into
`data/boundary_evidence.json`, which the site reads directly.

Re-run after any timetable rebuild before quoting the numbers publicly — they
move with the published schedule, and a stale figure is worse than none.
