# Worthing Brighton Bus

**Live passenger information and service performance measurement for the
Sussex coast, built entirely from open data. It is open source and runs on
free hosting.**

<https://worthingbrightonbus.co.uk>

Worthing Brighton Bus brings every operator on the Adur, Worthing and Brighton
corridor into one place, for passengers and the people who plan their buses:
Brighton & Hove, Metrobus, Stagecoach South, Compass Travel, National Express
and FlixBus, with National Rail alongside. It does two jobs from the same data:

1. **Passenger information:** a live map, a departure board for any stop, route
   and fares information, and a journey planner. Each time says whether it is
   live, estimated or timetabled.
2. **Performance measurement:** the BODS location feed, recorded every minute
   and matched to the timetable. From that it publishes punctuality at timing
   points, excess waiting time, bunching, running times and where time is lost,
   route by route.

Every published figure states its denominator, method, date and caveats, and
the evidence behind it is archived in public. The project is independent and
run by a volunteer. It is not run by, funded by or endorsed by any operator or
authority.

A guided tour of the platform is at
[worthingbrightonbus.co.uk/tour.html](https://worthingbrightonbus.co.uk/tour.html),
with a printable PDF in [`media/tour/`](./media/tour/).

---

## Passenger information

**One live map for every operator.**
- **Positions:** buses from the Bus Open Data Service (SIRI-VM and GTFS-RT),
  refreshed every 20 seconds. Each bus is drawn in its own livery, using the
  vehicle list kept by bustimes.org.
- **Other modes:** FlixBus coaches from FlixBus's own GTFS-RT feed, National
  Express through BODS, trains and platforms from Realtime Trains, and rail
  replacement stops from the train operator's own posters.
- **Tap a bus** to see the stops ahead with an estimate at each, the stop it
  has just passed and when it left it, its fleet number and model, and its
  route's registration history.

**A departure board for any stop, across operators.** Every row carries one of
three labels, and they are never mixed:
- **Live:** the operator's own prediction (through TransportAPI).
- **Estimated:** our estimate from where the bus is.
- **Timetabled:** the published time.

How the estimates are built:
- **Declared journeys only.** We estimate only for buses whose own feed names
  the journey they are running. A bus whose journey we have had to infer is
  shown with timetable times, labelled as such.
- **They lean early.** 80% of a late bus's lateness is passed on, so a passenger
  is more likely to wait than to miss the bus. Early running is held at the next
  timing point.
- **Diversions.** Where a recorded diversion is in force, a measured allowance
  is added at the stops beyond it.
- **Before the bus sets off.** The journey a bus runs next is learned each night
  from the recorded days, so a departure has a live time before its bus has set
  off (Brighton & Hove, Compass and Stagecoach). Until then, a journey that
  hasn't started shows its whole timetabled route.

Each board also shows:
- disruption notices, from SIRI-SX and hand-recorded operator notices, with the
  nearest stop still served when a stop is closed;
- the last bus home;
- when buses run at the stop each day;
- the stop's shelter, seating and step-free access, from OpenStreetMap.

**Every route, day and night.** The network is drawn from the published
timetables, coloured by operator, with a night view. Pick an operator and route
to see:
- its stops each way, with timing points marked;
- when it runs today and how often;
- the notices in force and the tickets valid on it;
- a link to its route performance report.

**Fares across operators.** Each operator's day-ticket zones are drawn on the
map. A checker works out the cheapest way to cover a journey, including
multi-operator tickets, weekly tickets and capped single fares. Single fares by
route come from the operators' NeTEx fares data on BODS. Every price carries
its source and the date it was checked, and a price is shown only where one can
be sourced.

**Journey planner (experimental).** Door to door, across operators.

**Discover.** Places worth a visit along the coast, and the ones a bus reaches
without changing from where you are, with the bus to take, when it leaves and
the walk at the far end (timetable only, from `/api/direct-from`).

**Travel updates and analysis.** Diversions in the operators' own words, one
card for each set of works, beside passenger reports and the project's own
analysis, such as Freedom of Information replies and fare changes.

---

## Performance measurement

The Cloudflare Worker records the BODS feeds once a minute. A nightly GitHub
Actions job matches each vehicle to its timetabled journey and works out when
it left each stop: the last position within 150 m of the stop, so a bus that
dwells is timed when it leaves.

**Journey identification, in three ways, in order of confidence:**
1. the journey ID the operator's feed declares;
2. the line, first stop and start time;
3. position and time, which is less certain and reported separately.

Each route report says how many of its journeys were identified each way. In
the recordings from 25 September to 14 October 2026, 96% of tracked journeys
were identified from the operator's own feed.

**Route performance reports** (for example
[`report.html?service=700&operator=SCSO`](https://worthingbrightonbus.co.uk/report.html?service=700&operator=SCSO)),
rebuilt nightly and printable:
- **Punctuality at timing points,** against the Traffic Commissioners' window of
  1 minute early to 5 minutes 59 seconds late, and their 95% expectation. It is
  broken down by direction, day type and time of day, with the day-to-day
  spread, and shown with and without journeys through recorded disruptions.
- **Excess waiting time** on frequent stretches: scheduled against actual
  waiting time at the busiest timing point, and gaps over 15 minutes. Only days
  where nearly every timetabled bus was seen are counted.
- **Bunching:** buses leaving behind a later one, and where it happens most.
- **Running times:** where buses lose the most time between timing points.
- **Trends:** week by week.
- **Recorded disruptions** in the period, and the full method.

The reports carry a Draft label while hand-timed checks of the recorded stop
times are under way. The tools for those checks are `timing.html`, used at a
stop, and `ride.html`, used on board.

**Bus Performance, on the site:**
- **Journey times** between any two stops: the median, and a 90th-percentile
  "allow" time, by weekday or weekend and time of day.
- **The timetable for comparison:** the scheduled time is recorded each night,
  so the comparison still holds after a timetable change.
- **A delay map** of where time is lost, segment by segment.

**Minimum samples.** A figure needs at least 10 journeys over at least 5 days.
Below that the site shows "too few", never a number. A journey we did not see is
"missing", never "cancelled".

**Accuracy of its own live times.** Live estimates are scored against the
accuracy windows recommended by RTIG (the Real Time Information Group), which
tighten as the bus gets closer. Two scorings so far:
- **Replay:** 28 recorded days (25 September to 24 October 2026) were replayed
  from each timing point a declared bus left. 83% of estimates fell within the
  window, against 64% for the timetable alone.
- **As shown:** scored against what the board actually displayed, minute by
  minute, 79% were within the window.

The [live data health page](https://worthingbrightonbus.co.uk/health.html)
re-scores the board every night, operator by operator, and publishes how often
it was wrong. From 3 November 2026 the board passes on 80% of a late bus's
lateness. On the replayed days that cut buses coming before the time shown from
5.4% to 3.4% of estimates, and cut those within the window from 77.3% to 72.0%
([method](./docs/reliability/next-journey-2026-11-03.md)).

**Evidence for planning.**
- **Service levels:** scheduled departures either side of the council boundary
  at Portslade, by stop, route and time of day, with denominators and caveats
  published alongside.
- **Better buses:** what would help, who is responsible for each change, and how
  far it has got.

---

## Data standards and sources

| Data | Source | Format |
|---|---|---|
| Timetables | Bus Open Data Service, South East GTFS export (from the operators' TransXChange), rebuilt weekly | GTFS, built into SQLite |
| Live positions | Bus Open Data Service | SIRI-VM, GTFS-RT |
| Disruptions | Bus Open Data Service, plus hand-recorded notices | SIRI-SX |
| Fares | Bus Open Data Service | NeTEx |
| Live predictions | TransportAPI | JSON |
| Stops | NaPTAN | CSV |
| Route registrations | DVSA, Traffic Commissioners' register | CSV |
| Coaches | FlixBus (transport.data.gouv.fr), National Express (BODS) | GTFS, GTFS-RT |
| Trains | Realtime Trains | JSON |
| Liveries and fleet | bustimes.org vehicle list | JSON |
| Stop facilities and maps | OpenStreetMap | ODbL |
| Boundaries and postcodes | ONS, postcodes.io | GeoJSON |

Full credits and licences: [`THIRD_PARTY_NOTICES.md`](./THIRD_PARTY_NOTICES.md)
and the site's [About page](https://worthingbrightonbus.co.uk/about.html#data).

---

## How it is built

| Piece | Where | What it does |
|---|---|---|
| Site | GitHub Pages | Plain HTML, CSS and JavaScript with Leaflet, with no build step or framework |
| API | FastAPI on Render's free tier ([`api/`](./api/)) | Boards, vehicles, routes, journeys and estimates |
| Recorder and submissions | Cloudflare Worker ([`worker/`](./worker/)) | Records the feeds every minute into R2, keeps the API warm, and files submissions privately for review |
| Pipelines | GitHub Actions ([`.github/workflows/`](./.github/workflows/)) | Weekly timetable build, and nightly matching, scoring and route reports |

The whole platform costs under £20 a year, which is the cost of the domain. The
limits it is built to fit are in [`LIMITS.md`](./LIMITS.md).

**Accessibility.** The site aims at WCAG 2.2 AA:
- 44 px targets, keyboard access throughout, and a stop search that does the
  map's job without a pointer;
- settings for dark mode, text size, reduced motion and a colour-blind-safe
  palette.

**Testing.**
- Python and Node test suites, run in CI.
- [`scripts/browser_check.mjs`](./scripts/browser_check.mjs), which drives the
  real site in headless Chrome at phone and desktop sizes, in light and dark
  mode. It checks layout, contrast, reachability and every tour step.

---

## Running it locally

```sh
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
python -m http.server 8765                       # the site
uvicorn api.main:app --port 8000                 # the API (needs BODS_API_KEY)
```

Open <http://127.0.0.1:8765/?api=http://localhost:8000>. Without `?api=`, the
page uses the deployed API. The override works only when the page is served
from localhost and points at a local address.

`DEBUG_ENABLED=1` turns on `/docs` and the `/api/debug/*` diagnostics, which are
off in production.

Tests:

```sh
.venv/bin/python -m pytest -q
node --test "tests/*.mjs"
```

For the browser check, start Chrome with
`--headless=new --remote-debugging-port=9222`, then run
`node scripts/browser_check.mjs`. The script's header comment covers flatpak
Chrome.

## Documentation

- [`docs/RUNBOOK.md`](./docs/RUNBOOK.md): what runs where, routine operations,
  the nightly pipeline and how to recover it.
- [`docs/MAINTAINING.md`](./docs/MAINTAINING.md): publishing ideas, proposals,
  articles and other curated content, and how the map and ticket checker
  behave.
- [`docs/reliability/`](./docs/reliability/): method notes and results for
  each measure.
- [`LIMITS.md`](./LIMITS.md): free-tier quotas and the rules for anything that
  uses them.

## Contributing

Issues and pull requests are welcome. Please keep the frontend plain HTML, CSS
and JavaScript, and the backend plain FastAPI. Check
[`LIMITS.md`](./LIMITS.md) before adding an external call, a scheduled job or
any polling.

Operators, authorities and other regions that want to check the figures or
reuse the platform can reach Connor Ranahan at
<privacy@worthingbrightonbus.co.uk>.

## Licence

- **Code:** MIT ([`LICENSE`](./LICENSE)).
- **Written content and the project's own data:** CC BY 4.0
  ([`LICENSE-content.md`](./LICENSE-content.md)).
- **Third-party data, fonts, libraries, operator names and liveries:** keep
  their own terms ([`THIRD_PARTY_NOTICES.md`](./THIRD_PARTY_NOTICES.md)).
