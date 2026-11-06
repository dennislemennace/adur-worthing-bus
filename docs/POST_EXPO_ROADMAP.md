# After the expo: what to build next, and how

Written 6 November 2026 from the maintainer's post-expo notes. Each note says
what was found, what is already built (with its commit), and the recommended
design for what is left, with its data, cost and risks. House rules apply
throughout: every published figure has a source, a date and a denominator;
nothing quota-costing without a LIMITS.md entry; prove each new check fails
before trusting it.

| Note | State |
|---|---|
| Accessibility information front and centre | Phase A built (2a22836); phase B below |
| Ramp use at a stop | Design below; needs data we do not have |
| Design: shortest path, coming back | Design below |
| Discover Worthing & Brighton, "Where to go from here" | Design below |
| Detect diversions automatically | Design below |
| Locality stats browser | Phase A built (358c5e3); phase B below |
| Stroudley Road car park rename | Built (177470f) |
| Coach destination renames | Built (177470f) |
| User location dot | Built (177470f) |

Also built this round: tour steps 2 and 4 no longer wait on a cold API
(321959d), and the tour ends on quick links to every part of the site
(e56b7e6).

## Accessibility information, phase B

Built in phase A: a row of facts under each stop's name (step-free, kerb,
shelter, seat, tactile paving, live screen, lighting), "Step-free: not
recorded" said rather than left out, OpenStreetMap facts merged across a
stop's node, platform and way, kerbs mapped beside a stop, NaPTAN's unmarked
and hail and ride stops, the coach operators' own wheelchair terms, and an
"I need step-free access" setting.

What the data says: step-free access is recorded at 28 of 1,496 matched stops
and a kerb at 14 (6 November 2026). The biggest gain is more data, not more
interface.

1. **Fill the gaps.** A "missing access" map layer for mappers: stops with no
   wheelchair or kerb tag, ringed, each linking to OpenStop. Publish the
   coverage figure on health.html each week so progress is visible. Cost: one
   layer from data/stop_osm.json, no new data.
2. **Use the setting further.** With "I need step-free access" on: the journey
   planner and the ticket checker flag a boarding or alighting stop that is
   not step-free or not recorded; Route view's stops filter to step-free;
   the board says when the next bus is a coach that needs booking ahead.
3. **Vehicles.** Local service buses have had to meet the PSV Accessibility
   Regulations 2000 since 2016 (single-deck) and 2017 (double-deck), and
   audio-visual next-stop announcements are being phased in under the PSV
   (Accessible Information) Regulations 2023. Say either in the Bus tab only
   with the regulation cited and dated (evidence-provenance), and show which
   of our operators' buses announce stops once an operator publishes it.
4. **Stations.** Step-free access at the 15 stations in the box, from each
   station's National Rail page, curated with `checked_on`, shown on the
   station board.

## Ramp use at a stop

The question: does a particular stop see the ramp deployed often, and does
that cost time?

**What open data can and cannot say.** BODS location feeds carry no ramp,
door or kneel status. Our recording is one position a minute, which cannot
time a dwell to the 30 to 90 seconds a ramp adds. So no open dataset shows it
today.

Recommended, in order:

1. **Hand-logged, now.** ride.html already offers "Wheelchair ramp used" as a
   note. Make it a one-tap "Ramp out" and "Ramp in" pair with times, so each
   deployment is timed, not just noted. Aggregate the exported logs per stop
   and publish only above a floor (for example 10 deployments over 5 days),
   labelled hand-logged. This answers "where" for the stops riders cover,
   not for the network.
2. **Ask the operators.** Many buses log ramp deployment through their
   telematics. Ask Brighton & Hove, Stagecoach and Compass for counts by stop
   through the Enhanced Partnership's data-sharing, framed as BSIP evidence on
   accessibility. Their data, published with permission and their caveats.
3. **A proxy, exploratory only.** Long dwells at stops that are not timing
   points, where a bus is not waiting time. Needs positions more often than
   once a minute: check LIMITS.md and the Worker's CPU budget first (two or
   three fetches inside each minute's run). Publish as "long dwells", never as
   "ramp use": boardings, fares and driver changes look the same.

## Design: the shortest path to what someone needs, and a reason to come back

What the site does now: it opens on the live map with a stop search; one
favourite stop per device opens on arrival; six sections sit behind a menu.

Recommended:

1. **A home sheet.** On arrival, before any stop is chosen: saved stops (more
   than one), each with its next two live departures; recently opened stops;
   one-tap "Stops near me"; the last bus home from the saved stop. Everything
   stays on the device (localStorage, wrapped in try/catch).
2. **Installable.** A web app manifest and icons, so it can sit on a phone's
   home screen and open full screen. No offline cache at first: a service
   worker that serves stale timetables is worse than none.
3. **A simpler menu.** Six sections grouped into two: Travel (live, routes,
   tickets, updates) and Evidence (performance, better buses). The tour's
   quick links (e56b7e6) are a first draft of the grouping.
4. **Test before and after.** A first-click test with five passengers on four
   tasks (next bus home, cheapest ticket, is this stop step-free, is the 700
   late), timed, before and after each change. GoatCounter's view counts show
   whether people find the new paths.

## Discover Worthing & Brighton, and "Where to go from here"

The idea: give a location, and see the places worth visiting that a direct
bus reaches.

1. **The places.** A curated data/destinations.json of about 40 places, each
   with its own source page and `checked_on`, its OpenStreetMap wheelchair
   access, and its nearest stops: Brighton Palace Pier, i360, the Royal
   Pavilion, Devil's Dyke, Booth Museum, Preston Manor, Shoreham Fort,
   Highdown Gardens, Cissbury Ring, Worthing Pier and others. Curated rather
   than pulled from OpenStreetMap's tourism tags, which mix the remarkable
   with the ordinary.
2. **What is reachable.** A new endpoint, `/api/direct-from?stops=…`, reading
   only the local timetable (no quota): from the reader's nearest stops, every
   stop a bus reaches without changing, today, with the ride time and how
   often it runs. Cached per stop set for five minutes.
3. **The answer.** The page crosses the reachable stops with the places near
   them: "Direct from here", nearest ride first, each with the bus to take,
   the next departure and the walk at the far end. Location stays on the
   device, as Stops near me does.
4. **Where it lives.** A new Discover section, and a link from the home sheet.
   It doubles as evidence for better buses: a place no direct bus reaches from
   a ward is a gap the Areas card can show.

## Detecting diversions automatically

Today diversions are recorded by hand (data/disruptions.json, add_disruption.py),
and their effect on the times is measured afterwards (build_diversion_adjustments.py).

1. **Nightly detection.** For each route and direction, the share of journeys
   that pass each stop: a stop that is normally passed on nearly every journey
   but is missed by 6 or more journeys across 2 or more days is a candidate
   "not served". Positions more than 100 m from the route's shape on the same
   journeys give the path actually taken. One-minute sampling misses short
   passes, so each stop is compared with its own usual pass rate, not with
   100%.
2. **What it publishes.** A detected-diversions.json artifact: the routes, the
   stops not served, the path taken, when it was first and last seen, and a
   confidence. Shown on boards and in Travel Updates as "Detected from the
   buses' own positions, not confirmed by the operator".
3. **A person confirms.** Each detection opens an issue for the maintainer
   (notify_warnings.py). Confirming it with add_disruption.py makes it a
   recorded diversion, so the allowance model applies and the live times
   allow for it. Nothing is called a diversion on the record without that.
4. **Later, the same day.** In the API, three or more buses off the route's
   shape on the same stretch within an hour, shown as provisional.

## Locality stats browser, phase B

Built in phase A (Route view, Areas): 43 wards shaded by buses per stop, each
with a card of departures, routes, first and last bus, late and night buses,
recorded access, and its councillors.

1. **Delays by ward.** From the delay map's nightly cells: the median time
   lost on stretches inside each ward, weekday daytime and evening peak, with
   the same floor (10 journeys over 5 days) and "too few" below it. Added to
   localities.json by the nightly build, not the weekly one.
2. **Who lives there.** Census 2021 households without a car and residents
   aged 65 and over, from Nomis, through the ONS output-area to 2025-ward
   lookup (Census tables are on older ward boundaries). Shown beside the bus
   figures, so a ward with few buses and many households without a car stands
   out.
3. **Write to them.** The card's councillors open the existing councillor
   letter with the ward's figures quoted, as the objectives do.
4. **Compare.** Two wards side by side, for example Lancing and South
   Portslade, the comparison the boundary evidence already makes.
