# Content licence: Creative Commons Attribution 4.0

The site's own written content and data are licensed under the
[Creative Commons Attribution 4.0 International licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
The full legal code is at <https://creativecommons.org/licenses/by/4.0/legalcode>.

You can share and adapt them for any purpose, including commercially, as long
as you give appropriate credit ("Worthing Brighton Bus", with a link to
<https://worthingbrightonbus.co.uk/> where practical), indicate if changes were
made, and do not suggest the project endorses you or your use.

## What this covers

- Articles, objectives, explanations and other text written for the site,
  including the prose in `index.html`, `about.html`, `privacy.html` and
  `terms.html`, and the curated files in `data/` that the project wrote:
  `objectives.json`, `updates.json`, `community_updates.json`, `proposals.json`,
  `suggestions.json`, `journey_presets.json`, `journey_time_presets.json`,
  `ticket_zones.json`, `boundary_evidence.json`, `comparison_areas.json`,
  `tour.json`, `conference.json` and `analysis_exclusions.json`. The notices
  in `disruptions.json` are recorded in their authors' own words, which stay
  theirs.
- The tour slides and their PDF (`tour.html`, `media/tour/`), and the route
  reports and evidence files the nightly workflow publishes, as far as they
  are the project's own analysis.
- Community submissions published on the site, which senders agree to publish
  under this licence (see `terms.html`).
- Figures the project derived from its own analysis, with their stated method.

## What this does not cover

These keep their own terms and are not relicensed by anything in this repository:

- Timetable, live, fares, disruption and stop data from the Bus Open Data
  Service, NaPTAN, DVSA, TransportAPI, Realtime Trains, the ONS, Ordnance
  Survey and Royal Mail (credited on `about.html` and in
  `THIRD_PARTY_NOTICES.md`), including `data/timetable.json`,
  `data/timetable.sqlite`, `data/stops.json`, `data/stop_details.json`,
  `data/fare_tables.json`, `data/registrations.json`,
  `data/council_boundaries.json`, `data/rail_stations.json` and
  `data/representatives.json`.
- FlixBus's timetable and live times (Open Database Licence), and the bus
  liveries, fleet numbers and models in `data/fleet.json`, from the vehicle
  list kept by bustimes.org.
- OpenStreetMap map data (© OpenStreetMap contributors, ODbL), and
  `data/stop_osm.json`, the stop shelter, seating and access facts built from
  it by `scripts/build_stop_osm.py`: a derived database, so it stays under the
  Open Database Licence 1.0, not CC BY, and is credited wherever it is shown.
- Operator names, logos, liveries and bus icons in `icons/`, which identify
  services and belong to their owners.
- Fonts in `fonts/` (SIL Open Font License) and Leaflet in `vendor/` (BSD 2-Clause).
- Photographs and other media credited to someone else, recorded in
  `docs/ASSET_RIGHTS.md`. Where a credit there is unconfirmed, do not assume the
  image can be reused.
- The code, which is under the MIT licence in `LICENSE`.
