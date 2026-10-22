/* The tour's figures: the numbers its words quote, worked out from the files
 * the pipeline publishes, and the {placeholder} filling that puts them in the
 * text. Shared by the tour in the app (tour.js) and the slides (tour.html,
 * tour-page.js), so both say the same thing. No dependencies: plain
 * functions of plain data, tested in tests/test_tour.mjs.
 */
"use strict";

const TOUR_MONTHS = ["January", "February", "March", "April", "May", "June", "July",
                     "August", "September", "October", "November", "December"];

/** "16 September" from 2026-09-16. Read from the string, not a Date, so no
 *  time zone can move it a day. */
function tourDay(iso, year = false) {
  const [y, m, d] = String(iso).slice(0, 10).split("-").map(Number);
  if (!y || !m || !d) return String(iso);
  return `${d} ${TOUR_MONTHS[m - 1]}${year ? ` ${y}` : ""}`;
}

function tourNextDay(iso) {
  const d = new Date(`${iso}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString().slice(0, 10);
}

/** Days missing between the first and last recorded day, runs of them as one
 *  span: "18 September to 16 October, 18 to 19 October". */
function tourGaps(days) {
  const have = new Set(days || []);
  const sorted = [...have].sort();
  if (sorted.length < 2) return "none so far";
  const spans = [];
  let start = null, prev = null;
  for (let d = tourNextDay(sorted[0]); d < sorted[sorted.length - 1]; d = tourNextDay(d)) {
    if (have.has(d)) {
      if (start) spans.push([start, prev]);
      start = null;
    } else {
      if (!start) start = d;
      prev = d;
    }
  }
  if (start) spans.push([start, prev]);
  if (!spans.length) return "none so far";
  return spans.map(([a, b]) => {
    if (a === b) return tourDay(a);
    if (a.slice(0, 7) === b.slice(0, 7)) return `${Number(a.slice(8))} to ${tourDay(b)}`;
    return `${tourDay(a)} to ${tourDay(b)}`;
  }).join(", ");
}

/** Every value a {placeholder} in data/tour.json can ask for: from the
 *  boundary evidence, the published journey-times index, and the published
 *  identification counts. */
function tourFigureValues(e, index, ident) {
  const v = {};
  const n = x => (x == null ? "unavailable" : String(x));
  const count = x => (x == null ? "unavailable" : Number(x).toLocaleString("en-GB"));
  const day = e && e.days && e.days.monday;
  if (day) {
    v.evidence_week = tourDay((e.measured_week || {}).monday || e.as_of, true);
    v.band_west_dps = n(day.west.departures_per_stop);
    v.band_east_dps = n(day.east.departures_per_stop);
    v.band_share = day.east.departures_per_stop
      ? String(Math.round(day.west.departures_per_stop * 100 / day.east.departures_per_stop)) : "unavailable";
    v.routes_west_weekday = n(day.west.routes);
    v.routes_east_weekday = n(day.east.routes);
    v.routes_west_sat = n(e.days.saturday && e.days.saturday.west.routes);
    v.routes_east_sat = n(e.days.saturday && e.days.saturday.east.routes);
    v.routes_west_sun = n(e.days.sunday && e.days.sunday.west.routes);
    v.routes_east_sun = n(e.days.sunday && e.days.sunday.east.routes);
    v.late_west = count(day.west.departures_after_2300);
    v.late_east = count(day.east.departures_after_2300);
    const place = e.places && e.places.days && e.places.days.monday;
    v.lancing_dps = n(place && place.west.departures_per_stop);
    v.sportslade_dps = n(place && place.east.departures_per_stop);
  }
  const days = (index && Array.isArray(index.days)) ? [...index.days].sort() : [];
  v.recorded_days = days.length ? String(days.length) : "unavailable";
  v.recorded_from = days.length ? tourDay(days[0]) : "unavailable";
  v.recorded_to = days.length ? tourDay(days[days.length - 1]) : "unavailable";
  v.recorded_gaps = days.length ? tourGaps(days) : "unavailable";
  // Named by the operator's own feed: its journey ID, or its line, first stop
  // and start time. The rest were worked out from position and time.
  const ops = ident && ident.operators ? Object.values(ident.operators) : [];
  const tracked = ops.reduce((sum, o) => sum + (o.tracked || 0), 0);
  const named = ops.reduce((sum, o) => sum + ((o.by || {}).journey_id || 0) + ((o.by || {}).start || 0), 0);
  v.identified_share = tracked ? String(Math.round(named * 100 / tracked)) : "unavailable";
  v.identified_tracked = tracked ? count(tracked) : "unavailable";
  return v;
}

/** Text with its {placeholders} filled. One with no value is left as it is,
 *  visible, rather than blanked: a gap in a sentence reads as a claim. */
function tourFill(text, values) {
  return String(text == null ? "" : text).replace(/\{([a-z_]+)\}/g,
    (whole, key) => (Object.prototype.hasOwnProperty.call(values || {}, key) ? values[key] : whole));
}
