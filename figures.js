/* Key figures: figures.html
 *
 * The site's headline numbers on one page, each with its denominator, how it
 * was made, when, and what would make it wrong (.claude/skills/
 * evidence-provenance). The boundary and identification figures are read
 * from the files the pipeline publishes, so they cannot drift from the site;
 * the two replay results quote their write-ups and say when they were made.
 */
"use strict";

const FIGURES_PUBLISHED = "https://adur-worthing-submissions.dennislemennace.workers.dev/journey-times";
const FIGURES_REPO = "https://github.com/dennislemennace/adur-worthing-bus/blob/main";

function fEsc(value) {
  return String(value == null ? "" : value).replace(/[&<>"']/g, ch =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch]);
}

function fPct(part, whole) { return whole ? Math.round(part * 100 / whole) : null; }
function fNum(n) { return Number(n).toLocaleString("en-GB"); }
function fDate(iso) {
  const d = new Date(`${String(iso).slice(0, 10)}T12:00:00Z`);
  return isNaN(d) ? String(iso) : d.toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" });
}

/** One figure: the claim, its value with denominators, and its evidence. */
function figureHtml(f) {
  return `<article class="figure" id="${fEsc(f.id)}">
    <p class="claim">${fEsc(f.claim)}</p>
    <p class="value">${f.value}</p>
    <dl>
      <dt>How</dt><dd>${fEsc(f.method)}</dd>
      <dt>As of</dt><dd>${fEsc(f.asOf)}</dd>
      <dt>Watch out for</dt><dd>${fEsc(f.caveat)}</dd>
    </dl>
    <p class="source"><a href="${fEsc(f.sourceUrl)}">${fEsc(f.sourceLabel)}</a></p>
  </article>`;
}

/** The boundary figures, from data/boundary_evidence.json. */
function boundaryFigures(e) {
  const day = e.days.monday, sat = e.days.saturday;
  const place = e.places.days.monday;
  const night = e.night && e.night.nights && e.night.nights.weeknight;
  const asOf = `Timetable of ${fDate(e.as_of)}, the week of ${fDate((e.measured_week || {}).monday || e.as_of)} ` +
    `(sha256 ${String((e.data_version || {}).sha256 || "").slice(0, 8)}…)`;
  const source = { sourceUrl: "data/boundary_evidence.json", sourceLabel: "The generated evidence file" };
  const caveat = "Counts scheduled buses, not buses that ran; being near a council boundary is not on its own proof the boundary caused the gap. Rebuilt every week.";
  const out = [{
    id: "band", claim: "West of the Brighton & Hove boundary, each stop gets about two thirds of the buses.",
    value: `<strong>${day.west.departures_per_stop}</strong> weekday departures per stop west of the line, against
      <strong>${day.east.departures_per_stop}</strong> east: <strong>${fPct(day.west.departures_per_stop, day.east.departures_per_stop)}%</strong>
      (${fNum(day.west.stops)} and ${fNum(day.east.stops)} stops). Saturday routes: ${sat.west.routes} against ${sat.east.routes}.`,
    method: e.method.summary, asOf, caveat, ...source,
  }, {
    id: "places", claim: "Lancing gets a quarter of the buses per stop that South Portslade does, about five miles along the same coast road.",
    value: `<strong>${place.west.departures_per_stop}</strong> against <strong>${place.east.departures_per_stop}</strong> weekday
      departures per stop (${fNum(e.places.west.stops)} and ${fNum(e.places.east.stops)} stops):
      <strong>${fPct(place.west.departures_per_stop, place.east.departures_per_stop)}%</strong>.`,
    method: "Stops inside the published ONS boundaries of Lancing county electoral division and South Portslade ward, counted on a dated weekday.",
    asOf, caveat: "South Portslade sits on a trunk corridor; part of the gap is geography as well as planning. " + caveat, ...source,
  }];
  if (night) {
    const by = night.by_area;
    out.push({
      id: "night", claim: "After 11pm, Lancing's stops see about a quarter of the buses Portslade's do.",
      value: `<strong>${by.lancing.departures_per_stop}</strong> buses per stop between 23:00 and 05:00 on a weeknight, against
        <strong>${by["south-portslade"].departures_per_stop}</strong> in South Portslade and ${by["north-portslade"].departures_per_stop}
        in North Portslade.`,
      method: "Departures from 23:00 to 05:00 at stops inside each area's ONS boundary, the small hours counted with the evening before.",
      asOf, caveat: "Lancing's count includes the National Express 025 coach, which needs its own ticket, so it flatters the local service.",
      ...source,
    });
  }
  return out;
}

/** How each operator's journeys were identified, from identification.json. */
function identificationFigure(ident, fileUrl) {
  const ops = Object.values(ident.operators || {});
  const tracked = ops.reduce((n, o) => n + o.tracked, 0);
  const exact = ops.reduce((n, o) => n + (o.by.journey_id || 0) + (o.by.start || 0), 0);
  const days = ident.days || [];
  return {
    id: "identified", claim: "Nearly every tracked bus is matched to its journey by the operator's own feed, not by our guess.",
    value: `<strong>${fNum(exact)}</strong> of <strong>${fNum(tracked)}</strong> tracked journeys
      (<strong>${fPct(exact, tracked)}%</strong>), across ${days.length} recorded days.`,
    method: "Each journey counts once a day. Identified by the journey ID the operator's feed names, or by its line, first stop and scheduled start; the rest were worked out from position and time.",
    asOf: days.length ? `${fDate(days[0])} to ${fDate(days[days.length - 1])}` : "",
    caveat: "Matching by start time relies on our timetable having the operator's times to the second; a journey it cannot match is left out, never given to the wrong bus.",
    sourceUrl: fileUrl, sourceLabel: "The published counts, by operator and route",
  };
}

/** Results written up from replays of recorded days: quoted, with their dates. */
const REPLAY_FIGURES = [{
  id: "estimates", claim: "Our live arrival estimates beat the timetable, and say which way they can be wrong.",
  value: "Within 2 minutes of when the bus left <strong>71%</strong> of the time, against <strong>47%</strong> for the timetable alone, over 2,803,361 estimate and outcome pairs; within 2 minutes 95% of the time when the bus was under 5 minutes away. About <strong>one estimate in eight</strong> had the bus leave 2 or more minutes earlier than said.",
  method: "The site's own estimate replayed from every timing point a bus was seen leaving, scored against when it really left each later stop, 25 September to 14 October 2026.",
  asOf: "Measured 16 October 2026",
  caveat: "The replay starts from a fresh position; live reports arrive a few minutes late, so real estimates are a little less accurate.",
  sourceUrl: `${FIGURES_REPO}/docs/reliability/live-eta-accuracy-2026-10-16.md`, sourceLabel: "The replay, its method and results",
}, {
  id: "diversion", claim: "Allowing for the Western Road diversion made estimates on diverted routes better.",
  value: "Typical error <strong>2.7 → 2.0 minutes</strong> on journeys past the diversion; estimates within 2 minutes 41% → 50% of the time. Buses 2 or more minutes earlier than estimated went from 2% to 3%.",
  method: "An allowance measured from buses that went round the diversion (the 15th percentile of the time they lost), calibrated on days before 10 October and tested on 10 to 14 October, which it never saw.",
  asOf: "Measured 15 October 2026",
  caveat: "A small rise in buses earlier than estimated is the price of better estimates overall; the allowance is kept deliberately low for that reason.",
  sourceUrl: `${FIGURES_REPO}/docs/reliability/diversion-adjustments-backtest-2026-10-15.md`, sourceLabel: "The backtest",
}];

async function figuresInit() {
  const host = document.getElementById("figures");
  const parts = [];
  try {
    const e = await (await fetch("data/boundary_evidence.json")).json();
    parts.push(...boundaryFigures(e));
  } catch { /* shown without them */ }
  try {
    const index = await (await fetch(`${FIGURES_PUBLISHED}/index.json`)).json();
    const entry = (index.artifacts || {})["identification.json"];
    if (entry) {
      const ident = await (await fetch(`${FIGURES_PUBLISHED}/${entry.file}`)).json();
      parts.push(identificationFigure(ident, `${FIGURES_PUBLISHED}/${entry.file}`));
    }
  } catch { /* shown without it */ }
  parts.push(...REPLAY_FIGURES);
  host.innerHTML = parts.map(figureHtml).join("");
}

if (typeof document !== "undefined" && document.getElementById("figures")) figuresInit();
