/* Live data health: health.html
 *
 * How well each operator's buses can be followed, and how good the live times
 * on the board were, by RTIG's measures (RTIGT041-1.0, sections 5.2 and 6).
 * Everything is read from the files the nightly pipeline publishes, so it
 * cannot drift from the site: identification.json (how each journey was
 * matched) and served-accuracy.json (what the board said against what the
 * buses did, scripts/score_served_estimates.py). Counts arrive with their
 * denominators; this page divides, and says "too few" rather than a share of
 * almost nothing.
 */
"use strict";

const HEALTH_PUBLISHED = "https://adur-worthing-submissions.dennislemennace.workers.dev/journey-times";
const HEALTH_REPO = "https://github.com/dennislemennace/adur-worthing-bus/blob/main";
// Until the nightly pipeline publishes its first window, the backfill.
const HEALTH_SEED = "data/reliability/served/served-accuracy.json";
const OPERATOR_NAMES = { BHBC: "Brighton & Hove", SCSO: "Stagecoach", COMT: "Compass", METR: "Metrobus" };
// The site's floor for a figure about a route (docs/RUNBOOK.md): fewer than
// this many journeys says "too few", never a number.
const MIN_JOURNEYS = 10;
// RTIGT041 8.8's suggested starting point, read with exactly 70% as satisfactory.
const RATINGS = [[0.9, "excellent"], [0.8, "good"], [0.7, "satisfactory"], [0, "unsatisfactory"]];
const RANGE_ORDER = ["0-3 min", "3-6 min", "6-12 min", "12-18 min", "18-30 min"];
// TransportAPI's predictions scored beside ours on the same departures
// (api/prediction_log.py). Published by the maintainer's decision of
// 25 October 2026, once enough departures have been compared.
const SHOW_NEXTBUSES_COMPARISON = true;
// Below this many departures compared, the comparison says "too few so far".
const MIN_COMPARED = 1000;
const RANGE_WINDOWS = { "0-3 min": "60 s / 60 s", "3-6 min": "90 s / 120 s", "6-12 min": "120 s / 210 s",
                        "12-18 min": "150 s / 240 s", "18-30 min": "180 s / 360 s" };

function hEsc(value) {
  return String(value == null ? "" : value).replace(/[&<>"']/g, ch =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch]);
}
function hNum(n) { return Number(n || 0).toLocaleString("en-GB"); }
function hPct(part, whole) { return whole ? `${Math.round(part * 100 / whole)}%` : "–"; }
function hDate(iso) {
  const d = new Date(`${String(iso).slice(0, 10)}T12:00:00Z`);
  return isNaN(d) ? String(iso || "") : d.toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" });
}
function opName(code) { return OPERATOR_NAMES[code] || code; }

/** RTIG's acceptable / early / late from a count of verdicts. */
function rtigShares(c) {
  const ok = (c && c.ok) || 0, early = (c && c.early) || 0, late = (c && c.late) || 0;
  const n = ok + early + late;
  if (!n) return null;
  const share = ok / n;
  return { n, acceptable: share, early: early / n, late: late / n,
           rating: RATINGS.find(([floor]) => share >= floor)[1] };
}

/** "83% good": the share and RTIG's word for it, so colour is never the only signal. */
function ratingHtml(s) {
  if (!s) return `<span class="muted">–</span>`;
  return `<strong>${Math.round(s.acceptable * 100)}%</strong> <span class="rating rating--${s.rating}">${s.rating}</span>`;
}

/** One row per operator: tracked, identified, live within 15 minutes, RTIG accuracy. */
function operatorRows(ident, served) {
  const codes = new Set([...Object.keys((ident && ident.operators) || {}),
    ...Object.keys((served && served.coverage) || {}).filter(k => k.startsWith("operator|")).map(k => k.slice(9))]);
  return [...codes].sort((a, b) => opName(a).localeCompare(opName(b))).map(code => {
    const id = ((ident && ident.operators) || {})[code];
    const cov = ((served && served.coverage) || {})[`operator|${code}`];
    const acc = rtigShares(((served && served.accuracy) || {})[`operator|${code}`]);
    return { code, name: opName(code),
      scheduled: id ? id.scheduled : null,
      tracked: id ? id.tracked : null,
      identified: id ? id.by.journey_id + id.by.start : null,
      calls: cov ? cov.calls : null,
      live15: cov ? cov.with_estimate : null,
      acc };
  });
}

function operatorTableHtml(rows) {
  const body = rows.map(r => `<tr>
      <th scope="row">${hEsc(r.name)}</th>
      <td>${r.scheduled == null ? "–" : `${hPct(r.tracked, r.scheduled)}<span class="of"> of ${hNum(r.scheduled)}</span>`}</td>
      <td>${r.tracked == null ? "–" : `${hPct(r.identified, r.tracked)}<span class="of"> of ${hNum(r.tracked)}</span>`}</td>
      <td>${r.calls == null ? "–" : `${hPct(r.live15, r.calls)}<span class="of"> of ${hNum(r.calls)}</span>`}</td>
      <td>${ratingHtml(r.acc)}</td>
      <td>${r.acc ? `${Math.round(r.acc.early * 100)}%` : "–"}</td>
      <td>${r.acc ? `${Math.round(r.acc.late * 100)}%` : "–"}</td>
    </tr>`).join("");
  return `<div class="table-wrap"><table>
    <caption>By operator</caption>
    <thead><tr><th scope="col">Operator</th><th scope="col">Journeys we saw</th>
      <th scope="col">Journey named by the feed</th><th scope="col">Departures with a live time 15 min before</th>
      <th scope="col">Live times acceptable (RTIG)</th><th scope="col">Bus came early</th><th scope="col">Bus came late</th></tr></thead>
    <tbody>${body}</tbody></table></div>`;
}

/** Why departures had no live time, as RTIGT041 3.6.13 asks: so the repeating ones can be fixed. */
function reasonsHtml(served) {
  const c = ((served && served.coverage) || {}).all;
  if (!c || !c.calls) return "";
  const missing = c.calls - (c.with_estimate || 0);
  const item = (n, words) => `<li><strong>${hPct(n || 0, c.calls)}</strong> ${words} (${hNum(n || 0)})</li>`;
  return `<p>Of ${hNum(c.calls)} timetabled departures in the area, <strong>${hPct(c.with_estimate, c.calls)}</strong>
      had a live time on the board at some point in the 15 minutes before they were due. The other ${hNum(missing)}:</p>
    <ul>
      ${item(c.no_bus_seen, "no bus was seen on that journey at all: it may not have run, or its tracker was off")}
      ${item(c.seen_not_named, "a bus was seen, but its feed did not say which journey it was running, so we gave no live time")}
      ${item(c.named_no_estimate, "the feed named the journey, but there was no estimate for that stop then (the bus had not set off, or had just gone)")}
    </ul>`;
}

function rangesTableHtml(served) {
  const acc = (served && served.accuracy) || {};
  const rows = RANGE_ORDER.map(r => [r, rtigShares(acc[`range|${r}`])]).filter(([, s]) => s);
  if (!rows.length) return "";
  return `<div class="table-wrap"><table>
    <caption>By how far ahead the board said the bus was</caption>
    <thead><tr><th scope="col">Ahead</th><th scope="col">Allowed early / late</th><th scope="col">Live times</th>
      <th scope="col">Acceptable</th><th scope="col">Bus came early</th><th scope="col">Bus came late</th></tr></thead>
    <tbody>${rows.map(([r, s]) => `<tr><th scope="row">${hEsc(r)}</th><td>${hEsc(RANGE_WINDOWS[r])}</td>
      <td>${hNum(s.n)}</td><td>${ratingHtml(s)}</td><td>${Math.round(s.early * 100)}%</td>
      <td>${Math.round(s.late * 100)}%</td></tr>`).join("")}</tbody></table></div>`;
}

function cleardownHtml(served) {
  const c = ((served && served.cleardown) || {}).all;
  if (!c || !c.calls) return "";
  return `<p>Of ${hNum(c.calls)} departures we timed, the board would still have listed
      <strong>${hPct(c.listed_1_min_after || 0, c.calls)}</strong> a minute after the bus had gone,
      ${hPct(c.listed_3_min_after || 0, c.calls)} three minutes after and ${hPct(c.listed_5_min_after || 0, c.calls)} five minutes after.
      ${hPct(c.left_before_timetable || 0, c.calls)} left before their timetabled time; with no live time, the board
      keeps those until the timetable says they go.</p>`;
}

/** TransportAPI's predictions and ours, on the same departures at the same
 *  moment, judged the same way. */
function versusHtml(served, show = SHOW_NEXTBUSES_COMPARISON) {
  const v = (served && served.versus) || {};
  if (!show || !v["both|nextbuses|all"]) return "";
  const theirs = rtigShares(v["both|nextbuses|all"]), ours = rtigShares(v["both|ours|all"]);
  if (!theirs || !ours || theirs.n < MIN_COMPARED) {
    return `<p class="muted">Too few departures compared so far (${hNum(theirs ? theirs.n : 0)}; we wait for
      ${hNum(MIN_COMPARED)}). TransportAPI's predictions are only fetched when someone opens a board.</p>`;
  }
  const only = rtigShares(v["nextbuses_only|nextbuses|all"]);
  const ran = ((v["cancelled|all"] || {}).said_cancelled_but_ran) || 0;
  return `<div class="table-wrap"><table>
    <caption>The same departures, at the same moment</caption>
    <thead><tr><th scope="col">Live times from</th><th scope="col">Compared</th><th scope="col">Acceptable (RTIG)</th>
      <th scope="col">Bus came early</th><th scope="col">Bus came late</th></tr></thead>
    <tbody>
      <tr><th scope="row">TransportAPI (NextBuses)</th><td>${hNum(theirs.n)}</td><td>${ratingHtml(theirs)}</td>
        <td>${Math.round(theirs.early * 100)}%</td><td>${Math.round(theirs.late * 100)}%</td></tr>
      <tr><th scope="row">This site's own estimate</th><td>${hNum(ours.n)}</td><td>${ratingHtml(ours)}</td>
        <td>${Math.round(ours.early * 100)}%</td><td>${Math.round(ours.late * 100)}%</td></tr>
    </tbody></table></div>
    <p>${only ? `TransportAPI also had a time for ${hNum(only.n)} departures we had none for (${ratingHtml(only)}).` : ""}
      ${ran ? `${hNum(ran)} departures it called cancelled were then seen running.` : ""}
      The board shows TransportAPI's time where it has one, and ours otherwise.</p>`;
}

/** One row per route, with the site's floor: too few journeys says so. */
function routeRows(ident, served) {
  const services = (ident && ident.services) || {};
  const journeys = (served && served.journeys) || {};
  const out = [];
  for (const [key, id] of Object.entries(services)) {
    const [svc, op] = key.split("|");
    const cov = ((served && served.coverage) || {})[`service|${svc}|${op}`];
    const j = journeys[`service|${svc}|${op}`];
    const acc = rtigShares(((served && served.accuracy) || {})[`service|${svc}|${op}`]);
    out.push({ svc, op, name: opName(op), scheduled: id.scheduled, tracked: id.tracked,
      identified: id.by.journey_id + id.by.start, calls: cov ? cov.calls : 0, live15: cov ? cov.with_estimate : 0,
      acc: j && j.with_estimate >= MIN_JOURNEYS ? acc : null, tooFew: !j || j.with_estimate < MIN_JOURNEYS });
  }
  const num = s => parseInt(String(s).replace(/\D/g, ""), 10) || 0;
  return out.sort((a, b) => num(a.svc) - num(b.svc) || a.svc.localeCompare(b.svc) || a.name.localeCompare(b.name));
}

function routeTableHtml(rows) {
  if (!rows.length) return "";
  return `<div class="table-wrap"><table class="routes">
    <caption>By route</caption>
    <thead><tr><th scope="col">Route</th><th scope="col">Journeys we saw</th><th scope="col">Named by the feed</th>
      <th scope="col">Live time 15 min before</th><th scope="col">Acceptable (RTIG)</th><th scope="col">Bus came early</th></tr></thead>
    <tbody>${rows.map(r => `<tr><th scope="row">${hEsc(r.svc)} <span class="of">${hEsc(r.name)}</span></th>
      <td>${hPct(r.tracked, r.scheduled)}</td><td>${hPct(r.identified, r.tracked)}</td>
      <td>${hPct(r.live15, r.calls)}</td>
      <td>${r.tooFew ? `<span class="muted">too few</span>` : ratingHtml(r.acc)}</td>
      <td>${r.acc ? `${Math.round(r.acc.early * 100)}%` : "–"}</td></tr>`).join("")}</tbody></table></div>`;
}

/** The whole page, from the two files; either may be missing. */
function healthHtml(ident, served, sources = {}) {
  const parts = [];
  const days = served && served.days && served.days.length
    ? `${hDate(served.days[0])} to ${hDate(served.days[served.days.length - 1])} (${served.days.length} days)` : "";
  const overall = rtigShares(((served && served.accuracy) || {}).all);
  if (overall) {
    parts.push(`<section class="headline">
      <p class="claim"><strong>${Math.round(overall.acceptable * 100)}%</strong> of the live times the board showed were
        within RTIG's accuracy window: <span class="rating rating--${overall.rating}">${overall.rating}</span> on RTIG's scale.</p>
      <p>The window is the Real Time Information Group's guidance on how close a live time should be
        (<em>Accuracy and Quality of Real Time Predictions</em>, RTIGT041-1.0, 2020): within a minute either way when
        the bus is under 3 minutes away, widening to 3 minutes early or 6 late when it is 18 to 30 minutes away. On
        <strong>${Math.round(overall.early * 100)}%</strong> the bus came earlier than allowed, which is the error that
        loses someone their bus; on ${Math.round(overall.late * 100)}% it came later. ${hNum(overall.n)} live times on
        ordinary days, ${hEsc(days)}; journeys through recorded roadworks and diversions are counted apart.</p>
    </section>`);
  }
  parts.push(operatorTableHtml(operatorRows(ident, served)));
  if (served) {
    parts.push(`<h2>Departures with no live time</h2>${reasonsHtml(served)}`);
    parts.push(`<h2>How far ahead</h2>${rangesTableHtml(served)}`);
    parts.push(`<h2>Buses still listed after they had gone</h2>${cleardownHtml(served)}`);
    const versus = versusHtml(served);
    if (versus) parts.push(`<h2>TransportAPI's predictions and ours</h2>${versus}`);
  }
  parts.push(`<h2>Every route</h2>${routeTableHtml(routeRows(ident, served))}`);
  const caveats = [...((served && served.caveats) || []), ...((ident && ident.caveats) || [])];
  parts.push(`<h2>How these are made</h2>
    ${served ? `<p>${hEsc(served.method)}</p>` : ""}
    ${ident ? `<p>${hEsc(ident.method)}</p>` : ""}
    ${caveats.length ? `<ul>${caveats.map(c => `<li>${hEsc(c)}</li>`).join("")}</ul>` : ""}
    <p class="source">${served ? `Live times scored as of ${hEsc(hDate(served.as_of))}` : "Live times not yet scored"} ·
      ${ident ? `journeys matched as of ${hEsc(hDate(ident.as_of))}` : ""}
      ${sources.served ? ` · <a href="${hEsc(sources.served)}">the counts</a>` : ""}
      ${sources.ident ? ` · <a href="${hEsc(sources.ident)}">the matching counts</a>` : ""}
      · <a href="${HEALTH_REPO}/scripts/score_served_estimates.py">the method, in code</a></p>`);
  return parts.join("\n");
}

async function healthFetchArtifact(index, name) {
  const entry = (index.artifacts || {})[name];
  if (!entry) return [null, null];
  const url = `${HEALTH_PUBLISHED}/${entry.file}`;
  return [await (await fetch(url)).json(), url];
}

async function healthInit() {
  const host = document.getElementById("health");
  let ident = null, served = null;
  const sources = {};
  try {
    const index = await (await fetch(`${HEALTH_PUBLISHED}/index.json`)).json();
    [ident, sources.ident] = await healthFetchArtifact(index, "identification.json");
    [served, sources.served] = await healthFetchArtifact(index, "served-accuracy.json");
  } catch { /* shown with what there is */ }
  if (!served) {
    try {
      served = await (await fetch(HEALTH_SEED)).json();
      sources.served = HEALTH_SEED;
    } catch { /* not scored yet */ }
  }
  host.innerHTML = ident || served ? healthHtml(ident, served, sources)
    : `<p>The figures could not be loaded just now. Try again in a minute.</p>`;
}

if (typeof document !== "undefined" && document.getElementById("health")) healthInit();
