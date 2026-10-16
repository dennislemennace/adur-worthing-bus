/* One route's report: report.html?service=700&operator=SCSO
 *
 * Reads the route's file from the published reliability build
 * (scripts/build_route_reports.py), the same build the journey times and the
 * delay map come from, and lays it out to be read, printed or downloaded.
 * Every figure keeps its denominator beside it, and a row below the floor
 * says how thin it is instead of giving a share. Self-contained, like
 * about.html: it does not need app.js, the map or the API.
 */
"use strict";

const REPORT_BASE = "https://adur-worthing-submissions.dennislemennace.workers.dev/journey-times";
// As OPERATOR_NAMES in app.js, for the operators the reports name.
const REPORT_OPERATORS = {
  SCSO: "Stagecoach South", SCSC: "Stagecoach South", BHBC: "Brighton & Hove Buses",
  COMT: "Compass Travel", CMPA: "Compass Travel", METR: "Metrobus",
  NATX: "National Express", ARBB: "Arriva", ARHE: "Arriva",
};
const REPORT_PERIODS = { "08-10": "Morning peak, 08:00–10:00", "10-16": "Daytime, 10:00–16:00",
  "16-18": "Evening peak, 16:00–18:00", other: "Early and late" };

function esc(value) {
  return String(value == null ? "" : value).replace(/[&<>"']/g, ch =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch]);
}

function reportFileName(service, operator) {
  const safe = String(service || "").replace(/[^A-Za-z0-9_-]/g, "") || "unknown";
  const noc = String(operator || "").replace(/[^A-Za-z0-9]/g, "") || "unknown";
  return `route-report-${safe}-${noc}.json`;
}

function operatorName(code) { return REPORT_OPERATORS[code] || code || ""; }

/** A share with its count: "3,781 of 4,414 (86%)". */
function share(part, whole) {
  if (!whole) return "none timed";
  return `${part.toLocaleString("en-GB")} of ${whole.toLocaleString("en-GB")} (${Math.round(part * 100 / whole)}%)`;
}

function dayLabel(iso) {
  const d = new Date(`${iso}T12:00:00Z`);
  return isNaN(d) ? String(iso) : d.toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" });
}

function minutes(secs) {
  if (secs == null) return "–";
  const m = Math.abs(secs) / 60;
  return `${m < 10 ? m.toFixed(1) : Math.round(m)} min`;
}

function direction(d) { return d ? d.charAt(0).toUpperCase() + d.slice(1) : "Unknown"; }

/** A punctuality cell: the on-time share with its count, or how thin it is. */
function onTimeCell(c, floor) {
  if (!c || !c.judged) return "none timed";
  if (!c.sample_sufficient) {
    return `<span class="thin">${c.judged.toLocaleString("en-GB")} timed over ${c.days} ${c.days === 1 ? "day" : "days"};
      needs ${floor.judged} over ${floor.distinct_days}</span>`;
  }
  return share(c.on_time, c.judged);
}

function identificationHtml(doc) {
  const id = doc.identification;
  if (!id || !id.scheduled) return `<p class="thin">How many timetabled journeys were tracked is not recorded for this route.</p>`;
  const by = id.by || {};
  return `<p>${share(id.tracked, id.scheduled)} journeys timetabled while we were recording were tracked.
    Of those tracked, ${by.journey_id || 0} were identified by the journey the operator's feed named,
    ${by.start || 0} by their line, first stop and start time, and ${by.position || 0} were worked out from
    where and when the bus was, which is less certain.</p>`;
}

function punctualityHtml(doc) {
  const p = doc.punctuality || {};
  const floor = doc.floor || { judged: 30, distinct_days: 5 };
  const std = Math.round(((p.definition || {}).standard || 0.95) * 100);
  const rows = (p.rows || []).map(r => {
    const ex = r.excluding_disruptions;
    const spread = r.daily && r.daily.days >= floor.distinct_days
      ? `${Math.round(r.daily.min * 100)}% to ${Math.round(r.daily.max * 100)}%, typically ${Math.round(r.daily.median * 100)}% (${r.daily.days} days)`
      : `<span class="thin">too few days</span>`;
    return `<tr><th scope="row">${esc(direction(r.direction))}, ${r.day_type === "weekday" ? "weekdays" : "weekends"}</th>
      <td>${onTimeCell(r, floor)}</td><td>${spread}</td>
      <td>${r.judged ? `${r.early.toLocaleString("en-GB")} early, ${r.late.toLocaleString("en-GB")} late, ${r.very_late.toLocaleString("en-GB")} very late` : ""}</td>
      <td>${ex && ex.judged !== r.judged ? onTimeCell(ex, floor) : "same"}</td></tr>`;
  }).join("");
  const periods = (p.by_period || []).map(r => `<tr><th scope="row">${esc(direction(r.direction))}</th>
      <td>${esc(REPORT_PERIODS[r.period] || r.period)}</td><td>${onTimeCell(r, floor)}</td></tr>`).join("");
  const marked = p.timings ? Math.round((p.disrupted_timings || 0) * 100 / p.timings) : 0;
  return `<h2>Punctuality at timing points</h2>
    <p>On time is leaving a timing point between 1 minute early and 5 minutes 59 seconds late. The Traffic
      Commissioners expect ${std}% of departures on services every 10 minutes or less often to be on time.
      ${marked ? `${marked}% of these timings were on journeys through a recorded disruption.` : ""}</p>
    <div class="scroll"><table>
      <thead><tr><th scope="col">Direction and days</th><th scope="col">On time</th><th scope="col">From day to day</th>
        <th scope="col">The rest</th><th scope="col">On time, leaving out disrupted journeys</th></tr></thead>
      <tbody>${rows}</tbody></table></div>
    ${periods ? `<h3>Weekdays, by time of day</h3><div class="scroll"><table>
      <thead><tr><th scope="col">Direction</th><th scope="col">Time of day</th><th scope="col">On time</th></tr></thead>
      <tbody>${periods}</tbody></table></div>` : ""}`;
}

function trendHtml(doc) {
  const floor = doc.floor || { judged: 30, distinct_days: 5 };
  const rows = (doc.trend || []).map(w => `<tr><th scope="row">${esc(w.week)}</th><td>${onTimeCell(w, floor)}</td>
    <td>${w.sample_sufficient ? minutes(w.median_lateness_secs) : ""}</td><td>${w.days}</td></tr>`).join("");
  return rows ? `<h2>Week by week</h2><div class="scroll"><table>
    <thead><tr><th scope="col">Week</th><th scope="col">On time</th><th scope="col">Typical lateness</th><th scope="col">Days recorded</th></tr></thead>
    <tbody>${rows}</tbody></table></div>
    <p class="thin">A week with fewer than ${floor.distinct_days} recorded days shows its count, not a share.</p>` : "";
}

function waitsHtml(doc) {
  const rows = (doc.waits || []).map(w => `<tr><th scope="row">${esc(direction(w.direction))}, ${w.day_type === "weekday" ? "weekdays" : "weekends"}</th>
    <td>${esc(w.stop_name || w.atco)}</td>
    <td>${w.sample_sufficient ? `${minutes(w.swt_secs)} timetabled, ${minutes(w.awt_secs)} recorded:
      ${minutes(w.ewt_secs)} ${w.ewt_secs >= 0 ? "longer" : "shorter"}` : `<span class="thin">${w.days_eligible} complete ${w.days_eligible === 1 ? "day" : "days"} so far; needs 5</span>`}</td>
    <td>${w.sample_sufficient ? `${w.gaps_over_15} of ${w.gaps_tested} gaps over 15 min` : ""}</td></tr>`).join("");
  return rows ? `<h2>Waiting, on frequent stretches</h2>
    <p>The average wait for someone arriving at random, against what the timetable implies, at the direction's
      busiest timing point. Only days where nearly every timetabled bus was seen count.</p>
    <div class="scroll"><table><thead><tr><th scope="col">Direction</th><th scope="col">At</th>
      <th scope="col">Average wait</th><th scope="col">Long gaps</th></tr></thead><tbody>${rows}</tbody></table></div>` : "";
}

function bottlenecksHtml(doc) {
  const rows = (doc.bottlenecks || []).map(b => `<tr><th scope="row">${esc(direction(b.direction))}</th>
    <td>${esc(b.from_name)} to ${esc(b.to_name)}</td>
    <td>${minutes(b.median_gained_secs)}</td>
    <td>${b.day_type === "weekday" ? "Weekdays" : "Weekends"}, ${esc(REPORT_PERIODS[b.period] || (b.hour != null ? `${String(b.hour).padStart(2, "0")}:00` : ""))}</td>
    <td>${b.journeys} journeys, ${b.distinct_days} days</td></tr>`).join("");
  return `<h2>Where buses lose most time</h2>
    ${rows ? `<div class="scroll"><table><thead><tr><th scope="col">Direction</th><th scope="col">Stretch</th>
      <th scope="col">Typical time lost</th><th scope="col">When</th><th scope="col">Evidence</th></tr></thead>
      <tbody>${rows}</tbody></table></div>
      <p class="thin">Time lost includes stop dwell and holding as well as traffic: a stretch here is a place to
        look, not proof of its cause.</p>`
      : `<p class="thin">No stretch has enough journeys yet (30 over 5 days in one time of day).</p>`}`;
}

function disruptionsHtml(doc) {
  const items = (doc.disruptions || []).map(d => `<li>${esc(d.label)}${d.from ? `, from ${esc(dayLabel(String(d.from).slice(0, 10)))}` : ""}${
    d.to ? ` to ${esc(dayLabel(String(d.to).slice(0, 10)))}` : ""}: ${d.journeys_marked} journeys marked.</li>`).join("");
  return items ? `<h2>Recorded disruptions in this period</h2><ul>${items}</ul>` : "";
}

function sourcesHtml(doc, index) {
  const known = new Map(((index && index.observation_sources) || []).map(s => [s.sha256, s]));
  const items = (doc.sources || []).map(s => {
    const src = known.get(s.sha256);
    const url = src && /^https:\/\//.test(src.url || "") ? src.url : "";
    return `<li>${url ? `<a href="${esc(url)}">${esc(s.day)}</a>` : esc(s.day)} <code>${esc(String(s.sha256 || "").slice(0, 12))}</code></li>`;
  }).join("");
  return `<h2>Method and evidence</h2>
    <p>${esc(doc.method)}</p>
    <ul>${(doc.caveats || []).map(c => `<li>${esc(c)}</li>`).join("")}</ul>
    <p>Method version ${esc(doc.method_version)}; observation methods ${esc((doc.method_versions || []).join(", "))};
      timetable builds ${esc((doc.data_versions || []).join(", "))}. Built ${esc(doc.as_of)}${
      index && index.build_id ? `, publication <code>${esc(String(index.build_id).slice(0, 12))}</code>` : ""}.</p>
    <details><summary>The ${(doc.sources || []).length} daily observation files behind it</summary><ul class="sources">${items}</ul></details>`;
}

/** The whole report, from the route's file and the build's index. */
function reportHtml(doc, index) {
  const name = `${esc(doc.service)}, ${esc(operatorName(doc.operator))}`;
  const [from, to] = doc.window || [];
  return `<h1>Route report: ${name}</h1>
    <p class="lede">${from ? `${esc(dayLabel(from))} to ${esc(dayLabel(to))}` : ""}${(doc.days || []).length
      ? `, ${doc.days.length} days recorded` : ""}.</p>
    <p class="draft"><strong>Draft.</strong> Stop times come from the buses' own location reports and have not yet
      been checked against times someone saw. Read every figure with its count, and the caveats below.</p>
    <h2>What was measured</h2>
    ${identificationHtml(doc)}
    ${punctualityHtml(doc)}
    ${trendHtml(doc)}
    ${waitsHtml(doc)}
    ${bottlenecksHtml(doc)}
    ${disruptionsHtml(doc)}
    ${sourcesHtml(doc, index)}`;
}

/** The report's tables as CSV, one row per count. */
function reportCsv(doc) {
  const rows = [["section", "direction", "day_type", "period_or_week", "judged", "on_time", "early", "late",
    "very_late", "days", "sample_sufficient"]];
  const p = doc.punctuality || {};
  const push = (section, r, key) => rows.push([section, r.direction || "", r.day_type || "", key || "",
    r.judged, r.on_time, r.early, r.late, r.very_late, r.days, r.sample_sufficient]);
  (p.rows || []).forEach(r => {
    push("punctuality", r);
    if (r.excluding_disruptions) {
      push("punctuality_excluding_disruptions", { ...r.excluding_disruptions, direction: r.direction, day_type: r.day_type });
    }
  });
  (p.by_period || []).forEach(r => push("by_period", r, r.period));
  (doc.trend || []).forEach(r => push("by_week", r, r.week));
  const cell = v => {
    const s = String(v == null ? "" : v);
    // A cell a spreadsheet would run as a formula is kept as text.
    const safe = /^[=+\-@]/.test(s) ? `'${s}` : s;
    return /[",\n]/.test(safe) ? `"${safe.replace(/"/g, '""')}"` : safe;
  };
  return rows.map(r => r.map(cell).join(",")).join("\n") + "\n";
}

async function reportFetch(path) {
  const res = await fetch(`${REPORT_BASE}/${path}`);
  if (!res.ok) throw new Error(`${path} ${res.status}`);
  return res.json();
}

async function reportInit() {
  const host = document.getElementById("report");
  const picker = document.getElementById("report-service");
  const params = new URLSearchParams(location.search);
  const build = /^[a-f0-9]{64}$/.test(params.get("build") || "") ? params.get("build") : "";
  let index;
  try {
    index = await reportFetch(build ? `builds/${build}/index.json` : "index.json");
  } catch (err) {
    host.innerHTML = `<p>The published reports could not be loaded. Try again in a moment.</p>`;
    return;
  }
  const names = Object.keys(index.artifacts || {}).filter(n => /^route-report-.+\.json$/.test(n));
  const choices = names.map(n => {
    const m = n.match(/^route-report-(.+)-([A-Za-z0-9]+)\.json$/);
    return m ? { file: n, service: m[1], operator: m[2] } : null;
  }).filter(Boolean).sort((a, b) => a.service.localeCompare(b.service, "en", { numeric: true })
    || a.operator.localeCompare(b.operator));
  picker.innerHTML = `<option value="">Choose a route…</option>` + choices.map(c =>
    `<option value="${esc(c.file)}">${esc(c.service)}, ${esc(operatorName(c.operator))}</option>`).join("");
  const wanted = reportFileName(params.get("service"), params.get("operator"));
  const actions = document.getElementById("report-actions");
  const show = async (file) => {
    const entry = (index.artifacts || {})[file];
    if (!entry) {
      host.innerHTML = file ? `<p>There is no report for that route in this publication.</p>` : `<p>Choose a route above.</p>`;
      actions.hidden = true;
      return;
    }
    host.innerHTML = `<p>Loading…</p>`;
    try {
      const doc = await reportFetch(entry.file);
      host.innerHTML = reportHtml(doc, index);
      document.title = `Route report: ${doc.service}, ${operatorName(doc.operator)} | Worthing Brighton Bus`;
      actions.hidden = false;
      document.getElementById("report-csv").onclick = () => {
        const blob = new Blob([reportCsv(doc)], { type: "text/csv" });
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = file.replace(/\.json$/, ".csv");
        a.click();
        setTimeout(() => URL.revokeObjectURL(a.href), 1000);
      };
      document.getElementById("report-json").href = `${REPORT_BASE}/${entry.file}`;
    } catch (err) {
      host.innerHTML = `<p>This report could not be loaded. Try again in a moment.</p>`;
    }
  };
  picker.addEventListener("change", () => {
    const c = choices.find(x => x.file === picker.value);
    const url = new URL(location.href);
    if (c) { url.searchParams.set("service", c.service); url.searchParams.set("operator", c.operator); }
    history.replaceState(null, "", url);
    show(picker.value);
  });
  if (names.includes(wanted)) picker.value = wanted;
  show(names.includes(wanted) ? wanted : "");
  document.getElementById("report-print").onclick = () => window.print();
}

if (typeof document !== "undefined" && document.getElementById("report")) reportInit();
