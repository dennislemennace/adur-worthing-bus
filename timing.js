/* Departure timing log: timing.html
 *
 * A phone page for timing buses by hand at a stop, to check the site's own
 * stop times against times someone saw (docs/ROADMAP.md, "Check our stop
 * times against an independent record"). Tap when the doors open and when the
 * bus leaves; note its route and the fleet number or registration on it, which
 * is what lets each time be matched to exactly the bus we recorded
 * (scripts/validate_hand_timings.py).
 *
 * Everything stays on the phone (localStorage) until it is exported. Nothing
 * is sent anywhere. Self-contained, like report.html: no app.js, no API.
 */
"use strict";

const TIMING_KEY = "timing-log-v1";
const TIMING_STOPS_URL = "data/stops.json";

function tEsc(value) {
  return String(value == null ? "" : value).replace(/[&<>"']/g, ch =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch]);
}

/** Metres between two points, near enough at this scale. */
function timingDistance(lat1, lon1, lat2, lon2) {
  const rad = Math.PI / 180;
  const x = (lon2 - lon1) * rad * Math.cos(((lat1 + lat2) / 2) * rad);
  const y = (lat2 - lat1) * rad;
  return Math.sqrt(x * x + y * y) * 6371000;
}

/** The `n` stops nearest a position, nearest first, with their distance. */
function timingNearest(stops, lat, lon, n = 6) {
  return stops.map(s => ({ ...s, metres: Math.round(timingDistance(lat, lon, s.latitude, s.longitude)) }))
    .sort((a, b) => a.metres - b.metres).slice(0, n);
}

/** Stops whose name or place contains every word typed. */
function timingSearch(stops, text, n = 12) {
  const words = String(text || "").toLowerCase().split(/\s+/).filter(Boolean);
  if (!words.length) return [];
  return stops.filter(s => {
    const hay = `${s.name} ${s.locality || ""} ${s.towards || ""} ${s.atco_code}`.toLowerCase();
    return words.every(w => hay.includes(w));
  }).slice(0, n);
}

/** "10950", "SCSO-10950" and "scso 10950" are one bus; so are "YX12 FRC" and
 *  "YX12_FRC". As scripts/validate_hand_timings.py normalises them. */
function timingVehicleKey(text) {
  return String(text || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
}

/** The London date of an instant, YYYY-MM-DD. */
function timingLondonDay(iso) {
  const parts = new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/London",
    year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(new Date(iso));
  const get = type => parts.find(p => p.type === type).value;
  return `${get("year")}-${get("month")}-${get("day")}`;
}

function timingClock(iso) {
  return iso ? new Date(iso).toLocaleTimeString("en-GB", { timeZone: "Europe/London", hour12: false }) : "";
}

/** Record a tap. `kind` is "arrived" or "departed". A departure completes the
 *  latest entry at the same stop still waiting for one, so "doors open" then
 *  "left" makes one row; a departure on its own makes its own row. Returns the
 *  new list, newest first. */
function timingTap(entries, kind, { stop, route, vehicle, offsetMs = 0, now = new Date() }) {
  const at = now.toISOString();
  const list = entries.map(e => ({ ...e }));
  if (kind === "departed") {
    const open = list.find(e => e.atco === stop.atco_code && e.arrived_at && !e.departed_at);
    if (open) {
      Object.assign(open, { departed_at: at,
        route: open.route || route || "", vehicle: open.vehicle || vehicle || "" });
      return list;
    }
  }
  list.unshift({
    id: `${now.getTime().toString(36)}${Math.random().toString(36).slice(2, 6)}`,
    atco: stop.atco_code, stop_name: stop.name, towards: stop.towards || "",
    route: route || "", vehicle: vehicle || "",
    arrived_at: kind === "arrived" ? at : "", departed_at: kind === "departed" ? at : "",
    clock_offset_ms: Math.round(offsetMs), note: "",
  });
  return list;
}

const TIMING_COLUMNS = ["id", "day", "atco", "stop_name", "towards", "route", "vehicle",
  "arrived_at", "departed_at", "clock_offset_ms", "note"];

/** The log as CSV, one row per bus, oldest first. A cell a spreadsheet would
 *  run as a formula is kept as text. */
function timingCsv(entries) {
  const cell = v => {
    const s = String(v == null ? "" : v);
    const safe = /^[=+\-@]/.test(s) ? `'${s}` : s;
    return /[",\n]/.test(safe) ? `"${safe.replace(/"/g, '""')}"` : safe;
  };
  const rows = entries.slice().reverse().map(e => TIMING_COLUMNS.map(c =>
    c === "day" ? timingLondonDay(e.departed_at || e.arrived_at) : e[c]));
  return [TIMING_COLUMNS, ...rows].map(r => r.map(cell).join(",")).join("\n") + "\n";
}

function timingLoad() {
  try { return JSON.parse(localStorage.getItem(TIMING_KEY) || "[]"); } catch { return []; }
}

function timingSave(entries) {
  try { localStorage.setItem(TIMING_KEY, JSON.stringify(entries)); return true; } catch { return false; }
}

/** How far the phone's clock is from the server's, in ms (positive: phone
 *  ahead), from the Date header of a fresh request. Good to about a second,
 *  which is all a header carries; phones that set their own time are closer. */
async function timingClockOffset() {
  const sent = Date.now();
  const res = await fetch(`${location.pathname}?clock=${sent}`, { method: "HEAD", cache: "no-store" });
  const received = Date.now();
  const server = Date.parse(res.headers.get("Date") || "");
  if (isNaN(server)) return null;
  return Math.round((sent + received) / 2 - (server + 500));
}

function timingInit() {
  const $ = id => document.getElementById(id);
  let stops = [], stop = null, entries = timingLoad(), offsetMs = 0;

  const renderStop = () => {
    $("stop-chosen").innerHTML = stop
      ? `<strong>${tEsc(stop.name)}</strong>${stop.towards ? `, towards ${tEsc(stop.towards)}` : ""}
         <span class="muted">${tEsc(stop.locality || "")} · ${tEsc(stop.atco_code)}</span>`
      : `<span class="muted">No stop chosen yet.</span>`;
    $("route-list").innerHTML = (stop?.services || []).map(s => `<option value="${tEsc(s)}">`).join("");
    for (const b of document.querySelectorAll(".tap")) b.disabled = !stop;
  };
  const choices = (list) => {
    $("stop-choices").innerHTML = list.map(s => `<li><button type="button" class="choice" data-atco="${tEsc(s.atco_code)}">
      ${tEsc(s.name)}${s.towards ? `, towards ${tEsc(s.towards)}` : ""}
      <span class="muted">${tEsc(s.locality || "")}${s.metres != null ? ` · ${s.metres} m` : ""}</span></button></li>`).join("")
      || `<li class="muted">No stops found.</li>`;
  };
  const renderLog = () => {
    $("count").textContent = `${entries.length} ${entries.length === 1 ? "bus" : "buses"} logged`;
    $("log").innerHTML = entries.map(e => `<li data-id="${tEsc(e.id)}">
      <div class="row-head"><strong>${tEsc(e.stop_name)}</strong>
        <span>${e.arrived_at ? `doors ${timingClock(e.arrived_at)}` : ""}${e.arrived_at && e.departed_at ? " · " : ""}${
          e.departed_at ? `left ${timingClock(e.departed_at)}` : `<em>not left yet</em>`}</span></div>
      <label>Route <input data-field="route" value="${tEsc(e.route)}" autocomplete="off"></label>
      <label>Fleet no. or reg <input data-field="vehicle" value="${tEsc(e.vehicle)}" autocomplete="off" autocapitalize="characters"></label>
      <label>Note <input data-field="note" value="${tEsc(e.note)}" autocomplete="off"></label>
      <button type="button" class="delete" data-act="delete">Delete</button></li>`).join("");
  };
  const persist = () => {
    if (!timingSave(entries)) $("status").textContent = "This browser would not save the log: export it before closing the page.";
  };
  const tick = () => {
    $("clock").textContent = new Date().toLocaleTimeString("en-GB", { timeZone: "Europe/London", hour12: false });
  };
  setInterval(tick, 250);
  tick();

  fetch(TIMING_STOPS_URL).then(r => r.json()).then(doc => {
    stops = doc.stops || [];
    $("status").textContent = `${stops.length} stops loaded.`;
  }).catch(() => { $("status").textContent = "The stop list did not load. Check your signal and reload."; });

  timingClockOffset().then(ms => {
    if (ms == null) return;
    offsetMs = Math.abs(ms) >= 2000 ? ms : 0;
    $("offset").textContent = Math.abs(ms) < 2000
      ? "Your phone's clock agrees with the server's, to within a second or two."
      : `Your phone's clock is ${Math.abs(Math.round(ms / 1000))} s ${ms > 0 ? "fast" : "slow"}. That is recorded with each time and corrected for.`;
  }).catch(() => {});

  $("near").addEventListener("click", () => {
    if (!navigator.geolocation) { $("status").textContent = "This phone will not share its location."; return; }
    $("status").textContent = "Finding you…";
    navigator.geolocation.getCurrentPosition(pos => {
      choices(timingNearest(stops, pos.coords.latitude, pos.coords.longitude));
      $("status").textContent = `Within about ${Math.round(pos.coords.accuracy)} m. Choose the pole you are at.`;
    }, () => { $("status").textContent = "Location was refused or timed out. Search instead."; },
    { enableHighAccuracy: true, timeout: 15000 });
  });
  $("search").addEventListener("input", e => choices(timingSearch(stops, e.target.value)));
  $("stop-choices").addEventListener("click", e => {
    const b = e.target.closest("button.choice");
    if (!b) return;
    stop = stops.find(s => s.atco_code === b.dataset.atco) || null;
    $("stop-choices").innerHTML = "";
    $("search").value = "";
    renderStop();
  });
  for (const b of document.querySelectorAll(".tap")) {
    b.addEventListener("click", () => {
      if (!stop) return;
      entries = timingTap(entries, b.dataset.kind, { stop, route: $("route").value.trim(),
        vehicle: $("vehicle").value.trim(), offsetMs });
      persist();
      renderLog();
      if (b.dataset.kind === "departed") { $("route").value = ""; $("vehicle").value = ""; }
      if (navigator.vibrate) navigator.vibrate(40);
    });
  }
  $("log").addEventListener("input", e => {
    const li = e.target.closest("li[data-id]");
    const entry = li && entries.find(x => x.id === li.dataset.id);
    if (!entry || !e.target.dataset.field) return;
    entry[e.target.dataset.field] = e.target.value;
    persist();
  });
  $("log").addEventListener("click", e => {
    if (e.target.dataset.act !== "delete") return;
    const li = e.target.closest("li[data-id]");
    if (li && confirm("Delete this bus from the log?")) {
      entries = entries.filter(x => x.id !== li.dataset.id);
      persist();
      renderLog();
    }
  });
  const file = () => new File([timingCsv(entries)], `timings-${timingLondonDay(new Date().toISOString())}.csv`,
    { type: "text/csv" });
  $("download").addEventListener("click", () => {
    const f = file();
    const a = document.createElement("a");
    a.href = URL.createObjectURL(f);
    a.download = f.name;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  });
  $("share").addEventListener("click", async () => {
    const f = file();
    try {
      if (navigator.canShare && navigator.canShare({ files: [f] })) await navigator.share({ files: [f], title: f.name });
      else { await navigator.clipboard.writeText(timingCsv(entries)); $("status").textContent = "Copied the log as CSV."; }
    } catch { /* cancelled */ }
  });
  $("clear").addEventListener("click", () => {
    if (entries.length && confirm(`Delete all ${entries.length} logged buses? Export first if you need them.`)) {
      entries = [];
      persist();
      renderLog();
    }
  });
  renderStop();
  renderLog();
}

if (typeof document !== "undefined" && document.getElementById("timing")) timingInit();
