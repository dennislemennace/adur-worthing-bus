/* On-bus door timing: ride.html
 *
 * The sibling of timing.html. There someone stands at one pole and times every
 * bus that calls; here someone rides one bus and taps when its doors open and
 * close at each stop. The phone's position at the tap says which stop it was,
 * guessed from the stops the route serves and correctable in the list.
 *
 * Each stop becomes one row with timing.html's columns first, so
 * scripts/validate_hand_timings.py reads a ride exactly as it reads a stop
 * session: arrived_at is when the doors opened, departed_at when they closed
 * (`departed_basis`: a few seconds before the bus pulls away, which a timer at
 * the stop would have tapped instead). The ride's own columns follow.
 *
 * Everything stays on the phone (localStorage) until it is exported. Nothing
 * is sent anywhere. Self-contained, like timing.html: no app.js, no API.
 */
"use strict";

const RIDE_KEY = "ride-log-v1";
const RIDE_STOPS_URL = "data/stops.json";
const RIDE_DETAILS_URL = "data/stop_details.json";
/* A stop the route serves this close to the tap is taken as the stop. A bus
   stands a little before or after its flag, and a phone fix in a street of
   tall buildings is often 20 to 40 m out. */
const RIDE_ROUTE_STOP_METRES = 80;
/* Any stop this close, when none on the route is: the route list in stops.json
   is this week's timetable, and a diverted bus calls where it can. */
const RIDE_ANY_STOP_METRES = 40;

function rEsc(value) {
  return String(value == null ? "" : value).replace(/[&<>"']/g, ch =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch]);
}

/** Metres between two points, near enough at this scale. As timing.js. */
function rideDistance(lat1, lon1, lat2, lon2) {
  const rad = Math.PI / 180;
  const x = (lon2 - lon1) * rad * Math.cos(((lat1 + lat2) / 2) * rad);
  const y = (lat2 - lat1) * rad;
  return Math.sqrt(x * x + y * y) * 6371000;
}

/** Which stop the bus is at. `choices` are the nearest stops, nearest first,
 *  for correcting the guess; `best` is the nearest the route serves within
 *  reach, else any stop very close, else null: no guess is better than a
 *  wrong one, which the validator would score as that stop's error. */
function rideStopGuess(stops, lat, lon, route, n = 6) {
  const want = String(route || "").trim().toUpperCase();
  const near = stops.map(s => ({ ...s, metres: Math.round(rideDistance(lat, lon, s.latitude, s.longitude)) }))
    .sort((a, b) => a.metres - b.metres);
  const onRoute = near.find(s => s.metres <= RIDE_ROUTE_STOP_METRES
    && (s.services || []).some(v => String(v).toUpperCase() === want));
  const close = near.find(s => s.metres <= RIDE_ANY_STOP_METRES);
  return { best: onRoute || close || null, choices: near.slice(0, n) };
}

/** Record a tap. `kind` is "open" or "closed". Doors closing completes this
 *  ride's latest stop still open; closing with no open tap, or opening, makes
 *  a new stop. Returns the new list, newest first. */
function rideTap(entries, kind, { ride, stop, position, offsetMs = 0, now = new Date() }) {
  const at = now.toISOString();
  const list = entries.map(e => ({ ...e }));
  if (kind === "closed") {
    const open = list.find(e => e.ride_id === ride.ride_id && e.arrived_at && !e.departed_at);
    if (open) { open.departed_at = at; return list; }
  }
  const seq = list.filter(e => e.ride_id === ride.ride_id).reduce((m, e) => Math.max(m, e.seq || 0), 0) + 1;
  list.unshift({
    id: `${now.getTime().toString(36)}${Math.random().toString(36).slice(2, 6)}`,
    ride_id: ride.ride_id, seq,
    atco: stop ? stop.atco_code : "", stop_name: stop ? stop.name : "", towards: stop ? stop.towards || "" : "",
    route: ride.route || "", vehicle: ride.vehicle || "",
    arrived_at: kind === "open" ? at : "", departed_at: kind === "closed" ? at : "",
    lat: position ? Number(position.lat.toFixed(6)) : "", lon: position ? Number(position.lon.toFixed(6)) : "",
    accuracy_m: position && position.accuracy != null ? Math.round(position.accuracy) : "",
    clock_offset_ms: Math.round(offsetMs), note: "",
  });
  return list;
}

/* timing.js's TIMING_COLUMNS first, in its order, then the ride's own. */
const RIDE_COLUMNS = ["id", "day", "atco", "stop_name", "towards", "route", "vehicle",
  "arrived_at", "departed_at", "clock_offset_ms", "note",
  "ride_id", "seq", "lat", "lon", "accuracy_m", "departed_basis"];

/** The London date of an instant, YYYY-MM-DD. */
function rideLondonDay(iso) {
  const parts = new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/London",
    year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(new Date(iso));
  const get = type => parts.find(p => p.type === type).value;
  return `${get("year")}-${get("month")}-${get("day")}`;
}

/** The log as CSV, one row per stop, oldest first. A cell a spreadsheet would
 *  run as a formula is kept as text. */
function rideCsv(entries) {
  const cell = v => {
    const s = String(v == null ? "" : v);
    const safe = /^[=+\-@]/.test(s) ? `'${s}` : s;
    return /[",\n]/.test(safe) ? `"${safe.replace(/"/g, '""')}"` : safe;
  };
  const rows = entries.slice().reverse().map(e => RIDE_COLUMNS.map(c =>
    c === "day" ? rideLondonDay(e.departed_at || e.arrived_at)
      : c === "departed_basis" ? "doors closed" : e[c]));
  return [RIDE_COLUMNS, ...rows].map(r => r.map(cell).join(",")).join("\n") + "\n";
}

function rideClock(iso) {
  return iso ? new Date(iso).toLocaleTimeString("en-GB", { timeZone: "Europe/London", hour12: false }) : "";
}

function rideLoad() {
  try {
    const doc = JSON.parse(localStorage.getItem(RIDE_KEY) || "{}");
    return { ride: doc.ride || null, entries: Array.isArray(doc.entries) ? doc.entries : [] };
  } catch { return { ride: null, entries: [] }; }
}

function rideSave(doc) {
  try { localStorage.setItem(RIDE_KEY, JSON.stringify(doc)); return true; } catch { return false; }
}

/** How far the phone's clock is from the server's, in ms (positive: phone
 *  ahead). As timing.js. */
async function rideClockOffset() {
  const sent = Date.now();
  const res = await fetch(`${location.pathname}?clock=${sent}`, { method: "HEAD", cache: "no-store" });
  const received = Date.now();
  const server = Date.parse(res.headers.get("Date") || "");
  if (isNaN(server)) return null;
  return Math.round((sent + received) / 2 - (server + 500));
}

function rideInit() {
  const $ = id => document.getElementById(id);
  let stops = [], offsetMs = 0, fix = null, watch = null, wake = null;
  let { ride, entries } = rideLoad();
  const stopsByAtco = new Map();

  const persist = () => {
    if (!rideSave({ ride, entries })) $("status").textContent = "This browser would not save the log: export it before closing the page.";
  };
  const say = (text) => { $("status").textContent = text; };

  // Keep the screen on while riding, where the phone allows it: a screen that
  // locks between stops is a stop missed.
  const holdScreen = async () => {
    try { if (navigator.wakeLock && !wake) wake = await navigator.wakeLock.request("screen"); } catch { wake = null; }
  };
  const releaseScreen = () => { try { if (wake) wake.release(); } catch { /* already gone */ } wake = null; };
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible" && ride) { wake = null; holdScreen(); }
  });

  const follow = () => {
    if (!navigator.geolocation) { $("gps").textContent = "This phone will not share its location: choose each stop by hand."; return; }
    if (watch != null) return;
    watch = navigator.geolocation.watchPosition(pos => {
      fix = { lat: pos.coords.latitude, lon: pos.coords.longitude, accuracy: pos.coords.accuracy, at: Date.now() };
      $("gps").textContent = `Location: within about ${Math.round(pos.coords.accuracy)} m.`;
    }, () => { $("gps").textContent = "Location was refused or lost: choose each stop by hand in the list."; },
    { enableHighAccuracy: true, maximumAge: 2000, timeout: 20000 });
  };
  const unfollow = () => {
    if (watch != null && navigator.geolocation) navigator.geolocation.clearWatch(watch);
    watch = null;
  };

  const renderRide = () => {
    $("ride-status").innerHTML = ride
      ? `<strong>Riding the ${rEsc(ride.route || "?")}</strong>${ride.vehicle ? `, bus ${rEsc(ride.vehicle)}` : ""}
         <span class="muted">since ${rEsc(rideClock(ride.started_at))}</span>`
      : `<span class="muted">Not riding. Type the route and start when you board.</span>`;
    $("start").hidden = !!ride;
    $("end").hidden = !ride;
    $("route").disabled = !!ride;
    $("vehicle").disabled = !!ride;
    for (const b of document.querySelectorAll(".tap")) b.disabled = !ride;
  };

  const choicesHtml = (e) => {
    const near = (e.lat !== "" && e.lat != null)
      ? rideStopGuess(stops, e.lat, e.lon, e.route).choices : [];
    const options = near.some(s => s.atco_code === e.atco) || !e.atco ? near
      : [stopsByAtco.get(e.atco) || { atco_code: e.atco, name: e.stop_name, metres: null }, ...near];
    return `<select data-field="atco" aria-label="Stop">
      <option value="">${e.atco ? "Not a stop" : "Choose the stop"}</option>
      ${options.map(s => `<option value="${rEsc(s.atco_code)}"${s.atco_code === e.atco ? " selected" : ""}>${
        rEsc(s.name)}${s.letter ? ` (${rEsc(s.letter)})` : ""}${s.metres != null ? `, ${s.metres} m` : ""}</option>`).join("")}
    </select>`;
  };

  const renderLog = () => {
    const n = entries.length;
    $("count").textContent = `${n} ${n === 1 ? "stop" : "stops"} logged`;
    $("log").innerHTML = entries.map(e => {
      const dwell = e.arrived_at && e.departed_at
        ? Math.round((Date.parse(e.departed_at) - Date.parse(e.arrived_at)) / 1000) : null;
      return `<li data-id="${rEsc(e.id)}">
        <div class="row-head"><strong>${rEsc(e.route)} · stop ${rEsc(e.seq)}</strong>
          <span>${e.arrived_at ? `open ${rideClock(e.arrived_at)}` : "<em>no open tap</em>"}${
            e.departed_at ? ` · closed ${rideClock(e.departed_at)}` : " · <em>still open</em>"}${
            dwell != null ? ` · ${dwell} s` : ""}</span></div>
        <label>Stop ${choicesHtml(e)}</label>
        <label>Note <input data-field="note" value="${rEsc(e.note)}" autocomplete="off"
          placeholder="e.g. wheelchair ramp, driver change"></label>
        <button type="button" class="delete" data-act="delete">Delete</button></li>`;
    }).join("");
  };

  const tick = () => {
    $("clock").textContent = new Date().toLocaleTimeString("en-GB", { timeZone: "Europe/London", hour12: false });
  };
  setInterval(tick, 250);
  tick();

  fetch(RIDE_STOPS_URL).then(r => r.json()).then(doc => {
    stops = doc.stops || [];
    for (const s of stops) stopsByAtco.set(s.atco_code, s);
    say(`${stops.length} stops loaded.`);
    // The letters on the flags, so the stop list can tell two poles apart.
    return fetch(RIDE_DETAILS_URL).then(r => r.json()).then(details => {
      const rows = details.stops || {};
      for (const s of stops) {
        const m = /^(?:stop|stand|bay)\s+(\S{1,3})$/i.exec(String((rows[s.atco_code] || [])[0] || "").trim());
        s.letter = m ? m[1].toUpperCase() : "";
      }
      renderLog();
    }).catch(() => renderLog());
  }).catch(() => say("The stop list did not load. Check your signal and reload."));

  rideClockOffset().then(ms => {
    if (ms == null) return;
    offsetMs = Math.abs(ms) >= 2000 ? ms : 0;
    $("offset").textContent = Math.abs(ms) < 2000
      ? "Your phone's clock agrees with the server's, to within a second or two."
      : `Your phone's clock is ${Math.abs(Math.round(ms / 1000))} s ${ms > 0 ? "fast" : "slow"}. That is recorded with each time and corrected for.`;
  }).catch(() => {});

  $("start").addEventListener("click", () => {
    const route = $("route").value.trim().toUpperCase();
    if (!route) { say("Type the route first: it is how each stop is matched to our records."); $("route").focus(); return; }
    const now = new Date();
    ride = { ride_id: `${now.getTime().toString(36)}`, route, vehicle: $("vehicle").value.trim().toUpperCase(),
      started_at: now.toISOString() };
    persist();
    follow();
    holdScreen();
    renderRide();
    say("Tap Doors open as the doors open at each stop, and Doors closed as they close.");
  });
  $("end").addEventListener("click", () => {
    if (!confirm("End this ride? The stops stay in the log below.")) return;
    ride = null;
    persist();
    unfollow();
    releaseScreen();
    renderRide();
    say("Ride ended. Export the log when you are done, or start another ride.");
  });
  for (const b of document.querySelectorAll(".tap")) {
    b.addEventListener("click", () => {
      if (!ride) return;
      // A fix older than half a minute is from somewhere else on the route.
      const position = fix && Date.now() - fix.at < 30000 ? fix : null;
      const guess = position && stops.length ? rideStopGuess(stops, position.lat, position.lon, ride.route).best : null;
      const kind = b.dataset.kind;
      entries = rideTap(entries, kind, { ride, stop: guess, position, offsetMs });
      persist();
      renderLog();
      const row = entries.find(e => e.ride_id === ride.ride_id && (kind === "open" ? e.arrived_at : e.departed_at));
      say(`Doors ${kind === "open" ? "open" : "closed"} at ${rideClock(new Date().toISOString())}${
        row && row.stop_name ? `, ${row.stop_name}` : position ? ", stop not recognised: choose it in the list" : ", no location: choose the stop in the list"}.`);
      if (navigator.vibrate) navigator.vibrate(40);
    });
  }
  $("log").addEventListener("change", e => {
    const li = e.target.closest("li[data-id]");
    const entry = li && entries.find(x => x.id === li.dataset.id);
    if (!entry || e.target.dataset.field !== "atco") return;
    const s = stopsByAtco.get(e.target.value);
    Object.assign(entry, { atco: s ? s.atco_code : "", stop_name: s ? s.name : "", towards: s ? s.towards || "" : "" });
    persist();
  });
  $("log").addEventListener("input", e => {
    const li = e.target.closest("li[data-id]");
    const entry = li && entries.find(x => x.id === li.dataset.id);
    if (!entry || e.target.dataset.field !== "note") return;
    entry.note = e.target.value;
    persist();
  });
  $("log").addEventListener("click", e => {
    if (e.target.dataset.act !== "delete") return;
    const li = e.target.closest("li[data-id]");
    if (li && confirm("Delete this stop from the log?")) {
      entries = entries.filter(x => x.id !== li.dataset.id);
      persist();
      renderLog();
    }
  });
  const file = () => new File([rideCsv(entries)], `rides-${rideLondonDay(new Date().toISOString())}.csv`,
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
      else { await navigator.clipboard.writeText(rideCsv(entries)); say("Copied the log as CSV."); }
    } catch { /* cancelled */ }
  });
  $("clear").addEventListener("click", () => {
    if (entries.length && confirm(`Delete all ${entries.length} logged stops? Export first if you need them.`)) {
      entries = [];
      persist();
      renderLog();
    }
  });

  if (ride) { follow(); holdScreen(); }
  renderRide();
  renderLog();
}

if (typeof document !== "undefined" && document.getElementById("ride")) rideInit();
