/* Departure timing log: timing.html
 *
 * A phone page for timing buses by hand at a stop, to check the site's own
 * stop times against times someone saw (docs/ROADMAP.md, "Check our stop
 * times against an independent record"). Tap when the doors open and when the
 * bus leaves, and note its route: scripts/validate_hand_timings.py matches each
 * to the bus of that route we recorded at the same pole. A registration plate
 * is optional, for telling apart two buses of one route at the stop together.
 *
 * A stop usually has a pole on each side of the road, one for each direction.
 * Standing between them, the timer keeps both: an L and an R side, each its
 * own pole, switched with one tap. Every row records the pole of the side lit
 * when it was tapped, so the validator matches it to that pole and never the
 * other side.
 *
 * Everything stays on the phone (localStorage) until it is exported. Nothing
 * is sent anywhere. Self-contained, like report.html: no app.js, no API.
 */
"use strict";

const TIMING_KEY = "timing-log-v1";
const TIMING_SIDES_KEY = "timing-sides-v1";
const TIMING_EXPORTED_KEY = "timing-exported-v1";
const TIMING_STOPS_URL = "data/stops.json";
const TIMING_DETAILS_URL = "data/stop_details.json";
/* The pole across the road is within this of the one chosen. */
const TIMING_OTHER_SIDE_METRES = 150;

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

/** "B6" from NaPTAN's "Stop B6" (or "Stand 3", "Bay 2"); "" where the flag
 *  carries no letter. As app.js's stopLetter. */
function timingLetter(indicator) {
  const m = /^(?:stop|stand|bay)\s+(\S{1,3})$/i.exec(String(indicator || "").trim());
  return m ? m[1].toUpperCase() : "";
}

const TIMING_COMPASS = { N: 0, NE: 45, E: 90, SE: 135, S: 180, SW: 225, W: 270, NW: 315 };

/** How far apart two NaPTAN compass points are, 0 to 180 degrees; null when
 *  either pole has none. */
function timingTurn(a, b) {
  const x = TIMING_COMPASS[String(a || "").toUpperCase()], y = TIMING_COMPASS[String(b || "").toUpperCase()];
  if (x == null || y == null) return null;
  const d = Math.abs(x - y) % 360;
  return d > 180 ? 360 - d : d;
}

/** The pole across the road from `stop`: one close by for buses the other
 *  way, the same stop's name first. Null when there is none. */
function timingOtherSide(stops, stop) {
  if (!stop) return null;
  const found = stops.filter(s => s.atco_code !== stop.atco_code)
    .map(s => ({ s, metres: timingDistance(stop.latitude, stop.longitude, s.latitude, s.longitude),
      turn: timingTurn(stop.bearing, s.bearing), same: s.name === stop.name }))
    .filter(c => c.metres <= TIMING_OTHER_SIDE_METRES
      && (c.turn != null ? c.turn >= 120 : c.same && c.metres <= 80)
      && (c.same || c.metres <= 60))
    .sort((a, b) => (b.same - a.same) || a.metres - b.metres);
  return found.length ? found[0].s : null;
}

/** Choose `stop` for one side ("L" or "R") and make that side current. The
 *  other side, if empty or only ever filled in by the page, becomes the pole
 *  across the road from it; one chosen by hand is kept. */
function timingSetPole(sides, side, stop, stops) {
  const other = side === "L" ? "R" : "L";
  const out = { L: sides.L || null, R: sides.R || null, current: side, auto: sides.auto || null };
  out[side] = stop;
  if (out.auto === side) out.auto = null;
  if (!out[other] || out.auto === other) {
    const across = timingOtherSide(stops, stop);
    out[other] = across;
    out.auto = across ? other : null;
  }
  return out;
}

/** Stops matching every word typed, best first. A word that is this pole's
 *  letter ("b6") counts most, then words in the stop's own name, then its
 *  place or street, and least a word found only in where its buses go: a
 *  search for "Brighton Station" is for the station's poles, not every stop
 *  with buses heading there. "Stop" before a letter is ignored. */
function timingSearch(stops, text, n = 12) {
  const words = String(text || "").toLowerCase().split(/[\s,]+/)
    .filter(w => w && !/^(stop|stand|bay)$/.test(w));
  if (!words.length) return [];
  const scored = [];
  for (const s of stops) {
    const name = s.name.toLowerCase(), letter = (s.letter || "").toLowerCase();
    const place = `${s.locality || ""} ${s.street || ""}`.toLowerCase();
    const towards = (s.towards || "").toLowerCase(), code = s.atco_code.toLowerCase();
    let score = 0;
    for (const w of words) {
      if (letter && w === letter) score += 100;
      else if (name.includes(w)) score += 10;
      else if (place.includes(w) || code.includes(w)) score += 3;
      else if (towards.includes(w)) score += 1;
      else { score = -1; break; }
    }
    if (score >= 0) scored.push({ s, score });
  }
  return scored.sort((a, b) => b.score - a.score || a.s.name.localeCompare(b.s.name))
    .slice(0, n).map(x => x.s);
}

/** "Brighton Station, stop B6, towards George Street": the pole, said as the
 *  flag says it, or the way it faces when stops.json has no "towards". */
function timingStopLabel(s) {
  return `${tEsc(s.name)}${s.letter ? `, stop ${tEsc(s.letter)}` : ""}${s.towards ? `, towards ${tEsc(s.towards)}`
    : s.bearing ? `, facing ${tEsc(s.bearing)}` : ""}`;
}

/** "10950", "SCSO-10950" and "scso 10950" are one bus; so are "YX12 FRC" and
 *  "YX12_FRC". As scripts/validate_hand_timings.py normalises them. */
function timingVehicleKey(text) {
  return String(text || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
}

/** A button for each route at the stop, the one in `current` pressed. */
function timingRouteChipsHtml(services, current) {
  const now = String(current || "").trim().toUpperCase();
  return (services || []).map(s => `<button type="button" class="chip" data-route="${tEsc(s)}"
    aria-pressed="${String(s).toUpperCase() === now}">${tEsc(s)}</button>`).join("");
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
 *  run as a formula is kept as text; a plain number, such as the clock offset
 *  of a phone running slow, is not a formula and stays a number. */
function timingCsv(entries) {
  const cell = v => {
    const s = String(v == null ? "" : v);
    const safe = /^[=+\-@]/.test(s) && !/^[+-]?\d+(\.\d+)?$/.test(s) ? `'${s}` : s;
    return /[",\n]/.test(safe) ? `"${safe.replace(/"/g, '""')}"` : safe;
  };
  const rows = entries.slice().reverse().map(e => TIMING_COLUMNS.map(c =>
    c === "day" ? timingLondonDay(e.departed_at || e.arrived_at) : e[c]));
  return [TIMING_COLUMNS, ...rows].map(r => r.map(cell).join(",")).join("\n") + "\n";
}

/** The rows not in the last export. */
function timingNewSinceExport(entries, exported) {
  const done = new Set((exported && exported.ids) || []);
  return entries.filter(e => !done.has(e.id));
}

/** Whether to remind, once per export, that the log already went out: when
 *  a bus logged since has left. Not between its doors opening and it
 *  leaving, where a dialog would be in the way of the next tap. */
function timingShouldRemind(entries, exported, touched) {
  if (!exported || !(exported.ids || []).length || exported.reminded || !touched) return false;
  if (exported.ids.includes(touched.id) || (touched.arrived_at && !touched.departed_at)) return false;
  return timingNewSinceExport(entries, exported).length > 0;
}

function timingLoad() {
  try { return JSON.parse(localStorage.getItem(TIMING_KEY) || "[]"); } catch { return []; }
}

function timingSave(entries) {
  try { localStorage.setItem(TIMING_KEY, JSON.stringify(entries)); return true; } catch { return false; }
}

function timingLoadJson(key, fallback) {
  try { return JSON.parse(localStorage.getItem(key) || "null") || fallback; } catch { return fallback; }
}

function timingSaveJson(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); return true; } catch { return false; }
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
  let stops = [], entries = timingLoad(), offsetMs = 0;
  let sides = { L: null, R: null, current: "L", auto: null };
  let finding = false;   // the stop finder opened by hand over a chosen stop
  let exported = timingLoadJson(TIMING_EXPORTED_KEY, { ids: [], reminded: false });
  if (!Array.isArray(exported.ids)) exported = { ids: [], reminded: false };
  const stop = () => sides[sides.current];
  const sideOf = (atco) => sides.L && sides.L.atco_code === atco ? "L" : sides.R && sides.R.atco_code === atco ? "R" : "";

  const saveSides = () => timingSaveJson(TIMING_SIDES_KEY, { L: sides.L && sides.L.atco_code,
    R: sides.R && sides.R.atco_code, current: sides.current, auto: sides.auto });

  const renderStop = () => {
    const s = stop();
    for (const side of ["L", "R"]) {
      const pole = sides[side];
      const b = document.querySelector(`.side[data-side="${side}"]`);
      b.setAttribute("aria-pressed", String(sides.current === side));
      $(`side-${side}`).innerHTML = pole
        ? `<strong>${tEsc(pole.name)}${pole.letter ? ` ${tEsc(pole.letter)}` : ""}</strong>
           <span class="side-towards">${pole.towards ? `towards ${tEsc(pole.towards)}` : pole.bearing ? `facing ${tEsc(pole.bearing)}` : ""}</span>`
        : `<span class="side-towards">${side === sides.current ? "Choose its stop above" : "Not set"}</span>`;
    }
    // Once the lit side has its pole, the finder folds away so the buttons
    // sit nearer the top; "Change the stop" brings it back.
    $("finder").hidden = !!s && !finding;
    $("change-stop").hidden = !s || finding;
    $("change-stop").textContent = `Change the stop for ${sides.current}`;
    $("stop-chosen").innerHTML = s
      ? `Timing <strong>${sides.current}: ${timingStopLabel(s)}</strong>
         <span class="muted">${tEsc(s.locality || "")} · ${tEsc(s.atco_code)}</span>`
      : `<span class="muted">No stop chosen for ${sides.current} yet.</span>`;
    $("route-chips").innerHTML = timingRouteChipsHtml(s?.services, $("route").value);
    for (const b of document.querySelectorAll(".tap")) b.disabled = !s;
  };
  const syncChips = () => {
    const now = $("route").value.trim().toUpperCase();
    for (const c of $("route-chips").querySelectorAll(".chip")) {
      c.setAttribute("aria-pressed", String(c.dataset.route.toUpperCase() === now));
    }
  };
  const choices = (list) => {
    $("stop-choices").innerHTML = list.map(s => `<li><button type="button" class="choice" data-atco="${tEsc(s.atco_code)}">
      ${timingStopLabel(s)}
      <span class="muted">${tEsc(s.locality || "")}${s.metres != null ? ` · ${s.metres} m` : ""}</span></button></li>`).join("")
      || `<li class="muted">No stops found.</li>`;
  };
  const renderLog = () => {
    $("count").textContent = `${entries.length} ${entries.length === 1 ? "bus" : "buses"} logged`;
    const fresh = timingNewSinceExport(entries, exported).length;
    $("export-note").textContent = exported.ids.length && entries.length
      ? (fresh ? `${fresh} new since your last export.` : "All of these are in your last export.") : "";
    $("log").innerHTML = entries.map(e => {
      const side = sideOf(e.atco);
      return `<li data-id="${tEsc(e.id)}">
      <div class="row-head"><strong>${side ? `<span class="side-badge">${side}</span> ` : ""}${tEsc(e.stop_name)}${
        e.towards ? ` <span class="muted">towards ${tEsc(e.towards)}</span>` : ""}</strong>
        <span>${e.arrived_at ? `doors ${timingClock(e.arrived_at)}` : ""}${e.arrived_at && e.departed_at ? " · " : ""}${
          e.departed_at ? `left ${timingClock(e.departed_at)}` : `<em>not left yet</em>`}</span></div>
      ${e.departed_at ? "" : `<button type="button" class="left-now" data-act="left">Bus left now</button>`}
      <label>Route <input data-field="route" value="${tEsc(e.route)}" autocomplete="off"></label>
      <label>Registration <input data-field="vehicle" value="${tEsc(e.vehicle)}" autocomplete="off" autocapitalize="characters"></label>
      <label>Note <input data-field="note" list="timing-notes" value="${tEsc(e.note)}" autocomplete="off"
        placeholder="Type, or choose an example"></label>
      <button type="button" class="delete" data-act="delete">Delete</button></li>`;
    }).join("");
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
    const restore = () => {
      const kept = timingLoadJson(TIMING_SIDES_KEY, null);
      if (!kept) return;
      const find = atco => stops.find(s => s.atco_code === atco) || null;
      sides = { L: find(kept.L), R: find(kept.R), current: kept.current === "R" ? "R" : "L", auto: kept.auto || null };
      renderStop();
      renderLog();
    };
    // The letters on the flags (NaPTAN), so "b6" finds stop B6, and the way
    // each pole faces, which finds the one across the road. Search works
    // without them, just less sharply.
    return fetch(TIMING_DETAILS_URL).then(r => r.json()).then(details => {
      const rows = details.stops || {};
      for (const s of stops) {
        const [indicator = "", street = "", , bearing = ""] = rows[s.atco_code] || [];
        s.letter = timingLetter(indicator);
        s.street = street;
        s.bearing = bearing;
      }
      restore();
      if ($("search").value.trim()) choices(timingSearch(stops, $("search").value));
    }).catch(restore);
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
    const chosen = stops.find(s => s.atco_code === b.dataset.atco) || null;
    if (!chosen) return;
    const before = sides[sides.current === "L" ? "R" : "L"];
    sides = timingSetPole(sides, sides.current, chosen, stops);
    finding = false;
    saveSides();
    $("stop-choices").innerHTML = "";
    $("search").value = "";
    renderStop();
    renderLog();
    const other = sides.current === "L" ? "R" : "L";
    $("status").textContent = sides[other] && sides[other] !== before
      ? `${sides.current} set. ${other} is the pole across the road, ${sides[other].name}${
        sides[other].towards ? ` towards ${sides[other].towards}` : ""}: tap ${other} to time it.`
      : `${sides.current} set.`;
  });
  $("change-stop").addEventListener("click", () => {
    finding = true;
    renderStop();
    $("search").focus();
  });
  $("sides").addEventListener("click", e => {
    const b = e.target.closest("button.side");
    if (!b) return;
    finding = false;
    sides = { ...sides, current: b.dataset.side };
    saveSides();
    renderStop();
    const s = stop();
    $("status").textContent = s ? `Timing ${sides.current}: ${s.name}${s.towards ? ` towards ${s.towards}` : ""}.`
      : `Choose the stop for ${sides.current} above.`;
  });
  $("route-chips").addEventListener("click", e => {
    const c = e.target.closest("button.chip");
    if (!c) return;
    $("route").value = c.dataset.route;
    syncChips();
  });
  $("route").addEventListener("input", syncChips);
  for (const b of document.querySelectorAll(".tap")) {
    b.addEventListener("click", () => {
      const s = stop();
      if (!s) return;
      entries = timingTap(entries, b.dataset.kind, { stop: s, route: $("route").value.trim(),
        vehicle: $("vehicle").value.trim(), offsetMs });
      persist();
      renderLog();
      if (b.dataset.kind === "departed") { $("route").value = ""; $("vehicle").value = ""; syncChips(); }
      if (navigator.vibrate) navigator.vibrate(40);
      const touched = b.dataset.kind === "departed"
        ? entries.filter(e => e.atco === s.atco_code && e.departed_at).sort((x, y) => y.departed_at.localeCompare(x.departed_at))[0]
        : entries[0];
      if (timingShouldRemind(entries, exported, touched)) showReminder(b);
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
    const li = e.target.closest("li[data-id]");
    const entry = li && entries.find(x => x.id === li.dataset.id);
    if (e.target.dataset.act === "left" && entry && !entry.departed_at) {
      // The bus at the other pole left while this side was lit.
      entry.departed_at = new Date().toISOString();
      persist();
      renderLog();
      if (navigator.vibrate) navigator.vibrate(40);
      // Its button has just been redrawn away, so focus comes back to Bus left.
      if (timingShouldRemind(entries, exported, entry)) showReminder(document.querySelector('.tap[data-kind="departed"]'));
      return;
    }
    if (e.target.dataset.act !== "delete") return;
    if (li && confirm("Delete this bus from the log?")) {
      entries = entries.filter(x => x.id !== li.dataset.id);
      persist();
      renderLog();
    }
  });

  /* Exporting, and the reminder that what went out once should not go out
     again. */
  const saveExported = () => timingSaveJson(TIMING_EXPORTED_KEY, exported);
  const stamp = () => new Date().toLocaleTimeString("en-GB", { timeZone: "Europe/London", hour12: false,
    hour: "2-digit", minute: "2-digit" }).replace(":", "");
  const file = (rows, partial) => new File([timingCsv(rows)],
    `timings-${timingLondonDay(new Date().toISOString())}${partial ? `-new-${stamp()}` : ""}.csv`, { type: "text/csv" });
  const markExported = (rows) => {
    exported = { ids: [...new Set([...exported.ids, ...rows.map(e => e.id)])].filter(id => entries.some(e => e.id === id)),
      reminded: false, at: new Date().toISOString() };
    saveExported();
    renderLog();
  };
  const download = (rows, partial = false) => {
    if (!rows.length) { $("status").textContent = "Nothing to export yet."; return; }
    const f = file(rows, partial);
    const a = document.createElement("a");
    a.href = URL.createObjectURL(f);
    a.download = f.name;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    markExported(rows);
    $("status").textContent = `Downloaded ${rows.length} ${rows.length === 1 ? "bus" : "buses"} as ${f.name}.`;
  };
  $("download").addEventListener("click", () => download(entries));
  $("share").addEventListener("click", async () => {
    if (!entries.length) { $("status").textContent = "Nothing to export yet."; return; }
    const f = file(entries, false);
    const rows = entries.slice();
    try {
      if (navigator.canShare && navigator.canShare({ files: [f] })) await navigator.share({ files: [f], title: f.name });
      else { await navigator.clipboard.writeText(timingCsv(rows)); $("status").textContent = "Copied the log as CSV."; }
      markExported(rows);
    } catch { /* cancelled: nothing went out */ }
  });
  $("clear").addEventListener("click", () => {
    if (entries.length && confirm(`Delete all ${entries.length} logged buses? Export first if you need them.`)) {
      entries = [];
      exported = { ids: [], reminded: false };
      saveExported();
      persist();
      renderLog();
    }
  });

  let returnFocus = null;
  const showReminder = (from) => {
    const dialog = $("dup");
    if (!dialog || typeof dialog.showModal !== "function" || dialog.open) return;
    const fresh = timingNewSinceExport(entries, exported).length;
    const old = entries.length - fresh;
    exported.reminded = true;
    saveExported();
    $("dup-text").textContent = `${old} ${old === 1 ? "bus in this log was" : "buses in this log were"} in the CSV you `
      + `already exported${exported.at ? ` at ${timingClock(exported.at)}` : ""}. Exporting everything again would send `
      + `${old === 1 ? "it" : "them"} twice.`;
    $("dup-new").textContent = `Download only the ${fresh} new ${fresh === 1 ? "bus" : "buses"}`;
    $("dup-clear").textContent = `Remove the ${old} exported ${old === 1 ? "bus" : "buses"} from this phone`;
    returnFocus = from || document.activeElement;
    dialog.showModal();
    $("dup-keep").focus();
  };
  if ($("dup")) {
    $("dup").addEventListener("close", () => {
      if (returnFocus && document.contains(returnFocus) && !returnFocus.hidden) returnFocus.focus();
      returnFocus = null;
    });
    // A tap on the dimmed page outside the box closes it, as Escape does.
    $("dup").addEventListener("click", e => { if (e.target === $("dup")) $("dup").close(); });
    $("dup-keep").addEventListener("click", () => $("dup").close());
    $("dup-new").addEventListener("click", () => { download(timingNewSinceExport(entries, exported), true); $("dup").close(); });
    $("dup-clear").addEventListener("click", () => {
      const done = new Set(exported.ids);
      entries = entries.filter(e => !done.has(e.id));
      exported = { ids: [], reminded: false };
      saveExported();
      persist();
      renderLog();
      $("status").textContent = "Removed the buses you had already exported. The new ones are still here.";
      $("dup").close();
    });
  }
  renderStop();
  renderLog();
}

if (typeof document !== "undefined" && document.getElementById("timing")) timingInit();
