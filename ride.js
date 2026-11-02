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
 * Two kinds of row are not door times, and say so in `event`: "skipped", a
 * stop the bus passed without stopping (when, in `passed_at`), and
 * "unscheduled", the bus standing somewhere that is not a stop, such as
 * lights or a driver change (arrived_at to departed_at, `departed_basis`
 * "moving again"). The validator counts them and never scores them.
 *
 * Where the position comes from. The 1 November 2026 ride lost three stops:
 * the first was tapped 1.7 s after Start, before the phone's first fix, and
 * two more came when the last fix was over 30 s old. The page took only the
 * fix it already held, never asked for another, and left a row without one
 * with an empty stop list. Now a tap without a fresh fix asks the phone for
 * one there and then, and fills the row in when it comes (`fix_offset_s`
 * says how long after the tap), and every row's list offers the stops ahead
 * of the last one logged and every stop on the route, position or not.
 *
 * The page has no route's stop order: data/stops.json lists the routes at
 * each stop, not the order they call. "Ahead" is worked out from the way the
 * bus is heading and the way each pole faces (data/stop_details.json).
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
/* A fix this recent at the tap is where the tap happened. An older one could
   be the previous stop, so the page asks for a fresh one instead. */
const RIDE_FRESH_FIX_MS = 10000;
/* A fix this long after a doors-open tap still finds the stop: the bus is
   standing there. A row with no doors-open tap (doors closed only, or a stop
   skipped) is a bus on the move, so it gets less. */
const RIDE_LATE_FIX_MS = 20000;
const RIDE_LATE_MOVING_MS = 8000;
/* How far ahead to look for the next stop, and how far off the line of
   travel it may lie: roads bend. */
const RIDE_AHEAD_METRES = 1200;
const RIDE_AHEAD_DEGREES = 60;
/* A pole facing further than this from the way the bus is going is for the
   other direction. */
const RIDE_FACING_DEGREES = 95;
/* Moving this far between fixes gives a direction of travel. */
const RIDE_HEADING_METRES = 25;

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

const RIDE_COMPASS = { N: 0, NE: 45, E: 90, SE: 135, S: 180, SW: 225, W: 270, NW: 315 };

/** Degrees for NaPTAN's compass point ("SW": 225), null when it has none. */
function rideCompass(point) {
  const d = RIDE_COMPASS[String(point || "").trim().toUpperCase()];
  return d == null ? null : d;
}

/** The direction from one point to another, in degrees from north. */
function rideBearing(lat1, lon1, lat2, lon2) {
  const rad = Math.PI / 180;
  const x = (lon2 - lon1) * Math.cos(((lat1 + lat2) / 2) * rad);
  return (Math.atan2(x, lat2 - lat1) / rad + 360) % 360;
}

/** How far apart two directions are, 0 to 180 degrees. */
function rideTurn(a, b) {
  const d = Math.abs(a - b) % 360;
  return d > 180 ? 360 - d : d;
}

const rideServes = (s, want) => (s.services || []).some(v => String(v).toUpperCase() === want);

/** Whether a pole is for buses going `heading`. A pole with no compass
 *  point, or no heading known, could be. */
function rideFacing(s, heading) {
  const b = rideCompass(s.bearing);
  return heading == null || b == null || rideTurn(b, heading) <= RIDE_FACING_DEGREES;
}

/** Which stop the bus is at. `choices` are the nearest stops, nearest first,
 *  for correcting the guess; `best` is the nearest the route serves within
 *  reach (facing the way the bus is going, when that is known and a pole
 *  does), else any stop very close, else null: no guess is better than a
 *  wrong one, which the validator would score as that stop's error. */
function rideStopGuess(stops, lat, lon, route, n = 6, heading = null) {
  const want = String(route || "").trim().toUpperCase();
  const near = stops.map(s => ({ ...s, metres: Math.round(rideDistance(lat, lon, s.latitude, s.longitude)) }))
    .sort((a, b) => a.metres - b.metres);
  const reach = near.filter(s => s.metres <= RIDE_ROUTE_STOP_METRES && rideServes(s, want));
  const onRoute = reach.find(s => rideFacing(s, heading)) || reach[0];
  const close = near.find(s => s.metres <= RIDE_ANY_STOP_METRES);
  return { best: onRoute || close || null, choices: near.slice(0, n) };
}

/** The route's stops ahead of `from` ({lat, lon}), nearest first: facing the
 *  way the bus is heading and lying roughly along it. With no heading, the
 *  route's nearest stops. A stop within 30 m counts whatever its direction,
 *  as the bus may be at it. */
function rideAhead(stops, from, heading, route, { exclude = [], n = 3 } = {}) {
  if (!from) return [];
  const want = String(route || "").trim().toUpperCase();
  const skip = new Set(exclude.filter(Boolean));
  // A stop's other poles close by are that same stop, never the next one.
  const skipNames = new Set(stops.filter(s => skip.has(s.atco_code)).map(s => s.name));
  const found = stops.filter(s => rideServes(s, want) && !skip.has(s.atco_code))
    .map(s => ({ ...s, metres: Math.round(rideDistance(from.lat, from.lon, s.latitude, s.longitude)) }))
    .filter(s => s.metres <= RIDE_AHEAD_METRES && !(skipNames.has(s.name) && s.metres <= 150))
    .filter(s => heading == null || (rideFacing(s, heading) && (s.metres < 30
      || rideTurn(rideBearing(from.lat, from.lon, s.latitude, s.longitude), heading) <= RIDE_AHEAD_DEGREES)));
  // Two poles of one stop both in reach: the one facing the way the bus goes.
  const turn = s => (heading == null || rideCompass(s.bearing) == null ? 0 : rideTurn(rideCompass(s.bearing), heading));
  const best = new Map();
  for (const s of found) {
    const had = best.get(s.name);
    if (!had || turn(s) < turn(had) || (turn(s) === turn(had) && s.metres < had.metres)) best.set(s.name, s);
  }
  return [...best.values()].sort((a, b) => a.metres - b.metres).slice(0, n);
}

/** What to offer when choosing a stop by hand, in three groups: `near`, the
 *  stops nearest `at` (where the tap was, if known); `ahead`, the route's
 *  stops after `after` (the stop logged before); `route`, every stop the
 *  route serves, A to Z. Something is always offered once the route has a
 *  stop, position or not. */
function rideCandidates(stops, { route, at = null, after = null, heading = null } = {}) {
  const want = String(route || "").trim().toUpperCase();
  const near = at ? rideStopGuess(stops, at.lat, at.lon, route).choices : [];
  const from = after ? { lat: after.latitude, lon: after.longitude } : null;
  const head = heading != null ? heading : after ? rideCompass(after.bearing) : null;
  const ahead = rideAhead(stops, from, head, route, { exclude: [after && after.atco_code], n: 4 });
  const onRoute = stops.filter(s => rideServes(s, want))
    .sort((a, b) => a.name.localeCompare(b.name) || String(a.towards || "").localeCompare(String(b.towards || "")));
  return { near, ahead, route: onRoute };
}

/** Fill in a row that has no position from a fix that came after its tap, if
 *  it came soon enough to be the same place. The stop is guessed from it
 *  unless one was already set or chosen by hand. Returns the new row, or
 *  null when the fix is no use for it. */
function rideLateFix(entry, fix, stops, heading = null) {
  if (!fix || (entry.lat !== "" && entry.lat != null)) return null;
  const tap = Date.parse(entry.arrived_at || entry.passed_at || entry.departed_at || "");
  const limit = entry.arrived_at ? RIDE_LATE_FIX_MS : RIDE_LATE_MOVING_MS;
  const lag = fix.at - tap;
  if (isNaN(lag) || lag < -RIDE_FRESH_FIX_MS || lag > limit) return null;
  const out = { ...entry, lat: Number(fix.lat.toFixed(6)), lon: Number(fix.lon.toFixed(6)),
    accuracy_m: fix.accuracy != null ? Math.round(fix.accuracy) : "", fix_offset_s: Math.round(lag / 1000) };
  if (!out.atco && out.stop_by !== "hand" && out.event !== "unscheduled") {
    const guess = rideStopGuess(stops, fix.lat, fix.lon, out.route, 6, heading).best;
    if (guess) Object.assign(out, { atco: guess.atco_code, stop_name: guess.name, towards: guess.towards || "",
      stop_by: "position" });
  }
  return out;
}

const rideEvent = e => e.event || "stop";

/** Whether a row is waiting for its second tap: doors open and not yet
 *  closed, or an unscheduled stop the bus has not moved on from. */
function rideWaiting(e) {
  return rideEvent(e) === "stop" ? !!(e.arrived_at && !e.departed_at)
    : rideEvent(e) === "unscheduled" && !e.departed_at;
}

/** Record a tap. `kind` is "open" or "closed" (the doors), "skipped" (the
 *  bus passed `stop` without stopping), "unscheduled" (it stopped where
 *  there is no stop) or "moving" (it moved on from that). Doors closing
 *  completes this ride's newest row if its doors are still open; closing
 *  with nothing open, or opening, makes a new stop. Returns the new list,
 *  newest first. */
function rideTap(entries, kind, { ride, stop, position, offsetMs = 0, now = new Date() }) {
  const at = now.toISOString();
  const list = entries.map(e => ({ ...e }));
  const mine = list.filter(e => e.ride_id === ride.ride_id);
  const fill = (row) => {
    // Doors closing with a position fills a row whose doors-open tap had none:
    // the bus is still at the stop as its doors close.
    if ((row.lat === "" || row.lat == null) && position) {
      const fixAt = position.at != null ? position.at : now.getTime();
      Object.assign(row, { lat: Number(position.lat.toFixed(6)), lon: Number(position.lon.toFixed(6)),
        accuracy_m: position.accuracy != null ? Math.round(position.accuracy) : "",
        fix_offset_s: Math.round((fixAt - Date.parse(row.arrived_at)) / 1000) });
      if (!row.atco && row.stop_by !== "hand" && stop) {
        Object.assign(row, { atco: stop.atco_code, stop_name: stop.name, towards: stop.towards || "", stop_by: "position" });
      }
    }
  };
  if (kind === "closed") {
    const open = mine[0];
    if (open && rideEvent(open) === "stop" && rideWaiting(open)) { open.departed_at = at; fill(open); return list; }
  }
  if (kind === "moving") {
    const open = mine.find(e => rideEvent(e) === "unscheduled" && !e.departed_at);
    if (open) open.departed_at = at;
    return list;
  }
  const seq = Math.max(ride.last_seq || 0, ...mine.map(e => e.seq || 0)) + 1;
  const event = kind === "skipped" ? "skipped" : kind === "unscheduled" ? "unscheduled" : "stop";
  const named = event !== "unscheduled" && stop ? stop : null;
  list.unshift({
    id: `${now.getTime().toString(36)}${Math.random().toString(36).slice(2, 6)}`,
    ride_id: ride.ride_id, seq, event,
    atco: named ? named.atco_code : "", stop_name: named ? named.name : "", towards: named ? named.towards || "" : "",
    stop_by: named ? named.by || "position" : "",
    route: ride.route || "", vehicle: ride.vehicle || "",
    arrived_at: kind === "open" || kind === "unscheduled" ? at : "", departed_at: kind === "closed" ? at : "",
    passed_at: kind === "skipped" ? at : "",
    lat: position ? Number(position.lat.toFixed(6)) : "", lon: position ? Number(position.lon.toFixed(6)) : "",
    accuracy_m: position && position.accuracy != null ? Math.round(position.accuracy) : "",
    fix_offset_s: position ? (position.at != null ? Math.round((position.at - now.getTime()) / 1000) : 0) : "",
    clock_offset_ms: Math.round(offsetMs), note: "",
  });
  return list;
}

/* timing.js's TIMING_COLUMNS first, in its order, then the ride's own. New
   columns go on the end, so an older export still lines up. */
const RIDE_COLUMNS = ["id", "day", "atco", "stop_name", "towards", "route", "vehicle",
  "arrived_at", "departed_at", "clock_offset_ms", "note",
  "ride_id", "seq", "lat", "lon", "accuracy_m", "departed_basis",
  "event", "passed_at", "fix_offset_s"];
const RIDE_BASIS = { stop: "doors closed", unscheduled: "moving again", skipped: "" };

/** The London date of an instant, YYYY-MM-DD. */
function rideLondonDay(iso) {
  const parts = new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/London",
    year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(new Date(iso));
  const get = type => parts.find(p => p.type === type).value;
  return `${get("year")}-${get("month")}-${get("day")}`;
}

/** The log as CSV, one row per stop, oldest first. A cell a spreadsheet would
 *  run as a formula is kept as text; a plain number, such as a longitude west
 *  of Greenwich, is not a formula and stays a number. */
function rideCsv(entries) {
  const cell = v => {
    const s = String(v == null ? "" : v);
    const safe = /^[=+\-@]/.test(s) && !/^[+-]?\d+(\.\d+)?$/.test(s) ? `'${s}` : s;
    return /[",\n]/.test(safe) ? `"${safe.replace(/"/g, '""')}"` : safe;
  };
  const rows = entries.slice().reverse().map(e => RIDE_COLUMNS.map(c =>
    c === "day" ? rideLondonDay(e.departed_at || e.arrived_at || e.passed_at)
      : c === "departed_basis" ? RIDE_BASIS[rideEvent(e)] || ""
        : c === "event" ? rideEvent(e) : e[c]));
  return [RIDE_COLUMNS, ...rows].map(r => r.map(cell).join(",")).join("\n") + "\n";
}

/** The rows not in the last export. */
function rideNewSinceExport(entries, exported) {
  const done = new Set((exported && exported.ids) || []);
  return entries.filter(e => !done.has(e.id));
}

/** Whether to remind, once per export, that the log already went out: when
 *  a row added since has had its last tap. Not between doors open and doors
 *  closed, where the next tap comes in seconds and a dialog would be in the
 *  way of it. */
function rideShouldRemind(entries, exported, touched) {
  if (!exported || !(exported.ids || []).length || exported.reminded || !touched) return false;
  if (exported.ids.includes(touched.id)) return false;
  return !rideWaiting(touched) && rideNewSinceExport(entries, exported).length > 0;
}

function rideClock(iso) {
  return iso ? new Date(iso).toLocaleTimeString("en-GB", { timeZone: "Europe/London", hour12: false }) : "";
}

/** "Melbourne Street (B), towards Sainsbury's": a pole as the flag says it,
 *  or the way it faces when stops.json has no "towards" for it. */
function rideStopLabel(s) {
  const where = s.towards ? `, towards ${s.towards}` : s.bearing ? `, facing ${s.bearing}` : "";
  return `${s.name}${s.letter ? ` (${s.letter})` : ""}${where}`;
}

function rideLoad() {
  try {
    const doc = JSON.parse(localStorage.getItem(RIDE_KEY) || "{}");
    return { ride: doc.ride || null, entries: Array.isArray(doc.entries) ? doc.entries : [],
      exported: doc.exported && Array.isArray(doc.exported.ids) ? doc.exported : { ids: [], reminded: false } };
  } catch { return { ride: null, entries: [], exported: { ids: [], reminded: false } }; }
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
  let stops = [], offsetMs = 0, fix = null, watch = null, wake = null, locError = "";
  let heading = null, headingAt = 0, anchor = null;
  let pick = "";   // a stop chosen by hand for the next tap, else ""
  let { ride, entries, exported } = rideLoad();
  const stopsByAtco = new Map();
  const routeOptions = new Map();   // route: <option>s of every stop it serves

  const persist = () => {
    if (!rideSave({ ride, entries, exported })) say("This browser would not save the log: export it before closing the page.");
  };
  const say = (text) => { $("status").textContent = text; };
  const mine = () => ride ? entries.filter(e => e.ride_id === ride.ride_id) : [];
  const liveFix = (ms) => fix && Date.now() - fix.at < ms ? fix : null;
  const liveHeading = () => Date.now() - headingAt < 120000 ? heading : null;

  // Keep the screen on while riding, where the phone allows it: a screen that
  // locks between stops is a stop missed.
  const holdScreen = async () => {
    try { if (navigator.wakeLock && !wake) wake = await navigator.wakeLock.request("screen"); } catch { wake = null; }
  };
  const releaseScreen = () => { try { if (wake) wake.release(); } catch { /* already gone */ } wake = null; };

  /* Every fix, from the watch or asked for at a tap: keep it, work out which
     way the bus is going, and fill in any row still waiting for a position. */
  const onFix = (pos) => {
    const at = Math.min(pos.timestamp || Date.now(), Date.now());
    fix = { lat: pos.coords.latitude, lon: pos.coords.longitude, accuracy: pos.coords.accuracy, at };
    locError = "";
    if (pos.coords.heading != null && !isNaN(pos.coords.heading) && (pos.coords.speed || 0) > 1) {
      heading = pos.coords.heading; headingAt = at;
    } else if (!anchor) {
      anchor = fix;
    } else if (rideDistance(anchor.lat, anchor.lon, fix.lat, fix.lon) >= RIDE_HEADING_METRES
      && (fix.accuracy || 0) <= 40) {
      heading = rideBearing(anchor.lat, anchor.lon, fix.lat, fix.lon); headingAt = at; anchor = fix;
    }
    let filled = false;
    entries = entries.map(e => {
      if (!ride || e.ride_id !== ride.ride_id) return e;
      const done = rideLateFix(e, fix, stops, liveHeading());
      if (done) filled = true;
      return done || e;
    });
    if (filled) { persist(); renderLog(); }
    renderWhere();
  };
  const onFixError = (err) => {
    locError = err && err.code === 1
      ? "Location was refused: choose each stop by hand in the list."
      : "No location yet. Keep tapping: each stop is filled in when a fix comes, or choose it.";
    renderWhere();
  };
  const follow = () => {
    if (!navigator.geolocation) { locError = "This phone will not share its location: choose each stop by hand."; return; }
    if (watch != null) return;
    watch = navigator.geolocation.watchPosition(onFix, onFixError,
      { enableHighAccuracy: true, maximumAge: 2000, timeout: 20000 });
  };
  const unfollow = () => {
    if (watch != null && navigator.geolocation) navigator.geolocation.clearWatch(watch);
    watch = null;
  };
  /* A watch can go quiet: before its first fix, with the page hidden, or
     while the phone is standing still. Ask for a fresh fix outright. */
  const askFix = () => {
    if (!navigator.geolocation) return;
    navigator.geolocation.getCurrentPosition(onFix, onFixError,
      { enableHighAccuracy: true, maximumAge: 0, timeout: 15000 });
  };
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState !== "visible" || !ride) return;
    wake = null; holdScreen();
    unfollow(); follow(); askFix();
  });

  /* Where the bus is: the row logged before, the stop it is at (or the one
     whose doors are open), and the stop after. */
  const where = () => {
    const rows = mine();
    const open = rows[0] && rideWaiting(rows[0]) ? rows[0] : null;
    const before = open ? rows[1] : rows[0];
    const live = liveFix(60000);
    const lastStop = rows.map(e => stopsByAtco.get(e.atco)).find(Boolean) || null;
    const head = liveHeading() != null ? liveHeading() : lastStop ? rideCompass(lastStop.bearing) : null;
    let now = null, basis = "";
    if (pick && stopsByAtco.get(pick)) { now = stopsByAtco.get(pick); basis = "chosen"; }
    else if (open && rideEvent(open) === "stop" && open.atco && stopsByAtco.get(open.atco)) {
      now = stopsByAtco.get(open.atco); basis = "open";
    } else if (live && stops.length) {
      now = rideStopGuess(stops, live.lat, live.lon, ride.route, 6, head).best;
      basis = now ? "position" : "";
      // Still beside the stop just logged: the bus is leaving it.
      if (now && before && rideEvent(before) !== "unscheduled" && before.atco === now.atco_code) {
        basis = "leaving";
      }
    }
    const from = live || (lastStop ? { lat: lastStop.latitude, lon: lastStop.longitude } : null);
    const after = rideAhead(stops, from, head, ride.route,
      { exclude: [now && now.atco_code, lastStop && lastStop.atco_code], n: 2 });
    return { open, before, now, basis, after, live, from, head, lastStop };
  };

  const rowSummary = (e) => {
    if (!e) return `<span class="muted">Nothing logged yet on this ride.</span>`;
    const name = e.stop_name ? rEsc(e.stop_name) : `<span class="muted">stop not chosen</span>`;
    if (rideEvent(e) === "skipped") return `${name} <span class="muted">skipped, ${rEsc(rideClock(e.passed_at))}</span>`;
    if (rideEvent(e) === "unscheduled") return `Unscheduled stop <span class="muted">${rEsc(rideClock(e.arrived_at))}</span>`;
    return `${name} <span class="muted">${e.departed_at ? `doors closed ${rEsc(rideClock(e.departed_at))}`
      : `doors open ${rEsc(rideClock(e.arrived_at))}`}</span>`;
  };

  let pickKey = "";
  const renderWhere = () => {
    if (!ride) return;
    const w = where();
    $("where-before").innerHTML = rowSummary(w.before);
    let nowHtml;
    if (w.open && rideEvent(w.open) === "unscheduled") {
      nowHtml = `Unscheduled stop <span class="muted">since ${rEsc(rideClock(w.open.arrived_at))}</span>`;
    } else if (w.now && w.basis === "leaving") {
      nowHtml = `<span class="muted">Leaving ${rEsc(w.now.name)}</span>`;
    } else if (w.now) {
      const m = w.live ? Math.round(rideDistance(w.live.lat, w.live.lon, w.now.latitude, w.now.longitude)) : null;
      const said = w.basis === "chosen" ? "chosen by you" : w.basis === "open" ? "doors open"
        : m != null ? `${m} m away` : "";
      nowHtml = `${rEsc(rideStopLabel(w.now))}${said ? ` <span class="muted nowrap">${rEsc(said)}</span>` : ""}`;
    } else if (w.open) {
      nowHtml = `<span class="muted">Doors open, stop not known yet: choose it below</span>`;
    } else if (w.live) {
      nowHtml = `<span class="muted">Between stops</span>`;
    } else {
      nowHtml = `<span class="muted">${fix ? "No fresh location" : "Waiting for the first location fix"}</span>`;
    }
    $("where-now").innerHTML = nowHtml;
    const next = w.after[0];
    $("where-after").innerHTML = next
      ? `${rEsc(rideStopLabel(next))} <span class="muted">${w.live ? `${next.metres} m` : "probably, from the last stop"}</span>`
      : `<span class="muted">${w.from ? "Not known on this road" : "Not known until a stop is found"}</span>`;
    // The picker: rebuilt only when what it offers changes, and never under
    // a finger.
    const c = rideCandidates(stops, { route: ride.route, at: w.live, after: w.lastStop, heading: w.head });
    const key = [pick, ...c.near.map(s => s.atco_code), "|", ...c.ahead.map(s => s.atco_code), c.route.length].join(",");
    if (key !== pickKey && document.activeElement !== $("pick")) {
      pickKey = key;
      $("pick").innerHTML = `<option value="">${pick ? "Back to the stop found from location" : "Found from location"}</option>`
        + optionsHtml(c, ride.route);
      $("pick").value = pick;
    }
    const age = fix ? Math.round((Date.now() - fix.at) / 1000) : null;
    $("gps").textContent = locError && !liveFix(RIDE_FRESH_FIX_MS) ? locError
      : fix ? `Location: within about ${Math.round(fix.accuracy)} m${age > 3 ? `, ${age} s ago` : ""}.`
        : "Waiting for the first location fix.";
    const open = w.open;
    $("doors-state").textContent = open && rideEvent(open) === "stop"
      ? `Doors open since ${rideClock(open.arrived_at)}. Tap Doors closed as they close.` : "";
    $("unscheduled").textContent = open && rideEvent(open) === "unscheduled" ? "Moving again" : "Unscheduled stop";
    $("unscheduled").setAttribute("aria-pressed", String(!!(open && rideEvent(open) === "unscheduled")));
  };

  const optionHtml = (s, withMetres) => `<option value="${rEsc(s.atco_code)}">${rEsc(rideStopLabel(s))}${
    withMetres && s.metres != null ? `, ${s.metres} m` : ""}</option>`;
  const optionsHtml = (c, route) => {
    if (!routeOptions.has(route)) routeOptions.set(route, c.route.map(s => optionHtml(s, false)).join(""));
    return (c.near.length ? `<optgroup label="Nearest where you tapped">${c.near.map(s => optionHtml(s, true)).join("")}</optgroup>` : "")
      + (c.ahead.length ? `<optgroup label="Next along the route">${c.ahead.map(s => optionHtml(s, false)).join("")}</optgroup>` : "")
      + (c.route.length ? `<optgroup label="Every stop on the ${rEsc(route)}">${routeOptions.get(route)}</optgroup>` : "");
  };

  const renderRide = () => {
    $("setup").hidden = !!ride;
    $("riding").hidden = !ride;
    $("ride-status").innerHTML = ride
      ? `<strong>Riding the ${rEsc(ride.route || "?")}</strong>${ride.vehicle ? `, bus ${rEsc(ride.vehicle)}` : ""}
         <span class="muted">since ${rEsc(rideClock(ride.started_at))}</span>` : "";
    pickKey = "";
    renderWhere();
  };

  const timesHtml = (e) => {
    if (rideEvent(e) === "skipped") return `passed ${rideClock(e.passed_at)}, no stop`;
    if (rideEvent(e) === "unscheduled") {
      const stood = e.departed_at ? Math.round((Date.parse(e.departed_at) - Date.parse(e.arrived_at)) / 1000) : null;
      return `stood ${rideClock(e.arrived_at)}${e.departed_at ? ` to ${rideClock(e.departed_at)} · ${stood} s` : ", <em>still stopped</em>"}`;
    }
    const dwell = e.arrived_at && e.departed_at
      ? Math.round((Date.parse(e.departed_at) - Date.parse(e.arrived_at)) / 1000) : null;
    return `${e.arrived_at ? `open ${rideClock(e.arrived_at)}` : "<em>no open tap</em>"}${
      e.departed_at ? ` · closed ${rideClock(e.departed_at)}` : " · <em>not closed</em>"}${dwell != null ? ` · ${dwell} s` : ""}`;
  };

  const renderLog = () => {
    const count = k => entries.filter(e => rideEvent(e) === k).length;
    const n = count("stop"), skipped = count("skipped"), extra = count("unscheduled");
    $("count").textContent = `${n} ${n === 1 ? "stop" : "stops"} logged${skipped ? `, ${skipped} skipped` : ""}${
      extra ? `, ${extra} unscheduled` : ""}`;
    const fresh = rideNewSinceExport(entries, exported).length;
    $("export-note").textContent = exported.ids.length && entries.length
      ? (fresh ? `${fresh} new since your last export.` : "All of these are in your last export.") : "";
    $("log").innerHTML = entries.map(e => {
      const kind = rideEvent(e);
      const title = kind === "skipped" ? "Skipped" : kind === "unscheduled" ? "Unscheduled stop" : "Stop";
      let stopField = "";
      if (kind === "unscheduled") {
        stopField = `<p class="muted">Not at a stop${e.lat !== "" && e.lat != null ? ` (position recorded)` : ""}.</p>`;
      } else {
        const rows = entries.filter(x => x.ride_id === e.ride_id && (x.seq || 0) < (e.seq || 0) && x.atco);
        const prev = rows.length ? stopsByAtco.get(rows.sort((a, b) => b.seq - a.seq)[0].atco) : null;
        const at = e.lat !== "" && e.lat != null ? { lat: e.lat, lon: e.lon } : null;
        const c = rideCandidates(stops, { route: e.route, at, after: prev });
        const known = e.atco && ![...c.near, ...c.ahead, ...c.route].some(s => s.atco_code === e.atco)
          ? optionHtml(stopsByAtco.get(e.atco) || { atco_code: e.atco, name: e.stop_name, towards: e.towards }, false) : "";
        const hint = !e.atco && e.stop_by !== "hand" ? (c.near.find(s => rideServes(s, String(e.route).toUpperCase())
          && s.metres <= 150) || c.ahead[0]) : null;
        stopField = `<label>Stop <select data-field="atco">
            <option value="">${e.atco || e.stop_by === "hand" ? "Not a stop" : "Choose the stop"}</option>
            ${known}${optionsHtml(c, e.route)}</select></label>
          ${hint ? `<button type="button" class="use" data-act="use" data-atco="${rEsc(hint.atco_code)}">Use ${rEsc(hint.name)}</button>` : ""}`;
      }
      return `<li data-id="${rEsc(e.id)}" data-event="${rEsc(kind)}">
        <div class="row-head"><strong>${rEsc(e.route)} · ${title} ${rEsc(e.seq)}${e.stop_name ? `: ${rEsc(e.stop_name)}` : ""}</strong>
          <span>${timesHtml(e)}</span></div>
        ${stopField}
        <label>Note <input data-field="note" list="ride-notes" value="${rEsc(e.note)}" autocomplete="off"
          placeholder="Type, or choose an example"></label>
        <button type="button" class="delete" data-act="delete">Delete</button></li>`;
    }).join("");
    for (const sel of $("log").querySelectorAll("select[data-field=atco]")) {
      const id = sel.closest("li").dataset.id;
      const e = entries.find(x => x.id === id);
      sel.value = e ? e.atco || "" : "";
    }
  };

  const tick = () => {
    $("clock").textContent = new Date().toLocaleTimeString("en-GB", { timeZone: "Europe/London", hour12: false });
  };
  setInterval(tick, 250);
  setInterval(() => { if (ride) renderWhere(); }, 1000);
  tick();

  fetch(RIDE_STOPS_URL).then(r => r.json()).then(doc => {
    stops = doc.stops || [];
    for (const s of stops) stopsByAtco.set(s.atco_code, s);
    say(`${stops.length} stops loaded.`);
    // The letters on the flags, so the stop list can tell two poles apart, and
    // the way each pole faces, which is how the stop ahead is found.
    return fetch(RIDE_DETAILS_URL).then(r => r.json()).then(details => {
      const rows = details.stops || {};
      for (const s of stops) {
        const [indicator = "", , , bearing = ""] = rows[s.atco_code] || [];
        const m = /^(?:stop|stand|bay)\s+(\S{1,3})$/i.exec(String(indicator).trim());
        s.letter = m ? m[1].toUpperCase() : "";
        s.bearing = bearing;
      }
      routeOptions.clear();
      renderLog(); renderRide();
    }).catch(() => { renderLog(); renderRide(); });
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
      started_at: now.toISOString(), last_seq: 0 };
    pick = ""; heading = null; anchor = null;
    persist();
    follow();
    askFix();
    holdScreen();
    renderRide();
    say("Tap Doors open as the doors open at each stop, and Doors closed as they close.");
  });
  $("end").addEventListener("click", () => {
    if (!confirm("End this ride? The stops stay in the log below.")) return;
    ride = null;
    pick = "";
    persist();
    unfollow();
    releaseScreen();
    renderRide();
    say("Ride ended. Export the log when you are done, or start another ride.");
  });

  const record = (kind, button) => {
    if (!ride) return;
    const position = liveFix(RIDE_FRESH_FIX_MS);
    const w = where();
    let stop = null;
    if (pick && stopsByAtco.get(pick)) stop = { ...stopsByAtco.get(pick), by: "hand" };
    else if (kind === "skipped") {
      // The stop being passed: the one the bus is at, else the next one if it
      // is close. Not scored, so a guess here is safe to correct later.
      stop = (w.basis !== "leaving" && w.now) || (w.after[0] && w.after[0].metres <= 200 ? w.after[0] : null);
    } else if (position && stops.length) {
      stop = rideStopGuess(stops, position.lat, position.lon, ride.route, 6, liveHeading()).best;
    }
    const before = entries.length;
    entries = rideTap(entries, kind, { ride, stop, position, offsetMs });
    const touched = entries.find(e => e.ride_id === ride.ride_id && (
      kind === "moving" ? rideEvent(e) === "unscheduled" : true));
    if (entries.length > before) ride.last_seq = entries[0].seq;
    if (!position) askFix();
    if (kind === "closed" || kind === "skipped") pick = "";
    persist();
    renderLog();
    pickKey = "";
    renderWhere();
    const row = entries.find(e => e.ride_id === ride.ride_id);
    const said = { open: "Doors open", closed: "Doors closed", skipped: "Stop skipped", unscheduled: "Unscheduled stop",
      moving: "Moving again" }[kind];
    const tail = kind === "unscheduled" ? ". Tap Moving again when the bus moves off."
      : kind === "moving" ? "."
        : row && row.stop_name ? `, ${row.stop_name}.`
          : position ? ", stop not recognised: choose it in the list."
            : ", finding the location: the stop is filled in when it comes.";
    say(`${said} at ${rideClock(new Date().toISOString())}${tail}`);
    if (navigator.vibrate) navigator.vibrate(40);
    if (rideShouldRemind(entries, exported, touched)) showReminder(button);
  };
  for (const b of document.querySelectorAll(".tap")) b.addEventListener("click", () => record(b.dataset.kind, b));
  $("skip").addEventListener("click", () => record("skipped", $("skip")));
  $("unscheduled").addEventListener("click", () => {
    const rows = mine();
    const open = rows[0] && rideEvent(rows[0]) === "unscheduled" && !rows[0].departed_at;
    record(open ? "moving" : "unscheduled", $("unscheduled"));
  });
  $("pick").addEventListener("change", () => {
    pick = $("pick").value;
    // Choosing while the doors are open puts the stop on that row too.
    const rows = mine();
    const s = stopsByAtco.get(pick);
    if (s && rows[0] && rideEvent(rows[0]) === "stop" && rideWaiting(rows[0])) {
      Object.assign(rows[0], { atco: s.atco_code, stop_name: s.name, towards: s.towards || "", stop_by: "hand" });
      persist();
      renderLog();
    }
    pickKey = "";
    renderWhere();
  });

  const setStop = (entry, atco) => {
    const s = stopsByAtco.get(atco);
    Object.assign(entry, { atco: s ? s.atco_code : "", stop_name: s ? s.name : "", towards: s ? s.towards || "" : "",
      stop_by: "hand" });
    persist();
  };
  $("log").addEventListener("change", e => {
    const li = e.target.closest("li[data-id]");
    const entry = li && entries.find(x => x.id === li.dataset.id);
    if (!entry || e.target.dataset.field !== "atco") return;
    setStop(entry, e.target.value);
    renderLog();
    renderWhere();
  });
  $("log").addEventListener("input", e => {
    const li = e.target.closest("li[data-id]");
    const entry = li && entries.find(x => x.id === li.dataset.id);
    if (!entry || e.target.dataset.field !== "note") return;
    entry.note = e.target.value;
    persist();
  });
  $("log").addEventListener("click", e => {
    const act = e.target.dataset.act;
    const li = e.target.closest("li[data-id]");
    const entry = li && entries.find(x => x.id === li.dataset.id);
    if (act === "use" && entry) { setStop(entry, e.target.dataset.atco); renderLog(); renderWhere(); return; }
    if (act !== "delete") return;
    if (li && confirm("Delete this row from the log?")) {
      entries = entries.filter(x => x.id !== li.dataset.id);
      persist();
      renderLog();
      renderWhere();
    }
  });

  /* Exporting, and the reminder that what went out once should not go out
     again. */
  const stamp = () => new Date().toLocaleTimeString("en-GB", { timeZone: "Europe/London", hour12: false,
    hour: "2-digit", minute: "2-digit" }).replace(":", "");
  const file = (rows, partial) => new File([rideCsv(rows)],
    `rides-${rideLondonDay(new Date().toISOString())}${partial ? `-new-${stamp()}` : ""}.csv`, { type: "text/csv" });
  const markExported = (rows) => {
    exported = { ids: [...new Set([...exported.ids, ...rows.map(e => e.id)])].filter(id => entries.some(e => e.id === id)),
      reminded: false, at: new Date().toISOString() };
    persist();
    renderLog();
  };
  const download = (rows, partial = false) => {
    if (!rows.length) { say("Nothing to export yet."); return; }
    const f = file(rows, partial);
    const a = document.createElement("a");
    a.href = URL.createObjectURL(f);
    a.download = f.name;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    markExported(rows);
    say(`Downloaded ${rows.length} ${rows.length === 1 ? "row" : "rows"} as ${f.name}.`);
  };
  $("download").addEventListener("click", () => download(entries));
  $("share").addEventListener("click", async () => {
    if (!entries.length) { say("Nothing to export yet."); return; }
    const f = file(entries, false);
    const rows = entries.slice();
    try {
      if (navigator.canShare && navigator.canShare({ files: [f] })) await navigator.share({ files: [f], title: f.name });
      else { await navigator.clipboard.writeText(rideCsv(rows)); say("Copied the log as CSV."); }
      markExported(rows);
    } catch { /* cancelled: nothing went out */ }
  });
  $("clear").addEventListener("click", () => {
    if (entries.length && confirm(`Delete all ${entries.length} logged rows? Export first if you need them.`)) {
      entries = [];
      exported = { ids: [], reminded: false };
      persist();
      renderLog();
      renderWhere();
    }
  });

  let returnFocus = null;
  const showReminder = (from) => {
    const dialog = $("dup");
    if (!dialog || typeof dialog.showModal !== "function" || dialog.open) return;
    const fresh = rideNewSinceExport(entries, exported).length;
    const old = entries.length - fresh;
    exported.reminded = true;
    persist();
    $("dup-text").textContent = `${old} ${old === 1 ? "row in this log was" : "rows in this log were"} in the CSV you `
      + `already exported${exported.at ? ` at ${rideClock(exported.at)}` : ""}. Exporting everything again would send `
      + `${old === 1 ? "it" : "them"} twice.`;
    $("dup-new").textContent = `Download only the ${fresh} new ${fresh === 1 ? "row" : "rows"}`;
    $("dup-clear").textContent = `Remove the ${old} exported ${old === 1 ? "row" : "rows"} from this phone`;
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
    $("dup-new").addEventListener("click", () => { download(rideNewSinceExport(entries, exported), true); $("dup").close(); });
    $("dup-clear").addEventListener("click", () => {
      const done = new Set(exported.ids);
      entries = entries.filter(e => !done.has(e.id));
      exported = { ids: [], reminded: false };
      persist();
      renderLog();
      renderWhere();
      say("Removed the rows you had already exported. The new ones are still here.");
      $("dup").close();
    });
  }

  if (ride) { follow(); askFix(); holdScreen(); }
  renderRide();
  renderLog();
}

if (typeof document !== "undefined" && document.getElementById("ride")) rideInit();
