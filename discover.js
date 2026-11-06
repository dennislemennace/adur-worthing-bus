/*
 * Discover Worthing & Brighton (asked for 6 November 2026): places worth
 * visiting (data/destinations.json), and which of them a bus reaches without
 * changing from where you are: the bus to take, when it leaves, the ride and
 * the walk at the far end. From your location (it stays on this device, as
 * Stops near me does) or from any stop tapped on the map. The reach comes
 * from /api/direct-from, which reads only the timetable.
 *
 * Loaded after app.js, and uses its helpers. Draws its places on the map in
 * its own view only; discoverHide takes them down.
 */
"use strict";

const disc = { data: null, promise: null, layer: null, from: null, reach: null, seq: 0 };
// Walking pace for the far end, in metres a minute: a steady 4.8 km/h.
const DISCOVER_WALK_M_PER_MIN = 80;

function loadDestinations() {
  if (!disc.promise) {
    disc.promise = fetch("data/destinations.json")
      .then(res => { if (!res.ok) throw new Error(`HTTP ${res.status}`); return res.json(); })
      .then(doc => (disc.data = doc))
      .catch(err => { disc.promise = null; throw err; });
  }
  return disc.promise;
}

/** The places a bus reaches without changing, nearest door to door first:
 *  for each place, its stop with the shortest ride plus walk. */
function discoverReachable(places, reach) {
  const out = [];
  for (const place of places || []) {
    let best = null;
    for (const s of place.stops || []) {
      const e = reach && reach[s.atco];
      if (!e) continue;
      const walk = Math.max(1, Math.round(s.metres / DISCOVER_WALK_M_PER_MIN));
      const total = e.ride_mins + walk;
      if (!best || total < best.total) best = { place, stop: s, e, walk, total };
    }
    if (best) out.push(best);
  }
  return out.sort((a, b) => a.total - b.total || a.place.name.localeCompare(b.place.name));
}

function discoverClock(iso) {
  const d = new Date(iso);
  return isNaN(d) ? "" : d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", timeZone: "Europe/London" });
}

function discoverAccess(place) {
  return place.wheelchair === "yes" ? "Wheelchair accessible"
    : place.wheelchair === "limited" ? "Partly wheelchair accessible"
    : place.wheelchair === "no" ? "Not wheelchair accessible" : "";
}

function discoverPlaceHtml(place, extra = "") {
  const access = discoverAccess(place);
  return `<li class="disc-place" id="disc-${escapeAttr(place.id)}">
    <p class="disc-place-name">${escapeHtml(place.name)} <span class="disc-cat">${escapeHtml(place.category)}</span></p>
    <p class="disc-line">${escapeHtml(place.line)}</p>
    ${extra}
    <p class="disc-meta">${access ? `${escapeHtml(access)}, says OpenStreetMap · ` : ""}<a href="${
      escapeAttr(safeUrl(place.source_url))}" target="_blank" rel="noopener">Its own page<span class="visually-hidden">
      (opens in a new tab)</span></a></p>
  </li>`;
}

function discoverResultsHtml(rows, fromLabel) {
  if (!rows.length) {
    return `<p class="disc-note">No place on this list is a direct bus ride from ${escapeHtml(fromLabel)} for the rest
      of today. Try another stop, or see every place below.</p>`;
  }
  return `<h3 class="disc-title" tabindex="-1">Direct from ${escapeHtml(fromLabel)}</h3>
    <ol class="disc-list">${rows.map(r => discoverPlaceHtml(r.place, `<p class="disc-how"><strong>${
      escapeHtml(r.e.service)}</strong> from ${escapeHtml(prettifyName(((state.stopData || {})[r.e.from] || {}).name || ""))} at ${
      escapeHtml(discoverClock(r.e.next))}: ${escapeHtml(String(r.e.ride_mins))} min to ${escapeHtml(prettifyName(r.stop.name))},
      then a ${escapeHtml(String(r.walk))} min walk. ${escapeHtml(String(r.e.trips))} journey${r.e.trips === 1 ? "" : "s"} there
      for the rest of today.</p>`)).join("")}</ol>
    <p class="disc-note">From the published timetable, not live times. Open the stop for the live board.</p>`;
}

function discoverPanelHtml() {
  const places = (disc.data && disc.data.places) || [];
  const results = disc.reach ? discoverResultsHtml(discoverReachable(places, disc.reach), disc.from.label) : "";
  return `<section class="disc-intro">
      <h2 class="improvements-title">Discover Worthing & Brighton</h2>
      <p>Places worth a visit, and the ones a bus reaches without changing from where you are.</p>
      <p class="disc-actions"><button type="button" class="editor-action-btn primary" data-disc-locate>
        <svg class="icon" aria-hidden="true"><use href="#i-pin"/></svg>From where I am</button>
        <span class="disc-or">or tap any stop on the map</span></p>
      <p class="disc-status" role="status" aria-live="polite"></p>
    </section>
    <div class="disc-results">${results}</div>
    <details class="disc-all"${disc.reach ? "" : " open"}>
      <summary>Every place (${places.length})</summary>
      <ul class="disc-list">${places.map(p => discoverPlaceHtml(p, (p.stops || []).length
        ? `<p class="disc-how">Nearest stop: ${escapeHtml(prettifyName(p.stops[0].name))}, a ${escapeHtml(String(
          Math.max(1, Math.round(p.stops[0].metres / DISCOVER_WALK_M_PER_MIN))))} min walk.</p>` : "")).join("")}</ul>
    </details>
    <p class="stop-access-source">Places chosen by hand, each with its own page. Where they are and their wheelchair
      access from OpenStreetMap, © OpenStreetMap contributors, ODbL.</p>`;
}

function discoverRender() {
  const host = document.getElementById("tab-content-discover");
  if (host) host.innerHTML = discoverPanelHtml();
}

function discoverSay(text) {
  const el = document.querySelector("#tab-content-discover .disc-status");
  if (el) el.textContent = text;
}

/** Reach from these stops, and show it. */
async function discoverFromStops(atcos, label) {
  const seq = ++disc.seq;
  discoverSay("Finding the places a bus reaches from here…");
  let reach;
  try {
    const data = await apiFetch(`/api/direct-from?stops=${atcos.slice(0, 6).map(encodeURIComponent).join(",")}`);
    reach = data && data.reach;
  } catch {
    if (seq === disc.seq) discoverSay("The timetable could not be reached just now. Try again in a moment.");
    return;
  }
  if (seq !== disc.seq || state.viewMode !== "discover") return;
  disc.from = { atcos, label };
  disc.reach = reach || {};
  discoverRender();
  discoverDraw();
  track("view-discover-from");
  document.querySelector("#tab-content-discover .disc-title")?.focus?.();
}

function discoverLocate() {
  if (!("geolocation" in navigator)) { discoverSay("This browser cannot share a location. Tap a stop on the map."); return; }
  discoverSay("Finding where you are…");
  navigator.geolocation.getCurrentPosition((pos) => {
    const near = nearestStops(pos.coords.latitude, pos.coords.longitude, 4);
    if (!near.length) {
      discoverSay("You look to be outside the area this site covers. Tap a stop on the map instead.");
      return;
    }
    discoverFromStops(near.map(s => s.atco), "where you are");
  }, (err) => {
    discoverSay(err && err.code === 1 ? "Location is switched off for this site. Tap a stop on the map instead."
      : "We couldn't find your location just now. Tap a stop on the map instead.");
  }, { enableHighAccuracy: true, timeout: 12_000, maximumAge: 60_000 });
}

/** The places on the map: reachable ones stand out once a start is chosen. */
function discoverDraw() {
  if (!state.map || !disc.data) return;
  if (disc.layer) state.map.removeLayer(disc.layer);
  if (!state.map.getPane("discoverPane")) state.map.createPane("discoverPane").style.zIndex = 640;
  const reachable = new Set(discoverReachable(disc.data.places, disc.reach || {}).map(r => r.place.id));
  disc.layer = L.layerGroup();
  for (const p of disc.data.places || []) {
    if (!Number.isFinite(p.lat)) continue;
    const on = !disc.reach || reachable.has(p.id);
    L.circleMarker([p.lat, p.lon], { pane: "discoverPane", radius: 8, weight: 2,
      className: `disc-marker${on ? "" : " disc-marker--off"}` })
      .bindTooltip(escapeHtml(p.name), { direction: "top" })
      .on("click", () => document.getElementById(`disc-${p.id}`)?.scrollIntoView({ block: "nearest" }))
      .addTo(disc.layer);
  }
  disc.layer.addTo(state.map);
}

/** Show the view: called by applyViewMode with its ownership check. */
async function discoverShow(mine = () => true) {
  const host = document.getElementById("tab-content-discover");
  try { await loadDestinations(); } catch {
    if (mine() && host) host.innerHTML = `<p class="proposals-empty">The places could not be loaded. Try again in a moment.</p>`;
    return;
  }
  if (!mine()) return;
  discoverRender();
  discoverDraw();
  if (host && !host.dataset.bound) {
    host.dataset.bound = "1";
    host.addEventListener("click", (e) => { if (e.target.closest("[data-disc-locate]")) discoverLocate(); });
  }
}

function discoverHide() {
  if (disc.layer && state.map) state.map.removeLayer(disc.layer);
  disc.layer = null;
}
