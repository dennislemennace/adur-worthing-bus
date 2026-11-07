/*
 * Discover Worthing & Brighton (asked for 6 November 2026): places worth
 * visiting (data/destinations.json), and which of them a bus reaches without
 * changing from where you are: the bus to take, when it leaves, the ride and
 * the walk at the far end. From your location (it stays on this device, as
 * Stops near me does) or from any stop tapped on the map. The reach comes
 * from /api/direct-from, which reads only the timetable.
 *
 * Each place opens in place (7 November 2026): a larger photo with its
 * credit, more about it, how to get there, and the map centred on it. A pin
 * tapped on the map opens its place in the list. Each place is listed once,
 * so a pin always has a row to open.
 *
 * Loaded after app.js, and uses its helpers. Draws its places on the map in
 * its own view only; discoverHide takes them down.
 */
"use strict";

const disc = { data: null, promise: null, layer: null, pins: new Map(), from: null, reach: null, seq: 0, open: null };
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

/** Minutes on foot from a stop: given by hand where a straight line misjudges
 *  the walk (Highdown's hill), otherwise from the distance. */
function discoverWalkMins(stop) {
  return stop.walk_mins || Math.max(1, Math.round(stop.metres / DISCOVER_WALK_M_PER_MIN));
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
      const walk = discoverWalkMins(s);
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

/** One line on getting there: the direct bus when there is one from the
 *  start chosen, otherwise the nearest stop and the walk. */
function discoverHowHtml(place, r) {
  if (r) {
    return `<strong>${escapeHtml(r.e.service)}</strong> from ${escapeHtml(prettifyName(((state.stopData || {})[r.e.from] || {}).name || ""))} at ${
      escapeHtml(discoverClock(r.e.next))}: ${escapeHtml(String(r.e.ride_mins))} min to ${escapeHtml(prettifyName(r.stop.name))},
      then a ${escapeHtml(String(r.walk))} min walk.`;
  }
  const s = (place.stops || [])[0];
  return s ? `Nearest stop: ${escapeHtml(prettifyName(s.name))}, a ${escapeHtml(String(discoverWalkMins(s)))} min walk.` : "";
}

function discoverPhotoHtml(photo) {
  if (!photo || !photo.src) return "";
  const licence = photo.licence_url
    ? `<a href="${escapeAttr(safeUrl(photo.licence_url))}" target="_blank" rel="noopener">${escapeHtml(photo.licence)}</a>`
    : escapeHtml(photo.licence);
  return `<figure class="disc-photo">
      <img src="${escapeAttr(photo.src)}" alt="${escapeAttr(photo.alt || "")}" width="720" height="480" loading="lazy" decoding="async" />
      <figcaption>Photo: ${escapeHtml(photo.author)}, ${licence}, via <a href="${
        escapeAttr(safeUrl(photo.source_url))}" target="_blank" rel="noopener">Wikimedia Commons</a></figcaption>
    </figure>`;
}

/** Getting there, in full: the direct bus from the start chosen, and the
 *  stops to get off at, with their walks and a link to each one's live board
 *  where the map has the stop. */
function discoverGettingHtml(place, r) {
  const stops = (place.stops || []).map(s => {
    const name = escapeHtml(prettifyName(s.name));
    const walk = `${escapeHtml(String(discoverWalkMins(s)))} min walk`;
    const live = (state.stopData || {})[s.atco] ? `<a href="#stop=${encodeURIComponent(s.atco)}">${name}</a>` : name;
    return `<li>${live}, ${walk}</li>`;
  }).join("");
  return `${r ? `<p class="disc-how">${discoverHowHtml(place, r)} ${escapeHtml(String(r.e.trips))} journey${
      r.e.trips === 1 ? "" : "s"} there for the rest of today, from the timetable.</p>` : ""}
    ${stops ? `<p class="disc-sub">Stops to get off at</p><ul class="disc-stops">${stops}</ul>` : ""}`;
}

function discoverPlaceHtml(place, r = null) {
  const id = escapeAttr(place.id);
  const open = disc.open === place.id;
  const access = discoverAccess(place);
  const thumb = place.photo && place.photo.thumb
    ? `<img class="disc-thumb" src="${escapeAttr(place.photo.thumb)}" alt="" width="64" height="64" loading="lazy" decoding="async" />`
    : `<span class="disc-thumb disc-thumb--none" aria-hidden="true"></span>`;
  return `<li class="disc-place${open ? " disc-place--open" : ""}" id="disc-${id}" data-place="${id}">
    <button type="button" class="disc-place-head" aria-expanded="${open}" aria-controls="disc-${id}-more">
      ${thumb}
      <span class="disc-place-text">
        <span class="disc-place-name">${escapeHtml(place.name)}<span class="disc-cat">${escapeHtml(place.category)}</span></span>
        <span class="disc-line">${escapeHtml(place.line)}</span>
        <span class="disc-how">${discoverHowHtml(place, r)}</span>
      </span>
      <svg class="icon disc-chevron" aria-hidden="true"><use href="#i-chevron-down"/></svg>
    </button>
    <div class="disc-more" id="disc-${id}-more"${open ? "" : " hidden"}>
      ${discoverPhotoHtml(place.photo)}
      ${place.about ? `<p class="disc-about">${escapeHtml(place.about)}</p>` : ""}
      ${discoverGettingHtml(place, r)}
      <p class="disc-meta">${access ? `${escapeHtml(access)}, says OpenStreetMap · ` : ""}<a href="${
        escapeAttr(safeUrl(place.source_url))}" target="_blank" rel="noopener">Its own page<span class="visually-hidden">
        (opens in a new tab)</span></a></p>
    </div>
  </li>`;
}

/** The places reached directly from a start, then the rest: each once. */
function discoverResultsHtml(rows, fromLabel, others = []) {
  const rest = others.length ? `<h3 class="disc-subtitle">A change of bus or more from ${escapeHtml(fromLabel)}</h3>
    <ul class="disc-list">${others.map(p => discoverPlaceHtml(p)).join("")}</ul>` : "";
  if (!rows.length) {
    return `<p class="disc-note">No place on this list is a direct bus ride from ${escapeHtml(fromLabel)} for the rest
      of today. Try another stop, or open any place below for its nearest stops.</p>${rest}`;
  }
  return `<h3 class="disc-title" tabindex="-1">Direct from ${escapeHtml(fromLabel)}</h3>
    <p class="disc-note">From the published timetable, not live times. Open a place for its stops and their live boards.</p>
    <ol class="disc-list">${rows.map(r => discoverPlaceHtml(r.place, r)).join("")}</ol>${rest}`;
}

function discoverPanelHtml() {
  const places = [...((disc.data && disc.data.places) || [])].sort((a, b) => a.name.localeCompare(b.name));
  let results;
  if (disc.reach) {
    const rows = discoverReachable(places, disc.reach);
    const reached = new Set(rows.map(r => r.place.id));
    results = discoverResultsHtml(rows, disc.from.label, places.filter(p => !reached.has(p.id)));
  } else {
    results = `<h3 class="disc-subtitle">Every place (${places.length})</h3>
      <ul class="disc-list">${places.map(p => discoverPlaceHtml(p)).join("")}</ul>`;
  }
  return `<section class="disc-intro">
      <h2 class="improvements-title">Discover Worthing & Brighton</h2>
      <p>Places worth a visit, and the ones a bus reaches without changing from where you are.</p>
      <p class="disc-actions"><button type="button" class="editor-action-btn primary" data-disc-locate>
        <svg class="icon" aria-hidden="true"><use href="#i-pin"/></svg>From where I am</button>
        <span class="disc-or">or tap any stop on the map</span></p>
      <p class="disc-status" role="status" aria-live="polite"></p>
    </section>
    <div class="disc-results">${results}</div>
    <p class="stop-access-source">Places chosen by hand, each with its own page. Where they are and their wheelchair
      access from OpenStreetMap, © OpenStreetMap contributors, ODbL. Photos from Wikimedia Commons, credited under
      each and on <a href="about.html#photo-credits">the About page</a>.</p>`;
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
  disc.open = null;
  discoverRender();
  discoverDraw();
  track("view-discover-from");
  const title = document.querySelector("#tab-content-discover .disc-title");
  if (title) {
    title.focus({ preventScroll: true });
    if (typeof scrollPanelTo === "function") scrollPanelTo(title, 8);
  }
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

/**
 * Open a place (or close it, when it is the one open): the others close, the
 * map centres on it in the part the sheet leaves showing, and the list
 * scrolls to it. From a pin, the sheet comes up first if it was down.
 */
function discoverOpen(id, { from = "list" } = {}) {
  const place = ((disc.data && disc.data.places) || []).find(p => p.id === id);
  if (!place) return;
  const closing = disc.open === id && from === "list";
  disc.open = closing ? null : id;
  document.querySelectorAll("#tab-content-discover .disc-place").forEach(li => {
    const on = li.dataset.place === disc.open;
    li.classList.toggle("disc-place--open", on);
    li.querySelector(".disc-place-head")?.setAttribute("aria-expanded", String(on));
    const more = li.querySelector(".disc-more");
    if (more) more.hidden = !on;
  });
  discoverMarkPins();
  if (closing) return;
  if (isSheetLayout()) {
    // The sheet comes up from a pin if it was down, and down from a list if
    // it hid the map (fully open, or a short landscape screen): either way
    // half, so the place and its spot on the map both show.
    const shown = state.map ? state.map.getSize().y - (sheetDetentPixels()[state.sheetDetent] || 0) : 0;
    if ((from === "map" && ["peek", "closed"].includes(state.sheetDetent)) || shown < 80) setSheetDetent("half");
  }
  if (state.map && Number.isFinite(place.lat)) {
    const overlap = isSheetLayout() ? sheetDetentPixels()[state.sheetDetent] || 0 : 0;
    centreAboveSheet([place.lat, place.lon], Math.max(state.map.getZoom(), 15), overlap);
  }
  const li = document.getElementById(`disc-${id}`);
  if (li && typeof scrollPanelTo === "function") {
    // Once the sheet has settled, so the scroll measures the height it ends at.
    const go = () => scrollPanelTo(li, 8);
    if (typeof afterSheetSettles === "function") afterSheetSettles(go); else go();
  }
  if (from === "map") li?.querySelector(".disc-place-head")?.focus({ preventScroll: true });
  track("view-discover-place");
}

/** The open place's pin stands out; reachable ones are full colour once a
 *  start is chosen, the rest faded. */
function discoverMarkPins() {
  const reachable = disc.reach ? new Set(discoverReachable(disc.data.places, disc.reach).map(r => r.place.id)) : null;
  for (const [id, pin] of disc.pins) {
    const cls = ["disc-marker", reachable && !reachable.has(id) ? "disc-marker--off" : "",
      disc.open === id ? "disc-marker--open" : ""].filter(Boolean).join(" ");
    const el = pin.getElement && pin.getElement();
    if (el) el.setAttribute("class", `leaflet-interactive ${cls}`);
    else pin.options.className = cls;
    pin.setRadius(disc.open === id ? 12 : 8);
    if (disc.open === id && el) pin.bringToFront();
  }
}

/** The places on the map, each pin opening its place. Its tooltip shows the
 *  place's photo and name. */
function discoverDraw() {
  if (!state.map || !disc.data) return;
  if (disc.layer) state.map.removeLayer(disc.layer);
  if (!state.map.getPane("discoverPane")) state.map.createPane("discoverPane").style.zIndex = 640;
  disc.layer = L.layerGroup();
  disc.pins = new Map();
  for (const p of disc.data.places || []) {
    if (!Number.isFinite(p.lat)) continue;
    const tip = `${p.photo && p.photo.thumb ? `<img class="disc-tip-img" src="${escapeAttr(p.photo.thumb)}" alt="" width="48" height="48" />` : ""}<span>${escapeHtml(p.name)}</span>`;
    const pin = L.circleMarker([p.lat, p.lon], { pane: "discoverPane", radius: 8, weight: 2, className: "disc-marker" })
      .bindTooltip(tip, { direction: "top", className: "disc-tip" })
      .on("click", () => discoverOpen(p.id, { from: "map" }))
      .addTo(disc.layer);
    disc.pins.set(p.id, pin);
  }
  disc.layer.addTo(state.map);
  discoverMarkPins();
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
    host.addEventListener("click", (e) => {
      if (e.target.closest("[data-disc-locate]")) { discoverLocate(); return; }
      const head = e.target.closest(".disc-place-head");
      if (head) discoverOpen(head.closest(".disc-place").dataset.place);
    });
  }
}

function discoverHide() {
  if (disc.layer && state.map) state.map.removeLayer(disc.layer);
  disc.layer = null;
  disc.pins = new Map();
}
