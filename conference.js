/* Conference travel: a short-lived guide for visitors to one event.
 *
 * Everything it says comes from data/conference.json, which carries its own
 * sources and a show_until date. It is offered only with ?preview=1, and
 * after that date not at all. Loaded after app.js and uses its helpers
 * (escapeHtml, escapeAttr, safeUrl, centreAboveSheet, openDepartures,
 * setViewMode, wireEvidenceClose, getRouteColour). Its map markers are its
 * own and come down whenever another view is chosen (conferenceHide, called
 * from applyViewMode).
 */
"use strict";

const conf = { data: null, promise: null, layer: null };

function loadConference() {
  if (!conf.promise) {
    conf.promise = fetch("data/conference.json")
      .then(res => (res.ok ? res.json() : null))
      .catch(() => null)
      .then(data => { conf.data = data; return data; });
  }
  return conf.promise;
}

/** London's date, so the guide disappears at midnight here, not in UTC. */
function conferenceToday(now = new Date()) {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/London",
    year: "numeric", month: "2-digit", day: "2-digit" }).format(now);
}

function conferenceActive(data, now = new Date()) {
  return Boolean(data && data.show_until && conferenceToday(now) <= data.show_until);
}

/** Offered to everyone while current; it leaves the menu after show_until. */
function conferenceOffered(data, now = new Date()) {
  return conferenceActive(data, now);
}

/** Show the menu option where the guide is offered. */
async function conferenceInitMenu() {
  const data = await loadConference();
  const item = document.querySelector('#section-nav-menu [data-mode="conference"]');
  if (item) item.hidden = !conferenceOffered(data);
}

function confWalk(min) {
  return `${min} min walk`;
}

/** Route numbers lowest first: 1, 1X, 2, 5B, 12, 12X, 700, N700. */
function confRouteOrder(a, b) {
  const key = r => { const m = /^([A-Z]*)(\d+)(.*)$/i.exec(r) || ["", "", "9999", r];
                     return [Number(m[2]), m[1], m[3]]; };
  const [x, y] = [key(a), key(b)];
  return x[0] - y[0] || x[1].localeCompare(y[1]) || x[2].localeCompare(y[2]);
}

function confRouteChips(routes, operator = "BHBC") {
  return [...(routes || [])].sort(confRouteOrder).map(r => {
    const colour = typeof getRouteColour === "function" ? getRouteColour(r, r === "700" || r === "N700" ? "SCSO" : operator) : "#444";
    const fg = typeof pickTextOn === "function" && pickTextOn(colour) === "dark" ? "#000" : "#fff";
    return `<span class="conf-route" style="background:${escapeAttr(colour)};color:${fg}">${escapeHtml(r)}</span>`;
  }).join("");
}

function confLink(url, text) {
  return `<a href="${escapeAttr(safeUrl(url))}" target="_blank" rel="noopener noreferrer">${escapeHtml(text)}</a>`;
}

function confStopButtons(stops) {
  return (stops || []).map(s =>
    `<li><button type="button" class="conf-stop${s.limited ? " conf-stop--limited" : ""}" data-conf-stop="${escapeAttr(s.atco)}" data-name="${escapeAttr(s.name)}">
       <span class="conf-stop-name">${escapeHtml(s.name)}</span>
       <span class="conf-stop-hint">${escapeHtml(s.hint)} · live times</span></button></li>`).join("");
}

function confList(items) {
  return `<ul class="conf-list">${(items || []).map(t => `<li>${escapeHtml(t)}</li>`).join("")}</ul>`;
}

function conferencePanelHtml(d) {
  const v = d.venue, st = d.station;
  const coaches = (d.coaches || []).map(c => `<li><strong>${escapeHtml(c.name)}</strong>
      (${confWalk(c.walk_minutes)}). ${escapeHtml(c.how)}${c.buses && c.buses.length
        ? ` <span class="conf-routes">${confRouteChips(c.buses)}</span>` : ""}</li>`).join("");
  const hotels = (d.hotel_areas || []).map(h => `
    <li class="conf-card">
      <p class="conf-card-title">${escapeHtml(h.name)}</p>
      <p><strong>Quickest:</strong> ${h.quickest === "walk"
        ? `walk, about ${h.walk_minutes} min.`
        : `bus, or a ${h.walk_minutes} min walk.`}</p>
      ${h.buses && h.buses.length ? `<p class="conf-routes"><span class="conf-small">Direct buses:</span> ${confRouteChips(h.buses)}</p>` : ""}
      ${h.note ? `<p class="conf-warn">${escapeHtml(h.note)}</p>` : ""}
    </li>`).join("");
  const venues = (d.social_venues || []).map(s => `
    <li class="conf-card">
      <p class="conf-card-title">${escapeHtml(s.name)}</p>
      <p><strong>From the conference:</strong> walk, about ${s.walk_minutes} min. ${escapeHtml(s.where)}</p>
    </li>`).join("");
  const taxis = (d.taxi_ranks || []).map(t => `<li>${escapeHtml(t.name)} (${confWalk(t.walk_minutes)})</li>`).join("");
  return `
    <section class="conf-section conf-glance">
      <h2 class="conf-h">${escapeHtml(d.conference.name)}: travel info</h2>
      <p class="conf-lede"><strong>${escapeHtml(v.name)}</strong>, ${escapeHtml(v.address)}.
        ${escapeHtml(d.conference.dates)}.</p>
      <p>The station, the coach stop, the Lanes and most hotels are within a 15 minute walk,
        and buses to the stops by the venue run every few minutes.</p>
      <div class="conf-actions">
        <button type="button" class="editor-action-btn primary" data-conf-locate>
          <svg class="icon" aria-hidden="true"><use href="#i-pin"/></svg><span>Find my nearest stop</span></button>
        <button type="button" class="editor-action-btn" data-conf-venue><span>About the venue</span></button>
      </div>
      <div class="conf-locate-result" id="conf-locate-result" aria-live="polite"></div>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">Nearest bus stops</h3>
      <p>Almost every bus route stops within 3 minutes' walk. Tap a stop for its live departures.</p>
      <ul class="conf-stops conf-stops--grid">${confStopButtons(d.nearest_stops)}</ul>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">From Brighton Station</h3>
      <div class="conf-card">
        <p><strong>Walk:</strong> ${escapeHtml(st.walk_text)}</p>
        <p><strong>Bus:</strong> ${escapeHtml(st.bus_text)}</p>
        <p class="conf-routes">${confRouteChips(st.buses)}</p>
        <p><strong>Going back:</strong> ${escapeHtml(st.back_text)}</p>
        <p class="conf-routes"><span class="conf-small">From Old Steine to the station:</span> ${confRouteChips(st.back_buses)}</p>
        <p class="conf-small">${escapeHtml(st.taxi)} Train times and engineering work:
          ${confLink(st.source_url, "National Rail Enquiries")}.</p>
      </div>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">By coach</h3>
      <div class="conf-card"><ul class="conf-list">${coaches}</ul></div>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">By car</h3>
      <div class="conf-card">${confList(d.driving.text)}
        <p class="conf-small">${confLink(d.driving.source_url, "Council car parks and prices")}.</p></div>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">Tickets and paying</h3>
      ${confList(d.tickets)}
      <p class="conf-small">${confLink(d.tickets_url, "Brighton & Hove Buses tickets")} ·
        <a href="#view=t" data-view-link="tickets">Compare tickets for a journey</a></p>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">From your hotel</h3>
      <ul class="conf-cards">${hotels}</ul>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">Getting to the socials</h3>
      <p>${escapeHtml(d.on_site)} ${confLink(d.socials_url, "Social events at Conference")}.</p>
      <ul class="conf-cards">${venues}</ul>
      <p class="conf-late"><strong>Getting back late.</strong> ${escapeHtml(d.late_night)}</p>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">Taxis</h3>
      <p>Closest ranks to the venue:</p>
      <ul class="conf-list">${taxis}</ul>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">Accessibility</h3>
      ${confList(d.accessibility)}
    </section>

    <p class="conf-small conf-checked">Checked ${escapeHtml(d.checked_on)} from the venue,
      the conference organisers, the operators and our own timetable data. Times can change:
      check live departures before you travel.</p>`;
}

// ── Find my nearest stop ───────────────────────────────────────
// The device's position never leaves it: the stops with a direct bus to the
// venue are a static file, and the nearest is worked out here.
const confLoc = { promise: null };

function loadConferenceStops() {
  if (!confLoc.promise) {
    confLoc.promise = fetch("data/conference_stops.json")
      .then(res => (res.ok ? res.json() : null)).catch(() => null)
      .then(d => (d && Array.isArray(d.stops) ? d.stops : []));
  }
  return confLoc.promise;
}

/** Nearest stops to (lat, lon), straight-line metres, closest first. Pure. */
function conferenceNearestStops(stops, lat, lon, n = 2) {
  const m = s => Math.hypot((s.lat - lat) * 111195, (s.lon - lon) * 111195 * Math.cos(lat * Math.PI / 180));
  return (stops || []).map(s => ({ ...s, metres: m(s) })).sort((a, b) => a.metres - b.metres).slice(0, n);
}

async function conferenceLocate() {
  const out = document.getElementById("conf-locate-result");
  if (!out) return;
  if (!("geolocation" in navigator)) {
    out.innerHTML = `<p class="conf-small">Your browser cannot share its location. Use the stops listed below.</p>`;
    return;
  }
  out.innerHTML = `<p class="conf-small">Finding where you are…</p>`;
  navigator.geolocation.getCurrentPosition(async pos => {
    const stops = await loadConferenceStops();
    const near = conferenceNearestStops(stops, pos.coords.latitude, pos.coords.longitude);
    if (!near.length || near[0].metres > 5000) {
      out.innerHTML = `<p class="conf-small">No stop with a direct bus to the venue is near you. Try the journey planner or a taxi.</p>`;
      return;
    }
    out.innerHTML = `<p class="conf-small">Nearest stops with a direct bus to the venue
        (Clock Tower, Churchill Square or North Street):</p>
      <ul class="conf-stops">${near.map(s => `<li><button type="button" class="conf-stop"
        data-conf-stop="${escapeAttr(s.atco)}" data-name="${escapeAttr(s.name)}">
        <span class="conf-stop-name">${escapeHtml(s.name)}${s.towards ? ` <span class="conf-small">towards ${escapeHtml(s.towards)}</span>` : ""}</span>
        <span class="conf-stop-hint">${Math.round(s.metres)} m, about ${Math.max(1, Math.round(s.metres * 1.3 / 80))} min walk · live times</span>
        <span class="conf-routes">${confRouteChips(s.routes)}</span></button></li>`).join("")}</ul>`;
  }, () => {
    out.innerHTML = `<p class="conf-small">Your location was not available. Use the stops listed below.</p>`;
  }, { enableHighAccuracy: true, timeout: 10000, maximumAge: 60000 });
}

function conferenceVenueDialog() {
  const d = conf.data;
  const dialog = document.getElementById("evidence-dialog");
  const body = document.getElementById("evidence-body");
  if (!d || !dialog || !body) return;
  const v = d.venue;
  body.innerHTML = `
    <button type="button" class="evidence-close" data-close-evidence aria-label="Close">&times;</button>
    <h2 class="evidence-title" id="evidence-title">${escapeHtml(v.name)}</h2>
    <p class="evidence-standfirst">${escapeHtml(v.address)} · ${escapeHtml(d.conference.name)},
      ${escapeHtml(d.conference.dates)}</p>
    <ul class="conf-list">${(v.notes || []).map(n => `<li>${escapeHtml(n)}</li>`).join("")}</ul>
    <h3 class="conf-h3">Nearest stops</h3>
    <ul class="conf-stops">${confStopButtons(d.nearest_stops)}</ul>
    <p class="conf-small">${confLink(v.access_url, "Access statement")} ·
      ${confLink(v.source_url, "Getting here, from the venue")}</p>`;
  if (typeof dialog.showModal === "function") { if (!dialog.open) dialog.showModal(); }
  else dialog.setAttribute("open", "");
  wireEvidenceClose(dialog, body);
  body.querySelectorAll("[data-conf-stop]").forEach(b => b.addEventListener("click", () => {
    if (typeof dialog.close === "function") dialog.close();
    conferenceOpenStop(b.dataset.confStop, b.dataset.name);
  }));
}

function conferenceOpenStop(atco, name) {
  setViewMode("live");
  openDepartures(atco, name);
}

function conferenceMarkers(d) {
  const group = L.layerGroup();
  const pin = (lat, lon, cls, label, onClick, size = 28) => {
    const m = L.marker([lat, lon], {
      icon: L.divIcon({ className: `conf-pin ${cls}`, html: `<span class="conf-pin-dot"></span>`,
                        iconSize: [size, size], iconAnchor: [size / 2, size / 2] }),
      title: label, keyboard: true, zIndexOffset: 900,
    });
    m.bindTooltip(escapeHtml(label), { direction: "top", offset: [0, -size / 2] });
    if (onClick) m.on("click", onClick);
    return m.addTo(group);
  };
  pin(d.venue.lat, d.venue.lon, "conf-pin--venue", `${d.venue.name}: ${d.conference.name}`, conferenceVenueDialog, 44);
  pin(d.station.lat, d.station.lon, "conf-pin--arrival", d.station.name, null);
  for (const c of d.coaches || []) pin(c.lat, c.lon, "conf-pin--arrival", c.name, null);
  for (const s of d.social_venues || []) pin(s.lat, s.lon, "conf-pin--social", s.name, null);
  for (const t of d.taxi_ranks || []) pin(t.lat, t.lon, "conf-pin--taxi", `Taxi rank: ${t.name}`, null, 22);
  return group;
}

/** Show the guide: called by applyViewMode with its ownership check. */
async function conferenceShow(mine = () => true) {
  const host = document.getElementById("tab-content-conference");
  const d = await loadConference();
  if (!mine()) return;
  if (!host) return;
  if (!d || !d.venue) {
    host.innerHTML = `<p class="proposals-empty">The conference guide could not be loaded.</p>`;
    return;
  }
  if (!conferenceActive(d)) {
    host.innerHTML = `<p class="proposals-empty">The conference has finished, so its travel guide is no longer shown.</p>`;
    return;
  }
  host.innerHTML = conferencePanelHtml(d);
  if (!host.dataset.bound) {
    host.dataset.bound = "1";
    host.addEventListener("click", (e) => {
      const stop = e.target.closest("[data-conf-stop]");
      if (stop) { conferenceOpenStop(stop.dataset.confStop, stop.dataset.name); return; }
      if (e.target.closest("[data-conf-venue]")) conferenceVenueDialog();
      if (e.target.closest("[data-conf-locate]")) conferenceLocate();
    });
  }
  conferenceHide();
  conf.layer = conferenceMarkers(d).addTo(state.map);
  centreAboveSheet([d.venue.lat, d.venue.lon], 16);
}

function conferenceHide() {
  if (conf.layer && state.map) state.map.removeLayer(conf.layer);
  conf.layer = null;
}

conferenceInitMenu();
