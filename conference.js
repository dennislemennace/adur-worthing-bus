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

/** Offered only with ?preview=1, and only while current. */
function conferenceOffered(data, now = new Date(), preview = previewEnabled()) {
  return Boolean(preview) && conferenceActive(data, now);
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

function confRouteChips(routes, operator = "BHBC") {
  return (routes || []).map(r => {
    const colour = typeof getRouteColour === "function" ? getRouteColour(r, r === "700" || r === "N700" ? "SCSO" : operator) : "#444";
    const fg = typeof pickTextOn === "function" && pickTextOn(colour) === "dark" ? "#000" : "#fff";
    return `<span class="conf-route" style="background:${escapeAttr(colour)};color:${fg}">${escapeHtml(r)}</span>`;
  }).join("");
}

function confLink(url, text) {
  return `<a href="${escapeAttr(safeUrl(url))}" target="_blank" rel="noopener noreferrer">${escapeHtml(text)}</a>`;
}

function conferencePanelHtml(d) {
  const v = d.venue;
  const stops = (d.nearest_stops || []).map(s =>
    `<li><button type="button" class="conf-stop" data-conf-stop="${escapeAttr(s.atco)}" data-name="${escapeAttr(s.name)}">
       <span class="conf-stop-name">${escapeHtml(s.name)}</span>
       <span class="conf-stop-hint">${escapeHtml(s.hint)} · live times</span></button></li>`).join("");
  const arrivals = (d.arrivals || []).map(a => `
    <li class="conf-card">
      <p class="conf-card-title">${escapeHtml(a.kind === "train" ? "By train" : `By coach: ${a.operator}`)}</p>
      <p class="conf-card-sub">${escapeHtml(a.name)} · ${confWalk(a.walk_minutes)} to the venue</p>
      <p>${escapeHtml(a.how)}</p>
      ${a.from_where ? `<ul class="conf-list">${a.from_where.map(x => `<li>${escapeHtml(x)}</li>`).join("")}</ul>
        <p class="conf-small">Times and engineering work: ${confLink(a.source_url, "National Rail Enquiries")}.</p>` : ""}
    </li>`).join("");
  const hotels = (d.hotel_areas || []).map(h => `
    <li class="conf-card">
      <p class="conf-card-title">${escapeHtml(h.name)}</p>
      <p class="conf-card-sub">Walk: ${escapeHtml(h.walk)}</p>
      ${h.buses && h.buses.length ? `<p class="conf-routes">${confRouteChips(h.buses)}</p>` : ""}
      ${h.night && h.night.length ? `<p class="conf-small">At night: ${confRouteChips(h.night)}</p>` : ""}
      ${h.note ? `<p class="conf-warn">${escapeHtml(h.note)}</p>` : ""}
    </li>`).join("");
  const socials = (d.socials || []).map(s => `
    <li class="conf-card">
      <p class="conf-card-title">${escapeHtml(s.event)}</p>
      <p class="conf-card-sub">${escapeHtml(s.day)}, ${escapeHtml(s.time)}</p>
      <p>${escapeHtml(s.name)}: ${escapeHtml(s.area)}, ${confWalk(s.walk_minutes)} from the venue.</p>
    </li>`).join("");
  const parking = (d.driving && d.driving.text || []).map(t => `<li>${escapeHtml(t)}</li>`).join("");
  return `
    <section class="conf-section conf-glance">
      <h2 class="conf-h">${escapeHtml(d.conference.name)}</h2>
      <p class="conf-lede"><strong>${escapeHtml(v.name)}</strong>, ${escapeHtml(v.address)}.
        ${escapeHtml(d.conference.dates)}.</p>
      <p>Central Brighton is compact: the station, the coach stop, the Lanes and most
        hotels are within a 15 minute walk of the venue, and buses run every few minutes.</p>
      <button type="button" class="editor-action-btn primary conf-venue-btn" data-conf-venue>
        <svg class="icon" aria-hidden="true"><use href="#i-pin"/></svg><span>About the venue</span></button>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">Nearest bus stops</h3>
      <p>Almost every bus route stops within 3 minutes' walk. Tap a stop for its live departures.</p>
      <ul class="conf-stops">${stops}</ul>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">Getting here</h3>
      <ul class="conf-cards">${arrivals}</ul>
      <div class="conf-card">
        <p class="conf-card-title">By car</p>
        <ul class="conf-list">${parking}</ul>
        <p class="conf-small">${confLink(d.driving.source_url, "Council car parks and prices")}.</p>
      </div>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">Tickets and paying</h3>
      <ul class="conf-list">${(d.tickets || []).map(t => `<li>${escapeHtml(t)}</li>`).join("")}</ul>
      <p class="conf-small">${confLink(d.tickets_url, "Brighton & Hove Buses tickets")} ·
        <a href="#view=t" data-view-link="tickets">Compare tickets for a journey</a></p>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">From your hotel</h3>
      <p>Buses that run between each area and the stops by the venue.
        ${confLink(d.hotels_url, "Conference hotel bookings")}.</p>
      <ul class="conf-cards">${hotels}</ul>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">Evenings and socials</h3>
      <p>${escapeHtml(d.on_site)}</p>
      <ul class="conf-cards">${socials}</ul>
      <p class="conf-small">Full list: ${confLink(d.socials_url, "social events at Conference")}.</p>
      <p class="conf-late"><strong>Getting back late.</strong> ${escapeHtml(d.late_night)}</p>
    </section>

    <section class="conf-section">
      <h3 class="conf-h3">Accessibility</h3>
      <ul class="conf-list">${(d.accessibility || []).map(t => `<li>${escapeHtml(t)}</li>`).join("")}</ul>
      <p class="conf-small">${confLink(v.access_url, "Brighton Centre access statement")}.</p>
    </section>

    <p class="conf-small conf-checked">Checked ${escapeHtml(d.checked_on)} from the venue,
      the conference organisers, the operators and our own timetable data. Times can change:
      check live departures before you travel.</p>`;
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
    <ul class="conf-stops">${(d.nearest_stops || []).map(s =>
      `<li><button type="button" class="conf-stop" data-conf-stop="${escapeAttr(s.atco)}" data-name="${escapeAttr(s.name)}">
        <span class="conf-stop-name">${escapeHtml(s.name)}</span>
        <span class="conf-stop-hint">${escapeHtml(s.hint)} · live times</span></button></li>`).join("")}</ul>
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
  for (const a of d.arrivals || []) pin(a.lat, a.lon, "conf-pin--arrival", a.name, null);
  for (const s of d.socials || []) pin(s.lat, s.lon, "conf-pin--social", `${s.name}: ${s.event}`, null);
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
  host.innerHTML = conferencePanelHtml(d);
  if (!host.dataset.bound) {
    host.dataset.bound = "1";
    host.addEventListener("click", (e) => {
      const stop = e.target.closest("[data-conf-stop]");
      if (stop) { conferenceOpenStop(stop.dataset.confStop, stop.dataset.name); return; }
      if (e.target.closest("[data-conf-venue]")) conferenceVenueDialog();
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
