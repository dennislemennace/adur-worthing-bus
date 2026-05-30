/**
 * app.js — Adur & Worthing Live Bus Tracker
 *
 * Responsibilities:
 *  1. Initialise the Leaflet map centred on Adur & Worthing
 *  2. Load bus stops from the backend and render them as map markers
 *  3. Periodically fetch live bus positions and animate their markers
 *  4. On stop-click, fetch the live departure board and render it in the panel
 *
 * All API calls go to the backend proxy (see api/main.py).
 * The BODS API key is kept server-side and never appears here.
 *
 * CONFIGURATION — change API_BASE_URL after deploying your backend:
 */

const CONFIG = {
  // ─── Backend API base URL ────────────────────────────────────────────────
  // Development:  "http://localhost:8000"
  // Production:   Render deployment URL.
  API_BASE_URL: "https://adur-worthing-bus.onrender.com",

  // Geographic centre of Adur & Worthing
  MAP_CENTER:  [50.818, -0.372],   // [lat, lon] — Worthing town centre area
  MAP_ZOOM:    13,
  MAP_ZOOM_MIN: 10,
  MAP_ZOOM_MAX: 18,

  // How often to refresh live bus positions (milliseconds)
  VEHICLE_REFRESH_MS: 20_000,      // 20 seconds

  // How many departures to request from the API
  DEPARTURES_COUNT: 10,
};

// ============================================================
// ICON HELPER
// ============================================================
function svgIcon(id) {
  return `<svg class="icon" aria-hidden="true"><use href="#${id}"/></svg>`;
}

// ============================================================
// STATE
// ============================================================
const TILES = {
  light: {
    url: "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    maxZoom: 19,
  },
  dark: {
    url: "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    maxZoom: 19,
  },
};

const state = {
  map: null,
  tileLayer: null,       // active Leaflet tile layer
  darkMode: false,
  stopMarkers:   {},    // atcoCode → Leaflet marker
  stopData:      {},    // atcoCode → { lat, lon }
  busMarkers:    {},    // vehicleRef → Leaflet marker
  selectedStop:  null,  // { atcoCode, name }
  refreshTimer:  null,  // setInterval handle for bus positions
  isRefreshing:  true,
  busesVisible:  true,  // header toggle: show bus markers + run live refresh

  // ── Bus info panel state ──
  selectedVehicleRef:      null,   // vehicle_ref of bus shown in Bus tab
  selectedVehicle:         null,   // last-known vehicle object for that ref
  selectedVehicleLastSeen: null,   // Date of last feed where the bus appeared
  selectedVehicleLost:     false,  // true once it drops out of the feed
  followSelectedBus:       false,  // map-follow checkbox state
  activeTab:               "stop", // "stop" | "bus"
  busInfoTickTimer:        null,   // setInterval handle for "X ago" text
  busDetails:              null,   // /api/vehicle response for selected bus
  busDetailsLoading:       false,  // true while waiting on /api/vehicle

  // ── Improvements view (network/proposals mode) ──
  viewMode:                "live", // "live" | "improvements"
  serviceMode:             "day",  // "day" | "night" — splits chips, route lines, proposals
  visibleCategories:       null,   // Set<"focused"|"express"|"other">; populated on first load
  visibleOperators:        null,   // Set of operator buckets ("BHBC","SCSO","COMT","OTHER",""); ""=unknown
  showLimitedServices:     false,  // false = hide services that don't run all week or finish before 18:00
  routeLines:              null,   // /api/route-lines response, fetched lazily
  routeLineLayers:         {},     // service short_name → array of L.polyline
  routeCategoryByService:  {},     // service → "focused"|"express"|"other"
  routeOperatorByService:  {},     // service → operator NOC (e.g. "COMT", "")
  routeFrequencyByService: {},     // service → { is_frequent_all_day, runs_days, ... }
  visibleRoutes:           null,   // Set of service short_names; null = all visible
  proposals:               null,   // data/proposals.json
  proposalLayers:          {},     // proposal id → array of L.polyline
  showProposals:           false,  // map overlay toggle in Improvements mode
  selectedProposalId:      null,

  // ── Proposal editor ──
  editor:              null,      // active draft object; null = editor closed
  editorMode:          "addStop", // "move" | "addStop" — stops-only routing
  editorLayers:        null,      // L.featureGroup holding draft polyline + markers
  editorDrafts:        [],        // cached copy of localStorage["proposalDrafts"]
  editorAutosaveTimer: null,      // setTimeout handle for debounced save
  editorStopsIndex:    null,      // atco → {name, lat, lon} for quick lookup
};

// ============================================================
// DOM REFERENCES
// ============================================================
const dom = {
  mapLoading:         document.getElementById("map-loading"),
  lastUpdatedLabel:   document.getElementById("last-updated-label"),
  toggleBusesBtn:     document.getElementById("toggle-buses-btn"),
  darkModeBtn:        document.getElementById("dark-mode-btn"),
  departurePanel:     document.getElementById("departure-panel"),
  panelStopName:      document.getElementById("panel-stop-name"),
  panelStopId:        document.getElementById("panel-stop-id"),
  closePanelBtn:      document.getElementById("close-panel-btn"),
  panelLoading:       document.getElementById("panel-loading"),
  panelError:         document.getElementById("panel-error"),
  panelErrorMsg:      document.getElementById("panel-error-msg"),
  panelRetryBtn:      document.getElementById("panel-retry-btn"),
  panelPrompt:        document.getElementById("panel-prompt"),
  departuresContainer:document.getElementById("departures-container"),
  departuresTbody:    document.getElementById("departures-tbody"),
  departuresCount:    document.getElementById("departures-count"),
  departuresNotice:   document.getElementById("departures-notice"),
  refreshStopBtn:     document.getElementById("refresh-stop-btn"),
  toast:              document.getElementById("toast"),

  // ── Tabs ──
  tabStop:            document.getElementById("tab-stop"),
  tabBus:             document.getElementById("tab-bus"),
  tabContentStop:     document.getElementById("tab-content-stop"),
  tabContentBus:      document.getElementById("tab-content-bus"),

  // ── Bus tab ──
  panelBusName:       document.getElementById("panel-bus-name"),
  panelBusId:         document.getElementById("panel-bus-id"),
  busPanelPrompt:     document.getElementById("bus-panel-prompt"),
  busInfoContainer:   document.getElementById("bus-info-container"),

  // ── View mode toggle ──
  viewModeLive:          document.getElementById("view-mode-live"),
  viewModeImprovements:  document.getElementById("view-mode-improvements"),

  // ── Improvements view ──
  closePanelBtnImprovements: document.getElementById("close-panel-btn-improvements"),
  serviceModeDay:         document.getElementById("service-mode-day"),
  serviceModeNight:       document.getElementById("service-mode-night"),
  serviceCategoryToggle:  document.getElementById("service-category-toggle"),
  serviceOperatorToggle:  document.getElementById("service-operator-toggle"),
  showLimitedServices:    document.getElementById("show-limited-services"),
  tabAbout:               document.getElementById("tab-about"),
  tabProposals:           document.getElementById("tab-proposals"),
  tabContentAbout:        document.getElementById("tab-content-about"),
  tabContentProposals:    document.getElementById("tab-content-proposals"),
  routeFilterChips:       document.getElementById("route-filter-chips"),
  routesAllBtn:           document.getElementById("routes-all-btn"),
  routesNoneBtn:          document.getElementById("routes-none-btn"),
  proposalsList:          document.getElementById("proposals-list"),
  mapOverlayControls:     document.getElementById("map-overlay-controls"),

  // ── Proposal editor ──
  proposalsView:          document.getElementById("proposals-view"),
  newProposalBtn:         document.getElementById("new-proposal-btn"),
  draftsSection:          document.getElementById("drafts-section"),
  draftsList:             document.getElementById("drafts-list"),
  proposalEditor:         document.getElementById("proposal-editor"),
};

// ============================================================
// URL HASH STATE — share-friendly deep links
// ============================================================
// Round-trips a minimal subset of `state` to/from location.hash so a stop,
// vehicle, proposal, view mode, or service mode can be shared as a single
// link. Defaults are omitted to keep URLs tidy. Loop avoidance: every
// mutator that calls pushUrlState() is suppressed while applyUrlState() is
// re-applying a URL we just navigated to (via popstate or initial load).

function parseUrlState() {
  const hash = (location.hash || "").replace(/^#/, "");
  const out = {};
  if (!hash) return out;
  for (const pair of hash.split("&")) {
    if (!pair) continue;
    const eq = pair.indexOf("=");
    const k = eq === -1 ? pair : pair.slice(0, eq);
    const v = eq === -1 ? "" : pair.slice(eq + 1);
    try { out[k] = decodeURIComponent(v.replace(/\+/g, " ")); }
    catch { out[k] = v; }
  }
  return out;
}

function buildUrlHash() {
  const parts = [];
  if (state.viewMode === "improvements") parts.push("view=i");
  if (state.serviceMode === "night")     parts.push("svc=n");
  if (state.viewMode === "live") {
    if (state.selectedStop && state.selectedStop.atcoCode) {
      parts.push("stop=" + encodeURIComponent(state.selectedStop.atcoCode));
    }
    if (state.selectedVehicleRef) {
      parts.push("bus=" + encodeURIComponent(state.selectedVehicleRef));
    }
  } else if (state.selectedProposalId) {
    parts.push("proposal=" + encodeURIComponent(state.selectedProposalId));
  }
  return parts.length ? "#" + parts.join("&") : "";
}

function pushUrlState({ major = false } = {}) {
  if (state._suppressUrlSync) return;
  const newHash = buildUrlHash();
  if (newHash === (location.hash || "")) return;
  const url = newHash || (location.pathname + location.search);
  try {
    if (major) history.pushState(null, "", url);
    else       history.replaceState(null, "", url);
  } catch { /* sandboxed contexts can block history APIs */ }
}

async function applyUrlState(parsed) {
  state._suppressUrlSync = true;
  // Clear any pending tokens from a prior URL — popstate may have arrived
  // before a previous deep-link finished resolving.
  state._pendingBusRef     = null;
  state._pendingProposalId = null;
  try {
    const svc = parsed.svc === "n" ? "night" : "day";
    if (state.serviceMode !== svc) setServiceMode(svc);

    const view = parsed.view === "i" ? "improvements" : "live";
    if (state.viewMode !== view) setViewMode(view);

    if (view === "improvements") {
      if (parsed.proposal) {
        if (state.proposals) {
          const match = state.proposals.find(x => x.id === parsed.proposal);
          if (match) selectProposal(parsed.proposal);
        } else {
          state._pendingProposalId = parsed.proposal;
        }
      }
      return;
    }

    if (parsed.stop) {
      const pos = state.stopData[parsed.stop];
      if (pos) {
        await window.openDepartures(parsed.stop, pos.name || parsed.stop);
      } else {
        showToast("Stop not found.");
      }
    }
    if (parsed.bus) {
      const marker = state.busMarkers[parsed.bus];
      if (marker && marker._vehicle) {
        openBusInfo(marker._vehicle);
      } else {
        // Vehicles populate after the first /api/vehicles tick — defer.
        state._pendingBusRef = parsed.bus;
      }
    }
  } finally {
    state._suppressUrlSync = false;
    // If anything changed during the apply, persist the canonical hash
    // (handles cases where parsed dropped invalid tokens).
    pushUrlState();
  }
}

function resolvePendingBusRef() {
  const ref = state._pendingBusRef;
  if (!ref) return;
  const marker = state.busMarkers[ref];
  if (marker && marker._vehicle) {
    state._pendingBusRef = null;
    openBusInfo(marker._vehicle);
  }
}

function resolvePendingProposalId() {
  const id = state._pendingProposalId;
  if (!id || !state.proposals) return;
  state._pendingProposalId = null;
  if (state.proposals.find(x => x.id === id)) selectProposal(id);
}

// ============================================================
// INITIALISE
// ============================================================
document.addEventListener("DOMContentLoaded", init);

async function init() {
  // Apply dark mode before map/paint so there's no flash
  if (localStorage.getItem("darkMode") === "1") {
    state.darkMode = true;
    document.body.classList.add("dark-mode");
    dom.darkModeBtn.innerHTML = svgIcon("i-sun");
    dom.darkModeBtn.title = "Switch to light mode";
  }

  initMap();
  bindUIEvents();

  // Restore any proposal drafts saved in localStorage from a previous session.
  state.editorDrafts = loadDraftsFromStorage();
  renderDraftsSection();

  // Load stops first (cached 24 h on backend, so fast after first call)
  await loadStops();

  // Hide initial loading overlay
  dom.mapLoading.classList.add("hidden");

  // Restore deep-linked state from URL hash, if any. Stops are loaded;
  // vehicles and proposals resolve asynchronously via _pending* tokens.
  await applyUrlState(parseUrlState());

  // Start live bus position loop
  startVehicleRefresh();

  // Warm the Improvements-view cache in the background once the page is
  // idle, so the first Live→Improvements switch doesn't pay the route-lines
  // network fetch (and layer build) synchronously. Idempotent + memoized.
  prefetchImprovementsData();
}

/** Pre-fetch + pre-build the Improvements layers during idle time. */
function prefetchImprovementsData() {
  const run = () => {
    loadRouteLines().catch(() => {});
    loadProposals().catch(() => {});
  };
  if ("requestIdleCallback" in window) {
    requestIdleCallback(run, { timeout: 3000 });
  } else {
    setTimeout(run, 1200);
  }
}

// ============================================================
// MAP
// ============================================================
function initMap() {
  state.map = L.map("map", {
    center: CONFIG.MAP_CENTER,
    zoom:   CONFIG.MAP_ZOOM,
    minZoom: CONFIG.MAP_ZOOM_MIN,
    maxZoom: CONFIG.MAP_ZOOM_MAX,
    zoomControl: true,
  });

  // Tile layer — swapped when dark mode toggles
  const t = state.darkMode ? TILES.dark : TILES.light;
  state.tileLayer = L.tileLayer(t.url, {
    attribution: t.attribution,
    maxZoom: t.maxZoom,
  }).addTo(state.map);

  // Shrink stop dots slightly when zoomed out a lot (≤ z12) so ~1400
  // markers don't crowd the map. CSS-driven via a class on the container.
  const applyStopDotScale = () => {
    state.map.getContainer().classList.toggle("stops-far", state.map.getZoom() <= 12);
  };
  state.map.on("zoomend", applyStopDotScale);
  applyStopDotScale();

  // Close panel when clicking an empty area of the map. Leaflet bubbles
  // marker/popup clicks up to the map 'click' event, so we need to
  // ignore anything whose DOM target is inside a marker or popup —
  // otherwise selecting a bus would immediately re-close the panel.
  state.map.on("click", (e) => {
    if (state._ignoreNextMapClick) {
      state._ignoreNextMapClick = false;
      return;
    }
    const t = e.originalEvent && e.originalEvent.target;
    if (t && t.closest &&
        t.closest(".leaflet-marker-icon, .leaflet-marker-pane, .leaflet-popup, .leaflet-popup-pane, .bus-marker-wrapper")) {
      return;
    }
    if (state.selectedStop || state.selectedVehicleRef) closePanel();
  });
}

// ============================================================
// BUS STOPS
// ============================================================
async function loadStops() {
  try {
    const data = await apiFetch("/api/stops");
    if (!data || !data.stops) throw new Error("Invalid stops response");

    // Render in chunks across animation frames so inserting ~1400 markers
    // doesn't block the main thread (which caused visible jutter); the map
    // stays responsive and markers populate progressively behind the
    // loading dialog. Resolves once the last chunk has painted.
    await renderStopsInChunks(data.stops);
    applyStopVisibility();   // honour any view/service mode set from URL before stops arrived
  } catch (err) {
    console.error("Failed to load stops:", err);
    showToast("Could not load bus stops. Check your API configuration.");
  }
}

function renderStopsInChunks(stops, chunkSize = 150) {
  return new Promise((resolve) => {
    let i = 0;
    function step() {
      const end = Math.min(i + chunkSize, stops.length);
      for (; i < end; i++) renderStopMarker(stops[i]);
      if (i < stops.length) {
        requestAnimationFrame(step);
      } else {
        resolve();
      }
    }
    if (stops.length === 0) { resolve(); return; }
    requestAnimationFrame(step);
  });
}

function renderStopMarker(stop) {
  // stop: { atco_code, name, latitude, longitude }
  const icon = L.divIcon({
    className: "stop-marker-icon",
    iconSize:  [12, 12],
    iconAnchor:[6, 6],
    popupAnchor:[0, -8],
  });

  const marker = L.marker([stop.latitude, stop.longitude], { icon, title: stop.name })
    .addTo(state.map);

  // Bind the popup content lazily — Leaflet evaluates this function only
  // when the popup actually opens. Building ~1400 HTML strings eagerly at
  // load was a measurable chunk of initial render time.
  marker.bindPopup(() => buildStopPopupHtml(stop.atco_code, stop.name),
                   { maxWidth: 220 });

  // Clicking anywhere on the marker opens the departure panel
  marker.on("click", () => {
    openDepartures(stop.atco_code, stop.name);
  });

  state.stopMarkers[stop.atco_code] = marker;
  state.stopData[stop.atco_code]    = {
    lat: stop.latitude,
    lon: stop.longitude,
    name: stop.name,
    night_serving: !!stop.night_serving,
  };
}

/** Hide/show stop markers based on the current view + service mode +
 *  selected proposal. Called when any of those change.
 *
 *  Rules:
 *    - Live view: every stop visible (live tracker shows the whole network).
 *    - Improvements + Day: every stop visible.
 *    - Improvements + Night: only stops with night_serving=true, PLUS any
 *      stops belonging to the currently-selected proposal (so opening a
 *      proposal reveals the stops it relies on even if a night route
 *      doesn't currently serve them).
 */
function applyStopVisibility() {
  if (!state.map || !state.stopMarkers) return;
  const filterToNight =
    state.viewMode === "improvements" && state.serviceMode === "night";

  const overrideShow = new Set();
  if (state.selectedProposalId) {
    const p = (state.proposals || []).find(x => x.id === state.selectedProposalId);
    if (p && Array.isArray(p.stops)) {
      for (const s of p.stops) {
        if (s && s.atco_code) overrideShow.add(s.atco_code);
      }
    }
  }

  for (const atco in state.stopMarkers) {
    const marker = state.stopMarkers[atco];
    let shouldShow = true;
    if (filterToNight) {
      const data = state.stopData[atco] || {};
      shouldShow = data.night_serving || overrideShow.has(atco);
    }
    const has = state.map.hasLayer(marker);
    if (shouldShow && !has)      state.map.addLayer(marker);
    else if (!shouldShow && has) state.map.removeLayer(marker);
  }
}

/** Popup body for a stop — built on demand (see renderStopMarker). */
function buildStopPopupHtml(atcoCode, name) {
  return `
    <div>
      <p class="popup-stop-name">${escapeHtml(name)}</p>
      <p class="popup-stop-id">Stop: ${escapeHtml(atcoCode)}</p>
      <button class="popup-btn" onclick="openDepartures('${atcoCode}', '${escapeAttr(name)}')">
        <svg class="icon" aria-hidden="true"><use href="#i-clock"/></svg>
        <span>Live departures</span>
      </button>
    </div>`;
}

// ============================================================
// LIVE BUS POSITIONS
// ============================================================
function startVehicleRefresh() {
  fetchVehicles();   // immediate first call
  state.refreshTimer = setInterval(fetchVehicles, CONFIG.VEHICLE_REFRESH_MS);
  state.isRefreshing = true;
}

function stopVehicleRefresh() {
  clearInterval(state.refreshTimer);
  state.refreshTimer = null;
  state.isRefreshing = false;
}

/** Header toggle: show/hide bus markers and pause/resume the live refresh
 *  together. Buses are always hidden in Improvements mode, so this only
 *  acts on the map while in Live view; the preference is remembered and
 *  re-applied when returning to Live (see applyViewMode). */
function setBusesVisible(on) {
  state.busesVisible = !!on;
  updateBusesToggleBtn();
  if (state.viewMode === "improvements") return;
  if (state.busesVisible) {
    showVehicleMarkers();
    if (!state.isRefreshing) startVehicleRefresh();
  } else {
    hideVehicleMarkers();
    if (state.isRefreshing) stopVehicleRefresh();
  }
}

function updateBusesToggleBtn() {
  if (!dom.toggleBusesBtn) return;
  const on = state.busesVisible;
  dom.toggleBusesBtn.setAttribute("aria-pressed", on ? "true" : "false");
  dom.toggleBusesBtn.setAttribute("aria-label", on ? "Hide buses" : "Show buses");
  dom.toggleBusesBtn.title = on ? "Hide buses" : "Show buses";
}

async function fetchVehicles() {
  try {
    const data = await apiFetch("/api/vehicles");
    if (!data || !data.vehicles) return;

    updateVehicleMarkers(data.vehicles);
    resolvePendingBusRef();

    const now = new Date().toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    dom.lastUpdatedLabel.textContent = `Updated ${now}`;
  } catch (err) {
    console.warn("Vehicle refresh failed:", err);
    dom.lastUpdatedLabel.textContent = "Update failed — retrying…";
  }
}

function updateVehicleMarkers(vehicles) {
  const seenRefs = new Set();

  vehicles.forEach(vehicle => {
    // vehicle: { vehicle_ref, service_ref, destination, latitude, longitude, bearing, delay_seconds, operator_ref }
    if (!vehicle.latitude || !vehicle.longitude) return;

    const ref = vehicle.vehicle_ref;
    seenRefs.add(ref);

    const label = vehicle.service_ref || "?";
    const bearing = (vehicle.bearing != null && !isNaN(vehicle.bearing))
      ? Number(vehicle.bearing)
      : null;

    let marker = state.busMarkers[ref];
    if (marker) {
      // Move existing marker smoothly and update bearing/label in place
      marker._vehicle = vehicle;
      marker.setLatLng([vehicle.latitude, vehicle.longitude]);
      // Only rebuild popup HTML for the (at most one) open popup; skipping
      // every closed bus avoids N string builds on each 20s refresh.
      if (marker.isPopupOpen()) {
        marker.setPopupContent(buildBusPopupHtml(vehicle, label));
      }
      updateBusMarkerInPlace(marker, label, bearing);
    } else {
      const icon = createBusIcon(vehicle.operator_ref, label, bearing);
      marker = L.marker([vehicle.latitude, vehicle.longitude], { icon, zIndexOffset: 200 })
        .bindPopup(() => buildBusPopupHtml(marker._vehicle,
                                           marker._vehicle.service_ref || "?"),
                   { maxWidth: 220 })
        .addTo(state.map);
      marker._vehicle = vehicle;
      marker.on("click", () => {
        state._ignoreNextMapClick = true;
        if (marker._vehicle) openBusInfo(marker._vehicle);
      });
      state.busMarkers[ref] = marker;
    }

    // Keep selected-bus state in sync if this is the one we're tracking
    if (state.selectedVehicleRef && ref === state.selectedVehicleRef) {
      state.selectedVehicle = vehicle;
      state.selectedVehicleLastSeen = new Date();
      state.selectedVehicleLost = false;
      if (state.activeTab === "bus") renderBusTab();
      if (state.followSelectedBus) {
        state.map.panTo([vehicle.latitude, vehicle.longitude], { animate: true });
      }
      // Refresh the upcoming-stops list as the bus moves
      fetchBusDetails(ref);
    }
  });

  // Remove markers for buses no longer in the feed
  Object.keys(state.busMarkers).forEach(ref => {
    if (!seenRefs.has(ref)) {
      state.map.removeLayer(state.busMarkers[ref]);
      delete state.busMarkers[ref];
    }
  });

  // If our selected bus dropped out of the feed, mark as lost
  if (state.selectedVehicleRef
      && !seenRefs.has(state.selectedVehicleRef)
      && !state.selectedVehicleLost) {
    state.selectedVehicleLost = true;
    if (state.activeTab === "bus") renderBusTab();
  }
}

/**
 * Build the small popup that appears when a bus marker is clicked.
 * Restyled in Phase 1 to show operator icon, badge, destination,
 * status chip and a hint pointing the user at the side panel.
 */
function buildBusPopupHtml(vehicle, label) {
  const iconUrl = OPERATOR_ICONS[vehicle.operator_ref];
  const colour  = getRouteColour(vehicle.service_ref || label, vehicle.operator_ref);
  const badgeTextCls = pickTextOn(colour) === "dark"
    ? "service-badge--dark-text"
    : "service-badge--light-text";

  const iconHtml = iconUrl
    ? `<img class="bus-popup-icon" src="${escapeAttr(iconUrl)}" alt="">`
    : `<div class="bus-popup-icon bus-popup-icon-fallback" style="background:${colour}"></div>`;

  let statusHtml = "";
  if (vehicle.delay_seconds != null) {
    const chip = buildStatusChip({ delay_seconds: vehicle.delay_seconds });
    statusHtml = `<p class="bus-popup-status"><span class="status-chip ${chip.cssClass}">${escapeHtml(chip.label)}</span></p>`;
  }

  const destText = prettifyName(vehicle.destination || vehicle.trip_headsign) || "Unknown";

  return `
    <div class="bus-popup">
      <div class="bus-popup-header">
        ${iconHtml}
        <span class="service-badge ${badgeTextCls}" style="background:${colour}">${escapeHtml(label)}</span>
      </div>
      <p class="bus-popup-destination">To ${escapeHtml(destText)}</p>
      ${statusHtml}
      <p class="bus-popup-hint">See Bus tab for full details →</p>
    </div>`;
}

/**
 * Build a Leaflet divIcon for a bus.
 * Icons are side-profile (wheels-at-bottom) and authored facing EAST.
 * We don't rotate them — rotating a side view puts the wheels on top
 * whenever the bus heads west. Instead we mirror the image horizontally
 * when the heading is in the western half, so the bus always stays
 * right-side-up and still indicates direction via left/right facing.
 */
function createBusIcon(operatorRef, label, bearing) {
  const iconUrl  = OPERATOR_ICONS[operatorRef];
  const transform = iconTransformForBearing(bearing);

  let inner;
  if (iconUrl) {
    inner = `
      <img class="bus-icon-img" src="${escapeAttr(iconUrl)}" alt=""
           style="transform:${transform}">
      <span class="bus-icon-label">${escapeHtml(label)}</span>`;
  } else {
    const bg     = getRouteColour(label, operatorRef);
    const border = getOperatorBorderColour(operatorRef);
    inner = `
      <div class="bus-icon-fallback"
           style="background:${bg};border-color:${border}">${escapeHtml(label)}</div>`;
  }

  return L.divIcon({
    className:  "bus-marker-divicon",
    html:       `<div class="bus-marker-wrapper">${inner}</div>`,
    iconSize:   [56, 56],
    iconAnchor: [28, 28],
    popupAnchor:[0, -28],
  });
}

/**
 * Pick a CSS transform for the bus image based on its compass bearing.
 *
 * Side-profile icons can't represent every angle naturally, so we
 * quantize the 360° compass into 8 buckets. Cardinal buckets (N, E,
 * S, W) are 60° wide so ±30° of BODS bearing noise on a straight road
 * still lands in the right bucket; intercardinal buckets (NE, SE, SW,
 * NW) are 30° wide, so a bus has to be genuinely within 15° of a true
 * diagonal heading to render tilted.
 *
 *   Bucket   Bearing range    Transform
 *   ────────────────────────────────────────────────────────
 *   N        330° – 30°       rotate(−45°)
 *   NE       30°  – 60°       rotate(−22.5°)
 *   E        60°  – 120°      rotate(0°)
 *   SE       120° – 150°      rotate(22.5°)
 *   S        150° – 210°      rotate(45°)
 *   SW       210° – 240°      scaleX(−1) rotate(22.5°)
 *   W        240° – 300°      scaleX(−1) rotate(0°)
 *   NW       300° – 330°      scaleX(−1) rotate(−22.5°)
 */
function iconTransformForBearing(bearing) {
  if (bearing == null) return "none";
  const b = ((Number(bearing) % 360) + 360) % 360;

  if (b >= 30  && b < 60)  return "rotate(-22.5deg)";            // NE
  if (b >= 60  && b < 120) return "rotate(0deg)";                // E
  if (b >= 120 && b < 150) return "rotate(22.5deg)";             // SE
  if (b >= 150 && b < 210) return "rotate(45deg)";               // S
  if (b >= 210 && b < 240) return "scaleX(-1) rotate(22.5deg)";  // SW
  if (b >= 240 && b < 300) return "scaleX(-1) rotate(0deg)";     // W
  if (b >= 300 && b < 330) return "scaleX(-1) rotate(-22.5deg)"; // NW
  return "rotate(-45deg)";                                       // N
}

/**
 * Update an existing bus marker's facing and route label without
 * recreating the icon. Cheaper and avoids a flash on every refresh.
 */
function updateBusMarkerInPlace(marker, label, bearing) {
  const el = marker.getElement();
  if (!el) return;

  const img = el.querySelector(".bus-icon-img");
  if (img) {
    img.style.transform = iconTransformForBearing(bearing);
  }

  const labelEl = el.querySelector(".bus-icon-label, .bus-icon-fallback");
  if (labelEl && labelEl.textContent !== label) {
    labelEl.textContent = label;
  }
}

// ============================================================
// DEPARTURE BOARD
// ============================================================

/**
 * openDepartures — called when a stop marker or popup button is clicked.
 * Exported to window so it can be used in inline onclick="" attributes
 * in Leaflet popup HTML.
 */
window.openDepartures = async function(atcoCode, stopName) {
  // Editor is in "add stop" mode AND the editor UI is on-screen — clicking
  // a stop adds it to the draft. We gate on the Proposals tab being the
  // active one so a stop click from the About tab doesn't silently mutate
  // the draft behind the user's back.
  const editorVisible =
    state.editor &&
    dom.tabContentProposals &&
    !dom.tabContentProposals.classList.contains("hidden");
  if (editorVisible && state.editorMode === "addStop") {
    const pos = state.stopData[atcoCode];
    if (pos) {
      addStopToDraft({ atco: atcoCode, name: stopName, lat: pos.lat, lon: pos.lon });
      pulseStopMarker(atcoCode);
    }
    state.map.closePopup();
    return;
  }

  // Stops are inert in Improvements mode (network-view rather than live).
  if (state.viewMode === "improvements") return;

  // Close any open Leaflet popup to avoid clutter
  state.map.closePopup();

  state.selectedStop = { atcoCode, stopName };
  pushUrlState();

  // Update panel header
  dom.panelStopName.textContent = stopName;
  dom.panelStopId.textContent   = `ATCO: ${atcoCode}`;

  // Make sure the Stop tab is the one in front
  setActiveTab("stop");

  // Show panel, hide prompt
  showPanelState("loading");

  // On mobile, scroll down so the panel is visible
  dom.departurePanel.scrollIntoView({ behavior: "smooth", block: "end" });

  await fetchDepartures(atcoCode);
};

async function fetchDepartures(atcoCode) {
  showPanelState("loading");
  try {
    let url = `/api/departures?stopId=${encodeURIComponent(atcoCode)}`;
    const pos = state.stopData[atcoCode];
    if (pos) url += `&lat=${pos.lat}&lon=${pos.lon}`;
    const data = await apiFetch(url);
    renderDepartures(data);
  } catch (err) {
    console.error("Departures fetch failed:", err);
    showPanelState("error", err.message || "Could not load departure data.");
  }
}

function renderDepartures(data) {
  // data: { stop_name, departures: [...], live?: bool, live_reason?: string }
  const raw = data?.departures ?? [];

  // Live-data notice: show when live=false and it's a degradation (not just "too far away")
  const liveNoticeMessages = {
    quota:       "Showing scheduled times only \u2014 live predictions paused for today",
    upstream:    "Showing scheduled times only \u2014 live data unavailable",
    no_coverage: "Showing scheduled times only \u2014 no live tracking for this stop",
    ip_quota:    "Showing scheduled times only \u2014 live predictions paused",
  };
  const reason = data?.live_reason;
  if (data?.live === false && reason && reason !== "too_far" && liveNoticeMessages[reason]) {
    dom.departuresNotice.textContent = liveNoticeMessages[reason];
    dom.departuresNotice.classList.remove("hidden");
  } else {
    dom.departuresNotice.classList.add("hidden");
  }

  // Belt-and-braces: drop anything whose display time is more than
  // 30 seconds in the past. The backend already filters past trips,
  // but cached responses can briefly contain entries that have just
  // departed.
  const now = Date.now();
  const departures = raw.filter(d => {
    const iso = d.expected_departure || d.aimed_departure;
    if (!iso) return true;
    const t = new Date(iso).getTime();
    return isNaN(t) || (t - now) > -30_000;
  });

  if (departures.length === 0) {
    dom.departuresTbody.innerHTML = `<tr><td colspan="4" class="no-departures">No departures found for this stop in the next 2 hours.</td></tr>`;
    dom.departuresCount.textContent = "No upcoming departures";
    showPanelState("results");
    return;
  }

  dom.departuresCount.textContent = `${departures.length} departure${departures.length !== 1 ? "s" : ""}`;

  dom.departuresTbody.innerHTML = departures
    .slice(0, CONFIG.DEPARTURES_COUNT)
    .map(dep => buildDepartureRow(dep))
    .join("");

  showPanelState("results");
}

function buildDepartureRow(dep) {
  // dep: { service, destination, aimed_departure, expected_departure, status, delay_seconds }

  const service     = dep.service     || "?";
  const destination = prettifyName(dep.destination) || "Unknown";
  const aimed       = dep.aimed_departure;
  const expected    = dep.expected_departure;

  // Format due time (prefer expected if available, fall back to aimed)
  const displayTime = expected || aimed || null;
  const dueText     = displayTime ? formatDueTime(displayTime) : "–";
  const isImminent  = displayTime ? isWithinMinutes(displayTime, 2) : false;

  // Status
  const { label, cssClass } = buildStatusChip(dep);

  const badgeColour  = getRouteColour(service, dep.operator_ref);
  const badgeTextCls = pickTextOn(badgeColour) === "dark"
    ? "service-badge--dark-text"
    : "service-badge--light-text";

  return `
    <tr class="departure-row" data-service="${escapeHtml(service)}" title="Show this bus on the map">
      <td><span class="service-badge ${badgeTextCls}" style="background:${badgeColour}">${escapeHtml(service)}</span></td>
      <td><span class="destination-text" title="${escapeAttr(destination)}">${escapeHtml(destination)}</span></td>
      <td><span class="due-time ${isImminent ? "due-imminent" : ""}">${escapeHtml(dueText)}</span></td>
      <td><span class="status-chip ${cssClass}">${escapeHtml(label)}</span></td>
    </tr>`;
}

/**
 * Open the Bus tab for whichever live vehicle currently runs `service`.
 * If multiple vehicles share the service number, picks the one closest
 * to the selected stop. Shows a toast if no live vehicle is tracked.
 */
function openBusFromService(service) {
  // Some operators (Stagecoach SCSO) publish night variants without the
  // leading "N" — e.g. the timetable says "N700" but the live vehicle
  // reports "700". Match either form.
  const target     = service || "";
  const targetBare = stripNightPrefix(target);

  const matches = [];
  Object.values(state.busMarkers).forEach(marker => {
    const v = marker._vehicle;
    if (!v) return;
    const ref = v.service_ref || "";
    if (ref === target || stripNightPrefix(ref) === targetBare) {
      matches.push(v);
    }
  });

  if (matches.length === 0) {
    showToast(`No live vehicle currently tracked for service ${service}.`);
    return;
  }

  let chosen = matches[0];

  // Prefer the closest match to the selected stop, if we know its position
  if (state.selectedStop) {
    const stopMarker = state.stopMarkers[state.selectedStop.atcoCode];
    if (stopMarker) {
      const { lat, lng } = stopMarker.getLatLng();
      let bestDist = Infinity;
      for (const v of matches) {
        const d = Math.hypot(v.latitude - lat, v.longitude - lng);
        if (d < bestDist) {
          bestDist = d;
          chosen = v;
        }
      }
    }
  }

  openBusInfo(chosen);
}

function buildStatusChip(dep) {
  // Use the status field from the API if available, otherwise derive from delay
  const status = (dep.status || "").toLowerCase();

  if (status === "on time")    return { label: "On time",  cssClass: "status-on-time" };
  if (status === "early")      return { label: "Early",    cssClass: "status-early"   };
  if (status === "late" || status === "delayed") return { label: "Delayed", cssClass: "status-late" };
  if (status === "cancelled")  return { label: "Cancelled",cssClass: "status-late"   };

  // Derive from delay_seconds if status not set
  if (dep.delay_seconds != null) {
    const mins = Math.round(dep.delay_seconds / 60);
    if (Math.abs(mins) <= 1) return { label: "On time",     cssClass: "status-on-time" };
    if (mins < -1)           return { label: `${Math.abs(mins)}m early`, cssClass: "status-early" };
    return { label: `${mins}m late`, cssClass: "status-late" };
  }

  return { label: "Scheduled", cssClass: "status-scheduled" };
}

/** Format an ISO datetime string as a due-time label */
function formatDueTime(isoString) {
  try {
    const d = new Date(isoString);
    if (isNaN(d.getTime())) return isoString; // Return as-is if not parseable
    const now = new Date();
    const diffMs = d - now;
    const diffMins = Math.round(diffMs / 60_000);

    if (diffMins < 0)    return "Departed";
    if (diffMins === 0)  return "Due";
    if (diffMins === 1)  return "1 min";
    if (diffMins < 60)   return `${diffMins} mins`;

    // Show clock time for further out
    return d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
  } catch {
    return isoString;
  }
}

/** Returns true if the ISO datetime is within `minutes` minutes from now */
function isWithinMinutes(isoString, minutes) {
  try {
    const d = new Date(isoString);
    const diff = (d - new Date()) / 60_000;
    return diff >= 0 && diff <= minutes;
  } catch {
    return false;
  }
}

// ============================================================
// TABS + BUS INFO PANEL
// ============================================================

/** Switch the active panel tab. */
function setActiveTab(tab) {
  state.activeTab = tab;
  const stopActive = tab === "stop";

  dom.tabStop.classList.toggle("active", stopActive);
  dom.tabBus.classList.toggle("active", !stopActive);
  dom.tabStop.setAttribute("aria-selected", String(stopActive));
  dom.tabBus.setAttribute("aria-selected", String(!stopActive));

  dom.tabContentStop.classList.toggle("hidden", !stopActive);
  dom.tabContentBus.classList.toggle("hidden", stopActive);

  // Re-render the bus tab when becoming visible so its "X ago" is fresh
  if (!stopActive && state.selectedVehicle) {
    renderBusTab();
  }
}

/**
 * openBusInfo — called when a bus marker is clicked.
 * Switches to the Bus tab and renders the latest known data.
 */
function openBusInfo(vehicle) {
  state.selectedVehicleRef      = vehicle.vehicle_ref;
  state.selectedVehicle         = vehicle;
  state.selectedVehicleLastSeen = new Date();
  state.selectedVehicleLost     = false;
  state.busDetails              = null;
  state.busDetailsLoading       = true;
  pushUrlState();

  setActiveTab("bus");
  renderBusTab();
  startBusInfoTicker();

  // Fetch the matched GTFS trip + upcoming stops. Fire-and-forget:
  // when it resolves we update state.busDetails and re-render, but
  // only if the user is still looking at the same vehicle.
  fetchBusDetails(vehicle.vehicle_ref);

  // On mobile, scroll the panel into view
  dom.departurePanel.scrollIntoView({ behavior: "smooth", block: "end" });
}

async function fetchBusDetails(vehicleRef) {
  try {
    const data = await apiFetch(`/api/vehicle?vehicleRef=${encodeURIComponent(vehicleRef)}`);
    if (state.selectedVehicleRef !== vehicleRef) return;   // user moved on
    state.busDetails        = data;
    state.busDetailsLoading = false;
    renderBusTab();
  } catch (err) {
    if (state.selectedVehicleRef !== vehicleRef) return;
    state.busDetails        = null;
    state.busDetailsLoading = false;
    renderBusTab();
  }
}

/** Build the Bus tab body from the latest selected vehicle. */
function renderBusTab() {
  const v = state.selectedVehicle;

  if (!v) {
    dom.busPanelPrompt.classList.remove("hidden");
    dom.busInfoContainer.classList.add("hidden");
    dom.panelBusName.textContent = "No bus selected";
    dom.panelBusId.textContent   = "";
    return;
  }

  const operatorName = getOperatorName(v.operator_ref);
  const iconUrl      = OPERATOR_ICONS[v.operator_ref];
  const service      = v.service_ref || "?";
  const colour       = getRouteColour(service, v.operator_ref);
  const badgeTextCls = pickTextOn(colour) === "dark"
    ? "service-badge--dark-text"
    : "service-badge--light-text";
  const destination  = prettifyName(
                         v.destination
                         || state.busDetails?.vehicle?.trip_headsign
                         || v.trip_headsign
                       ) || "Unknown";
  const fleetId      = v.vehicle_ref || "–";
  const chip         = buildStatusChip({ delay_seconds: v.delay_seconds });
  const upcomingHtml = buildUpcomingStopsHtml();
  const ticketHtml   = buildTicketInfoHtml(v.operator_ref);

  const iconHtml = iconUrl
    ? `<img class="bus-info-icon" src="${escapeAttr(iconUrl)}" alt="">`
    : `<div class="bus-info-icon bus-info-icon-fallback" style="background:${colour}"></div>`;

  const lostBanner = state.selectedVehicleLost
    ? `<div class="bus-info-lost"><svg class="icon" aria-hidden="true"><use href="#i-signal-off"/></svg><span>Signal lost — last seen ${escapeHtml(formatTimeOfDay(state.selectedVehicleLastSeen))}</span></div>`
    : "";

  dom.panelBusName.textContent = `Service ${service}`;
  dom.panelBusId.textContent   = operatorName;

  dom.busInfoContainer.innerHTML = `
    ${lostBanner}
    <div class="bus-info-hero">
      ${iconHtml}
      <div class="bus-info-hero-text">
        <span class="service-badge service-badge-large ${badgeTextCls}" style="background:${colour}">${escapeHtml(service)}</span>
        <p class="bus-info-operator">${escapeHtml(operatorName)}</p>
      </div>
    </div>

    <dl class="bus-info-grid">
      <div class="bus-info-row">
        <dt>Destination</dt>
        <dd>${escapeHtml(destination)}</dd>
      </div>
      <div class="bus-info-row">
        <dt>Status</dt>
        <dd><span class="status-chip ${chip.cssClass}">${escapeHtml(chip.label)}</span></dd>
      </div>
      <div class="bus-info-row">
        <dt>Fleet ID</dt>
        <dd class="bus-info-mono">${escapeHtml(fleetId)}</dd>
      </div>
      <div class="bus-info-row">
        <dt>Updated</dt>
        <dd id="bus-info-updated">${escapeHtml(formatAgo(state.selectedVehicleLastSeen))}</dd>
      </div>
    </dl>

    <label class="follow-bus-toggle">
      <input type="checkbox" id="follow-bus-checkbox" ${state.followSelectedBus ? "checked" : ""}>
      <span>Follow this bus on the map</span>
    </label>

    ${upcomingHtml}

    ${ticketHtml}

    <p class="bus-info-footer">Live data · auto-refreshes every 20s</p>
  `;

  dom.busPanelPrompt.classList.add("hidden");
  dom.busInfoContainer.classList.remove("hidden");

  const cb = document.getElementById("follow-bus-checkbox");
  if (cb) {
    cb.addEventListener("change", (e) => {
      state.followSelectedBus = e.target.checked;
      if (state.followSelectedBus && state.selectedVehicle) {
        state.map.panTo(
          [state.selectedVehicle.latitude, state.selectedVehicle.longitude],
          { animate: true }
        );
      }
    });
  }
}

/**
 * Build the "Tickets" section for the Bus tab.
 * Uses static OPERATOR_TICKETS data for now; designed so that a future
 * API response (e.g. from /api/tickets?operatorRef=...) can be merged in
 * by passing it as the optional `liveData` argument.
 */
function buildTicketInfoHtml(operatorRef, liveData = null) {
  // Future: merge liveData fields over the static entry when available.
  const info = OPERATOR_TICKETS[operatorRef] || null;
  if (!info && !liveData) return "";

  const rows = [];

  if (info?.dayPass) {
    rows.push(`
      <div class="ticket-row">
        <span class="ticket-label">Day pass</span>
        <span class="ticket-value">${escapeHtml(info.dayPass)}</span>
      </div>`);
  }

  if (info?.app) {
    rows.push(`
      <div class="ticket-row">
        <span class="ticket-label">Mobile app</span>
        <span class="ticket-value">
          <a href="${escapeAttr(info.app.url)}" target="_blank" rel="noopener">${escapeHtml(info.app.name)}</a>
        </span>
      </div>`);
  }

  const footerLink = info?.url
    ? `<a class="ticket-more-link" href="${escapeAttr(info.url)}" target="_blank" rel="noopener">Full fares &amp; tickets →</a>`
    : "";

  return `
    <div class="ticket-info">
      <h3 class="ticket-info-title">Tickets</h3>
      <div class="ticket-rows">${rows.join("")}</div>
      ${footerLink}
    </div>`;
}

/**
 * Build the "Upcoming stops" section for the Bus tab from the detail
 * fetch. Returns an empty string when the fetch is still in flight or
 * returned no stops — keeps the layout tidy.
 */
function buildUpcomingStopsHtml() {
  if (state.busDetailsLoading) {
    return `
      <div class="upcoming-stops">
        <h3 class="upcoming-stops-title">Upcoming stops</h3>
        <p class="upcoming-stops-loading">Loading route…</p>
      </div>`;
  }
  const stops = state.busDetails?.upcoming_stops || [];
  if (stops.length === 0) return "";

  const rows = stops.map((s, i) => {
    const iso  = s.expected_departure || s.aimed_departure;
    const time = iso ? formatTimeOfDay(new Date(iso)) : "–";
    const name = escapeHtml(prettifyName(s.stop_name) || s.stop_id);
    if (s.is_terminus) {
      return `
        <li class="upcoming-stop-gap" aria-hidden="true">···</li>
        <li class="upcoming-stop upcoming-stop--terminus">
          <span class="upcoming-stop-marker" aria-hidden="true">◉</span>
          <span class="upcoming-stop-name">${name}</span>
          <span class="upcoming-stop-time">${escapeHtml(time)}</span>
        </li>`;
    }
    const marker = i === 0 ? "●" : "○";
    return `
      <li class="upcoming-stop">
        <span class="upcoming-stop-marker" aria-hidden="true">${marker}</span>
        <span class="upcoming-stop-name">${name}</span>
        <span class="upcoming-stop-time">${escapeHtml(time)}</span>
      </li>`;
  }).join("");

  const sourceNote = state.busDetails?.source === "siri_onward_calls"
    ? `<p class="upcoming-stops-note">From live vehicle · may be partial</p>`
    : "";

  return `
    <div class="upcoming-stops">
      <h3 class="upcoming-stops-title">Upcoming stops</h3>
      <ol class="upcoming-stops-list">${rows}</ol>
      ${sourceNote}
    </div>`;
}

/** Re-tick the "X ago" line every second without re-rendering the tab. */
function tickBusInfoUpdated() {
  const el = document.getElementById("bus-info-updated");
  if (el && state.selectedVehicleLastSeen) {
    el.textContent = formatAgo(state.selectedVehicleLastSeen);
  }
}

function startBusInfoTicker() {
  if (state.busInfoTickTimer) return;
  state.busInfoTickTimer = setInterval(tickBusInfoUpdated, 1000);
}

function stopBusInfoTicker() {
  if (state.busInfoTickTimer) {
    clearInterval(state.busInfoTickTimer);
    state.busInfoTickTimer = null;
  }
}

function formatAgo(ts) {
  if (!ts) return "–";
  const secs = Math.floor((Date.now() - ts.getTime()) / 1000);
  if (secs < 5)  return "just now";
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins} min${mins !== 1 ? "s" : ""} ago`;
  return ts.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
}

function formatTimeOfDay(ts) {
  if (!ts) return "–";
  return ts.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
}

// ============================================================
// PANEL STATE MACHINE
// ============================================================
function showPanelState(stateKey, errorMsg) {
  // Hide all inner states
  dom.panelLoading.classList.add("hidden");
  dom.panelError.classList.add("hidden");
  dom.panelPrompt.classList.add("hidden");
  dom.departuresContainer.classList.add("hidden");

  switch (stateKey) {
    case "loading": dom.panelLoading.classList.remove("hidden"); break;
    case "error":
      dom.panelError.classList.remove("hidden");
      if (errorMsg) dom.panelErrorMsg.textContent = errorMsg;
      break;
    case "results": dom.departuresContainer.classList.remove("hidden"); break;
    default:        dom.panelPrompt.classList.remove("hidden");  break;
  }
}

function toggleDarkMode() {
  state.darkMode = !state.darkMode;
  document.body.classList.toggle("dark-mode", state.darkMode);
  localStorage.setItem("darkMode", state.darkMode ? "1" : "0");
  dom.darkModeBtn.innerHTML = svgIcon(state.darkMode ? "i-sun" : "i-moon");
  dom.darkModeBtn.title = state.darkMode ? "Switch to light mode" : "Toggle dark mode";

  // Swap the map tile layer
  if (state.tileLayer) {
    state.map.removeLayer(state.tileLayer);
  }
  const t = state.darkMode ? TILES.dark : TILES.light;
  state.tileLayer = L.tileLayer(t.url, {
    attribution: t.attribution,
    maxZoom: t.maxZoom,
  }).addTo(state.map);
  // Ensure tiles sit below markers
  state.tileLayer.bringToBack();
}

/** Toggle the mobile collapsed state — keeps the tab strip visible but
 *  hides everything below it so the map can take the rest of the
 *  viewport. Updates aria-pressed + aria-label on each collapse button
 *  so a screen reader follows along. */
function togglePanelCollapsed() {
  const collapsed = document.body.classList.toggle("panel-collapsed");
  document.querySelectorAll(".btn-collapse-panel").forEach(btn => {
    btn.setAttribute("aria-pressed", collapsed ? "true" : "false");
    btn.setAttribute("aria-label", collapsed ? "Show panel" : "Hide panel");
  });
}

function closePanel() {
  // If the proposal editor is open, close it first (persists the draft).
  if (state.editor) closeEditor();

  // Clear stop selection
  state.selectedStop = null;
  showPanelState("prompt");
  dom.panelStopName.textContent = "Select a stop";
  dom.panelStopId.textContent   = "";

  // Clear bus selection
  state.selectedVehicleRef      = null;
  state.selectedVehicle         = null;
  state.selectedVehicleLastSeen = null;
  state.selectedVehicleLost     = false;
  state.followSelectedBus       = false;
  state.busDetails              = null;
  state.busDetailsLoading       = false;
  stopBusInfoTicker();
  dom.panelBusName.textContent  = "No bus selected";
  dom.panelBusId.textContent    = "";
  dom.busInfoContainer.classList.add("hidden");
  dom.busInfoContainer.innerHTML = "";
  dom.busPanelPrompt.classList.remove("hidden");

  // Default back to the Stop tab
  setActiveTab("stop");
  pushUrlState();
}

// ============================================================
// UI EVENT BINDINGS
// ============================================================
function bindUIEvents() {
  // Tab switcher
  dom.tabStop.addEventListener("click", () => setActiveTab("stop"));
  dom.tabBus.addEventListener("click",  () => setActiveTab("bus"));

  // Close panel button
  dom.closePanelBtn.addEventListener("click", closePanel);

  // Retry button in error state
  dom.panelRetryBtn.addEventListener("click", () => {
    if (state.selectedStop) {
      fetchDepartures(state.selectedStop.atcoCode);
    }
  });

  // Refresh departures button
  dom.refreshStopBtn.addEventListener("click", () => {
    if (state.selectedStop) {
      fetchDepartures(state.selectedStop.atcoCode);
    }
  });

  // Click a departure row → open the matching live vehicle in the Bus tab
  dom.departuresTbody.addEventListener("click", (e) => {
    const tr = e.target.closest("tr.departure-row");
    if (!tr) return;
    const service = tr.dataset.service;
    if (service) openBusFromService(service);
  });

  // Dark mode toggle
  dom.darkModeBtn.addEventListener("click", toggleDarkMode);

  // Toggle showing buses on the map (hides markers + pauses the refresh).
  if (dom.toggleBusesBtn) {
    dom.toggleBusesBtn.addEventListener("click", () => {
      const willShow = !state.busesVisible;
      setBusesVisible(willShow);
      showToast(willShow ? "Showing buses." : "Buses hidden.");
    });
    updateBusesToggleBtn();
  }

  // Panel error message setter
  dom.panelRetryBtn.addEventListener("click", () => {
    if (state.selectedStop) fetchDepartures(state.selectedStop.atcoCode);
  });

  // View mode toggle (Live ↔ Improvements)
  dom.viewModeLive.addEventListener("click", () => setViewMode("live"));
  dom.viewModeImprovements.addEventListener("click", () => setViewMode("improvements"));

  // Browser back/forward — re-apply state from the URL hash.
  window.addEventListener("popstate", () => applyUrlState(parseUrlState()));

  // Improvements panel: Day/Night service mode toggle
  if (dom.serviceModeDay) {
    dom.serviceModeDay.addEventListener("click",   () => setServiceMode("day"));
  }
  if (dom.serviceModeNight) {
    dom.serviceModeNight.addEventListener("click", () => setServiceMode("night"));
  }

  // Improvements panel: "Show limited services" toggle. Default off — hide
  // routes that don't run all week or end before 18:00.
  if (dom.showLimitedServices) {
    dom.showLimitedServices.checked = state.showLimitedServices;
    dom.showLimitedServices.addEventListener("change", (e) => {
      state.showLimitedServices = !!e.target.checked;
      showRouteLines();
      renderRouteFilterChips();
      renderProposalsList();
    });
  }

  // Mobile-only "hide panel" button — collapses the side panel down to
  // just the tab strip so the map gets the full mobile viewport. There's
  // one in each panel-tabs-bar (live + improvements modes).
  document.querySelectorAll(".btn-collapse-panel").forEach(btn => {
    btn.addEventListener("click", togglePanelCollapsed);
  });

  // Improvements panel: tab switching + close
  dom.tabAbout.addEventListener("click",     () => setImprovementsTab("about"));
  dom.tabProposals.addEventListener("click", () => setImprovementsTab("proposals"));
  dom.closePanelBtnImprovements.addEventListener("click", closePanel);

  // Route filter bulk actions
  dom.routesAllBtn.addEventListener("click",  () => setAllRoutesVisible(true));
  dom.routesNoneBtn.addEventListener("click", () => setAllRoutesVisible(false));

  // Proposal editor: "+ New proposal"
  if (dom.newProposalBtn) {
    dom.newProposalBtn.addEventListener("click", () => openEditor());
  }

  // Flush pending draft autosaves on tab hide / page unload so a reload
  // or mobile background suspend right after an edit doesn't lose it.
  // pagehide covers bfcache + full unload; visibilitychange catches tab
  // switches and mobile suspends where pagehide may not fire.
  window.addEventListener("pagehide", flushEditorAutosave);
  window.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") flushEditorAutosave();
  });
}

// ── Category & operator filter strips ────────────────────────
//
// Two multi-select chip strips driven by the same render path. Each
// strip's `set` accessor returns the current `state.visibleX` Set;
// clicking a chip flips that key in the Set and re-syncs.

const FILTER_STRIPS = [
  {
    container: () => dom.serviceCategoryToggle,
    set:       () => state.visibleCategories,
    options: [
      { key: "focused", label: "Adur & Worthing" },
      { key: "express", label: "Express"         },
      { key: "other",   label: "Other"           },
    ],
  },
  {
    container: () => dom.serviceOperatorToggle,
    set:       () => state.visibleOperators,
    options: [
      { key: "BHBC",  label: "B&H"        },
      { key: "SCSO",  label: "Stagecoach" },
      { key: "COMT",  label: "Compass"    },
      { key: "OTHER", label: "Other"      },
    ],
  },
];

function syncFilterStrips() {
  for (const strip of FILTER_STRIPS) renderFilterStrip(strip);
}

function renderFilterStrip(strip) {
  const container  = strip.container();
  const visibleSet = strip.set();
  if (!container) return;
  container.innerHTML = strip.options.map(opt => {
    const on = visibleSet ? visibleSet.has(opt.key) : true;
    return `
      <button type="button"
              class="filter-chip ${on ? "active" : ""}"
              data-filter-key="${escapeAttr(opt.key)}"
              aria-pressed="${on ? "true" : "false"}">
        ${escapeHtml(opt.label)}
      </button>`;
  }).join("");
  container.querySelectorAll(".filter-chip").forEach(btn => {
    btn.addEventListener("click", () => {
      if (!visibleSet) return;
      const key = btn.dataset.filterKey;
      if (visibleSet.has(key)) visibleSet.delete(key);
      else                     visibleSet.add(key);
      syncFilterStrips();
      showRouteLines();
      renderRouteFilterChips();
    });
  });
}

/** Switch between Day and Night service period in Improvements mode.
 *  Reconciles route-line layers, re-renders chips and the proposals list,
 *  and clears any selection that no longer belongs in the new mode. */
function setServiceMode(mode) {
  if (mode !== "day" && mode !== "night") return;
  if (state.serviceMode === mode) return;
  state.serviceMode = mode;

  const day = (mode === "day");
  if (dom.serviceModeDay) {
    dom.serviceModeDay.classList.toggle("active", day);
    dom.serviceModeDay.setAttribute("aria-selected", day ? "true" : "false");
  }
  if (dom.serviceModeNight) {
    dom.serviceModeNight.classList.toggle("active", !day);
    dom.serviceModeNight.setAttribute("aria-selected", !day ? "true" : "false");
  }
  // Used by CSS to keep destination tags fully opaque in night mode —
  // the proposals-tab-active dim is meant to push the day network back
  // when looking at proposals, but in night mode the visible network
  // IS the night service, so the tags shouldn't fade.
  document.body.classList.toggle("service-mode-night", !day);

  // If a proposal from the other mode was selected, drop the selection
  // before its layer gets hidden — otherwise hideAllProposals would
  // immediately re-show it.
  if (state.selectedProposalId) {
    const sp = (state.proposals || []).find(x => x.id === state.selectedProposalId);
    if (sp && isProposalNight(sp) !== !day) {
      state.selectedProposalId = null;
    }
  }

  showRouteLines();           // reconciles in/out of new mode
  renderRouteFilterChips();
  renderProposalsList();

  // Refresh proposal overlay to match new mode (also handles selection)
  if (state.showProposals) showAllProposals();
  else                     hideAllProposals();

  applyStopVisibility();
  pushUrlState();
}

/** Switch between the About and Proposals tabs in Improvements mode. */
function setImprovementsTab(tab) {
  const aboutActive = (tab === "about");
  dom.tabAbout.classList.toggle("active", aboutActive);
  dom.tabAbout.setAttribute("aria-selected", aboutActive ? "true" : "false");
  dom.tabProposals.classList.toggle("active", !aboutActive);
  dom.tabProposals.setAttribute("aria-selected", !aboutActive ? "true" : "false");
  dom.tabContentAbout.classList.toggle("hidden", !aboutActive);
  dom.tabContentProposals.classList.toggle("hidden", aboutActive);
  document.body.classList.toggle("proposals-tab-active", tab === "proposals");
}

// ============================================================
// VIEW MODE (Live ↔ Improvements)
// ============================================================

/**
 * Switch between the live tracker and the Improvements (network + proposals)
 * view. In Improvements mode the live vehicle refresh pauses, vehicle
 * markers are hidden, route-line polylines are drawn, and the side panel
 * swaps from Stop/Bus tabs to About/Proposals tabs. Stop markers stay
 * visible but don't react to clicks.
 */
function setViewMode(mode) {
  if (mode !== "live" && mode !== "improvements") return;
  if (state.viewMode === mode) return;
  state.viewMode = mode;

  // Toggle button visual + aria state
  const live = (mode === "live");
  dom.viewModeLive.classList.toggle("active", live);
  dom.viewModeLive.setAttribute("aria-selected", live ? "true" : "false");
  dom.viewModeImprovements.classList.toggle("active", !live);
  dom.viewModeImprovements.setAttribute("aria-selected", !live ? "true" : "false");

  // Body class for CSS-level swaps
  document.body.classList.toggle("improvements-mode", !live);

  // Side-effects wired in later tasks (vehicle refresh, polylines, panel).
  applyViewMode();

  // View mode is a major shift — users expect Back to return to live.
  pushUrlState({ major: true });
}

async function applyViewMode() {
  // The "show buses" toggle only does anything in Live view, so hide it in
  // Improvements to avoid a control that visibly does nothing.
  if (dom.toggleBusesBtn) dom.toggleBusesBtn.hidden = (state.viewMode === "improvements");

  if (state.viewMode === "improvements") {
    // Pause the live refresh — Improvements mode is a static network view.
    if (state.isRefreshing) stopVehicleRefresh();
    hideVehicleMarkers();
    state.map.closePopup();
    closePanel();
    ensureMapOverlayControls();
    try {
      await Promise.all([loadRouteLines(), loadProposals()]);
      showRouteLines();
      if (state.showProposals) showAllProposals();
      if (state.selectedProposalId) showProposal(state.selectedProposalId);
    } catch (err) {
      console.warn("Improvements data fetch failed:", err);
      showToast("Could not load Improvements data. Try again later.");
    }
  } else {
    // If the editor is open, tear it down — it only makes sense in Improvements mode.
    if (state.editor) closeEditor({ skipSave: false });
    hideRouteLines();
    hideAllProposalLayers();
    // Respect the header "show buses" toggle when returning to Live.
    if (state.busesVisible) {
      showVehicleMarkers();
      if (!state.isRefreshing) startVehicleRefresh();
    }
  }
  applyStopVisibility();
}

// ============================================================
// ROUTE LINES (Improvements view)
// ============================================================

/**
 * Fetch /api/route-lines once and pre-build Leaflet polylines for every
 * route. Subsequent mode toggles just add/remove the cached layers.
 */
// Memoized: a background prefetch and a tab-open click can both call this,
// so cache the in-flight promise. Without it, two concurrent runs would
// each build a fresh layer set and the second would orphan the first's
// on-map layers (hideRouteLines could no longer find them).
function loadRouteLines() {
  if (!state._routeLinesPromise) {
    // Reset on failure so a transient prefetch error doesn't permanently
    // break the Improvements tab — a later tab-open can retry.
    state._routeLinesPromise = loadRouteLinesImpl().catch(err => {
      state._routeLinesPromise = null;
      throw err;
    });
  }
  return state._routeLinesPromise;
}

async function loadRouteLinesImpl() {
  if (state.routeLines) return;
  const data = await apiFetch("/api/route-lines");
  if (!data || !Array.isArray(data.routes)) {
    state.routeLines = [];
    return;
  }
  state.routeLines = data.routes;
  state.visibleRoutes = new Set();

  for (const r of data.routes) {
    state.visibleRoutes.add(r.service);
    state.routeCategoryByService[r.service]  = r.category || "other";
    state.routeOperatorByService[r.service]  = r.operator || "";
    state.routeFrequencyByService[r.service] = r.frequency || null;

    const colour = getLineColour(r.service, r.operator);
    const fg = (pickTextOn(colour) === "dark") ? "#1a1a1a" : "#ffffff";
    // Endpoint tags only make sense for routes that visibly run off
    // the edge (express) or whose night-time destination is the point
    // — the backend force-emits names for night routes so the disparity
    // between Brighton-side and Worthing-side night coverage reads on
    // the map even when a route terminates inside the bbox.
    const showEndpointTags = r.category === "express" || isNightService(r.service);
    const layers = [];

    r.polylines.forEach((coords, i) => {
      layers.push(L.polyline(coords, {
        color:        colour,
        weight:       4,
        opacity:      0.85,
        smoothFactor: 1.5,
        interactive:  false,
        className:    "route-network-line",
      }));

      if (!showEndpointTags || coords.length < 2) return;
      const ep = (r.endpoints || [])[i] || {};
      // Only render the `to` end. Each direction's `from` is the OTHER
      // direction's `to` at the same physical terminus — drawing both
      // produces overlapping/duplicate pills (e.g. N5 Hangleton). One
      // tag per polyline pair gives one tag per terminus.
      if (ep.to_name) {
        const last = coords[coords.length - 1];
        if (!inCentralBrighton(last)) {
          const prev = coords[coords.length - 2];
          // Place the pill in the direction the line is heading so the
          // polyline reads as flowing into the tag rather than crossing it.
          const placement = (last[1] >= prev[1]) ? "right" : "left";
          const place = prettyDestination(r.service, ep.to_name, ep.to_headsign);
          layers.push(makeEndpointTag(last, r.service, place, "to", placement, colour, fg));
        }
      }
    });

    state.routeLineLayers[r.service] = layers;
  }

  state.visibleCategories = new Set(Object.values(state.routeCategoryByService));
  state.visibleOperators  = new Set(Object.values(state.routeOperatorByService));

  syncFilterStrips();
  renderRouteFilterChips();
}

/**
 * Render a clickable chip for each route GROUP into the About tab,
 * filtered to the current Day/Night service mode. Variants of the same
 * route (1/1A/1X) collapse to one chip that toggles them all together.
 * The chip's background is the base route's livery colour; sorted
 * natural-numeric so "5" comes before "10" comes before "106".
 */
function renderRouteFilterChips() {
  if (!state.routeLines || !dom.routeFilterChips) return;
  const isNight = state.serviceMode === "night";

  // Group services by base name, filtered to the current mode + the
  // active category and operator filters. Variants of one base must all
  // share a category/operator (they always do in practice), so it's
  // enough to test the first variant.
  const groups = new Map(); // base -> [variants]
  for (const r of state.routeLines) {
    const svc = r.service;
    if (isNightService(svc) !== isNight) continue;
    if (!isServicePassingFilters(svc)) continue;
    const base = routeBaseName(svc);
    if (!groups.has(base)) groups.set(base, []);
    groups.get(base).push(svc);
  }

  if (groups.size === 0) {
    dom.routeFilterChips.innerHTML = isNight
      ? `<p class="route-filters-empty">No matching night services.</p>`
      : `<p class="route-filters-empty">No matching routes — try toggling a category or operator back on.</p>`;
    return;
  }

  const bases = [...groups.keys()].sort(compareServiceNames);

  dom.routeFilterChips.innerHTML = bases.map(base => {
    const variants = groups.get(base).slice().sort(compareServiceNames);
    const label    = variants.join("/");
    const operator = state.routeOperatorByService[variants[0]] || "";
    const bg       = getLineColour(base, operator);
    const fg       = pickTextOn(bg) === "dark" ? "#1a1a1a" : "#ffffff";
    const allOn    = variants.every(v => state.visibleRoutes.has(v));
    return `
      <button type="button"
              class="route-chip"
              data-variants="${escapeAttr(variants.join(","))}"
              aria-pressed="${allOn ? "true" : "false"}"
              style="--chip-bg:${bg};--chip-fg:${fg}">
        ${escapeHtml(label)}
      </button>`;
  }).join("");

  dom.routeFilterChips.querySelectorAll(".route-chip").forEach(btn => {
    btn.addEventListener("click", () => {
      const variants   = (btn.dataset.variants || "").split(",").filter(Boolean);
      const nowVisible = btn.getAttribute("aria-pressed") !== "true";
      btn.setAttribute("aria-pressed", nowVisible ? "true" : "false");
      for (const svc of variants) setRouteVisible(svc, nowVisible);
    });
  });
}

/** Show or hide every route in the current service mode. */
function setAllRoutesVisible(visible) {
  if (!state.routeLines) return;
  const isNight = state.serviceMode === "night";
  for (const r of state.routeLines) {
    if (isNightService(r.service) !== isNight) continue;
    setRouteVisible(r.service, visible);
  }
  if (dom.routeFilterChips) {
    dom.routeFilterChips.querySelectorAll(".route-chip").forEach(btn => {
      btn.setAttribute("aria-pressed", visible ? "true" : "false");
    });
  }
}

/** True for services whose short_name is N + digits ("N1", "N700"). */
function isNightService(svc) {
  return /^N\d/i.test(String(svc || ""));
}

/** Tags at Brighton city-centre termini (Old Steine / Churchill Sq /
 *  Royal Pavilion cluster) would pile up confusingly. Marina and
 *  Kemptown sit east of this box and keep their tags. */
const CENTRAL_BRIGHTON = {minLat: 50.815, maxLat: 50.830, minLon: -0.158, maxLon: -0.130};
function inCentralBrighton(latlon) {
  if (!latlon) return false;
  const [lat, lon] = latlon;
  return lat >= CENTRAL_BRIGHTON.minLat && lat <= CENTRAL_BRIGHTON.maxLat
      && lon >= CENTRAL_BRIGHTON.minLon && lon <= CENTRAL_BRIGHTON.maxLon;
}

/** Final-stop names are mixed-quality ("College Close"); GTFS headsigns
 *  carry the true destination for off-map routes ("Lewes") but reduce
 *  to a stop name for city circulars. So: hand-mapped override first
 *  (for city night routes whose stop name doesn't name the area), then
 *  sanitised headsign (for outbound routes like N29), then sanitised
 *  stop name (last-resort fallback). */
const NIGHT_DESTINATION_OVERRIDES = {
  "N1":   { "College Close": "Mile Oak",
            "Cowley Drive Shops": "Whitehawk" },
  "N5":   { "Hardwick Road": "Hangleton",
            "Grenadier Hotel": "Hangleton",
            "Hollingbury Asda": "Hollingbury" },
  "N7":   { "Marina": "Marina",
            "Marina Cinema": "Marina" },
  "N25":  { "Sussex House": "Coldean",
            "Coldean Lane": "Coldean" },
  "N700": { "Wallace Avenue": "Worthing",
            "Durrington Tesco": "Worthing",
            "West Worthing Wallace Avenue": "Worthing" },
};
function stripStopCruft(s) {
  return (s || "")
    .replace(/\s*\(stop [A-Z0-9]+\)\s*$/i, "")
    .replace(/^\s*Stop\s+[A-Z0-9]+\s*[—-]\s*/i, "")
    .trim();
}
function prettyDestination(svc, stopName, headsign) {
  // Check both stopName and headsign against the override map — for
  // some routes the headsign carries the area cue (N1 "Cowley Drive
  // Shops"), for others the last stop name does (N1 "College Close").
  const overrides = NIGHT_DESTINATION_OVERRIDES[svc] || {};
  if (overrides[stopName]) return overrides[stopName];
  if (overrides[headsign]) return overrides[headsign];
  if (headsign) return stripStopCruft(headsign);
  return stripStopCruft(stopName);
}

/** True iff the service passes the active category + operator + frequency
 *  filters (independent of the day/night mode and the per-service
 *  visibility toggle). Used both when rendering chips and when reconciling
 *  layers. */
function isServicePassingFilters(svc) {
  if (state.visibleCategories) {
    const cat = state.routeCategoryByService[svc] || "other";
    if (!state.visibleCategories.has(cat)) return false;
  }
  if (state.visibleOperators) {
    const op = state.routeOperatorByService[svc] || "";
    if (!state.visibleOperators.has(op)) return false;
  }
  if (!state.showLimitedServices) {
    const freq = state.routeFrequencyByService[svc];
    // Night routes are inherently "not all day" by definition (they
    // exist for the post-23:00 window), so the "frequent all-day" gate
    // doesn't apply to them — they show whenever Night mode is active.
    if (!isNightService(svc) && !(freq && freq.is_frequent_all_day)) return false;
  }
  return true;
}

/** Strip a trailing letter variant from a numeric route to find its base.
 *  "1" → "1", "1A" → "1", "1X" → "1", "19A" → "19", "N1" → "N1",
 *  "N700" → "N700". Fully alpha codes (OXF, LGW) keep their full name. */
function routeBaseName(svc) {
  const m = String(svc || "").match(/^([A-Za-z]*\d+)/);
  return m ? m[1] : String(svc || "");
}

/** Natural sort: optional letter prefix + numeric segment + suffix.
 *  "1" < "1X" < "5" < "10"; "N1" < "N5" < "N21" < "N700"; falls back
 *  to lex for purely-alpha codes ("OXF", "LGW"). */
function compareServiceNames(a, b) {
  const ra = /^([A-Za-z]*)(\d+)(.*)$/.exec(String(a));
  const rb = /^([A-Za-z]*)(\d+)(.*)$/.exec(String(b));
  if (ra && rb) {
    const pa = ra[1].toUpperCase(), pb = rb[1].toUpperCase();
    if (pa !== pb) return pa.localeCompare(pb);
    const na = parseInt(ra[2], 10), nb = parseInt(rb[2], 10);
    if (na !== nb) return na - nb;
    return ra[3].localeCompare(rb[3]);
  }
  if (ra) return -1;
  if (rb) return 1;
  return String(a).localeCompare(String(b));
}

/** Build a tiny pill marker placed at the truncated end of a route line,
 *  so the user sees "1X → Brighton" or "Bognor ← N700" where the line
 *  runs off the edge of the focused area. The marker is non-interactive
 *  and inherits the route's livery colour. */
function makeEndpointTag([lat, lon], service, place, side, placement, bg, fg) {
  // `side` drives the arrow text (→ for "to", ← for "from").
  // `placement` is "left" | "right" — which side of the geographical
  // anchor the pill sits on; chosen by the caller based on the
  // polyline's outward heading at this end.
  const text = (side === "to")
    ? `${service} → ${place}`
    : `${place} ← ${service}`;
  const W = 220, H = 22, GAP = 4;
  const anchor = (placement === "right")
    ? [-GAP, H / 2]      // pill to the right of the geographical point
    : [W + GAP, H / 2];  // pill to the left
  return L.marker([lat, lon], {
    icon: L.divIcon({
      className: `route-endpoint-tag route-endpoint-tag--${placement}`,
      html: `<span class="route-endpoint-pill" `
          + `style="background:${bg};color:${fg}">${escapeHtml(text)}</span>`,
      iconSize:   [W, H],
      iconAnchor: anchor,
    }),
    interactive: false,
    keyboard:    false,
    zIndexOffset: 600,
  });
}

/** Reconcile every route layer against the current visible set + service
 *  mode. Adds layers that should be on the map, removes any that
 *  shouldn't be. Used both when entering Improvements and when flipping
 *  Day↔Night. */
function showRouteLines() {
  if (!state.visibleRoutes) return;
  const isNight = state.serviceMode === "night";
  for (const [service, layers] of Object.entries(state.routeLineLayers)) {
    const matchesMode = isNightService(service) === isNight;
    const shouldShow  = matchesMode
                        && state.visibleRoutes.has(service)
                        && isServicePassingFilters(service);
    for (const layer of layers) {
      if (shouldShow && !state.map.hasLayer(layer))      layer.addTo(state.map);
      else if (!shouldShow && state.map.hasLayer(layer)) state.map.removeLayer(layer);
    }
  }
}

function hideRouteLines() {
  for (const layers of Object.values(state.routeLineLayers)) {
    for (const layer of layers) {
      if (state.map.hasLayer(layer)) state.map.removeLayer(layer);
    }
  }
}

/**
 * Toggle a single route's polylines on/off. Used by the route filter chips.
 * Only adds the layer to the map if the route also matches the current
 * Day/Night service mode.
 */
function setRouteVisible(service, visible) {
  if (!state.visibleRoutes) return;
  if (visible) state.visibleRoutes.add(service);
  else         state.visibleRoutes.delete(service);

  const matchesMode = isNightService(service) === (state.serviceMode === "night");
  const showOnMap   = visible && matchesMode && isServicePassingFilters(service);

  const layers = state.routeLineLayers[service] || [];
  for (const layer of layers) {
    if (showOnMap && !state.map.hasLayer(layer))      layer.addTo(state.map);
    else if (!showOnMap && state.map.hasLayer(layer)) state.map.removeLayer(layer);
  }
}

// ============================================================
// PROPOSALS (Improvements view)
// ============================================================

/**
 * Load data/proposals.json (hand-authored — see file for schema) and
 * pre-build the polyline + endpoint-marker layers for each proposal.
 * Idempotent: subsequent calls are no-ops.
 */
// Memoized for the same reason as loadRouteLines (prefetch + tab-open race).
function loadProposals() {
  if (!state._proposalsPromise) {
    state._proposalsPromise = loadProposalsImpl().catch(err => {
      state._proposalsPromise = null;
      throw err;
    });
  }
  return state._proposalsPromise;
}

async function loadProposalsImpl() {
  if (state.proposals) return;
  let data;
  try {
    const res = await fetch("data/proposals.json");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    data = await res.json();
  } catch (err) {
    console.warn("Proposals load failed:", err);
    state.proposals = [];
    renderProposalsList();
    return;
  }
  state.proposals = Array.isArray(data.proposals) ? data.proposals : [];
  // Resolve a deep-linked proposal id that arrived before this data did.
  // Defer to the end of the function so layers exist when selectProposal runs.

  if (!state.map.getPane("proposalPane")) {
    const pane = state.map.createPane("proposalPane");
    pane.style.zIndex = 405; // above overlayPane (400)
  }

  for (const p of state.proposals) {
    const colour = p.color || "#444";
    const layers = [];

    // Main route line — dashed to distinguish from existing services
    if (Array.isArray(p.polyline) && p.polyline.length >= 2) {
      layers.push(L.polyline(p.polyline, {
        color:       colour,
        weight:      5,
        opacity:     0.92,
        dashArray:   "8 6",
        smoothFactor: 1.2,
        interactive: false,
        pane:        "proposalPane",
      }));
    }

    // Endpoint / stop dots — small open circles at each proposed stop
    if (Array.isArray(p.stops)) {
      for (const s of p.stops) {
        if (typeof s.lat !== "number" || typeof s.lon !== "number") continue;
        layers.push(L.circleMarker([s.lat, s.lon], {
          radius:      5,
          color:       colour,
          weight:      2,
          fillColor:   "#fff",
          fillOpacity: 1,
          interactive: false,
          pane:        "proposalPane",
        }));
      }
    }

    state.proposalLayers[p.id] = layers;
  }

  renderProposalsList();
  resolvePendingProposalId();
}

/** True when a proposal is flagged as a night service. */
function isProposalNight(p) {
  return !!(p && p.is_night);
}

function renderProposalsList() {
  if (!dom.proposalsList) return;
  const isNight = state.serviceMode === "night";
  const proposals = (state.proposals || []).filter(p => {
    if (isProposalNight(p) !== isNight) return false;
    // Hide proposals tagged as limited-service unless the Show-limited
    // toggle is on; night proposals are exempt (Night mode already implies
    // a limited operating window).
    if (!state.showLimitedServices && !isNight && p.frequency_class === "limited") {
      return false;
    }
    return true;
  });
  if (proposals.length === 0) {
    dom.proposalsList.innerHTML = isNight
      ? `<p class="proposals-empty">No night-service proposals yet.</p>`
      : `<p class="proposals-empty">No proposals yet. Add ideas to <code>data/proposals.json</code>.</p>`;
    return;
  }
  dom.proposalsList.innerHTML = proposals.map(p => {
    const sel = (p.id === state.selectedProposalId);
    const summaryText = (p.from && p.to)
      ? `${escapeHtml(p.from)} › ${escapeHtml(p.to)}`
      : escapeHtml(p.summary || "");
    const hasLinks = Array.isArray(p.links) && p.links.length > 0;
    const detail = sel ? `
      <div class="proposal-detail">
        ${p.description ? `
          <p class="proposal-detail-heading">About this proposal</p>
          <p class="proposal-detail-body">${escapeHtml(p.description)}</p>
        ` : ""}
        ${hasLinks ? `
          <p class="proposal-detail-heading">Links</p>
          <ul class="proposal-links">
            ${p.links.map(l => `
              <li><a class="proposal-link"
                     href="${escapeAttr(l.url)}"
                     target="_blank"
                     rel="noopener noreferrer">${escapeHtml(l.label || l.url)}</a></li>
            `).join("")}
          </ul>
        ` : ""}
      </div>` : "";
    return `
      <button type="button"
              class="proposal-card ${sel ? "selected" : ""}"
              data-proposal-id="${escapeAttr(p.id)}"
              style="border-left-color:${escapeAttr(p.color || "#444")}">
        <span class="proposal-card-name">${escapeHtml(p.name || p.id)}</span>
        <span class="proposal-card-summary">${summaryText}</span>
        ${detail}
      </button>`;
  }).join("");

  dom.proposalsList.querySelectorAll(".proposal-card").forEach(card => {
    card.addEventListener("click", () => {
      const id = card.dataset.proposalId;
      selectProposal(id === state.selectedProposalId ? null : id);
    });
  });
}

/**
 * Highlight one proposal: ensure its layer is visible, dim the others
 * if Show-all is off, scroll-into-view in the panel, and re-render the
 * list so the description block expands inline.
 */
function selectProposal(id) {
  state.selectedProposalId = id;

  if (id) {
    showProposal(id);
    // Pan / zoom to fit the selected proposal's polyline
    const p = (state.proposals || []).find(x => x.id === id);
    if (p && Array.isArray(p.polyline) && p.polyline.length) {
      state.map.fitBounds(L.latLngBounds(p.polyline), {
        padding: [40, 40], maxZoom: 14,
      });
    }
  } else if (!state.showProposals) {
    hideAllProposals();
  }

  renderProposalsList();
  applyStopVisibility();   // proposal stops may need to be unhidden in night mode
  pushUrlState({ major: true });
}

function showProposal(id) {
  const layers = state.proposalLayers[id] || [];
  for (const layer of layers) {
    if (!state.map.hasLayer(layer)) layer.addTo(state.map);
  }
}

function hideProposal(id) {
  const layers = state.proposalLayers[id] || [];
  for (const layer of layers) {
    if (state.map.hasLayer(layer)) state.map.removeLayer(layer);
  }
}

function showAllProposals() {
  // Only show proposals matching the current Day/Night service mode;
  // hide any from the other mode that may already be on the map.
  const isNight = state.serviceMode === "night";
  for (const p of state.proposals || []) {
    if (isProposalNight(p) === isNight) showProposal(p.id);
    else                                hideProposal(p.id);
  }
}

/** Remove every proposal layer from the map, ignoring selection. Used when
 *  leaving Improvements for Live — nothing proposal-related should linger.
 *  (hideAllProposals deliberately keeps the selected one for the in-tab
 *  "Show proposals" toggle, which is wrong when switching views.) */
function hideAllProposalLayers() {
  for (const id of Object.keys(state.proposalLayers)) hideProposal(id);
}

function hideAllProposals() {
  for (const id of Object.keys(state.proposalLayers)) hideProposal(id);
  // Restore the selected proposal if there is one and it matches mode
  // (selection trumps the toggle).
  if (state.selectedProposalId) {
    const sp = (state.proposals || []).find(x => x.id === state.selectedProposalId);
    if (sp && isProposalNight(sp) === (state.serviceMode === "night")) {
      showProposal(state.selectedProposalId);
    }
  }
}

function setShowProposals(on) {
  state.showProposals = !!on;
  if (state.showProposals) showAllProposals();
  else                     hideAllProposals();
  if (dom.mapOverlayControls) {
    const btn = dom.mapOverlayControls.querySelector("[data-overlay='proposals']");
    if (btn) btn.setAttribute("aria-pressed", state.showProposals ? "true" : "false");
  }
}

/**
 * Render the "Show proposals" map-overlay button. Idempotent — only
 * builds the DOM once.
 */
function ensureMapOverlayControls() {
  if (!dom.mapOverlayControls) return;
  if (dom.mapOverlayControls.dataset.built === "1") return;
  dom.mapOverlayControls.innerHTML = `
    <button type="button"
            class="map-overlay-btn"
            data-overlay="proposals"
            aria-pressed="${state.showProposals ? "true" : "false"}">
      <svg class="icon" aria-hidden="true" style="width:14px;height:14px"><use href="#i-lightbulb"/></svg>
      <span>Show proposals</span>
    </button>`;
  dom.mapOverlayControls.dataset.built = "1";
  dom.mapOverlayControls.querySelector("[data-overlay='proposals']")
    .addEventListener("click", () => setShowProposals(!state.showProposals));
}

// ============================================================
// VEHICLE MARKER VISIBILITY (toggled by view mode)
// ============================================================

function hideVehicleMarkers() {
  for (const marker of Object.values(state.busMarkers)) {
    if (state.map.hasLayer(marker)) state.map.removeLayer(marker);
  }
}

function showVehicleMarkers() {
  for (const marker of Object.values(state.busMarkers)) {
    if (!state.map.hasLayer(marker)) marker.addTo(state.map);
  }
}

// ============================================================
// PROPOSAL EDITOR
// ============================================================
//
// In-browser editor for sketching a new route proposal. Drafts live in
// localStorage under "proposalDrafts". Finished drafts can be copied to
// clipboard, downloaded, or sent to GitHub as a pre-filled issue so the
// proposal can be PR'd into data/proposals.json.
//
// Data model (in-memory + persisted):
//   {
//     draftId, name, summary, description, color,
//     points: [ { type: "stop"|"waypoint", lat, lon, name?, atco? } ],
//     updatedAt: ISO string
//   }

const EDITOR_STORAGE_KEY = "proposalDrafts";
const EDITOR_AUTOSAVE_MS = 400;
const EDITOR_REPO = "dennislemennace/adur-worthing-bus";

function newDraftId() {
  return "d_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 7);
}

function emptyDraft() {
  return {
    draftId: newDraftId(),
    name: "",
    summary: "",
    description: "",
    color: "#1e88e5",
    points: [],
    updatedAt: new Date().toISOString(),
  };
}

function loadDraftsFromStorage() {
  try {
    const raw = localStorage.getItem(EDITOR_STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(d => d && typeof d === "object" && Array.isArray(d.points));
  } catch {
    return [];
  }
}

function persistDrafts() {
  try {
    localStorage.setItem(EDITOR_STORAGE_KEY, JSON.stringify(state.editorDrafts));
  } catch (err) {
    console.warn("Could not persist proposal drafts:", err);
  }
}

/**
 * Commit the currently-open draft into state.editorDrafts + localStorage
 * immediately. Used by scheduleAutosave's timer and by flushEditorAutosave
 * (page unload, tab hide, closeEditor) to avoid losing the last edits.
 */
function commitEditorDraft() {
  if (!state.editor) return;
  state.editor.updatedAt = new Date().toISOString();
  const i = state.editorDrafts.findIndex(d => d.draftId === state.editor.draftId);
  const snapshot = JSON.parse(JSON.stringify(state.editor));
  if (i === -1) state.editorDrafts.push(snapshot);
  else          state.editorDrafts[i] = snapshot;
  persistDrafts();
}

/** Debounced save of the currently-open draft into state.editorDrafts + localStorage. */
function scheduleAutosave() {
  if (!state.editor) return;
  clearTimeout(state.editorAutosaveTimer);
  state.editorAutosaveTimer = setTimeout(() => {
    state.editorAutosaveTimer = null;
    if (!state.editor) return;
    commitEditorDraft();
    renderDraftsSection();
  }, EDITOR_AUTOSAVE_MS);
}

/**
 * If a debounced save is pending, flush it now. Called on page unload,
 * tab hide, and before closeEditor — so a reload / mobile-suspend right
 * after an edit doesn't lose the last keystroke.
 */
function flushEditorAutosave() {
  if (!state.editorAutosaveTimer) return;
  clearTimeout(state.editorAutosaveTimer);
  state.editorAutosaveTimer = null;
  if (state.editor) commitEditorDraft();
}

function deleteDraft(draftId) {
  state.editorDrafts = state.editorDrafts.filter(d => d.draftId !== draftId);
  persistDrafts();
  renderDraftsSection();
}

/** Render the "Your drafts" subsection (above the published proposals list). */
function renderDraftsSection() {
  if (!dom.draftsSection || !dom.draftsList) return;
  const drafts = state.editorDrafts.slice().sort(
    (a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt))
  );
  if (drafts.length === 0) {
    dom.draftsSection.classList.add("hidden");
    dom.draftsList.innerHTML = "";
    return;
  }
  dom.draftsSection.classList.remove("hidden");
  dom.draftsList.innerHTML = drafts.map(d => {
    const name = d.name || "(unnamed draft)";
    const stopsCount = d.points.filter(p => p.type === "stop").length;
    const ptsCount   = d.points.length;
    return `
      <div class="draft-card" role="button" tabindex="0" data-draft-id="${escapeAttr(d.draftId)}"
           style="border-left-color:${escapeAttr(d.color || "#444")}">
        <div class="draft-card-main">
          <span class="draft-card-name">${escapeHtml(name)}</span>
          <span class="draft-card-meta">${stopsCount} stop${stopsCount === 1 ? "" : "s"} · ${ptsCount} point${ptsCount === 1 ? "" : "s"}</span>
        </div>
        <button class="draft-card-delete" data-delete-id="${escapeAttr(d.draftId)}"
                aria-label="Delete draft ${escapeAttr(name)}">
          <svg class="icon" aria-hidden="true"><use href="#i-trash"/></svg>
        </button>
      </div>`;
  }).join("");

  dom.draftsList.querySelectorAll(".draft-card").forEach(card => {
    card.addEventListener("click", (e) => {
      if (e.target.closest(".draft-card-delete")) return;
      const id = card.dataset.draftId;
      const draft = state.editorDrafts.find(d => d.draftId === id);
      if (draft) openEditor(draft);
    });
  });
  dom.draftsList.querySelectorAll(".draft-card-delete").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const id = btn.dataset.deleteId;
      if (!id) return;
      if (confirm("Delete this draft? This cannot be undone.")) deleteDraft(id);
    });
  });
}

/** Open the editor with an existing draft, or a fresh empty one. */
function openEditor(draft) {
  // Deep-copy so edits to state.editor don't mutate the cached list entry
  // until scheduleAutosave() snapshots.
  state.editor = draft
    ? JSON.parse(JSON.stringify(draft))
    : emptyDraft();
  // Start in addStop so a fresh draft is immediately stop-clickable.
  state.editorMode = "addStop";

  if (!state.editorLayers) {
    state.editorLayers = L.featureGroup().addTo(state.map);
  }

  document.body.classList.add("editor-mode");
  applyEditorModeBodyClass();

  // Swap the Proposals tab from list view → editor view
  if (dom.proposalsView)   dom.proposalsView.classList.add("hidden");
  if (dom.proposalEditor)  dom.proposalEditor.classList.remove("hidden");

  // Ensure we're on the Proposals tab
  setImprovementsTab("proposals");

  renderEditor();
  redrawEditorLayers();
  fitEditorLayers();
}

function closeEditor(opts = {}) {
  const { skipSave = false } = opts;

  // Always cancel a pending debounced save; then decide whether to persist.
  clearTimeout(state.editorAutosaveTimer);
  state.editorAutosaveTimer = null;

  if (state.editor && !skipSave) {
    const hasContent = state.editor.name || state.editor.points.length > 0;
    if (hasContent) {
      commitEditorDraft();
    } else {
      // Empty draft — don't litter storage
      const i = state.editorDrafts.findIndex(d => d.draftId === state.editor.draftId);
      if (i !== -1) {
        state.editorDrafts.splice(i, 1);
        persistDrafts();
      }
    }
  }

  state.editor = null;
  state.editorMode = "addStop";

  if (state.editorLayers) {
    state.editorLayers.clearLayers();
  }

  document.body.classList.remove("editor-mode");
  applyEditorModeBodyClass();

  if (dom.proposalsView)  dom.proposalsView.classList.remove("hidden");
  if (dom.proposalEditor) dom.proposalEditor.classList.add("hidden");

  renderDraftsSection();
}

function setEditorMode(mode) {
  if (!state.editor) return;
  state.editorMode = (mode === "addStop") ? "addStop" : "move";
  applyEditorModeBodyClass();
  // Re-render just the mode-button active state + re-wire dragging
  syncEditorModeButtons();
  redrawEditorLayers();
}

function applyEditorModeBodyClass() {
  document.body.classList.toggle("editor-mode-add-stop",
    state.editorMode === "addStop" && !!state.editor);
}

function syncEditorModeButtons() {
  const container = dom.proposalEditor;
  if (!container) return;
  container.querySelectorAll(".editor-mode-btn").forEach(btn => {
    const m = btn.dataset.mode;
    btn.classList.toggle("active", m === state.editorMode);
  });
  const hint = container.querySelector(".editor-mode-hint");
  if (hint) hint.textContent = modeHint(state.editorMode);
}

function modeHint(mode) {
  switch (mode) {
    case "addStop": return "Click any bus stop on the map to add it to the route.";
    default:        return "Drag any dot to move it. Click the route line to add a waypoint between stops. Shift-click to remove.";
  }
}

/** Render the whole editor form into #proposal-editor. */
function renderEditor() {
  if (!state.editor || !dom.proposalEditor) return;
  const d = state.editor;

  const canExport = d.points.filter(p => p.type === "stop").length >= 2;

  dom.proposalEditor.innerHTML = `
    <div class="editor-header">
      <button class="editor-back-btn" id="ed-back-btn" type="button" aria-label="Back to proposals">
        <svg class="icon" aria-hidden="true"><use href="#i-arrow-left"/></svg>
        <span>Back</span>
      </button>
      <span class="editor-header-title">${escapeHtml(d.name || "New proposal")}</span>
      <button class="editor-delete-btn" id="ed-delete-btn" type="button" aria-label="Delete this draft">
        <svg class="icon" aria-hidden="true"><use href="#i-trash"/></svg>
        <span>Delete</span>
      </button>
    </div>

    <div class="editor-scroll">
      <div class="editor-field">
        <label for="ed-name">Name</label>
        <input id="ed-name" type="text" maxlength="80" placeholder="e.g. Coastal Sprinter X1"
               value="${escapeAttr(d.name)}">
      </div>

      <div class="editor-field">
        <label for="ed-summary">Summary</label>
        <input id="ed-summary" type="text" maxlength="160"
               placeholder="One-line pitch shown in the proposals list"
               value="${escapeAttr(d.summary)}">
      </div>

      <div class="editor-field">
        <label for="ed-description">Description</label>
        <textarea id="ed-description" maxlength="1000"
                  placeholder="What does this service do, and why is it needed?">${escapeHtml(d.description)}</textarea>
      </div>

      <div class="editor-field">
        <label>Colour</label>
        <div class="editor-color-row">
          <input id="ed-color" type="color" value="${escapeAttr(d.color || "#1e88e5")}">
          <span class="editor-color-row-caption">Line colour on the map</span>
        </div>
      </div>

      <div class="editor-field">
        <span class="editor-modes-label">Route builder</span>
        <div class="editor-mode-buttons" role="radiogroup" aria-label="Route edit mode">
          <button class="editor-mode-btn" data-mode="addStop" type="button" role="radio">
            <svg class="icon" aria-hidden="true"><use href="#i-pin"/></svg>
            <span>Add stop</span>
          </button>
          <button class="editor-mode-btn" data-mode="move" type="button" role="radio">
            <svg class="icon" aria-hidden="true"><use href="#i-move"/></svg>
            <span>Move / delete</span>
          </button>
        </div>
        <p class="editor-mode-hint">${escapeHtml(modeHint(state.editorMode))}</p>
      </div>

      <div class="editor-field">
        <div class="editor-points-label">
          <span>Points</span>
          <span class="editor-points-count" id="ed-points-count"></span>
        </div>
        <div class="editor-point-list" id="ed-point-list"></div>
      </div>
    </div>

    <div class="editor-actions">
      <button class="editor-action-btn" id="ed-copy-btn" type="button" ${canExport ? "" : "disabled"}>
        <svg class="icon" aria-hidden="true"><use href="#i-copy"/></svg>
        <span>Copy JSON</span>
      </button>
      <button class="editor-action-btn" id="ed-download-btn" type="button" ${canExport ? "" : "disabled"}>
        <svg class="icon" aria-hidden="true"><use href="#i-download"/></svg>
        <span>Download</span>
      </button>
      <button class="editor-action-btn primary" id="ed-github-btn" type="button" ${canExport ? "" : "disabled"}>
        <svg class="icon" aria-hidden="true"><use href="#i-github"/></svg>
        <span>Contribute</span>
      </button>
      <span class="editor-status" id="ed-status"></span>
    </div>
  `;

  // Header actions
  dom.proposalEditor.querySelector("#ed-back-btn")
    .addEventListener("click", () => closeEditor());
  dom.proposalEditor.querySelector("#ed-delete-btn")
    .addEventListener("click", () => {
      if (!confirm("Delete this draft? This cannot be undone.")) return;
      const id = state.editor.draftId;
      // Skip the save-on-close — we want the draft gone.
      closeEditor({ skipSave: true });
      deleteDraft(id);
    });

  // Field listeners (live update + autosave)
  const nameInput = dom.proposalEditor.querySelector("#ed-name");
  nameInput.addEventListener("input", (e) => {
    state.editor.name = e.target.value;
    const titleEl = dom.proposalEditor.querySelector(".editor-header-title");
    if (titleEl) titleEl.textContent = state.editor.name || "New proposal";
    scheduleAutosave();
  });
  dom.proposalEditor.querySelector("#ed-summary").addEventListener("input", (e) => {
    state.editor.summary = e.target.value;
    scheduleAutosave();
  });
  dom.proposalEditor.querySelector("#ed-description").addEventListener("input", (e) => {
    state.editor.description = e.target.value;
    scheduleAutosave();
  });
  dom.proposalEditor.querySelector("#ed-color").addEventListener("input", (e) => {
    state.editor.color = e.target.value;
    redrawEditorLayers();
    scheduleAutosave();
  });

  // Mode buttons
  dom.proposalEditor.querySelectorAll(".editor-mode-btn").forEach(btn => {
    btn.addEventListener("click", () => setEditorMode(btn.dataset.mode));
  });

  // Export actions
  dom.proposalEditor.querySelector("#ed-copy-btn").addEventListener("click", copyDraftJson);
  dom.proposalEditor.querySelector("#ed-download-btn").addEventListener("click", downloadDraftJson);
  dom.proposalEditor.querySelector("#ed-github-btn").addEventListener("click", openGitHubIssue);

  syncEditorModeButtons();
  renderPointList();
}

function renderPointList() {
  if (!state.editor || !dom.proposalEditor) return;
  const listEl = dom.proposalEditor.querySelector("#ed-point-list");
  const countEl = dom.proposalEditor.querySelector("#ed-points-count");
  if (!listEl || !countEl) return;

  // Only show stops in the sidebar; waypoints live on the map only.
  const stops = state.editor.points
    .map((p, i) => ({ ...p, _fullIdx: i }))
    .filter(p => p.type === "stop");

  countEl.textContent = `${stops.length} stop${stops.length === 1 ? "" : "s"}`;

  if (stops.length === 0) {
    listEl.innerHTML = `<p class="editor-point-list-empty">No stops yet. Click any bus stop on the map to add it to the route.</p>`;
    return;
  }

  const flashIdx = state.editor._flashIndex;
  listEl.innerHTML = stops.map((p, stopNum) => {
    const label = p.name || p.atco || "Unnamed stop";
    const cls   = p._fullIdx === flashIdx ? "editor-point-row just-added" : "editor-point-row";
    return `
      <div class="${cls}" data-type="stop" data-stop-index="${stopNum}">
        <span class="editor-drag-handle" aria-hidden="true"><svg class="icon"><use href="#i-grip"/></svg></span>
        <span class="editor-point-row-index">${stopNum + 1}</span>
        <svg class="icon" aria-hidden="true"><use href="#i-pin"/></svg>
        <span class="editor-point-row-label">${escapeHtml(label)}</span>
        <button class="editor-point-row-remove" data-remove-index="${p._fullIdx}" aria-label="Remove stop ${stopNum + 1}">×</button>
      </div>`;
  }).join("");
  state.editor._flashIndex = -1;

  listEl.querySelectorAll(".editor-point-row-remove").forEach(btn => {
    btn.addEventListener("click", () => {
      const idx = parseInt(btn.dataset.removeIndex, 10);
      if (!Number.isNaN(idx)) removePoint(idx);
    });
  });

  // Wire drag handles for stop reordering
  listEl.querySelectorAll(".editor-drag-handle").forEach((handle, stopNum) => {
    handle.addEventListener("pointerdown", (e) => startStopDrag(e, stopNum));
  });

  // Update export button enabled state
  const canExport = stops.length >= 2;
  ["#ed-copy-btn", "#ed-download-btn", "#ed-github-btn"].forEach(sel => {
    const btn = dom.proposalEditor.querySelector(sel);
    if (btn) btn.disabled = !canExport;
  });
}

// State for the pointer-events drag-to-reorder interaction.
const _editorDragState = { active: false, ghost: null, srcStopIdx: null, overEl: null };

function startStopDrag(e, stopIdx) {
  e.preventDefault();
  const handle = e.currentTarget;
  const row = handle.closest(".editor-point-row");
  if (!row) return;

  const rowRect = row.getBoundingClientRect();
  const ghost = row.cloneNode(true);
  ghost.className = "editor-drag-ghost";
  ghost.style.width = rowRect.width + "px";
  ghost.style.left = (e.clientX - rowRect.width / 2) + "px";
  ghost.style.top = (e.clientY - 16) + "px";
  document.body.appendChild(ghost);

  _editorDragState.active = true;
  _editorDragState.srcStopIdx = stopIdx;
  _editorDragState.ghost = ghost;
  _editorDragState.overEl = null;

  handle.setPointerCapture(e.pointerId);

  function onMove(ev) {
    ghost.style.left = (ev.clientX - ghost.offsetWidth / 2) + "px";
    ghost.style.top = (ev.clientY - 16) + "px";

    const el = document.elementFromPoint(ev.clientX, ev.clientY);
    const hoverRow = el && el.closest(".editor-point-row[data-stop-index]");

    if (_editorDragState.overEl && _editorDragState.overEl !== hoverRow) {
      _editorDragState.overEl.classList.remove("drag-over-above", "drag-over-below");
      _editorDragState.overEl = null;
    }
    if (hoverRow && hoverRow !== row) {
      const hoverIdx = parseInt(hoverRow.dataset.stopIndex, 10);
      hoverRow.classList.toggle("drag-over-above", hoverIdx < stopIdx);
      hoverRow.classList.toggle("drag-over-below", hoverIdx > stopIdx);
      _editorDragState.overEl = hoverRow;
    }
  }

  function onUp() {
    _editorDragState.active = false;
    ghost.remove();
    handle.removeEventListener("pointermove", onMove);
    handle.removeEventListener("pointerup", onUp);
    handle.removeEventListener("pointercancel", onUp);

    const target = _editorDragState.overEl;
    if (target) {
      target.classList.remove("drag-over-above", "drag-over-below");
      const toIdx = parseInt(target.dataset.stopIndex, 10);
      _editorDragState.overEl = null;
      if (!isNaN(toIdx) && toIdx !== stopIdx) {
        commitStopReorder(stopIdx, toIdx);
        return;
      }
    }
    _editorDragState.overEl = null;
  }

  handle.addEventListener("pointermove", onMove);
  handle.addEventListener("pointerup", onUp);
  handle.addEventListener("pointercancel", onUp);
}

function commitStopReorder(from, to) {
  if (!state.editor) return;
  const stops = state.editor.points.filter(p => p.type === "stop");
  const [moved] = stops.splice(from, 1);
  stops.splice(to, 0, moved);
  state.editor.points = stops; // waypoints cleared — now invalid after reorder
  redrawEditorLayers();
  renderPointList();
  scheduleAutosave();
}

function addStopToDraft(stop) {
  if (!state.editor) return;
  state.editor.points.push({
    type: "stop",
    lat: stop.lat,
    lon: stop.lon,
    name: stop.name || "",
    atco: stop.atco || "",
  });
  state.editor._flashIndex = state.editor.points.length - 1;
  redrawEditorLayers();
  renderPointList();
  scheduleAutosave();
}

/** Brief scale pulse on a stop marker — confirmation that an editor click
 *  registered. Marker is looked up by atco_code from state.stopMarkers; the
 *  CSS animation is single-shot (no infinite loop), so we just toggle the
 *  class and let it auto-clear. */
function pulseStopMarker(atcoCode) {
  const m = state.stopMarkers[atcoCode];
  if (!m) return;
  const el = m.getElement();
  if (!el) return;
  el.classList.remove("just-added");
  // Force a reflow so re-adding the class restarts the animation.
  void el.offsetWidth;
  el.classList.add("just-added");
  setTimeout(() => el.classList.remove("just-added"), 400);
}

function removePoint(index) {
  if (!state.editor) return;
  if (index < 0 || index >= state.editor.points.length) return;
  state.editor.points.splice(index, 1);
  redrawEditorLayers();
  renderPointList();
  scheduleAutosave();
}

function movePoint(index, lat, lon) {
  if (!state.editor) return;
  const p = state.editor.points[index];
  if (!p) return;
  p.lat = lat;
  p.lon = lon;
  // Leave the on-screen marker position to Leaflet's drag; just update the polyline.
  const line = state.editorLayers && state.editorLayers._editorPolyline;
  if (line) line.setLatLngs(state.editor.points.map(q => [q.lat, q.lon]));
  scheduleAutosave();
}

/**
 * Tear down and rebuild the draft's layers from scratch. Simpler than
 * diffing and fast enough at the tens-of-points scale we expect.
 */
function redrawEditorLayers() {
  if (!state.editorLayers) return;
  state.editorLayers.clearLayers();
  state.editorLayers._editorPolyline = null;
  if (!state.editor) return;

  const pts = state.editor.points;
  const latlngs = pts.map(p => [p.lat, p.lon]);
  const colour = state.editor.color || "#1e88e5";

  const inMoveMode = state.editorMode === "move";

  if (latlngs.length >= 2) {
    const line = L.polyline(latlngs, {
      color: colour,
      weight: 5,
      opacity: 0.95,
      dashArray: "8 6",
      smoothFactor: 1.2,
      interactive: inMoveMode,
    });
    if (inMoveMode) {
      // Click the line to insert a waypoint at the nearest segment midpoint.
      line.on("click", (e) => {
        L.DomEvent.stopPropagation(e);
        const clickedLL = e.latlng;
        let bestSeg = 0, bestDist = Infinity;
        for (let i = 0; i < pts.length - 1; i++) {
          const midLat = (pts[i].lat + pts[i + 1].lat) / 2;
          const midLon = (pts[i].lon + pts[i + 1].lon) / 2;
          const d = state.map.distance(clickedLL, L.latLng(midLat, midLon));
          if (d < bestDist) { bestDist = d; bestSeg = i; }
        }
        state.editor.points.splice(bestSeg + 1, 0, {
          type: "waypoint", lat: clickedLL.lat, lon: clickedLL.lng,
        });
        redrawEditorLayers();
        scheduleAutosave();
      });
    }
    line.addTo(state.editorLayers);
    state.editorLayers._editorPolyline = line;
  }

  // Markers for every point. Draggable in "move" mode; stops and waypoints distinguished visually.
  pts.forEach((p, i) => {
    if (inMoveMode) {
      const isWaypoint = p.type === "waypoint";
      const size = isWaypoint ? 10 : 14;
      const anchor = isWaypoint ? 5 : 7;
      const iconHtml = isWaypoint
        ? `<span style="display:block;width:10px;height:10px;border-radius:2px;transform:rotate(45deg);border:2px solid ${colour};background:#fff;box-shadow:0 0 0 2px rgba(255,255,255,0.6);"></span>`
        : `<span style="display:block;width:14px;height:14px;border-radius:50%;border:2px solid ${colour};background:#fff;box-shadow:0 0 0 2px rgba(255,255,255,0.7);"></span>`;
      const drag = L.marker([p.lat, p.lon], {
        draggable: true,
        icon: L.divIcon({
          className: isWaypoint ? "editor-draggable-waypoint" : "editor-draggable-point",
          html: iconHtml,
          iconSize: [size, size],
          iconAnchor: [anchor, anchor],
        }),
      });
      drag.on("drag", (e) => {
        const ll = e.target.getLatLng();
        movePoint(i, ll.lat, ll.lng);
      });
      drag.on("dragend", () => {
        redrawEditorLayers();
      });
      drag.on("click", (e) => {
        if (e.originalEvent && e.originalEvent.shiftKey) {
          removePoint(i);
        }
      });
      drag.addTo(state.editorLayers);
    } else {
      // In addStop mode: stops are larger circles, waypoints smaller.
      const isWaypoint = p.type === "waypoint";
      L.circleMarker([p.lat, p.lon], {
        radius: isWaypoint ? 4 : 7,
        color: colour,
        weight: 2,
        fillColor: "#fff",
        fillOpacity: 1,
        interactive: false,
      }).addTo(state.editorLayers);
    }
  });
}

function fitEditorLayers() {
  if (!state.editor || !state.editorLayers) return;
  const pts = state.editor.points;
  if (pts.length < 2) return;
  try {
    const bounds = L.latLngBounds(pts.map(p => [p.lat, p.lon]));
    state.map.fitBounds(bounds, { padding: [40, 40], maxZoom: 14 });
  } catch { /* ignore */ }
}

// ── Export actions ──────────────────────────────────────────

/** In-memory draft → the export schema used by data/proposals.json. */
function draftToProposalJson(draft) {
  const id = slugify(draft.name || draft.draftId);
  const stops = draft.points
    .filter(p => p.type === "stop")
    .map(p => ({
      atco_code: p.atco || "",
      name:      p.name || "",
      lat:       round5(p.lat),
      lon:       round5(p.lon),
    }));
  // Polyline uses all points in order (stops + waypoints) to support road-aligned routes.
  const polyline = draft.points.map(p => [round5(p.lat), round5(p.lon)]);
  return {
    id,
    name: draft.name || "",
    summary: draft.summary || "",
    color: draft.color || "#1e88e5",
    frequency_class: "frequent_all_day",
    polyline,
    stops,
    description: draft.description || "",
  };
}

function slugify(s) {
  return String(s || "proposal")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48) || "proposal";
}

function round5(n) {
  return Math.round(n * 1e5) / 1e5;
}

function setEditorStatus(msg) {
  const el = dom.proposalEditor && dom.proposalEditor.querySelector("#ed-status");
  if (!el) return;
  el.textContent = msg;
  clearTimeout(setEditorStatus._t);
  setEditorStatus._t = setTimeout(() => { el.textContent = ""; }, 2500);
}

async function copyDraftJson() {
  if (!state.editor) return;
  const json = JSON.stringify(draftToProposalJson(state.editor), null, 2);
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(json);
      setEditorStatus("Copied!");
      return;
    }
  } catch (err) {
    console.warn("Clipboard write failed, falling back:", err);
  }
  // Fallback: textarea select + execCommand
  const ta = document.createElement("textarea");
  ta.value = json;
  ta.style.position = "fixed";
  ta.style.opacity  = "0";
  document.body.appendChild(ta);
  ta.select();
  try { document.execCommand("copy"); setEditorStatus("Copied!"); }
  catch { setEditorStatus("Copy failed — select and copy manually."); }
  document.body.removeChild(ta);
}

function downloadDraftJson() {
  if (!state.editor) return;
  const obj = draftToProposalJson(state.editor);
  const json = JSON.stringify(obj, null, 2);
  const blob = new Blob([json], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `proposal-${obj.id || "draft"}.json`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
  setEditorStatus("Downloaded.");
}

function openGitHubIssue() {
  if (!state.editor) return;
  const obj = draftToProposalJson(state.editor);
  const title = `Proposal: ${obj.name || obj.id}`;
  const fullBody =
    `Submitted from the in-app proposal editor. ` +
    `Please paste the JSON below into \`data/proposals.json\` and open a PR.\n\n` +
    "```json\n" +
    JSON.stringify(obj, null, 2) +
    "\n```\n";
  const fullUrl = `https://github.com/${EDITOR_REPO}/issues/new` +
    `?title=${encodeURIComponent(title)}` +
    `&body=${encodeURIComponent(fullBody)}`;

  // GitHub silently truncates pre-filled issue URLs around 8 KB. For large
  // proposals, copy the JSON to the clipboard and open a stub body asking
  // the author to paste it in.
  if (fullUrl.length <= 7000) {
    window.open(fullUrl, "_blank", "noopener");
    setEditorStatus("Opening GitHub…");
    return;
  }

  copyDraftJson(); // fire-and-forget — clipboard on most browsers
  const stubBody =
    `Submitted from the in-app proposal editor. The proposal JSON was too ` +
    `large to pre-fill here — it's on your clipboard. **Please paste the ` +
    `JSON into a fenced \`\`\`json block below**, then either paste the ` +
    `same JSON into \`data/proposals.json\` and open a PR, or leave it in ` +
    `this issue for someone else to pick up.\n\n` +
    "```json\n(paste here)\n```\n";
  const stubUrl = `https://github.com/${EDITOR_REPO}/issues/new` +
    `?title=${encodeURIComponent(title)}` +
    `&body=${encodeURIComponent(stubBody)}`;
  window.open(stubUrl, "_blank", "noopener");
  setEditorStatus("JSON copied; paste it into the issue.");
}

// ============================================================
// API HELPER
// ============================================================
async function apiFetch(path) {
  if (!CONFIG.API_BASE_URL || CONFIG.API_BASE_URL.includes("YOUR-BACKEND-URL")) {
    throw new Error("API_BASE_URL is not configured. Please edit app.js and set it to your deployed backend URL.");
  }

  const url = CONFIG.API_BASE_URL.replace(/\/$/, "") + path;
  const response = await fetch(url, {
    headers: { "Accept": "application/json" },
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(`API error ${response.status}: ${text.slice(0, 120)}`);
  }

  return response.json();
}

// ============================================================
// TOAST NOTIFICATION
// ============================================================
let _toastTimer = null;

function showToast(message, durationMs = 3500) {
  dom.toast.textContent = message;
  dom.toast.classList.remove("hidden");

  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => dom.toast.classList.add("hidden"), durationMs);
}
// ============================================================
// OPERATOR COLOURS
// Keyed by National Operator Code (operator_ref from BODS).
// Add more entries here as you discover operator codes in your area.
// To find a code: check the Render logs — operator_ref is logged
// with each vehicle fetch, or visit /api/vehicles and look at
// the operator_ref field in the JSON.
// ============================================================
const OPERATOR_COLOURS = {
  // Stagecoach South
  "SCSC": "#0000FF",   // Stagecoach orange
  "SCSO": "#0000FF",   // Stagecoach orange

  // Brighton & Hove Buses (Go-Ahead)
  "BHBC": "#e30613",   // Bright red

  // Arriva
  "ARBB": "#00a0df",   // Arriva cyan/blue
  "ARHE": "#00a0df",

  // National Express / Coaches
  "NATX": "#ffcc00",   // National Express yellow
  "TNXB": "#ffcc00",

  // Metrobus (Go-Ahead)
  "METR": "#007a4c",   // Metrobus green

  // Southern Vectis / Go Southern
  "SVCT": "#007a4c",

  "COMT": "#800020",   // Compass Bus burgundy

  // Default fallback — used for any operator not listed above
  "DEFAULT": "#f4a020",
};

// ============================================================
// OPERATOR ICONS
// Map National Operator Code → PNG path under icons/.
// Source icons should face EAST (right) at 0 degrees so that the
// `bearing - 90` rotation in createBusIcon points them correctly.
// Operators not listed here fall back to the coloured-box marker.
// ============================================================
const OPERATOR_ICONS = {
  // Stagecoach South — share one icon across the region's NOCs
  "SCSO": "icons/SCSO.png",
  "SCSC": "icons/SCSO.png",

  // Brighton & Hove Buses
  "BHBC": "icons/BHBC.png",

  // Compass Travel
  "CMPA": "icons/CMPA.png",
  "COMT": "icons/CMPA.png",

  // National Express
  "NATX": "icons/NTXP.png",
  "NTXP": "icons/NTXP.png",
  "TNXB": "icons/TNXB.png",
};

const OPERATOR_BORDER_COLOURS = {
  "SCSC": "#0000FF",
  "SCSO": "#0000FF",
  "BHBC": "#a00010",
  "ARBB": "#007aaf",
  "ARHE": "#007aaf",
  "NATX": "#c8a000",
  "TNXB": "#c8a000",
  "METR": "#005a38",
  "SVCT": "#005a38",
  "COMT": "#580016",
  "DEFAULT": "#c07800",
};

function getOperatorColour(operatorRef) {
  return OPERATOR_COLOURS[operatorRef] || OPERATOR_COLOURS["DEFAULT"];
}

// ============================================================
// ROUTE LIVERY COLOURS
// Only verified, branded liveries. Unknown routes fall through
// to the operator colour in OPERATOR_COLOURS, which already gives
// visual variety across operators. Add a route here only when you
// have evidence the operator paints that route in a distinct livery
// (e.g. marketing page, fleet photography).
// ============================================================
const ROUTE_COLOURS = {
  // Stagecoach South — Coastliner 700 / 700X (Coastliner branded blue)
  "700":  "#005EB8",
  "700X": "#005EB8",
  "N700": "#005EB8",

  // Brighton & Hove Buses — per the official route-colour guide.
  // Note: routes 37, 37B, 52, 47 intentionally omitted.

  // 1 / 1X / N1 — pink
  "1":   "#E5007E",
  "1X":  "#E5007E",
  "N1":  "#E5007E",

  // 2 — dark green
  "2":   "#006838",

  // 3X — teal-green
  "3X":  "#007F5C",

  // 5 / 5A / 5B / N5 — orange
  "5":   "#F39200",
  "5A":  "#F39200",
  "5B":  "#F39200",
  "N5":  "#F39200",

  // 6 — plum
  "6":   "#8E1B6B",

  // 7 / N7 — red
  "7":   "#D7282F",
  "N7":  "#D7282F",

  // Coaster family 11X / 12 / 12A / 12X / 13 / 13X / 14 / 14C — lime green
  "11X": "#7AB800",
  "12":  "#7AB800",
  "12A": "#7AB800",
  "12X": "#7AB800",
  "13":  "#7AB800",
  "13X": "#7AB800",
  "14":  "#7AB800",
  "14C": "#7AB800",

  // 17 — maroon
  "17":  "#8B1A32",

  // 18 — turquoise
  "18":  "#1FB5C4",

  // 21 — red
  "21":  "#D7282F",

  // 22 — teal
  "22":  "#008C8C",

  // 24 — red-orange
  "24":  "#E8491A",

  // 25 / 25X / N25 — lime green
  "25":  "#8CC540",
  "25X": "#8CC540",
  "N25": "#8CC540",

  // 26 — purple
  "26":  "#6E2A8C",

  // 27 — green
  "27":  "#00A651",

  // 28 / 29 / 29X — purple
  "28":  "#4E2A84",
  "29":  "#4E2A84",
  "29X": "#4E2A84",

  // 46 — red
  "46":  "#C8102E",

  // 48 — blue
  "48":  "#0072CE",

  // 49 — blue
  "49":  "#1E5AA8",

  // 50 — blue
  "50":  "#004B87",

  // Breeze 77 / 78 / 79 — green
  "77":  "#00A651",
  "78":  "#00A651",
  "79":  "#00A651",

  // 270 — grey
  "270": "#6D6E71",
};

function getRouteColour(service, operatorRef) {
  if (!service) return getOperatorColour(operatorRef);
  const key = String(service).trim().toUpperCase();
  return ROUTE_COLOURS[key] || getOperatorColour(operatorRef);
}

/**
 * Colour for a route line/chip in the Improvements view, where the
 * backend doesn't (yet) supply operator info. Branded routes use their
 * livery; everything else falls back to a deterministic HSL hash so each
 * route gets a distinct hue rather than all sharing the operator default.
 */
function getLineColour(service, operator) {
  if (!service) return "#888";
  const key = String(service).trim().toUpperCase();
  if (ROUTE_COLOURS[key]) return ROUTE_COLOURS[key];
  // Fall back to the operator's brand colour before the hash. Lets every
  // Compass route render burgundy, Stagecoach blue, B&H red, etc., without
  // needing a per-route entry in ROUTE_COLOURS.
  if (operator && OPERATOR_COLOURS[operator]) return OPERATOR_COLOURS[operator];
  let h = 0;
  for (let i = 0; i < key.length; i++) {
    h = ((h << 5) - h + key.charCodeAt(i)) | 0;
  }
  const hue = ((h % 360) + 360) % 360;
  return `hsl(${hue}, 55%, 42%)`;
}

// Return 'light' or 'dark' text depending on background luminance (WCAG-ish).
function pickTextOn(bgHex) {
  const h = String(bgHex || "").replace("#", "");
  if (h.length !== 6) return "light";
  const r = parseInt(h.slice(0, 2), 16);
  const g = parseInt(h.slice(2, 4), 16);
  const b = parseInt(h.slice(4, 6), 16);
  const lum = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
  return lum > 0.62 ? "dark" : "light";
}

function getOperatorBorderColour(operatorRef) {
  return OPERATOR_BORDER_COLOURS[operatorRef] || OPERATOR_BORDER_COLOURS["DEFAULT"];
}

// ============================================================
// OPERATOR FULL NAMES
// Used by the Bus info tab to show a friendly operator name
// alongside the National Operator Code.
// ============================================================
const OPERATOR_NAMES = {
  "SCSO": "Stagecoach South",
  "SCSC": "Stagecoach South",
  "BHBC": "Brighton & Hove Buses",
  "CMPA": "Compass Travel",
  "COMT": "Compass Travel",
  "NATX": "National Express",
  "NTXP": "National Express",
  "TNXB": "National Express",
  "ARBB": "Arriva",
  "ARHE": "Arriva",
  "METR": "Metrobus",
  "SVCT": "Southern Vectis",
};

function getOperatorName(operatorRef) {
  return OPERATOR_NAMES[operatorRef] || operatorRef || "Unknown operator";
}

// ============================================================
// OPERATOR TICKET INFO
// Static ticket details per operator. Each entry can have:
//   app:     { name, url }    — mobile ticketing app
//   dayPass: string           — short description of day ticket
//   url:     string           — link to full fares/tickets page
// Future: replace or merge with live data from a tickets API.
// ============================================================
const OPERATOR_TICKETS = {
  "SCSO": {
    app:     { name: "Stagecoach Bus App", url: "https://www.stagecoachbus.com/app" },
    dayPass: "Stagecoach South dayrider from £5.50",
    url:     "https://www.stagecoachbus.com/tickets",
  },
  "SCSC": {
    app:     { name: "Stagecoach Bus App", url: "https://www.stagecoachbus.com/app" },
    dayPass: "Stagecoach South dayrider from £5.50",
    url:     "https://www.stagecoachbus.com/tickets",
  },
  "BHBC": {
    app:     { name: "B&H Buses App", url: "https://www.buses.co.uk/app" },
    dayPass: "NETWORK Saver day ticket available",
    url:     "https://www.buses.co.uk/tickets",
  },
  "CMPA": {
    app:     null,
    dayPass: "Day tickets available on bus",
    url:     "https://www.compass-travel.co.uk/fares.html",
  },
  "COMT": {
    app:     null,
    dayPass: "Day tickets available on bus",
    url:     "https://www.compass-travel.co.uk/fares.html",
  },
  "NATX": {
    app:     null,
    dayPass: "Coach tickets — book in advance online",
    url:     "https://www.nationalexpress.com/en/cheap-coach-tickets",
  },
  "NTXP": {
    app:     null,
    dayPass: "Coach tickets — book in advance online",
    url:     "https://www.nationalexpress.com/en/cheap-coach-tickets",
  },
};
// Strip a leading "N" from a service label when the rest is all digits,
// so "N700" and "700" can be treated as the same service. Used when
// matching scheduled departures to live vehicles, because some operators
// (Stagecoach SCSO) publish night variants without the N prefix.
function stripNightPrefix(svc) {
  if (!svc) return "";
  return /^N\d+$/i.test(svc) ? svc.slice(1) : svc;
}

function prettifyName(s) {
  if (!s) return "";
  return String(s)
    .replace(/_/g, " ")
    .replace(/\w\S*/g, w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase());
}

// ============================================================
// SECURITY HELPERS — prevent XSS in dynamically built HTML
// ============================================================
function escapeHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeAttr(str) {
  return String(str).replace(/'/g, "\\'").replace(/"/g, "&quot;");
}
