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
  notifyOnMove:            false,  // "Notify when this bus moves" checkbox state
  notifyBaseline:          null,   // {lat, lon, ref} captured when arming
  notifyOverThresholdCount: 0,     // consecutive frames over deadzone (requires 2 to fire)
  activeTab:               "stop", // "stop" | "bus"
  busInfoTickTimer:        null,   // setInterval handle for "X ago" text
  busDetails:              null,   // /api/vehicle response for selected bus
  busDetailsLoading:       false,  // true while waiting on /api/vehicle

  // ── Rail (Realtime Trains) ──
  railVisible:             true,   // header toggle: show stations + train dots
  railStations:            null,   // /api/rail-stations response, fetched lazily
  railStationMarkers:      {},     // crs → L.marker
  railStationByCrs:        {},     // crs → station object (lat/lon/name)
  selectedRailStation:     null,   // { crs, name } shown in the panel, or null
  railBoard:               null,   // last /api/rail-departures response for selectedRailStation
  railBoardLoading:        false,
  selectedRailServices:    {},     // uid → { date, calling, marker, lastPos, lastSeg }
  railRefreshTimer:        null,   // setInterval handle for service polling
  railFreezeNoticed:       false,  // shown once if the rail panel is opened with rail hidden

  // ── Improvements view (network/proposals mode) ──
  viewMode:                "live", // "live" | "improvements"
  improvementsTab:         "about",// "about" | "proposals" — official proposal lines only show on the Proposals tab
  serviceMode:             "day",  // "day" | "night" — splits chips, route lines, proposals
  visibleCategories:       null,   // Set holding the active type-filter key: "all" | "express" | "standard"
  visibleOperators:        null,   // Set of operator buckets ("BHBC","SCSO","COMT","OTHER",""); ""=unknown
  showLimitedServices:     false,  // false = hide services that don't run all week or finish before 18:00
  routeLines:              null,   // /api/route-lines response, fetched lazily
  routeLineLayers:         {},     // service short_name → array of L.polyline
  routeOperatorByService:  {},     // service → operator NOC (e.g. "COMT", "")
  routeFrequencyByService: {},     // service → { is_frequent_all_day, runs_days, ... }
  visibleRoutes:           null,   // Set of service short_names; null = all visible
  proposals:               null,   // data/proposals.json
  proposalLayers:          {},     // proposal id → array of L.polyline
  showProposals:           false,  // map overlay toggle in Improvements mode
  selectedProposalId:      null,

  // ── Ticket view (fare zones) ──
  ticketZones:             null,   // data/ticket_zones.json (array); null = not loaded
  ticketZoneLayers:        {},     // zone id → L.polygon (only for zones with geometry)
  ticketReachLayers:       {},     // zone id → [L.marker] reach pills (networkSAVER-style)
  selectedZoneId:          null,
  expandedOperators:       null,    // Set of operator codes whose ticket sub-cards are revealed
  _ticketZonesPromise:     null,

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
  toggleRailBtn:      document.getElementById("toggle-rail-btn"),
  railBoardHost:      document.getElementById("rail-board-host"),

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

  // ── Section nav (dropdown) ──
  sectionNavTrigger: document.getElementById("section-nav-trigger"),
  sectionNavLabel:   document.getElementById("section-nav-label"),
  sectionNavMenu:    document.getElementById("section-nav-menu"),

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
  ticketZonesList:        document.getElementById("ticket-zones-list"),
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
  else if (state.viewMode === "tickets") parts.push("view=t");
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

    const view = parsed.view === "i" ? "improvements"
               : parsed.view === "t" ? "tickets"
               : "live";
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
  // The pre-paint script in index.html already applied html.dark-mode from
  // localStorage (or the OS preference on first visit), so there's no flash.
  // Sync our state + the toggle button to whatever it decided.
  state.darkMode = document.documentElement.classList.contains("dark-mode");
  if (state.darkMode) {
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

  // Load rail stations + render in Live view. Failure here is non-fatal —
  // buses still work even if RTT is unconfigured / unreachable.
  if (state.viewMode === "live" && state.railVisible) {
    loadRailStations().then(() => showRailStations())
                      .catch(err => console.warn("Rail init failed:", err));
  }

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
  // In the proposal editor every stop is selectable, so don't hide any
  // even if the underlying view+service mode would normally filter.
  const editorOpen = !!state.editor;
  const filterToNight = !editorOpen
    && state.viewMode === "improvements" && state.serviceMode === "night";

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
  if (state.viewMode !== "live") return;   // buses only shown in Live
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

    // A request in flight when the user switched to a non-live view (Route or
    // Ticket) must not re-add bus markers after the switch cleared them.
    if (state.viewMode !== "live" || !state.busesVisible) return;

    updateVehicleMarkers(data.vehicles);
    resolvePendingBusRef();

    const now = new Date().toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    setStatusLabel({ text: `Updated ${now}`, loading: false });
  } catch (err) {
    console.warn("Vehicle refresh failed:", err);
    setStatusLabel({ text: "Update failed — retrying", loading: true, error: true });
  }
}

// "Notify me when this bus moves" — distance deadzone in metres. Typical AVL
// jitter is 10–30 m; 50 m is well above that and a real moving bus clears it
// in a single 20 s frame. We also require 2 consecutive over-threshold frames
// to kill single-frame GPS spikes.
const NOTIFY_MOVE_THRESHOLD_M = 50;
const NOTIFY_MOVE_FRAMES_REQ  = 2;

function checkNotifyOnMove(vehicle) {
  if (!state.notifyOnMove || !state.notifyBaseline) return;
  if (state.notifyBaseline.ref !== vehicle.vehicle_ref) return;
  if (vehicle.latitude == null || vehicle.longitude == null) return;
  const d = state.map.distance(
    [state.notifyBaseline.lat, state.notifyBaseline.lon],
    [vehicle.latitude,         vehicle.longitude]
  );
  if (d >= NOTIFY_MOVE_THRESHOLD_M) {
    state.notifyOverThresholdCount += 1;
    if (state.notifyOverThresholdCount >= NOTIFY_MOVE_FRAMES_REQ) {
      fireMoveNotification(vehicle, d);
      // Latch off: clear so it doesn't re-fire every frame as the bus rolls.
      state.notifyOnMove = false;
      state.notifyBaseline = null;
      state.notifyOverThresholdCount = 0;
      if (state.activeTab === "bus") renderBusTab();
    }
  } else {
    // Drop below threshold (e.g. jitter spike) — reset the counter so we
    // only fire when the bus has *sustained* movement away from the baseline.
    state.notifyOverThresholdCount = 0;
  }
}

function fireMoveNotification(vehicle, distanceM) {
  const svc = vehicle.service_ref || "Bus";
  const dest = prettifyName(
    vehicle.destination
    || state.busDetails?.vehicle?.trip_headsign
    || vehicle.trip_headsign
  ) || "";
  const title = `${svc} is on the move`;
  const body = dest
    ? `Service ${svc} to ${dest} has moved ~${Math.round(distanceM)} m.`
    : `Service ${svc} has moved ~${Math.round(distanceM)} m.`;
  try {
    if ("Notification" in window && Notification.permission === "granted") {
      new Notification(title, { body, tag: `bus-move-${vehicle.vehicle_ref}` });
    }
  } catch (e) {
    console.warn("Notification failed:", e);
  }
}

function setStatusLabel({ text, loading, error = false }) {
  const el = dom.lastUpdatedLabel;
  if (!el) return;
  el.classList.toggle("is-loading", !!loading);
  el.classList.toggle("is-error", !!error);
  const dots = loading ? '<span class="loading-dots" aria-hidden="true"></span>' : "";
  el.innerHTML = `<span class="status-pill-label">${escapeHtml(text)}</span>${dots}`;
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
      checkNotifyOnMove(vehicle);
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

  // Stops are inert in the non-live network views (Route view / Ticket view).
  if (state.viewMode !== "live") return;

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
  // Clear notify-on-move state when switching buses (latch + baseline are
  // per-vehicle; a stale baseline against a new bus would fire instantly).
  if (state.selectedVehicleRef !== vehicle.vehicle_ref) {
    state.notifyOnMove = false;
    state.notifyBaseline = null;
    state.notifyOverThresholdCount = 0;
  }
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
  const ticketHtml   = buildTicketInfoHtml(v.operator_ref, null, service);

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

    <label class="follow-bus-toggle">
      <input type="checkbox" id="notify-move-checkbox" ${state.notifyOnMove ? "checked" : ""}>
      <span>Notify me when this bus moves
        <small class="follow-bus-hint">(while this site is open)</small>
      </span>
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

  const nb = document.getElementById("notify-move-checkbox");
  if (nb) {
    nb.addEventListener("change", (e) => armNotifyOnMove(e.target.checked, nb));
  }
}

// Arm/disarm the "notify on move" latch. Requesting Notification permission
// must happen on a user gesture; if the user denies (or has previously
// blocked) we revert the checkbox so the UI doesn't pretend it's armed.
function armNotifyOnMove(wantOn, checkboxEl) {
  if (!wantOn) {
    state.notifyOnMove = false;
    state.notifyBaseline = null;
    state.notifyOverThresholdCount = 0;
    return;
  }
  if (!("Notification" in window)) {
    alert("Your browser doesn't support notifications.");
    if (checkboxEl) checkboxEl.checked = false;
    return;
  }
  const v = state.selectedVehicle;
  if (!v || v.latitude == null || v.longitude == null) {
    if (checkboxEl) checkboxEl.checked = false;
    return;
  }
  const captureBaseline = () => {
    state.notifyOnMove = true;
    state.notifyBaseline = {
      ref: state.selectedVehicleRef,
      lat: v.latitude,
      lon: v.longitude,
    };
    state.notifyOverThresholdCount = 0;
  };
  if (Notification.permission === "granted") {
    captureBaseline();
  } else if (Notification.permission === "denied") {
    alert("Notifications are blocked. Enable them in your browser settings for this site.");
    if (checkboxEl) checkboxEl.checked = false;
  } else {
    Notification.requestPermission().then((perm) => {
      if (perm === "granted") {
        captureBaseline();
      } else {
        if (checkboxEl) checkboxEl.checked = false;
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
function buildTicketInfoHtml(operatorRef, liveData = null, service = "") {
  // Future: merge liveData fields over the static entry when available.
  const info = OPERATOR_TICKETS[operatorRef] || null;
  const isN700 = /^N700$/i.test(String(service || "").trim());
  if (!info && !liveData && !isN700) return "";

  const rows = [];

  if (isN700) {
    rows.push(`
      <div class="ticket-row">
        <span class="ticket-label">N700 single</span>
        <span class="ticket-value">Anytime single £5, or £2 supplement on a Stagecoach Day/Night Rider.</span>
      </div>`);
  }

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
  if (dom.railBoardHost) dom.railBoardHost.classList.add("hidden");

  switch (stateKey) {
    case "loading": dom.panelLoading.classList.remove("hidden"); break;
    case "error":
      dom.panelError.classList.remove("hidden");
      if (errorMsg) dom.panelErrorMsg.textContent = errorMsg;
      break;
    case "results": dom.departuresContainer.classList.remove("hidden"); break;
    case "rail":
      if (dom.railBoardHost) dom.railBoardHost.classList.remove("hidden");
      break;
    default:        dom.panelPrompt.classList.remove("hidden");  break;
  }
}

function toggleDarkMode() {
  state.darkMode = !state.darkMode;
  document.documentElement.classList.toggle("dark-mode", state.darkMode);
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

  // Clear rail station selection (any panel-level Train rendering)
  state.selectedRailStation = null;
  state.railBoard           = null;
  state.railBoardLoading    = false;
  if (dom.railBoardHost) {
    dom.railBoardHost.innerHTML = "";
    dom.railBoardHost.classList.add("hidden");
  }

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

  // Toggle showing trains (rail stations + tracked train markers).
  if (dom.toggleRailBtn) {
    dom.toggleRailBtn.addEventListener("click", () => {
      const willShow = !state.railVisible;
      setRailVisible(willShow);
      showToast(willShow ? "Showing trains." : "Trains hidden.");
    });
    syncRailToggleUI();
  }

  // Panel error message setter
  dom.panelRetryBtn.addEventListener("click", () => {
    if (state.selectedStop) fetchDepartures(state.selectedStop.atcoCode);
  });

  // Section nav dropdown (Live / Improvements / future sections)
  initSectionNav();

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
    single:    true,   // mutually-exclusive: All / Express / Standard
    options: [
      { key: "all",      label: "All"      },
      { key: "express",  label: "Express"  },
      { key: "standard", label: "Standard" },
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
      if (strip.single) {
        // Radio behaviour: exactly one key selected, never empty.
        visibleSet.clear();
        visibleSet.add(key);
      } else if (visibleSet.has(key)) {
        visibleSet.delete(key);
      } else {
        visibleSet.add(key);
      }
      syncFilterStrips();
      showRouteLines();
      renderRouteFilterChips();
      // The type filter also gates proposals, so reconcile them too.
      reconcileProposalLayers();
      renderProposalsList();
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

  // Refresh proposal overlay to match new mode (respects active tab +
  // overlay toggle; officials only on the Proposals tab).
  reconcileProposalLayers();
  if (state.selectedProposalId) showProposal(state.selectedProposalId);

  applyStopVisibility();
  pushUrlState();
}

/** Switch between the About and Proposals tabs in Improvements mode. */
function setImprovementsTab(tab) {
  const aboutActive = (tab === "about");
  state.improvementsTab = aboutActive ? "about" : "proposals";
  dom.tabAbout.classList.toggle("active", aboutActive);
  dom.tabAbout.setAttribute("aria-selected", aboutActive ? "true" : "false");
  dom.tabProposals.classList.toggle("active", !aboutActive);
  dom.tabProposals.setAttribute("aria-selected", !aboutActive ? "true" : "false");
  dom.tabContentAbout.classList.toggle("hidden", !aboutActive);
  dom.tabContentProposals.classList.toggle("hidden", aboutActive);
  document.body.classList.toggle("proposals-tab-active", tab === "proposals");
  // Official proposal lines are tied to the Proposals tab — reconcile when
  // the tab changes (only meaningful once proposal layers exist).
  if (state.proposals) reconcileProposalLayers();
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
  if (mode !== "live" && mode !== "improvements" && mode !== "tickets") return;
  if (state.viewMode === mode) return;
  state.viewMode = mode;

  syncSectionNavToViewMode();

  // data-view drives CSS-level panel/map swaps across the three sections.
  document.body.dataset.view = mode;

  // Side-effects wired in later tasks (vehicle refresh, polylines, panel).
  applyViewMode();

  // View mode is a major shift — users expect Back to return to live.
  pushUrlState({ major: true });
}

/** Update the section-nav trigger label + listbox aria-selected to match
 *  the current view mode. Called from setViewMode and also at init so a
 *  deep-linked ?view=i lands on the right label. */
function syncSectionNavToViewMode() {
  if (!dom.sectionNavMenu || !dom.sectionNavLabel) return;
  const items = dom.sectionNavMenu.querySelectorAll('[role="option"]');
  for (const li of items) {
    const selected = li.dataset.mode === state.viewMode;
    li.setAttribute("aria-selected", selected ? "true" : "false");
    if (selected) dom.sectionNavLabel.textContent = li.textContent.trim();
  }
}

/** Wire the section-nav dropdown: trigger toggles open/close, options
 *  switch the view, Escape closes + returns focus, Arrow keys move within
 *  the menu, Enter/Space selects, outside-click closes. Standard listbox
 *  interactions so keyboard parity with the previous button-pair is kept. */
function initSectionNav() {
  if (!dom.sectionNavTrigger || !dom.sectionNavMenu) return;
  const trigger = dom.sectionNavTrigger;
  const menu    = dom.sectionNavMenu;
  const items   = () => [...menu.querySelectorAll('[role="option"]')];

  const isOpen = () => trigger.getAttribute("aria-expanded") === "true";
  const open = () => {
    trigger.setAttribute("aria-expanded", "true");
    menu.classList.remove("hidden");
    // Focus the currently-selected option (or first) for arrow-key flow.
    const all = items();
    const sel = all.find(li => li.getAttribute("aria-selected") === "true") || all[0];
    if (sel) sel.focus();
  };
  const close = (returnFocus = false) => {
    trigger.setAttribute("aria-expanded", "false");
    menu.classList.add("hidden");
    if (returnFocus) trigger.focus();
  };

  trigger.addEventListener("click", () => isOpen() ? close() : open());
  trigger.addEventListener("keydown", (e) => {
    if (e.key === "ArrowDown" || e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      if (!isOpen()) open(); else items()[0]?.focus();
    }
  });

  menu.addEventListener("click", (e) => {
    const li = e.target.closest('[role="option"]');
    if (!li) return;
    setViewMode(li.dataset.mode);
    close(true);
  });

  menu.addEventListener("keydown", (e) => {
    const all = items();
    const idx = all.indexOf(document.activeElement);
    if (e.key === "Escape")   { e.preventDefault(); close(true); }
    else if (e.key === "ArrowDown") { e.preventDefault(); all[(idx + 1) % all.length]?.focus(); }
    else if (e.key === "ArrowUp")   { e.preventDefault(); all[(idx - 1 + all.length) % all.length]?.focus(); }
    else if (e.key === "Home")      { e.preventDefault(); all[0]?.focus(); }
    else if (e.key === "End")       { e.preventDefault(); all[all.length - 1]?.focus(); }
    else if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      const li = document.activeElement;
      if (li && li.dataset.mode) { setViewMode(li.dataset.mode); close(true); }
    }
  });

  document.addEventListener("click", (e) => {
    if (!isOpen()) return;
    if (trigger.contains(e.target) || menu.contains(e.target)) return;
    close();
  });

  syncSectionNavToViewMode();
}

async function applyViewMode() {
  const live = state.viewMode === "live";
  // The "show buses" toggle only does anything in Live view.
  if (dom.toggleBusesBtn) dom.toggleBusesBtn.hidden = !live;
  if (dom.toggleRailBtn)  dom.toggleRailBtn.hidden  = !live;

  // Any non-live section is a static network view: pause the live refresh,
  // hide vehicles, and dismiss the live stop/bus panel + popups.
  if (!live) {
    if (state.isRefreshing) stopVehicleRefresh();
    hideVehicleMarkers();
    hideRailStations();
    clearAllSelectedRailServices();
    state.map.closePopup();
    closePanel();
  }

  if (state.viewMode === "improvements") {
    hideTicketZones();
    ensureMapOverlayControls();
    try {
      await Promise.all([loadRouteLines(), loadProposals()]);
      showRouteLines();
      reconcileProposalLayers();
      if (state.selectedProposalId) showProposal(state.selectedProposalId);
    } catch (err) {
      console.warn("Route view data fetch failed:", err);
      showToast("Could not load route data. Try again later.");
    }
  } else if (state.viewMode === "tickets") {
    // The editor only makes sense in Route view; tear it down.
    if (state.editor) closeEditor({ skipSave: false });
    hideRouteLines();
    hideAllProposalLayers();
    try {
      await loadTicketZones();
      showTicketZones();
    } catch (err) {
      console.warn("Ticket view data fetch failed:", err);
      showToast("Could not load ticket data. Try again later.");
    }
  } else {
    // Live: tear down all network-view layers and restore the live map.
    if (state.editor) closeEditor({ skipSave: false });
    hideRouteLines();
    hideAllProposalLayers();
    hideTicketZones();
    if (state.busesVisible) {
      showVehicleMarkers();
      if (!state.isRefreshing) startVehicleRefresh();
    }
    if (state.railVisible) {
      try {
        await loadRailStations();
        showRailStations();
      } catch (err) {
        console.warn("Rail station load failed:", err);
      }
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

  // Two-pass build: (1) polylines + collect endpoint-tag intents; (2) cluster
  // intents at shared termini and assign a stackIndex so co-located tags
  // (e.g. N12 + N14 at Marine Gate) render in a vertical column instead of
  // piling on top of each other.
  const tagIntents = []; // {service, anchor, place, placement, colour, fg}

  for (const r of data.routes) {
    state.visibleRoutes.add(r.service);
    state.routeOperatorByService[r.service]  = r.operator || "";
    state.routeFrequencyByService[r.service] = r.frequency || null;

    const colour = getLineColour(r.service, r.operator);
    const fg = (pickTextOn(colour) === "dark") ? "#1a1a1a" : "#ffffff";
    const showEndpointTags = isExpressService(r.service) || isNightService(r.service);
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
          const placement = (last[1] >= prev[1]) ? "right" : "left";
          const place = prettyDestination(r.service, ep.to_name, ep.to_headsign);
          tagIntents.push({
            service: r.service,
            anchor: last,
            place,
            placement,
            colour,
            fg,
            layers,
          });
        }
      }
    });

    state.routeLineLayers[r.service] = layers;
  }

  // Cluster co-located tag intents by real distance + placement. Bucket-keying
  // by rounded coords splits tags that fall on opposite sides of a rounding
  // boundary (the OSRM-built polyline ends near, not at, the GTFS stop, so
  // two routes terminating at the same stop can have tail points 10–80 m
  // apart). Greedy O(n²) with a 250 m threshold handles both exact-match and
  // near-by termini; well below the spacing between distinct termini in this
  // region (~600 m+), so no false merges. Same-placement only — tags on
  // opposite sides of a point don't overlap.
  const STACK_RADIUS_M = 250;
  const clusters = [];
  const nearby = (a, b) => {
    const dy = (a[0] - b[0]) * 110540;
    const dx = (a[1] - b[1]) * 111320 * Math.cos(a[0] * Math.PI / 180);
    return Math.hypot(dx, dy) <= STACK_RADIUS_M;
  };
  for (const t of tagIntents) {
    const cluster = clusters.find(c =>
      c[0].placement === t.placement && nearby(c[0].anchor, t.anchor));
    if (cluster) cluster.push(t);
    else clusters.push([t]);
  }
  for (const group of clusters) {
    group.sort((a, b) => compareServiceNames(a.service, b.service));
    group.forEach((t, idx) => {
      t.layers.push(makeEndpointTag(
        t.anchor, t.service, t.place, "to", t.placement, t.colour, t.fg, idx));
    });
  }

  // Type filter = tri-state radio (All / Express / Standard), default All.
  state.visibleCategories = new Set(["all"]);
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

/** True for express variants — short_name ends in "X" ("700X", "60X", "1X").
 *  Drives both the off-edge destination tags and the Express type filter. */
function isExpressService(svc) {
  return /x$/i.test(String(svc || ""));
}

/** Current Type filter mode: "all" | "express" | "standard". */
function currentTypeFilter() {
  const s = state.visibleCategories;
  if (s && s.has("express"))  return "express";
  if (s && s.has("standard")) return "standard";
  return "all";
}

/** Whether a route/proposal (express or not) passes the active Type filter.
 *  Shared by network routes and proposals so the two stay in sync. */
function passesTypeFilter(isExpress) {
  const m = currentTypeFilter();
  return m === "all" || (m === "express") === !!isExpress;
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
// Destinations taken from the operator's published night-services page
// (https://www.buses.co.uk/services/night-services). Keys cover both
// stop-name and headsign variants the backend may surface so the
// override fires regardless of which string ends up in the pill.
const NIGHT_DESTINATION_OVERRIDES = {
  "N1":   { "College Close": "Downs Park",
            "Cowley Drive Shops": "Woodingdean" },
  "N5":   { "Hardwick Road": "Hangleton",
            "Grenadier Hotel": "Hangleton",
            "Hollingbury Asda": "Hollingbury",
            "Asda Crowhurst Road": "Hollingbury" },
  "N7":   { "George Street": "Hove",
            "George Street (stop J)": "Hove",
            "Marina": "Marina",
            "Marina Cinema": "Marina" },
  "N12":  { "Marine Gate": "Eastbourne",
            "Marine Gate Flats": "Eastbourne",
            "Seaford Library": "Eastbourne" },
  "N14":  { "Marine Gate": "Seaford",
            "Marine Gate Flats": "Seaford",
            "Meridian Centre": "Seaford" },
  "N25":  { "Sussex House": "Universities",
            "Coldean Lane": "Universities",
            "Park Road": "Universities" },
  "N29":  { "Coldean Lane": "Lewes",
            "School Hill Bottom": "Lewes" },
  "N48":  { "Bolney Road": "Lower Bevendean" },
  "N700": { "Wallace Avenue": "Worthing",
            "Durrington Tesco": "Worthing",
            "Tesco": "Worthing",
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
  // Type filter (All / Express / Standard) — applies to express and non-express alike.
  if (!passesTypeFilter(isExpressService(svc))) return false;
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
function makeEndpointTag([lat, lon], service, place, side, placement, bg, fg, stackIndex = 0) {
  // `side` drives the arrow text (→ for "to", ← for "from").
  // `placement` is "left" | "right" — which side of the geographical
  // anchor the pill sits on; chosen by the caller based on the
  // polyline's outward heading at this end.
  // `stackIndex` shifts the pill downward when multiple routes share
  // this terminus, so they stack vertically instead of overlapping.
  const text = (side === "to")
    ? `${service} → ${place}`
    : `${place} ← ${service}`;
  const W = 220, H = 22, GAP = 4, STACK_GAP = 2;
  const yOffset = stackIndex * (H + STACK_GAP);
  const anchor = (placement === "right")
    ? [-GAP, H / 2 - yOffset]      // pill to the right of the geographical point
    : [W + GAP, H / 2 - yOffset];  // pill to the left
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

    // Normalise once at load: a proposal carries one polyline (existing
    // schema) OR an array of polylines (new for proposals that fork at
    // a junction, e.g. N2 splitting at Red Lion). Downstream code reads
    // p._polylines and never has to branch on schema shape.
    p._polylines = Array.isArray(p.polylines) && p.polylines.length
      ? p.polylines.filter(pl => Array.isArray(pl) && pl.length >= 2)
      : (Array.isArray(p.polyline) && p.polyline.length >= 2 ? [p.polyline] : []);

    // One dashed line per branch — same colour, indistinguishable until
    // a multi-branch proposal needs them.
    for (const pl of p._polylines) {
      layers.push(L.polyline(pl, {
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
  // Draw proposal layers appropriate to the current tab once data lands
  // (officials only on the Proposals tab).
  if (state.viewMode === "improvements") reconcileProposalLayers();
  resolvePendingProposalId();
}

/** True when a proposal is flagged as a night service. */
function isProposalNight(p) {
  return !!(p && p.is_night);
}

/** True for project-maintained proposals (auto-shown in the Improvements
 *  view). Community-submitted proposals (category === "community", or
 *  any other value) stay hidden until the user clicks them in. */
function isOfficialProposal(p) {
  return (p && p.category) === "official";
}

/** Decide which proposal layers belong on the map right now, given the
 *  active tab + the "Show proposals" overlay toggle. Official lines are
 *  gated behind the Proposals tab so the Improvements view opens on a clean
 *  network map (About tab) — the official proposals only draw once the user
 *  switches to Proposals. The explicit overlay toggle still trumps the tab.
 *  Idempotent — safe to call on every tab/mode/toggle change. */
function reconcileProposalLayers() {
  if (state.showProposals) { showAllProposals(); return; }
  if (state.improvementsTab === "proposals") {
    hideAllProposals();        // clear any other-mode community layers first
    showOfficialProposals();
  } else {
    // About tab: nothing auto-drawn. hideAllProposals keeps a selected
    // proposal visible (selection trumps), which is what deep-links want.
    hideAllProposals();
  }
}

/** Add all official proposals matching the current day/night mode to
 *  the map. Idempotent — safe to call on every mode toggle. */
function showOfficialProposals() {
  const isNight = state.serviceMode === "night";
  for (const p of state.proposals || []) {
    if (!isOfficialProposal(p)) continue;
    if (isProposalNight(p) !== isNight) continue;
    if (!passesTypeFilter(isExpressService(p.name))) continue;
    showProposal(p.id);
  }
}

function renderProposalsList() {
  if (!dom.proposalsList) return;
  const isNight = state.serviceMode === "night";
  const proposals = (state.proposals || []).filter(p => {
    if (isProposalNight(p) !== isNight) return false;
    // Type filter (All / Express / Standard) — matches the map layers.
    if (!passesTypeFilter(isExpressService(p.name))) return false;
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

  const official  = proposals.filter(isOfficialProposal);
  const community = proposals.filter(p => !isOfficialProposal(p));

  const cardHtml = (p) => {
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
  };

  const section = (title, blurb, items) => items.length === 0 ? "" : `
    <section class="proposals-section">
      <h3 class="proposals-section-title">${escapeHtml(title)}</h3>
      ${blurb ? `<p class="proposals-section-blurb">${escapeHtml(blurb)}</p>` : ""}
      <div class="proposals-section-cards">${items.map(cardHtml).join("")}</div>
    </section>`;

  dom.proposalsList.innerHTML =
    section("Maintained routes",
            "Shown on the map by default — curated by the project.",
            official) +
    section("Community submissions",
            "Click to show on the map. Add yours via a pull request to data/proposals.json.",
            community);

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
  const prevId = state.selectedProposalId;
  state.selectedProposalId = id;

  if (id) {
    showProposal(id);
    // Pan / zoom to fit the selected proposal's polyline(s)
    const p = (state.proposals || []).find(x => x.id === id);
    const allPts = (p && p._polylines || []).flat();
    if (allPts.length) {
      state.map.fitBounds(L.latLngBounds(allPts), {
        padding: [40, 40], maxZoom: 14,
      });
    }
  } else if (!state.showProposals && prevId) {
    // Deselect → hide just the previously-shown community proposal.
    // Official ones stay visible (they're the baseline of this view).
    const prev = (state.proposals || []).find(x => x.id === prevId);
    if (prev && !isOfficialProposal(prev)) hideProposal(prevId);
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
    if (isProposalNight(p) === isNight && passesTypeFilter(isExpressService(p.name))) {
      showProposal(p.id);
    } else {
      hideProposal(p.id);
    }
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
  // Reconcile honours the active tab: turning the overlay off on the
  // Proposals tab falls back to the official lines, not a blank map.
  reconcileProposalLayers();
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
// TICKET VIEW (fare zones)
// ============================================================

/** Memoised loader, mirrors loadRouteLines: fetch the zone catalogue once and
 *  pre-build a Leaflet polygon for every zone that ships geometry. */
function loadTicketZones() {
  if (!state._ticketZonesPromise) {
    state._ticketZonesPromise = loadTicketZonesImpl().catch(err => {
      state._ticketZonesPromise = null;
      throw err;
    });
  }
  return state._ticketZonesPromise;
}

async function loadTicketZonesImpl() {
  if (state.ticketZones) return;
  let data;
  try {
    const res = await fetch("data/ticket_zones.json");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    data = await res.json();
  } catch (err) {
    // No catalogue yet (or fetch failed) — degrade to an empty list, don't throw.
    console.warn("Ticket zones load failed:", err);
    state.ticketZones = [];
    renderTicketZonesList();
    return;
  }
  state.ticketZones = Array.isArray(data.zones) ? data.zones : [];
  state.ticketOperatorNotes = (data.operator_notes && typeof data.operator_notes === "object")
    ? data.operator_notes : {};

  if (!state.map.getPane("ticketZonePane")) {
    const pane = state.map.createPane("ticketZonePane");
    pane.style.zIndex = 404; // above overlayPane (400), below markers
  }

  const zoneById = Object.fromEntries(state.ticketZones.map(z => [z.id, z]));
  for (const z of state.ticketZones) {
    const colour = z.color || "#444";
    // A zone's drawn area can come from its own `polygon`, an explicit
    // `polygons` array, or `polygons_from` — a list of other zone ids whose
    // shapes are reused (Metrovoyager = citySAVER + Worthing, without copying
    // ~500 coords). Several rings render as one multipolygon layer; Leaflet
    // treats [[ring],[ring]] as separate fills vs [ring,ring] (ring + holes).
    let rings = [];
    if (Array.isArray(z.polygons_from)) {
      rings.push(...z.polygons_from
        .map(id => zoneById[id] && zoneById[id].polygon)
        .filter(p => Array.isArray(p) && p.length >= 3));
    }
    if (Array.isArray(z.polygons)) rings.push(...z.polygons);   // inline rings (e.g. a seam bridge)
    if (Array.isArray(z.polygon) && z.polygon.length >= 3) rings.push(z.polygon);
    rings = rings.filter(p => Array.isArray(p) && p.length >= 3);
    if (rings.length) {
      const latlngs = rings.length > 1 ? rings.map(r => [r]) : rings[0];
      state.ticketZoneLayers[z.id] = L.polygon(latlngs, {
        color:       colour,
        weight:      2,
        opacity:     0.9,
        fillColor:   colour,
        fillOpacity: 0.12,
        interactive: false,
        pane:        "ticketZonePane",
      });
    }
    // Reach pills (networkSAVER-style): the furthest points routes reach beyond
    // the city zone, rendered as "{routes} → {place}" tags. Each pill is tinted
    // with its route's livery colour (first route number wins for "28/29").
    if (Array.isArray(z.reach_points) && z.reach_points.length) {
      state.ticketReachLayers[z.id] = z.reach_points.map(rp => {
        const firstRoute = rp.routes ? String(rp.routes).split(/[\/,]/)[0].trim() : "";
        const bg = getRouteColour(firstRoute, z.operator);
        const fg = pickTextOn(bg) === "dark" ? "#1a1a1a" : "#ffffff";
        return makeReachPill(rp.lat, rp.lon,
          `${rp.routes ? rp.routes + " → " : ""}${rp.name}`, bg, fg);
      });
    }
  }
  renderTicketZonesList();
}

/** A standalone destination pill centred on a point (reuses the route-endpoint
 *  pill look). Used for networkSAVER "reach" tags. */
function makeReachPill(lat, lon, label, bg, fg) {
  return L.marker([lat, lon], {
    icon: L.divIcon({
      className: "ticket-reach-tag",
      html: `<span class="route-endpoint-pill ticket-reach-pill" `
          + `style="background:${bg};color:${fg}">${escapeHtml(label)}</span>`,
      iconSize:   [0, 0],
      iconAnchor: [0, 0],
    }),
    interactive: false,
    keyboard:    false,
    zIndexOffset: 650,
  });
}

function showTicketZones() {
  reconcileTicketDisplay();
}

function hideTicketZones() {
  for (const layer of Object.values(state.ticketZoneLayers)) {
    if (state.map.hasLayer(layer)) state.map.removeLayer(layer);
  }
  for (const pills of Object.values(state.ticketReachLayers)) {
    for (const m of pills) if (state.map.hasLayer(m)) state.map.removeLayer(m);
  }
}

/** Look up a zone's operator from the loaded catalogue. */
function zoneOperator(zoneId) {
  const z = (state.ticketZones || []).find(z => z.id === zoneId);
  return z ? z.operator : null;
}

/** Map display is operator-driven: a zone's polygon + reach pills are shown
 *  only while its operator is expanded in the list. Nothing shows by default;
 *  clicking an operator reveals all of that operator's zones. The selected
 *  sub-card's zone just gets a heavier stroke for emphasis. */
function reconcileTicketDisplay() {
  const expanded = state.expandedOperators || new Set();
  const inTickets = state.viewMode === "tickets";
  for (const [id, layer] of Object.entries(state.ticketZoneLayers)) {
    const show = inTickets && expanded.has(zoneOperator(id));
    if (show) {
      if (!state.map.hasLayer(layer)) layer.addTo(state.map);
      const sel = id === state.selectedZoneId;
      layer.setStyle({ weight: sel ? 4 : 2.5, fillOpacity: sel ? 0.3 : 0.22 });
      if (sel) layer.bringToFront();
    } else if (state.map.hasLayer(layer)) {
      state.map.removeLayer(layer);
    }
  }
  for (const [zid, pills] of Object.entries(state.ticketReachLayers)) {
    const show = inTickets && expanded.has(zoneOperator(zid));
    for (const m of pills) {
      if (show && !state.map.hasLayer(m))      m.addTo(state.map);
      else if (!show && state.map.hasLayer(m)) state.map.removeLayer(m);
    }
  }
}

/** Combined map bounds of every shown zone (polygons + reach pills) for an
 *  operator, or null if it has nothing to fit to. */
function operatorBounds(op) {
  let b = null;
  for (const z of (state.ticketZones || [])) {
    if (z.operator !== op) continue;
    const layer = state.ticketZoneLayers[z.id];
    if (layer) b = b ? b.extend(layer.getBounds()) : L.latLngBounds(layer.getBounds());
    for (const m of (state.ticketReachLayers[z.id] || [])) {
      const ll = m.getLatLng();
      b = b ? b.extend(ll) : L.latLngBounds(ll, ll);
    }
  }
  return b;
}

/** Toggle selection of a zone (click again to deselect); zoom to its polygon,
 *  or to its reach-pill cluster for a card/reach zone like networkSAVER. */
function selectZone(id) {
  state.selectedZoneId = (state.selectedZoneId === id) ? null : id;
  reconcileTicketDisplay();
  const sel   = state.selectedZoneId;
  const layer = state.ticketZoneLayers[sel];
  const pills = state.ticketReachLayers[sel];
  if (layer) {
    state.map.fitBounds(layer.getBounds(), { padding: [40, 40], maxZoom: 13 });
  } else if (pills && pills.length) {
    const b = L.latLngBounds(pills.map(m => m.getLatLng()));
    state.map.fitBounds(b, { padding: [50, 50], maxZoom: 12 });
  }
  renderTicketZonesList();
}

function renderTicketZonesList() {
  if (!dom.ticketZonesList) return;
  const zones = state.ticketZones || [];
  if (zones.length === 0) {
    dom.ticketZonesList.innerHTML = `<p class="proposals-empty">No ticket zones yet.</p>`;
    return;
  }
  const cardHtml = (z) => {
    const sel      = z.id === state.selectedZoneId;
    const hasGeo   = !!state.ticketZoneLayers[z.id];
    const hasReach = !!(state.ticketReachLayers[z.id] || []).length;
    const border   = getOperatorBorderColour(z.operator);
    const meta     = z.price || "";
    // Reach-aware note: a reach zone IS shown (as tags), so don't say "not drawn".
    // A card may also carry its own `note`; intentional card-only tickets (those
    // with a `coverage` blurb, e.g. Gold) shouldn't get the "not drawn" fallback.
    let note = z.note || "";
    if (!note) {
      if (hasReach && !hasGeo) note = "Reach shown as tags — see official map for the full zone";
      else if (!hasGeo && !hasReach && !z.coverage) note = "Whole-network — see official map";
    }
    return `
      <div class="proposal-card ticket-zone-card ${sel ? "selected" : ""} ${z.category === "proposed" ? "ticket-zone--proposed" : ""}"
           role="button" tabindex="0"
           data-zone-id="${escapeAttr(z.id)}"
           style="border-left-color:${escapeAttr(border)}">
        <span class="proposal-card-name">${escapeHtml(z.name || z.id)}${z.category === "proposed" ? ` <span class="ticket-zone-proposed-tag">Proposed</span>` : ""}</span>
        ${meta ? `<span class="proposal-card-summary">${escapeHtml(meta)}</span>` : ""}
        ${z.coverage ? `<span class="proposal-card-summary">${escapeHtml(z.coverage)}</span>` : ""}
        ${z.restrictions ? `<span class="ticket-zone-restrict">${escapeHtml(z.restrictions)}</span>` : ""}
        ${z.official_map_url ? `<a class="ticket-zone-link" href="${escapeAttr(z.official_map_url)}" target="_blank" rel="noopener noreferrer">View official zone map ↗</a>` : ""}
        ${note ? `<span class="ticket-zone-note">${escapeHtml(note)}</span>` : ""}
      </div>`;
  };

  // Group by operator; each operator is a collapsible card revealing its zones.
  const byOp = new Map();              // operator → [zones], preserving first-seen order
  for (const z of zones) {
    if (!byOp.has(z.operator)) byOp.set(z.operator, []);
    byOp.get(z.operator).push(z);
  }
  if (!state.expandedOperators) state.expandedOperators = new Set();
  // Keep the operator of the selected zone open so the highlighted card shows
  // (expansion is exclusive — only that operator's zones stay on the map).
  if (state.selectedZoneId) {
    const selZone = zones.find(z => z.id === state.selectedZoneId);
    if (selZone) state.expandedOperators = new Set([selZone.operator]);
  }

  const operatorSection = (op, items) => {
    const fill = OPERATOR_COLOURS[op] || "#444";
    const fg   = pickTextOn(fill) === "dark" ? "#1a1a1a" : "#ffffff";
    const open = state.expandedOperators.has(op);
    const n    = items.length;
    // Ungrouped cards first, then any named sub-categories (e.g. "Gold tickets"),
    // then an operator-wide footnote (e.g. the N700 supplement).
    const ungrouped = items.filter(z => !z.subgroup);
    const subgroups = new Map();
    for (const z of items) {
      if (!z.subgroup) continue;
      if (!subgroups.has(z.subgroup)) subgroups.set(z.subgroup, []);
      subgroups.get(z.subgroup).push(z);
    }
    let inner = ungrouped.map(cardHtml).join("");
    for (const [name, sub] of subgroups) {
      inner += `<p class="ticket-subgroup-heading">${escapeHtml(name)}</p>` + sub.map(cardHtml).join("");
    }
    const footnote = (state.ticketOperatorNotes || {})[op];
    if (footnote) inner += `<p class="ticket-operator-footnote">${escapeHtml(footnote)}</p>`;
    return `
      <section class="ticket-operator-group">
        <div class="ticket-operator-card" role="button" tabindex="0"
             data-operator="${escapeAttr(op)}" aria-expanded="${open ? "true" : "false"}"
             style="--op-bg:${fill};--op-fg:${fg}">
          <span class="ticket-operator-name">${escapeHtml(getOperatorName(op) || op || "Other operators")}</span>
          <span class="ticket-operator-count">${n} ${n === 1 ? "ticket" : "tickets"}</span>
          <svg class="icon ticket-operator-chevron" aria-hidden="true"><use href="#i-chevron-down"/></svg>
        </div>
        <div class="ticket-operator-zones ${open ? "expanded" : ""}">
          ${inner}
        </div>
      </section>`;
  };
  dom.ticketZonesList.innerHTML =
    [...byOp.entries()].map(([op, items]) => operatorSection(op, items)).join("");

  // Operator card: open this operator (revealing its zones on the map) and
  // close any other — expansion is exclusive so overlapping operators never
  // pile filled polygons on top of each other. Click an open one to collapse.
  dom.ticketZonesList.querySelectorAll(".ticket-operator-card").forEach(card => {
    const toggle = () => {
      const op = card.dataset.operator;
      const nowOpen = !state.expandedOperators.has(op);
      // A header click is an operator switch, not a zone pick — always clear the
      // sub-card selection, else renderTicketZonesList's auto-expand would snap
      // expansion back to the previously-selected zone's operator.
      state.selectedZoneId = null;
      state.expandedOperators = nowOpen ? new Set([op]) : new Set();
      renderTicketZonesList();                      // reflect exclusive state
      reconcileTicketDisplay();
      if (nowOpen) {
        const b = operatorBounds(op);
        if (b && b.isValid()) state.map.fitBounds(b, { padding: [45, 45], maxZoom: 13 });
      }
    };
    card.addEventListener("click", toggle);
    card.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggle(); }
    });
  });

  // Zone sub-card: select/zoom its zone.
  dom.ticketZonesList.querySelectorAll(".ticket-zone-card").forEach(card => {
    const activate = (e) => {
      if (e.target.closest("a")) return;      // let the official-map link through
      selectZone(card.dataset.zoneId);
    };
    card.addEventListener("click", activate);
    card.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); activate(e); }
    });
  });
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
  applyStopVisibility();   // editor needs every stop visible regardless of mode
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
  applyStopVisibility();   // restore normal mode-driven filtering
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
      <button class="editor-action-btn editor-icon-only" id="ed-download-btn" type="button"
              aria-label="Download JSON" title="Download JSON" ${canExport ? "" : "disabled"}>
        <svg class="icon" aria-hidden="true"><use href="#i-download"/></svg>
      </button>
      <button class="editor-action-btn editor-help-btn" id="ed-help-btn" type="button"
              aria-label="How to contribute" title="How to contribute">
        <svg class="icon" aria-hidden="true"><use href="#i-info"/></svg>
      </button>
      <button class="editor-action-btn primary editor-action-btn--focal" id="ed-github-btn" type="button" ${canExport ? "" : "disabled"}>
        <svg class="icon" aria-hidden="true"><use href="#i-github"/></svg>
        <span>Contribute</span>
      </button>
      <span class="editor-status" id="ed-status"></span>

      <div class="editor-help-popover hidden" id="ed-help-popover" role="dialog"
           aria-labelledby="ed-help-title" aria-modal="false">
        <button class="editor-help-close" id="ed-help-close" type="button"
                aria-label="Close help">
          <svg class="icon" aria-hidden="true"><use href="#i-x"/></svg>
        </button>
        <h4 class="editor-help-title" id="ed-help-title">Submitting your proposal</h4>
        <p class="editor-help-blurb">
          Contribute opens a GitHub page with your route already filled in.
          The maintainers review submissions and turn the good ones into
          live route lines on the map.
        </p>
        <ol class="editor-help-steps">
          <li>
            <svg class="icon editor-help-step-icon" aria-hidden="true"><use href="#i-github"/></svg>
            <span><strong>Need a GitHub account.</strong>
              <a href="https://github.com/signup" target="_blank" rel="noopener noreferrer">Sign up free</a> if you don't have one — takes about 30 seconds.</span>
          </li>
          <li>
            <svg class="icon editor-help-step-icon" aria-hidden="true"><use href="#i-pin"/></svg>
            <span><strong>Click Contribute</strong> — a GitHub page opens with the title and details of your proposal already filled in.</span>
          </li>
          <li>
            <svg class="icon editor-help-step-icon" aria-hidden="true"><use href="#i-plus"/></svg>
            <span><strong>Scroll down and click the green button at the bottom of the page.</strong> That's it — you're done.</span>
          </li>
        </ol>
      </div>
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

  // Help popover (Contribute walkthrough)
  const helpBtn   = dom.proposalEditor.querySelector("#ed-help-btn");
  const helpPop   = dom.proposalEditor.querySelector("#ed-help-popover");
  const helpClose = dom.proposalEditor.querySelector("#ed-help-close");
  const closeHelp = () => helpPop.classList.add("hidden");
  helpBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    helpPop.classList.toggle("hidden");
  });
  helpClose.addEventListener("click", closeHelp);
  // Click anywhere outside the popover (and not on the trigger) dismisses
  document.addEventListener("click", (e) => {
    if (helpPop.classList.contains("hidden")) return;
    if (helpPop.contains(e.target) || helpBtn.contains(e.target)) return;
    closeHelp();
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && !helpPop.classList.contains("hidden")) closeHelp();
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

  // Metrobus
  "METR": "icons/METR.png",

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
// REALTIME TRAINS — rail integration
// ------------------------------------------------------------
// Architecture: stations are static markers (loaded once from
// /api/rail-stations). Clicking a station opens a live departure
// board in the existing side panel. Each board row has a
// "Show on map" action which fetches the service's calling
// pattern and renders an interpolated train dot, polled every
// 30 s while the service stays selected.
//
// There is no train GPS in this data — positions are estimates
// from the per-stop timestamps RTT exposes. The microcopy on
// every train marker and the board footer says so.
// ============================================================

const RAIL_REFRESH_MS         = 30_000;   // matches RTT upstream cadence + cache TTL
const RAIL_STATION_COLOUR     = "#1e8e3e"; // neutral rail green (not per-operator; station ≠ operator)
const RAIL_TRAIN_FILL_DEFAULT = "#0b66c2";

// ATOC code → brand colour for the operator badge on board rows.
// Started with the ATOCs serving our 15 in-bbox stations; extend on first
// sight of a different code in the live feed.
const RAIL_OPERATOR_COLOURS = {
  "SN": "#00a560",   // Southern
  "TL": "#ec4e9b",   // Thameslink
  "GX": "#f04e23",   // Gatwick Express
  "GW": "#1c4d8d",   // Great Western
  "SW": "#00a3e0",   // South Western
  "GR": "#143d6b",   // LNER
  "GN": "#3c2974",   // Great Northern
  "VT": "#004354",   // Avanti
  "XC": "#600f48",   // CrossCountry
};
const RAIL_OPERATOR_NAMES = {
  "SN": "Southern",
  "TL": "Thameslink",
  "GX": "Gatwick Express",
  "GW": "GWR",
  "SW": "South Western",
};

function railOperatorColour(code) {
  return RAIL_OPERATOR_COLOURS[String(code || "").toUpperCase()] || "#555";
}
function railOperatorName(code, fallback) {
  const k = String(code || "").toUpperCase();
  return RAIL_OPERATOR_NAMES[k] || fallback || k || "Rail";
}

// Parse an ISO 8601 string from the RTT projection. Returns null on bad input.
function parseRailTime(iso) {
  if (!iso) return null;
  const t = Date.parse(iso);
  return Number.isFinite(t) ? t : null;
}

// Format an ISO string as HH:mm for the board / popup.
function formatRailTime(iso) {
  const t = parseRailTime(iso);
  if (t == null) return "–";
  return new Date(t).toLocaleTimeString("en-GB",
    { hour: "2-digit", minute: "2-digit", hour12: false });
}

// Pick the most useful "expected" time for display: actual > forecast > scheduled.
function pickRailDisplayTime(td) {
  if (!td) return { iso: null, kind: "missing" };
  if (td.actual)    return { iso: td.actual,   kind: "actual"   };
  if (td.forecast)  return { iso: td.forecast, kind: "forecast" };
  if (td.scheduled) return { iso: td.scheduled, kind: "scheduled" };
  return { iso: null, kind: "missing" };
}

// Ensure the rail panes exist. Stations sit just above the route/zone layers,
// trains above stations (so a station marker doesn't occlude a train on it).
function ensureRailPanes() {
  if (!state.map) return;
  if (!state.map.getPane("railStationPane")) {
    const p = state.map.createPane("railStationPane");
    p.style.zIndex = "406";
  }
  if (!state.map.getPane("railTrainPane")) {
    const p = state.map.createPane("railTrainPane");
    p.style.zIndex = "407";
  }
}

// SVG glyph used inside both station markers and train markers.
function railStationDivIcon() {
  const html = `
    <div class="rail-station-icon" title="Train station">
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <rect x="6" y="3" width="12" height="14" rx="3" ry="3" fill="${RAIL_STATION_COLOUR}"/>
        <rect x="8" y="6" width="3.5" height="4" rx="0.5" fill="#fff" opacity="0.95"/>
        <rect x="12.5" y="6" width="3.5" height="4" rx="0.5" fill="#fff" opacity="0.95"/>
        <circle cx="9" cy="14" r="1.4" fill="#1a1a1a"/>
        <circle cx="15" cy="14" r="1.4" fill="#1a1a1a"/>
        <path d="M9 18 L7 21" stroke="${RAIL_STATION_COLOUR}" stroke-width="1.6" stroke-linecap="round"/>
        <path d="M15 18 L17 21" stroke="${RAIL_STATION_COLOUR}" stroke-width="1.6" stroke-linecap="round"/>
      </svg>
    </div>`;
  return L.divIcon({
    html, className: "rail-station-divicon",
    iconSize:   [22, 22],
    iconAnchor: [11, 11],
  });
}

function railTrainDivIcon(colour, label, opts = {}) {
  const bg = colour || RAIL_TRAIN_FILL_DEFAULT;
  // No bearing rotation — the pill has no directional glyph and the label
  // needs to stay readable. Pulse class is removed by JS after ~4 s.
  const cls = ["rail-train-icon-wrap"];
  if (opts.pulse) cls.push("rail-train-icon-wrap--pulse");
  const html = `
    <div class="${cls.join(' ')}" style="--rail-train-bg:${bg};">
      <div class="rail-train-pulse" aria-hidden="true"></div>
      <div class="rail-train-icon" style="background:${bg};">
        <span class="rail-train-label">${escapeHtml(label || "")}</span>
      </div>
    </div>`;
  return L.divIcon({
    html, className: "rail-train-divicon",
    iconSize:   [48, 26],
    iconAnchor: [24, 13],
  });
}

async function loadRailStations() {
  if (state.railStations) return state.railStations;
  try {
    const data = await apiFetch("/api/rail-stations");
    state.railStations    = data?.stations || [];
    state.railStationByCrs = Object.fromEntries(state.railStations.map(s => [s.crs, s]));
    return state.railStations;
  } catch (err) {
    console.warn("Failed to load rail stations:", err);
    state.railStations = [];
    return [];
  }
}

function showRailStations() {
  if (!state.map || state.viewMode !== "live" || !state.railVisible) return;
  if (!state.railStations) return;
  ensureRailPanes();
  for (const s of state.railStations) {
    if (state.railStationMarkers[s.crs]) continue;
    const m = L.marker([s.lat, s.lon], {
      icon:        railStationDivIcon(),
      pane:        "railStationPane",
      title:       `${s.name} (${s.crs})`,
      keyboard:    false,
      bubblingMouseEvents: false,
    });
    m.on("click", (e) => {
      L.DomEvent.stopPropagation(e);
      state._ignoreNextMapClick = true;
      openRailBoard(s.crs, s.name);
    });
    m.addTo(state.map);
    state.railStationMarkers[s.crs] = m;
  }
}

function hideRailStations() {
  for (const crs of Object.keys(state.railStationMarkers)) {
    state.map.removeLayer(state.railStationMarkers[crs]);
    delete state.railStationMarkers[crs];
  }
}

// ── Departure board (panel reuse) ────────────────────────────
async function openRailBoard(crs, name) {
  if (!crs) return;
  // Clear any bus stop / bus selection so the panel's other state is consistent.
  state.selectedStop            = null;
  state.selectedVehicleRef      = null;
  state.selectedVehicle         = null;
  state.busDetails              = null;
  state.busDetailsLoading       = false;
  stopBusInfoTicker();
  state.selectedRailStation     = { crs, name };
  state.railBoardLoading        = true;
  state.railBoard               = null;

  dom.panelStopName.textContent = `🚆 ${name}`;
  dom.panelStopId.textContent   = `CRS: ${crs}`;
  setActiveTab("stop");
  showPanelState("rail");      // shows the dedicated rail host, hides bus children
  renderRailBoard();           // shows "loading" rows while we fetch
  dom.departurePanel.scrollIntoView({ behavior: "smooth", block: "end" });

  try {
    const data = await apiFetch(`/api/rail-departures?crs=${encodeURIComponent(crs)}`);
    if (!state.selectedRailStation || state.selectedRailStation.crs !== crs) return;
    state.railBoard = data;
  } catch (err) {
    console.warn("Rail board fetch failed:", err);
    if (state.selectedRailStation && state.selectedRailStation.crs === crs) {
      state.railBoard = { error: err?.message || "Could not load rail data." };
    }
  } finally {
    state.railBoardLoading = false;
    renderRailBoard();
  }
}

function renderRailBoard() {
  const host = dom.railBoardHost;
  if (!host || !state.selectedRailStation) return;
  const board   = state.railBoard;
  const loading = state.railBoardLoading;
  const services = (board && board.services) || [];
  let rowsHtml;
  if (loading) {
    rowsHtml = `<tr><td colspan="5" class="rail-board-loading">Loading live trains…</td></tr>`;
  } else if (board && board.error) {
    rowsHtml = `<tr><td colspan="5" class="rail-board-error">${escapeHtml(board.error)}</td></tr>`;
  } else if (services.length === 0) {
    rowsHtml = `<tr><td colspan="5" class="rail-board-empty">No live departures right now.</td></tr>`;
  } else {
    rowsHtml = services.map(svc => renderRailBoardRow(svc)).join("");
  }
  host.innerHTML = `
    <div class="rail-board">
      <div class="rail-board-header">
        <span class="rail-board-eyebrow">Live trains</span>
        <span class="rail-board-station">${escapeHtml(state.selectedRailStation.name)} (${escapeHtml(state.selectedRailStation.crs)})</span>
      </div>
      <table class="departures-table rail-board-table">
        <thead>
          <tr>
            <th class="rail-col-time">Time</th>
            <th class="rail-col-dest">Destination</th>
            <th class="rail-col-plat">Plat</th>
            <th class="rail-col-op">Operator</th>
            <th class="rail-col-act" aria-label="Track on map"></th>
          </tr>
        </thead>
        <tbody>${rowsHtml}</tbody>
      </table>
      <p class="rail-board-footer">
        Live data from Realtime Trains. Train positions are estimated
        between stations — there is no GPS on this feed.
      </p>
    </div>
  `;
  // Wire the "Show on map" buttons after the table is in the DOM.
  host.querySelectorAll("[data-rail-action='select']").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.preventDefault();
      const uid  = btn.getAttribute("data-uid");
      const date = btn.getAttribute("data-date");
      const isSelected = !!state.selectedRailServices[uid];
      if (isSelected) deselectRailService(uid);
      else            selectRailService(uid, date);
    });
  });
}

function renderRailBoardRow(svc) {
  if (!svc) return "";
  const dep = svc.departure || {};
  const arr = svc.arrival   || {};
  // For board display prefer the departure time (origin/intermediate); fall back to arrival (terminus).
  const sched = dep.scheduled || arr.scheduled;
  const live  = pickRailDisplayTime(dep.scheduled ? dep : arr);
  const schedTxt = formatRailTime(sched);
  const liveTxt  = formatRailTime(live.iso);
  const isCancelled = dep.cancelled || arr.cancelled
    || /CANCELLED/i.test(svc.displayAs || "")
    || /CANCELLED/i.test(svc.callType || "");
  const isChanged = sched && live.iso && sched !== live.iso;
  const destList = (svc.destination || []).map(d => d.description).filter(Boolean);
  const destination = destList.join(" & ") || (svc.headcode ? `Headcode ${svc.headcode}` : "—");
  const plat   = svc.platform || "–";
  const platCls = svc.platformConfirmed ? "rail-plat rail-plat--confirmed"
                                        : "rail-plat rail-plat--unconfirmed";
  const opCode = svc.atocCode || "";
  const opCol  = railOperatorColour(opCode);
  const opName = railOperatorName(opCode, svc.atocName);
  const opTxt  = pickTextOn(opCol) === "dark" ? "#1a1a1a" : "#ffffff";
  const uid = svc.uid || "";
  const date = svc.departureDate || "";
  const isSelected = !!state.selectedRailServices[uid];
  const actLabel = isSelected ? "Hide" : "Track";
  const actCls   = isSelected ? "rail-track-btn rail-track-btn--on" : "rail-track-btn";
  const liveCls = isCancelled ? "rail-time rail-time--cancelled"
                : isChanged   ? "rail-time rail-time--changed"
                : live.kind === "actual"   ? "rail-time rail-time--actual"
                                            : "rail-time";
  return `
    <tr class="rail-row ${isCancelled ? 'rail-row--cancelled' : ''}">
      <td class="rail-col-time">
        <span class="rail-time-sched">${escapeHtml(schedTxt)}</span>
        ${liveTxt !== schedTxt
          ? `<span class="${liveCls}">→ ${escapeHtml(liveTxt)}</span>`
          : ''}
      </td>
      <td class="rail-col-dest">${escapeHtml(destination)}</td>
      <td class="rail-col-plat"><span class="${platCls}">${escapeHtml(plat)}</span></td>
      <td class="rail-col-op">
        <span class="rail-op-badge" style="background:${opCol};color:${opTxt};">
          ${escapeHtml(opName)}
        </span>
      </td>
      <td class="rail-col-act">
        ${uid && date
          ? `<button type="button" class="${actCls}"
                  data-rail-action="select" data-uid="${escapeAttr(uid)}"
                  data-date="${escapeAttr(date)}">${actLabel}</button>`
          : ''}
      </td>
    </tr>`;
}

// ── Selected-service tracking (Stage 2) ──────────────────────
async function selectRailService(uid, date) {
  if (!uid || !date) return;
  if (state.selectedRailServices[uid]) return; // already tracked
  state.selectedRailServices[uid] = { date, calling: null, marker: null, lastPos: null };
  startRailRefreshTimer();
  startRailAnimationLoop();
  try {
    await refreshRailService(uid);
  } catch (err) {
    console.warn("rail service initial fetch failed", uid, err);
  }
  recomputeRailPositions();
  // recomputeRailPositions handles the one-shot flyTo when a marker first
  // materialises. If nothing was drawn (forecast-only train not in-bbox yet),
  // tell the user so they know to wait.
  const entry = state.selectedRailServices[uid];
  if (entry && !entry.marker) {
    showToast("Service not yet showing on map — will appear when it enters the area.");
  }
  renderRailBoard(); // re-render so the row button flips to "Hide"
}

function deselectRailService(uid) {
  const entry = state.selectedRailServices[uid];
  if (!entry) return;
  if (entry.marker) state.map.removeLayer(entry.marker);
  delete state.selectedRailServices[uid];
  if (Object.keys(state.selectedRailServices).length === 0) {
    stopRailRefreshTimer();
    stopRailAnimationLoop();
  }
  renderRailBoard();
}

function clearAllSelectedRailServices() {
  for (const uid of Object.keys(state.selectedRailServices)) {
    const entry = state.selectedRailServices[uid];
    if (entry.marker) state.map.removeLayer(entry.marker);
  }
  state.selectedRailServices = {};
  stopRailRefreshTimer();
  stopRailAnimationLoop();
}

function startRailRefreshTimer() {
  if (state.railRefreshTimer) return;
  state.railRefreshTimer = setInterval(async () => {
    for (const uid of Object.keys(state.selectedRailServices)) {
      const entry = state.selectedRailServices[uid];
      try { await refreshRailService(uid); }
      catch (e) { console.warn("rail service refresh failed", uid, e); }
    }
    recomputeRailPositions();
  }, RAIL_REFRESH_MS);
}

function stopRailRefreshTimer() {
  if (state.railRefreshTimer) {
    clearInterval(state.railRefreshTimer);
    state.railRefreshTimer = null;
  }
}

async function refreshRailService(uid) {
  const entry = state.selectedRailServices[uid];
  if (!entry) return;
  const data = await apiFetch(
    `/api/rail-service?uid=${encodeURIComponent(uid)}&date=${encodeURIComponent(entry.date)}`
  );
  if (!state.selectedRailServices[uid]) return; // user deselected mid-fetch
  state.selectedRailServices[uid].calling = data;
}

// Time-wrap helper for midnight-rollover services.
// If `tB < tA`, the segment crosses midnight so push `tB` forward 24 h.
// Only wrap `now` past midnight when A is an actualised departure (we know the
// train *did* leave A on day-N and we're now on day-N+1). For a forecast-seed
// A, `now < tA` is the normal "not yet arrived" case — f should clamp to 0,
// not roll over.
function wrapRailTimes(tA, tB, now, aActualised) {
  const DAY = 24 * 60 * 60 * 1000;
  if (tB < tA) tB += DAY;
  if (aActualised && now < tA) now += DAY;
  return { tA, tB, now };
}

// Recompute the *target* window (A, B, tA, tB, coords, bearing) for every
// tracked service. Called by the 30 s refresh path. This function does NOT
// move the marker each frame — that's tickRailAnimation's job. It just sets
// entry.target, and creates the marker on first availability.
function recomputeRailPositions() {
  const now = Date.now();
  for (const uid of Object.keys(state.selectedRailServices)) {
    const entry = state.selectedRailServices[uid];
    const svc = entry.calling;
    if (!svc) continue;
    const locs = svc.locations || [];
    if (locs.length < 2) {
      removeRailTrainMarker(uid);
      entry.target = null;
      continue;
    }

    // Cancelled — drop the marker, keep the entry until the user explicitly
    // hits Hide so the board's toggle state stays consistent.
    const allCancelled = locs.every(l => (l.arrival && l.arrival.cancelled)
                                       || (l.departure && l.departure.cancelled));
    if (allCancelled) { removeRailTrainMarker(uid); entry.target = null; continue; }
    const terminus = locs[locs.length - 1];
    if (terminus && terminus.arrival && terminus.arrival.actual) {
      // Train has arrived at destination — auto-clean.
      deselectRailService(uid);
      continue;
    }

    // ── Resolve A (last-known position) and B (next call) ──────────────
    // We ONLY render a marker once the train has actualised an in-bbox stop.
    // Forecast-only / "approaching from out-of-bbox" states deliberately
    // produce no marker — a dot pinned at a station the train hasn't reached
    // reads as "the train is there" which is wrong (user feedback). The
    // toast at Track time already explains "will appear when it enters the
    // area"; the board row keeps showing live progress.
    let A = null, B = null;
    let aActualised = false;
    for (let i = 0; i < locs.length; i++) {
      const l = locs[i];
      if (!l.inBbox) continue;
      const actDep = l.departure && l.departure.actual;
      const actArr = l.arrival   && l.arrival.actual;
      if (actDep || actArr) { A = { loc: l, idx: i }; aActualised = true; }
    }
    if (A) {
      for (let i = A.idx + 1; i < locs.length; i++) {
        const l = locs[i];
        if (!l.inBbox) continue;
        const actArr = l.arrival && l.arrival.actual;
        if (actArr) { A = { loc: l, idx: i }; continue; } // catch up
        B = { loc: l, idx: i };
        break;
      }
    }

    if (!A || !B) { removeRailTrainMarker(uid); entry.target = null; continue; }

    const aStation = state.railStationByCrs[A.loc.crs];
    const bStation = state.railStationByCrs[B.loc.crs];
    if (!aStation || !bStation) { removeRailTrainMarker(uid); entry.target = null; continue; }

    // Times for the interpolation window.
    const aDep = pickRailDisplayTime(A.loc.departure);
    const aArr = pickRailDisplayTime(A.loc.arrival);
    const bArr = pickRailDisplayTime(B.loc.arrival);
    const bDep = pickRailDisplayTime(B.loc.departure);
    const tAraw = parseRailTime(aDep.iso || aArr.iso);
    const tBraw = parseRailTime(bArr.iso || bDep.iso);
    if (tAraw == null || tBraw == null) { removeRailTrainMarker(uid); entry.target = null; continue; }

    entry.target = {
      aLat: aStation.lat, aLon: aStation.lon,
      bLat: bStation.lat, bLon: bStation.lon,
      tA: tAraw, tB: tBraw,
      aActualised,
      fromName: aStation.name,
      toName:   bStation.name,
      fromCrs:  A.loc.crs,
      toCrs:    B.loc.crs,
    };

    // Compute initial position so the marker can materialise immediately.
    const { lat, lon } = railPosAt(entry.target, now);

    const colour   = railOperatorColour(svc.atocCode);
    const headcode = svc.headcode || (svc.uid || "").slice(-3);
    const destCrs  = (svc.destination && svc.destination[0]
                      && svc.destination[0].crs) || "";
    const label    = destCrs || headcode;

    const justCreated = !entry.marker;
    if (justCreated) {
      ensureRailPanes();
      entry.marker = L.marker([lat, lon], {
        icon: railTrainDivIcon(colour, label, { pulse: true }),
        pane: "railTrainPane",
        title: railTrainTitle(svc, entry.target),
        keyboard: false,
      });
      entry.marker.on("click", (e) => {
        L.DomEvent.stopPropagation(e);
        state._ignoreNextMapClick = true;
        showRailTrainPopup(uid);
      });
      entry.marker.addTo(state.map);
      entry.markerColour = colour;
      entry.markerLabel  = label;
      // Pulse fades after ~4 s so a long-tracked marker doesn't strobe forever.
      setTimeout(() => {
        if (entry.marker) {
          const el = entry.marker.getElement();
          if (el) el.querySelector(".rail-train-icon-wrap")
                    ?.classList.remove("rail-train-icon-wrap--pulse");
        }
      }, 4000);
    } else if (entry.markerColour !== colour || entry.markerLabel !== label) {
      entry.marker.setIcon(railTrainDivIcon(colour, label));
      entry.markerColour = colour;
      entry.markerLabel  = label;
    }
    entry.marker.setLatLng([lat, lon]);
    const tipEl = entry.marker.getElement();
    if (tipEl) tipEl.setAttribute("title", railTrainTitle(svc, entry.target));

    entry.lastPos = { lat, lon };
    entry.lastSeg = { fromCrs: A.loc.crs, toCrs: B.loc.crs };

    // One-shot flyTo when the marker first materialises (whether on Track
    // click or a later refresh that finally produced an in-bbox actual).
    if (justCreated && !entry.flownTo) {
      state.map.flyTo([lat, lon],
                      Math.max(state.map.getZoom(), 13),
                      { duration: 0.8 });
      entry.flownTo = true;
    }
  }
}

// Helper: where is the train *right now* given a target window? Used both by
// recomputeRailPositions (initial placement) and tickRailAnimation (per frame).
function railPosAt(t, now) {
  const { tA, tB, now: tnow } = wrapRailTimes(t.tA, t.tB, now, t.aActualised);
  let f = (tB > tA) ? (tnow - tA) / (tB - tA) : 0;
  if (f < 0) f = 0; else if (f > 1) f = 1;
  return {
    lat: t.aLat + f * (t.bLat - t.aLat),
    lon: t.aLon + f * (t.bLon - t.aLon),
    f,
  };
}

function railTrainTitle(svc, target) {
  const hc = svc.headcode || svc.uid;
  return `Service ${hc} — estimated position between ${target.fromName} and ${target.toName}`;
}

// ── Per-frame animation loop ─────────────────────────────────
// Runs while at least one service is tracked. Each frame, for every entry
// with a target, recompute the position from current wall-clock time and
// move the marker. No DOM writes beyond Leaflet's own setLatLng → cheap.
let _railAnimFrame = null;
function startRailAnimationLoop() {
  if (_railAnimFrame != null) return;
  const loop = () => {
    _railAnimFrame = null;
    if (Object.keys(state.selectedRailServices).length === 0) return;
    const now = Date.now();
    for (const uid of Object.keys(state.selectedRailServices)) {
      const entry = state.selectedRailServices[uid];
      if (!entry || !entry.marker || !entry.target) continue;
      const { lat, lon } = railPosAt(entry.target, now);
      entry.marker.setLatLng([lat, lon]);
      entry.lastPos = { lat, lon };
    }
    _railAnimFrame = requestAnimationFrame(loop);
  };
  _railAnimFrame = requestAnimationFrame(loop);
}
function stopRailAnimationLoop() {
  if (_railAnimFrame != null) {
    cancelAnimationFrame(_railAnimFrame);
    _railAnimFrame = null;
  }
}

function removeRailTrainMarker(uid) {
  const entry = state.selectedRailServices[uid];
  if (!entry) return;
  if (entry.marker) {
    state.map.removeLayer(entry.marker);
    entry.marker = null;
  }
}

function showRailTrainPopup(uid) {
  const entry = state.selectedRailServices[uid];
  if (!entry || !entry.marker || !entry.calling) return;
  const svc = entry.calling;
  const nextCalls = (svc.locations || [])
    .filter(l => !(l.departure && l.departure.actual)
              && !(l.arrival   && l.arrival.actual))
    .slice(0, 3);
  const callsHtml = nextCalls.map(l => {
    const t = pickRailDisplayTime(l.arrival.scheduled ? l.arrival : l.departure);
    return `<li><b>${escapeHtml(formatRailTime(t.iso))}</b> ${escapeHtml(l.description || l.crs || "")}</li>`;
  }).join("");
  const headcode = svc.headcode || svc.uid || "";
  const opName = railOperatorName(svc.atocCode, svc.atocName);
  const html = `
    <div class="rail-train-popup">
      <div class="rail-train-popup-head">
        <span class="rail-train-popup-headcode">${escapeHtml(headcode)}</span>
        <span class="rail-train-popup-op">${escapeHtml(opName)}</span>
      </div>
      <div class="rail-train-popup-seg">
        Estimated between
        <b>${escapeHtml(state.railStationByCrs[entry.lastSeg?.fromCrs]?.name || entry.lastSeg?.fromCrs || "?")}</b>
        and
        <b>${escapeHtml(state.railStationByCrs[entry.lastSeg?.toCrs]?.name || entry.lastSeg?.toCrs || "?")}</b>
      </div>
      ${callsHtml ? `<ol class="rail-train-popup-calls">${callsHtml}</ol>` : ''}
      <p class="rail-train-popup-foot">Estimated from timetable + realtime predictions. No GPS.</p>
    </div>`;
  entry.marker.bindPopup(html, { className: "rail-train-popup-wrap" }).openPopup();
}

// Show/hide all rail UI as a group — driven by header toggle and view gating.
function setRailVisible(on) {
  state.railVisible = !!on;
  syncRailToggleUI();
  if (state.railVisible && state.viewMode === "live") {
    loadRailStations().then(() => showRailStations());
  } else {
    hideRailStations();
    clearAllSelectedRailServices();
    if (state.selectedRailStation) {
      // The board is currently shown; collapse the panel so we don't render a
      // stale board with no way to refresh.
      closePanel();
    }
  }
}
function syncRailToggleUI() {
  if (!dom.toggleRailBtn) return;
  const on = state.railVisible;
  dom.toggleRailBtn.setAttribute("aria-pressed", on ? "true" : "false");
  dom.toggleRailBtn.setAttribute("aria-label", on ? "Hide trains" : "Show trains");
  dom.toggleRailBtn.title = on ? "Hide trains" : "Show trains";
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
