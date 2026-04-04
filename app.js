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
  // Production:   Replace with your Vercel/Render deployment URL, e.g.
  //               "https://adur-worthing-bus-api.vercel.app"
  API_BASE_URL: "https://adur-worthing-bus.vercel.app",

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
// STATE
// ============================================================
const state = {
  map: null,
  stopMarkers:   {},    // atcoCode → Leaflet marker
  busMarkers:    {},    // vehicleRef → Leaflet marker
  selectedStop:  null,  // { atcoCode, name }
  refreshTimer:  null,  // setInterval handle for bus positions
  isRefreshing:  true,
  currentPopup:  null,  // open Leaflet popup (so we can close it)
};

// ============================================================
// DOM REFERENCES
// ============================================================
const dom = {
  mapLoading:         document.getElementById("map-loading"),
  lastUpdatedLabel:   document.getElementById("last-updated-label"),
  toggleRefreshBtn:   document.getElementById("toggle-refresh-btn"),
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
  refreshStopBtn:     document.getElementById("refresh-stop-btn"),
  toast:              document.getElementById("toast"),
};

// ============================================================
// INITIALISE
// ============================================================
document.addEventListener("DOMContentLoaded", init);

async function init() {
  initMap();
  bindUIEvents();

  // Load stops first (cached 24 h on backend, so fast after first call)
  await loadStops();

  // Hide initial loading overlay
  dom.mapLoading.classList.add("hidden");

  // Start live bus position loop
  startVehicleRefresh();
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

  // OpenStreetMap tiles — free, no API key required
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    maxZoom: 19,
  }).addTo(state.map);

  // Close panel when clicking an empty area of the map
  state.map.on("click", () => {
    if (state.selectedStop) closePanel();
  });
}

// ============================================================
// BUS STOPS
// ============================================================
async function loadStops() {
  try {
    const data = await apiFetch("/api/stops");
    if (!data || !data.stops) throw new Error("Invalid stops response");

    data.stops.forEach(renderStopMarker);
  } catch (err) {
    console.error("Failed to load stops:", err);
    showToast("⚠️ Could not load bus stops. Check your API configuration.");
  }
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

  // Popup with stop name and "Show departures" button
  const popupHtml = `
    <div>
      <p class="popup-stop-name">${escapeHtml(stop.name)}</p>
      <p class="popup-stop-id">Stop: ${escapeHtml(stop.atco_code)}</p>
      <button class="popup-btn" onclick="openDepartures('${stop.atco_code}', '${escapeAttr(stop.name)}')">
        🕐 Live departures
      </button>
    </div>`;

  marker.bindPopup(popupHtml, { maxWidth: 220 });

  // Clicking anywhere on the marker opens the departure panel
  marker.on("click", () => {
    openDepartures(stop.atco_code, stop.name);
  });

  state.stopMarkers[stop.atco_code] = marker;
}

// ============================================================
// LIVE BUS POSITIONS
// ============================================================
function startVehicleRefresh() {
  fetchVehicles();   // immediate first call
  state.refreshTimer = setInterval(fetchVehicles, CONFIG.VEHICLE_REFRESH_MS);
  state.isRefreshing = true;
  dom.toggleRefreshBtn.textContent = "⏸";
  dom.toggleRefreshBtn.setAttribute("aria-label", "Pause live updates");
}

function stopVehicleRefresh() {
  clearInterval(state.refreshTimer);
  state.refreshTimer = null;
  state.isRefreshing = false;
  dom.toggleRefreshBtn.textContent = "▶";
  dom.toggleRefreshBtn.setAttribute("aria-label", "Resume live updates");
}

async function fetchVehicles() {
  try {
    const data = await apiFetch("/api/vehicles");
    if (!data || !data.vehicles) return;

    updateVehicleMarkers(data.vehicles);

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
    // vehicle: { vehicle_ref, service_ref, destination, latitude, longitude, bearing, delay_seconds }
    if (!vehicle.latitude || !vehicle.longitude) return;

    const ref = vehicle.vehicle_ref;
    seenRefs.add(ref);

    const label = vehicle.service_ref || "?";
    const popupHtml = `
      <div>
        <p class="bus-popup-line"><span class="bus-popup-service">Service ${escapeHtml(label)}</span></p>
        <p class="bus-popup-line">To: ${escapeHtml(vehicle.destination || "Unknown")}</p>
        ${vehicle.delay_seconds != null
          ? `<p class="bus-popup-line">${formatDelayText(vehicle.delay_seconds)}</p>`
          : ""}
      </div>`;

    if (state.busMarkers[ref]) {
      // Move existing marker smoothly
      state.busMarkers[ref].setLatLng([vehicle.latitude, vehicle.longitude]);
      state.busMarkers[ref].setPopupContent(popupHtml);
    } else {
      // Create new marker
      const icon = L.divIcon({
        className: "",
        html: `<div class="bus-marker-icon">${escapeHtml(label)}</div>`,
        iconSize:  [28, 20],
        iconAnchor:[14, 10],
        popupAnchor:[0, -12],
      });

      const marker = L.marker([vehicle.latitude, vehicle.longitude], { icon, zIndexOffset: 200 })
        .bindPopup(popupHtml, { maxWidth: 200 })
        .addTo(state.map);

      state.busMarkers[ref] = marker;
    }
  });

  // Remove markers for buses no longer in the feed
  Object.keys(state.busMarkers).forEach(ref => {
    if (!seenRefs.has(ref)) {
      state.map.removeLayer(state.busMarkers[ref]);
      delete state.busMarkers[ref];
    }
  });
}

function formatDelayText(delaySecs) {
  if (delaySecs == null) return "";
  const abs = Math.abs(delaySecs);
  const mins = Math.round(abs / 60);
  if (mins === 0) return "🟢 On time";
  if (delaySecs < 0) return `🔵 ${mins} min${mins !== 1 ? "s" : ""} early`;
  return `🔴 ${mins} min${mins !== 1 ? "s" : ""} late`;
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
  // Close any open Leaflet popup to avoid clutter
  state.map.closePopup();

  state.selectedStop = { atcoCode, stopName };

  // Update panel header
  dom.panelStopName.textContent = stopName;
  dom.panelStopId.textContent   = `ATCO: ${atcoCode}`;

  // Show panel, hide prompt
  showPanelState("loading");

  // On mobile, scroll down so the panel is visible
  dom.departurePanel.scrollIntoView({ behavior: "smooth", block: "end" });

  await fetchDepartures(atcoCode);
};

async function fetchDepartures(atcoCode) {
  showPanelState("loading");
  try {
    const data = await apiFetch(`/api/departures?stopId=${encodeURIComponent(atcoCode)}`);
    renderDepartures(data);
  } catch (err) {
    console.error("Departures fetch failed:", err);
    showPanelState("error", err.message || "Could not load departure data.");
  }
}

function renderDepartures(data) {
  // data: { stop_name, departures: [ { service, destination, aimed_departure, expected_departure, status } ] }
  const departures = data?.departures ?? [];

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
  const destination = dep.destination || "Unknown";
  const aimed       = dep.aimed_departure;
  const expected    = dep.expected_departure;

  // Format due time (prefer expected if available, fall back to aimed)
  const displayTime = expected || aimed || null;
  const dueText     = displayTime ? formatDueTime(displayTime) : "–";
  const isImminent  = displayTime ? isWithinMinutes(displayTime, 2) : false;

  // Status
  const { label, cssClass } = buildStatusChip(dep);

  return `
    <tr>
      <td><span class="service-badge">${escapeHtml(service)}</span></td>
      <td><span class="destination-text" title="${escapeAttr(destination)}">${escapeHtml(destination)}</span></td>
      <td><span class="due-time ${isImminent ? "due-imminent" : ""}">${escapeHtml(dueText)}</span></td>
      <td><span class="status-chip ${cssClass}">${escapeHtml(label)}</span></td>
    </tr>`;
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

/** Format an ISO datetime string as HH:MM */
function formatDueTime(isoString) {
  try {
    const d = new Date(isoString);
    if (isNaN(d.getTime())) return isoString; // Return as-is if not parseable
    const now = new Date();
    const diffMs = d - now;
    const diffMins = Math.round(diffMs / 60_000);

    if (diffMins < 0)    return "Due";
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
// PANEL STATE MACHINE
// ============================================================
function showPanelState(state) {
  // Hide all inner states
  dom.panelLoading.classList.add("hidden");
  dom.panelError.classList.add("hidden");
  dom.panelPrompt.classList.add("hidden");
  dom.departuresContainer.classList.add("hidden");

  switch (state) {
    case "loading": dom.panelLoading.classList.remove("hidden"); break;
    case "error":   dom.panelError.classList.remove("hidden");   break;
    case "results": dom.departuresContainer.classList.remove("hidden"); break;
    default:        dom.panelPrompt.classList.remove("hidden");  break;
  }
}

function closePanel() {
  state.selectedStop = null;
  showPanelState("prompt");
  dom.panelStopName.textContent = "Select a stop";
  dom.panelStopId.textContent   = "";
}

// ============================================================
// UI EVENT BINDINGS
// ============================================================
function bindUIEvents() {
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

  // Toggle live refresh pause/resume
  dom.toggleRefreshBtn.addEventListener("click", () => {
    if (state.isRefreshing) {
      stopVehicleRefresh();
      showToast("Live updates paused.");
    } else {
      startVehicleRefresh();
      showToast("Live updates resumed.");
    }
  });

  // Panel error message setter
  dom.panelRetryBtn.addEventListener("click", () => {
    if (state.selectedStop) fetchDepartures(state.selectedStop.atcoCode);
  });
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
// PANEL ERROR TEXT HELPER
// ============================================================
// Extend showPanelState to also set error text
const _originalShowPanelState = showPanelState;
function showPanelState(stateKey, errorMsg) {
  _originalShowPanelState(stateKey);
  if (stateKey === "error" && errorMsg) {
    dom.panelErrorMsg.textContent = errorMsg;
  }
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

/*
 * ============================================================
 * FUTURE EXTENSION POINTS
 * ============================================================
 *
 * SERVICE ALERTS
 *   - Add a call to apiFetch("/api/alerts") in init()
 *   - Display results in a new banner below the header or in the panel
 *
 * ROUTE DETAIL PAGE
 *   - Create a route.html page
 *   - Add apiFetch("/api/route?serviceRef=...") to the backend
 *   - Link from the departure table row or the bus vehicle popup
 *
 * TICKETING INFO
 *   - Add a static "ticketing.html" page or a modal triggered from the header
 *   - No backend needed — this is static content from Stagecoach/Brighton Buses websites
 *
 * NEARBY STOPS
 *   - Use navigator.geolocation.getCurrentPosition() to get the user's coords
 *   - Call apiFetch(`/api/stops?lat=...&lon=...&radius=...`)  (add that filter to the backend)
 *   - Highlight the nearest 3 stops on the map
 *
 * OPERATOR FILTER
 *   - Add a checkbox UI in the header to filter bus markers by operator
 *   - The backend /api/vehicles response already includes operator_ref
 * ============================================================
 */
