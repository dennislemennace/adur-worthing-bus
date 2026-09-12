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

  // ─── Community submissions (ideas, proposals, stop issues) ───────────────
  // A small Cloudflare Worker takes submissions and files them as GitHub
  // issues, so nothing needs an account on the sender's side and every
  // submission gets a public, followable home. See worker/README.md for
  // deployment. Live since 2026-08-30.
  //
  // If you ever unset this, put the YOUR-WORKER marker back rather than
  // blanking it or leaving a plausible-looking URL. postSubmission() checks
  // for that marker and shows "not switched on yet"; a URL that merely fails
  // to resolve produces a network error instead, which tells people to try
  // again — something they can do forever without it ever working.
  SUBMIT_ENDPOINT: "https://adur-worthing-submissions.dennislemennace.workers.dev/submit",

  // Turnstile SITE key — public by design (the secret half lives in the
  // Worker). Empty means the widget is skipped, which the Worker will
  // reject, so this must be set for submissions to work in production.
  TURNSTILE_SITE_KEY: "0x4AAAAAAEh6f3QF6heuoWww",
};

// Local development: point the page at a backend you're running yourself with
//   ?api=http://localhost:8000
//
// Honoured only when the page itself came from a local or private-network
// address, AND the backend is one too. That is the property worth keeping: a
// link pasted to a real user browsing the deployed site can never redirect
// their traffic, because a public hostname fails the first test.
//
// Private LAN addresses are allowed as well as loopback so the site can be
// previewed on a phone against a laptop's backend. Reaching that case at all
// means the attacker is already serving pages inside your network.
const LOCAL_HOST_RE =
  /^(localhost|127\.0\.0\.1|\[::1\]|10\.\d+\.\d+\.\d+|192\.168\.\d+\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+\.\d+)$/i;

(function applyApiOverride() {
  try {
    if (!LOCAL_HOST_RE.test(location.hostname)) return;
    const override = new URLSearchParams(location.search).get("api");
    if (!override) return;
    let target;
    try { target = new URL(override); } catch { return; }
    if (!/^https?:$/.test(target.protocol) || !LOCAL_HOST_RE.test(target.hostname)) {
      console.warn("Ignoring ?api= override — only local backends are allowed.");
      return;
    }
    CONFIG.API_BASE_URL = override.replace(/\/$/, "");
    console.info("Using local API:", CONFIG.API_BASE_URL);
  } catch (err) {
    console.warn("API override failed:", err);
  }
})();

// ============================================================
// ICON HELPER
// ============================================================
function svgIcon(id) {
  return `<svg class="icon" aria-hidden="true"><use href="#${id}"/></svg>`;
}

// ============================================================
// STATE
// ============================================================
// One raster source for both themes. Dark mode is a CSS filter over these same
// tiles (see `html.dark-mode .leaflet-tile-pane` in style.css) rather than a
// second tile provider.
//
// CARTO's free dark basemap used to serve this and was withdrawn in August
// 2026 — it began stamping "API KEY REQUIRED" across every tile while still
// returning HTTP 200, so nothing detected it and dark mode quietly broke for
// everyone. Filtering tiles we already fetch can't be withdrawn, needs no key,
// and makes no extra requests: switching theme re-renders rather than
// re-downloading, which is also kinder to the OSM tile usage policy.
const TILES = {
  url: "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  maxZoom: 19,
};

const state = {
  map: null,
  tileLayer: null,       // active Leaflet tile layer
  darkMode: false,
  stopMarkers:   {},    // atcoCode → Leaflet marker
  stopClusters:  [],    // count bubbles shown in place of stops when zoomed out
  stopData:      {},    // atcoCode → { lat, lon }
  busMarkers:    {},    // vehicleRef → Leaflet marker
  selectedStop:  null,  // { atcoCode, name }
  refreshTimer:  null,  // setInterval handle for bus positions
  isRefreshing:  true,
  busesVisible:  true,  // header toggle: show bus markers + run live refresh
  vehicleFetchInFlight: false,  // guards against overlapping /api/vehicles polls
  // Has the backend answered anything yet this session? Until it has, calls
  // get the cold-start budget and the "waking up" banner (see apiFetch).
  apiEverResponded: false,
  stopsGeneratedOn: null,  // date stamp from the static stop list, if used
  // Operators the user has switched OFF, by NOC. A hide-list rather than a
  // show-list on purpose: an operator that appears in the feed for the first
  // time should show up, not be silently filtered out by a preference saved
  // before it existed.
  hiddenOperators: new Set(),
  busFilterOpen: false,
  lastVehicles:  [],    // last feed, so a filter change redraws without waiting

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
  viewMode:                "live", // "live" | "improvements" | "tickets" | "network" | "updates"
  updatesTab:              "official", // "official" | "community"
  officialUpdates:         null,   // data/updates.json
  communityUpdates:        null,   // data/community_updates.json
  sheetDetent:             "half", // "peek" | "half" | "full" — the mobile sheet
  improvementsTab:         "about",// "about" | "proposals" — official proposal lines only show on the Proposals tab
  serviceMode:             "day",  // "day" | "night" — splits chips, route lines, proposals
  visibleCategories:       null,   // Set holding the active type-filter key: "all" | "express" | "standard"
  visibleOperators:        null,   // Set of operator buckets ("BHBC","SCSO","COMT","OTHER",""); ""=unknown
  showLimitedServices:     false,  // false = hide services that don't run all week or finish before 18:00
  routeChipsExpanded:      false,  // "+N more" pressed; see reflowRouteChips()
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
  journeyLayers:           [],     // the checked journey, drawn on the map
  councilBoundaryLayers:   {},     // boundary id → L.polyline / L.polygon
  councilBoundaryHits:     {},     // boundary id → invisible fat L.polyline, hover/click target
  councilBoundaryLabels:   {},     // boundary id → [L.tooltip] naming each side
  _councilBoundariesPromise: null,
  ticketZoneLayers:        {},     // zone id → L.polygon (only for zones with geometry)
  ticketReachLayers:       {},     // zone id → [L.marker] reach pills (networkSAVER-style)
  selectedZoneId:          null,
  expandedOperators:       null,    // Set of operator codes whose ticket sub-cards are revealed
  ticketFaresMeta:         null,    // fares_meta: commute basis + unified-fare comparator
  _ticketZonesPromise:     null,
  _stopIndex:              null,    // stop picker: one entry per place, not per pole

  // ── Network plan view (objectives + community ideas) ──
  networkTab:              "objectives", // "objectives" | "ideas"
  expandedBodies:          null,        // Set of responsible-body codes expanded
  objectives:              null,    // data/objectives.json (array); null = not loaded
  representatives:         null,    // data/representatives.json; null = not loaded
  councillorObjective:     null,    // objective id the councillor dialog is about
  councillorBody:          null,    // which authority of that objective is being written to
  councillorResult:        null,    // rendered result HTML, kept so a re-render does not lose it
  suggestions:             null,    // data/suggestions.json (array); null = not loaded
  selectedObjectiveId:     null,    // expanded objective card in the list
  _networkPromise:         null,

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
  busFilter:          document.getElementById("bus-filter"),
  busFilterAll:       document.getElementById("bus-filter-all"),
  busFilterList:      document.getElementById("bus-filter-list"),
  darkModeBtn:        document.getElementById("dark-mode-btn"),
  departurePanel:     document.getElementById("departure-panel"),
  sheetHandle:        document.getElementById("sheet-handle"),
  tabOfficialNews:    document.getElementById("tab-official-news"),
  tabCommunityNews:   document.getElementById("tab-community-news"),
  tabContentOfficial: document.getElementById("tab-content-official-news"),
  tabContentCommunityNews: document.getElementById("tab-content-community-news"),
  officialUpdatesList:  document.getElementById("official-updates-list"),
  communityUpdatesList: document.getElementById("community-updates-list"),
  liveStatusPill:     document.getElementById("live-status-pill"),
  panelStopName:      document.getElementById("panel-stop-name"),
  stopSpan:           document.getElementById("stop-span"),
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
  reportStopBtn:      document.getElementById("report-stop-btn"),
  reportStopForm:     document.getElementById("report-stop-form"),
  reportStopStatus:   document.getElementById("rs-status"),
  reportStopSubmit:   document.getElementById("rs-submit"),
  reportStopTurnstile: document.getElementById("rs-turnstile"),
  toast:              document.getElementById("toast"),
  routeFiltersCount:  document.getElementById("route-filters-count"),
  routeChipsMore:     document.getElementById("route-chips-more"),
  routeFiltersDisc:   document.getElementById("route-filters-disclosure"),
  wakingBanner:       document.getElementById("waking-banner"),
  wakingText:         document.getElementById("waking-text"),
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

  // ── Network plan view ──
  tabObjectives:          document.getElementById("tab-objectives"),
  tabIdeas:               document.getElementById("tab-ideas"),
  tabContentObjectives:   document.getElementById("tab-content-objectives"),
  tabContentIdeas:        document.getElementById("tab-content-ideas"),
  objectivesList:         document.getElementById("objectives-list"),
  communityIdeasList:     document.getElementById("community-ideas-list"),
  suggestForm:            document.getElementById("suggest-form"),
  suggestObjective:       document.getElementById("sg-objective"),
  suggestStatus:          document.getElementById("sg-status"),
  suggestSubmit:          document.getElementById("sg-submit"),
  suggestTurnstile:       document.getElementById("sg-turnstile"),

  // ── Ticket view: boundary penalty calculator ──
  jcFrom:                 document.getElementById("jc-from"),
  jcTo:                   document.getElementById("jc-to"),
  jcFromList:             document.getElementById("jc-from-list"),
  jcToList:               document.getElementById("jc-to-list"),
  jcCheck:                document.getElementById("jc-check"),
  jcPresets:              document.getElementById("jc-presets"),
  jcStatus:               document.getElementById("jc-status"),
  jcResult:               document.getElementById("jc-result"),

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
  else if (state.viewMode === "network") parts.push("view=n");
  else if (state.viewMode === "updates") parts.push("view=u");
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
               : parsed.view === "n" ? "network"
               : parsed.view === "u" ? "updates"
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

  // Before bindUIEvents, so the bus button's label can report how many
  // operators are switched off the moment the page settles.
  loadHiddenOperators();

  initMap();
  bindUIEvents();
  bindStopSearch();
  bindRovingTabs();
  ["evidence-dialog", "councillor-dialog"]
    .forEach(id => bindDialogBackdropClose(document.getElementById(id)));
  bindNewsForm();
  bindLoaderRetries();

  // Restore any proposal drafts saved in localStorage from a previous session.
  state.editorDrafts = loadDraftsFromStorage();
  renderDraftsSection();

  // Apply the requested section *before* waiting for anything.
  //
  // This awaited loadStops() first, so opening #view=n — a panel-only
  // section with no map layers of its own — sat on "Loading live bus data…"
  // for as long as /api/stops took, and showed Live rather than the section
  // that was asked for. A campaign page a councillor has been sent must not
  // be gated on a live vehicle service.
  const urlState = parseUrlState();
  const wantsLive = !urlState.view || urlState.view === "live";
  if (!wantsLive) {
    dom.mapLoading.classList.add("hidden");
    await applyUrlState(urlState);
  }

  // Stops are needed for the Live map and for search; everything else has
  // already been shown by this point.
  const stopsLoaded = loadStops();
  if (wantsLive) await stopsLoaded;
  else stopsLoaded.catch(() => {});

  dom.mapLoading.classList.add("hidden");

  if (wantsLive) await applyUrlState(urlState);

  // Start live bus position loop
  startVehicleRefresh();

  // The council boundary is part of the Live map, so it loads with it rather
  // than waiting for the first switch into Route view.
  if (state.viewMode === "live") {
    loadCouncilBoundaries().then(reconcileCouncilBoundaries)
                           .catch(err => console.warn("Council boundary init failed:", err));
  }

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
  // Route lines and proposals are about 304 KiB — roughly a third of
  // everything the landing page transfers — and they are for a view the
  // visitor has not asked for. A `requestIdleCallback` with a 3s timeout
  // fires whether or not Live has finished, so on a throttled connection this
  // competed with the vehicle feed for the same pipe: first bus marker
  // appeared at 10.1 seconds.
  //
  // Two guards. Skip it entirely where bytes are expensive or scarce, and
  // otherwise wait until Live has actually rendered something before spending
  // them. Nothing about Route view gets slower once it is asked for — the
  // loaders are memoized, so opening the view still fetches once.
  const conn = navigator.connection;
  if (conn && (conn.saveData || /(^|-)2g$/.test(conn.effectiveType || ""))) {
    return;
  }

  const run = () => {
    loadRouteLines().catch(() => {});
    loadProposals().catch(() => {});
  };
  const whenIdle = () => {
    if ("requestIdleCallback" in window) requestIdleCallback(run, { timeout: 5000 });
    else setTimeout(run, 1200);
  };

  // "Live is useful" means vehicles have been drawn, or we have waited long
  // enough that they are not coming.
  if (state.viewMode !== "live" || state._vehiclesRendered) return whenIdle();
  let done = false;
  const go = () => { if (!done) { done = true; whenIdle(); } };
  document.addEventListener("busmarkers:first", go, { once: true });
  setTimeout(go, 8000);
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

  // Train livery images fade in when they load and disappear when they don't.
  // These were inline onload/onerror attributes on markup built by string
  // concatenation. `load` and `error` do not bubble, so the listeners are
  // registered in the capture phase on the map container — one pair for every
  // marker, rather than an executable attribute on each.
  const liveryState = (cls) => (e) => {
    const img = e.target;
    if (img && img.classList && img.classList.contains("rail-train-livery")) {
      img.classList.add(cls);
    }
  };
  const mapEl = state.map.getContainer();
  mapEl.addEventListener("load",  liveryState("is-loaded"), true);
  mapEl.addEventListener("error", liveryState("is-broken"), true);

  // Tile layer — one source for both themes; dark mode is a CSS filter.
  state.tileLayer = L.tileLayer(TILES.url, {
    attribution: TILES.attribution,
    maxZoom: TILES.maxZoom,
  }).addTo(state.map);

  // Stops are zoom-gated (see applyStopVisibility): below STOP_ZOOM_INDIVIDUAL
  // they are drawn as count bubbles instead. At the threshold itself the dots
  // come in small, so arriving at them is a fade rather than a pop.
  const applyStopDotScale = () => {
    state.map.getContainer()
      .classList.toggle("stops-far", state.map.getZoom() <= STOP_ZOOM_INDIVIDUAL);
  };
  // moveend covers panning as well as zooming, which matters because stops
  // are culled to the viewport: pan somewhere new and its stops have to
  // arrive. Leaflet fires moveend after a zoom too, so one hook does both.
  state.map.on("moveend", () => {
    applyStopDotScale();
    applyStopVisibility();
    reconcileCouncilBoundaries();   // the boundary label is zoom-gated too
  });
  applyStopDotScale();

  // Close panel when clicking an empty area of the map. Leaflet bubbles
  // marker/popup clicks up to the map 'click' event, so we need to
  // ignore anything whose DOM target is inside a marker or popup —
  // otherwise selecting a bus would immediately re-close the panel.
  // The boundary label is rebuilt by Leaflet whenever the layer is re-added,
  // so the handler lives on the container rather than on the element.
  state.map.getContainer().addEventListener("click", (e) => {
    const label = e.target.closest && e.target.closest(".council-boundary-label");
    if (!label) return;
    e.stopPropagation();
    state._ignoreNextMapClick = true;
    openBoundaryEvidence();
  });

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

// Where the stop list is published. Built from the same timetable database the
// API serves, by the same Timetable.stop_list(), in the workflow that
// publishes both — see scripts/build_stops_json.py.
//
// Relative, so it resolves against whatever origin the page was loaded from:
// GitHub Pages in production, the dev server locally. Never against the API.
const STATIC_STOPS_URL = "data/stops.json";

/**
 * Fetch the published stop list.
 *
 * Returns null rather than throwing when it is not there: a missing static
 * file is a reason to ask the API, not a failure to report. A malformed one
 * is the same — better to fall back than to draw a broken map.
 */
async function fetchStaticStops() {
  try {
    const res = await fetch(STATIC_STOPS_URL, { headers: { "Accept": "application/json" } });
    if (!res.ok) return null;
    const data = await res.json();
    if (!data || !Array.isArray(data.stops) || !data.stops.length) return null;
    return data;
  } catch (err) {
    console.warn("Static stop list unavailable:", err);
    return null;
  }
}

// The stop list is rebuilt weekly by the timetable workflow. A fortnight gives
// one missed run the benefit of the doubt; beyond that the rebuild has stopped,
// and since GitHub disables scheduled workflows in a quiet repository that is a
// thing which happens silently and stays broken for months.
const STOPS_STALE_DAYS = 14;

/** Days between `generatedOn` (an ISO date) and today, or null if unreadable. */
function stopListAgeDays(generatedOn) {
  if (!generatedOn) return null;
  const then = Date.parse(`${generatedOn}T00:00:00Z`);
  if (Number.isNaN(then)) return null;
  return Math.floor((Date.now() - then) / 86_400_000);
}

/**
 * Say so when the published stop list has gone stale.
 *
 * Deliberately quiet in the normal case — this fires only when something
 * upstream has stopped, and staying silent then is how a map ends up missing
 * a year of new stops with nobody noticing.
 */
function warnIfStopListIsStale(generatedOn) {
  const age = stopListAgeDays(generatedOn);
  if (age === null || age <= STOPS_STALE_DAYS) return;
  console.warn(`Stop list is ${age} days old (built ${generatedOn})`);
  showToast(`These stops were last rebuilt ${age} days ago — a very recent `
            + `stop may be missing from the map. Live times are unaffected.`,
            6000);
}

async function loadStops() {
  // Static first. The stop list changes when the timetable is rebuilt and not
  // otherwise, so there is nothing live about it — and asking the API for it
  // meant the map stayed empty for the twenty seconds a free-tier container
  // takes to wake up, which is the single worst moment of a first visit.
  const staticData = await fetchStaticStops();
  if (staticData) {
    state.stopsGeneratedOn = staticData.generated_on || null;
    await renderStopsInChunks(staticData.stops);
    applyStopVisibility();
    warnIfStopListIsStale(state.stopsGeneratedOn);
    return;
  }

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
    // Passenger-facing copy. "Check your API configuration" is an instruction
    // to the person who deployed this, shown to someone waiting for a bus.
    // Do not promise a fallback that depends on what just failed: the Ticket
    // view's stop picker is built from this same data, so it is empty too.
    showToast("Couldn't load the bus stops — the live service isn't "
              + "responding. Route, Network and Updates still work.");
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
  marker.bindPopup(() => buildStopPopup(stop.atco_code, stop.name),
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
    // Present only where the name is shared by more than one pole — see the
    // note in /api/stops. Their absence is the signal that the name is
    // already unambiguous.
    towards: stop.towards || "",
    services: Array.isArray(stop.services) ? stop.services : null,
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
/* ============================================================
 * ZOOM-GATING THE STOPS
 *
 * There are just over 1,500 stops in the area and every one of them used to
 * be on the map at every zoom. At the default z13 that is a carpet: it hides
 * the basemap, and in Route and Network view it hides the coloured route
 * lines, which are the entire content of those views.
 *
 * Below the threshold the stops are replaced by count bubbles. Above it they
 * are themselves. This is also what stops ~1,500 markers being live in the
 * DOM at once, because Leaflet only builds an element for a marker that is
 * actually on the map.
 * ============================================================ */
const STOP_ZOOM_INDIVIDUAL = 14;   // at or above this, draw stops one by one

/**
 * Cluster cell size in degrees.
 *
 * A flat 60px cell put a bubble every 60px across the whole coast: at the two
 * zooms where clustering first kicks in that is a curtain of numbered discs
 * with the map behind it, which is the opposite of what clustering is for. The
 * cell widens as you zoom out, so the first levels below the individual-stop
 * threshold get far fewer, fatter bubbles and the coastline is legible again.
 *
 * Beyond two levels out the cell can come back down — at zoom 11 a 120px cell
 * already spans several kilometres, and the bubbles are sparse whatever we do.
 */
const CLUSTER_CELL_PX = { 1: 150, 2: 120 };   // levels below STOP_ZOOM_INDIVIDUAL
const CLUSTER_CELL_PX_FAR = 80;

function clusterCellPixels(zoom) {
  const below = STOP_ZOOM_INDIVIDUAL - zoom;
  return CLUSTER_CELL_PX[below] || CLUSTER_CELL_PX_FAR;
}

function clusterCellDegrees(zoom) {
  return clusterCellPixels(zoom) * 360 / (256 * Math.pow(2, zoom));
}

/** Bubble diameter for a bucket of `n` stops.
 *
 * Fixed at 34px, a bubble of 4 and a bubble of 180 looked identical, so the
 * count had to be read to tell a hamlet from the middle of Worthing. Area
 * roughly tracks the count (hence the cube-ish root of n), clamped so the
 * biggest never becomes an obstacle in its own right.
 *
 * Scaled to about a third of the first version at the owner's request: the
 * bubbles are a hint that stops are there, not a thing to read, and at 34-64px
 * they were still the loudest object on the map. 12-22px keeps the size
 * difference between a hamlet and a town centre legible while letting the
 * coastline through.
 *
 * Note these fall below the 24px pointer-target guideline the rest of the site
 * keeps. That is deliberate and already true of the individual stop dots
 * (12px): the map is not the only way in, and the keyboard/assistive route is
 * the stop search, which is exactly why it exists. */
const CLUSTER_SCALE = 0.35;
function clusterDiameter(n) {
  return Math.max(12, Math.min(22,
    Math.round((30 + 11 * Math.cbrt(n)) * CLUSTER_SCALE)));
}

function clearStopClusters() {
  for (const m of state.stopClusters) state.map.removeLayer(m);
  state.stopClusters = [];
}

function renderStopClusters(atcos) {
  clearStopClusters();
  const cell = clusterCellDegrees(state.map.getZoom());
  const buckets = new Map();

  for (const atco of atcos) {
    const d = state.stopData[atco];
    if (!d) continue;
    const key = `${Math.floor(d.lat / cell)}:${Math.floor(d.lon / cell)}`;
    let b = buckets.get(key);
    if (!b) { b = { lat: 0, lon: 0, n: 0 }; buckets.set(key, b); }
    b.lat += d.lat; b.lon += d.lon; b.n++;
  }

  for (const b of buckets.values()) {
    const at = [b.lat / b.n, b.lon / b.n];
    const d = clusterDiameter(b.n);
    const marker = L.marker(at, {
      icon: L.divIcon({
        // iconSize alone sizes the disc — .stop-cluster-icon is the circle, so
        // a nested sized box would just be a second one inside it.
        className: `stop-cluster-icon${b.n >= 50 ? " stop-cluster-icon--big" : ""}`,
        html: `<span>${b.n}</span>`,
        iconSize: [d, d],
        iconAnchor: [d / 2, d / 2],
      }),
      // Not a destination in its own right — it exists to be zoomed into.
      keyboard: false,
      title: `${b.n} stop${b.n === 1 ? "" : "s"} — zoom in to see them`,
    });
    marker.on("click", () => {
      state.map.setView(at, Math.max(state.map.getZoom() + 2, STOP_ZOOM_INDIVIDUAL));
    });
    marker.addTo(state.map);
    state.stopClusters.push(marker);
  }
}

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

  // Cull to what is actually on screen, padded so a small pan does not
  // reveal a hole. Leaflet builds no DOM for a marker that is not on the
  // map, so this is what keeps the node count proportional to the view
  // rather than to the size of the dataset.
  const bounds = state.map.getBounds().pad(0.3);

  const wanted = [];
  for (const atco in state.stopMarkers) {
    const data = state.stopData[atco] || {};
    if (filterToNight && !(data.night_serving || overrideShow.has(atco))) continue;
    if (data.lat === undefined || !bounds.contains([data.lat, data.lon])) continue;
    wanted.push(atco);
  }

  // The editor is the exception: every stop there is a thing you click, so
  // collapsing them into bubbles would make it unusable at a working zoom.
  const clustered = !editorOpen && state.map.getZoom() < STOP_ZOOM_INDIVIDUAL;

  if (clustered) {
    for (const atco in state.stopMarkers) {
      const marker = state.stopMarkers[atco];
      if (state.map.hasLayer(marker)) state.map.removeLayer(marker);
    }
    renderStopClusters(wanted);
    return;
  }

  clearStopClusters();
  const show = new Set(wanted);
  for (const atco in state.stopMarkers) {
    const marker = state.stopMarkers[atco];
    const has = state.map.hasLayer(marker);
    if (show.has(atco) && !has)      state.map.addLayer(marker);
    else if (!show.has(atco) && has) state.map.removeLayer(marker);
  }
}

/* ============================================================
 * COUNCIL BOUNDARY + CONNECTION GAP (Route view)
 *
 * Note the name. "Boundary" already means the *fare* seam everywhere else in
 * this file, so anything administrative is spelled out as councilBoundary to
 * keep the two apart.
 * ============================================================ */
function loadCouncilBoundaries() {
  if (!state._councilBoundariesPromise) {
    state._councilBoundariesPromise = loadCouncilBoundariesImpl()
      .catch(err => { state._councilBoundariesPromise = null; throw err; });
  }
  return state._councilBoundariesPromise;
}

async function loadCouncilBoundariesImpl() {
  if (Object.keys(state.councilBoundaryLayers).length) return;
  let data;
  try {
    const res = await fetch("data/council_boundaries.json");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    data = await res.json();
  } catch (err) {
    // A missing overlay is not worth breaking the view for.
    console.warn("Council boundaries unavailable:", err);
    return;
  }

  if (!state.map.getPane("councilBoundaryPane")) {
    const pane = state.map.createPane("councilBoundaryPane");
    // Below the ticket zones (404) and below route lines' own pane, so it
    // reads as a backdrop rather than competing with the network it explains.
    pane.style.zIndex = 402;
  }
  if (!state.map.getPane("councilBoundaryLabelPane")) {
    const pane = state.map.createPane("councilBoundaryLabelPane");
    // The label is the opposite case from the line: a line can afford to sit
    // underneath things, but a label nobody can read explains nothing. Above
    // markers (600), below tooltips and popups (650/700), and pointer-events
    // off in CSS so it never intercepts a click meant for a bus.
    pane.style.zIndex = 620;
    pane.style.pointerEvents = "none";
  }

  for (const b of (data.boundaries || [])) {
    const colour = bodyColour((b.bodies || [])[0]) || "var(--color-text-muted)";
    if (b.kind !== "line" || !Array.isArray(b.polyline)) continue;
    const st = b.style || {};
    state.councilBoundaryLayers[b.id] = L.polyline(b.polyline, {
      color:     colour,
      weight:    st.weight  ?? 5,
      opacity:   st.opacity ?? 0.9,
      dashArray: st.dashArray ?? "12 7",
      lineCap:   "round",
      interactive: false,
      pane: "councilBoundaryPane",
      className: "council-boundary-line",
    });

    // A second line, invisible and fat, doing the pointer work. The visible
    // boundary is a 5px dash — under half the 24px WCAG target floor, and in
    // practice unhittable — so the line looked inert even though it is the
    // way into the evidence. Kept separate from the visible line rather than
    // widening it, because the dash pattern is the thing that says "this is
    // an administrative edge, not a route".
    state.councilBoundaryHits[b.id] = L.polyline(b.polyline, {
      color:   colour,
      weight:  22,
      opacity: 0,
      interactive: true,
      bubblingMouseEvents: false,
      pane: "councilBoundaryPane",
      className: "council-boundary-hit",
    });
    bindCouncilBoundaryHover(b.id);

    state.councilBoundaryLabels[b.id] = (b.label_at || [])
      .filter(at => Array.isArray(at) && at.length === 2)
      .map(at => L.tooltip({
        permanent:   true,
        direction:   "center",
        opacity:     1,
        interactive: false,
        className:   "council-boundary-tooltip",
        pane:        "councilBoundaryLabelPane",
      }).setLatLng(at).setContent(councilBoundaryLabelHtml(b)));
  }
}

/**
 * Make the boundary react to the cursor along its whole length.
 *
 * Hover lights the line *and* its label, so the two read as one object rather
 * than a decoration that happens to sit near a button. Click goes to the same
 * place the label goes — the evidence — because a line that highlights under
 * the cursor and then does nothing when clicked is worse than one that never
 * highlighted.
 *
 * This adds no focusable element: the label is already a real <button>, and it
 * remains the keyboard route in. The hit line is a pointer affordance only.
 */
function bindCouncilBoundaryHover(id) {
  const hit = state.councilBoundaryHits[id];
  if (!hit) return;

  const setHot = (on) => {
    const line = state.councilBoundaryLayers[id];
    if (line && line._path) line._path.classList.toggle("is-hot", on);
    // The label is rebuilt by Leaflet each time the tooltip is re-added, so
    // it is looked up per event rather than captured once.
    document.querySelectorAll(`.council-boundary-label[data-boundary="${cssEscape(id)}"]`)
      .forEach(el => el.classList.toggle("is-hot", on));
  };

  hit.on("mouseover", () => setHot(true));
  hit.on("mouseout",  () => setHot(false));
  hit.on("click", () => {
    // Leaflet forwards layer clicks to the map, whose handler closes the
    // panel. Same guard the label's own handler uses.
    state._ignoreNextMapClick = true;
    setHot(false);        // a tap never fires mouseout, so it would stay lit
    openBoundaryEvidence();
  });
}

/** CSS.escape where it exists, and a conservative fallback where it doesn't. */
function cssEscape(value) {
  const s = String(value);
  return (window.CSS && typeof CSS.escape === "function")
    ? CSS.escape(s)
    : s.replace(/[^a-zA-Z0-9_-]/g, ch => "\\" + ch);
}

/**
 * The label that says what the dashed line is.
 *
 * A boundary drawn without a name is just a line on a map. Naming both sides
 * — and putting them left and right, the way they sit on the ground — is what
 * turns it into the point being made: two authorities, one coast.
 */
function councilBoundaryLabelHtml(boundary) {
  const sides = boundary.sides || {};
  const side = (entry) => {
    if (!entry || !entry.body) return "";
    // The short place name, not the council's full title: this is read at a
    // glance over a moving map, and "Brighton & Hove City Council" is three
    // times the width of the thing it is naming.
    const text = entry.label || bodyName(entry.body);
    return `<span class="council-boundary-side" style="--side-colour:${escapeAttr(bodyColour(entry.body))}">`
         + `${escapeHtml(text)}</span>`;
  };
  const caption = boundary.label_caption
    ? `<span class="council-boundary-caption">${escapeHtml(boundary.label_caption)}</span>` : "";
  // A button, not a span: the label is the way into the evidence behind the
  // line, and a div with a click handler is unreachable by keyboard and
  // unannounced to a screen reader. The label pane sets pointer-events: none
  // so it never swallows a click meant for a bus; this element opts back in.
  return `<button type="button" class="council-boundary-label"
                  data-boundary="${escapeAttr(boundary.id || "")}"
                  aria-label="What this boundary means — bus service either side">`
       + `${side(sides.west)}<span class="council-boundary-tick" aria-hidden="true"></span>${side(sides.east)}`
       + `${caption}</button>`;
}

// ============================================================
// BOUNDARY EVIDENCE
//
// The line on the map states the campaign's central claim; this is what backs
// it. Figures come from data/boundary_evidence.json, recomputed by
// scripts/build_evidence.py on every timetable rebuild, and are shown with
// their method, their data version and their caveats — including the one that
// weakens the case, which is the one worth showing first.
// ============================================================
function loadBoundaryEvidence() {
  if (!state._boundaryEvidencePromise) {
    state._boundaryEvidencePromise = (async () => {
      const res = await fetch("data/boundary_evidence.json");
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res.json();
    })().catch(err => {
      state._boundaryEvidencePromise = null;
      throw err;
    });
  }
  return state._boundaryEvidencePromise;
}

// Saturday and Sunday are published separately in data/boundary_evidence.json
// and shown as one panel here. Two weekend panels said much the same thing
// twice and left no room for the comparison that makes the case concrete; the
// weekend note carries both days' route counts, which is the part an average
// would have flattened.
const EVIDENCE_DAYS = [
  ["monday",  "Weekday"],
  ["weekend", "Weekend"],
];

async function openBoundaryEvidence() {
  const dialog = document.getElementById("evidence-dialog");
  const body   = document.getElementById("evidence-body");
  if (!dialog || !body) return;

  body.innerHTML = `<p class="evidence-loading">Loading the figures…</p>`;
  if (typeof dialog.showModal === "function") dialog.showModal();
  else dialog.setAttribute("open", "");

  let data;
  try {
    data = await loadBoundaryEvidence();
  } catch (err) {
    console.warn("Boundary evidence unavailable:", err);
    body.innerHTML = `
      <button type="button" class="evidence-close" data-close-evidence aria-label="Close">&times;</button>
      <h2 class="evidence-title" id="evidence-title">Bus service either side of the boundary</h2>
      <p class="journey-note">We can't load the figures right now. They are
      rebuilt weekly from the published timetable — try again shortly.</p>`;
    wireEvidenceClose(dialog, body);
    return;
  }
  body.innerHTML = renderBoundaryEvidence(data);
  wireEvidenceClose(dialog, body);
}

function wireEvidenceClose(dialog, body) {
  body.querySelectorAll("[data-close-evidence]").forEach(btn => {
    btn.addEventListener("click", () => {
      if (typeof dialog.close === "function") dialog.close();
      else dialog.removeAttribute("open");
    });
  });
}

/**
 * Render the comparison.
 *
 * Bars are drawn from zero and carry their own number, because a truncated axis
 * turns "two thirds" into a visual wipeout and would overstate a case that does
 * not need overstating. Nothing here is conveyed by colour alone.
 */
function renderBoundaryEvidence(data) {
  const days = data.days || {};
  const west = (data.sides && data.sides.west) || "West Sussex";
  const east = (data.sides && data.sides.east) || "Brighton & Hove";

  // One scale across every row, so bars are comparable between days as well as
  // between sides.
  const places    = data.places || null;
  const placeDay  = places && places.days ? places.days.monday : null;

  let max = 0;
  const scaleAgainst = d => {
    if (!d) return;
    max = Math.max(max, d.west.departures_per_stop, d.east.departures_per_stop);
  };
  for (const [key] of EVIDENCE_DAYS) scaleAgainst(days[key]);
  scaleAgainst(placeDay);
  if (!max) max = 1;

  const bar = (label, value, cls) => {
    const pct = Math.max(2, (value / max) * 100);
    return `
      <div class="evidence-bar-row">
        <span class="evidence-bar-label">${escapeHtml(label)}</span>
        <span class="evidence-bar-track">
          <span class="evidence-bar ${cls}" style="width:${pct.toFixed(1)}%"></span>
        </span>
        <span class="evidence-bar-value">${value.toFixed(1)}</span>
      </div>`;
  };

  const rows = EVIDENCE_DAYS.map(([key, label]) => {
    const d = days[key];
    if (!d) return "";
    const ratio = d.ratio && d.ratio.departures_per_stop;
    return `
      <section class="evidence-day">
        <h3 class="evidence-day-title">
          ${escapeHtml(label)}
          ${ratio != null ? `<span class="evidence-day-ratio">${Math.round(ratio * 100)}% of the Brighton side</span>` : ""}
        </h3>
        ${bar(west, d.west.departures_per_stop, "evidence-bar--west")}
        ${bar(east, d.east.departures_per_stop, "evidence-bar--east")}
        <p class="evidence-day-note">
          <strong>${d.west.routes}</strong> routes west of the line against
          <strong>${d.east.routes}</strong> east${
            // An averaged weekend would hide Sunday being the thinner day, so
            // the panel that averages it says what it averaged.
            d.west.routes_sunday != null
              ? ` — on Sunday alone, <strong>${d.west.routes_sunday}</strong> against <strong>${d.east.routes_sunday}</strong>`
              : ""}
          ${d.west.departures_after_2300 != null
            ? ` · after 23:00, <strong>${d.west.departures_after_2300}</strong> departures against <strong>${d.east.departures_after_2300}</strong>`
            : ""}.
        </p>
      </section>`;
  }).join("");

  // The band measures the line's average effect; this measures what it means
  // in one place, which is the form the argument takes when it reaches a
  // resident. Boundaries are ONS ward and division polygons, not a distance
  // band, so the two are different geographies and the panel says so.
  const placeSection = (() => {
    if (!places || !placeDay) return "";
    const sunday = places.days.sunday;
    const ratio  = placeDay.ratio && placeDay.ratio.departures_per_stop;
    return `
      <section class="evidence-day evidence-day--place">
        <p class="evidence-place-eyebrow">One place either side of the line</p>
        <h3 class="evidence-day-title">
          ${escapeHtml(places.west.name)} against ${escapeHtml(places.east.name)}
          ${ratio != null ? `<span class="evidence-day-ratio">${Math.round(ratio * 100)}% of ${escapeHtml(places.east.name)}</span>` : ""}
        </h3>
        ${bar(places.west.name, placeDay.west.departures_per_stop, "evidence-bar--west")}
        ${bar(places.east.name, placeDay.east.departures_per_stop, "evidence-bar--east")}
        <p class="evidence-day-note">
          Weekday departures per stop, six miles apart along the same coast road.
          ${escapeHtml(places.west.name)} has <strong>${placeDay.west.routes}</strong>
          routes across <strong>${places.west.stops}</strong> stops against
          <strong>${placeDay.east.routes}</strong> across
          <strong>${places.east.stops}</strong>${
            // The point is frequency, not choice — but only while the two route
            // counts really are close. If a rebuild pulls them apart the clause
            // drops out rather than becoming a claim the figures contradict.
            placeDay.ratio && placeDay.ratio.routes >= 0.8
              ? " — much the same choice of route" : ""}${
            ratio != null ? `, and <strong>${Math.round(ratio * 100)}%</strong> of the buses` : ""}${sunday
            ? `. On a Sunday it is <strong>${sunday.west.routes}</strong> routes against <strong>${sunday.east.routes}</strong>`
            : ""}.
        </p>
      </section>`;
  })();

  const caveats = (data.caveats || []).map(c => `
    <li class="evidence-caveat evidence-caveat--${escapeAttr(c.direction || "unknown")}">
      <span class="evidence-caveat-tag">${escapeHtml(
        c.direction === "understates" ? "Understates the gap"
        : c.direction === "overstates" ? "Overstates the gap"
        : "Direction unknown")}</span>
      ${escapeHtml(c.text)} ${c.effect ? `<em>${escapeHtml(c.effect)}</em>` : ""}
    </li>`).join("");

  const method = data.method || {};
  return `
    <button type="button" class="evidence-close" data-close-evidence aria-label="Close">&times;</button>
    <p class="evidence-eyebrow">Council boundary</p>
    <h2 class="evidence-title" id="evidence-title">${escapeHtml(
      data.headline || "Bus service either side of the boundary")}</h2>
    <p class="evidence-standfirst">
      Scheduled departures <strong>per stop</strong>, in a
      ${escapeHtml(String((method.band && method.band.approx_half_width_km) || 4))} km band
      either side of the line along the same coastal strip — so the two sides are
      the same kind of place. The last panel leaves the band behind and compares
      two named places, one each side.
    </p>
    <div class="evidence-days">${rows}${placeSection}</div>
    <p class="evidence-axis">
      Bars start at zero and share one scale across every panel —
      0 to ${max.toFixed(1)} departures per stop, per day.
    </p>
    <details class="evidence-method">
      <summary>How this was worked out</summary>
      <p>${escapeHtml(method.summary || "")}</p>
      ${method.denominator ? `<p>${escapeHtml(method.denominator)}</p>` : ""}
      ${Array.isArray(method.steps) ? `<ol>${method.steps.map(x =>
        `<li>${escapeHtml(x)}</li>`).join("")}</ol>` : ""}
      ${caveats ? `<p class="evidence-caveats-title">What would make this wrong</p>
        <ul class="evidence-caveats">${caveats}</ul>` : ""}
      <p class="evidence-provenance">
        From <span class="mono">${escapeHtml((data.data_version || {}).artefact || "the timetable")}</span>,
        built from ${escapeHtml((data.data_version || {}).source || "the published timetable")}.
        Computed ${escapeHtml(data.as_of || "—")}${
          (data.data_version || {}).sha256
            ? ` · data <span class="mono">${escapeHtml(data.data_version.sha256.slice(0, 12))}</span>` : ""}.
        Recomputed by <span class="mono">${escapeHtml(method.script || "scripts/build_evidence.py")}</span>
        on every timetable rebuild.
      </p>
    </details>`;
}

// Below this the line is a few pixels long and the label would be shouting
// over a coastline nobody can make out yet.
const COUNCIL_LABEL_MIN_ZOOM = 11;

/**
 * The boundary shows on the two views that have a map worth putting it on:
 * Route, where it explains the shape of the network, and Live, where it
 * explains why the buses you are watching stop where they do.
 */
function reconcileCouncilBoundaries() {
  if (!state.map) return;
  const show = state.viewMode === "improvements" || state.viewMode === "live";
  // The hit line is reconciled with the line it belongs to, not separately:
  // one left behind is an invisible strip of map that opens a dialog.
  for (const layer of [...Object.values(state.councilBoundaryLayers),
                       ...Object.values(state.councilBoundaryHits)]) {
    if (show && !state.map.hasLayer(layer))      layer.addTo(state.map);
    else if (!show && state.map.hasLayer(layer)) state.map.removeLayer(layer);
  }
  const labelled = show && state.map.getZoom() >= COUNCIL_LABEL_MIN_ZOOM;
  for (const tips of Object.values(state.councilBoundaryLabels)) {
    for (const tip of tips) {
      if (labelled && !state.map.hasLayer(tip))      tip.addTo(state.map);
      else if (!labelled && state.map.hasLayer(tip)) state.map.removeLayer(tip);
    }
  }
}

function hideCouncilBoundaries() {
  if (!state.map) return;
  for (const layer of [...Object.values(state.councilBoundaryLayers),
                       ...Object.values(state.councilBoundaryHits)]) {
    if (state.map.hasLayer(layer)) state.map.removeLayer(layer);
  }
  for (const tips of Object.values(state.councilBoundaryLabels)) {
    for (const tip of tips) {
      if (state.map.hasLayer(tip)) state.map.removeLayer(tip);
    }
  }
}


/** Popup body for a stop — built on demand (see renderStopMarker).
 *
 * Returns an element, not a string. Leaflet accepts either, and an element
 * cannot be built by concatenation: the stop name goes in as a text node and
 * the button carries a real listener, so there is no attribute for a quote to
 * close and no JavaScript context for an entity to decode into. The previous
 * version interpolated the name into an `onclick`, which broke every stop with
 * an apostrophe in its name and made a crafted one executable.
 */
function buildStopPopup(atcoCode, name) {
  const wrap = document.createElement("div");

  const title = document.createElement("p");
  title.className = "popup-stop-name";
  title.textContent = name;

  const id = document.createElement("p");
  id.className = "popup-stop-id";
  id.textContent = `Stop: ${atcoCode}`;

  const btn = document.createElement("button");
  btn.className = "popup-btn";
  btn.type = "button";
  btn.innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-clock"/></svg>'
                + "<span>Live departures</span>";
  btn.addEventListener("click", () => { openDepartures(atcoCode, name); });

  wrap.appendChild(title);
  wrap.appendChild(id);
  wrap.appendChild(btn);
  return wrap;
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
  const hidden = state.hiddenOperators.size;
  // The button opens the filter, so aria-expanded is what it owns. Whether
  // buses are on is a property of the filter's contents, said in the label.
  dom.toggleBusesBtn.setAttribute("aria-expanded", state.busFilterOpen ? "true" : "false");
  dom.toggleBusesBtn.classList.toggle("is-off", !on);
  dom.toggleBusesBtn.classList.toggle("is-filtered", on && hidden > 0);
  const label = !on
    ? "Buses hidden — open bus filter"
    : hidden
      ? `Bus filter — ${hidden} operator${hidden === 1 ? "" : "s"} hidden`
      : "Bus filter";
  dom.toggleBusesBtn.setAttribute("aria-label", label);
  dom.toggleBusesBtn.title = label;
  if (dom.busFilterAll) dom.busFilterAll.checked = on;
}

// ============================================================
// BUS OPERATOR FILTER
//
// The map can carry two hundred buses at once, and most of the time somebody
// watching it cares about one company's. The bus button in the status pill
// opens this: a master switch that still turns everything off in one click,
// and a checkbox per operator actually present in the feed.
//
// Choices persist, because a filter you have to re-apply on every visit is
// one you stop using.
// ============================================================
const HIDDEN_OPERATORS_KEY = "hiddenOperators";

function loadHiddenOperators() {
  try {
    const raw = localStorage.getItem(HIDDEN_OPERATORS_KEY);
    const list = raw ? JSON.parse(raw) : [];
    if (Array.isArray(list)) state.hiddenOperators = new Set(list.filter(x => typeof x === "string"));
  } catch (err) {
    console.warn("Could not read the bus filter:", err);
  }
}

function saveHiddenOperators() {
  try {
    localStorage.setItem(HIDDEN_OPERATORS_KEY, JSON.stringify([...state.hiddenOperators]));
  } catch (err) {
    // Private browsing, quota, a locked-down profile — none of it is worth
    // losing the filter the user just set, which still applies this session.
    console.warn("Could not save the bus filter:", err);
  }
}

/** Is this vehicle's operator switched on? */
function operatorShown(operatorRef) {
  return !state.hiddenOperators.has(operatorRef || "");
}

function setBusFilterOpen(open) {
  state.busFilterOpen = !!open;
  if (dom.busFilter) dom.busFilter.classList.toggle("hidden", !state.busFilterOpen);
  updateBusesToggleBtn();
  if (state.busFilterOpen) renderBusFilter();
}

/** Flip one operator on or off and redraw from the feed already in hand. */
function setOperatorShown(operatorRef, shown) {
  const code = operatorRef || "";
  if (shown) state.hiddenOperators.delete(code);
  else       state.hiddenOperators.add(code);
  saveHiddenOperators();
  // Unhiding an operator while everything is off would leave the user
  // checking a box and seeing nothing happen.
  if (shown && !state.busesVisible) setBusesVisible(true);
  else {
    updateVehicleMarkers(state.lastVehicles);
    updateBusesToggleBtn();
    renderBusFilter();
  }
}

/**
 * The operators to offer.
 *
 * Everything in the current feed, busiest first, plus anything the user has
 * switched off that has since left the feed — otherwise turning an operator
 * off at midnight would remove the only way to turn it back on.
 */
function busFilterOperators() {
  const counts = new Map();
  for (const v of (state.lastVehicles || [])) {
    const code = v.operator_ref || "";
    counts.set(code, (counts.get(code) || 0) + 1);
  }
  for (const code of state.hiddenOperators) {
    if (!counts.has(code)) counts.set(code, 0);
  }
  return [...counts.entries()]
    .sort((a, b) => (b[1] - a[1]) || getOperatorName(a[0]).localeCompare(getOperatorName(b[0])))
    .map(([code, count]) => ({ code, count }));
}

function renderBusFilter() {
  if (!dom.busFilterList) return;
  const rows = busFilterOperators();
  if (!rows.length) {
    dom.busFilterList.innerHTML =
      `<p class="bus-filter-empty">No buses in the feed right now.</p>`;
    return;
  }
  dom.busFilterList.innerHTML = rows.map(({ code, count }) => {
    const on = operatorShown(code);
    const name = code ? getOperatorName(code) : "Unknown operator";
    return `
      <label class="bus-filter-row">
        <input type="checkbox" data-operator="${escapeAttr(code)}" ${on ? "checked" : ""}>
        <span class="bus-filter-swatch" aria-hidden="true"
              style="background:${escapeAttr(getOperatorColour(code))}"></span>
        <span class="bus-filter-name">${escapeHtml(name)}</span>
        <span class="bus-filter-count">${count}</span>
      </label>`;
  }).join("");
  dom.busFilterList.querySelectorAll("input[data-operator]").forEach(input => {
    input.addEventListener("change", () => {
      setOperatorShown(input.dataset.operator, input.checked);
    });
  });
}

async function fetchVehicles() {
  // A response slower than the 20s interval used to leave two requests in the
  // air, racing to write the same markers. The flag is enough — an
  // AbortController would let us cancel the older one, but the newer answer is
  // the one we want either way, so there is nothing to cancel *to*.
  if (state.vehicleFetchInFlight) return;
  state.vehicleFetchInFlight = true;
  try {
    const data = await apiFetch("/api/vehicles");
    if (!data || !data.vehicles) return;
    // Kept whatever the view, so toggling an operator redraws immediately
    // rather than at the next refresh, and so the filter can list operators
    // with the counts actually in the feed.
    state.lastVehicles = data.vehicles;
    if (state.busFilterOpen) renderBusFilter();

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
  } finally {
    state.vehicleFetchInFlight = false;
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
    // A filtered-out operator is left out of `seenRefs` so the sweep at the
    // bottom removes any marker it already had — the same path a bus takes
    // when it drops out of the feed. The bus being followed is the exception:
    // hiding the thing the user asked to be shown helps nobody.
    if (!operatorShown(vehicle.operator_ref) && ref !== state.selectedVehicleRef) return;
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
      const name = busMarkerName(vehicle, label);
      if (marker.options.title !== name) {
        marker.options.title = name;
        const el = marker.getElement && marker.getElement();
        if (el) el.setAttribute("title", name);
      }
    } else {
      const icon = createBusIcon(vehicle.operator_ref, label, bearing);
      // A name a screen reader can tell apart. Every bus announced only its
      // route number, so eleven vehicles on the 700 were eleven identical
      // buttons. Destination and operator are what distinguish them, where
      // the feed supplies them — and where it does not, the number alone is
      // still the honest answer rather than an invented one.
      marker = L.marker([vehicle.latitude, vehicle.longitude],
                        { icon, zIndexOffset: 200, title: busMarkerName(vehicle, label) })
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

    if (!state._vehiclesRendered) {
      state._vehiclesRendered = true;
      document.dispatchEvent(new Event("busmarkers:first"));
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
    ? `<img class="bus-popup-icon" src="${escapeAttr(safeUrl(iconUrl))}" alt="">`
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
/** What a bus marker is called, for anything that reads names rather than art. */
function busMarkerName(vehicle, label) {
  const parts = [`Service ${label || "?"}`];
  const dest = prettifyName(vehicle.destination || vehicle.trip_headsign);
  if (dest) parts.push(`to ${dest}`);
  const op = getOperatorName(vehicle.operator_ref);
  if (op && op !== "Unknown operator") parts.push(`(${op})`);
  return parts.join(" ");
}

function createBusIcon(operatorRef, label, bearing) {
  const iconUrl  = iconForService(operatorRef, label);
  const transform = iconTransformForBearing(bearing);

  let inner;
  if (iconUrl) {
    inner = `
      <img class="bus-icon-img" src="${escapeAttr(safeUrl(iconUrl))}" alt=""
           width="56" height="56" decoding="async"
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

  const changedStop = !state.selectedStop || state.selectedStop.atcoCode !== atcoCode;
  state.selectedStop = { atcoCode, stopName };
  pushUrlState();

  // A report form left open from the previous stop would submit against the
  // new one — close it, and clear anything half-typed with it.
  if (changedStop && dom.reportStopForm) {
    dom.reportStopForm.reset();
    toggleReportStopForm(false);
  }

  // Update panel header
  dom.panelStopName.textContent = stopName;
  dom.panelStopId.textContent   = `ATCO: ${atcoCode}`;

  // Make sure the Stop tab is the one in front
  setActiveTab("stop");

  revealPanelForSelection();

  // Show panel, hide prompt
  showPanelState("loading");

  // On mobile, scroll down so the panel is visible
  dom.departurePanel.scrollIntoView({ behavior: "smooth", block: "end" });

  // Span and departures are independent: the span is a local timetable read
  // and answers even when live predictions are quota-paused, so it is not
  // chained behind them.
  fetchStopSpan(atcoCode);
  await fetchDepartures(atcoCode);
};

// ============================================================
// SERVICE SPAN — when buses actually run from a stop
//
// The departure board answers "what is next". It cannot answer the question
// this network turns on: is there a bus here in the evening, or on a Sunday at
// all? A peak-only stop with no weekend service looks identical on a Tuesday
// morning to one served every ten minutes until midnight. This is the part of
// the case that a live board structurally cannot show.
// ============================================================
const SPAN_DAYS = [
  ["monday",    "Mon"], ["tuesday",  "Tue"], ["wednesday", "Wed"],
  ["thursday",  "Thu"], ["friday",   "Fri"], ["saturday",  "Sat"],
  ["sunday",    "Sun"],
];

async function fetchStopSpan(atcoCode) {
  if (!dom.stopSpan) return;
  dom.stopSpan.classList.add("hidden");
  dom.stopSpan.innerHTML = "";
  try {
    let url = `/api/stop-span?stopId=${encodeURIComponent(atcoCode)}`;
    const pos = state.stopData[atcoCode];
    if (pos) url += `&lat=${pos.lat}&lon=${pos.lon}`;
    const data = await apiFetch(url);
    // A stop the user has since navigated away from must not paint over the
    // one they are looking at now. The old test was `selectedStop &&
    // selectedStop.atcoCode !== atcoCode`, which is false when `selectedStop`
    // is null — exactly the state selecting a rail station leaves behind — so
    // the span rendered anyway.
    if (!state.selectedStop || state.selectedStop.atcoCode !== atcoCode) return;
    renderStopSpan(data);
  } catch (err) {
    // A missing span is a missing extra, not a broken panel. The departure
    // board is the primary content and is fetched separately.
    console.warn("Stop span unavailable:", err);
  }
}

/**
 * Collapse the week into the runs a timetable would print.
 *
 * Seven identical weekday rows is noise; "Mon–Fri" is what a passenger reads.
 * Consecutive days whose service is the same are merged, so the rows that
 * remain are the ones that differ — which is the finding.
 */
function groupSpanDays(span) {
  const key = (v) => v
    ? `${v.first}|${v.last}|${v.routes.join(",")}`
    : "none";
  const runs = [];
  for (const [day, label] of SPAN_DAYS) {
    const value = span[day] || null;
    const k = key(value);
    const last = runs[runs.length - 1];
    if (last && last.key === k) {
      last.to = label;
    } else {
      runs.push({ key: k, from: label, to: label, value });
    }
  }
  return runs.map(r => ({
    label: r.from === r.to ? r.from : `${r.from}–${r.to}`,
    value: r.value,
  }));
}

function renderStopSpan(data) {
  if (!dom.stopSpan) return;
  const span = (data && data.span) || {};
  const runs = groupSpanDays(span);
  if (!runs.length || runs.every(r => !r.value)) {
    // No service on any day is itself worth saying, and is not an error.
    dom.stopSpan.innerHTML =
      `<p class="stop-span-none">No scheduled service from this stop.</p>`;
    dom.stopSpan.classList.remove("hidden");
    return;
  }

  const rows = runs.map(r => {
    if (!r.value) {
      return `
        <tr class="stop-span-row stop-span-row--none">
          <th scope="row">${escapeHtml(r.label)}</th>
          <td colspan="2">
            <span class="stop-span-nil">No service</span>
          </td>
        </tr>`;
    }
    const night = r.value.night
      ? `<span class="stop-span-night" title="Departures before ${escapeAttr(data.night_before || "04:00")}">+${r.value.night} night</span>`
      : "";
    return `
      <tr class="stop-span-row">
        <th scope="row">${escapeHtml(r.label)}</th>
        <td class="stop-span-hours">
          <span class="stop-span-time">${escapeHtml(r.value.first)}</span>
          <span class="stop-span-dash" aria-hidden="true">–</span>
          <span class="stop-span-time">${escapeHtml(r.value.last)}</span>
          ${night}
        </td>
        <td class="stop-span-routes">${r.value.routes.map(rt =>
          `<span class="stop-span-route">${escapeHtml(rt)}</span>`).join("")}</td>
      </tr>`;
  }).join("");

  dom.stopSpan.innerHTML = `
    <details class="stop-span-disclosure">
      <summary class="stop-span-summary">
        <svg class="icon" aria-hidden="true" style="width:15px;height:15px"><use href="#i-info"/></svg>
        <span>When buses run here</span>
        <svg class="icon stop-span-chevron" aria-hidden="true" style="width:14px;height:14px"><use href="#i-chevron-down"/></svg>
      </summary>
      <table class="stop-span-table">
        <caption class="stop-span-caption">
          First and last scheduled departure, from the published timetable.
        </caption>
        <thead>
          <tr><th scope="col">Days</th><th scope="col">Service</th><th scope="col">Routes</th></tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </details>`;
  dom.stopSpan.classList.remove("hidden");
}

/**
 * Async ownership.
 *
 * Several loaders write into the same places — the departure panel is shared
 * by the bus board and the rail board, the journey box by every check — and a
 * response that arrives after the reader has moved on has no right to render.
 * Nothing enforced that, so a slow bus board painted itself under a rail
 * station's heading in production, 19 seconds after the reader had asked for
 * something else.
 *
 * `claimRender(slot)` records that this request is now the one that owns
 * `slot`, and returns a predicate to ask before rendering. The predicate is
 * false once a later request has claimed the same slot, or once the reader has
 * changed view — a result belonging to a section they have left is as wrong as
 * one belonging to a stop they have left.
 *
 * Callers check it on success *and* on failure: an obsolete request's error is
 * no more entitled to replace a good board than its data is.
 *
 * This does not cancel anything. Cancellation would save a little bandwidth
 * and cannot be relied on for correctness — a request already past the wire is
 * still going to resolve — so the guard is the mechanism and cancellation, if
 * it is ever added, is an optimisation on top.
 */
const renderClaims = Object.create(null);
let renderClaimSeq = 0;

function claimRender(slot) {
  const token = ++renderClaimSeq;
  const view = state.viewMode;
  renderClaims[slot] = token;
  return function stillOurs() {
    return renderClaims[slot] === token && state.viewMode === view;
  };
}

/** The panel is one place. Bus boards and rail boards share this slot, which
 *  is what makes one unable to paint over the other. */
const PANEL_SLOT = "panel";

async function fetchDepartures(atcoCode) {
  const stillOurs = claimRender(PANEL_SLOT);
  // Still the stop the reader is looking at, and still the request that owns
  // the panel. The second condition is the one that stops a rail station
  // being painted over: selecting one clears `selectedStop` and claims the
  // panel, so this board has nowhere legitimate to go.
  const mine = () => stillOurs()
    && state.selectedStop && state.selectedStop.atcoCode === atcoCode;

  showPanelState("loading");
  try {
    let url = `/api/departures?stopId=${encodeURIComponent(atcoCode)}`;
    const pos = state.stopData[atcoCode];
    if (pos) url += `&lat=${pos.lat}&lon=${pos.lon}`;
    const data = await apiFetch(url);
    if (!mine()) return;
    renderDepartures(data);
  } catch (err) {
    console.error("Departures fetch failed:", err);
    if (!mine()) return;
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

  const shown = departures.slice(0, CONFIG.DEPARTURES_COUNT);
  dom.departuresTbody.innerHTML = shown.map(dep => buildDepartureRow(dep)).join("");

  // Say when this was fetched, and describe the table rather than the
  // response. This said "16 departures" over ten rows, with no hint that six
  // more existed — and a second, earlier assignment of the same element sat
  // just above, immediately overwritten and long dead.
  state.departuresAsOf = new Date();
  updateDepartureCount(shown.length, departures.length);

  startDepartureTicker();
  showPanelState("results");
}

/** The line above the board: how many rows it is showing, of how many, and
 *  when the data was fetched.
 *
 *  Owned in one place because the ticker changes it too. Ageing every row out
 *  used to leave "16 departures · as of 21:07" sitting over an empty table.
 */
function updateDepartureCount(shown, total) {
  if (!dom.departuresCount) return;
  const at = state.departuresAsOf;
  const asOf = at
    ? ` · as of ${at.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" })}`
    : "";
  if (shown === 0) {
    dom.departuresCount.textContent = total > 0
      ? `Nothing further due${asOf}`
      : `No upcoming departures${asOf}`;
    return;
  }
  const more = total > shown ? ` of ${total}` : "";
  dom.departuresCount.textContent =
    `${shown}${more} departure${total === 1 ? "" : "s"}${asOf}`;
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
      <td><button type="button" class="service-badge-btn"
                  data-service="${escapeAttr(service)}"
                  aria-label="Show service ${escapeAttr(service)} on the map"
          ><span class="service-badge ${badgeTextCls}" style="background:${badgeColour}">${escapeHtml(service)}</span></button></td>
      <td><span class="destination-text" title="${escapeAttr(destination)}">${escapeHtml(destination)}</span></td>
      <td><span class="due-time ${isImminent ? "due-imminent" : ""}"
                ${displayTime ? `data-due-at="${escapeAttr(displayTime)}"` : ""}
          >${escapeHtml(dueText)}</span></td>
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

/** Format an ISO datetime string as a due-time label.
 *
 * `now` is a parameter so the ticker below can re-derive every label against
 * one consistent instant, and so a test can age a board without waiting. The
 * label used to be produced once at render time and never revisited: a board
 * left open kept saying "5 mins" indefinitely, which is worse than saying
 * nothing.
 */
function formatDueTime(isoString, now = new Date()) {
  try {
    const d = new Date(isoString);
    if (isNaN(d.getTime())) return isoString; // Return as-is if not parseable
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

  revealPanelForSelection();
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
    ? `<img class="bus-info-icon" src="${escapeAttr(safeUrl(iconUrl))}" alt="">`
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

    <div class="departures-meta bus-report-meta">
      <button class="btn-text" id="report-bus-btn" type="button" aria-expanded="false"
              aria-controls="report-bus-form"
              aria-label="Report a problem with this bus">
        <svg class="icon" aria-hidden="true"><use href="#i-alert"/></svg>
        <span>Report an issue</span>
      </button>
    </div>

    <!-- A bus fault is the operator's, not the council's, and it moves — so
         this is its own form rather than a category on the stop one. The two
         accessibility options exist because a missing next-stop announcement
         is evidence about a legal deadline and had nowhere to be recorded. -->
    <form class="report-stop-form hidden" id="report-bus-form" novalidate>
      <div class="editor-field">
        <label for="rb-category">What's wrong?</label>
        <select id="rb-category" name="category">
          <option value="full">Too full to board</option>
          <option value="cancelled">Didn't turn up</option>
          <option value="late">Badly late</option>
          <option value="accessibility">Access problem (ramp, wheelchair space, driver help)</option>
          <option value="onboard-info">No audible or visible next-stop information</option>
          <option value="other">Something else</option>
        </select>
      </div>
      <div class="editor-field">
        <label for="rb-details">Tell us more</label>
        <textarea id="rb-details" name="details" maxlength="1000" required
                  placeholder="What happened, and where were you?"></textarea>
      </div>
      <div class="editor-field">
        <label for="rb-name">Your name (optional)</label>
        <input id="rb-name" name="name" type="text" maxlength="60">
      </div>

      <!-- Honeypot -->
      <div class="suggest-honeypot" aria-hidden="true">
        <label for="rb-botcheck">Leave this field empty</label>
        <input id="rb-botcheck" name="botcheck" type="checkbox" tabindex="-1" autocomplete="off">
      </div>

      <div class="suggest-turnstile" id="rb-turnstile"></div>

      <p class="suggest-privacy">
        This posts a public record to the project's issue tracker so it can be
        counted and quoted. It is not a complaint to the operator and it does
        not reach the driver — for that, contact
        ${operatorComplaintLinkHtml(state.selectedVehicle && state.selectedVehicle.operator_ref)}.
      </p>

      <div class="suggest-actions">
        <button class="editor-action-btn primary" id="rb-submit" type="submit">
          <svg class="icon" aria-hidden="true"><use href="#i-plus"/></svg>
          <span>Send report</span>
        </button>
        <button class="editor-action-btn" id="rb-cancel" type="button">Cancel</button>
        <span class="editor-status" id="rb-status" role="status" aria-live="polite"></span>
      </div>
    </form>

    <p class="bus-info-footer">Live data · auto-refreshes every 20s</p>
  `;

  dom.busPanelPrompt.classList.add("hidden");
  dom.busInfoContainer.classList.remove("hidden");

  // innerHTML above replaced the form, so its listeners went with it.
  bindReportBusForm();

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
          <a href="${escapeAttr(safeUrl(info.app.url))}" target="_blank" rel="noopener">${escapeHtml(info.app.name)}</a>
        </span>
      </div>`);
  }

  const footerLink = info?.url
    ? `<a class="ticket-more-link" href="${escapeAttr(safeUrl(info.url))}" target="_blank" rel="noopener">Full fares &amp; tickets →</a>`
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
  // The report form lives inside the departures container, so it's hidden
  // with it — collapse it properly so aria-expanded doesn't go stale.
  if (stateKey !== "results") toggleReportStopForm(false);

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

  // No tile swap needed: both themes render the same tiles, and the
  // `dark-mode` class on <html> drives the CSS filter that darkens them.
}

/* ============================================================
 * THE BOTTOM SHEET
 *
 * Below MOBILE_BREAKPOINT the panel is a sheet with three detents rather
 * than a fixed 45vh slab. The slab could show 5 of 10 departures with no
 * way to show more, and "collapsed" meant display:none — which is how
 * Ticket view became unreachable, since it had no control to undo it.
 *
 * A sheet cannot reproduce that: its smallest state is still a sheet, with
 * a handle and a tab strip on screen. `panel-collapsed` is kept as the name
 * for the peek detent so existing controls, tests and the desktop rules
 * carry on working unchanged.
 * ============================================================ */

/* Where the layout stops being side-by-side. 900, not 700: a 768px portrait
   tablet was getting a squeezed map beside a half-width panel of mostly
   whitespace. Must stay in step with the media queries in style.css. */
const MOBILE_BREAKPOINT = 900;

const SHEET_DETENTS = ["peek", "half", "full"];

/* Where the sheet sits when a view opens. Each view is answering a different
   question, so each wants a different share of the screen:
     live    — the map is the point; peek still shows the next departure
     route   — half, so the lines and the list are both readable
     tickets — half, same reason
     network — full, because there is nothing on the map to look at */
const VIEW_DEFAULT_DETENT = {
  live:         "peek",
  improvements: "half",
  tickets:      "half",
  network:      "full",
  updates:      "full",   // panel-only, same as the network view
};

/** The detent a view opens at, given how much screen there is.
 *
 * On a landscape phone — 740x360 is the case that forced this — the fixed
 * chrome comes to 241px of a 360px viewport: a 52px header, a 47px footer,
 * and inside the sheet a 44px drag handle, a 53px Day/Night toggle and a 45px
 * tab strip. There is no split of what remains that gives both a usable map
 * and a visible row of services, so the choice has to be made rather than
 * averaged: open on the content, because that is what someone who opened
 * Route view came for, and leave the handle to drag the map back.
 */
const SHORT_VIEWPORT_HEIGHT = 430;

function defaultDetentForViewport() {
  const preferred = VIEW_DEFAULT_DETENT[state.viewMode] || "half";
  if (window.innerHeight > SHORT_VIEWPORT_HEIGHT) return preferred;
  // "peek" is a fixed 248px and would be taller than the whole layout here,
  // so it is left alone; everything else opens full.
  return preferred === "peek" ? preferred : "full";
}

function isSheetLayout() {
  return window.innerWidth <= MOBILE_BREAKPOINT;
}

/** Resolve the CSS detents to pixels, so a drag can snap to them. */
function sheetDetentPixels() {
  const cs = getComputedStyle(document.documentElement);
  const read = (name, fallback) => {
    const v = cs.getPropertyValue(name).trim();
    const n = parseFloat(v);
    if (!isFinite(n)) return fallback;
    if (v.endsWith("px")) return n;
    // svh and vh both resolve against innerHeight closely enough for snapping.
    if (v.endsWith("svh") || v.endsWith("vh") || v.endsWith("dvh")) {
      return window.innerHeight * n / 100;
    }
    return fallback;
  };
  return {
    peek: read("--sheet-peek", 200),
    half: read("--sheet-half", window.innerHeight * 0.48),
    full: read("--sheet-full", window.innerHeight * 0.88),
  };
}

function syncCollapseButtons(collapsed) {
  document.querySelectorAll(".btn-collapse-panel").forEach(btn => {
    btn.setAttribute("aria-pressed", collapsed ? "true" : "false");
    btn.setAttribute("aria-label", collapsed ? "Show panel" : "Hide panel");
  });
}

/** Move the sheet to a named detent and tell everything else about it. */
function setSheetDetent(detent) {
  if (!SHEET_DETENTS.includes(detent)) return;
  state.sheetDetent = detent;
  document.body.dataset.sheet = detent;

  const collapsed = detent === "peek";
  document.body.classList.toggle("panel-collapsed", collapsed);
  syncCollapseButtons(collapsed);
  if (dom.sheetHandle) {
    dom.sheetHandle.setAttribute("aria-expanded", collapsed ? "false" : "true");
  }
}

/** How much of the map the sheet is covering. */
function sheetOverlapPx() {
  if (!isSheetLayout() || !dom.departurePanel) return 0;
  return Math.round(dom.departurePanel.getBoundingClientRect().height);
}

/**
 * fitBounds, then shift the result clear of the sheet.
 *
 * Not fitBounds' own asymmetric padding: passing the sheet height as
 * paddingBottomRight shrinks the box Leaflet fits into, so it picks a much
 * lower zoom — fitting a Brighton zone came out showing Crawley — and the
 * shape ended up under the sheet anyway. Fitting to the whole map and then
 * panning keeps the zoom honest and moves the shape by exactly as much as
 * the sheet covers.
 */
/**
 * Fit into the part of the map the sheet is not covering.
 *
 * Different from fitBoundsAboveSheet, deliberately. That one fits to the whole
 * map and then pans, which keeps the zoom honest — the right trade for a large
 * shape like a ticket zone, where treating the visible strip as the viewport
 * zoomed out far enough to show Crawley.
 *
 * A selected proposal is the opposite case. It is a local shape, the reader
 * has just asked to look at it, and half of it was landing under the sheet. So
 * here the visible strip *is* the viewport: pass the sheet height as padding
 * and accept the lower zoom, which for a shape this size is a level at most.
 *
 * Mobile only. On desktop the panel is a side column, nothing is covered
 * vertically, and Leaflet's own fit is already right.
 */
function fitBoundsInVisibleMap(bounds, options) {
  const overlap = sheetOverlapPx();
  if (!overlap) {
    state.map.fitBounds(bounds, options);
    return;
  }
  state.map.fitBounds(bounds, {
    ...options,
    paddingTopLeft: [(options && options.padding && options.padding[0]) || 20, 20],
    paddingBottomRight: [(options && options.padding && options.padding[0]) || 20,
                         overlap + 20],
    padding: undefined,
    animate: false,
  });
}

function fitBoundsAboveSheet(bounds, options) {
  // animate:false matters. fitBounds animates by default, and an animated fit
  // finishes *after* the panBy below runs — so the pan was applied and then
  // immediately overwritten by the settling animation, which is why the shape
  // kept landing back underneath the sheet.
  state.map.fitBounds(bounds, { ...options, animate: false });
  const overlap = sheetOverlapPx();
  if (overlap > 0) state.map.panBy([0, Math.round(overlap / 2)], { animate: false });
}

function cycleSheetDetent() {
  const i = SHEET_DETENTS.indexOf(state.sheetDetent);
  setSheetDetent(SHEET_DETENTS[(i + 1) % SHEET_DETENTS.length]);
  if (state.map) state.map.invalidateSize();
}

/** Collapsed is the peek detent; expanded returns to half. */
function setPanelCollapsed(collapsed) {
  setSheetDetent(collapsed ? "peek" : "half");
}

function togglePanelCollapsed() {
  setPanelCollapsed(!document.body.classList.contains("panel-collapsed"));
}

/** The collapse control is only rendered below the breakpoint. Crossing back
 *  above it while collapsed used to strand the panel hidden with no way to
 *  reopen it, because the button that would undo it had gone. */
function syncPanelCollapsedToWidth() {
  if (window.innerWidth > MOBILE_BREAKPOINT &&
      document.body.classList.contains("panel-collapsed")) {
    setPanelCollapsed(false);
  }
}

/**
 * Drag the handle to resize; tap, Enter or Space to cycle the detents.
 *
 * The keyboard path is not a nicety — the sheet is the only way to see more
 * than a few departures, so it cannot be pointer-only. That is also why the
 * handle is a <button> rather than a styled div.
 */
function initSheet() {
  const handle = dom.sheetHandle;
  const panel  = dom.departurePanel;
  if (!handle || !panel) return;

  const TAP_SLOP = 6;          // px of movement still counted as a tap
  let dragging = false, startY = 0, startH = 0, moved = 0, wasDrag = false;

  handle.addEventListener("pointerdown", (e) => {
    if (!isSheetLayout()) return;
    dragging = true;
    moved    = 0;
    startY   = e.clientY;
    startH   = panel.getBoundingClientRect().height;
    try { handle.setPointerCapture(e.pointerId); } catch { /* not fatal */ }
    document.body.classList.add("sheet-dragging");
  });

  handle.addEventListener("pointermove", (e) => {
    if (!dragging) return;
    const dy = startY - e.clientY;            // dragging up grows the sheet
    moved = Math.max(moved, Math.abs(dy));
    const d = sheetDetentPixels();
    const h = Math.min(d.full, Math.max(d.peek, startH + dy));
    document.body.style.setProperty("--sheet-h", `${h}px`);
  });

  const endDrag = (e) => {
    if (!dragging) return;
    dragging = false;
    document.body.classList.remove("sheet-dragging");
    try { handle.releasePointerCapture(e.pointerId); } catch { /* fine */ }

    const height = panel.getBoundingClientRect().height;
    document.body.style.removeProperty("--sheet-h");

    wasDrag = moved >= TAP_SLOP;
    if (!wasDrag) return;                     // a tap: the click handler has it

    const d = sheetDetentPixels();
    const nearest = SHEET_DETENTS.reduce((best, name) =>
      Math.abs(d[name] - height) < Math.abs(d[best] - height) ? name : best);
    setSheetDetent(nearest);
    if (state.map) state.map.invalidateSize();
  };
  handle.addEventListener("pointerup", endDrag);
  handle.addEventListener("pointercancel", endDrag);

  // Fires for a tap and for Enter/Space on the focused button. A click that
  // merely ends a drag would otherwise advance the detent a second time.
  handle.addEventListener("click", () => {
    if (wasDrag) { wasDrag = false; return; }
    cycleSheetDetent();
  });
}

/**
 * Keep an open departure board honest about the passage of time.
 *
 * The rows are rendered once, from a response that may already be a minute
 * old, and then sat there. A probe during the pre-release audit advanced the
 * clock and the board still read "5 mins" — for a bus that had gone. Vehicle
 * polling updates the map, not this.
 *
 * So every thirty seconds each label is re-derived from the time the row
 * carries, rows whose bus has actually left are removed, and the count line
 * says when the data was fetched. No network call: this is arithmetic on data
 * already on the page, which is why it can run this often.
 */
const DEPARTURE_TICK_MS = 30_000;

function ageDepartureBoard(now = new Date()) {
  const cells = document.querySelectorAll(".due-time[data-due-at]");
  if (!cells || !cells.length) return 0;
  const before = cells.length;
  let removed = 0;
  cells.forEach(cell => {
    const iso = cell.dataset.dueAt;
    const when = new Date(iso).getTime();
    const row = cell.closest && cell.closest("tr");
    if (!isNaN(when) && (when - now.getTime()) < -30_000) {
      if (row && row.remove) { row.remove(); removed += 1; }
      return;
    }
    cell.textContent = formatDueTime(iso, now);
    if (cell.classList) {
      cell.classList.toggle("due-imminent", isWithinMinutes(iso, 2));
    }
  });

  // The count described the response, so removing every row left a positive
  // number over an empty table — the board looked broken rather than spent.
  if (removed) {
    const left = before - removed;
    updateDepartureCount(left, left);
    if (left === 0 && dom.departuresTbody) {
      dom.departuresTbody.innerHTML =
        `<tr><td colspan="4" class="no-departures">Nothing further due at this
         stop. Refresh for the next departures.</td></tr>`;
      stopDepartureTicker();
    }
  }
  return removed;
}

function startDepartureTicker() {
  stopDepartureTicker();
  state.departureTicker = setInterval(() => {
    // Nothing to age once the reader has closed or replaced the board.
    if (!state.selectedStop) return stopDepartureTicker();
    ageDepartureBoard();
  }, DEPARTURE_TICK_MS);
}

function stopDepartureTicker() {
  if (state.departureTicker) {
    clearInterval(state.departureTicker);
    state.departureTicker = null;
  }
}

function closePanel() {
  // If the proposal editor is open, close it first (persists the draft).
  if (state.editor) closeEditor();

  // Clear stop selection
  state.selectedStop = null;
  stopDepartureTicker();
  showPanelState("prompt");
  dom.panelStopName.textContent = "Select a stop";
  if (dom.stopSpan) {
    dom.stopSpan.classList.add("hidden");
    dom.stopSpan.innerHTML = "";
  }
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

  // Report an issue at this stop
  if (dom.reportStopBtn) {
    dom.reportStopBtn.addEventListener("click", () => toggleReportStopForm());
  }
  if (dom.reportStopForm) {
    dom.reportStopForm.addEventListener("submit", (e) => {
      e.preventDefault();
      submitStopIssue();
    });
    const cancel = dom.reportStopForm.querySelector("#rs-cancel");
    if (cancel) cancel.addEventListener("click", () => toggleReportStopForm(false));
  }

  // Click a departure row → open the matching live vehicle in the Bus tab.
  // The whole row stays clickable for pointer users; the badge inside it is a
  // real button so the same action has a keyboard route. A <tr> with a click
  // listener has none — it cannot be focused and Enter does nothing on it.
  dom.departuresTbody.addEventListener("click", (e) => {
    const btn = e.target.closest("button.service-badge-btn");
    const tr  = e.target.closest("tr.departure-row");
    const service = (btn && btn.dataset.service) || (tr && tr.dataset.service);
    if (service) openBusFromService(service);
  });

  // Dark mode toggle
  dom.darkModeBtn.addEventListener("click", toggleDarkMode);

  // The bus button opens the operator filter; the master switch inside it is
  // what still turns every bus off in one go.
  if (dom.toggleBusesBtn) {
    dom.toggleBusesBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      setBusFilterOpen(!state.busFilterOpen);
    });
    updateBusesToggleBtn();
  }
  if (dom.busFilterAll) {
    dom.busFilterAll.addEventListener("change", () => {
      const willShow = dom.busFilterAll.checked;
      setBusesVisible(willShow);
      showToast(willShow ? "Showing buses." : "Buses hidden.");
    });
  }
  if (dom.busFilter) {
    // A popover has to be dismissible without hunting for the button again.
    dom.busFilter.addEventListener("click", (e) => e.stopPropagation());
    document.addEventListener("click", () => {
      if (state.busFilterOpen) setBusFilterOpen(false);
    });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && state.busFilterOpen) {
        setBusFilterOpen(false);
        if (dom.toggleBusesBtn) dom.toggleBusesBtn.focus();
      }
    });
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
  if (dom.routeChipsMore) {
    dom.routeChipsMore.addEventListener("click", () => {
      state.routeChipsExpanded = !state.routeChipsExpanded;
      reflowRouteChips();
    });
  }
  if (dom.routeFiltersDisc) {
    // Chips inside a closed <details> have no layout, so offsetTop is 0 for
    // every one of them and the measurement above would clip nothing. Measure
    // when it opens, which is the first moment there is anything to measure.
    dom.routeFiltersDisc.addEventListener("toggle", () => {
      if (dom.routeFiltersDisc.open) reflowRouteChips();
    });
  }
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
  initSheet();
  // First paint gets the same per-view default a view change would give it.
  setSheetDetent(isSheetLayout() ? defaultDetentForViewport() : "half");
  window.addEventListener("resize", syncPanelCollapsedToWidth);
  // Rotating a phone changes how many chips fit in a row, so the cap has to be
  // measured again. Debounced: resize fires continuously during a drag.
  let _chipReflowTimer = null;
  window.addEventListener("resize", () => {
    clearTimeout(_chipReflowTimer);
    _chipReflowTimer = setTimeout(reflowRouteChips, 150);
  });

  // Improvements panel: tab switching + close
  dom.tabAbout.addEventListener("click",     () => setImprovementsTab("about"));
  dom.tabProposals.addEventListener("click", () => setImprovementsTab("proposals"));
  dom.closePanelBtnImprovements.addEventListener("click", closePanel);

  // Network plan panel: Objectives / Ideas tab switching + suggestion form
  if (dom.tabObjectives) dom.tabObjectives.addEventListener("click", () => setNetworkTab("objectives"));
  if (dom.tabIdeas)      dom.tabIdeas.addEventListener("click",      () => setNetworkTab("ideas"));
  if (dom.tabOfficialNews)  dom.tabOfficialNews.addEventListener("click",  () => setUpdatesTab("official"));
  if (dom.tabCommunityNews) dom.tabCommunityNews.addEventListener("click", () => setUpdatesTab("community"));
  if (dom.suggestForm) {
    dom.suggestForm.addEventListener("submit", (e) => {
      e.preventDefault();
      submitSuggestion();
    });
  }

  // Ticket view: boundary penalty calculator
  if (dom.jcFrom) {
    dom.jcFrom.addEventListener("input", () => fillStopDatalist(dom.jcFromList, dom.jcFrom.value));
  }
  if (dom.jcTo) {
    dom.jcTo.addEventListener("input", () => fillStopDatalist(dom.jcToList, dom.jcTo.value));
  }
  if (dom.jcCheck) dom.jcCheck.addEventListener("click", checkJourney);
  // Delegated: the result panel is rebuilt on every check, so the "see what
  // we're asking for" button can't be bound directly.
  if (dom.jcResult) {
    dom.jcResult.addEventListener("click", (e) => {
      if (!e.target.closest("[data-goto-objectives]")) return;
      setViewMode("network");
      setNetworkTab("objectives");
    });
  }
  // Enter in either box runs the check, rather than doing nothing.
  [dom.jcFrom, dom.jcTo].forEach(el => {
    if (!el) return;
    el.addEventListener("keydown", (e) => {
      if (e.key === "Enter") { e.preventDefault(); checkJourney(); }
    });
  });

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
    syncVehicleRefreshToVisibility();
  });
}

/**
 * Stop polling for buses while nobody is looking at them.
 *
 * The refresh was a bare setInterval that ran for as long as the tab existed,
 * so a page left open in a background tab fetched every twenty seconds for
 * hours. LIMITS.md counts that against the BODS budget *per tab*, and on a
 * phone it is the kind of thing that quietly costs battery.
 *
 * Coming back restarts it, and startVehicleRefresh fetches immediately before
 * setting its interval — so returning to the tab shows current positions rather
 * than up-to-twenty-second-old ones. The guards match the ones setBusesVisible
 * and applyViewMode already use: nothing restarts if the user turned buses off
 * or navigated away.
 */
function syncVehicleRefreshToVisibility() {
  const wanted = document.visibilityState !== "hidden"
              && state.viewMode === "live"
              && state.busesVisible;
  if (wanted && !state.isRefreshing)      startVehicleRefresh();
  else if (!wanted && state.isRefreshing) stopVehicleRefresh();
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

/** Switch between the Objectives and Ideas tabs in Network plan mode.
 *  Mirrors setActiveTab() so the same ARIA (aria-selected) + .active/.hidden
 *  contract the rest of the panel relies on is preserved. */
/* ============================================================
 * NETWORK UPDATES
 *
 * Short articles on what is changing. Two sources, kept in separate files
 * for the same reason objectives and ideas are: one is written here, the
 * other comes from passengers and is published only after review.
 * ============================================================ */
async function loadUpdates() {
  if (state.officialUpdates && state.communityUpdates) return;
  const read = async (path) => {
    try {
      const res = await fetch(path);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      return Array.isArray(data.updates) ? data.updates : [];
    } catch (err) {
      // An empty feed is a fine thing to show; a broken view is not.
      console.warn(`Updates unavailable (${path}):`, err);
      return [];
    }
  };
  const [official, community] = await Promise.all([
    read("data/updates.json"),
    read("data/community_updates.json"),
  ]);
  state.officialUpdates  = official;
  state.communityUpdates = community;
}

/**
 * Newest first — a news list in any other order is a filing cabinet.
 *
 * Several items often share a date, and a plain date sort then falls back to
 * whatever order they sit in the file, which puts the item added first at the
 * top. Ties break on position instead, latest addition leading.
 */
function sortUpdates(list) {
  return (list || [])
    .map((u, i) => ({ u, i }))
    .sort((a, b) => {
      const byDate = String(b.u.date || "").localeCompare(String(a.u.date || ""));
      return byDate !== 0 ? byDate : b.i - a.i;
    })
    .map(x => x.u);
}

/**
 * One article, laid out as an article.
 *
 * A news list is read by scanning: picture, date, headline, first line, and
 * only then the piece itself. So each of those is a distinct thing here with
 * its own weight, rather than three sizes of the same grey paragraph.
 *
 * Long pieces are folded after their opening paragraphs behind a "Continue
 * reading" button, so ten items stay a list rather than becoming a wall. The
 * fold is by paragraph count, not by measuring the rendered height: it gives
 * the same answer on every screen and never reflows after paint.
 */
const UPDATE_FOLD_AFTER_PARAGRAPHS = 2;

function updateCardHtml(u, opts = {}) {
  const id    = String(u.id || "");
  const date  = u.date
    ? `<time class="update-card-date" datetime="${escapeAttr(u.date)}">${escapeHtml(formatUpdateDate(u.date))}</time>`
    : "";
  const topic = u.topic
    ? `<span class="update-card-topic">${escapeHtml(u.topic)}</span>` : "";
  const byline = opts.community && u.name
    ? `<span class="update-card-byline">Reported by ${escapeHtml(u.name)}</span>` : "";

  const img = u.image && u.image.src ? u.image : null;
  // A decorative-looking photo still carries the meaning of the piece, so it
  // gets real alt text or none at all — never a filename.
  const media = img ? `
      <figure class="update-card-media">
        <img src="${escapeAttr(safeUrl(img.src))}" alt="${escapeAttr(img.alt || "")}"
             loading="lazy" decoding="async"
             style="object-position:${escapeAttr(img.focus || "50% 50%")}">
        ${img.credit ? `<figcaption class="update-card-credit">${escapeHtml(img.credit)}</figcaption>` : ""}
      </figure>` : "";

  const links = Array.isArray(u.links) && u.links.length
    ? `<footer class="update-card-sources">
         <p class="update-card-sources-title">Sources</p>
         <ul class="proposal-links">${u.links.map(l => `
           <li><a class="proposal-link" href="${escapeAttr(safeUrl(l.url))}"
                  target="_blank" rel="noopener noreferrer">${escapeHtml(l.label || l.url)} ↗</a></li>`
         ).join("")}</ul>
       </footer>`
    : "";

  // Paragraph breaks are meaningful in an article, and escapeHtml alone
  // would render the whole body as one run-on block.
  const paras = String(u.body || "").split(/\n\s*\n/).filter(Boolean);
  const open  = paras.slice(0, UPDATE_FOLD_AFTER_PARAGRAPHS);
  const rest  = paras.slice(UPDATE_FOLD_AFTER_PARAGRAPHS);
  const para  = (t) => `<p>${escapeHtml(t)}</p>`;
  const restId = `update-rest-${escapeAttr(id || String(Math.random()).slice(2))}`;
  const folded = rest.length
    ? `<div class="update-card-rest hidden" id="${restId}">${rest.map(para).join("")}</div>
       <button type="button" class="update-card-more" data-update-more="${restId}"
               aria-expanded="false" aria-controls="${restId}">Continue reading</button>`
    : "";

  return `
    <article class="update-card">
      ${media}
      <div class="update-card-text">
        <p class="update-card-kicker">${topic}${date}${byline}</p>
        <h3 class="update-card-title">${escapeHtml(u.title || "Untitled")}</h3>
        ${u.summary ? `<p class="update-card-lede">${escapeHtml(u.summary)}</p>` : ""}
        <div class="update-card-body">${open.map(para).join("")}${folded}</div>
        ${links}
      </div>
    </article>`;
}

/** Wire the "Continue reading" buttons in a freshly rendered list. */
function bindUpdateFolds(host) {
  if (!host) return;
  host.querySelectorAll("[data-update-more]").forEach(btn => {
    btn.addEventListener("click", () => {
      const rest = host.querySelector(`#${CSS.escape(btn.dataset.updateMore)}`);
      if (!rest) return;
      const showing = rest.classList.toggle("hidden");
      btn.setAttribute("aria-expanded", showing ? "false" : "true");
      btn.textContent = showing ? "Continue reading" : "Show less";
    });
  });
}

function formatUpdateDate(iso) {
  const d = new Date(iso);
  if (isNaN(d)) return iso;
  return d.toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric" });
}

function renderUpdates() {
  if (dom.officialUpdatesList) {
    const list = sortUpdates(state.officialUpdates);
    dom.officialUpdatesList.innerHTML = list.length
      ? list.map(u => updateCardHtml(u)).join("")
      : `<p class="proposals-empty">No updates published yet.</p>`;
    bindUpdateFolds(dom.officialUpdatesList);
  }
  if (dom.communityUpdatesList) {
    const list = sortUpdates(state.communityUpdates);
    dom.communityUpdatesList.innerHTML = list.length
      ? list.map(u => updateCardHtml(u, { community: true })).join("")
      : `<p class="proposals-empty">Nothing from passengers yet — this is where
         reviewed reports from the network will appear.</p>`;
    bindUpdateFolds(dom.communityUpdatesList);
  }
}

function setUpdatesTab(tab) {
  const officialActive = (tab !== "community");
  state.updatesTab = officialActive ? "official" : "community";
  if (!dom.tabOfficialNews || !dom.tabCommunityNews) return;
  dom.tabOfficialNews.classList.toggle("active", officialActive);
  dom.tabCommunityNews.classList.toggle("active", !officialActive);
  dom.tabOfficialNews.setAttribute("aria-selected", String(officialActive));
  dom.tabCommunityNews.setAttribute("aria-selected", String(!officialActive));
  dom.tabContentOfficial.classList.toggle("hidden", !officialActive);
  dom.tabContentCommunityNews.classList.toggle("hidden", officialActive);
}

function setNetworkTab(tab) {
  const objectivesActive = (tab !== "ideas");
  state.networkTab = objectivesActive ? "objectives" : "ideas";
  if (!dom.tabObjectives || !dom.tabIdeas) return;
  dom.tabObjectives.classList.toggle("active", objectivesActive);
  dom.tabIdeas.classList.toggle("active", !objectivesActive);
  dom.tabObjectives.setAttribute("aria-selected", String(objectivesActive));
  dom.tabIdeas.setAttribute("aria-selected", String(!objectivesActive));
  dom.tabContentObjectives.classList.toggle("hidden", !objectivesActive);
  dom.tabContentIdeas.classList.toggle("hidden", objectivesActive);

  // Fetch the Turnstile script only once someone actually opens the form.
  if (!objectivesActive) mountTurnstile(dom.suggestTurnstile);
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
  if (!["live", "improvements", "tickets", "network", "updates"].includes(mode)) return;
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
  // The sheet position is per-view, and resetting it on every view change is
  // also what retired the Phase 1b bug: `panel-collapsed` lives on <body>, so
  // it used to outlive the view that set it and hide the incoming view's
  // content too — unrecoverably in Ticket view, which had no control to undo
  // it. Desktop has no sheet, so it always opens expanded.
  setSheetDetent(isSheetLayout() ? defaultDetentForViewport() : "half");

  const live = state.viewMode === "live";
  // The "show buses" toggle only does anything in Live view.
  if (dom.toggleBusesBtn) dom.toggleBusesBtn.hidden = !live;
  if (!live && state.busFilterOpen) setBusFilterOpen(false);
  if (dom.toggleRailBtn)  dom.toggleRailBtn.hidden  = !live;
  // ...and hide the pill that holds them, not just its contents. On mobile it
  // is positioned over the map with its own background, so an empty one reads
  // as a stray box. The timestamp it also carries describes the vehicle feed,
  // which is paused outside Live view anyway.
  if (dom.liveStatusPill) dom.liveStatusPill.hidden = !live;

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

  // A view's data can take seconds on a cold load, and the reader is free to
  // leave in the meantime. Everything below this point is drawn *into a view*,
  // so each await has to be followed by "is this still the view we are in?" —
  // without it, a slow route-lines response put 35 route layers on the map
  // underneath Network Objectives.
  const mine = claimRender("view");

  if (state.viewMode === "improvements") {
    hideTicketZones();
    clearJourneyLayers();
    ensureMapOverlayControls();
    try {
      await Promise.all([loadRouteLines(), loadProposals(), loadCouncilBoundaries()]);
      if (!mine()) return;
      showRouteLines();
      reconcileCouncilBoundaries();
      reconcileProposalLayers();
      if (state.selectedProposalId) showProposal(state.selectedProposalId);
    } catch (err) {
      console.warn("Route view data fetch failed:", err);
      if (!mine()) return;
      showToast("Could not load route data. Try again later.");
    }
  } else if (state.viewMode === "tickets") {
    // The editor only makes sense in Route view; tear it down.
    if (state.editor) closeEditor({ skipSave: false });
    hideRouteLines();
    hideAllProposalLayers();
    hideCouncilBoundaries();
    clearJourneyLayers();
    try {
      await loadTicketZones();
      if (!mine()) return;
      showTicketZones();
      renderJourneyPresets();     // examples for the checker; failure is silent
    } catch (err) {
      console.warn("Ticket view data fetch failed:", err);
      if (!mine()) return;
      showToast("Could not load ticket data. Try again later.");
    }
  } else if (state.viewMode === "updates") {
    // Panel-only, like the network view — no map layers of its own.
    if (state.editor) closeEditor({ skipSave: false });
    hideRouteLines();
    hideAllProposalLayers();
    hideTicketZones();
    hideCouncilBoundaries();
    clearJourneyLayers();
    try {
      await loadUpdates();
      if (!mine()) return;
      renderUpdates();
    } catch (err) {
      console.warn("Updates fetch failed:", err);
    }
  } else if (state.viewMode === "network") {
    // Network plan is panel-only (objectives + ideas); no map overlays.
    if (state.editor) closeEditor({ skipSave: false });
    hideRouteLines();
    hideAllProposalLayers();
    hideTicketZones();
    hideCouncilBoundaries();
    clearJourneyLayers();
    try {
      await loadNetworkData();
      if (!mine()) return;
    } catch (err) {
      console.warn("Network plan data fetch failed:", err);
      if (!mine()) return;
      showToast("Could not load network plan data. Try again later.");
    }
  } else {
    // Live: tear down all network-view layers and restore the live map. The
    // council boundary stays — it is as much a live fact as the buses are.
    if (state.editor) closeEditor({ skipSave: false });
    hideRouteLines();
    hideAllProposalLayers();
    hideTicketZones();
    clearJourneyLayers();
    try {
      await loadCouncilBoundaries();
      if (!mine()) return;
      reconcileCouncilBoundaries();
    } catch (err) {
      console.warn("Council boundary unavailable:", err);
    }
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
    const fg = textColourOn(colour);
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
    const fg       = textColourOn(bg);
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
      updateRouteFilterCount();
    });
  });

  updateRouteFilterCount();
  reflowRouteChips();
}

/* ── How many services, and how many fit ─────────────────────
 *
 * The list lives inside a closed disclosure now, so the summary has to say
 * what is in it. And because narrowing the filters removes chips, the rows
 * they occupied come back — so the cap is measured against the layout after
 * every render rather than fixed at a chip count, which would be wrong the
 * moment a "700X" sat next to a "9".
 */

/** Rows of chips shown before "+N more" appears. Three is what fits above the
 *  prose on a 390x844 phone with the filters open; beyond that the disclosure
 *  itself starts scrolling and the point of collapsing it is lost. */
const ROUTE_CHIP_ROWS = 3;

function updateRouteFilterCount() {
  if (!dom.routeFiltersCount) return;
  const chips = dom.routeFilterChips
    ? [...dom.routeFilterChips.querySelectorAll(".route-chip")] : [];
  if (!chips.length) { dom.routeFiltersCount.textContent = ""; return; }
  const on = chips.filter(c => c.getAttribute("aria-pressed") === "true").length;
  // "18 of 24 on the map" rather than a bare number: the figure that matters
  // is how much of the network you are looking at.
  dom.routeFiltersCount.textContent = on === chips.length
    ? `all ${chips.length}`
    : `${on} of ${chips.length}`;
}

/**
 * Clip the chip list to ROUTE_CHIP_ROWS and offer the rest.
 *
 * Measured, not counted. Chips are natural-width — "N700" is nearly three
 * times "9" — so the number that fits in three rows depends on which services
 * passed the filter. Every chip's own offsetTop against the first row's is the
 * only honest answer, and it is what makes the list grow by itself when a
 * filter frees a row.
 */
function reflowRouteChips() {
  const host = dom.routeFilterChips;
  const more = dom.routeChipsMore;
  if (!host || !more) return;

  const chips = [...host.querySelectorAll(".route-chip")];
  if (!chips.length) { more.classList.add("hidden"); return; }

  if (state.routeChipsExpanded) {
    host.style.maxHeight = "";
    for (const c of chips) c.classList.remove("route-chip--clipped");
    more.classList.remove("hidden");
    more.textContent = "Show fewer";
    more.setAttribute("aria-expanded", "true");
    return;
  }

  // Reset before measuring: a cap left over from the previous render would
  // make every chip below it report the same offsetTop and hide the lot.
  host.style.maxHeight = "";
  for (const c of chips) c.classList.remove("route-chip--clipped");

  const top0 = chips[0].offsetTop;
  const rows = [];
  for (const c of chips) {
    if (!rows.includes(c.offsetTop)) rows.push(c.offsetTop);
  }
  rows.sort((a, b) => a - b);

  if (rows.length <= ROUTE_CHIP_ROWS) {
    more.classList.add("hidden");
    return;
  }

  const cutoff = rows[ROUTE_CHIP_ROWS];      // first row that does not fit
  const hidden = chips.filter(c => c.offsetTop >= cutoff);
  for (const c of hidden) c.classList.add("route-chip--clipped");
  host.style.maxHeight = `${cutoff - top0}px`;
  more.classList.remove("hidden");
  more.textContent = `+${hidden.length} more`;
  more.setAttribute("aria-expanded", "false");
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
  updateRouteFilterCount();
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
                     href="${escapeAttr(safeUrl(l.url))}"
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
      // Was a plain fitBounds, which centres in the whole map — and on a phone
      // the bottom half of the map is under the sheet, so a proposal selected
      // from the list below it was drawn half behind the list.
      fitBoundsInVisibleMap(L.latLngBounds(allPts), { padding: [40, 40], maxZoom: 14 });
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
  // Fare basis for the boundary calculator: commute assumptions and the
  // hypothetical unified fare the saving is measured against.
  state.ticketFaresMeta = (data.fares_meta && typeof data.fares_meta === "object")
    ? data.fares_meta : {};

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
        const fg = textColourOn(bg);
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

/**
 * Map bounds for an operator's zones.
 *
 * The outlines only — deliberately not the reach pills. Those mark where a
 * ticket can carry you, which for Brighton & Hove runs out to Lewes, Ringmer
 * and Devil's Dyke, and fitting them dragged the view all the way to
 * Crowborough. On a phone's 331px-tall map that left the actual zone as a
 * shape in the bottom corner, which is what "the boundaries aren't visible"
 * turned out to mean. The pills are still drawn; they are just not allowed to
 * decide the framing.
 *
 * Falls back to including them when an operator has no polygon at all, so an
 * operator-wide ticket still has something to fit to.
 */
function operatorBounds(op) {
  let outlines = null;
  let withPills = null;

  /* Copy, don't alias. L.latLngBounds(x) returns x itself when handed a
     LatLngBounds, and extend() mutates in place — so seeding both variables
     from one layer's bounds made them the same object, and adding the reach
     pills to one silently grew the other. That is what dragged the framing
     out to Crowborough while the geometry was perfectly correct. */
  const copy = (b) => L.latLngBounds(b.getSouthWest(), b.getNorthEast());
  const grow = (b, x) => (b ? b.extend(x) : L.latLngBounds(x, x));

  for (const z of (state.ticketZones || [])) {
    if (z.operator !== op) continue;
    const layer = state.ticketZoneLayers[z.id];
    if (layer) {
      const lb = layer.getBounds();
      outlines  = outlines  ? outlines.extend(copy(lb))  : copy(lb);
      withPills = withPills ? withPills.extend(copy(lb)) : copy(lb);
    }
    for (const m of (state.ticketReachLayers[z.id] || [])) {
      withPills = grow(withPills, m.getLatLng());
    }
  }
  return outlines || withPills;
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
    fitBoundsAboveSheet(layer.getBounds(), { padding: [40, 40], maxZoom: 13 });
  } else if (pills && pills.length) {
    const b = L.latLngBounds(pills.map(m => m.getLatLng()));
    fitBoundsAboveSheet(b, { padding: [30, 30], maxZoom: 13 });
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
    // The card's stripe is the colour the zone is drawn in, so a card and its
    // shape on the map are recognisably the same thing. Only card-only
    // tickets with no geometry (Gold) fall back to the operator's colour.
    const border   = z.color || getOperatorBorderColour(z.operator);
    // Prefer a sourced fare over the free-text `price` blurb, and date it so a
    // stale figure is visibly stale rather than quietly wrong.
    const day      = z.fares && z.fares.adult_day;
    const meta     = (day && typeof day.price_pence === "number")
      ? `${formatGbp(day.price_pence)}${z.fares.checked_on ? ` · checked ${z.fares.checked_on}` : ""}`
      : (z.price || "");
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
        ${z.official_map_url ? `<a class="ticket-zone-link" href="${escapeAttr(safeUrl(z.official_map_url))}" target="_blank" rel="noopener noreferrer">View official zone map ↗</a>` : ""}
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
    const fg   = textColourOn(fill);
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
        if (b && b.isValid()) {
          fitBoundsAboveSheet(b, { padding: [45, 45], maxZoom: 13 });
        }
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
      if (e.key !== "Enter" && e.key !== " ") return;
      // Check where the key came from *before* cancelling it. This used to
      // preventDefault first and test afterwards, so pressing Enter on "View
      // official zone map" cancelled the link's navigation and then did
      // nothing — a link that worked with a mouse and not with a keyboard.
      if (e.target.closest("a")) return;
      e.preventDefault();
      activate(e);
    });
  });
}

// ============================================================
// BOUNDARY PENALTY CALCULATOR (Ticket view)
// ============================================================
//
// Answers "does this journey need more than one ticket, and what does that
// cost me?" — the question the zone map implies but can't answer.
//
// The path matters, not just the endpoints: a bus can dip through a third
// operator's zone on the way, so a journey that looks intra-zone at both ends
// can still need two tickets. /api/journey returns the real ordered stop list;
// every stop on it is tested against every zone.
//
// Money is only ever shown when it can be sourced. If any zone on the path has
// no fare data, the boundary warning still renders but the £ figures do not —
// an advocacy claim with an invented number in it is worse than no number.

/** Ray-casting point-in-polygon. `ring` is [[lat, lon], ...], unclosed. */
function pointInRing(lat, lon, ring) {
  if (!Array.isArray(ring) || ring.length < 3) return false;
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const yi = ring[i][0], xi = ring[i][1];
    const yj = ring[j][0], xj = ring[j][1];
    // Ring is treated as implicitly closed (Leaflet does the same), so the
    // i/j pair wraps from last back to first.
    const straddles = (yi > lat) !== (yj > lat);
    if (!straddles) continue;
    if (lon < ((xj - xi) * (lat - yi)) / (yj - yi) + xi) inside = !inside;
  }
  return inside;
}

/** Every geometry ring belonging to a zone, following `polygons_from` refs. */
function zoneRings(zone, byId) {
  const rings = [];
  for (const refId of zone.polygons_from || []) {
    const ref = byId[refId];
    if (ref && Array.isArray(ref.polygon)) rings.push(ref.polygon);
  }
  for (const ring of zone.polygons || []) {
    if (Array.isArray(ring)) rings.push(ring);
  }
  if (Array.isArray(zone.polygon)) rings.push(zone.polygon);
  return rings.filter(r => r.length >= 3);
}

/**
 * Is this ticket accepted on `operator`'s buses?
 *
 * Geography isn't enough — a ticket is only valid on the operators that accept
 * it. Mostly that's just its own (Stagecoach tickets are explicitly not valid
 * on Metrobus or Brighton & Hove), but two cross over: networkSAVER is accepted
 * on Metrobus inside the zone, and Metrovoyager on Brighton & Hove.
 *
 * With no operator known (no direct bus, so we're comparing endpoints only) we
 * can't filter, and the caller labels the answer as the less certain one.
 */
function ticketValidOn(zone, operator) {
  if (!operator) return true;
  const accepted = zone.valid_on_operators;
  if (Array.isArray(accepted) && accepted.length) return accepted.includes(operator);
  return !zone.operator || zone.operator === operator;
}

/** "HH:MM" to minutes past midnight, or null. */
function hhmmToMinutes(hhmm) {
  const m = /^(\d{1,2}):(\d{2})$/.exec(String(hhmm || "").trim());
  if (!m) return null;
  return (parseInt(m[1], 10) % 24) * 60 + parseInt(m[2], 10);
}

/**
 * Is this ticket usable at the journey's departure time?
 *
 * The Gold Nightrider is £4 against the DayRider's £8.50, but only from 19:30 —
 * offering it for a midday commute would quote less than half the real fare.
 * The window wraps past midnight (19:30 to 04:00 is one night), which matters
 * because the N700 runs in the small hours.
 *
 * When a ticket is time-restricted and we don't know the departure time, it's
 * excluded: we can't assert it's usable, and quoting it might be wrong.
 */
function ticketValidAtTime(zone, departHhmm) {
  const from = zone.valid_from_time;
  const to   = zone.valid_to_time;
  if (!from && !to) return true;              // no restriction
  const now = hhmmToMinutes(departHhmm);
  if (now === null) return false;             // restricted, and unverifiable
  const f = from ? hhmmToMinutes(from) : 0;
  const t = to   ? hhmmToMinutes(to)   : 24 * 60;
  if (f === null || t === null) return true;
  return (f <= t) ? (now >= f && now < t) : (now >= f || now < t);
}

/** Cheap bbox pre-filter so the ray-cast only runs on plausible candidates. */
function ringBounds(ring) {
  let minLat = Infinity, maxLat = -Infinity, minLon = Infinity, maxLon = -Infinity;
  for (const [lat, lon] of ring) {
    if (lat < minLat) minLat = lat;
    if (lat > maxLat) maxLat = lat;
    if (lon < minLon) minLon = lon;
    if (lon > maxLon) maxLon = lon;
  }
  return { minLat, maxLat, minLon, maxLon };
}

/**
 * Which zones cover a given stop, for a journey run by `operatorOfStop`.
 *
 * `coverage_rule` decides how the geographic test works, because three of the
 * seven zones have no geometry at all: networkSAVER and the two Gold tickets
 * mean "this operator's whole network round here", which no polygon expresses.
 *
 * Geography is necessary but not sufficient. A ticket is only valid on its own
 * operator's buses, so a Metrobus Metrovoyager doesn't cover a Stagecoach 700
 * journey however neatly its zone contains both stops. When the operator is
 * unknown (no direct bus, so we're comparing endpoints only) the operator test
 * is skipped and the caller labels the result as the less certain answer.
 */
/** One NOC, a list of them, or nothing — always out as an array of codes. */
function normaliseOperators(operatorOfStop) {
  if (!operatorOfStop) return [];
  const list = Array.isArray(operatorOfStop) ? operatorOfStop : [operatorOfStop];
  return list.filter(Boolean);
}

/**
 * The tickets that could actually carry this whole journey.
 *
 * `zonesForStop` answers a per-stop question: does an operator accepting this
 * ticket call here? That is the right test for a direct bus, where one operator
 * runs the lot. On a journey needing a change it is not enough.
 *
 * Metrovoyager is accepted by Metrobus and Brighton & Hove, and Metrobus does
 * reach Worthing — on the route-23 corridor down from Horsham. So a
 * Worthing-to-Hangleton trip had a Metrovoyager at each end and was reported as
 * covered by one ticket. It is not. The bus anyone would actually take is a
 * Stagecoach 700, which the Metrovoyager is not valid on; travelling only on
 * operators that do accept it would mean going north to Horsham and back down.
 *
 * So a ticket survives here only when a *single* operator accepting it serves
 * every endpoint. Two accepting operators that happen to touch opposite ends of
 * a journey do not add up to a ticket you can use, and whether their networks
 * meet somewhere in between is not something this data can answer. Guessing at
 * it is precisely how the Boundary Road answer went wrong.
 */
function commonOperators(endpointOperators) {
  let common = null;
  for (const ops of endpointOperators) {
    // An endpoint we know nothing about makes the whole answer unknown. It must
    // not collapse to "no operators in common", which would read as a finding
    // about the network rather than a gap in what the API told us.
    if (ops === null || ops === undefined) return null;
    const set = new Set(normaliseOperators(ops));
    common = common === null ? set : new Set([...common].filter(o => set.has(o)));
  }
  return common === null ? null : [...common];
}

function ticketsUsableEndToEnd(zones, endpointOperators, legOperators) {
  // If we know which buses the journey is actually on, that is the question:
  // a ticket has to be valid on *every* leg, not merely on some operator that
  // happens to serve both ends. Endpoints are the fallback for a journey the
  // API could not break into legs.
  if (Array.isArray(legOperators) && legOperators.length) {
    return zones.filter(z => legOperators.every(op => ticketValidOn(z, op)));
  }
  // Operators present at *every* endpoint. On a direct journey that is the one
  // operator running it, so nothing changes; on an interchange it is often
  // empty, which is itself the finding.
  const common = commonOperators(endpointOperators);
  if (common === null) return zones;      // nothing to go on
  return zones.filter(z => common.some(op => ticketValidOn(z, op)));
}

function zonesForStop(stop, zones, byId, operatorOfStop) {
  const covering = [];
  // `operatorOfStop` may be a single NOC or a list of every operator calling
  // at the stop. A list is the honest form: a ticket is usable here only if
  // some operator that accepts it actually stops here.
  const serving = normaliseOperators(operatorOfStop);

  for (const z of zones) {
    const rule = z.coverage_rule || "polygon";
    // A ticket is only valid on the operators it says it is. Mostly that's
    // just its own (Stagecoach tickets are explicitly not valid on Metrobus
    // or B&H), but some cross over: networkSAVER covers Metrobus inside the
    // zone, and Metrovoyager covers Brighton & Hove.
    //
    // This used to be skipped whenever the operator was unknown, which is how
    // Boundary Road — inside the Brighton DayRider polygon, served only by
    // Brighton & Hove — was reported as covered by a Stagecoach DayRider. An
    // unknown operator now means we say we don't know, not that we assume yes.
    if (serving.length && !serving.some(op => ticketValidOn(z, op))) continue;

    if (rule === "operator_network") {
      // No geometry to test — validity is exactly "this operator's network".
      if (serving.some(op => ticketValidOn(z, op))) covering.push(z.id);
      continue;
    }
    if (rule === "reach_points") {
      continue;   // illustrative markers only — never load-bearing for fares
    }
    const rings = z._rings || (z._rings = zoneRings(z, byId));
    const boxes = z._boxes || (z._boxes = rings.map(ringBounds));
    for (let i = 0; i < rings.length; i++) {
      const b = boxes[i];
      if (stop.lat < b.minLat || stop.lat > b.maxLat
          || stop.lon < b.minLon || stop.lon > b.maxLon) continue;
      if (pointInRing(stop.lat, stop.lon, rings[i])) { covering.push(z.id); break; }
    }
  }
  return covering;
}

/**
 * Cheapest set of zones covering every stop on the path.
 *
 * Brute force over all subsets — with seven zones that's 128 combinations, so
 * there's nothing to optimise. Returns null when no combination covers the
 * whole path (a gap in our zone data, not a fare fact), and marks the result
 * `priced: false` when any chosen zone has no sourced fare.
 *
 * `zones` is the candidate set, which the caller narrows deliberately: see
 * splitZonesByRule() for why operator-wide tickets are considered separately.
 */
function cheapestCover(coverPerStop, zones) {
  const ids = zones.map(z => z.id);
  const n = ids.length;
  const byId = Object.fromEntries(zones.map(z => [z.id, z]));

  let best = null;
  for (let mask = 1; mask < (1 << n); mask++) {
    const chosen = [];
    for (let i = 0; i < n; i++) if (mask & (1 << i)) chosen.push(ids[i]);

    // Must cover every stop.
    const covers = coverPerStop.every(set => chosen.some(id => set.includes(id)));
    if (!covers) continue;

    let total = 0;
    let priced = true;
    for (const id of chosen) {
      const day = byId[id] && byId[id].fares && byId[id].fares.adult_day;
      if (!day || typeof day.price_pence !== "number") { priced = false; break; }
      total += day.price_pence;
    }

    // Prefer fewest tickets, then cheapest. An unpriced combination still
    // counts for the "how many tickets" answer.
    if (best === null
        || chosen.length < best.zones.length
        || (chosen.length === best.zones.length && priced && best.priced && total < best.total)) {
      best = { zones: chosen, total, priced };
    }
  }
  return best;
}

/**
 * Split zones into the two things a passenger actually chooses between.
 *
 * `zonal` are the geographic day tickets (Worthing Dayrider, citySAVER…).
 * `network` are the operator-wide ones (Stagecoach Gold, networkSAVER), which
 * cover any stop that operator serves.
 *
 * They have to be judged separately. An operator-wide ticket covers *every*
 * all-Stagecoach journey by definition, so folding it into the same search
 * makes almost every journey come back "one ticket, no problem" — which hides
 * the boundary this whole feature exists to show. The real choice on a
 * Worthing-to-Brighton run is "two zone tickets, or one premium network
 * ticket", and both halves of that are worth showing.
 */
function splitZonesByRule(zones) {
  const zonal = [], network = [];
  for (const z of zones) {
    ((z.coverage_rule === "operator_network") ? network : zonal).push(z);
  }
  return { zonal, network };
}

/** The cheapest single operator-wide ticket covering the whole path, if any. */
function bestNetworkTicket(coverPerStop, networkZones) {
  let best = null;
  for (const z of networkZones) {
    if (!coverPerStop.every(set => set.includes(z.id))) continue;
    const day = z.fares && z.fares.adult_day;
    const pence = day && typeof day.price_pence === "number" ? day.price_pence : null;
    if (best === null || (pence !== null && (best.pence === null || pence < best.pence))) {
      best = { zone: z, pence };
    }
  }
  return best;
}

// ─── Stop picker index ──────────────────────────────────────
// There are ~1,500 stops but only ~800 distinct names: most names are a pair of
// poles on opposite sides of one road. Listing both is noise — "Tesco · 4400WO0013"
// twice tells a passenger nothing. So stops are grouped into one entry per
// physical place, and only genuinely different places sharing a name get a
// district suffix to tell them apart.
//
// The district comes from the NaPTAN administrative-area code at the front of
// the ATCO, which is real data rather than a guess from coordinates.

const ATCO_DISTRICTS = {
  "149000": "Brighton & Hove",
  "149010": "Brighton & Hove",
  "4400AD": "Adur",
  "4400WO": "Worthing",
  "4400LH": "Arun",
  "4400HR": "Horsham",
};

function districtForAtco(atco) {
  return ATCO_DISTRICTS[String(atco || "").slice(0, 6)] || "";
}

/** Rough metric distance in km — fine at this scale, and cheap. */
function roughKm(aLat, aLon, bLat, bLon) {
  const dy = (aLat - bLat) * 111;
  const dx = (aLon - bLon) * 111 * Math.cos((aLat * Math.PI) / 180);
  return Math.sqrt(dx * dx + dy * dy);
}

const STOP_CLUSTER_KM = 0.4;   // opposite poles are metres apart, not hundreds

/**
 * Group stops into one entry per place: same name, within STOP_CLUSTER_KM.
 * Memoized — it only depends on the stop list, which is loaded once.
 */
function stopPickerIndex() {
  if (state._stopIndex) return state._stopIndex;

  const byName = new Map();
  for (const [atco, s] of Object.entries(state.stopData)) {
    if (!s.name) continue;
    if (!byName.has(s.name)) byName.set(s.name, []);
    byName.get(s.name).push({ atco, ...s });
  }

  const clusters = [];
  for (const [name, stops] of byName) {
    const groups = [];
    for (const stop of stops) {
      const near = groups.find(g =>
        roughKm(g.lat, g.lon, stop.lat, stop.lon) <= STOP_CLUSTER_KM);
      if (near) {
        near.atcos.push(stop.atco);
      } else {
        groups.push({ name, lat: stop.lat, lon: stop.lon, atcos: [stop.atco] });
      }
    }
    // Only ambiguous names need a district to tell them apart; the rest stay clean.
    const ambiguous = groups.length > 1;
    for (const g of groups) {
      const district = ambiguous ? districtForAtco(g.atcos[0]) : "";
      g.label = district ? `${name}, ${district}` : name;
      clusters.push(g);
    }
  }

  // Distinct labels can still collide (two "Marine Parade" in one district).
  // Fall back to the ATCO for those rather than offering two identical rows.
  const seen = new Map();
  for (const c of clusters) {
    const n = (seen.get(c.label) || 0) + 1;
    seen.set(c.label, n);
    c._dupIndex = n;
  }
  for (const c of clusters) {
    if (seen.get(c.label) > 1) c.label = `${c.label} (${c.atcos[0]})`;
  }

  state._stopIndex = clusters.sort((a, b) => a.label.localeCompare(b.label));
  return state._stopIndex;
}

/**
 * Stop and station search for the live view.
 *
 * Until now the only way to open a departure board was to point at a marker.
 * That is not a route a keyboard has, and clusters are 34px targets besides,
 * so the site's primary task had no accessible path to it at all. This reuses
 * the cluster index the journey checker already builds, and adds rail
 * stations, which have the same problem for the same reason.
 *
 * Results are real buttons in a list, so Tab reaches them and Enter activates
 * them, with no dependence on the map having focus or even being visible.
 */
const STOP_SEARCH_LIMIT = 12;
// Slots a railway station cannot be pushed out of by bus stops.
const RAIL_SEARCH_RESERVE = 2;

/** How well a name answers a query: lower sorts first.
 *
 *  A plain `includes` treats "Worthing" matching the *district suffix* of
 *  "Marine Parade, Worthing" as being as good as matching the name "Worthing
 *  Station", which is how twelve weak bus matches came to bury both railway
 *  stations people were searching for.
 */
function searchRank(name, label, q) {
  const n = (name || "").toLowerCase();
  const l = (label || "").toLowerCase();
  if (n === q) return 0;                       // exact name
  if (n.startsWith(q)) return 1;               // name begins with it
  if (new RegExp(`\\b${q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`).test(n)) return 2;
  if (l.startsWith(q)) return 3;               // label begins with it
  return 4;                                    // matched somewhere else
}

/**
 * The stop list the *live search* works from.
 *
 * Deliberately not `stopPickerIndex()`. That clusters poles within 400 m into
 * one place, which is right for the journey checker — somebody choosing where
 * to travel from wants a place, not a side of the road — and wrong here,
 * where the answer is a departure board for one specific pole. Colebrook Road
 * and Shoreham Port each have two, and showing one sent half the people
 * asking to the opposite kerb.
 *
 * Poles are kept apart only where the name is shared *and* the API gave a
 * direction to tell them apart with. Everywhere else this is the clustered
 * list, unchanged.
 */
function liveSearchIndex() {
  // Keyed to the index it derives from, not memoized outright: `_stopIndex`
  // is rebuilt when the stop list changes, and a cache that outlived it would
  // serve a search over stops that are no longer there.
  const source = stopPickerIndex();
  if (state._liveSearchIndex && state._liveSearchFrom === source) {
    return state._liveSearchIndex;
  }

  const byName = new Map();
  for (const [atco, s] of Object.entries(state.stopData || {})) {
    if (!s.name) continue;
    if (!byName.has(s.name)) byName.set(s.name, []);
    byName.get(s.name).push({ atco, ...s });
  }

  const out = [];
  for (const cluster of source) {
    const siblings = byName.get(cluster.name) || [];
    const split = siblings.filter(s => s.towards);
    // Two or more poles that can actually be told apart: list them.
    if (split.length > 1 && cluster.atcos.length > 1) {
      for (const s of split) {
        if (!cluster.atcos.includes(s.atco)) continue;
        out.push({
          label: cluster.label, name: cluster.name, atcos: [s.atco],
          towards: s.towards, services: s.services,
        });
      }
      continue;
    }
    out.push(cluster);
  }
  state._liveSearchIndex = out;
  state._liveSearchFrom  = source;
  return out;
}

function stopSearchMatches(query) {
  const q = (query || "").trim().toLowerCase();
  if (q.length < 2) return [];

  // Gather everything, then rank. This used to return the moment the bus list
  // filled up, so railway stations were never even considered: "Worthing" and
  // "Hove" each filled all twelve slots with bus stops and excluded the
  // station of that name entirely.
  const candidates = [];
  for (const c of liveSearchIndex()) {
    if (!c.label.toLowerCase().includes(q)) continue;
    // "Bus stop · towards Old Steine · 700, N700" — which way, and what on.
    const bits = ["Bus stop"];
    if (c.towards) bits.push(`towards ${c.towards}`);
    if (c.services && c.services.length) {
      bits.push(c.services.slice(0, 4).join(", ")
                + (c.services.length > 4 ? "…" : ""));
    }
    candidates.push({
      kind: "stop", label: c.label, name: c.name || c.label,
      atco: c.atcos[0], mode: bits.join(" · "),
    });
  }
  for (const st of (state.railStations || [])) {
    if (!st.name || !st.name.toLowerCase().includes(q)) continue;
    candidates.push({
      kind: "rail", label: `${st.name} station`, name: st.name,
      crs: st.crs, railName: st.name, mode: "Railway station",
    });
  }

  candidates.forEach((c, i) => {
    c._rank = searchRank(c.name, c.label, q);
    c._i = i;                                  // stable within a rank
  });
  candidates.sort((a, b) => (a._rank - b._rank) || (a._i - b._i));

  // A railway station is a different kind of answer from a bus stop, and a
  // long list of stops must not be able to hide the one station someone was
  // looking for. Reserve room for the best few, then fill the rest by rank.
  const rail = candidates.filter(c => c.kind === "rail").slice(0, RAIL_SEARCH_RESERVE);
  const out = [...rail];
  for (const c of candidates) {
    if (out.length >= STOP_SEARCH_LIMIT) break;
    if (!out.includes(c)) out.push(c);
  }
  out.sort((a, b) => (a._rank - b._rank) || (a._i - b._i));
  return out.slice(0, STOP_SEARCH_LIMIT);
}

function renderStopSearchResults(listEl, matches, query) {
  if (!listEl) return;
  if (!matches.length) {
    listEl.innerHTML = (query || "").trim().length >= 2
      ? `<li class="stop-search-empty">No stop or station matches that.</li>`
      : "";
    return;
  }
  listEl.innerHTML = matches.map(m => `
    <li>
      <button type="button" class="stop-search-result"
              data-kind="${escapeAttr(m.kind)}"
              data-atco="${escapeAttr(m.atco || "")}"
              data-crs="${escapeAttr(m.crs || "")}"
              data-rail-name="${escapeAttr(m.railName || "")}"
              data-label="${escapeAttr(m.label)}">
        <span class="stop-search-result-name">${escapeHtml(m.name || m.label)}</span>
        <span class="stop-search-result-mode">${escapeHtml(m.mode || "")}</span>
      </button>
    </li>`).join("");
}

/** "Try again" wherever a loader failed.
 *
 *  Delegated, so it survives the rerender that follows a retry, and keyed by
 *  what failed rather than by which button was pressed. */
/**
 * Close a native <dialog> when the backdrop is clicked.
 *
 * `showModal()` gives Escape and focus trapping for free, but not this: the
 * backdrop is a pseudo-element, so there is nothing to attach a listener to.
 * The test is geometric rather than `e.target === dialog`, because a click on
 * the dialog's own padding also reports the dialog as the target and would
 * close it while the reader was aiming at the text inside.
 */
/**
 * Open the sheet far enough to show what the reader just selected.
 *
 * `peek` is 248px, and the handle, tabs, heading and meta row fill it — so
 * selecting something used to put its content at exactly the panel's bottom
 * edge: rows in the DOM, none of them visible. Asking for a stop, a bus or a
 * station is asking to see it.
 *
 * Only lifts from `peek`, and only on a sheet layout. If the reader has
 * already dragged the sheet somewhere, that is their choice and it stands.
 */
function revealPanelForSelection() {
  if (!isSheetLayout()) return;
  if (document.body.dataset.sheet !== "peek") return;
  const preferred = defaultDetentForViewport();
  setSheetDetent(preferred === "peek" ? "half" : preferred);
}

function bindDialogBackdropClose(dialog) {
  if (!dialog || dialog._backdropBound) return;
  dialog._backdropBound = true;
  dialog.addEventListener("click", (e) => {
    // A keyboard-activated button inside the dialog dispatches a click at
    // (0, 0), which is outside every rect. Closing on that would make Enter
    // dismiss the dialog instead of pressing the button.
    if (e.detail === 0) return;
    const r = dialog.getBoundingClientRect();
    const inside = e.clientX >= r.left && e.clientX <= r.right
                && e.clientY >= r.top  && e.clientY <= r.bottom;
    if (!inside) dialog.close();
  });
}

/**
 * Close `el` when a click lands outside it and outside whatever opened it.
 *
 * The bus filter and the section-nav menu already did this, each in their own
 * way. This is the same behaviour for the things that did not — and the
 * inconsistency was the complaint.
 */
function bindDismissOnOutsideClick(el, trigger, close) {
  if (!el || el._outsideBound) return;
  el._outsideBound = true;
  document.addEventListener("click", (e) => {
    if (el.classList.contains("hidden")) return;
    if (el.contains(e.target)) return;
    if (trigger && trigger.contains(e.target)) return;
    close();
  });
}

function bindLoaderRetries() {
  document.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-retry]");
    if (!btn) return;
    const what = btn.dataset.retry;
    if (what === "objectives") {
      state.objectivesError = null;
      state.objectives = null;
      state._networkPromise = null;
      loadNetworkData().catch(() => {});
    }
  });
}

/**
 * Make every `role="tablist"` behave like one.
 *
 * The markup declares the tabs pattern; the behaviour did not follow it.
 * Every tab carried `tabIndex=0`, so a keyboard user tabbed through each one
 * in turn instead of arrowing between them, and ArrowRight on the Routes
 * About tab did nothing at all — focus stayed put and Proposals was never
 * selected. The pattern is one tab stop per tablist, arrows to move.
 *
 * Applied generically rather than per tab strip: there are five tablists and
 * they were all declared the same way, so they should all behave the same way.
 * See https://www.w3.org/WAI/ARIA/apg/patterns/tabs/
 */
function bindRovingTabs() {
  for (const list of document.querySelectorAll('[role="tablist"]')) {
    const tabs = () => [...list.querySelectorAll('[role="tab"]')]
      .filter(t => t.offsetParent !== null || t === document.activeElement);

    const sync = () => {
      for (const t of list.querySelectorAll('[role="tab"]')) {
        // Only the selected tab is in the tab order; the rest are reached
        // with the arrow keys, which is what makes this one stop.
        t.tabIndex = t.getAttribute("aria-selected") === "true" ? 0 : -1;
      }
    };
    sync();
    // Selection is changed by the app's own click handlers, so mirror it
    // rather than duplicating the logic here.
    new MutationObserver(sync).observe(list,
      { subtree: true, attributes: true, attributeFilter: ["aria-selected"] });

    list.addEventListener("keydown", (e) => {
      const all = tabs();
      const i = all.indexOf(document.activeElement);
      if (i === -1) return;
      let next = null;
      if (e.key === "ArrowRight" || e.key === "ArrowDown") next = all[(i + 1) % all.length];
      else if (e.key === "ArrowLeft" || e.key === "ArrowUp") next = all[(i - 1 + all.length) % all.length];
      else if (e.key === "Home") next = all[0];
      else if (e.key === "End") next = all[all.length - 1];
      if (!next) return;
      e.preventDefault();
      // Follow-focus selection, which is what this pattern expects when
      // switching panels is cheap — and here it is a class toggle.
      next.focus();
      next.click();
    });
  }
}

function bindStopSearch() {
  const form  = document.getElementById("stop-search-form");
  const input = document.getElementById("stop-search-input");
  const list  = document.getElementById("stop-search-results");
  if (!form || !input || !list) return;

  const update = () => {
    const matches = stopSearchMatches(input.value);
    renderStopSearchResults(list, matches, input.value);
    input.setAttribute("aria-expanded", matches.length ? "true" : "false");
  };

  input.addEventListener("input", update);
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    update();
    // Enter in the box goes straight to the only sensible answer rather than
    // making the reader Tab to a list of one.
    const first = list.querySelector("button.stop-search-result");
    if (first && stopSearchMatches(input.value).length === 1) first.click();
    else if (first) first.focus();
  });

  list.addEventListener("click", (e) => {
    const btn = e.target.closest("button.stop-search-result");
    if (!btn) return;
    if (btn.dataset.kind === "rail") {
      // `openRailBoard`, not `selectRailStation` — the latter has never
      // existed. It was invented from the name of the state field it sets
      // (`state.selectedRailStation`), so activating any station result threw
      // a ReferenceError and did nothing. The tests missed it because they
      // asserted that the result button was rendered, never that pressing it
      // worked; see the browser check added alongside this fix.
      openRailBoard(btn.dataset.crs, btn.dataset.railName || btn.dataset.label);
    } else if (btn.dataset.atco) {
      openDepartures(btn.dataset.atco, btn.dataset.label);
    }
  });
}

/** Rebuild a datalist with the places matching what's been typed so far. */
function fillStopDatalist(listEl, query) {
  if (!listEl) return;
  const q = (query || "").trim().toLowerCase();
  const out = [];
  if (q.length >= 2) {
    for (const c of stopPickerIndex()) {
      if (!c.label.toLowerCase().includes(q)) continue;
      out.push(`<option value="${escapeAttr(c.label)}"></option>`);
      if (out.length >= 50) break;   // an 800-option datalist helps nobody
    }
  }
  listEl.innerHTML = out.join("");
}

/**
 * Resolve what the user typed to a stop. Returns the cluster's primary ATCO —
 * the backend widens the search to the other poles at the same place, so it
 * doesn't matter which side of the road this one is.
 */
function atcoFromPickerValue(value) {
  const raw = (value || "").trim();
  if (!raw) return "";
  const lower = raw.toLowerCase();
  const index = stopPickerIndex();

  const exact = index.find(c => c.label.toLowerCase() === lower);
  if (exact) return exact.atcos[0];

  // Typed a bare name without picking from the list.
  const byName = index.find(c => c.name.toLowerCase() === lower);
  if (byName) return byName.atcos[0];

  // Or pasted a raw ATCO code.
  if (state.stopData[raw]) return raw;
  return "";
}

function setJourneyStatus(msg, isError = false) {
  if (!dom.jcStatus) return;
  dom.jcStatus.textContent = msg;
  dom.jcStatus.classList.toggle("is-error", !!isError);
}

/** Run the calculator for whatever's in the two pickers. */
/**
 * Worked examples for the journey checker.
 *
 * The checker is the strongest thing on this site and it opens as an empty
 * form, which asks the visitor to already know which two stops demonstrate the
 * problem. These are the journeys the campaign is actually about — the Lancing
 * change, the DayRider boundary, the corridors with no through service — one
 * tap each.
 */
function loadJourneyPresets() {
  if (!state._journeyPresetsPromise) {
    state._journeyPresetsPromise = (async () => {
      const res = await fetch("data/journey_presets.json");
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      return Array.isArray(data.presets) ? data.presets : [];
    })().catch(err => {
      state._journeyPresetsPromise = null;
      throw err;
    });
  }
  return state._journeyPresetsPromise;
}

async function renderJourneyPresets() {
  if (!dom.jcPresets) return;
  let presets;
  try {
    presets = await loadJourneyPresets();
  } catch (err) {
    // Examples are a convenience; the form works without them.
    console.warn("Journey presets unavailable:", err);
    return;
  }
  if (!presets.length) return;
  const row = dom.jcPresets.querySelector(".jc-presets-row");
  if (!row) return;

  row.innerHTML = presets.map(p => `
    <button type="button" class="jc-preset" data-preset="${escapeAttr(p.id)}">
      <span class="jc-preset-label">${escapeHtml(p.label)}</span>
      ${p.note ? `<span class="jc-preset-note">${escapeHtml(p.note)}</span>` : ""}
    </button>`).join("");

  row.querySelectorAll("[data-preset]").forEach(btn => {
    btn.addEventListener("click", () => {
      const p = presets.find(x => x.id === btn.dataset.preset);
      if (p) runJourneyPreset(p);
    });
  });
  dom.jcPresets.hidden = false;
}

/**
 * The value to put in a stop picker so it both reads well and resolves.
 *
 * atcoFromPickerValue matches a cluster label first and accepts a bare ATCO
 * last, so the label is preferred — "Old Steine" is what a person recognises —
 * and the code is the fallback when the stop is not in the picker index, which
 * keeps a preset working rather than silently reading as "pick both stops from
 * the suggestions".
 */
function journeyPickerValue(name, atco) {
  const cluster = stopPickerIndex().find(c => c.atcos.includes(atco));
  if (cluster) return cluster.label;
  return atco || name || "";
}

/**
 * Scroll a panel tab so `el` sits near the top of it.
 *
 * Deliberately scrolls the tab's own scroller rather than calling
 * scrollIntoView: the panel is a bottom sheet under 700px, and letting the
 * browser pick a scrolling ancestor there drags the sheet itself rather than
 * its contents. `offset` keeps whatever sits above `el` in view - the point is
 * usually to show a result *and* the control that produced it.
 */
function scrollPanelTo(el, offset = 0) {
  const box = el && el.closest(".panel-tab-content");
  if (!box) return;
  const top = box.scrollTop
            + el.getBoundingClientRect().top
            - box.getBoundingClientRect().top
            - offset;
  const still = window.matchMedia
    && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  box.scrollTo({ top: Math.max(0, top), behavior: still ? "auto" : "smooth" });
}

/**
 * Fill the checker from a preset, run it, and show the answer.
 *
 * A preset is a worked example, so pressing Check afterwards is a step nobody
 * wants: choosing the journey *is* the request. The scroll matters as much as
 * the run - the presets sit above both stop pickers and the button, so on a
 * short panel the result appears entirely below the fold and the click looks
 * like it did nothing.
 *
 * Awaited, because checkJourney fetches a route: scrolling before it resolves
 * would move the panel to an empty result box.
 */
async function runJourneyPreset(preset) {
  if (!dom.jcFrom || !dom.jcTo) return;
  dom.jcFrom.value = journeyPickerValue(preset.from_name, preset.from);
  dom.jcTo.value   = journeyPickerValue(preset.to_name,   preset.to);
  await checkJourney();

  // Only scroll to something worth reading. A failed check leaves the result
  // empty and puts its reason in the status line by the button, so that is
  // what to show instead - scrolling past it to a blank box hides the message.
  const result = dom.jcResult && dom.jcResult.innerHTML.trim()
    ? dom.jcResult
    : dom.jcStatus;
  if (result) scrollPanelTo(result, 72);
}

async function checkJourney() {
  const fromAtco = atcoFromPickerValue(dom.jcFrom && dom.jcFrom.value);
  const toAtco   = atcoFromPickerValue(dom.jcTo && dom.jcTo.value);
  if (dom.jcResult) dom.jcResult.innerHTML = "";

  if (!fromAtco || !toAtco) {
    setJourneyStatus("Pick both stops from the suggestions.", true);
    return;
  }
  if (fromAtco === toAtco) {
    setJourneyStatus("Those are the same stop.", true);
    return;
  }

  const mine = claimRender("journey");
  setJourneyStatus("Checking…");
  clearJourneyLayers();
  await loadTicketZones();
  // Ticket zones can take a moment on a cold load; the reader may already
  // have asked for a different journey, or left the view entirely.
  if (!mine()) return;

  let journey;
  try {
    journey = await apiFetch(
      `/api/journey?from=${encodeURIComponent(fromAtco)}`
      + `&to=${encodeURIComponent(toAtco)}`
      + `&at=${encodeURIComponent(JOURNEY_TIME_ANCHOR)}`);
  } catch (err) {
    if (!mine()) return;
    setJourneyStatus("Couldn't look up that journey — please try again.", true);
    return;
  }
  if (!mine()) return;
  setJourneyStatus("");
  renderJourneyResult(journey, fromAtco, toAtco);
}

/**
 * Operators serving each stop we're costing.
 *
 * A direct journey is simple — one operator runs the whole thing. An
 * interchange journey is where this matters: the two ends can be served by
 * completely different companies, and a ticket valid in the zone but not on
 * the buses that stop there is no use to anybody. Boundary Road sits inside
 * the Brighton DayRider zone and sees only Brighton & Hove buses.
 *
 * Returns one entry per stop: an array of NOCs, or `null` where the API
 * hasn't told us (an older deployment), which callers must not read as "any".
 */
function journeyEndpointOperators(journey, option, pathStops) {
  if (option) return pathStops.map(() => [option.operator].filter(Boolean));
  const from = journey && journey.from && journey.from.operators;
  const to   = journey && journey.to   && journey.to.operators;
  return pathStops.map((_, i) => {
    const ops = i === 0 ? from : (i === pathStops.length - 1 ? to : null);
    return Array.isArray(ops) ? ops : null;
  });
}

/**
 * The operators actually being ridden, where the API says what they are.
 *
 * This is the difference between "an operator serves both ends" and "an
 * operator carries you the whole way", and the two gave opposite answers on
 * Shoreham to the universities. That journey is the 700 (Stagecoach) then the
 * 5B (Brighton & Hove). Both endpoints are served by Brighton & Hove, so the
 * endpoint intersection is {BHBC} — and a citySAVER was offered as covering a
 * journey that begins on a Stagecoach bus, printed directly underneath an
 * itinerary naming the 700.
 *
 * Returns null when the API sent no legs, so callers fall back to endpoints.
 */
function journeyLegOperators(journey) {
  const legs = journey && journey.interchange && journey.interchange.legs;
  if (!Array.isArray(legs) || !legs.length) return null;
  const ops = legs.map(l => l && l.operator).filter(Boolean);
  return ops.length === legs.length ? ops : null;
}

/** The services those legs run as, for saying which leg a ticket fails on. */
function journeyLegServices(journey) {
  const legs = (journey && journey.interchange && journey.interchange.legs) || [];
  return legs.map(l => (l && l.service) || "?").filter(Boolean);
}

/**
 * What this journey costs if you just buy singles.
 *
 * Often the honest answer, and the one the zone machinery cannot see. England
 * caps a single at £3, so two buses each way is £12 return — which is why an
 * interchange costs what it does, and why quoting only day tickets overstates
 * what a careful passenger pays.
 */
function singlesBaseline(meta, legs, returnTrip = true) {
  const sf = meta && meta.single_fare;
  if (!sf || typeof sf.price_pence !== "number" || !legs) return null;
  const each = sf.price_pence;
  const total = each * legs * (returnTrip ? 2 : 1);
  return {
    kind: "singles",
    total,
    legs,
    each,
    returnTrip,
    source_url: sf.source_url || "",
    label: sf.label || "Single fare",
  };
}

/** Every zone touching any stop on the path, de-duped. */
function coveringZoneIds(coverPerStop) {
  const out = [];
  for (const ids of coverPerStop) {
    for (const id of ids) if (!out.includes(id)) out.push(id);
  }
  return out;
}

/** The cheaper of two priced options; either may be null. */
function cheaperOf(a, b) {
  if (!a) return b || null;
  if (!b) return a;
  return b.total < a.total ? b : a;
}

/**
 * The time of day a journey is costed at.
 *
 * What a trip costs must not depend on when you happen to ask. It used to:
 * the checker took whichever bus left first in the service day, which on this
 * coast is the 00:45 N700 — a night service carrying a £2 supplement, and one
 * the all-operator Discovery ticket is not valid on. Every quote was therefore
 * a night fare, at two in the afternoon as readily as at two in the morning.
 *
 * Midday is the anchor: an ordinary hour, on an ordinary service, for the
 * journey most people are actually costing. It is sent to /api/journey and
 * applied again to whatever comes back, so a deployment of the API that
 * predates the parameter still produces a daytime answer.
 */
const JOURNEY_TIME_ANCHOR = "12:00";

/**
 * The option to cost, out of everything the API offered.
 *
 * The departure closest to the anchor on the clock, in either direction. Not
 * "the next one after midday": an API that only offered small-hours trips —
 * which is exactly what the deployed one does until it catches up with the
 * anchor parameter — would answer that with the 00:45, having wrapped all the
 * way round. Nearest-in-either-direction picks the 06:07 instead, which is at
 * least a daytime fare. Where a midday trip exists both rules agree on it.
 *
 * Wrapping is still needed for the other end of the day: 23:50 and 00:10 are
 * twenty minutes apart, not twenty-three hours forty.
 *
 * Options with no departure time rank behind every option that has one, and
 * still beat returning nothing.
 */
function pickRepresentativeOption(options) {
  const list = Array.isArray(options) ? options.filter(Boolean) : [];
  if (!list.length) return null;
  const anchor = hhmmToMinutes(JOURNEY_TIME_ANCHOR) ?? 0;
  const DAY = 24 * 60;
  let best = null, bestKey = Infinity;
  list.forEach((o, i) => {
    const m = hhmmToMinutes(o.depart);
    let key;
    if (m === null) {
      key = DAY + i;
    } else {
      const forward = (((m - anchor) % DAY) + DAY) % DAY;
      key = Math.min(forward, DAY - forward);
    }
    if (key < bestKey) { bestKey = key; best = o; }
  });
  return best;
}

// ============================================================
// THE JOURNEY, DRAWN
//
// The checker could say what a trip costs and how long it takes. Saying it in
// numbers understates it: "700, change at Brunswick Place, then the 5B" is a
// sentence, but two coloured lines that meet at a dot and set off again is the
// shape of the problem. Route liveries are the map's existing vocabulary, so a
// journey drawn in them reads as the buses you would actually be on.
// ============================================================
function clearJourneyLayers() {
  if (!state.map) return;
  for (const layer of state.journeyLayers) {
    if (state.map.hasLayer(layer)) state.map.removeLayer(layer);
  }
  state.journeyLayers = [];
}

function journeyMarker(latlng, label, bg, fg, cls) {
  return L.marker(latlng, {
    interactive: false,
    keyboard: false,
    zIndexOffset: 700,
    icon: L.divIcon({
      className: "journey-marker-icon",
      html: `<span class="journey-marker ${cls}"
                   style="background:${escapeAttr(bg)};color:${escapeAttr(fg)}">${escapeHtml(label)}</span>`,
      iconSize: null,
    }),
  });
}

/**
 * Draw the legs of a journey.
 *
 * `legs` is [{service, operator, stops:[{lat,lon,name}]}] — one for a through
 * bus, two when a change is needed. Each is drawn in its own route's colour so
 * a two-operator journey is visibly two journeys.
 */
function drawJourneyOnMap(legs, changeAt, boardAt, opts) {
  clearJourneyLayers();
  if (!state.map) return;
  const { from, to, notToday } = opts || {};

  if (!state.map.getPane("journeyPane")) {
    const pane = state.map.createPane("journeyPane");
    pane.style.zIndex = 405;
  }

  // The two ends, always. A preset that finds no itinerary used to leave the
  // map exactly as it was — so the reader clicked "Sompting → Brighton Marina"
  // and nothing happened anywhere, which reads as a broken button rather than
  // as the answer. Where the journey is is worth drawing even when how to make
  // it is the thing we cannot answer.
  const ends = [];
  for (const [end, label] of [[from, "From"], [to, "To"]]) {
    if (!end || typeof end.lat !== "number" || typeof end.lon !== "number") continue;
    ends.push([end.lat, end.lon]);
    state.journeyLayers.push(
      journeyMarker([end.lat, end.lon], label, "#141c24", "#ffffff",
                    "journey-marker--end").addTo(state.map));
  }

  if (!legs || !legs.length) {
    // No itinerary at all. Join the ends with a dashed hop so the pair reads
    // as one unanswered journey rather than two unrelated pins, and fit them
    // both so the distance is visible — that distance is the point.
    if (ends.length === 2) {
      state.journeyLayers.push(L.polyline(ends, {
        color: "#141c24", weight: 3, opacity: 0.55, dashArray: "3 8",
        lineCap: "round", interactive: false, pane: "journeyPane",
      }).addTo(state.map));
      fitBoundsAboveSheet(L.latLngBounds(ends).pad(0.15));
    }
    return;
  }

  // The pane sits above the ticket zones (404) so the route being costed reads
  // on top of the shading that explains its price, below the markers.
  const all = ends.slice();
  legs.forEach((leg, i) => {
    const pts = (leg.stops || [])
      .filter(s => typeof s.lat === "number" && typeof s.lon === "number")
      .map(s => [s.lat, s.lon]);
    if (pts.length < 2) return;
    all.push(...pts);
    const colour = getLineColour(leg.service, leg.operator);

    // A casing under the line: liveries are chosen to stand out against each
    // other, not against every basemap, and a bare stroke disappears over the
    // A259 at some zooms.
    state.journeyLayers.push(L.polyline(pts, {
      color: "#ffffff", weight: 9, opacity: 0.85, lineCap: "round",
      lineJoin: "round", interactive: false, pane: "journeyPane",
    }).addTo(state.map));
    state.journeyLayers.push(L.polyline(pts, {
      color: colour, weight: 5, opacity: notToday ? 0.75 : 1,
      lineCap: "round", lineJoin: "round",
      // Dashed when the itinerary belongs to another day. Drawing a Monday
      // journey in the same solid line as one leaving in ten minutes would
      // say something untrue about today.
      dashArray: notToday ? "10 7" : null,
      interactive: false, pane: "journeyPane",
      className: `journey-leg journey-leg--${i}${notToday ? " journey-leg--other-day" : ""}`,
    }).addTo(state.map));

    const fg = textColourOn(colour);
    state.journeyLayers.push(
      journeyMarker(pts[Math.floor(pts.length / 2)],
                    leg.service || "?", colour, fg, "journey-marker--service")
        .addTo(state.map));
  });

  if (changeAt && typeof changeAt.lat === "number") {
    state.journeyLayers.push(
      journeyMarker([changeAt.lat, changeAt.lon], "Change", "#ffffff", "#141c24",
                    "journey-marker--change").addTo(state.map));
  }
  // Where the second bus is caught, when it is not the same stop. Drawn as a
  // dashed hop so the walk is visible as part of the journey rather than a gap
  // the map forgot to join up.
  if (boardAt && typeof boardAt.lat === "number"
      && changeAt && typeof changeAt.lat === "number"
      && (boardAt.atco !== changeAt.atco)) {
    state.journeyLayers.push(L.polyline(
      [[changeAt.lat, changeAt.lon], [boardAt.lat, boardAt.lon]], {
        color: "#141c24", weight: 3, opacity: 0.8, dashArray: "2 6",
        lineCap: "round", interactive: false, pane: "journeyPane",
      }).addTo(state.map));
    state.journeyLayers.push(
      journeyMarker([boardAt.lat, boardAt.lon], "Walk", "#ffffff", "#141c24",
                    "journey-marker--change").addTo(state.map));
  }

  if (all.length > 1) {
    fitBoundsAboveSheet(L.latLngBounds(all).pad(0.12));
  }
}

/**
 * The itinerary in words, above the fare verdict.
 *
 * Two buses and a wait is the thing being complained about, so it is stated
 * plainly with its total rather than left implicit in a price.
 */
function itineraryHtml(interchange, onDay) {
  if (!interchange || !interchange.legs || interchange.legs.length < 2) return "";
  const [one, two] = interchange.legs;
  const chip = (leg) => {
    const bg = getLineColour(leg.service, leg.operator);
    return `<span class="journey-leg-chip" style="background:${escapeAttr(bg)};color:${escapeAttr(textColourOn(bg))}">`
         + `${escapeHtml(leg.service || "?")}</span>`;
  };
  // The two buses often do not share a stop. At Portslade the 1X is around the
  // corner from the 46 and the 1 is five minutes down the road — a walk is part
  // of the journey, not a footnote, so it is said where it happens.
  const board = interchange.board_at || {};
  const metres = interchange.walk_metres || 0;
  // Two poles of one road carry the same name, so naming the destination would
  // read "Southern Cross, then a walk to Southern Cross". Say what is actually
  // happening instead.
  const sameName = board.name && board.name === interchange.change_at.name;
  const walk = !metres ? ""
    : sameName
      ? `, then a <strong>${metres} m walk</strong> to the other stop`
      : `, then a <strong>${metres} m walk</strong> to ${escapeHtml(board.name || "another stop")}`;
  return `
    <div class="journey-itinerary${onDay ? " journey-itinerary--other-day" : ""}">
      <p class="journey-zones-title">What the journey actually is</p>
      ${onDay ? `<p class="journey-other-day">
        There is no such journey today. This is a <strong>${escapeHtml(onDay)}</strong>
        — at weekends these two buses do not connect at all.</p>` : ""}
      <p class="journey-itinerary-line">
        ${chip(one)} <strong>${escapeHtml(one.depart)}</strong> from
        ${escapeHtml((one.stops[0] || {}).name || "the stop")}
        &rarr; ${escapeHtml(interchange.change_at.name)} at
        <strong>${escapeHtml(one.arrive)}</strong>${walk}, wait
        ${interchange.wait_minutes} min, then
        ${chip(two)} arriving <strong>${escapeHtml(two.arrive)}</strong>.
      </p>
      <p class="journey-basis">
        <strong>${interchange.total_minutes} minutes</strong> door to door on two
        buses${one.operator && two.operator && one.operator !== two.operator
          ? `, run by ${escapeHtml(getOperatorName(one.operator))} and
             ${escapeHtml(getOperatorName(two.operator))}`
          : ""}.
      </p>
    </div>`;
}

function renderJourneyResult(journey, fromAtco, toAtco) {
  const host = dom.jcResult;
  if (!host) return;

  const allZones = state.ticketZones || [];
  const byId  = Object.fromEntries(allZones.map(z => [z.id, z]));
  const meta  = state.ticketFaresMeta || {};

  // Path-aware when a direct bus exists; endpoints-only otherwise, and we say
  // which of the two we did.
  const option = pickRepresentativeOption(journey.options);
  const pathStops = option ? option.stops : [
    Object.assign({ atco: fromAtco }, state.stopData[fromAtco] || {}),
    Object.assign({ atco: toAtco },   state.stopData[toAtco]   || {}),
  ];
  const operator = option ? option.operator : "";

  // Who actually runs a bus from each end. On a direct journey every stop is
  // served by the operator running it; on an interchange journey the API
  // tells us per endpoint. Older deployments of the API don't send this, so
  // `null` means "unknown" and is reported as such rather than assumed away.
  const endpointOperators = journeyEndpointOperators(journey, option, pathStops);
  const operatorsKnown = endpointOperators.every(o => o !== null);

  // Drawn before any verdict is chosen, so the map shows the journey whichever
  // branch the fare logic takes — including the ones that end in "we can't say".
  const interchange = journey.interchange || null;
  const notToday    = !option && !!journey.interchange_on;
  drawJourneyOnMap(
    option ? [{ service: option.service, operator: option.operator, stops: option.stops }]
           : (interchange ? interchange.legs : []),
    interchange ? interchange.change_at : null,
    interchange ? interchange.board_at : null,
    { from: state.stopData[fromAtco], to: state.stopData[toAtco], notToday });
  const itinerary = itineraryHtml(interchange, journey.interchange_on || "");

  const usable = pathStops.filter(s => typeof s.lat === "number" && typeof s.lon === "number");
  if (usable.length < 2) {
    host.innerHTML = `<p class="journey-note">We don't have locations for those stops, so we can't check the zones.</p>`;
    return;
  }

  // Evening-only tickets are dropped unless the bus actually leaves in their
  // window — a Gold Nightrider at £4 would otherwise undercut every quote.
  const departAt = option ? option.depart : "";
  // Evening-only products are left out of the costing entirely: a £4 Nightrider
  // is cheap because it's restricted, and letting it set the headline would
  // understate what an ordinary commuter actually pays.
  const allStandardZones = allZones.filter(z =>
    isStandardFareZone(z) && ticketValidAtTime(z, departAt));

  // Tickets no single operator could carry you the whole way on are dropped
  // before anything is costed — see ticketsUsableEndToEnd. `shared` is kept
  // separately because an empty one is itself the answer: the two ends have no
  // operator in common, so no day ticket can span them at any price.
  const shared = operatorsKnown ? commonOperators(endpointOperators) : null;
  // Which buses this journey is actually on, where the API broke it into legs.
  const legOperators = journeyLegOperators(journey);
  const legServices  = journeyLegServices(journey);
  const zones = (operatorsKnown || legOperators)
    ? ticketsUsableEndToEnd(allStandardZones, endpointOperators, legOperators)
    : allStandardZones;
  const droppedForOperator = allStandardZones.filter(z => !zones.includes(z));

  // A ticket valid on some legs but not all is the interesting case, and the
  // one the campaign is about: the fastest way to Brighton is the 700, and a
  // Brighton & Hove ticket covers everything after it but not the 700 itself.
  const partiallyValid = legOperators
    ? droppedForOperator
        .map(z => ({
          zone: z,
          on:  legServices.filter((_, i) => ticketValidOn(z, legOperators[i])),
          off: legServices.filter((_, i) => !ticketValidOn(z, legOperators[i])),
        }))
        .filter(v => v.on.length && v.off.length)
    : [];

  const coverPerStop = usable.map((s, i) =>
    zonesForStop(s, zones, byId, endpointOperators[i] ?? operator));
  const uncovered = coverPerStop.filter(c => c.length === 0).length;

  // Zone tickets answer "how many tickets does this journey need?"; an
  // operator-wide ticket is shown alongside as the alternative, not folded in.
  // Drop tickets no single operator could carry you the whole way on. See
  // ticketsUsableEndToEnd — this is what stops a Metrovoyager being offered
  // for a journey whose only realistic bus is a Stagecoach 700.
  const { zonal, network } = splitZonesByRule(zones);
  const best = cheapestCover(coverPerStop, zonal);
  const networkOption = bestNetworkTicket(coverPerStop, network);

  // Night services charge on top of whichever ticket you hold.
  const service = option ? option.service : "";
  const supplement = serviceSupplement(meta, service, operator);
  // The all-operator ticket competes on price like any other option.
  const unifiedOption = unifiedTicketOption(meta, service);
  // A journey with no direct bus needs at least two buses each way.
  const legs = option ? 1 : 2;
  const singlesOption = singlesBaseline(meta, legs);
  // What an unlimited day costs today, with singles deliberately excluded:
  // the reform asks replace a day ticket, so this is the like-for-like
  // baseline for "what would change". The null is written out rather than
  // dropped — an argument quietly omitted is what produced the £12.50 claim.
  const dayBaseline = cheapestRealOption(best, networkOption, supplement,
                                         unifiedOption, null);

  const routeLine = option
    ? `<p class="journey-note">Following the ${escapeHtml(option.service)} — ${option.stop_count} stops, ${escapeHtml(option.depart)} to ${escapeHtml(option.arrive)}.</p>`
    : `<p class="journey-note">${escapeHtml(journey.note || "No direct bus found.")}</p>${itinerary}`;

  const header = `
    <p class="journey-endpoints">
      <strong>${escapeHtml(journey.from.name || fromAtco)}</strong> →
      <strong>${escapeHtml(journey.to.name || toAtco)}</strong>
    </p>${routeLine}`;

  // ── No zonal ticket spans the journey, but a network one does ──
  // This is a boundary penalty in its own right: the zone tickets stop short,
  // so the passenger is pushed onto the operator's pricier network ticket.
  if (!best && networkOption && uncovered === 0) {
    const only = cheapestRealOption(null, networkOption, supplement,
                                    unifiedOption, singlesOption);
    host.innerHTML = header + `
      <div class="journey-alert journey-alert--penalty">
        <p><strong>No zone day ticket covers this whole journey</strong> — the
        zones stop short of it.</p>
        ${only ? penaltyMoneyHtml(only, meta, service)
               : `<p class="journey-basis">The only ticket that covers it is
                  ${escapeHtml(networkOption.zone.name)}, but we don't have a
                  current price for it.</p>`}
        ${weeklyOptionHtml(allZones, legOperators || shared || [], meta, singlesOption)}
        ${reformComparisonHtml(only, dayBaseline, coveringZoneIds(coverPerStop), byId, meta)}
      </div>` + zoneListHtml(coverPerStop, byId, droppedForOperator, shared, partiallyValid, legServices);
    return;
  }

  // ── The two ends share no operator ────────────────────────
  //
  // Not a gap in our data — a fact about the network, and the sharpest thing
  // this calculator can say. But it is a statement about *operators' own*
  // tickets, and it used to be written as though it were a statement about
  // every ticket in existence: "no single ticket can cover this journey",
  // printed on a route for which this same file configures, prices and
  // recommends the all-operator South Downs Discovery Ticket.
  //
  // An empty operator intersection means no company's own day ticket spans
  // the journey. It does not mean no ticket does. The two questions get
  // separate answers now.
  if (operatorsKnown && shared && shared.length === 0) {
    const singles = singlesOption
      ? Object.assign({}, singlesOption, { noSpanningTicket: !unifiedOption })
      : null;
    const crossing = cheapestRealOption(null, null, supplement, unifiedOption, singles);
    const opPhrase = `${escapeHtml(operatorPhrase(endpointOperators[0]))}
        at one end, ${escapeHtml(operatorPhrase(endpointOperators[endpointOperators.length - 1]))}
        at the other`;

    const verdict = unifiedOption
      ? `<p><strong>No operator's own ticket covers this journey.</strong> No bus
         company runs a service at both ends of it &mdash; ${opPhrase} &mdash; so
         whichever operator's day ticket you buy stops working when you change.</p>`
      : `<p><strong>No single ticket can cover this journey.</strong> No bus company
         runs a service at both ends of it &mdash; ${opPhrase} &mdash; so whichever
         ticket you buy stops working when you change.</p>`;

    host.innerHTML = header + `
      <div class="journey-alert journey-alert--penalty">
        ${verdict}
        ${crossing ? penaltyMoneyHtml(crossing, meta, service)
                   : `<p class="journey-basis">We don't have a current price for
                      any ticket that would cross this boundary, so we're not
                      showing a total.</p>`}
      </div>
      ${reformComparisonHtml(crossing, dayBaseline, coveringZoneIds(coverPerStop), byId, meta)}`
      + zoneListHtml(coverPerStop, byId, droppedForOperator, shared, partiallyValid, legServices);
    return;
  }

  // ── Every ticket was ruled out, and that is an answer ─────
  //
  // Not the same as having no data. `allStandardZones` had tickets; each was
  // dropped because it is not valid on some bus this journey is actually on.
  // Reported as missing coverage, that read as a gap in *our* data about the
  // stops — which is a statement about us, when the true statement is about
  // the network. Two journeys landed here and both were wrong:
  //
  //   Shoreham to the universities — the 700 then the 5B, and no ticket is
  //   valid on both. That is the campaign's whole argument.
  //
  //   Sompting to Brighton Marina — Compass is the only operator serving both
  //   ends, and no *day* ticket we hold is valid on Compass. Compass sells a
  //   weekly Rover, and the all-operator Discovery covers it; neither is a
  //   day ticket, so the zone comparison rightly finds nothing and wrongly
  //   said the stops were uncharted.
  if (allStandardZones.length && !zones.length) {
    const crossing = cheapestRealOption(
      null, null, supplement, unifiedOption,
      singlesOption ? Object.assign({}, singlesOption,
                                    { noSpanningTicket: !unifiedOption }) : null);
    const why = legOperators
      ? `This journey is ${escapeHtml(legPhrase(legServices))}, run by
         ${escapeHtml(operatorPhrase(legOperators))}, and no day ticket is
         valid on all of it.`
      : `${escapeHtml(operatorPhrase(shared && shared.length ? shared : endpointOperators[0]))}
         is the only operator serving both ends, and we hold no day ticket
         valid on it.`;
    host.innerHTML = header + `
      <div class="journey-alert journey-alert--penalty">
        <p><strong>No day ticket covers this whole journey.</strong> ${why}</p>
        ${crossing ? penaltyMoneyHtml(crossing, meta, service) : ""}
        ${weeklyOptionHtml(allZones, legOperators || shared || [], meta, singlesOption)}
      </div>`
      + zoneListHtml(coverPerStop, byId, droppedForOperator, shared, partiallyValid, legServices);
    return;
  }

  // ── No zone data covers part of the path ──────────────────
  if (!best || uncovered > 0) {
    host.innerHTML = header + `
      <div class="journey-alert journey-alert--unknown">
        <p>We don't have ticket-zone coverage for every stop on this journey, so
        we can't say for certain how many tickets it needs.</p>
      </div>` + zoneListHtml(coverPerStop, byId, droppedForOperator, shared, partiallyValid, legServices);
    return;
  }

  // ── One ticket covers it ──────────────────────────────────
  //
  // This used to print the ticket's name and stop. That read as "good news"
  // for a £9.20 Metrovoyager on a journey a passenger could make for £12 in
  // singles, or £7.30 if one ticket were accepted across operators — so it
  // was cheerful about exactly the fare gap the site exists to point at. One
  // ticket covering a journey is worth saying, with its price, and with what
  // a reform would change.
  if (best.zones.length === 1) {
    const z = byId[best.zones[0]];
    const fare = zoneDayFare(z);
    const singles = singlesOption;
    const dayTotal = fare === null ? null : fare + (supplement ? supplement.price_pence : 0);

    // Compare against what you'd really pay, not just against this ticket.
    const cheapest = cheaperOf(
      dayTotal === null ? null : { kind: "zonal", total: dayTotal },
      singles,
    );

    const priceLine = fare === null
      ? `<p>One ticket covers this journey — <strong>${escapeHtml(z.name)}</strong>
         (${escapeHtml(z.operator)}) — but we don't have a current price for it.</p>`
      : `<p>One ticket covers this journey:
         <strong>${escapeHtml(z.name)}</strong> (${escapeHtml(z.operator)})
         at <strong>${formatGbp(fare)}</strong>.</p>`;

    const singlesLine = (singles && cheapest && cheapest.kind === "singles")
      ? `<p class="journey-basis">Singles are cheaper here:
         ${legs} bus${legs === 1 ? "" : "es"} each way at
         ${formatGbp(singles.each)} is ${formatGbp(singles.total)} for a return.</p>`
      : "";

    host.innerHTML = header + `
      <div class="journey-alert journey-alert--ok">
        ${priceLine}
        ${operatorsKnown ? "" : `<p class="journey-basis">We couldn't confirm which
          operators serve these stops, so this assumes the ticket is usable at both
          ends.</p>`}
        ${singlesLine}
      </div>`
      + reformComparisonHtml(cheapest, dayBaseline, coveringZoneIds(coverPerStop), byId, meta)
      + zoneListHtml(coverPerStop, byId, droppedForOperator, shared, partiallyValid, legServices)
      + faresProvenanceHtml(best.zones, byId);
    return;
  }

  // ── Multiple zone tickets needed ──────────────────────────
  //
  // Careful with the money here. The headline has to describe what a passenger
  // would really pay, which is the cheapest option open to them — not the zone
  // combination. On a Worthing-to-Brighton Stagecoach run the two zone tickets
  // come to £12, but a Gold DayRider covers the same journey for £8.50, so
  // claiming £12 would be plainly wrong and would discredit the point.
  const cheapest = cheapestRealOption(best, networkOption, supplement,
                                      unifiedOption, singlesOption);
  const money = cheapest
    ? penaltyMoneyHtml(cheapest, meta, service)
    : `<p class="journey-basis">We don't have current prices for all of these
       tickets, so we're not showing a total. The boundary is real even though
       the figure isn't published here.</p>`;

  host.innerHTML = header + `
    <div class="journey-alert journey-alert--penalty">
      <p><strong>No single zone ticket covers this journey.</strong>
      It crosses ${best.zones.length} ticket zones:</p>
      ${zoneCostHtml(best, byId)}
      ${money}
      ${reformComparisonHtml(cheapest, dayBaseline, best.zones, byId, meta)}
    </div>` + zoneListHtml(coverPerStop, byId, droppedForOperator, shared, partiallyValid, legServices) + faresProvenanceHtml(best.zones, byId);
}

/** The zones this journey crosses, itemised with what each ticket costs. */
function zoneCostHtml(best, byId) {
  const rows = best.zones.map(id => {
    const z = byId[id] || {};
    const fare = zoneDayFare(z);
    return `
      <li>
        <span class="journey-cost-name">${escapeHtml(z.name || id)}
          <span class="journey-zone-op">${escapeHtml(z.operator || "")}</span></span>
        <span class="journey-cost-price">${fare === null ? "—" : formatGbp(fare)}</span>
      </li>`;
  }).join("");

  const total = best.priced
    ? `<li class="journey-cost-total">
         <span class="journey-cost-name">Buying all ${best.zones.length}</span>
         <span class="journey-cost-price">${formatGbp(best.total)}</span>
       </li>`
    : "";

  return `<ul class="journey-costs">${rows}${total}</ul>`;
}

/**
 * What a night service adds to the fare.
 *
 * The N700 needs a £2 add-on on any Stagecoach Day/Night Rider. Ignoring it
 * under-prices exactly the journeys the boundary hurts most, so it's added to
 * every priced option for the operators it applies to.
 */
function serviceSupplement(meta, service, operator) {
  const table = meta && meta.service_supplements;
  const rule = table && table[service];
  if (!rule || typeof rule.price_pence !== "number") return null;
  const ops = rule.applies_to_operators;
  if (Array.isArray(ops) && ops.length && operator && !ops.includes(operator)) return null;
  return rule;
}

/**
 * The all-operator day ticket, as a buyable option, when it applies.
 *
 * The South Downs Discovery Ticket is the only ticket today that crosses
 * operators, so on a journey needing two operators' tickets it can genuinely be
 * the cheapest thing to buy. It's offered on price like anything else — but
 * never on a service it isn't valid for.
 */
function unifiedTicketOption(meta, service) {
  const u = meta && meta.unified_ticket;
  if (!u || u.recommend === false) return null;
  if (typeof u.price_pence !== "number") return null;
  if (Array.isArray(u.not_valid_on_services) && service
      && u.not_valid_on_services.includes(service)) return null;
  return { name: u.name || "all-operator day ticket", pence: u.price_pence, meta: u };
}

/**
 * The cheapest thing a passenger could actually buy for this journey: the
 * combination of zone tickets, a single operator-wide ticket, or the
 * all-operator ticket — whichever costs least. Returns null when nothing is
 * priced.
 *
 * `supplement` is added to the operator tickets, since it's charged on top of
 * whichever one you hold. It is not added to the all-operator ticket, which is
 * excluded from the services that carry a supplement rather than surcharged.
 */
function cheapestRealOption(zonalBest, networkOption, supplement, unifiedOption,
                            singlesOption) {
  const extra = supplement ? supplement.price_pence : 0;
  const options = [];
  // Buying singles is a real option and often the cheapest one, especially on
  // a journey that needs a change. Leaving it out overstated what a careful
  // passenger pays, which makes every saving quoted against it look bigger
  // than it is.
  if (singlesOption) options.push(singlesOption);
  if (zonalBest && zonalBest.priced) {
    options.push({
      kind: "zonal", total: zonalBest.total + extra,
      tickets: zonalBest.zones.length, zone: null, supplement,
    });
  }
  if (networkOption && networkOption.pence !== null) {
    options.push({
      kind: "network", total: networkOption.pence + extra,
      tickets: 1, zone: networkOption.zone, supplement,
    });
  }
  if (unifiedOption) {
    options.push({
      kind: "unified", total: unifiedOption.pence,
      tickets: 1, zone: { name: unifiedOption.name }, supplement: null,
      unified: unifiedOption.meta,
    });
  }
  if (!options.length) return null;
  return options.reduce((a, b) => (b.total < a.total ? b : a));
}

/** Headline cost + the unified-ticket comparison, when it genuinely saves. */
function penaltyMoneyHtml(cheapest, meta, service) {
  const parts = [];

  if (cheapest.kind === "unified") {
    // The one ticket that already crosses operators — and it costs more than
    // the local day tickets it stands in for, which is the argument.
    const src = cheapest.unified && cheapest.unified.source_url;
    const name = escapeHtml(cheapest.zone.name);
    parts.push(`<p class="journey-headline">
      The cheapest ticket covering it is the
      <strong>${src ? `<a href="${escapeAttr(safeUrl(src))}" target="_blank" rel="noopener noreferrer">${name}</a>` : name}</strong>
      at ${formatGbp(cheapest.total)} — the only day ticket that's valid on every
      operator, priced as a day-out rover rather than a local fare.</p>`);
  } else if (cheapest.kind === "singles") {
    // Not a day ticket at all — and on a journey needing a change, usually
    // the cheapest honest answer. Saying so is the point: the cost is the
    // interchange, not the distance.
    const legs = cheapest.legs;
    // Why singles win differs by journey, and asserting the wrong reason is
    // its own kind of wrong answer: on a cross-operator trip no day ticket
    // spans the journey at all, but on a Stagecoach-only run one does — it is
    // just dearer than two capped singles. Only the first is a boundary fact.
    const why = cheapest.noSpanningTicket
      ? ", because no day ticket is valid on every operator that serves these stops"
      : " — cheaper here than any day ticket covering the journey";
    parts.push(`<p class="journey-headline">
      The cheapest way to make this journey is
      <strong>${formatGbp(cheapest.total)}</strong> in single fares —
      ${legs} bus${legs === 1 ? "" : "es"} each way at
      ${formatGbp(cheapest.each)}${why}.</p>`);
    if (cheapest.source_url) {
      parts.push(`<p class="journey-basis">Single fares are capped nationally:
        <a href="${escapeAttr(safeUrl(cheapest.source_url))}" target="_blank"
           rel="noopener noreferrer">${escapeHtml(cheapest.label)}</a>.</p>`);
    }
  } else if (cheapest.kind === "network") {
    parts.push(`<p class="journey-headline">
      The cheapest ticket covering it is a
      <strong>${escapeHtml(cheapest.zone.name)}</strong> at ${formatGbp(cheapest.total)} —
      an operator-wide ticket you have to buy just to cross the boundary.</p>`);
  } else {
    parts.push(`<p class="journey-headline">
      This journey requires multiple tickets costing ${formatGbp(cheapest.total)}.</p>`);
  }

  if (cheapest.supplement) {
    parts.push(`<p class="journey-basis">Includes the
      ${escapeHtml(cheapest.supplement.label || "night-service add-on")}
      (${formatGbp(cheapest.supplement.price_pence)}).
      ${escapeHtml(meta.supplement_basis || "")}</p>`);
  }

  return parts.join("");
}

/**
 * A ticket that's usable for an ordinary all-day journey.
 *
 * Evening-only products (the £4 Gold Nightrider) are excluded from the costing.
 * They're cheap because they're restricted, so letting one set the headline
 * would compare a commuter's real fare against a ticket most journeys can't
 * use, and would understate the boundary rather than describe it.
 */
function isStandardFareZone(zone) {
  return !zone.valid_from_time && !zone.valid_to_time;
}

/** The day fare for a zone in pence, or null when we don't have one. */
function zoneDayFare(zone) {
  const day = zone && zone.fares && zone.fares.adult_day;
  return day && typeof day.price_pence === "number" ? day.price_pence : null;
}

/**
 * Which of the campaign's asks would help on this particular journey, and by
 * how much.
 *
 * Two are computable from the zones the journey actually crosses:
 *
 *  - **Merging zones** applies when the journey needs two tickets from the same
 *    operator — the Worthing/Brighton DayRider case. The asked-for price is
 *    fixed (£6, what each already costs).
 *  - **Cross-operator acceptance** applies when the tickets come from different
 *    operators. Here the price isn't invented: it's the cheapest day ticket
 *    already covering part of the route, because that's what one accepted
 *    ticket would cost.
 */
function reformsForJourney(zoneIds, byId, meta) {
  const reforms = (meta && meta.reforms) || [];
  // No blanket two-zone gate. A journey that one expensive ticket happens to
  // cover still crosses the zones a reform would merge, and refusing to look
  // was why the Boundary Road case showed no reform at all. Each rule below
  // states its own precondition, and nothing is offered unless it is actually
  // cheaper than what you'd pay today.
  if (!zoneIds || !zoneIds.length) return [];

  const operators = new Set(zoneIds.map(id => (byId[id] || {}).operator).filter(Boolean));
  const fares = zoneIds.map(id => zoneDayFare(byId[id])).filter(p => p !== null);
  const cheapestSingle = fares.length ? Math.min(...fares) : null;

  // Only geographic zones can be merged. An operator-wide ticket has no
  // polygon and no boundary — counting the Gold DayRider as a second "zone"
  // is how "if the Worthing and Brighton zones were merged" came to be
  // offered on a journey that never entered the Brighton zone.
  const zonal = zoneIds.filter(id => ((byId[id] || {}).coverage_rule || "polygon") === "polygon");

  const out = [];
  for (const r of reforms) {
    let price = null;
    if (r.applies === "same_operator_multi_zone") {
      if (operators.size !== 1 || zonal.length < 2) continue;
      price = typeof r.price_pence === "number" ? r.price_pence : cheapestSingle;
    } else if (r.applies === "multi_operator") {
      if (operators.size < 2) continue;
      price = typeof r.price_pence === "number" ? r.price_pence : cheapestSingle;
    } else {
      continue;
    }
    if (price === null) continue;
    out.push({ ...r, price_pence: price });
  }
  return out;
}

/**
 * What this journey would cost under the changes the site campaigns for.
 *
 * This is what turns the boundary from an assertion into something a reader can
 * test on their own commute. Each ask is only shown when it would actually make
 * this journey cheaper.
 */
function reformComparisonHtml(cheapest, dayBaseline, zoneIds, byId, meta) {
  if (!cheapest) return "";
  const days = meta && meta.commute_days_per_week;

  // A saving is only a saving against what the reader would otherwise pay.
  // Measuring it against a selected expensive product instead — a £8.50 Gold
  // DayRider on a return trip two £3 capped singles cover for £6 — produced a
  // "£12.50 a week" that no passenger could ever have saved.
  //
  // The ask still stands when it beats no price: these reforms replace an
  // unlimited day ticket, and a day ticket is what someone travelling more
  // than twice buys. That case is shown without a weekly headline figure,
  // because the number would not describe the journey on screen.
  const rows = reformsForJourney(zoneIds, byId, meta).map(r => {
    const saving = cheapest.total - r.price_pence;
    const dayOnly = dayBaseline ? dayBaseline.total - r.price_pence : 0;
    if (saving <= 0 && dayOnly <= 0) return "";

    const perWeek = (saving > 0 && typeof days === "number") ? saving * days : null;
    const headline = saving > 0
      ? `this journey would cost <strong>${formatGbp(r.price_pence)}</strong>${
          perWeek !== null ? ` — saving ${formatGbp(perWeek)} a week` : ""}.`
      : `a day's travel here would cost <strong>${formatGbp(r.price_pence)}</strong>
         instead of ${formatGbp(dayBaseline.total)}.`;
    const caveat = saving > 0 ? "" : `
        <span class="journey-basis">This particular return is already
        ${formatGbp(cheapest.total)} in capped single fares, so the change would
        tell on days you travel more than twice.</span>`;
    return `
      <li>
        <span class="journey-reform-head">${escapeHtml(r.headline)}, ${headline}</span>
        <span class="journey-basis">${escapeHtml(r.detail || "")}</span>${caveat}
      </li>`;
  }).filter(Boolean).join("");
  if (!rows) return "";

  return `
    <div class="journey-reform">
      <p class="journey-zones-title">What we're asking for</p>
      <ul class="journey-reform-list">${rows}</ul>
      <button type="button" class="btn-text journey-reform-link" data-goto-objectives>
        See what we're asking for →
      </button>
    </div>`;
}

/**
 * Which zones the journey passed through, for transparency — and which were set
 * aside, which is the more interesting half.
 *
 * A ticket whose zone covers these stops but which no operator serving both
 * ends accepts is not a ticket you can use, and listing it without saying so is
 * how a Metrovoyager came to look like the answer to a Worthing-to-Hangleton
 * trip that anyone would actually make on a Stagecoach 700.
 */
function zoneListHtml(coverPerStop, byId, dropped, shared, partiallyValid, legServices) {
  const seen = [];
  for (const set of coverPerStop) {
    for (const id of set) if (!seen.includes(id)) seen.push(id);
  }
  // A ticket with no day fare is a different product and must say so. The
  // Compass Rover is a week; listed unlabelled among day tickets it reads as
  // one, which is the sort of quiet inconsistency that discredits the rest.
  const term = (z) => (z.fares && !z.fares.adult_day && z.fares.adult_week)
    ? ` <span class="journey-zone-term">weekly</span>` : "";
  const item = (z) =>
    `<li>${escapeHtml(z.name)} <span class="journey-zone-op">${escapeHtml(z.operator)}</span>${term(z)}</li>`;

  let out = "";
  if (seen.length) {
    out += `<div class="journey-zones"><p class="journey-zones-title">Zones along the way</p>`
         + `<ul>${seen.map(id => item(byId[id])).join("")}</ul></div>`;
  }
  if (dropped && dropped.length) {
    // Where we know the legs, say which one each ticket dies on. This is the
    // campaign argument stated as a fact about one journey: the quickest way
    // to Brighton is the 700, and a Brighton & Hove ticket covers every bus
    // after it and not that one.
    const partial = new Map((partiallyValid || []).map(v => [v.zone.id, v]));
    const detail = (z) => {
      const v = partial.get(z.id);
      if (!v) return "";
      return ` <span class="journey-zone-part">covers the
        ${escapeHtml(v.on.join(" and "))}, not the
        ${escapeHtml(v.off.join(" or "))}</span>`;
    };
    const row = (z) =>
      `<li>${escapeHtml(z.name)} <span class="journey-zone-op">${escapeHtml(z.operator)}</span>${term(z)}${detail(z)}</li>`;

    const who = partial.size
      ? `this journey is ${escapeHtml(legPhrase(legServices))}, and no one
         ticket is valid on all of it`
      : (shared && shared.length)
        ? `only ${escapeHtml(shared.map(getOperatorName).join(" and "))} runs a bus at both ends`
        : `no one company runs a bus at both ends`;
    out += `<div class="journey-zones journey-zones--dropped">
      <p class="journey-zones-title">Not usable for the whole journey</p>
      <ul>${dropped.map(row).join("")}</ul>
      <p class="journey-basis">These cover the ground, but ${who}, so a ticket
      the other operators accept would stop working when you change.</p>
    </div>`;
  }
  return out;
}

/**
 * A weekly ticket that does cover this journey, where one exists.
 *
 * Kept apart from the day-ticket costing on purpose. Compass sells a £30
 * weekly Rover and no day ticket at all, so dividing it by seven to force it
 * into a per-journey comparison would invent a product. It is mentioned as
 * what it is: an answer for somebody travelling this route regularly.
 */
function weeklyOptionHtml(allZones, operators, meta, singlesOption) {
  const ops = normaliseOperators(operators);
  if (!ops.length) return "";
  const weekly = (allZones || []).find(z => {
    const w = z.fares && z.fares.adult_week;
    return w && typeof w.price_pence === "number"
        && ops.every(op => ticketValidOn(z, op));
  });
  if (!weekly) return "";
  const w = weekly.fares.adult_week;
  const days = meta && meta.commute_days_per_week;
  // The singles baseline for *this* journey, which already counts how many
  // buses it takes each way. Recomputing it from the bare single fare assumed
  // one bus each way and quoted £30 for a trip that needs two — making a
  // break-even look like a saving.
  const versus = (days && singlesOption)
    ? ` For a ${days}-day week that is ${formatGbp(w.price_pence)} against
        ${formatGbp(singlesOption.total * days)} in capped singles
        (${singlesOption.legs} bus${singlesOption.legs === 1 ? "" : "es"} each way).`
    : "";
  return `<p class="journey-basis">Travelling this route regularly, the
    <strong>${escapeHtml(weekly.name)}</strong> at
    ${formatGbp(w.price_pence)} a week is valid on the whole of it.${versus}</p>`;
}

/** "the 700 then the 5B" — the services, in the order they are ridden. */
function legPhrase(legServices) {
  const all = (legServices || []).filter(Boolean);
  if (!all.length) return "on more than one operator";
  return all.length === 2 ? `the ${all[0]} then the ${all[1]}`
                          : `the ${all.join(", then the ")}`;
}

/** "Stagecoach South", or "Brighton &amp; Hove Buses and Compass Travel". */
function operatorPhrase(ops) {
  const names = normaliseOperators(ops).map(getOperatorName);
  if (!names.length) return "no operator we know of";
  if (names.length === 1) return names[0];
  return names.slice(0, -1).join(", ") + " and " + names[names.length - 1];
}

/** Every price shown must carry its source and the date it was checked. */
function faresProvenanceHtml(zoneIds, byId) {
  const rows = [];
  const today = new Date().toISOString().slice(0, 10);
  for (const id of zoneIds) {
    const f = byId[id] && byId[id].fares;
    if (!f || !f.source_url) continue;

    // A price the operator publishes differently is not a footnote. Showing
    // one figure as *the* fare, when a sourced table says another, is the
    // kind of thing a councillor's office checks first.
    const c = f.conflicts_with;
    const conflict = (c && typeof c.price_pence === "number")
      ? ` — Stagecoach's <a href="${escapeAttr(safeUrl(c.source_url || f.source_url))}"
          target="_blank" rel="noopener noreferrer">published table</a>
          (${escapeHtml(c.effective_from || "")}) lists
          ${escapeHtml(formatGbp(c.price_pence))}`
      : "";

    // Fares expire. An advocacy site quoting a stale one is worse than one
    // quoting none, so the page says so rather than waiting to be caught.
    const stale = (f.review_by && f.review_by < today)
      ? ` <strong class="journey-stale">due for re-checking</strong>`
      : "";

    rows.push(
      `<li>${escapeHtml(byId[id].name)}: <a href="${escapeAttr(safeUrl(f.source_url))}" ` +
      `target="_blank" rel="noopener noreferrer">published fare</a>, ` +
      `checked ${escapeHtml(f.checked_on || "—")}${conflict}${stale}</li>`);
  }
  if (!rows.length) return "";
  return `<div class="journey-sources"><p class="journey-zones-title">Where these prices come from</p><ul>${rows.join("")}</ul></div>`;
}

function formatGbp(pence) {
  const pounds = Math.round(pence) / 100;
  return "£" + pounds.toFixed(2);
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
  // Editing needs the whole sheet. At the half detent on a 390x844 phone the
  // editor got 348px, and its own header and action area took 199 of them —
  // leaving 149px of scrolling space for 544px of form, with the Name field
  // clipped. Drawing a route is not a glance-at-it task.
  if (isSheetLayout()) setSheetDetent("full");

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
        <!-- A for= attribute, not a bare label. Without it the control has
             no accessible name at all: labels.length was 0, so a screen
             reader announced an unlabelled colour picker. (No backticks in
             this comment: it lives inside a template literal.) -->
        <label for="ed-color">Colour</label>
        <div class="editor-color-row">
          <input id="ed-color" type="color" aria-describedby="ed-color-caption"
                 value="${escapeAttr(d.color || "#1e88e5")}">
          <span class="editor-color-row-caption" id="ed-color-caption">Line colour on the map</span>
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
      <button class="editor-action-btn primary editor-action-btn--focal" id="ed-submit" type="button" ${canExport ? "" : "disabled"}>
        <svg class="icon" aria-hidden="true"><use href="#i-plus"/></svg>
        <span>Submit</span>
      </button>
      <span class="editor-status" id="ed-status"></span>
      <div class="suggest-turnstile" id="ed-turnstile"></div>

      <!-- At the action, not only behind the help button. What a submission
           does — a public issue, immediately, reviewed afterwards — is
           something to know before pressing Submit, not after. -->
      <p class="suggest-privacy editor-submit-note">
        Posts to the project's <strong>public</strong> issue tracker straight
        away, with your name; reviewed before it appears on the site.
        <button type="button" class="btn-text" id="ed-privacy-more">What this means</button>
      </p>

      <div class="editor-help-popover hidden" id="ed-help-popover" role="dialog"
           aria-labelledby="ed-help-title" aria-modal="false">
        <button class="editor-help-close" id="ed-help-close" type="button"
                aria-label="Close help">
          <svg class="icon" aria-hidden="true"><use href="#i-x"/></svg>
        </button>
        <h4 class="editor-help-title" id="ed-help-title">Submitting your proposal</h4>
        <p class="editor-help-blurb">
          Sending your route to the maintainers, who review submissions and turn
          the good ones into live route lines on the map.
        </p>
        <ol class="editor-help-steps">
          <li>
            <svg class="icon editor-help-step-icon" aria-hidden="true"><use href="#i-plus"/></svg>
            <span><strong>Submit</strong> posts your route to the project's public issue
              tracker — <strong>no account needed.</strong> We'll open your proposal in a new
              tab so you can follow what happens to it.</span>
          </li>
          <li>
            <svg class="icon editor-help-step-icon" aria-hidden="true"><use href="#i-copy"/></svg>
            <span><strong>Copy JSON</strong> or the download button give you the raw file, if
              you'd rather open a pull request yourself.</span>
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
  dom.proposalEditor.querySelector("#ed-submit").addEventListener("click", submitProposal);

  // The compact submission notice defers its detail to the help popover
  // rather than repeating five lines of it above every action.
  const privacyMore = dom.proposalEditor.querySelector("#ed-privacy-more");
  if (privacyMore && helpBtn) {
    privacyMore.addEventListener("click", () => helpBtn.click());
  }

  // The editor's markup is rebuilt on open, so the widget mounts here rather
  // than once at boot.
  mountTurnstile(dom.proposalEditor.querySelector("#ed-turnstile"));

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
  ["#ed-copy-btn", "#ed-download-btn", "#ed-submit"].forEach(sel => {
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

/** A status line that stays put.
 *
 *  `setEditorStatus` clears itself after 2.5 seconds, which is right for
 *  "Sending…" and wrong for the one message carrying a link to the reader's
 *  own submission — the only record they have of where it went.
 */
function setEditorStatusHtml(html) {
  const el = dom.proposalEditor && dom.proposalEditor.querySelector("#ed-status");
  if (!el) return;
  clearTimeout(setEditorStatus._t);
  el.innerHTML = html;
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

/**
 * Submit the current draft as a public GitHub issue, via the Worker.
 *
 * This used to be two buttons: a "Via GitHub" path that opened a pre-filled
 * issue URL (and so needed a GitHub account) and a no-account Web3Forms path
 * that emailed the maintainer. The Worker files the issue server-side, so one
 * button now covers both — no account, and still a public, followable record.
 */
async function submitProposal() {
  if (!state.editor) return;
  const obj = draftToProposalJson(state.editor);
  const widget = dom.proposalEditor
    ? dom.proposalEditor.querySelector("#ed-turnstile") : null;
  setEditorStatus("Sending…");

  const result = await postSubmission("proposal", {
    title: obj.name || obj.id,
    name:  obj.author || "",
    // Compacted JSON (no pretty-print) keeps a polyline-heavy draft under the
    // Worker's body cap; it re-formats before writing the issue.
    proposalJson: JSON.stringify(obj),
  }, widget);
  resetTurnstile(widget);

  if (result.ok) {
    // The link stays on the page rather than being opened for the reader:
    // after an await the gesture has expired and the popup is blocked, which
    // loses the only reference they have to their own submission.
    setEditorStatusHtml(submissionReceiptHtml(result));
  } else if (result.reason === "in-flight" || result.reason === "timeout") {
    setEditorStatus(result.message);
  } else if (result.reason === "unconfigured") {
    setEditorStatus("Submissions aren't switched on yet — use Copy JSON for now.");
  } else {
    const msg = /^HTTP \d+$/.test(result.reason || "")
      ? "Couldn't send — try Copy JSON instead."
      : result.reason;
    setEditorStatus(msg);
  }
}

// ============================================================
// NETWORK PLAN (objectives + community ideas)
// ============================================================

// Status tracks how far those who run the buses (councils + operators) have got
// with each objective — this site reports on delivery, it doesn't run services.
const OBJECTIVE_STATUS = {
  not_considered: { label: "Not considered", cls: "not-considered" },
  discussed:      { label: "Discussed",      cls: "discussed"      },
  in_progress:    { label: "In progress",    cls: "in-progress"    },
  delivered:      { label: "Delivered",      cls: "delivered"      },
};

/** Load objectives + community ideas once, the first time the Network plan
 *  view is opened. Memoized like loadProposals so a prefetch + view-open
 *  don't double-fetch. */
function loadNetworkData() {
  if (!state._networkPromise) {
    state._networkPromise = Promise.all([loadObjectives(), loadCommunityIdeas()])
      .then(result => {
        // Both loaders swallow their own errors, so this promise resolves even
        // when nothing loaded. Drop the memo in that case, or the retry the
        // loaders now allow can never actually happen.
        if (!state.objectives || !state.suggestions) state._networkPromise = null;
        return result;
      })
      .catch(err => { state._networkPromise = null; throw err; });
  }
  return state._networkPromise;
}

async function loadObjectives() {
  if (state.objectives) return;
  try {
    const res = await fetch("data/objectives.json");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    state.objectives = Array.isArray(data.objectives) ? data.objectives : [];
  } catch (err) {
    // Left unset, so the next visit to the view retries. Set to `[]` this
    // read as "there are no objectives", which is both wrong and permanent.
    console.warn("Objectives load failed:", err);
    state.objectives = null;
    state.objectivesError = err.message || "Could not load the objectives.";
  }
  renderObjectivesList();
  populateObjectiveSelect();
}

async function loadCommunityIdeas() {
  if (state.suggestions) return;
  try {
    const res = await fetch("data/suggestions.json");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    state.suggestions = Array.isArray(data.suggestions) ? data.suggestions : [];
  } catch (err) {
    console.warn("Suggestions load failed:", err);
    state.suggestions = null;
    state.suggestionsError = err.message || "Could not load community ideas.";
  }
  renderCommunityIdeas();
}

function objectiveStatusMeta(status) {
  return OBJECTIVE_STATUS[status] || { label: status || "", cls: "not-considered" };
}

/**
 * One objective, as a card that expands.
 *
 * The card is a wrapper, not a single button. It used to be one <button>
 * holding everything including the expanded detail — which put anchors and,
 * later, a "write to your councillor" button inside a button. Nested
 * interactive elements are invalid, and in practice the links inside were
 * awkward to activate. Only the summary row toggles now; the detail is a
 * sibling, so anything interactive in it works normally.
 */
function objectiveCardHtml(o) {
  const sel = (o.id === state.selectedObjectiveId);
  const st  = objectiveStatusMeta(o.status);
  const hasLinks = Array.isArray(o.links) && o.links.length > 0;
  const detail = sel ? `
    <div class="proposal-detail">
      ${o.description ? `
        <p class="proposal-detail-heading">Why this matters</p>
        <p class="proposal-detail-body">${escapeHtml(o.description)}</p>
      ` : ""}
      ${hasLinks ? `
        <p class="proposal-detail-heading">Links</p>
        <ul class="proposal-links">
          ${o.links.map(l => `
            <li><a class="proposal-link" href="${escapeAttr(safeUrl(l.url))}"
                   target="_blank" rel="noopener noreferrer">${escapeHtml(l.label || l.url)}</a></li>
          `).join("")}
        </ul>
      ` : ""}
      ${objectiveContactHtml(o)}
    </div>` : "";
  return `
    <div class="proposal-card-wrap ${sel ? "selected" : ""}"
         style="border-left-color:${escapeAttr(o.color || "#444")}">
      <button type="button"
              class="proposal-card ${sel ? "selected" : ""}"
              data-objective-id="${escapeAttr(o.id)}"
              aria-expanded="${sel ? "true" : "false"}">
        <span class="objective-card-head">
          <span class="proposal-card-name">${escapeHtml(o.title || o.id)}</span>
          <span class="status-badge status-${st.cls}">${escapeHtml(st.label)}</span>
        </span>
        <span class="proposal-card-summary">${escapeHtml(o.summary || "")}</span>
        <span class="objective-chips">${objectiveBodyChips(o)}</span>
      </button>
      ${detail}
    </div>`;
}

/** The "write to whoever has to act on this" row inside an expanded card. */
function objectiveContactHtml(o) {
  const bodies = objectiveAuthorities(o);
  if (!bodies.length) return "";
  // Named, so it is obvious before clicking who this is going to reach. The
  // objectives whose lead is an operator still show this when an authority is
  // needed alongside — "this needs the county on board" is a fair reason to
  // write to a county councillor even when the operator leads.
  const who = bodies.map(c => bodyName(c)).join(" or ");
  return `
    <p class="proposal-detail-heading">Make the case</p>
    <button type="button" class="objective-contact-btn"
            data-contact-objective="${escapeAttr(o.id)}">
      <svg class="icon" aria-hidden="true"><use href="#i-mail"/></svg>
      <span>Email your councillor about this</span>
    </button>
    <p class="objective-contact-note">Finds whoever represents your postcode at
      ${escapeHtml(who)}, and drafts a letter you can edit before sending.</p>`;
}

// ── Who is responsible ──────────────────────────────────────
//
// The objectives are grouped by who would actually have to act, because
// "which body do I write to about this?" is the question that makes an
// objective actionable. Responsibility here is genuinely split, and the data
// says so rather than rounding to a single owner:
//
//   lead   — has to do the thing. A list, because some asks have no single
//            owner: fitting audio-visual announcements is every operator's job,
//            not one nominated operator's.
//   shared — can't be done without them. The authority that funds or brokers
//            it, or the council that owns the pavement the stop stands on.
//
// Operators make commercial decisions (routes, timetables, fares, on-bus kit).
// Authorities own the infrastructure and the funding, and broker anything that
// needs more than one operator to agree.
const RESPONSIBLE_BODIES = {
  BHBC: { name: "Brighton & Hove Buses",       kind: "operator",
          url: "https://www.buses.co.uk/contact-us" },
  SCSO: { name: "Stagecoach South",            kind: "operator",
          url: "https://www.stagecoachbus.com/help-and-contact" },
  METR: { name: "Metrobus",                    kind: "operator",
          url: "https://www.metrobus.co.uk/" },
  COMT: { name: "Compass Travel",              kind: "operator",
          url: "https://compass-travel.co.uk/contact-us/" },

  WSCC: { name: "West Sussex County Council",  kind: "authority", colour: "#1a4b82",
          note: "Transport and highway authority for Adur & Worthing, and holder of the Bus Service Improvement Plan.",
          url: "https://www.westsussex.gov.uk/roads-and-travel/travel-and-public-transport/bus-travel/" },
  ADUR_WORTHING: { name: "Adur & Worthing Councils", kind: "authority", colour: "#c07808",
          note: "Owns and maintains many of the bus shelters in the boroughs — 53 of the 108 in Worthing.",
          url: "https://www.adur-worthing.gov.uk/streets-and-travel/report-a-problem/bus-shelters/" },
  BHCC: { name: "Brighton & Hove City Council", kind: "authority", colour: "#6a4ea3",
          note: "Unitary authority at the Brighton end, with its own Enhanced Partnership with the bus company.",
          url: "https://www.brighton-hove.gov.uk/travel-and-road-safety/travel-transport-and-road-safety/brighton-hove-bus-service-improvement-plan-bsip" },
  ESCC: { name: "East Sussex County Council",  kind: "authority", colour: "#0e7c86",
          note: "Neighbouring authority — relevant to anything crossing the county boundary eastwards.",
          url: "https://www.eastsussex.gov.uk/roads-transport/public" },
};

/** Display name for a body code. */
function bodyName(code) {
  const b = RESPONSIBLE_BODIES[code];
  if (b) return b.name;
  return getOperatorName(code) || code;
}

/** Brand colour for an operator, palette colour for an authority. */
function bodyColour(code) {
  const b = RESPONSIBLE_BODIES[code] || {};
  return b.colour || OPERATOR_COLOURS[code] || "#444";
}

/**
 * Group objectives by responsible body.
 *
 * Each body's group lists the objectives it leads first, then the ones where
 * it's needed but isn't the one who has to act. An objective therefore appears
 * in several groups — once as a lead, again as shared — which is the honest
 * shape of the thing rather than duplication to apologise for.
 *
 * Operators come before authorities so the list reads "who runs the buses,
 * then who funds and builds around them".
 */
function groupObjectivesByBody(objectives) {
  const groups = new Map();
  const slot = (code) => {
    if (!groups.has(code)) groups.set(code, { key: code, lead: [], shared: [] });
    return groups.get(code);
  };

  for (const o of objectives) {
    for (const code of o.lead || []) slot(code).lead.push(o);
    for (const code of o.shared || []) slot(code).shared.push(o);
  }

  const kind = (c) => (RESPONSIBLE_BODIES[c] || {}).kind || "operator";
  return [...groups.values()]
    .sort((a, b) => {
      const ka = kind(a.key) === "operator" ? 0 : 1;
      const kb = kind(b.key) === "operator" ? 0 : 1;
      // Within a kind, the body carrying the most work comes first.
      return ka - kb
        || (b.lead.length - a.lead.length)
        || bodyName(a.key).localeCompare(bodyName(b.key));
    })
    .map(g => ({ ...g, label: bodyName(g.key), total: g.lead.length + g.shared.length }));
}

/** Chips on a card naming who leads and who else is involved. */
function objectiveBodyChips(o) {
  const chip = (code, isLead) => {
    const fill = bodyColour(code);
    const fg   = textColourOn(fill);
    // Only the lead carries a filled chip. A body that merely has to agree
    // shouldn't look like the one you write to first.
    return isLead
      ? `<span class="objective-chip objective-chip--lead" style="--chip-bg:${escapeAttr(fill)};--chip-fg:${fg}">${escapeHtml(bodyName(code))}</span>`
      : `<span class="objective-chip">${escapeHtml(bodyName(code))}</span>`;
  };
  return [
    ...(o.lead || []).map(c => chip(c, true)),
    ...(o.shared || []).map(c => chip(c, false)),
  ].join("");
}

/**
 * One collapsible group per responsible body, styled as the Ticket view's
 * operator accordion. Unlike that one, expansion is not exclusive: there are
 * no map layers to collide, and comparing two bodies side by side is the
 * point.
 */
function bodyGroupHtml(g, itemHtml, noun) {
  if (!state.expandedBodies) state.expandedBodies = new Set();
  const open   = state.expandedBodies.has(g.key);
  const fill   = bodyColour(g.key);
  const fg     = textColourOn(fill);
  const body   = RESPONSIBLE_BODIES[g.key] || {};
  const n      = g.total;

  let inner = g.lead.map(itemHtml).join("");
  if (g.shared.length) {
    inner += `<p class="ticket-subgroup-heading">Also needs them on board</p>`
           + g.shared.map(itemHtml).join("");
  }
  if (body.note) {
    inner += `<p class="ticket-operator-footnote">${escapeHtml(body.note)}</p>`;
  }
  if (body.url) {
    inner += `<a class="ticket-zone-link" href="${escapeAttr(safeUrl(body.url))}"
                 target="_blank" rel="noopener noreferrer">Contact ${escapeHtml(g.label)} ↗</a>`;
  }

  return `
    <section class="ticket-operator-group body-group">
      <div class="ticket-operator-card" role="button" tabindex="0"
           data-body="${escapeAttr(g.key)}" aria-expanded="${open ? "true" : "false"}"
           style="--op-bg:${escapeAttr(fill)};--op-fg:${fg}">
        <span class="ticket-operator-name">${escapeHtml(g.label)}</span>
        <span class="ticket-operator-count">${n} ${n === 1 ? noun : noun + "s"}</span>
        <svg class="icon ticket-operator-chevron" aria-hidden="true"><use href="#i-chevron-down"/></svg>
      </div>
      <div class="ticket-operator-zones ${open ? "expanded" : ""}">
        ${inner}
      </div>
    </section>`;
}

// ============================================================
// WRITE TO YOUR COUNCILLOR
//
// An objective names the bodies that would have to act on it. This turns that
// into a person: the reader's postcode goes to postcodes.io, which returns the
// ONS codes for their electoral division and their ward, and
// data/representatives.json maps those codes to the councillors who sit for
// them. The tier follows the body — a county objective finds the county
// councillor, a district one finds the district councillor — because writing
// to the wrong tier about buses is how a letter gets politely forwarded and
// forgotten.
//
// Nothing is sent from here. The draft opens in the reader's own mail client,
// so the site cannot be used to send mail on anyone's behalf, and the letter
// stays theirs to edit. That is deliberate, not a limitation to route around.
// ============================================================

// Which postcodes.io field identifies one of this body's areas, and where that
// body's writ runs. A county council is bounded by its county, a district or
// unitary one by its districts. A body absent from here has no councillors to
// find — operators are companies, and the existing "Contact" link is the right
// route for them.
const BODY_AREA = {
  WSCC:          { field: "ced",  counties: ["West Sussex"] },
  // East Sussex matters because the corridor does not stop at Brighton: the
  // integrated-plan objective needs all three authorities, and a reader in
  // Lewes or Seaford has a county councillor with a say in it.
  ESCC:          { field: "ced",  counties: ["East Sussex"] },
  ADUR_WORTHING: { field: "ward", districts: ["Adur", "Worthing"] },
  BHCC:          { field: "ward", districts: ["Brighton and Hove"] },
};

/** The authorities an objective needs, lead first. */
function objectiveAuthorities(o) {
  const seen = new Set();
  const out = [];
  // Lead before shared: whoever has to act is who you write to first. Both are
  // included, because most objectives here are operator-led and would show no
  // contact route at all if only leads counted.
  for (const code of [...(o.lead || []), ...(o.shared || [])]) {
    if (seen.has(code) || !BODY_AREA[code]) continue;
    seen.add(code);
    out.push(code);
  }
  return out;
}

/** data/representatives.json, fetched once. */
async function loadRepresentatives() {
  if (state.representatives) return state.representatives;
  if (!state._representativesPromise) {
    state._representativesPromise = (async () => {
      try {
        const res = await fetch("data/representatives.json");
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        state.representatives = await res.json();
      } catch (err) {
        console.warn("Representatives unavailable:", err);
        state.representatives = { areas: {} };
      } finally {
        state._representativesPromise = null;
      }
      return state.representatives;
    })();
  }
  return state._representativesPromise;
}

/** UK postcode, loosely: enough to catch a typo, not enough to reject a valid one. */
function normalisePostcode(raw) {
  const s = String(raw || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
  if (!/^[A-Z]{1,2}\d[A-Z\d]?\d[A-Z]{2}$/.test(s)) return null;
  return `${s.slice(0, -3)} ${s.slice(-3)}`;
}

/**
 * Postcode to areas, via postcodes.io.
 *
 * Free, no key, and CORS-enabled, so this runs from the page rather than
 * through the backend — which keeps it off the free-tier budget entirely and
 * means a postcode is never sent to a server this site controls.
 */
async function lookupPostcode(postcode) {
  const url = `https://api.postcodes.io/postcodes/${encodeURIComponent(postcode)}`;
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const d = (await res.json()).result;
  return {
    district: d.admin_district,
    // Null in a unitary area, which is how Brighton & Hove is told apart from
    // the two-tier counties either side of it.
    county: d.admin_county || null,
    // E99999999 is ONS for "not applicable" — a unitary authority has wards
    // but no electoral divisions, so Brighton & Hove always reads that way.
    ced:  d.codes && d.codes.ced !== "E99999999" ? d.codes.ced : null,
    ward: d.codes ? d.codes.admin_ward : null,
  };
}

/** The area entry that answers for `body` at this postcode, or a reason it doesn't. */
function resolveRepresentatives(reps, body, place) {
  const spec = BODY_AREA[body];
  if (!spec) return { error: "no-councillors" };
  // Checked before the code lookup so the reason is specific. Falling through
  // to "we don't hold your area" would be true but useless: a Worthing reader
  // asking about East Sussex is not missing data, they are in the wrong county.
  if (spec.counties && !spec.counties.includes(place.county)) {
    return { error: "out-of-area" };
  }
  if (spec.districts && !spec.districts.includes(place.district)) {
    return { error: "out-of-area" };
  }
  const code = spec.field === "ced" ? place.ced : place.ward;
  const area = code && reps.areas ? reps.areas[code] : null;
  if (!area || area.body !== body) return { error: "not-listed" };
  return { area };
}

/**
 * The draft.
 *
 * Left deliberately incomplete: the paragraph about the sender's own journeys
 * is a prompt, not prose. A councillor's office can tell an identical template
 * from a real letter at a glance, and the identical ones carry less weight —
 * so the most useful thing this can do is give someone a structure and get out
 * of the way.
 */
function councillorDraft(objective, area) {
  const names = area.members.map(m => m.name).join(" and ");
  const lines = [
    `Dear ${names},`,
    "",
    `I am writing as one of your constituents in ${area.name} about bus services in Adur and Worthing.`,
    "",
    objective.title + (objective.summary ? `: ${objective.summary}` : ""),
    "",
    objective.description || "",
    "",
    "[Please add a sentence or two here about how this affects you — which journeys you make, what goes wrong, and what would change if this were fixed. This is the part that carries the most weight, and a letter without it reads as a template.]",
    "",
    "I would be grateful to know your view on this, and whether you would be willing to raise it.",
    "",
    "Yours sincerely,",
    "",
  ];
  return {
    subject: `Bus services in Adur and Worthing: ${objective.title}`,
    body: lines.filter((l, i, a) => !(l === "" && a[i - 1] === "")).join("\n"),
  };
}

/** A mailto: URL, and whether it is short enough to be trusted to survive one. */
function councillorMailto(area, draft) {
  const to = area.members.map(m => m.email).join(",");
  // "@" and "," are legal in a mailto addr-spec (RFC 6068) and several clients
  // mishandle them percent-encoded, so they are put back literally.
  const url = `mailto:${encodeURIComponent(to).replace(/%40/g, "@").replace(/%2C/g, ",")}`
            + `?subject=${encodeURIComponent(draft.subject)}`
            + `&body=${encodeURIComponent(draft.body)}`;
  // Some webmail clients silently truncate a long mailto body. Past this the
  // copy button is the honest primary route rather than a fallback.
  return { url, long: url.length > 1800 };
}

/**
 * The mailto: for the draft as it stands right now.
 *
 * The dialog asks the reader to rewrite the letter in their own words, and
 * then used to hand their email app a link built when the dialog first
 * rendered — so the words that arrived were the generated ones, including any
 * the reader had deliberately deleted. Copy used the edited text; the email
 * button did not. Now both read the same textarea, at the moment they are
 * used.
 */
function councillorMailtoFromDraft(text) {
  const ctx = state.councillorDraftContext;
  if (!ctx) return null;
  return councillorMailto(ctx.area, { subject: ctx.subject, body: String(text ?? "") });
}

/** Whatever is in the draft box, or "" if it isn't on the page. */
function currentCouncillorDraft() {
  const ta = document.getElementById("councillor-draft-text");
  return ta ? ta.value : "";
}

// ── The dialog ──────────────────────────────────────────────

function openCouncillorDialog(objectiveId) {
  const dialog = document.getElementById("councillor-dialog");
  const objective = (state.objectives || []).find(o => o.id === objectiveId);
  if (!dialog || !objective) return;
  state.councillorObjective = objectiveId;
  state.councillorBody = objectiveAuthorities(objective)[0] || null;
  state.councillorResult = null;
  renderCouncillorDialog();
  if (!dialog.open) dialog.showModal();
  const input = document.getElementById("councillor-postcode");
  if (input) input.focus();
}

function renderCouncillorDialog(status) {
  const body = document.getElementById("councillor-body");
  const objective = (state.objectives || []).find(o => o.id === state.councillorObjective);
  if (!body || !objective) return;

  const bodies = objectiveAuthorities(objective);
  const chosen = state.councillorBody || bodies[0];
  const picker = bodies.length > 1 ? `
    <div class="councillor-bodies" role="group" aria-label="Who to write to">
      ${bodies.map(code => `
        <button type="button" class="councillor-body-btn ${code === chosen ? "selected" : ""}"
                data-councillor-body="${escapeAttr(code)}">${escapeHtml(bodyName(code))}</button>
      `).join("")}
    </div>` : "";

  body.innerHTML = `
    <button type="button" class="evidence-close" data-close-councillor
            aria-label="Close">&times;</button>
    <p class="evidence-eyebrow">Make the case</p>
    <h2 class="evidence-title" id="councillor-title">${escapeHtml(objective.title)}</h2>
    <p class="councillor-intro">Your postcode finds the ${
      BODY_AREA[chosen] && BODY_AREA[chosen].field === "ced"
        ? "county councillor for your electoral division"
        : "councillors for your ward"}. Nothing is sent from this site — the
      draft opens in your own email app, and it is yours to change.</p>
    ${picker}
    <form class="councillor-form" id="councillor-form">
      <label class="councillor-label" for="councillor-postcode">Your postcode</label>
      <div class="councillor-row">
        <input class="councillor-input" id="councillor-postcode" name="postcode"
               type="text" inputmode="text" autocomplete="postal-code"
               placeholder="BN11 1AA" spellcheck="false">
        <button class="editor-action-btn primary councillor-submit" type="submit">Find</button>
      </div>
      <p class="councillor-status ${status && status.error ? "is-error" : ""}"
         id="councillor-status" aria-live="polite">${status ? escapeHtml(status.text) : ""}</p>
    </form>
    <div id="councillor-result">${state.councillorResult || ""}</div>
    <p class="councillor-privacy">Postcode lookup is by
      <a class="proposal-link" href="https://postcodes.io/" target="_blank"
         rel="noopener noreferrer">postcodes.io</a>. Your postcode is not stored,
      and never reaches this site's own servers.</p>`;
  bindCouncillorDialog();
}

function bindCouncillorDialog() {
  // Escape already closes a native <dialog>, but a visible control is the one
  // most people reach for, and on touch there is no Escape key at all.
  document.querySelectorAll("[data-close-councillor]").forEach(btn => {
    btn.addEventListener("click", () => {
      const d = document.getElementById("councillor-dialog");
      if (d && d.open) d.close();
    });
  });
  const form = document.getElementById("councillor-form");
  if (form) form.addEventListener("submit", onCouncillorSubmit);
  document.querySelectorAll("[data-councillor-body]").forEach(btn => {
    btn.addEventListener("click", () => {
      state.councillorBody = btn.dataset.councillorBody;
      state.councillorResult = null;
      renderCouncillorDialog();
    });
  });
  const copy = document.getElementById("councillor-copy");
  if (copy) copy.addEventListener("click", onCouncillorCopy);

  // Both actions now read the same box at the moment they are used.
  const openMail = document.getElementById("councillor-open-mail");
  if (openMail) openMail.addEventListener("click", onCouncillorOpenMail);

  // And the "this may be trimmed" warning follows what is actually in it,
  // rather than describing the draft the reader started from.
  const draftBox = document.getElementById("councillor-draft-text");
  const warn = document.getElementById("councillor-long-warn");
  if (draftBox && warn) {
    draftBox.addEventListener("input", () => {
      const built = councillorMailtoFromDraft(draftBox.value);
      warn.hidden = !(built && built.long);
    });
  }
}

/** Hand the reader's current draft to their email app. */
function onCouncillorOpenMail() {
  const built = councillorMailtoFromDraft(currentCouncillorDraft());
  if (!built) return;
  window.location.href = built.url;
}

async function onCouncillorSubmit(e) {
  e.preventDefault();
  const raw = (document.getElementById("councillor-postcode") || {}).value;
  const postcode = normalisePostcode(raw);
  if (!postcode) {
    state.councillorResult = null;
    return renderCouncillorDialog({ error: true, text: "That doesn't look like a UK postcode." });
  }
  state.councillorResult = null;
  renderCouncillorDialog({ text: "Looking up…" });

  const objective = (state.objectives || []).find(o => o.id === state.councillorObjective);
  const body = state.councillorBody;
  let place;
  try {
    place = await lookupPostcode(postcode);
  } catch (err) {
    return renderCouncillorDialog({ error: true, text: "Couldn't reach the postcode service — please try again." });
  }
  if (!place) {
    return renderCouncillorDialog({ error: true, text: `We couldn't find ${postcode}.` });
  }

  const reps = await loadRepresentatives();
  const found = resolveRepresentatives(reps, body, place);
  if (found.error) {
    state.councillorResult = councillorFallbackHtml(body, place, found.error);
    return renderCouncillorDialog();
  }
  state.councillorResult = councillorResultHtml(objective, found.area);
  renderCouncillorDialog();
}

/**
 * Why we have nothing, and what to do instead.
 *
 * Always says which, rather than one shrug for every case: "you live outside
 * the area this body covers" and "we don't hold your ward yet" call for
 * different next steps, and a reader who is told the wrong one gives up.
 */
function councillorFallbackHtml(bodyCode, place, reason) {
  const b = RESPONSIBLE_BODIES[bodyCode] || {};
  const spec = BODY_AREA[bodyCode] || {};
  const where = (spec.counties && place.county) ? place.county : place.district;
  const why = reason === "out-of-area"
    ? `${escapeHtml(where)} is outside ${escapeHtml(bodyName(bodyCode))},
       so they have no councillor for your address.`
    : `We don't have a published address for your area yet.`;
  const link = b.url
    ? `<p><a class="proposal-link" href="${escapeAttr(safeUrl(b.url))}" target="_blank"
             rel="noopener noreferrer">Contact ${escapeHtml(bodyName(bodyCode))} directly ↗</a></p>`
    : "";
  return `<div class="councillor-result"><p class="councillor-empty">${why}</p>${link}</div>`;
}

function councillorResultHtml(objective, area) {
  const draft = councillorDraft(objective, area);
  // Kept so the click handler can rebuild the link from the edited text.
  state.councillorDraftContext = { area, subject: draft.subject };
  const { long } = councillorMailto(area, draft);
  const people = area.members.map(m => `
    <li class="councillor-member">
      <span class="councillor-member-name">${escapeHtml(m.name)}</span>
      ${m.party ? `<span class="councillor-member-party">${escapeHtml(m.party)}</span>` : ""}
      <a class="councillor-member-email" href="mailto:${escapeAttr(m.email)}">${escapeHtml(m.email)}</a>
    </li>`).join("");

  return `
    <div class="councillor-result">
      <p class="proposal-detail-heading">${escapeHtml(area.name)} — ${escapeHtml(area.council)}</p>
      <ul class="councillor-members">${people}</ul>
      <p class="councillor-checked">Addresses published by the council and checked on
        ${escapeHtml(area.checked_on)}.
        <a class="proposal-link" href="${escapeAttr(safeUrl(area.source_url))}" target="_blank"
           rel="noopener noreferrer">Verify ↗</a></p>

      <p class="proposal-detail-heading">Your draft</p>
      <p class="councillor-draft-note">Edit this before you send it. The bracketed
        paragraph is the part that matters most — a letter in your own words
        counts for far more than an identical one.</p>
      <textarea class="councillor-draft" id="councillor-draft-text" rows="12"
                aria-label="Draft email">${escapeHtml(draft.body)}</textarea>
      <div class="councillor-actions">
        ${long ? `
          <button type="button" class="editor-action-btn primary" id="councillor-copy">Copy the draft</button>
          <button type="button" class="editor-action-btn" id="councillor-open-mail">Open in your email app</button>
        ` : `
          <button type="button" class="editor-action-btn primary" id="councillor-open-mail">Open in your email app</button>
          <button type="button" class="editor-action-btn" id="councillor-copy">Copy the draft</button>
        `}
      </div>
      <p class="councillor-warn" id="councillor-long-warn" ${long ? "" : "hidden"}>Copying is
        offered first because this draft is longer than some email apps will
        carry in a link — a few will silently trim it. Opening it directly
        still works in most.</p>
    </div>`;
}

async function onCouncillorCopy() {
  const ta = document.getElementById("councillor-draft-text");
  const btn = document.getElementById("councillor-copy");
  if (!ta || !btn) return;
  try {
    await navigator.clipboard.writeText(ta.value);
    btn.textContent = "Copied";
  } catch {
    // Clipboard access is refused in plenty of ordinary situations (no
    // permission, insecure context). Selecting the text still leaves the
    // reader one keystroke away, which is better than a dead button.
    ta.focus();
    ta.select();
    btn.textContent = "Press Ctrl+C to copy";
  }
  setTimeout(() => { btn.textContent = "Copy the draft"; }, 4000);
}

/** Wire up the body accordion headers inside a container. */
function bindBodyGroups(container, rerender) {
  if (!state.expandedBodies) state.expandedBodies = new Set();
  container.querySelectorAll(".ticket-operator-card").forEach(card => {
    const toggle = () => {
      const code = card.dataset.body;
      // Additive, not exclusive: several bodies can be open at once.
      if (state.expandedBodies.has(code)) state.expandedBodies.delete(code);
      else state.expandedBodies.add(code);
      rerender();
    };
    card.addEventListener("click", toggle);
    card.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggle(); }
    });
  });
}

function renderObjectivesList() {
  if (!dom.objectivesList) return;
  // "None published" and "we could not load them" are different facts, and
  // the second one has something the reader can do about it.
  if (state.objectivesError) {
    dom.objectivesList.innerHTML = `
      <p class="proposals-empty proposals-empty--error">
        Couldn't load the objectives just now.
        <button type="button" class="btn-text" data-retry="objectives">Try again</button>
      </p>`;
    return;
  }
  const objectives = state.objectives || [];
  if (objectives.length === 0) {
    dom.objectivesList.innerHTML = `<p class="proposals-empty">No objectives published yet.</p>`;
    return;
  }

  // The live campaign asks sit open at the top. Everything else is behind a
  // body heading, so the tab opens on what we're actually pushing for rather
  // than on a wall of collapsed headers.
  const featured = objectives.filter(o => o.featured);
  const featuredHtml = featured.length ? `
    <section class="objective-featured">
      <h3 class="objective-group-head">
        <span class="objective-group-name">What we're pushing for now</span>
        <span class="objective-group-count">${featured.length}</span>
      </h3>
      ${featured.map(objectiveCardHtml).join("")}
    </section>
    <p class="objective-bodies-intro">Every objective below, grouped by who would have to act on it.</p>` : "";

  dom.objectivesList.innerHTML = featuredHtml + groupObjectivesByBody(objectives)
    .map(g => bodyGroupHtml(g, objectiveCardHtml, "objective")).join("");

  bindBodyGroups(dom.objectivesList, renderObjectivesList);
  dom.objectivesList.querySelectorAll(".proposal-card").forEach(card => {
    card.addEventListener("click", () => {
      const id = card.dataset.objectiveId;
      state.selectedObjectiveId = (id === state.selectedObjectiveId) ? null : id;
      renderObjectivesList();
    });
  });
  // Bound separately from the card: the contact button is a sibling of the
  // toggle now, not nested inside it, so its click must not collapse the card.
  dom.objectivesList.querySelectorAll("[data-contact-objective]").forEach(btn => {
    btn.addEventListener("click", () => openCouncillorDialog(btn.dataset.contactObjective));
  });
}

function formatIdeaDate(iso) {
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
  } catch { return iso; }
}

function ideaCardHtml(s) {
  const bits = [];
  if (s.area) bits.push(escapeHtml(s.area));
  if (s.name) bits.push(escapeHtml(s.name));
  if (s.date) bits.push(escapeHtml(formatIdeaDate(s.date)));
  const meta = bits.length ? `<span class="idea-card-meta">${bits.join(" · ")}</span>` : "";
  return `
    <div class="proposal-card idea-card">
      <span class="proposal-card-name">${escapeHtml(s.title || "Idea")}</span>
      ${meta}
      <span class="proposal-card-summary">${escapeHtml(s.body || "")}</span>
    </div>`;
}

function renderCommunityIdeas() {
  if (!dom.communityIdeasList) return;
  const ideas = (state.suggestions || []).filter(s => (s.status || "published") === "published");
  if (ideas.length === 0) {
    dom.communityIdeasList.innerHTML =
      `<p class="proposals-empty">No published ideas yet — yours could be the first.</p>`;
    return;
  }

  // Grouped the same way as the objectives: by who would have to act. The
  // submitter picks an area ("Fares", "Stops & shelters") which stays on the
  // card, but they can't be expected to know which body owns the problem —
  // that's assigned when the idea is published.
  const groups = new Map();
  for (const idea of ideas) {
    const code = idea.responsible || "UNASSIGNED";
    if (!groups.has(code)) groups.set(code, { key: code, lead: [], shared: [] });
    groups.get(code).lead.push(idea);
  }

  const kind = (c) => (RESPONSIBLE_BODIES[c] || {}).kind || "operator";
  const ordered = [...groups.values()]
    .sort((a, b) => {
      // Anything not yet assigned sorts last rather than leading the list.
      const ra = a.key === "UNASSIGNED" ? 2 : (kind(a.key) === "operator" ? 0 : 1);
      const rb = b.key === "UNASSIGNED" ? 2 : (kind(b.key) === "operator" ? 0 : 1);
      return ra - rb || b.lead.length - a.lead.length;
    })
    .map(g => ({
      ...g,
      label: g.key === "UNASSIGNED" ? "Not yet assigned" : bodyName(g.key),
      total: g.lead.length,
    }));

  dom.communityIdeasList.innerHTML =
    ordered.map(g => bodyGroupHtml(g, ideaCardHtml, "idea")).join("");
  bindBodyGroups(dom.communityIdeasList, renderCommunityIdeas);
}

/** Fill the "Related objective" select from loaded objectives, preserving any
 *  current selection. */
function populateObjectiveSelect() {
  const sel = dom.suggestObjective;
  if (!sel) return;
  const current = sel.value;
  const opts = ['<option value="">— none in particular —</option>'];
  for (const o of state.objectives || []) {
    const label = o.title || o.id;
    opts.push(`<option value="${escapeAttr(label)}">${escapeHtml(label)}</option>`);
  }
  sel.innerHTML = opts.join("");
  if (current) sel.value = current;
}

function setSuggestStatus(msg, isError = false) {
  if (!dom.suggestStatus) return;
  dom.suggestStatus.textContent = msg;
  dom.suggestStatus.classList.toggle("is-error", !!isError);
}

function setSuggestBusy(busy) {
  if (dom.suggestSubmit) dom.suggestSubmit.disabled = busy;
}

// ─── Turnstile ──────────────────────────────────────────────
// Loaded on demand, the first time a form that needs it becomes visible, so
// the script isn't fetched for the majority of visitors who only use the map.

let _turnstileLoading = null;

function loadTurnstile() {
  if (!CONFIG.TURNSTILE_SITE_KEY) return Promise.resolve(false);
  if (window.turnstile) return Promise.resolve(true);
  if (_turnstileLoading) return _turnstileLoading;

  _turnstileLoading = new Promise((resolve) => {
    const s = document.createElement("script");
    s.src = "https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit";
    s.async = true;
    s.defer = true;
    s.onload  = () => resolve(true);
    s.onerror = () => { _turnstileLoading = null; resolve(false); };
    document.head.appendChild(s);
  });
  return _turnstileLoading;
}

/** Render the widget into a container once. Safe to call repeatedly. */
async function mountTurnstile(container) {
  if (!container || container.dataset.rendered === "1") return;
  const ready = await loadTurnstile();
  if (!ready || !window.turnstile) return;
  window.turnstile.render(container, { sitekey: CONFIG.TURNSTILE_SITE_KEY });
  container.dataset.rendered = "1";
}

/** Read the current token, if the widget is present and solved. */
function turnstileToken(container) {
  if (!container || !window.turnstile) return "";
  try {
    return window.turnstile.getResponse(container) || "";
  } catch { return ""; }
}

/** Reset the widget so a second submission gets a fresh token. */
function resetTurnstile(container) {
  if (!container || !window.turnstile) return;
  try { window.turnstile.reset(container); } catch { /* nothing to reset */ }
}

/**
 * POST a submission to the Worker, which files it as a GitHub issue.
 * Returns {ok, reason, url, number} — `url` links the sender to their issue.
 */
// A form has no built-in idea of "already sending". Without one, an impatient
// second click files a second issue — and the reader, having seen nothing
// happen, has every reason to click again.
const SUBMIT_TIMEOUT_MS = 20_000;
const submissionsInFlight = new Set();

async function postSubmission(kind, fields, turnstileContainer) {
  const endpoint = CONFIG.SUBMIT_ENDPOINT;
  if (!endpoint || endpoint.includes("YOUR-WORKER")) {
    return { ok: false, reason: "unconfigured" };
  }
  if (submissionsInFlight.has(kind)) {
    return { ok: false, reason: "in-flight",
             message: "That's already sending — give it a moment." };
  }
  submissionsInFlight.add(kind);

  // The request had no time limit of its own, so a Worker that never answered
  // left the form saying "Sending…" forever with no way back to it.
  const abort = new AbortController();
  const timer = setTimeout(() => abort.abort(), SUBMIT_TIMEOUT_MS);

  try {
    const res = await fetch(endpoint, {
      method:  "POST",
      signal:  abort.signal,
      headers: { "Content-Type": "application/json", "Accept": "application/json" },
      body:    JSON.stringify({
        kind,
        turnstileToken: turnstileToken(turnstileContainer),
        ...fields,
      }),
    });
    const data = await res.json().catch(() => ({}));
    if (res.ok && data.ok === true) {
      return { ok: true, url: data.url, number: data.number };
    }
    return { ok: false, reason: data.error || `HTTP ${res.status}` };
  } catch (err) {
    if (err && err.name === "AbortError") {
      return { ok: false, reason: "timeout",
               message: "That took too long. Your draft is still here — try again." };
    }
    return { ok: false, reason: err.message || "Network error" };
  } finally {
    clearTimeout(timer);
    submissionsInFlight.delete(kind);
  }
}

/**
 * What a reader is told after a submission is accepted.
 *
 * Two things this has to get right. "Sent" on its own reads as "published",
 * and it is not: a submission becomes an entry on a public tracker and waits
 * for a person to review it. And the link has to stay on the page — it used
 * to be handed to `window.open` after an await, which browsers block as a
 * popup because the user gesture is long gone, so the only record of where
 * the submission went could vanish silently.
 */
function submissionReceiptHtml(result) {
  const link = result.url
    ? ` <a href="${escapeAttr(safeUrl(result.url))}" target="_blank"
           rel="noopener noreferrer">View it${result.number ? ` (#${escapeHtml(String(result.number))})` : ""} ↗</a>`
    : "";
  return `Received for review — it isn't on the site yet.${link}`;
}

/** Validate + file a community suggestion as a public GitHub issue. */
async function submitSuggestion() {
  const form = dom.suggestForm;
  if (!form) return;

  // Honeypot: bots tick the visually-hidden checkbox. Pretend success, drop it.
  const botcheck = form.querySelector("#sg-botcheck");
  if (botcheck && botcheck.checked) {
    setSuggestStatus("Thanks!");
    form.reset();
    return;
  }

  const title   = form.querySelector("#sg-title").value.trim();
  const details = form.querySelector("#sg-details").value.trim();
  if (!title) {
    setSuggestStatus("Please add a one-line summary.", true);
    form.querySelector("#sg-title").focus();
    return;
  }
  if (!details) {
    setSuggestStatus("Please add a few more details.", true);
    form.querySelector("#sg-details").focus();
    return;
  }

  const area      = form.querySelector("#sg-area").value;
  const objective = form.querySelector("#sg-objective").value;
  const name      = form.querySelector("#sg-name").value.trim();

  // The Worker builds the ready-to-publish JSON itself rather than trusting a
  // blob from the browser, and files the issue. No email is collected: the
  // issue is public, so an address here would be published with it.
  const fields = { title, details, area, objective, name };

  setSuggestBusy(true);
  setSuggestStatus("Sending…");
  const result = await postSubmission("idea", fields, dom.suggestTurnstile);
  setSuggestBusy(false);
  resetTurnstile(dom.suggestTurnstile);

  if (result.ok) {
    setSuggestStatus("");
    renderSuggestSuccess(result.url);
    form.reset();
  } else if (result.reason === "unconfigured") {
    setSuggestStatus("Suggestions aren't switched on yet — please try again later.", true);
  } else {
    // The Worker's rejections are already written for humans ("please try
    // again later", "couldn't verify you're human"), so pass them through.
    // Anything that looks like a bare status code gets the generic line.
    const msg = /^HTTP \d+$/.test(result.reason || "")
      ? "Couldn't send — please try again."
      : result.reason;
    setSuggestStatus(msg, true);
  }
}

// ============================================================
// STOP ISSUE REPORTING (departure board → GitHub issue)
// ============================================================

/** Show/hide the report form under the departure board. */
function toggleReportStopForm(show) {
  const form = dom.reportStopForm;
  if (!form) return;
  const open = (show === undefined) ? form.classList.contains("hidden") : show;
  form.classList.toggle("hidden", !open);
  if (dom.reportStopBtn) dom.reportStopBtn.setAttribute("aria-expanded", String(open));
  if (open) {
    setReportStopStatus("");
    mountTurnstile(dom.reportStopTurnstile);
    const details = form.querySelector("#rs-details");
    if (details) details.focus();
  }
}

function setReportStopStatus(msg, isError = false) {
  if (!dom.reportStopStatus) return;
  dom.reportStopStatus.textContent = msg;
  dom.reportStopStatus.classList.toggle("is-error", !!isError);
}

/** Validate + file a stop fault as a public GitHub issue. */
/** Show or hide the bus report form. Mirrors toggleReportStopForm. */
function toggleReportBusForm(show) {
  const form = document.getElementById("report-bus-form");
  const btn  = document.getElementById("report-bus-btn");
  if (!form || !btn) return;
  const open = show === undefined ? form.classList.contains("hidden") : show;
  form.classList.toggle("hidden", !open);
  btn.setAttribute("aria-expanded", open ? "true" : "false");
  if (open) {
    mountTurnstile(document.getElementById("rb-turnstile"));
    const first = form.querySelector("#rb-details");
    if (first) first.focus();
  }
}

function setReportBusStatus(msg, isError = false) {
  const el = document.getElementById("rb-status");
  if (!el) return;
  el.classList.toggle("is-error", !!isError);
  el.textContent = msg;
}

/** Wire the bus report controls. Called after every renderBusTab, because
 *  that rebuilds the panel's innerHTML and takes the listeners with it. */
function bindReportBusForm() {
  const btn = document.getElementById("report-bus-btn");
  if (btn) btn.addEventListener("click", () => toggleReportBusForm());
  const cancel = document.getElementById("rb-cancel");
  if (cancel) cancel.addEventListener("click", () => toggleReportBusForm(false));
  const form = document.getElementById("report-bus-form");
  if (form) {
    form.addEventListener("submit", (e) => {
      e.preventDefault();
      submitBusIssue();
    });
  }
}

function setNewsStatus(msg, isError = false) {
  const el = document.getElementById("nw-status");
  if (!el) return;
  el.classList.toggle("is-error", !!isError);
  el.textContent = msg;
}

/** Community News submissions. Same path as ideas and reports: to the Worker,
 *  into a public issue, published by hand with add_update.py --from-issue. */
function bindNewsForm() {
  const form = document.getElementById("news-form");
  if (!form) return;
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    submitNews();
  });
  mountTurnstile(document.getElementById("nw-turnstile"));
}

async function submitNews() {
  const form = document.getElementById("news-form");
  if (!form) return;

  const botcheck = form.querySelector("#nw-botcheck");
  if (botcheck && botcheck.checked) {
    setNewsStatus("Thanks!");
    form.reset();
    return;
  }

  const val = (id) => (form.querySelector(id)?.value || "").trim();
  for (const [id, message] of [
    ["#nw-title",   "Please add a headline."],
    ["#nw-summary", "Please add a one-line summary."],
    ["#nw-details", "Please say what happened."],
  ]) {
    if (!val(id)) {
      setNewsStatus(message, true);
      form.querySelector(id).focus();
      return;
    }
  }

  const submit = form.querySelector("#nw-submit");
  if (submit) submit.disabled = true;
  setNewsStatus("Sending…");
  const result = await postSubmission("news", {
    title:     val("#nw-title"),
    summary:   val("#nw-summary"),
    details:   val("#nw-details"),
    topic:     val("#nw-topic"),
    sourceUrl: val("#nw-source"),
    name:      val("#nw-name"),
  }, document.getElementById("nw-turnstile"));
  if (submit) submit.disabled = false;
  resetTurnstile(document.getElementById("nw-turnstile"));

  if (result.ok) {
    form.reset();
    const el = document.getElementById("nw-status");
    if (el && result.url) {
      el.classList.remove("is-error");
      el.innerHTML = `Thanks — received for review. It's on the `
        + `<a href="${escapeAttr(safeUrl(result.url))}" target="_blank" `
        + `rel="noopener noreferrer">public tracker</a>; it isn't on this page yet.`;
    } else {
      setNewsStatus("Thanks — received for review. It isn't on this page yet.");
    }
  } else if (result.reason === "in-flight" || result.reason === "timeout") {
    setNewsStatus(result.message, true);
  } else if (result.reason === "unconfigured") {
    setNewsStatus("Submissions aren't switched on yet — please try later.", true);
  } else {
    setNewsStatus(/^HTTP \d+$/.test(result.reason || "")
      ? "Couldn't send — please try again." : result.reason, true);
  }
}

async function submitBusIssue() {
  const form = document.getElementById("report-bus-form");
  if (!form) return;

  const botcheck = form.querySelector("#rb-botcheck");
  if (botcheck && botcheck.checked) {
    setReportBusStatus("Thanks!");
    form.reset();
    return;
  }

  // The panel can only be open on a selected bus, but a report with no
  // service attached would be useless to whoever picks it up.
  const v = state.selectedVehicle;
  if (!v || !v.service_ref) {
    setReportBusStatus("Select a bus first.", true);
    return;
  }

  const details = form.querySelector("#rb-details").value.trim();
  if (!details) {
    setReportBusStatus("Please describe the problem.", true);
    form.querySelector("#rb-details").focus();
    return;
  }

  const submit = form.querySelector("#rb-submit");
  const fields = {
    service:    v.service_ref,
    operator:   v.operator_ref || "",
    vehicleRef: v.vehicle_ref || "",
    category:   form.querySelector("#rb-category").value,
    details,
    name:       form.querySelector("#rb-name").value.trim(),
    // Where the bus was when the report was written, and when — both are
    // what make it checkable against the timetable afterwards.
    lat:        v.latitude,
    lon:        v.longitude,
    when:       new Date().toISOString().slice(0, 16),
  };

  if (submit) submit.disabled = true;
  setReportBusStatus("Sending…");
  const result = await postSubmission("bus_issue", fields,
                                      document.getElementById("rb-turnstile"));
  if (submit) submit.disabled = false;
  resetTurnstile(document.getElementById("rb-turnstile"));

  if (result.ok) {
    form.reset();
    toggleReportBusForm(false);
    const el = document.getElementById("rb-status");
    if (el && result.url) {
      el.classList.remove("is-error");
      el.innerHTML = `Thanks — received for review. `
        + `<a href="${escapeAttr(safeUrl(result.url))}" target="_blank" `
        + `rel="noopener noreferrer">Track it here</a>.`;
    } else {
      setReportBusStatus("Thanks — your report has been received for review.");
    }
  } else if (result.reason === "in-flight" || result.reason === "timeout") {
    setReportBusStatus(result.message, true);
  } else if (result.reason === "unconfigured") {
    setReportBusStatus("Reporting isn't switched on yet — please try later.", true);
  } else {
    setReportBusStatus(/^HTTP \d+$/.test(result.reason || "")
      ? "Couldn't send — please try again." : result.reason, true);
  }
}

async function submitStopIssue() {
  const form = dom.reportStopForm;
  if (!form) return;

  const botcheck = form.querySelector("#rs-botcheck");
  if (botcheck && botcheck.checked) {
    setReportStopStatus("Thanks!");
    form.reset();
    return;
  }

  // The board can only be open on a selected stop, but guard anyway — a
  // report with no stop attached would be useless to whoever picks it up.
  const selected = state.selectedStop;
  if (!selected || !selected.atcoCode) {
    setReportStopStatus("Select a stop first.", true);
    return;
  }

  const details = form.querySelector("#rs-details").value.trim();
  if (!details) {
    setReportStopStatus("Please describe the problem.", true);
    form.querySelector("#rs-details").focus();
    return;
  }

  // Coordinates come from the stop index we already hold, so the report
  // carries a location without another round-trip.
  const pos = state.stopData[selected.atcoCode] || {};

  const fields = {
    stopName: selected.stopName || pos.name || selected.atcoCode,
    atco:     selected.atcoCode,
    category: form.querySelector("#rs-category").value,
    details,
    name:     form.querySelector("#rs-name").value.trim(),
    lat:      pos.lat,
    lon:      pos.lon,
  };

  if (dom.reportStopSubmit) dom.reportStopSubmit.disabled = true;
  setReportStopStatus("Sending…");
  const result = await postSubmission("stop_issue", fields, dom.reportStopTurnstile);
  if (dom.reportStopSubmit) dom.reportStopSubmit.disabled = false;
  resetTurnstile(dom.reportStopTurnstile);

  if (result.ok) {
    form.reset();
    if (result.url) {
      dom.reportStopStatus.classList.remove("is-error");
      dom.reportStopStatus.innerHTML =
        `Thanks — received for review. <a href="${escapeAttr(safeUrl(result.url))}" ` +
        `target="_blank" rel="noopener noreferrer">Track it here</a>.`;
    } else {
      setReportStopStatus("Thanks — your report has been received for review.");
    }
  } else if (result.reason === "in-flight" || result.reason === "timeout") {
    setReportStopStatus(result.message, true);
  } else if (result.reason === "unconfigured") {
    setReportStopStatus("Reporting isn't switched on yet — please try later.", true);
  } else {
    const msg = /^HTTP \d+$/.test(result.reason || "")
      ? "Couldn't send — please try again."
      : result.reason;
    setReportStopStatus(msg, true);
  }
}

/** Replace the form status line with a link to the issue that was just filed. */
function renderSuggestSuccess(url) {
  if (!dom.suggestStatus) return;
  dom.suggestStatus.classList.remove("is-error");
  if (url) {
    // "on the tracker" is the honest description: the issue is public
    // immediately, and appearing on the site is a separate, later decision by
    // a person. Saying "published" here would be wrong in both directions.
    dom.suggestStatus.innerHTML =
      `Thanks — received for review. Your idea is on the ` +
      `<a href="${escapeAttr(safeUrl(url))}" target="_blank" ` +
      `rel="noopener noreferrer">public tracker</a>; it isn't on the site yet.`;
  } else {
    dom.suggestStatus.textContent =
      "Thanks — received for review. It isn't on the site yet.";
  }
}

// ============================================================
// API HELPER
// ============================================================
async function apiFetch(path) {
  if (!CONFIG.API_BASE_URL || CONFIG.API_BASE_URL.includes("YOUR-BACKEND-URL")) {
    throw new Error("API_BASE_URL is not configured. Please edit app.js and set it to your deployed backend URL.");
  }

  // Until something has answered, assume the container may be asleep: allow
  // it the time a cold start actually takes, and say what is happening while
  // it does. Afterwards the ordinary deadline applies — a warm service that
  // stops answering is a fault, not a start-up, and waiting 45 seconds to say
  // so helps nobody.
  const cold = !state.apiEverResponded;
  // Startup fires several calls at once. Counting them means the first to
  // settle does not pull the banner out from under the ones still waiting.
  let slow = null;
  if (cold) {
    _wakingCalls++;
    slow = setTimeout(showWakingBanner, WAKING_ANNOUNCE_MS);
  }

  try {
    const data = await apiRequest(path, cold ? COLD_START_TIMEOUT_MS : API_TIMEOUT_MS);
    state.apiEverResponded = true;
    return data;
  } catch (err) {
    // A timeout while we were waiting out a cold start is its own thing: the
    // service never woke, which is not "you may be offline".
    if (cold && err instanceof ApiError && err.kind === "timeout") {
      throw new ApiError("waking", 0);
    }
    throw err;
  } finally {
    if (slow !== null) clearTimeout(slow);
    if (cold && --_wakingCalls <= 0) {
      _wakingCalls = 0;
      hideWakingBanner();
    }
  }
}

/** Cold calls still outstanding; see the banner bookkeeping in apiFetch. */
let _wakingCalls = 0;

/** One HTTP call with a deadline. Split out of apiFetch so the cold-start
 *  handling above has something to wrap. */
async function apiRequest(path, timeoutMs) {
  const url = CONFIG.API_BASE_URL.replace(/\/$/, "") + path;

  // A deadline of our own. Without one a hung backend leaves the caller
  // awaiting forever and the panel saying "Loading…" with no way out.
  const abort = new AbortController();
  const timer = setTimeout(() => abort.abort(), timeoutMs);

  let response;
  try {
    response = await fetch(url, {
      signal: abort.signal,
      headers: { "Accept": "application/json" },
    });
  } catch (err) {
    if (err && err.name === "AbortError") throw new ApiError("timeout", 0);
    throw new ApiError("offline", 0);
  } finally {
    clearTimeout(timer);
  }

  if (!response.ok) {
    // The upstream body is for the log, not for the passenger. This used to
    // put `API error 503: {"detail":"Service temporarily unavailable"}` on
    // screen, raw JSON and all.
    const text = await response.text().catch(() => "");
    console.warn(`API ${response.status} for ${path}: ${text.slice(0, 200)}`);
    throw new ApiError(response.status === 503 ? "unavailable" : "http",
                       response.status);
  }

  return response.json();
}

// How long any single API call may take before the caller gives up.
const API_TIMEOUT_MS = 15_000;

// The budget for the first call of a session. The backend runs on a free
// instance that spins down after 15 minutes of inactivity; a measured cold
// start took 22.4 seconds to first byte, so the ordinary 15-second deadline
// guaranteed the first visitor after a quiet spell an error. 45 seconds
// leaves room for a slower-than-measured start without waiting all day.
const COLD_START_TIMEOUT_MS = 45_000;

// How long a first call may run before we explain the wait. Short enough that
// nobody stares at an inert screen; long enough that a warm service — which
// answers in about 150 ms — never shows the banner at all.
const WAKING_ANNOUNCE_MS = 2_500;

/**
 * A failure with a kind, so callers can say something useful.
 *
 * `message` is passenger-facing and deliberately says what still works where
 * that is true. The status and the upstream body stay in the console.
 */
class ApiError extends Error {
  constructor(kind, status) {
    super(API_ERROR_TEXT[kind] || API_ERROR_TEXT.http);
    this.name = "ApiError";
    this.kind = kind;
    this.status = status;
  }
}

const API_ERROR_TEXT = {
  timeout:     "That is taking longer than it should. Check your connection, "
             + "then try again.",
  offline:     "Can't reach the live service — you may be offline.",
  unavailable: "The live service is busy just now. Timetabled departures "
             + "should return shortly.",
  http:        "Something went wrong fetching that. Please try again.",
  // Distinct from `timeout`: we know why it was slow and we know it is not
  // the reader's connection. Naming what still works matters more here than
  // usual, because the timetabled parts of the site are served statically and
  // are genuinely unaffected.
  waking:      "The live service didn't finish waking up. The map, routes and "
             + "timetables still work — try live times again in a moment.",
};

// ============================================================
// WAKING UP
// ============================================================
// Free-tier hosting sleeps the API after 15 minutes of inactivity. The first
// person to arrive after that waits about 20 seconds. They used to wait at a
// blank panel and then be told they might be offline; this says what is
// actually happening.

function showWakingBanner() {
  if (!dom.wakingBanner) return;
  dom.wakingText.textContent =
    "Waking the live service up — this takes about 20 seconds after a quiet "
    + "spell. Timetables and routes are ready now.";
  dom.wakingBanner.classList.remove("hidden");
}

function hideWakingBanner() {
  if (!dom.wakingBanner) return;
  dom.wakingBanner.classList.add("hidden");
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
  // Stagecoach South. The comment here used to say "Stagecoach orange" over
  // a pure #0000FF — not a brand colour, not orange, and the harshest thing
  // on the map for the operator that runs most of the network.
  "SCSC": "#00447C",   // Stagecoach corporate blue
  "SCSO": "#00447C",

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
// ============================================================
// ROUTE ICONS
// Per-route liveries, keyed "NOC:ROUTE". Keyed by operator as well as
// route number on purpose: route numbers repeat across operators, and an
// icon is a far stronger claim than a colour — a Stagecoach 1 wearing
// Brighton & Hove pink would be simply wrong.
//
// Checked before OPERATOR_ICONS, so an operator without a route entry
// still gets its generic bus.
// ============================================================
const ROUTE_ICONS = {
  // Route 1 family — pink
  "BHBC:1":   "icons/BHBC-1.png",
  "BHBC:1X":  "icons/BHBC-1.png",

  // Route 7 — purple
  "BHBC:7":   "icons/BHBC-7.png",

  // Route 49 — blue, shared with 3X
  "BHBC:49":  "icons/BHBC-49.png",
  "BHBC:3X":  "icons/BHBC-49.png",

  // Coaster family — lime green over blue
  "BHBC:11X": "icons/BHBC-12.png",
  "BHBC:12":  "icons/BHBC-12.png",
  "BHBC:12A": "icons/BHBC-12.png",
  "BHBC:12X": "icons/BHBC-12.png",
  "BHBC:13":  "icons/BHBC-12.png",
  "BHBC:13X": "icons/BHBC-12.png",
  "BHBC:14":  "icons/BHBC-12.png",
  "BHBC:14C": "icons/BHBC-12.png",
  // 27 and its variants run in Coaster colours too. Note that no 27 appears
  // in the GTFS feed at all — like CSS, it exists here only as a livery for
  // whatever the live vehicle feed reports, which is where it shows up. 27B
  // and 27C have not been seen yet; they are listed so that they are right
  // the first time one does, rather than a fortnight later.
  "BHBC:27":  "icons/BHBC-12.png",
  "BHBC:27B": "icons/BHBC-12.png",
  "BHBC:27C": "icons/BHBC-12.png",

  // Regency 28 / 29 — magenta and purple
  "BHBC:28":  "icons/BHBC-28.png",
  "BHBC:29":  "icons/BHBC-28.png",
  "BHBC:29X": "icons/BHBC-28.png",
  "BHBC:29B": "icons/BHBC-28.png",

  // City Sightseeing — the open-top tour bus. It runs as a Brighton & Hove
  // vehicle and shows on the live map tagged CSS, but has no timetable in
  // the GTFS feed, so it exists here only as a livery.
  "BHBC:CSS": "icons/BHBC-CSS.png",
};

/** The livery for this service, falling back to the operator's generic bus.
 *
 *  A miss on "N29" retries on "29", because a night service is the same route
 *  after dark — same vehicles, same livery. Without the retry only the routes
 *  someone remembered to list twice looked right: the N1 wore its pink because
 *  ROUTE_ICONS happened to carry a "BHBC:N1" entry, while the N14 and N29 fell
 *  all the way through to Brighton & Hove's generic red bus. The retry cannot
 *  over-claim, because it only ever resolves to a livery already asserted for
 *  that exact route on that exact operator. */
function iconForService(operatorRef, service) {
  const svc = String(service || "").trim().toUpperCase();
  return ROUTE_ICONS[`${operatorRef}:${svc}`]
      || ROUTE_ICONS[`${operatorRef}:${stripNightPrefix(svc)}`]
      || OPERATOR_ICONS[operatorRef];
}

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
  "SCSC": "#002F55",
  "SCSO": "#002F55",
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

  // 7 / N7 — purple. Sampled from the route's own livery (icons/BHBC-7.png)
  // rather than taken from the colour guide like its neighbours: the guide
  // lists 7 as red, and a red chip above a purple bus is worse than a
  // citation that says where it actually came from.
  "7":   "#622aa9",
  "N7":  "#622aa9",

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

  // 27 / 27B / 27C — Coaster lime, not the older standalone green (#00A651).
  // Kept in step with the livery above: a Coaster-painted bus wearing a
  // different green on its badge and its route line reads as a bug rather
  // than as two deliberate choices.
  "27":  "#7AB800",
  "27B": "#7AB800",
  "27C": "#7AB800",

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
  // Night variants inherit the day route's colour — see iconForService. The
  // table lists N1, N5, N7, N25 and N700 but not N12, N14, N29 or N48, so
  // those four were drawing in the operator's colour instead of their own.
  return ROUTE_COLOURS[key]
      || ROUTE_COLOURS[stripNightPrefix(key)]
      || getOperatorColour(operatorRef);
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
  // Night variant of a branded route — the N14 is the 14 after dark.
  if (ROUTE_COLOURS[stripNightPrefix(key)]) return ROUTE_COLOURS[stripNightPrefix(key)];
  // Fall back to the operator's brand colour before the hash. Lets every
  // Compass route render burgundy, Stagecoach blue, B&H red, etc., without
  // needing a per-route entry in ROUTE_COLOURS.
  if (operator && OPERATOR_COLOURS[operator]) return OPERATOR_COLOURS[operator];
  let h = 0;
  for (let i = 0; i < key.length; i++) {
    h = ((h << 5) - h + key.charCodeAt(i)) | 0;
  }
  const hue = ((h % 360) + 360) % 360;
  // Hex, not hsl(): every other branch here returns hex, and pickTextOn used
  // to answer "light" for anything that was not a six-digit hex — silently,
  // with no way to tell a real answer from a shrug. Unknown routes were
  // getting white text on mid-tone fills at 3.0:1 for exactly that reason.
  return hslToHex(hue, 55, 42);
}

/** hsl() in degrees/percent to a #rrggbb string. */
function hslToHex(h, s, l) {
  s /= 100; l /= 100;
  const k = (n) => (n + h / 30) % 12;
  const a = s * Math.min(l, 1 - l);
  const f = (n) => l - a * Math.max(-1, Math.min(k(n) - 3, Math.min(9 - k(n), 1)));
  const hx = (n) => Math.round(f(n) * 255).toString(16).padStart(2, "0");
  return `#${hx(0)}${hx(8)}${hx(4)}`;
}

// Return 'light' or 'dark' text depending on background luminance (WCAG-ish).
/**
 * Which of the two label colours to draw on a coloured fill — "dark" meaning
 * the callers draw as black, "light" meaning white.
 *
 * This used to weigh the background with the YIQ brightness formula against a
 * 0.62 threshold, which is not contrast and does not agree with it: several
 * route liveries came out at 3.0:1 because the brighter-looking option won on
 * brightness while losing on contrast. Since one of black or white always
 * clears AA on any background, a failing chip meant the wrong one was picked.
 *
 * So: compute WCAG relative luminance and take whichever candidate actually
 * contrasts more. Same two candidates, same return values, correct choice.
 */
/* The two candidates, and their relative luminances. Pure black rather than
   the old #1a1a1a for a specific reason: whichever of black and white
   contrasts better is guaranteed at least 4.58:1 on ANY background, which
   clears AA everywhere. #1a1a1a only guaranteed 4.17:1, and the hash-derived
   route colours landed right in that gap. */
const TEXT_DARK     = "#000000";
const TEXT_LIGHT    = "#ffffff";
const TEXT_ON_DARK  = 0;
const TEXT_ON_LIGHT = 1;

function relativeLuminance(r, g, b) {
  const f = (v) => {
    v /= 255;
    return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
  };
  return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b);
}

function contrastRatio(l1, l2) {
  const hi = Math.max(l1, l2), lo = Math.min(l1, l2);
  return (hi + 0.05) / (lo + 0.05);
}

/** The label colour itself, for callers that just want to draw it. */
function textColourOn(bgHex) {
  return pickTextOn(bgHex) === "dark" ? TEXT_DARK : TEXT_LIGHT;
}

function pickTextOn(bgHex) {
  let h = String(bgHex || "").replace("#", "").trim();
  if (h.length === 3) h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];
  if (!/^[0-9a-fA-F]{6}$/.test(h)) return "light";
  const bg = relativeLuminance(
    parseInt(h.slice(0, 2), 16),
    parseInt(h.slice(2, 4), 16),
    parseInt(h.slice(4, 6), 16),
  );
  return contrastRatio(bg, TEXT_ON_DARK) >= contrastRatio(bg, TEXT_ON_LIGHT)
    ? "dark" : "light";
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

/** Where an actual complaint about a bus should go.
 *
 * The report form files a public record; it does not reach the operator, and
 * saying so is worth nothing unless the page also says where to go instead.
 * Only operators whose contact page we have are linked — the rest get the
 * honest "their own contact page", because guessing a URL here would send
 * someone's complaint into a 404.
 */
const OPERATOR_COMPLAINT_URLS = {
  "SCSO": "https://www.stagecoachbus.com/help-and-contact",
  "SCSC": "https://www.stagecoachbus.com/help-and-contact",
  "BHBC": "https://www.buses.co.uk/contact-us",
  "CMPA": "https://compass-travel.co.uk/contact-us/",
  "COMT": "https://compass-travel.co.uk/contact-us/",
  "METR": "https://www.metrobus.co.uk/contact-us",
};

function operatorComplaintLinkHtml(operatorRef) {
  const url = OPERATOR_COMPLAINT_URLS[operatorRef];
  const name = escapeHtml(getOperatorName(operatorRef));
  return url
    ? `<a href="${escapeAttr(safeUrl(url))}" target="_blank" rel="noopener noreferrer">${name}</a> directly`
    : `${name} directly`;
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

// Front-carriage livery images from Realtime Trains (credit: realtimetrains.co.uk).
// Keyed by ATOC code → [stockClass, operatorPath, frontCarriageFile].
// onerror on the <img> silently falls back to the operator colour pill.
const RTT_LIVERY_BASE = "https://www.realtimetrains.co.uk/assets/train-svgs";
const RTT_FRONT_CAR = {
  TL: ["700", "tl", "DMC"],
  SN: ["377", "sn", "DMSL"],
  GX: ["387", "gx", "DMSL"],
};
function rttFrontCarUrl(atocCode) {
  const e = RTT_FRONT_CAR[(atocCode || "").toUpperCase()];
  return e ? `${RTT_LIVERY_BASE}/${e[0]}/${e[1]}/${e[2]}.png` : null;
}

function railTrainDivIcon(colour, label, opts = {}) {
  const bg = colour || RAIL_TRAIN_FILL_DEFAULT;
  const cls = ["rail-train-icon-wrap"];
  if (opts.pulse) cls.push("rail-train-icon-wrap--pulse");
  const liveryHtml = opts.liveryUrl
    ? `<img class="rail-train-livery" src="${escapeAttr(safeUrl(opts.liveryUrl))}" alt="">`
    : "";
  const html = `
    <div class="${cls.join(' ')}" style="--rail-train-bg:${bg};">
      <div class="rail-train-icon" style="background:${bg};">
        ${liveryHtml}
        <div class="rail-train-pulse" aria-hidden="true"></div>
        <span class="rail-train-label">${escapeHtml(label || "")}</span>
      </div>
    </div>`;
  return L.divIcon({
    html, className: "rail-train-divicon",
    iconSize:   [0, 0],
    iconAnchor: [0, 0],
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
    // Not `[]`. An empty array is a perfectly good answer to "which stations
    // are there", and the guard at the top of this function treats it as one —
    // so a single transient failure meant "no stations" for the rest of the
    // page's life, with nothing the reader could do about it. Leaving the
    // state unset is what lets the next call try again.
    console.warn("Failed to load rail stations:", err);
    state.railStations = null;
    state.railStationsError = err.message || "Could not load rail stations.";
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
  revealPanelForSelection();
  setActiveTab("stop");
  showPanelState("rail");      // shows the dedicated rail host, hides bus children
  renderRailBoard();           // shows "loading" rows while we fetch
  dom.departurePanel.scrollIntoView({ behavior: "smooth", block: "end" });

  // Same slot as the bus board — they write to the same panel, so the later
  // selection must win whichever kind it is.
  const stillOurs = claimRender(PANEL_SLOT);
  const mine = () => stillOurs()
    && state.selectedRailStation && state.selectedRailStation.crs === crs;

  try {
    const data = await apiFetch(`/api/rail-departures?crs=${encodeURIComponent(crs)}`);
    if (!mine()) return;
    state.railBoard = data;
  } catch (err) {
    console.warn("Rail board fetch failed:", err);
    if (!mine()) return;
    state.railBoard = { error: err?.message || "Could not load rail data." };
  } finally {
    // `finally` runs for the superseded request too, and clearing the loading
    // flag or repainting there would undo the board that replaced it.
    if (mine()) {
      state.railBoardLoading = false;
      renderRailBoard();
    }
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
  const opTxt  = textColourOn(opCol);
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
  state.selectedRailServices[uid] = {
    date,
    calling: null,
    marker: null,
    lastPos: null,
    displayed: null,
    target: null,
    flownTo: false,
    // Pin the board CRS at Track time — the user may switch boards while
    // tracking; the pill should keep showing the time at the board the
    // service was selected from.
    boardCrs: state.selectedRailStation?.crs || null,
  };
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

    const colour    = railOperatorColour(svc.atocCode);
    const label     = railPillLabel(svc, entry);
    if (!entry.liveryUrl) entry.liveryUrl = rttFrontCarUrl(svc.atocCode);

    const justCreated = !entry.marker;
    if (justCreated) {
      ensureRailPanes();
      // Place the marker at the freshly computed position and seed `displayed`
      // so the rAF loop has a starting point to ease from. The rAF loop is the
      // ONLY steady-state position-writer — subsequent refreshes update target
      // only, so transitions glide instead of snapping.
      entry.displayed = { lat, lon };
      entry.marker = L.marker([lat, lon], {
        icon: railTrainDivIcon(colour, label, { pulse: true, liveryUrl: entry.liveryUrl }),
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

      // One-shot flyTo when the marker first materialises (whether on Track
      // click or a later refresh that finally produced an in-bbox actual).
      if (!entry.flownTo) {
        state.map.flyTo([lat, lon],
                        Math.max(state.map.getZoom(), 13),
                        { duration: 0.8 });
        entry.flownTo = true;
      }
    } else if (entry.markerColour !== colour || entry.markerLabel !== label) {
      // Colour/label change only — no setLatLng; rAF loop owns position.
      entry.marker.setIcon(railTrainDivIcon(colour, label, { liveryUrl: entry.liveryUrl }));
      entry.markerColour = colour;
      entry.markerLabel  = label;
    }
    // Title (tooltip) may have changed if A/B advanced; cheap to update.
    const tipEl = entry.marker.getElement();
    if (tipEl) tipEl.setAttribute("title", railTrainTitle(svc, entry.target));

    entry.lastSeg = { fromCrs: A.loc.crs, toCrs: B.loc.crs };
  }
}

// Build the pill label: destination name + due time at the board CRS.
// e.g. "Bedford 04:12". Fallback chain: name → CRS → headcode so the pill
// never shows nothing.
function railPillLabel(svc, entry) {
  const dest = svc.destination && svc.destination[0];
  const destName = (dest && (dest.description || dest.crs)) || "";
  const boardTime = railBoardTimeFor(svc, entry.boardCrs);
  const name = destName || svc.headcode || (svc.uid || "").slice(-3);
  return boardTime ? `${name} ${boardTime}` : name;
}

function railBoardTimeFor(svc, boardCrs) {
  if (!boardCrs || !svc || !Array.isArray(svc.locations)) return "";
  const loc = svc.locations.find(l => l && l.crs === boardCrs);
  if (!loc) return "";
  // Prefer departure for non-terminus, arrival for terminus.
  const td = pickRailDisplayTime(loc.departure && (loc.departure.actual
              || loc.departure.forecast || loc.departure.scheduled)
              ? loc.departure : loc.arrival);
  return formatRailTime(td.iso);
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
// Runs while at least one service is tracked. Each frame, the rAF loop
// computes where the train *should* be (the raw target) and eases the
// *displayed* position toward it. This is the ONLY steady-state writer
// of marker.setLatLng — recomputeRailPositions deliberately doesn't
// touch position after marker creation, so a 30s refresh that advances
// A→B doesn't snap the dot.
const RAIL_EASE = 0.18;   // ~85% of distance closed in ~10 frames @ 60Hz
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
      const target = railPosAt(entry.target, now);
      const cur = entry.displayed || target;
      entry.displayed = {
        lat: cur.lat + (target.lat - cur.lat) * RAIL_EASE,
        lon: cur.lon + (target.lon - cur.lon) * RAIL_EASE,
      };
      entry.marker.setLatLng([entry.displayed.lat, entry.displayed.lon]);
      entry.lastPos = { lat: entry.displayed.lat, lon: entry.displayed.lon };
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

/**
 * A URL safe to put in an href or src, or "" if it is not one.
 *
 * Escaping a URL stops it breaking out of the attribute; it does nothing
 * about what the URL then does. `javascript:alert(1)` survives every HTML
 * escape intact and runs on click. Most of these values come from curated
 * data files, but proposals and updates are published through a moderation
 * script, and a moderator reading JSON should not have to be the check.
 *
 * Relative paths are kept — `media/updates/x.jpg` is how every image on the
 * site is referenced — and anything with a scheme must use one of the three
 * that make sense here.
 */
const SAFE_URL_SCHEMES = new Set(["http:", "https:", "mailto:"]);

function safeUrl(url) {
  const raw = String(url == null ? "" : url).trim();
  if (!raw) return "";
  // A control character before the colon is how "java\tscript:" gets past a
  // naive prefix test; the URL parser below would also normalise it away, but
  // relative paths never reach the parser, so strip them first.
  const cleaned = raw.replace(/[\u0000-\u001F\u007F]/g, "");
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(cleaned)) {
    // No scheme: a relative path, a fragment or a protocol-relative URL.
    // The last of these inherits the page's scheme, which is fine.
    return cleaned;
  }
  try {
    const parsed = new URL(cleaned, "https://worthingbrightonbus.co.uk/");
    return SAFE_URL_SCHEMES.has(parsed.protocol) ? cleaned : "";
  } catch {
    return "";
  }
}

function escapeAttr(str) {
  // HTML attribute escaping, and only that. This used to mix in JavaScript
  // string escaping — a backslash before an apostrophe — because values were
  // interpolated into inline onclick handlers. That put a literal backslash
  // into every stop named like "Church Place St Mary's Hall", and left
  // ampersands alone, so "&#39;" survived to be decoded by the HTML parser
  // and then read as JavaScript. There are no inline handlers now, so there
  // is no second context to escape for.
  return escapeHtml(str);
}
