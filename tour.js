/* The guided tour: a walk through the site for anyone meeting the project for
 * the first time (the industry at Euro Bus Expo first, and whoever follows the
 * link afterwards).
 *
 * Words come from data/tour.json, which also drives tour.html (the slides and
 * the PDF), so they are written once. Numbers never come from that file: each
 * {placeholder} is filled here from data/boundary_evidence.json and the
 * published journey-times index, so a weekly rebuild cannot leave the tour
 * quoting old figures (.claude/skills/evidence-provenance).
 *
 * The tour's own page (view=tour) is only the way in: the opening, a start
 * button and a contents list. The tour itself is a walk through the real site.
 * Each view a step opens is a stop: the tour opens that view, with the same
 * map, panel and controls a visitor would use, driven by the same functions
 * their own tap would run (openDepartures, runJourneyPreset,
 * journeyTimesResolvePair, jtEnterDelays), and a card beside it names the
 * step, gives its headline figure and says what to try. A ring marks the
 * control the card is talking about. The card never blocks the page: the
 * reader can use everything on screen, and the tour waits.
 *
 * The reader stays in the tour while it is open: the menu still says so, the
 * address names the stop (tour=2-3), and Back walks back through it. Only the
 * menu, the header, the address and the card's own End button leave.
 *
 * Loaded after app.js and tour-figures.js, and uses their helpers. Draws nothing
 * of its own on the map.
 */
"use strict";

const tour = {
  data: null, promise: null, values: null, bound: false, rendered: false,
  lastStep: 0, seq: 0, minimised: null, timer: 0, frame: 0, watching: false,
  scrolledFor: "", frontUntil: 0, ringKey: "",
};

/** What a "Show me" can do. data/tour.json is tested against this list. */
const TOUR_ACTIONS = ["board", "live-map", "evidence", "route-view", "tickets", "planner",
                      "journey-times", "delay-map", "link"];

/** The actions that open a view, and so are stops on the walk. The others
 *  (the evidence over the page, another page in a new tab) are buttons on the
 *  card and leave the walk where it is. */
const TOUR_VIEWS = ["board", "live-map", "route-view", "tickets", "planner",
                    "journey-times", "delay-map"];

/** The site's view each of those opens. */
const TOUR_VIEW_OF = {
  board: "live", "live-map": "live", planner: "live", "route-view": "improvements",
  tickets: "tickets", "journey-times": "journeytimes", "delay-map": "journeytimes",
};

/** The control each stop's hint is about, for the ring. The live map has
 *  none: the hint is about the buses, which are the whole map. */
const TOUR_TARGETS = {
  "route-view":    '.panel-mode[data-mode="improvements"] .service-mode-toggle',
  "board":         "#departures-container",
  "tickets":       "#jc-result",
  "planner":       "#plan-journey",
  "journey-times": ".jt-simple-chart",
  "delay-map":     "#jt-delay-panel",
};

/** Stops whose target is the point of the stop: it goes to the top of the
 *  panel, front and centre, rather than only into view. */
const TOUR_FRONT = ["journey-times"];

// Lancing to Brighton: the council line at Portslade and the coast either
// side of it. On a phone the map stops show this between the card and the
// sheet (tourFrameMap).
const TOUR_COAST = [[50.815, -0.335], [50.85, -0.125]];

/** How far up the phone's sheet comes for each stop: as low as it can while
 *  still showing what the ring points at, so the map keeps the rest. The
 *  journey's chart is the stop, so it has the whole sheet. */
const TOUR_DETENTS = {
  "live-map": "peek", "delay-map": "peek", "route-view": "peek", "board": "half",
  "tickets": "half", "planner": "half", "journey-times": "full",
};

function loadTour() {
  if (!tour.promise) {
    tour.promise = fetch("data/tour.json")
      .then(res => (res.ok ? res.json() : null))
      .catch(() => null)
      .then(data => { tour.data = data; return data; });
  }
  return tour.promise;
}

/** Shown to everyone once its copy is approved; until then, only with
 *  ?preview=1, so drafts can be checked on the live site. */
function tourOffered(data, preview = (typeof previewEnabled === "function" && previewEnabled())) {
  return Boolean(data && (!data.preview_only || preview));
}

async function tourInitMenu() {
  const data = await loadTour();
  const item = document.querySelector('#section-nav-menu [data-mode="tour"]');
  if (!item) return;
  item.hidden = !tourOffered(data);
  if (!data || !data.menu) return;
  const label = item.querySelector(".section-nav-text");
  const badge = item.querySelector(".section-nav-new");
  if (label) label.textContent = data.menu.label;
  if (badge) badge.textContent = data.menu.badge;
}

function tourText(text, values) {
  return escapeHtml(tourFill(text, values));
}

function tourSourcesHtml(sources) {
  const safe = (sources || []).filter(s => /^(https:\/\/|data\/)/.test(String(s.url || "")));
  if (!safe.length) return "";
  return `<p class="tour-sources">Sources: ${safe.map(s =>
    `<a href="${escapeAttr(safeUrl(s.url))}"${/^https:/.test(s.url) ? ' target="_blank" rel="noopener noreferrer"' : ""}>${
      escapeHtml(s.label)}</a> (checked ${escapeHtml(tourDay(s.checked_on, true))})`).join("; ")}.</p>`;
}

/** The figures every card quotes, fetched once. */
function tourLoadValues() {
  if (!tour.values) {
    tour.values = Promise.all([
      loadBoundaryEvidence().catch(() => null),
      loadJourneyTimesIndex().catch(() => null),
    ]).then(async ([evidence, index]) =>
      tourFigureValues(evidence, index, await tourLoadIdentification(index)));
  }
  return tour.values;
}

/** The published identification counts, through the index that names the
 *  current build's file. Null when either is unavailable. */
async function tourLoadIdentification(index) {
  const entry = index && index.artifacts && index.artifacts["identification.json"];
  if (!entry) return null;
  try {
    const res = await fetch(`${CONFIG.JOURNEY_TIMES_BASE}/${entry.file}`);
    return res.ok ? await res.json() : null;
  } catch {
    return null;
  }
}

// ── The way in (view=tour) ──────────────────────────────────────

/** The opening, a button that starts the walk, the slides and the PDF, and
 *  the steps by name for anyone who wants to go straight to one. The steps'
 *  words are on the cards, not here: this page is short on purpose. */
function tourPanelHtml(d, values) {
  const steps = d.steps || [];
  const modes = (d.opening.modes || []).map(m => m.href
    ? `<a class="tour-mode" data-tour-mode="${escapeAttr(m.id)}" href="${escapeAttr(safeUrl(m.href) + (d.preview_only && /^tour\.html$/.test(m.href) ? "?preview=1" : ""))}"${
        m.id === "pdf" ? " download" : ""}>${escapeHtml(m.label)}</a>`
    : `<button type="button" class="tour-mode tour-mode--primary" data-tour-mode="${escapeAttr(m.id)}"
        data-tour-start="0"><svg class="icon" aria-hidden="true"><use href="#i-play"/></svg>${escapeHtml(m.label)}</button>`).join("");
  return `
    <section class="tour-opening">
      <h2 class="tour-headline">${escapeHtml(d.opening.headline)}</h2>
      <p class="tour-lead">${tourText(d.opening.lead, values)}</p>
      <div class="tour-modes">${modes}</div>
      <p class="tour-share"><button type="button" class="tour-copy" data-tour-copy>Copy a link to this tour</button>
        <span class="tour-copy-status" aria-live="polite"></span></p>
    </section>
    <nav class="tour-contents" aria-labelledby="tour-contents-title">
      <h3 class="tour-contents-title" id="tour-contents-title">Or go straight to a step</h3>
      <ol class="tour-contents-list">
        ${steps.map((s, i) => `<li><button type="button" class="tour-contents-item" data-tour-start="${i}">
          <span class="tour-contents-num" aria-hidden="true">${i + 1}</span>
          <span class="tour-contents-name">${escapeHtml(s.title)}</span>
        </button></li>`).join("")}
      </ol>
    </nav>`;
}

// ── The walk ────────────────────────────────────────────────────

function tourStopAvailable(t) {
  return t.action !== "delay-map" || (typeof jtDelayMapAvailable === "function" && jtDelayMapAvailable());
}

/** The tour as the reader walks it: each view a step opens is a stop of its
 *  own, in the step's order, and a step that opens no view is one stop. */
function tourStops(d, available = tourStopAvailable) {
  const out = [];
  ((d && d.steps) || []).forEach((s, i) => {
    const views = (s.try || [])
      .map((t, j) => ({ t, j }))
      .filter(x => TOUR_VIEWS.includes(x.t.action) && available(x.t));
    if (!views.length) out.push({ step: i, tryIndex: null });
    for (const x of views) out.push({ step: i, tryIndex: x.j });
  });
  return out;
}

/** Where a demo sits on the walk. A step named without a view (an old link,
 *  or a view reached some other way inside the tour) is its first stop. */
function tourStopIndex(stops, demo) {
  if (!demo) return -1;
  const exact = stops.findIndex(x => x.step === demo.step && x.tryIndex === demo.tryIndex);
  return exact >= 0 ? exact : stops.findIndex(x => x.step === demo.step);
}

/** A button for one of the step's other things to see. One that opens a view
 *  is a toggle, pressed while its view is open; a link says it opens a new
 *  tab, since the tour stays here. */
/** A step's headline: the claim it supports, what was measured, then the
 *  figure, or for a comparison each figure on its own labelled row with a bar
 *  to scale, so the difference is seen as well as read. Bars start at zero;
 *  percentages are drawn out of 100, other figures against the largest. */
function tourHeadlineHtml(head, values) {
  const figs = Array.isArray(head.figures) && head.figures.length
    ? head.figures : [{ value: head.value, label: head.label }];
  const claim = head.claim ? `<p class="tour-coach-claim">${tourText(head.claim, values)}</p>` : "";
  const measure = head.measure ? `<p class="tour-coach-measure">${tourText(head.measure, values)}</p>` : "";
  if (figs.length < 2) {
    return `${claim}<p class="tour-coach-figure"><strong>${tourText(figs[0].value, values)}</strong>
      <span>${tourText(figs[0].label, values)}</span></p>${measure}`;
  }
  const filled = figs.map(f => tourFill(f.value, values));
  const nums = filled.map(v => parseFloat(String(v).replace(/[^0-9.]/g, "")));
  const max = filled.every(v => /%\s*$/.test(v)) ? 100 : Math.max(...nums.filter(Number.isFinite));
  const rows = figs.map((f, i) => {
    const pct = Number.isFinite(nums[i]) && max > 0 ? Math.min(100, Math.round((nums[i] / max) * 100)) : null;
    return `<li class="tour-compare-row${i === 0 ? " tour-compare-row--lead" : ""}">
      <span class="tour-compare-label">${tourText(f.label, values)}</span>
      <strong class="tour-compare-value">${tourText(f.value, values)}</strong>
      ${pct == null ? "" : `<span class="tour-compare-bar" aria-hidden="true"><span data-pct="${pct}"></span></span>`}
    </li>`;
  }).join("");
  return `${claim}${measure}<ul class="tour-compare">${rows}</ul>`;
}

function tourTryHtml(t, i, j, className, pressed = false) {
  const toggle = TOUR_VIEWS.includes(t.action) ? ` aria-pressed="${pressed}"` : "";
  const away = t.action === "link" && !/^mailto:/i.test(String(t.href || ""))
    ? `<span class="visually-hidden"> (opens in a new tab)</span>` : "";
  return `<button type="button" class="${className}" data-tour-try="${i}:${j}"${toggle}>${
    escapeHtml(t.label)}${away}</button>`;
}

/** The card for one stop. */
function tourCoachHtml(d, demo, values, stops) {
  const steps = d.steps;
  const step = steps[demo.step];
  const at = tourStopIndex(stops, demo);
  const t = demo.tryIndex == null ? null : (step.try || [])[demo.tryIndex];
  const firstOfStep = at <= 0 || stops[at - 1].step !== demo.step;
  const kind = /^[a-z]+$/.test(step.badge && step.badge.kind) ? step.badge.kind : "about";
  const tries = step.try || [];
  const views = tries
    .map((x, j) => ({ x, j }))
    .filter(v => TOUR_VIEWS.includes(v.x.action) && stops.some(s => s.step === demo.step && s.tryIndex === v.j));
  const extras = tries.map((x, j) => ({ x, j })).filter(v => !TOUR_VIEWS.includes(v.x.action));
  const head = step.headline;
  // A headline's claim says on the card what the lead would, so the lead
  // waits under More.
  const claimed = Boolean(head && head.claim && firstOfStep);
  const last = at === stops.length - 1;
  const pct = stops.length ? Math.round(((at + 1) / stops.length) * 100) : 100;
  // The lead opens the step; later stops of the same step say only what is
  // new, and keep the lead under More. On a phone the card keeps to the
  // figure, what to try and the way on, so the map stays in sight: the
  // badge, the lead and the extras are under More there (style.css shows
  // one copy or the other, by where the card is placed).
  const lead = (where) => `<p class="tour-coach-lead tour-coach-lead--${where}">${tourText(step.lead, values)}</p>`;
  const badge = (where) => `<p class="tour-badge tour-badge--${kind} tour-coach-${where}">${
    tourText(step.badge && step.badge.label, values)}</p>`;
  const extrasHtml = (where) => extras.length ? `<div class="tour-coach-extras tour-coach-${where}">${extras.map(v =>
    tourTryHtml(v.x, demo.step, v.j, "editor-action-btn tour-coach-extra")).join("")}</div>` : "";
  const more = [
    badge("dock"),
    extrasHtml("dock"),
    lead(firstOfStep && !claimed ? "again" : "more"),
    (step.points || []).length ? `<ul class="tour-points">${step.points.map(p =>
      `<li>${tourText(p, values)}</li>`).join("")}</ul>` : "",
    tourSourcesHtml(step.sources),
  ].join("");
  return `
    <div class="tour-coach-top">
      <p class="tour-coach-eyebrow">Step ${demo.step + 1} of ${steps.length}${t
        ? `<span class="tour-coach-chapter">: ${escapeHtml(step.title)}</span>` : ""}</p>
      <button type="button" class="tour-coach-icon tour-coach-min" data-tour-coach="min" aria-expanded="true"
        aria-controls="tour-coach-body" aria-label="Hide the tour's card"><svg class="icon"
        aria-hidden="true"><use href="#i-chevron-down"/></svg></button>
      <button type="button" class="tour-coach-icon" data-tour-coach="end" aria-label="End the tour"><svg
        class="icon" aria-hidden="true"><use href="#i-x"/></svg></button>
    </div>
    <div class="tour-coach-progress" aria-hidden="true"><span data-pct="${pct}"></span></div>
    <h2 class="tour-coach-title" id="tour-coach-title" tabindex="-1">${escapeHtml(t ? t.label : step.title)}</h2>
    <div class="tour-coach-body${t ? "" : " tour-coach-body--solo"}" id="tour-coach-body">
      ${badge("wide")}
      ${head && firstOfStep ? tourHeadlineHtml(head, values) : ""}
      ${firstOfStep && !claimed ? lead("main") : ""}
      ${t && t.hint ? `<p class="tour-coach-hint"><svg class="icon" aria-hidden="true"><use href="#i-info"/></svg><span>${
        tourText(t.hint, values)}</span></p>` : ""}
      ${views.length > 1 ? `<div class="tour-coach-views" role="group" aria-label="In this step">${views.map(v =>
        tourTryHtml(v.x, demo.step, v.j, "tour-coach-view target-dense", v.j === demo.tryIndex)).join("")}</div>` : ""}
      ${extrasHtml("wide")}
      ${more ? `<details class="tour-coach-more"><summary>More on this step</summary>${more}</details>` : ""}
    </div>
    <div class="tour-coach-nav">
      <button type="button" class="editor-action-btn tour-coach-back" data-tour-coach="back"${at <= 0 ? " disabled" : ""}><svg
        class="icon" aria-hidden="true"><use href="#i-arrow-left"/></svg>Back</button>
      <span class="tour-coach-count">${at + 1} of ${stops.length}</span>
      <button type="button" class="editor-action-btn primary tour-coach-next" data-tour-coach="next">${
        last ? "Finish" : "Next"}${last ? "" : `<svg class="icon" aria-hidden="true"><use href="#i-arrow-left"/></svg>`}</button>
    </div>`;
}

/** `tour=3-2` in the address: step 3, its second Show me (1-based, as read). */
function tourDemoFromAddress(value, view) {
  const m = /^(\d{1,2})(?:-(\d{1,2}))?$/.exec(value == null ? "" : String(value));
  if (!m || view === "tour") return null;
  const step = Number(m[1]) - 1;
  const tryIndex = m[2] == null ? null : Number(m[2]) - 1;
  if (step < 0 || (tryIndex != null && tryIndex < 0)) return null;
  return { step, tryIndex };
}

/** Put the reader at a stop (or, with null, take them out of the tour). */
function tourSetDemo(demo) {
  state.tourDemo = demo
    ? { step: demo.step, tryIndex: demo.tryIndex == null ? null : demo.tryIndex }
    : null;
  if (state.tourDemo) tour.lastStep = state.tourDemo.step;
  syncSectionNavToViewMode();
  tourSyncCoach();
}

/** Called by setViewMode before every change of view. The menu, the header
 *  and the address are the reader choosing to go somewhere (`chosen`), and
 *  leave the tour; anything else that changes view from inside the tour, such
 *  as the evidence's "see the buses" or a link between views, stays in it. */
function tourCarry(from, to, chosen) {
  if (to === "tour" || chosen) {
    if (!state.tourDemo) return;
    tourSetDemo(null);
    // Choosing the view already open changes nothing else, so nothing else
    // would take the step out of the address.
    if (from === to) pushUrlState();
    return;
  }
  if (from === "tour" && !state.tourDemo) tourSetDemo({ step: tour.lastStep, tryIndex: null });
}

// ── Running a stop ──────────────────────────────────────────────

/** Switch view and wait until it has drawn, so what runs next lands in it. */
async function tourGo(mode) {
  setViewMode(mode);
  try { await state.viewApplied; } catch { /* the view reports its own errors */ }
}

async function tourRunAction(t) {
  switch (t.action) {
    case "board":
      await tourGo("live");
      await openDepartures(t.atco, t.name);
      return;
    case "live-map":
      await tourGo("live");
      closePanel();
      return;
    case "evidence":
      // A dialog over whatever is open; the map behind it shows the line.
      await openBoundaryEvidence();
      return;
    case "route-view":
      await tourGo("improvements");
      return;
    case "tickets": {
      await tourGo("tickets");
      const presets = await loadJourneyPresets().catch(() => []);
      const preset = presets.find(p => p.id === t.preset);
      if (preset) await runJourneyPreset(preset);
      return;
    }
    case "planner": {
      await tourGo("live");
      closePanel();
      const section = document.getElementById("plan-journey");
      if (section && !section.hidden) scrollPanelTo(section, 8);
      return;
    }
    case "journey-times":
      await tourGo("journeytimes");
      jtEntry.section = "journey";
      hideDelayMapPanel();
      jtEntry.a = t.from;
      jtEntry.b = t.to;
      jtSyncChrome();
      await journeyTimesResolvePair();
      return;
    case "delay-map":
      await tourGo("journeytimes");
      if (!jtDelayMapAvailable()) return;
      jtEntry.section = "delays";
      jtEnterDelays();
      renderJourneyTimes();
      return;
    case "link":
      // Another page opens beside the tour rather than in place of it.
      if (/^mailto:/i.test(t.href)) location.href = t.href;
      else if (/^(https:\/\/|[a-z0-9_-]+\.html(#[a-z0-9_-]*)?$)/i.test(t.href)) window.open(t.href, "_blank", "noopener");
      return;
    default:
  }
}

/** Walk to a stop: show its card at once, then open its view. */
async function tourGoTo(stepIndex, tryIndex) {
  const d = await loadTour();
  const step = d && d.steps && d.steps[stepIndex];
  if (!step) return;
  const t = tryIndex == null ? null : (step.try || [])[tryIndex];
  if (t && !TOUR_VIEWS.includes(t.action)) return;
  const seq = ++tour.seq;
  tour.frontUntil = Date.now() + 3000;
  if (tour.minimised == null) tour.minimised = window.innerHeight < 500;
  tourSetDemo({ step: stepIndex, tryIndex });
  // Each stop is a page of its own in the history, so Back walks back. A
  // stop that opens another view gets its page from setViewMode. One that
  // stays in this view gets it here, first: its action (a board, a closed
  // panel) rewrites the address in place, and would otherwise overwrite the
  // stop before.
  const view = t ? TOUR_VIEW_OF[t.action] : (state.viewMode === "tour" ? "live" : state.viewMode);
  if (view === state.viewMode) pushUrlState({ major: true });
  track(`tour-stop-${step.id}-${tryIndex == null ? 0 : tryIndex + 1}`);
  if (t) await tourRunAction(t);
  // A stop with nothing to open shows over the view the reader is on, or
  // over the live map when they start from the tour's own page.
  else if (state.viewMode === "tour") { await tourGo("live"); closePanel(); }
  if (seq !== tour.seq || !state.tourDemo) return;
  if (t && typeof isSheetLayout === "function" && isSheetLayout()) setSheetDetent(TOUR_DETENTS[t.action] || "half");
  await tourSyncCoach();
  // The stop's page is made above; bring it up to date with what opened.
  pushUrlState();
  if (t) await tourFrameMap(t.action, seq);
}

/** On a phone the card covers the top of the map and the sheet its foot, so
 *  a stop about the whole map fits the coast it is about (Lancing, the
 *  council line at Portslade, Brighton) into the strip between them. Views
 *  that frame the map themselves (a journey, a board's stop) already leave
 *  the card room: fitBoundsAboveSheet and its kin count it (app.js). */
async function tourFrameMap(action, seq) {
  if (!state.map || typeof isSheetLayout !== "function" || !isSheetLayout()) return;
  if (action !== "route-view" && action !== "live-map") return;
  // The sheet takes 180ms to reach its new height.
  await new Promise(r => setTimeout(r, 220));
  if (seq !== tour.seq || !state.tourDemo) return;
  fitBoundsAboveSheet(L.latLngBounds(TOUR_COAST), { padding: [16, 8] });
}

/** Next (+1) or Back (-1). Past the last stop, the tour ends where it is. */
function tourMove(delta) {
  const stops = tourStops(tour.data);
  const at = tourStopIndex(stops, state.tourDemo);
  const next = stops[at + delta];
  if (next) { tourGoTo(next.step, next.tryIndex); return; }
  if (delta > 0) tourEnd();
}

/** A button on the card: another view of the step, the evidence, or a link. */
async function tourRun(stepIndex, tryIndex) {
  const step = tour.data && tour.data.steps[stepIndex];
  const t = step && step.try && step.try[tryIndex];
  if (!t || !TOUR_ACTIONS.includes(t.action)) return;
  if (TOUR_VIEWS.includes(t.action)) { await tourGoTo(stepIndex, tryIndex); return; }
  track(`tour-try-${step.id}-${tryIndex + 1}`);
  await tourRunAction(t);
}

/** Leave the tour and stay where it ended: everything on screen is the
 *  reader's to use. */
function tourEnd() {
  if (!state.tourDemo) return;
  tour.seq++;
  track("tour-end");
  tourSetDemo(null);
  pushUrlState({ major: true });
  tourAnnounce("The tour has ended. You can carry on from here.");
  document.getElementById("section-nav-trigger")?.focus({ preventScroll: true });
}

// ── The card and the ring ───────────────────────────────────────

function tourAnnounce(text) {
  const status = document.getElementById("tour-coach-status");
  if (status) status.textContent = text;
}

/** Bring the card up to date with state.tourDemo. */
async function tourSyncCoach() {
  const card = document.getElementById("tour-coach");
  const demo = state.tourDemo;
  const showing = Boolean(demo) && state.viewMode !== "tour";
  if (!card) return;
  if (!showing) { tourHideCoach(card); return; }
  const d = await loadTour();
  if (state.tourDemo !== demo || !d || !Array.isArray(d.steps)) return;
  // A step from an old link that the tour no longer has.
  if (!d.steps[demo.step]) { tourSetDemo(null); pushUrlState(); return; }
  const values = await tourLoadValues();
  if (state.tourDemo !== demo || state.viewMode === "tour") return;
  const stops = tourStops(d);
  const key = `${demo.step}:${demo.tryIndex}`;
  if (card.dataset.stop !== key) {
    const wasHidden = card.hidden;
    const had = card.contains(document.activeElement) ? document.activeElement : null;
    const keep = had && had.dataset ? had.dataset.tourCoach : null;
    card.innerHTML = tourCoachHtml(d, demo, values, stops);
    card.dataset.stop = key;
    tour.scrolledFor = "";
    tourBindCoach(card);
    tourApplyMinimised(card);
    // The progress bar and a comparison's bars: widths from the markup.
    for (const bar of card.querySelectorAll("[data-pct]")) bar.style.width = `${bar.dataset.pct}%`;
    card.hidden = false;
    const at = tourStopIndex(stops, demo);
    const title = card.querySelector(".tour-coach-title");
    tourAnnounce(`Stop ${at + 1} of ${stops.length}: ${title ? title.textContent : ""}`);
    // Keep the keyboard where the reader left it; on the tour's first card,
    // or when the button pressed has gone, start them on the title.
    const same = keep && card.querySelector(`[data-tour-coach="${keep}"]:not([disabled])`);
    if (same) same.focus({ preventScroll: true });
    else if (wasHidden || had) title?.focus({ preventScroll: true });
  }
  card.hidden = false;
  document.body.dataset.tourCoach = "";
  tourWatch(true);
  tourPlace();
}

function tourHideCoach(card) {
  card.hidden = true;
  card.dataset.stop = "";
  delete document.body.dataset.tourCoach;
  document.body.style.removeProperty?.("--tour-coach-h");
  const ring = document.getElementById("tour-ring");
  if (ring) ring.hidden = true;
  tourWatch(false);
}

function tourApplyMinimised(card) {
  const min = Boolean(tour.minimised);
  card.classList.toggle("tour-coach--min", min);
  const btn = card.querySelector('[data-tour-coach="min"]');
  if (btn) {
    btn.setAttribute("aria-expanded", String(!min));
    btn.setAttribute("aria-label", min ? "Show the tour's card" : "Hide the tour's card");
  }
}

function tourBindCoach(card) {
  if (card.dataset.bound) return;
  card.dataset.bound = "1";
  card.addEventListener("click", (e) => {
    const go = e.target.closest("[data-tour-try]");
    if (go) {
      const [s, t] = go.dataset.tourTry.split(":").map(Number);
      tourRun(s, t);
      return;
    }
    const nav = e.target.closest("[data-tour-coach]");
    if (!nav || nav.disabled) return;
    const what = nav.dataset.tourCoach;
    if (what === "next") tourMove(1);
    else if (what === "back") tourMove(-1);
    else if (what === "end") tourEnd();
    else if (what === "min") {
      tour.minimised = !tour.minimised;
      tourApplyMinimised(card);
      tourPlace();
    }
  });
  // Arrow keys move on while the keyboard is on the card's own buttons, and
  // Escape ends the tour. Nothing is taken from the rest of the page.
  card.addEventListener("keydown", (e) => {
    if (e.key === "Escape") { e.preventDefault(); tourEnd(); return; }
    if (e.altKey || e.ctrlKey || e.metaKey || e.target.closest("summary, a")) return;
    if (e.key === "ArrowRight") { e.preventDefault(); tourMove(1); }
    else if (e.key === "ArrowLeft") { e.preventDefault(); tourMove(-1); }
  });
  card.addEventListener("toggle", () => tourPlace(), true);
}

function tourTarget() {
  const demo = state.tourDemo;
  const step = demo && tour.data && tour.data.steps[demo.step];
  const t = step && demo.tryIndex != null ? step.try[demo.tryIndex] : null;
  const sel = t && TOUR_TARGETS[t.action];
  const el = sel ? document.querySelector(sel) : null;
  if (!el || el.hidden || !el.getClientRects().length) return null;
  return el;
}

/** The part of `el` the reader can actually see: inside the window, and
 *  inside the panel's scroller when it is in one. Null if none of it is. */
function tourVisibleRect(el) {
  const r = el.getBoundingClientRect();
  let top = r.top, left = r.left, right = r.right, bottom = r.bottom;
  const box = el.closest(".panel-tab-content");
  if (box) {
    const b = box.getBoundingClientRect();
    top = Math.max(top, b.top); bottom = Math.min(bottom, b.bottom);
    left = Math.max(left, b.left); right = Math.min(right, b.right);
  }
  top = Math.max(top, 0); left = Math.max(left, 0);
  bottom = Math.min(bottom, window.innerHeight); right = Math.min(right, window.innerWidth);
  if (bottom - top < 8 || right - left < 8) return null;
  return { top, left, right, bottom, width: right - left, height: bottom - top };
}

/** Put the card where it can be read beside what it is talking about, and
 *  the ring round that. On a phone the card sits under the header and the
 *  sheet is kept below it; on a wider screen it floats over the map. */
function tourPlace() {
  const card = document.getElementById("tour-coach");
  const ring = document.getElementById("tour-ring");
  if (!card || card.hidden) return;
  const header = document.querySelector(".site-header");
  const top0 = (header ? header.getBoundingClientRect().bottom : 0) + 8;
  const target = tourTarget();
  // Bring the target into the panel's view once per stop, as it arrives.
  const key = card.dataset.stop;
  // A stop's own subject (TOUR_FRONT) goes to the top of the panel, and again
  // if the view draws it afresh; anything else only when it is out of sight.
  const demo = state.tourDemo;
  const t = demo && demo.tryIndex != null && tour.data ? tour.data.steps[demo.step].try[demo.tryIndex] : null;
  const front = Boolean(t) && TOUR_FRONT.includes(t.action);
  if (target && front) {
    // Held at the top while the view settles: what is drawn above it late
    // (a summary, a preview) would push it down out of sight. Until the
    // reader touches the panel, or a few seconds pass.
    const box = target.closest(".panel-tab-content");
    if (box && Date.now() < tour.frontUntil) {
      const r = target.getBoundingClientRect(), b = box.getBoundingClientRect();
      const chart = target.querySelector("svg");
      const c = chart ? chart.getBoundingClientRect() : r;
      // Its top at the panel's top; or, where the panel cannot hold all of
      // it (a phone's sheet under the card), the chart itself in full, or
      // failing that the chart's top.
      let off = r.top - b.top - 8;
      if (c.bottom - r.top + 12 > b.height) off = c.height + 8 > b.height ? c.top - b.top - 4 : c.bottom - b.bottom + 4;
      if (Math.abs(off) > 4) box.scrollTop += off;
    }
  } else if (target && tour.scrolledFor !== key) {
    tour.scrolledFor = key;
    if (!tourVisibleRect(target) || target.getBoundingClientRect().top > window.innerHeight * 0.6) scrollPanelTo(target, 8);
  }
  const sheet = typeof isSheetLayout === "function" && isSheetLayout();
  card.dataset.place = sheet ? "dock" : "float";
  const cw = card.offsetWidth, ch = card.offsetHeight;
  if (sheet) {
    card.style.left = "";
    card.style.top = `${Math.round(top0)}px`;
    document.body.style.setProperty?.("--tour-coach-h", `${Math.round(ch + 16)}px`);
  } else {
    document.body.style.removeProperty?.("--tour-coach-h");
    // The part of the map the panel leaves free: [lo, hi]. The panel is on
    // the right on a wider screen; read where it is rather than assume.
    const w = window.innerWidth;
    const panel = document.getElementById("departure-panel");
    const p = panel && !panel.hidden ? panel.getBoundingClientRect() : null;
    const open = Boolean(p) && p.width > 40 && p.right > 0 && p.left < w;
    const onRight = open && p.left + p.width / 2 > w / 2;
    const lo = open && !onRight ? p.right : 0;
    const hi = onRight ? p.left : w;
    const r = target ? tourVisibleRect(target) : null;
    const inPanel = Boolean(r) && open && r.left >= p.left - 2 && r.right <= p.right + 2;
    // Beside the panel, or with no panel the top right, clear of the zoom.
    const besidePanel = open && !onRight ? lo + 16 : hi - cw - 16;
    let left, top;
    if (inPanel) {
      // In the panel: just beside it, level with what it points at.
      left = besidePanel;
      top = r.top + Math.min(r.height, 120) / 2 - ch / 2;
    } else if (r) {
      // On the map: on whichever side of it has room.
      left = r.right + 16 + cw <= hi - 16 ? r.right + 16 : r.left - cw - 16;
      top = r.top;
    } else {
      // Nothing to point at: the top of the map, beside the panel.
      left = besidePanel;
      top = top0 + 8;
    }
    // Keep to the free part of the map where it is wide enough, and always
    // on screen.
    left = Math.min(Math.max(left, lo + 16), hi - cw - 16);
    left = Math.min(Math.max(left, 16), w - cw - 16);
    top = Math.min(Math.max(top, top0), window.innerHeight - ch - 16);
    card.style.left = `${Math.round(left)}px`;
    card.style.top = `${Math.round(Math.max(top, top0))}px`;
  }
  if (!ring) return;
  const r = target ? tourVisibleRect(target) : null;
  if (!r) { ring.hidden = true; return; }
  // A little outside what it rings, but its 3px line never under the header
  // or past the window's edge, where it would be cut off.
  const pad = 4, line = 3;
  const box = {
    top: Math.max(r.top - pad, top0 - 8 + line), left: Math.max(r.left - pad, line),
    bottom: Math.min(r.bottom + pad, window.innerHeight - line), right: Math.min(r.right + pad, window.innerWidth - line),
  };
  const k = [box.top, box.left, box.right, box.bottom].map(Math.round).join(",");
  if (k !== tour.ringKey || ring.hidden) {
    tour.ringKey = k;
    ring.style.top = `${Math.round(box.top)}px`;
    ring.style.left = `${Math.round(box.left)}px`;
    ring.style.width = `${Math.round(box.right - box.left)}px`;
    ring.style.height = `${Math.round(box.bottom - box.top)}px`;
  }
  ring.hidden = false;
}

function tourPanelTouched(e) {
  if (e.target && e.target.closest && e.target.closest("#departure-panel")) tour.frontUntil = 0;
}

function tourQueuePlace() {
  if (tour.frame) return;
  tour.frame = requestAnimationFrame(() => { tour.frame = 0; tourPlace(); });
}

/** Keep the card and ring with what they point at while the page moves:
 *  scrolling, resizing, the sheet being dragged, a result arriving late. */
function tourWatch(on) {
  if (on === tour.watching) return;
  tour.watching = on;
  const opts = { capture: true, passive: true };
  if (on) {
    window.addEventListener("scroll", tourQueuePlace, opts);
    window.addEventListener("resize", tourQueuePlace);
    // A reader who reaches into the panel has it from then on.
    for (const type of ["wheel", "touchstart", "pointerdown"]) window.addEventListener(type, tourPanelTouched, opts);
    // Results, boards and the sheet change size without telling anyone.
    tour.timer = setInterval(tourQueuePlace, 400);
  } else {
    window.removeEventListener("scroll", tourQueuePlace, opts);
    window.removeEventListener("resize", tourQueuePlace);
    for (const type of ["wheel", "touchstart", "pointerdown"]) window.removeEventListener(type, tourPanelTouched, opts);
    clearInterval(tour.timer);
    tour.timer = 0;
  }
}

// ── The tour's own page ─────────────────────────────────────────

async function tourCopyLink(host) {
  const status = host.querySelector(".tour-copy-status");
  const url = `${location.origin}${location.pathname}#view=tour`;
  try {
    await navigator.clipboard.writeText(url);
    if (status) status.textContent = "Link copied.";
  } catch {
    if (status) status.textContent = url;
  }
}

/** Hide a mode whose file is not published yet (the PDF is made last), so
 *  the opening never offers a link that leads nowhere. */
function tourHideMissing(host) {
  for (const a of host.querySelectorAll("a[data-tour-mode]")) {
    fetch(a.getAttribute("href"), { method: "HEAD", cache: "no-store" })
      .then(res => { if (!res.ok) a.hidden = true; })
      .catch(() => { a.hidden = true; });
  }
}

function tourBind(host) {
  if (tour.bound) return;
  tour.bound = true;
  host.addEventListener("click", (e) => {
    const mode = e.target.closest("[data-tour-mode]");
    if (mode) track(`tour-mode-${mode.dataset.tourMode}`);
    const start = e.target.closest("[data-tour-start]");
    if (start) {
      const stops = tourStops(tour.data);
      const first = stops[tourStopIndex(stops, { step: Number(start.dataset.tourStart), tryIndex: null })];
      if (first) tourGoTo(first.step, first.tryIndex);
      return;
    }
    if (e.target.closest("[data-tour-copy]")) tourCopyLink(host);
  });
}

/** Draw the tour's page into its panel. True once it is there. */
async function tourRender(mine = () => true) {
  const host = document.getElementById("tab-content-tour");
  const d = await loadTour();
  if (!mine() || !host) return false;
  if (!d || !Array.isArray(d.steps)) {
    host.innerHTML = `<p class="proposals-empty">The tour could not be loaded. Check your connection and try again.</p>`;
    return false;
  }
  const values = await tourLoadValues();
  if (!mine()) return false;
  host.innerHTML = tourPanelHtml(d, values);
  tour.rendered = true;
  tourHideMissing(host);
  tourBind(host);
  return true;
}

/** Show the tour's page: called by applyViewMode with its ownership check. */
async function tourShow(mine = () => true) {
  tourSyncCoach();
  await tourRender(mine);
}

/** Called by applyViewMode on every other view: one opened inside the tour
 *  keeps its card. */
function tourHide() {
  tourSyncCoach();
}

tourInitMenu();
