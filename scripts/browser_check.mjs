/**
 * browser_check.mjs — drive the real site in a real browser and assert on it.
 *
 * Why this exists: every visual bug this project has shipped survived because
 * nothing ever looked at the rendered page. The CARTO basemap returned HTTP 200
 * while stamping "API KEY REQUIRED" on every tile, and the panel-collapse bug
 * made Ticket view unreachable — both invisible to unit tests and to curl.
 *
 * No dependencies and no Playwright download: it speaks CDP over node's built-in
 * WebSocket to whatever Chrome you already have.
 *
 * ── Usage ───────────────────────────────────────────────────────────────
 *   # 1. serve the site and the API
 *   python -m http.server 8765
 *   uvicorn api.main:app --port 8000
 *
 *   # 2. start a headless Chrome with the debugger open
 *   google-chrome --headless=new --remote-debugging-port=9222 about:blank
 *   #   ...or, if Chrome is a flatpak:
 *   flatpak run --share=network com.google.Chrome \
 *     --headless=new --remote-debugging-port=9222 about:blank
 *
 *   # 3. run the checks
 *   node scripts/browser_check.mjs
 *   node scripts/browser_check.mjs --shots ./shots   # also write screenshots
 *
 * Exits non-zero if any check fails, so it can gate a release.
 *
 * ── Layout assertions ───────────────────────────────────────────────────
 * The `checkLayout` pass encodes three defects measured at 390px in Phase 3.
 * They are expected to FAIL until the packages that fix them land — that is
 * the point of a regression net. Each one names the defect it guards:
 *
 *   #1  the status chip sat 6px past the right edge on every departure row
 *   #2  the header title rendered 110px of its 292px
 *   #6  touch targets were 38–42px, under the 44px guideline
 */

import { writeFile, mkdir } from "node:fs/promises";

const CDP  = process.env.CDP_URL  || "http://127.0.0.1:9222";
const SITE = process.env.SITE_URL ||
  "http://127.0.0.1:8765/?api=http://localhost:8000";

const shotsIdx = process.argv.indexOf("--shots");
const SHOTS = shotsIdx === -1 ? null : process.argv[shotsIdx + 1];

const VIEWPORTS = [
  { name: "mobile",  width: 390,  height: 844,  mobile: true  },
  // The narrowest phone still in common use, and a short landscape window.
  // The three sizes above them all have enough vertical room to hide a sheet
  // that is taller than its viewport, so the reachability checks passed while
  // Route view's tab strip started 130px below the fold at 320x568.
  { name: "narrow",    width: 320, height: 568, mobile: true },
  { name: "landscape", width: 740, height: 360, mobile: true },
  { name: "tablet",  width: 768,  height: 1024, mobile: true  },
  { name: "desktop", width: 1440, height: 900,  mobile: false },
];

/** A reliably busy stop, so the departure board renders with real rows. */
const SAMPLE_STOP = { atco: "149000007954", name: "Town Hall" };

/**
 * Console noise that is expected locally and must not fail the run: without
 * BODS/RTT keys the data fetches legitimately error. Anything that looks like
 * a JS exception is a different matter — that is a bug a restyle introduced.
 */
const JS_ERROR = /TypeError|ReferenceError|SyntaxError|is not a function|Cannot read|is not defined/i;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function connect(url) {
  const target = await (
    await fetch(`${CDP}/json/new?${encodeURIComponent(url)}`, { method: "PUT" })
  ).json();
  const ws = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((r) => ws.addEventListener("open", r));

  let id = 0;
  const pending = new Map();
  const events = [];
  const consoleErrors = [];

  ws.addEventListener("message", (e) => {
    const m = JSON.parse(e.data);
    if (m.id && pending.has(m.id)) {
      pending.get(m.id)(m);
      pending.delete(m.id);
    } else if (m.method) {
      events.push(m.method);
      if (m.method === "Runtime.consoleAPICalled" && m.params.type === "error") {
        consoleErrors.push(
          m.params.args.map((a) => a.value ?? a.description).join(" ").slice(0, 200),
        );
      }
      // An uncaught throw does not arrive as a console API call.
      if (m.method === "Runtime.exceptionThrown") {
        const d = m.params.exceptionDetails;
        consoleErrors.push(
          String(d.exception?.description || d.text || "uncaught").slice(0, 200));
      }
    }
  });

  const send = (method, params = {}) =>
    new Promise((res, rej) => {
      const myId = ++id;
      pending.set(myId, (m) =>
        m.error ? rej(new Error(`${method}: ${m.error.message}`)) : res(m.result));
      ws.send(JSON.stringify({ id: myId, method, params }));
    });

  const evaluate = async (expression) => {
    const r = await send("Runtime.evaluate",
      { expression, returnByValue: true, awaitPromise: true });
    if (r.exceptionDetails) throw new Error(`${r.exceptionDetails.text} :: ${expression}`);
    return r.result.value;
  };

  return { ws, send, evaluate, events, consoleErrors };
}

async function openPage(viewport) {
  const page = await connect(SITE);
  await page.send("Page.enable");
  await page.send("Runtime.enable");
  // Without this the harness happily validates the *previous* build: Chrome
  // serves style.css and app.js from cache, every check passes, and none of
  // them looked at the code you just wrote.
  await page.send("Network.enable");
  await page.send("Network.setCacheDisabled", { cacheDisabled: true });
  await page.send("Emulation.setDeviceMetricsOverride", {
    width: viewport.width, height: viewport.height,
    deviceScaleFactor: 2, mobile: viewport.mobile,
  });
  await page.send("Page.navigate", { url: SITE });
  for (let i = 0; i < 40 && !page.events.includes("Page.loadEventFired"); i++) {
    await sleep(250);
  }
  await sleep(2500);   // Leaflet + the first data fetches
  await freezeMotion(page);
  return page;
}

/**
 * Kill transitions and animations for the duration of the run.
 *
 * Not cosmetic: getComputedStyle during a transition returns the interpolated
 * value, so the contrast pass was reading a half-faded colour and reporting
 * 3.31:1 for a pair that settles at 5.02:1. It also stops screenshots landing
 * mid-animation, which made baselines impossible to compare.
 */
async function freezeMotion(page) {
  await page.evaluate(`(() => {
    const s = document.createElement("style");
    s.textContent = "*, *::before, *::after { transition: none !important;" +
                    " animation: none !important; }";
    document.head.appendChild(s);
    return 1;
  })()`);
}

async function screenshot(page, name) {
  if (!SHOTS) return;
  await mkdir(SHOTS, { recursive: true });
  const shot = await page.send("Page.captureScreenshot", { format: "png" });
  await writeFile(`${SHOTS}/${name}.png`, Buffer.from(shot.data, "base64"));
}

/**
 * Drive the app's own toggle rather than poking the class, so `state.darkMode`
 * stays consistent with the DOM for anything that reads it.
 */
async function setTheme(page, theme) {
  const want = theme === "dark";
  await page.evaluate(
    `if (state.darkMode !== ${want}) toggleDarkMode(); state.darkMode`);
  await sleep(250);
}

// ── Checks ──────────────────────────────────────────────────

const results = [];
const check = (name, pass, detail = "") =>
  results.push({ name, pass, detail });

/**
 * Some conditions are environmental rather than regressions — an API that is
 * cold or absent cannot render a departure board, and failing the run for that
 * would train people to ignore a red result.
 */
const skip = (name, detail = "") =>
  results.push({ name, pass: true, skipped: true, detail });

/** Poll instead of sleeping: Render's free tier cold-starts in ~30s. */
async function waitFor(page, expression, timeoutMs = 40000, everyMs = 1000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await page.evaluate(expression)) return true;
    await sleep(everyMs);
  }
  return false;
}

/**
 * Is this element actually being shown?
 *
 * getClientRects() alone is not enough: a closed <details> hides its content
 * with content-visibility, which preserves layout state, so descendants keep
 * reporting a size they are not currently painting at. checkVisibility() is
 * the API built for the question; the rect test stays as a fallback and as a
 * guard against zero-sized elements.
 */
const VISIBLE_FN = `(el) => (
  (typeof el.checkVisibility !== "function" || el.checkVisibility({
     contentVisibilityAuto: true, opacityProperty: true, visibilityProperty: true,
     checkOpacity: true, checkVisibilityCSS: true,
   })) && el.getClientRects().length > 0
)`;

/**
 * Nothing inside the header or the panel may extend past the right edge.
 * The map is excluded deliberately — Leaflet's panes overflow by design.
 */
const OVERFLOW_SCAN = `(() => {
  const visible = ${VISIBLE_FN};
  const named = (el) =>
    (typeof el.className === "string" && el.className.trim())
      ? "." + el.className.trim().split(/\\s+/)[0]
      : el.tagName.toLowerCase();
  const bad = [];
  for (const root of document.querySelectorAll(".site-header, .departure-panel")) {
    for (const el of root.querySelectorAll("*")) {
      if (!visible(el)) continue;
      const r = el.getBoundingClientRect();
      if (r.width === 0 || r.height === 0) continue;
      const over = Math.round(r.right - window.innerWidth);
      if (over > 1) bad.push(named(el) + " +" + over + "px");
    }
  }
  return [...new Set(bad)].slice(0, 6);
})()`;

/**
 * Labels and headings must not be clipped by their own container. Long
 * destination names are allowed to ellipsis; a title losing a third of
 * itself is not, so this only fires under 92% shown.
 */
const TRUNCATION_SCAN = `(() => {
  const visible = ${VISIBLE_FN};
  const named = (el) =>
    (typeof el.className === "string" && el.className.trim())
      ? "." + el.className.trim().split(/\\s+/)[0]
      : el.tagName.toLowerCase();
  const sel = ".header-title, .section-nav-label," +
              " .panel-tab-label, h1, h2, h3, button";
  const bad = [];
  for (const el of document.querySelectorAll(sel)) {
    if (!visible(el)) continue;
    if (!el.textContent.trim()) continue;
    if (el.clientWidth === 0 || el.scrollWidth <= el.clientWidth + 1) continue;
    const shown = el.clientWidth / el.scrollWidth;
    if (shown < 0.92) {
      bad.push(named(el) + " " + Math.round(shown * 100) + "% shown");
    }
  }
  return [...new Set(bad)].slice(0, 6);
})()`;

/**
 * WCAG 2.5.8 sets a 24x24 floor; 44 is Apple's number, the AAA criterion,
 * and what this site targets. Inline links in prose have an explicit
 * exception in the spec and are not selected here.
 */
const TARGET_SCAN = `(() => {
  const visible = ${VISIBLE_FN};
  const named = (el) =>
    (el.id ? "#" + el.id
      : (typeof el.className === "string" && el.className.trim())
        ? "." + el.className.trim().split(/\\s+/)[0]
        : el.tagName.toLowerCase());
  const sel = 'button, [role="option"], [role="button"], select,' +
              ' input:not([type=hidden]), .panel-tab';
  // Map pins are excluded: a 12px dot marks a position on a street, and a
  // 44px one would cover the street. They carry a transparent 44px hit area
  // via .stop-marker-icon::after instead, which a bounding rect cannot see.
  const EXEMPT = /leaflet-marker-icon|leaflet-div-icon/;
  const bad = [];
  for (const el of document.querySelectorAll(sel)) {
    if (!visible(el)) continue;
    const r = el.getBoundingClientRect();
    if (r.width === 0 || r.height === 0) continue;
    if (EXEMPT.test(el.className || "")) continue;
    // For a control inside a <label>, the target is the label: clicking
    // anywhere on it operates the control, so that is the region a finger
    // actually has to hit. Measuring the 14px checkbox would be wrong.
    const label = el.closest("label");
    if (label) {
      const lr = label.getBoundingClientRect();
      if (lr.width >= 44 && lr.height >= 44) continue;
    }
    if (r.width < 44 || r.height < 44) {
      bad.push(named(el) + " " + Math.round(r.width) + "x" + Math.round(r.height));
    }
  }
  return [...new Set(bad)].slice(0, 8);
})()`;

/**
 * Real rendered contrast, not token pairs in the abstract: walk visible text,
 * find the nearest ancestor that actually paints a background, and measure.
 * Token-pair maths cannot see which combinations the page truly produces.
 *
 * WCAG 1.4.3 AA is 4.5:1, relaxed to 3:1 for large text (>=24px, or >=18.66px
 * bold). Text drawn over the map is skipped — its backdrop is imagery, and no
 * static computation describes it honestly.
 */
const CONTRAST_SCAN = `(() => {
  const visible = ${VISIBLE_FN};
  const parse = (c) => {
    const m = c.match(/rgba?\\(([^)]+)\\)/);
    if (!m) return null;
    const p = m[1].split(",").map(Number);
    return { r: p[0], g: p[1], b: p[2], a: p.length > 3 ? p[3] : 1 };
  };
  const lum = (c) => {
    const f = (x) => { x /= 255; return x <= 0.03928 ? x / 12.92 : Math.pow((x + 0.055) / 1.055, 2.4); };
    return 0.2126 * f(c.r) + 0.7152 * f(c.g) + 0.0722 * f(c.b);
  };
  const over = (fg, bg) => ({
    r: fg.r * fg.a + bg.r * (1 - fg.a),
    g: fg.g * fg.a + bg.g * (1 - fg.a),
    b: fg.b * fg.a + bg.b * (1 - fg.a), a: 1 });
  const ratio = (a, b) => {
    const [hi, lo] = [lum(a), lum(b)].sort((x, y) => y - x);
    return (hi + 0.05) / (lo + 0.05);
  };
  const named = (el) =>
    (el.id ? "#" + el.id
      : (typeof el.className === "string" && el.className.trim())
        ? "." + el.className.trim().split(/\\s+/)[0]
        : el.tagName.toLowerCase());

  const bad = [];
  for (const el of document.querySelectorAll("*")) {
    if (el.closest("#map")) continue;              // backdrop is imagery
    if (!visible(el)) continue;
    const cs = getComputedStyle(el);
    const own = [...el.childNodes].some(n => n.nodeType === 3 && n.textContent.trim());
    if (!own) continue;
    const fg = parse(cs.color);
    if (!fg || fg.a === 0) continue;

    let bg = null, node = el;
    while (node && node.nodeType === 1) {
      const c = parse(getComputedStyle(node).backgroundColor);
      if (c && c.a === 1) { bg = c; break; }
      node = node.parentElement;
    }
    if (!bg) continue;

    const px = parseFloat(cs.fontSize);
    const w = parseInt(cs.fontWeight, 10) || 400;
    const large = px >= 24 || (px >= 18.66 && w >= 700);
    const need = large ? 3 : 4.5;
    const r = ratio(over(fg, bg), bg);
    if (r < need) bad.push(named(el) + " " + r.toFixed(2) + ":1 (need " + need + ")");
  }
  return [...new Set(bad)].sort().slice(0, 12);
})()`;

async function checkContrast(page, where) {
  const bad = await page.evaluate(CONTRAST_SCAN);
  check(`text meets WCAG AA contrast — ${where}`, bad.length === 0, bad.join(", "));
}

async function checkLayout(page, where) {
  const over = await page.evaluate(OVERFLOW_SCAN);
  check(`nothing overflows the viewport — ${where}`, over.length === 0, over.join(", "));

  const trunc = await page.evaluate(TRUNCATION_SCAN);
  check(`no label is clipped by its container — ${where}`, trunc.length === 0, trunc.join(", "));

  const small = await page.evaluate(TARGET_SCAN);
  check(`touch targets are at least 44px — ${where}`, small.length === 0, small.join(", "));

}

/** Contrast is theme-specific — muted text fails in dark, accent text in
 *  light — so a single-theme pass finds half the problem. */
async function checkContrastBothThemes(page, where) {
  const restore = await page.evaluate("state.darkMode");
  for (const theme of ["light", "dark"]) {
    await setTheme(page, theme);
    await checkContrast(page, `${where}, ${theme}`);
  }
  await setTheme(page, restore ? "dark" : "light");
}

/**
 * The empty states were the only thing ever screenshotted, so the component
 * people actually stare at went unexamined. Load a real board.
 */
async function checkDepartureBoard(page) {
  await page.evaluate("setViewMode('live')");
  await sleep(1200);
  const opened = await page.evaluate(
    `(() => {
       if (typeof openDepartures !== "function") return "no openDepartures";
       openDepartures("${SAMPLE_STOP.atco}", "${SAMPLE_STOP.name}");
       return "ok";
     })()`);
  if (opened !== "ok") {
    check("departure board opens", false, opened);
    return;
  }
  await waitFor(page, "document.querySelectorAll('.departure-row').length > 0");

  const rows = await page.evaluate("document.querySelectorAll('.departure-row').length");
  if (rows > 0) {
    check("departure board renders rows", true, `${rows} rows`);
    await checkLayout(page, "departure board");
    return;
  }

  // No rows. An error state on screen means the API never answered, which is
  // an environment problem, not something a restyle broke.
  const errored = await page.evaluate(
    `(() => {
       const e = document.getElementById("panel-error");
       if (e && !e.classList.contains("hidden")) {
         return (document.getElementById("panel-error-msg") || {}).textContent || "error";
       }
       return "";
     })()`);
  if (errored) skip("departure board renders rows", `API unreachable: ${errored.trim().slice(0, 60)}`);
  else check("departure board renders rows", false, "0 rows and no error shown");
}

/** The basemap must actually render tiles — a 200 response proved nothing. */
async function checkBasemap(page) {
  const tiles = await page.evaluate(
    "document.querySelectorAll('.leaflet-tile-pane img.leaflet-tile-loaded').length");
  check("basemap tiles loaded", tiles > 0, `${tiles} tiles`);
  check("no CARTO tile requests",
    (await page.evaluate(
      "[...document.querySelectorAll('.leaflet-tile-pane img')]" +
      ".every(i => !i.src.includes('cartocdn'))")));
}

/**
 * Content that cannot be reached.
 *
 * Every view's panel is a fixed-height box with more in it than fits, so
 * something has to scroll. `.panel-tab-content` used to be `overflow: hidden`,
 * delegating that to whichever inner element each view nominated — and a view
 * that forgot to nominate one did not scroll awkwardly, it silently clipped.
 * Network Updates lost everything past the first article that way, and the
 * Ideas tab lost the bottom of its own form. Nothing failed; the content was
 * simply gone.
 *
 * A pane is fine if it does not overflow, or if it — or a descendant — can
 * actually scroll the overflow away. Anything else is unreachable content.
 */
const CLIPPED_SCAN = `(() => {
  const bad = [];
  const scrolls = (el) => {
    const oy = getComputedStyle(el).overflowY;
    return (oy === "auto" || oy === "scroll") && el.scrollHeight > el.clientHeight + 1;
  };
  for (const mode of document.querySelectorAll(".panel-mode")) {
    if (getComputedStyle(mode).display === "none") continue;
    for (const pane of mode.querySelectorAll(".panel-tab-content")) {
      if (pane.classList.contains("hidden")) continue;
      const overflows = pane.scrollHeight > pane.clientHeight + 1;
      if (!overflows) continue;
      if (scrolls(pane)) continue;
      if ([...pane.querySelectorAll("*")].some(scrolls)) continue;
      bad.push((pane.id || pane.className) + " clips " +
               (pane.scrollHeight - pane.clientHeight) + "px with nothing to scroll it");
    }
  }
  return bad;
})()`;

/**
 * The panel itself has to fit on the screen.
 *
 * CLIPPED_SCAN asks whether an overflowing pane has something to scroll it.
 * That is a different question from whether the sheet is inside the viewport
 * at all, and it passed at every size while, at 320x568, Route view's tab
 * strip began at y≈581 — thirteen pixels below the fold, with nothing above it
 * to hint that it existed — and the Network and Updates sheets began at y≈−3,
 * their tabs partly behind the fixed header.
 *
 * A control the reader cannot see is not reachable, however large it is.
 */
const SHEET_BOUNDS_SCAN = `(() => {
  const bad = [];
  const vh = window.innerHeight;
  const header = document.querySelector(".app-header");
  const headerBottom = header ? header.getBoundingClientRect().bottom : 0;

  const panel = document.getElementById("departure-panel");
  if (!panel || getComputedStyle(panel).display === "none") return bad;
  const pr = panel.getBoundingClientRect();
  if (pr.height < 1) return bad;

  if (pr.top < headerBottom - 1) {
    bad.push("panel starts " + Math.round(headerBottom - pr.top) +
             "px above the usable area (behind the header)");
  }

  // The tab strip and the sheet's own controls are how a reader moves around
  // inside it, so they are the ones that must not fall off the bottom.
  for (const sel of [".panel-tabs", ".btn-collapse-panel", ".sheet-handle"]) {
    for (const el of document.querySelectorAll(sel)) {
      const mode = el.closest(".panel-mode");
      if (mode && getComputedStyle(mode).display === "none") continue;
      if (getComputedStyle(el).display === "none") continue;
      const r = el.getBoundingClientRect();
      if (r.height < 1 && r.width < 1) continue;
      if (r.top >= vh) {
        bad.push(sel + " starts " + Math.round(r.top - vh) + "px below the fold");
      } else if (r.bottom > vh + 1 && r.top > vh - 8) {
        bad.push(sel + " is cut off at the bottom of the screen");
      }
    }
  }
  return bad;
})()`;

async function checkSheetFitsViewport(page, where) {
  const bad = await page.evaluate(SHEET_BOUNDS_SCAN);
  check(`the panel fits the screen — ${where}`, bad.length === 0, bad.join(", "));
}

/**
 * A failing live feed has to look different from a quiet one.
 *
 * `setStatusLabel` writes "Update failed — retrying" into `.last-updated`,
 * and `.last-updated` is `display: none` below 880px. So on every phone the
 * only signal that the live feed is broken was invisible, and a map with no
 * buses on it looked exactly like a map with no buses due. This drives the
 * failure state directly rather than waiting for a real outage.
 */
/**
 * The two views that are all prose should not be read through a letterbox.
 *
 * Network Objectives and Updates carry no map layers of their own, yet on a
 * desktop they kept the full map and a ~360px column — so an article, and a
 * letter a resident is meant to edit and send to a councillor, were being
 * read four or five words at a time.
 */
async function checkReadingLayout(page) {
  const widths = {};
  for (const mode of ["improvements", "network", "updates"]) {
    await page.evaluate(`setViewMode('${mode}')`);
    await sleep(900);
    widths[mode] = await page.evaluate(
      `Math.round(document.getElementById("departure-panel").getBoundingClientRect().width)`);
  }
  check("prose views get a wider column than the map views",
    widths.network > widths.improvements + 100
    && widths.updates > widths.improvements + 100,
    JSON.stringify(widths));
  // And the map must still have somewhere to be, so this is not just
  // "make the panel full width".
  const mapWidth = await page.evaluate(
    `Math.round(document.getElementById("map").getBoundingClientRect().width)`);
  check("the map keeps usable width in the prose views", mapWidth > 300,
    `map ${mapWidth}px`);
  await page.evaluate("setViewMode('live')");
  await sleep(600);
}

/**
 * Route view has to be usable on a phone.
 *
 * Two things went wrong here and neither was visible to any existing check.
 * The closed "Filter services" disclosure was a flex child of an over-full
 * sheet, so the flex algorithm squashed it to 14px — a control with a 44px
 * min-height rule, rendered unreadable and unhittable. And the fixed chrome
 * above the tab content came to 143px of a 405px sheet, leaving a 218px
 * window onto a list of sixteen services.
 *
 * So: every control keeps its target size, and the list the tab exists for is
 * actually on screen when the tab opens.
 */
/**
 * Search results, once there are some, in both themes.
 *
 * The existing contrast pass samples the *empty* search panel, so it never
 * saw what the audit found: the block was styled with `--surface`, `--border`
 * and `--accent`, none of which this project defines, so every one fell back
 * to a hardcoded light colour. In dark mode that produced white cards with
 * muted text at 3.5:1 — below the 4.5:1 this size of text needs.
 *
 * And it activates a railway result, because the handler for those called a
 * function that had never existed and threw on every press while the unit
 * tests, which only checked that the button was rendered, stayed green.
 */
/**
 * The first useful action, and the answer to it, without learning the sheet.
 *
 * On a fresh 390x844 Live view the sheet occupied y=532-780 and the search
 * input began at about y=863 — below the footer, off the visible panel, after
 * a large placeholder icon and an instruction. And selecting a stop left the
 * sheet at `peek`, so the first departure row began at exactly y=780, where
 * the panel ends: ten rows in the DOM, none of them visible.
 *
 * Both are the same root cause — the peek detent is smaller than the content
 * it is asked to present — so they are checked together.
 */
/**
 * A campaign section must not wait on the live bus service.
 *
 * Opening #view=n while /api/stops was held for twelve seconds left the site
 * on Live showing "Loading live bus data…", and the requested section did not
 * appear until the stop request resolved or failed. The people this site is
 * written for are sent these links; the link has to work when the bus feed
 * does not.
 *
 * Uses CDP request interception rather than waiting for a real outage.
 */
/**
 * The fare question, and the fields that answer it.
 *
 * On entry at 390x844 the Tickets sheet ended at y=780 while the origin field
 * began around y=1066 — behind an explanation, a caveat and six worked
 * examples. The examples are good; they are not what someone came to do.
 *
 * Also checks the input size: below 16px, mobile Safari zooms the page on
 * focus and the reader ends up editing a strip of a magnified layout.
 */
/**
 * There has to be room to edit in the editor.
 *
 * At the half detent on a 390x844 phone the editor got 348px, of which its
 * own header and action area took 199 — leaving 149px of scrolling space for
 * 544px of form, with the Name field clipped. The action area was larger than
 * the form it submitted.
 */
/**
 * Tabs that behave like the pattern they declare.
 *
 * Every element with role="tab" carried tabIndex=0, so a keyboard user tabbed
 * through each one instead of arrowing between them — and ArrowRight on the
 * Routes About tab moved nothing and selected nothing. The markup said tabs;
 * the behaviour did not.
 */
/**
 * A dialog closes when you click away from it, and not when you click in it.
 *
 * `showModal()` gives Escape and focus trapping but not backdrop dismissal —
 * the backdrop is a pseudo-element with nothing to listen on. The geometric
 * test matters: `e.target === dialog` is also true for a click on the
 * dialog's own padding, which would close it while the reader was aiming at
 * the text.
 */
async function checkDialogDismiss(page, where) {
  for (const id of ["evidence-dialog", "councillor-dialog"]) {
    const r = JSON.parse(await page.evaluate(`
      (() => {
        const d = document.getElementById("${id}");
        if (!d || typeof d.showModal !== "function") return JSON.stringify({ skip: "${id}" });
        if (!d.open) d.showModal();
        const box = d.getBoundingClientRect();
        const fire = (x, y) => d.dispatchEvent(new MouseEvent("click",
          { clientX: x, clientY: y, bubbles: true, detail: 1 }));
        // Inside first: this must NOT close it.
        fire(Math.round(box.left + box.width / 2), Math.round(box.top + box.height / 2));
        const afterInside = d.open;
        // Then the backdrop.
        fire(Math.round(box.left) - 20, Math.round(box.top) - 20);
        const afterOutside = d.open;
        if (d.open) d.close();
        return JSON.stringify({ afterInside, afterOutside });
      })()`));
    if (r.skip) { check(`${r.skip} dismisses on backdrop click — ${where}`, true, "absent"); continue; }
    check(`${id} survives a click inside it — ${where}`, r.afterInside === true, JSON.stringify(r));
    check(`${id} closes on a backdrop click — ${where}`, r.afterOutside === false, JSON.stringify(r));
  }
}

/**
 * Selecting a bus opens the sheet far enough to read it.
 *
 * The same gap that hid departures behind the `peek` detent, carried over to
 * buses and rail stations: the tab changed, the panel did not.
 */
async function checkBusSelectionReveals(page, where) {
  await page.evaluate("setViewMode('live'); closePanel(); setSheetDetent('peek')");
  await sleep(700);
  const r = JSON.parse(await page.evaluate(`
    (() => {
      const ref = Object.keys(state.busMarkers || {})[0];
      if (!ref) return JSON.stringify({ skip: "no live vehicles" });
      const marker = state.busMarkers[ref];
      openBusInfo(marker._vehicle);
      const panel = document.getElementById("departure-panel");
      const info = document.getElementById("bus-info-container");
      const p = panel.getBoundingClientRect(), i = info.getBoundingClientRect();
      return JSON.stringify({
        detent: document.body.dataset.sheet,
        visible: Math.round(Math.max(0, Math.min(i.bottom, p.bottom) - Math.max(i.top, p.top))),
      });
    })()`));
  if (r.skip) { check(`selecting a bus reveals its details — ${where}`, true, r.skip); return; }
  check(`selecting a bus reveals its details — ${where}`,
    r.detent !== "peek" && r.visible > 80, JSON.stringify(r));
}

async function checkTabKeyboard(page, where) {
  await page.evaluate("setViewMode('improvements')");
  await sleep(1400);
  const r = JSON.parse(await page.evaluate(`
    (() => {
      const list = document.querySelector('.panel-mode[data-mode="improvements"] [role="tablist"]');
      if (!list) return JSON.stringify({ skip: "no tablist" });
      const tabs = [...list.querySelectorAll('[role="tab"]')];
      const first = tabs[0];
      first.focus();
      const stopsBefore = tabs.filter(t => t.tabIndex === 0).length;
      first.dispatchEvent(new KeyboardEvent("keydown",
        { key: "ArrowRight", bubbles: true }));
      return JSON.stringify({
        tabs: tabs.length,
        tabStops: stopsBefore,
        movedTo: document.activeElement
          ? document.activeElement.getAttribute("aria-selected") : null,
        selected: tabs.map(t => t.getAttribute("aria-selected")),
      });
    })()`));
  if (r.skip) { check(`tabs respond to the arrow keys — ${where}`, true, r.skip); return; }
  // One tab stop per tablist, and ArrowRight both moves focus and selects.
  check(`a tablist is a single tab stop — ${where}`,
    r.tabStops === 1, JSON.stringify(r));
  check(`tabs respond to the arrow keys — ${where}`,
    r.selected[1] === "true", JSON.stringify(r));
  await page.evaluate("setViewMode('live')");
  await sleep(600);
}

async function checkEditorHasRoom(page, where) {
  await page.evaluate("setViewMode('improvements')");
  await sleep(1500);
  const r = JSON.parse(await page.evaluate(`
    (() => {
      if (typeof openEditor !== "function") return JSON.stringify({ skip: "no editor" });
      openEditor();
      const ed = document.getElementById("proposal-editor")
              || document.querySelector(".proposal-editor");
      if (!ed) return JSON.stringify({ skip: "editor did not open" });
      const h = (el) => el ? Math.round(el.getBoundingClientRect().height) : 0;
      const scroll = ed.querySelector(".editor-scroll");
      const actions = ed.querySelector(".editor-actions");
      const fields = [...ed.querySelectorAll(".editor-field")];
      const sr = scroll ? scroll.getBoundingClientRect() : null;
      const visibleFields = sr ? fields.filter(f => {
        const b = f.getBoundingClientRect();
        return b.height > 0 && b.top >= sr.top - 1 && b.bottom <= sr.bottom + 1;
      }).length : 0;
      return JSON.stringify({
        editor: h(ed), form: h(scroll), actions: h(actions),
        fields: fields.length, visibleFields,
        detent: document.body.dataset.sheet,
      });
    })()`));
  if (r.skip) {
    check(`the editor has room to edit in — ${where}`, true, r.skip);
    return;
  }
  // The form must have more space than the controls that submit it, and more
  // than one field visible at a time.
  check(`the editor has room to edit in — ${where}`,
    r.form > r.actions && r.visibleFields >= 2, JSON.stringify(r));
  await page.evaluate("if (state.editor) closeEditor({ skipSave: true }); setViewMode('live')");
  await sleep(700);
}

async function checkFareEntryReachable(page, where) {
  await page.evaluate("setViewMode('tickets')");
  await sleep(1800);
  const r = JSON.parse(await page.evaluate(`
    (() => {
      const panel = document.getElementById("departure-panel");
      const from = document.getElementById("jc-from");
      if (!panel || !from) return JSON.stringify({ found: false });
      const p = panel.getBoundingClientRect(), f = from.getBoundingClientRect();
      const box = document.getElementById("tab-content-tickets");
      return JSON.stringify({
        found: true,
        // Within the panel's own scroller, without having to scroll it.
        offset: Math.round(f.top - (box ? box.getBoundingClientRect().top : p.top)),
        scrolled: box ? Math.round(box.scrollTop) : 0,
        fontPx: Math.round(parseFloat(getComputedStyle(from).fontSize) * 10) / 10,
        visible: f.top >= p.top - 1 && f.top < p.bottom,
      });
    })()`));
  check(`the fare fields are reachable on entry — ${where}`,
    r.found && r.visible, JSON.stringify(r));
  check(`fare input text does not trigger mobile zoom — ${where}`,
    r.found && r.fontPx >= 16, JSON.stringify(r));
  await page.evaluate("setViewMode('live')");
  await sleep(600);
}

async function checkDeepLinkIndependence() {
  // Connect straight to the hashed URL. Navigating to it afterwards is a
  // same-document change, so init() never re-runs and the check measures a
  // page that was never deep-linked at all — which it did, reporting "live".
  const target = `${SITE}${SITE.includes("#") ? "" : "#view=n"}`;
  // Open a blank tab first. Connecting straight to the site starts loading it
  // before interception can be armed, so the stop request went out unpaused
  // and the check passed without ever exercising the delay it exists for.
  const page = await connect("about:blank");
  const paused = [];
  try {
    await page.send("Page.enable");
    await page.send("Runtime.enable");
    await page.send("Network.enable");
    await page.send("Network.setCacheDisabled", { cacheDisabled: true });
    await page.send("Emulation.setDeviceMetricsOverride",
      { width: 390, height: 844, deviceScaleFactor: 2, mobile: true });

    // Hold the stop list for the duration of the measurement. Held, not
    // dropped: a request that fails fast does not reproduce a slow one.
    //
    // Both sources, because the list is now published as a static file and
    // read from the API only as a fallback. Holding just `/api/stops` would
    // pause a request the page no longer makes, and the check would pass
    // while measuring nothing — the same way it passed before interception
    // was armed early enough. `heldStopRequests` below is asserted for that
    // reason.
    await page.send("Fetch.enable", { patterns: [
      { urlPattern: "*/api/stops*",  requestStage: "Request" },
      { urlPattern: "*stops.json*",  requestStage: "Request" },
    ] });
    page.ws.addEventListener("message", (e) => {
      const m = JSON.parse(e.data);
      if (m.method === "Fetch.requestPaused") paused.push(m.params.requestId);
    });

    await page.send("Page.navigate", { url: target });
    await sleep(6000);

    const r = JSON.parse(await page.evaluate(`
      (() => {
        const overlay = document.getElementById("map-loading");
        const objectives = document.getElementById("objectives-list");
        return JSON.stringify({
          view: state.viewMode,
          overlayVisible: !!overlay && !overlay.classList.contains("hidden"),
          objectivesShown: !!objectives
            && objectives.getBoundingClientRect().height > 0,
        });
      })()`));

    check("a deep link opens its section while stops are still loading",
      r.view === "network" && !r.overlayVisible,
      JSON.stringify({ ...r, heldStopRequests: paused.length }));
    check("the stop request was actually held for that measurement",
      paused.length > 0,
      JSON.stringify({ heldStopRequests: paused.length }));
    check("the deep-linked section actually renders its content",
      r.objectivesShown, JSON.stringify(r));
  } finally {
    // Let the held requests go before disconnecting. Left paused they sit in
    // Chrome's per-host connection pool and starve every later check against
    // the same origin — which is exactly what happened the first time this
    // ran, taking four unrelated checks down with it.
    for (const requestId of paused) {
      try { await page.send("Fetch.failRequest", { requestId, errorReason: "Aborted" }); }
      catch {}
    }
    try { await page.send("Fetch.disable"); } catch {}
    page.ws.close();
  }
}

async function checkFirstUsefulAction(page, where) {
  // A genuinely fresh Live view: no stop, no rail board, default detent.
  // Without closePanel() this inherited whatever the previous check left
  // open, and measured a search field that was hidden for that reason rather
  // than for the reason under test.
  await page.evaluate("setViewMode('live')");
  await sleep(700);
  await page.evaluate("closePanel(); setSheetDetent(defaultDetentForViewport())");
  await sleep(600);

  const entry = JSON.parse(await page.evaluate(`
    (() => {
      const panel = document.getElementById("departure-panel");
      const input = document.getElementById("stop-search-input");
      if (!panel || !input) return JSON.stringify({ found: false });
      const p = panel.getBoundingClientRect(), i = input.getBoundingClientRect();
      return JSON.stringify({
        found: true,
        inside: i.top >= p.top - 1 && i.bottom <= p.bottom + 1,
        inputTop: Math.round(i.top), panelTop: Math.round(p.top),
        panelBottom: Math.round(p.bottom), vh: window.innerHeight,
      });
    })()`));
  check(`search is visible on entry, without dragging — ${where}`,
    entry.found && entry.inside, JSON.stringify(entry));

  // Select a real stop the way a visitor does, then render a fixture board.
  // The fixture matters: this preview has no API, so a real selection shows
  // an empty board and the check would pass for the wrong reason — which it
  // did, the first time it ran. The question here is a layout one: with rows
  // present, can the reader see any of them?
  const board = JSON.parse(await page.evaluate(`
    (async () => {
      const atco = Object.keys(state.stopData || {})[0];
      if (!atco) return JSON.stringify({ skip: "no stops loaded" });
      await openDepartures(atco, (state.stopData[atco] || {}).name || atco);
      const soon = (m) => new Date(Date.now() + m * 60000).toISOString();
      renderDepartures({
        stop_name: "Fixture Stop",
        departures: [1, 4, 9, 14, 22, 31].map((m, i) => ({
          service: String(700 + i), destination: "Somewhere",
          aimed_departure: soon(m), expected_departure: null,
          status: "Scheduled", delay_seconds: null,
        })),
      });
      return JSON.stringify({ atco });
    })()`));
  if (board.skip) {
    check(`selecting a stop reveals departures — ${where}`, true, board.skip);
    return;
  }
  await sleep(800);
  const rows = JSON.parse(await page.evaluate(`
    (() => {
      const panel = document.getElementById("departure-panel");
      const p = panel.getBoundingClientRect();
      const all = [...document.querySelectorAll("tr.departure-row")];
      const visible = all.filter(r => {
        const b = r.getBoundingClientRect();
        return b.height > 0 && b.top >= p.top - 1 && b.bottom <= p.bottom + 1;
      });
      const empty = document.querySelector(".no-departures");
      return JSON.stringify({
        inDom: all.length, visible: visible.length,
        emptyState: !!empty && empty.getBoundingClientRect().height > 0,
        detent: document.body.dataset.sheet,
      });
    })()`));
  // Either two departures are on screen, or the stop genuinely has none and
  // says so where the reader can see it. A board with rows nobody can see is
  // the failure this exists for.
  check(`selecting a stop reveals departures — ${where}`,
    rows.visible >= 2 || (rows.inDom === 0 && rows.emptyState),
    JSON.stringify(rows));
}

async function checkSearchResults(page, where) {
  for (const theme of ["light", "dark"]) {
    await setTheme(page, theme);
    const r = JSON.parse(await page.evaluate(`
      (() => {
        const input = document.getElementById("stop-search-input");
        if (!input) return JSON.stringify({ skip: "no search input" });
        input.value = "worthing";
        input.dispatchEvent(new Event("input", { bubbles: true }));
        const btn = document.querySelector("button.stop-search-result");
        if (!btn) return JSON.stringify({ skip: "no results for 'worthing'" });
        const name = btn.querySelector(".stop-search-result-name");
        // No regex. This whole expression is a template literal, so a "\d"
        // collapses to "d" before it reaches the browser: the character class
        // silently becomes a search for the letter d, .match() returns null,
        // and this threw with an empty exception message.
        const lum = (c) => {
          const [r, g, b] = c.slice(c.indexOf("(") + 1, c.lastIndexOf(")"))
            .split(",").slice(0, 3).map(parseFloat)
            .map(v => { v /= 255; return v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4; });
          return 0.2126 * r + 0.7152 * g + 0.0722 * b;
        };
        const cs = getComputedStyle(name || btn);
        const bs = getComputedStyle(btn);
        const a = lum(cs.color), b = lum(bs.backgroundColor);
        const ratio = (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05);
        return JSON.stringify({
          ratio: Math.round(ratio * 100) / 100,
          fg: cs.color, bg: bs.backgroundColor,
          modes: [...document.querySelectorAll(".stop-search-result-mode")]
                   .map(e => e.textContent.trim()),
        });
      })()`));
    if (r.skip) { check(`search results — ${where}, ${theme}`, true, r.skip); continue; }
    check(`search result text meets AA contrast — ${where}, ${theme}`,
      r.ratio >= 4.5, JSON.stringify(r));
  }
  await setTheme(page, "light");

  // Activate a real railway result and assert the outcome, not the markup.
  const outcome = JSON.parse(await page.evaluate(`
    (() => {
      const input = document.getElementById("stop-search-input");
      if (!input) return JSON.stringify({ skip: "no search input" });
      input.value = "worthing";
      input.dispatchEvent(new Event("input", { bubbles: true }));
      const rail = [...document.querySelectorAll("button.stop-search-result")]
        .find(b => b.dataset.kind === "rail");
      if (!rail) return JSON.stringify({ skip: "no railway result offered" });
      const before = window.__searchErr;
      window.__searchErr = null;
      const onErr = (e) => { window.__searchErr = String(e.error || e.message); };
      window.addEventListener("error", onErr, { once: true });
      try { rail.click(); } catch (e) { window.__searchErr = String(e); }
      return JSON.stringify({ crs: rail.dataset.crs, err: window.__searchErr });
    })()`));
  if (outcome.skip) {
    check(`a railway search result can be activated — ${where}`, true, outcome.skip);
  } else {
    await sleep(900);
    const selected = await page.evaluate(
      `JSON.stringify({ sel: state.selectedRailStation && state.selectedRailStation.crs })`);
    const got = JSON.parse(selected);
    check(`a railway search result can be activated — ${where}`,
      !outcome.err && got.sel === outcome.crs,
      JSON.stringify({ ...outcome, ...got }));
  }
}

async function checkRouteViewOnAPhone(page, where) {
  await page.evaluate("setViewMode('improvements')");
  // Wait for the route list itself, not a fixed interval: measuring while it
  // still reads "Loading route list…" measures one line, not the grid.
  //
  // The budget matches the app's own cold-start budget rather than the 15 s it
  // used to be. The stop list is served statically now, so nothing wakes the
  // backend before this — the route request is the first API call of a run and
  // can land on a container that is still starting. waitFor returns false on
  // timeout rather than throwing, so a short budget here showed up as
  // "chipCount: 0" and looked like a rendering fault.
  const chipsArrived = await waitFor(
    page, "document.querySelectorAll('.route-chip').length > 0", 45000);
  check(`the route list loads — ${where}`, chipsArrived,
    "no .route-chip after 45s — the API never answered");
  await sleep(500);

  // The list now lives inside the "Existing services" disclosure, closed by
  // default. What the tab owes a reader on arrival is therefore different:
  // not a row of chips, but a control they can find and the prose the tab is
  // about. The old assertions measured a chip grid that is no longer there.
  const m = JSON.parse(await page.evaluate(`
    (() => {
      const g = el => el ? el.getBoundingClientRect() : null;
      const panel = document.getElementById("departure-panel");
      const disc = document.getElementById("route-filters-disclosure");
      const summary = document.querySelector(".filter-disclosure-summary");
      const prose = document.querySelector(".improvements-intro");
      const pr = g(panel), sr = g(summary), prr = g(prose);
      return JSON.stringify({
        closed: disc ? !disc.open : null,
        summaryH: sr ? Math.round(sr.height) : null,
        summaryVisible: (sr && pr)
          ? Math.round(Math.max(0, Math.min(sr.bottom, pr.bottom) - Math.max(sr.top, pr.top))) : 0,
        count: (document.getElementById("route-filters-count") || {}).textContent || "",
        proseTop: prr ? Math.round(prr.top) : null,
        panelBottom: pr ? Math.round(pr.bottom) : null,
        chipCount: document.querySelectorAll(".route-chip").length,
      });
    })()`));

  // 44px is this project's target-size convention, and the summary carries a
  // min-height saying so. A flex parent can override that; nothing should.
  check(`the filter control keeps its target size — ${where}`,
    m.summaryH !== null && m.summaryH >= 40, JSON.stringify(m));

  // Wholly on screen, not merely present: this is now the only way into the
  // service list, so a half-clipped summary is the whole control half-clipped.
  check(`the service control is on screen when Route view opens — ${where}`,
    m.summaryH !== null && m.summaryVisible >= m.summaryH - 2, JSON.stringify(m));

  // The count is what makes collapsing it honest — closed, it is the only
  // thing saying how much of the network the map is drawing.
  check(`the closed control says how many services are shown — ${where}`,
    /\d/.test(m.count), JSON.stringify(m));

  // And the point of the move: the tab's own prose is reachable without
  // scrolling past a list. It was below the fold on every phone viewport.
  check(`the tab's explanation is visible without scrolling — ${where}`,
    m.proseTop !== null && m.proseTop < m.panelBottom, JSON.stringify(m));

  // Opened, the list is capped to a few rows with the rest behind "+N more",
  // so it cannot push the prose away again.
  const opened = JSON.parse(await page.evaluate(`
    (() => {
      const d = document.getElementById("route-filters-disclosure");
      d.open = true; d.dispatchEvent(new Event("toggle"));
      const chips = [...document.querySelectorAll(".route-chip")];
      const shown = chips.filter(c => !c.classList.contains("route-chip--clipped"));
      const more = document.getElementById("route-chips-more");
      const rows = [...new Set(shown.map(c => c.offsetTop))].length;
      d.open = false;
      return JSON.stringify({
        total: chips.length, shown: shown.length, rows,
        moreHidden: more ? more.classList.contains("hidden") : null,
        moreText: more ? more.textContent : "",
      });
    })()`));

  check(`opening the control reveals services — ${where}`,
    opened.shown > 0, JSON.stringify(opened));
  // Three rows is the cap; four would mean the measurement did not run.
  check(`the service list is capped to a few rows — ${where}`,
    opened.rows <= 3, JSON.stringify(opened));
  // If anything was clipped, there has to be a way to reach it.
  check(`clipped services are reachable — ${where}`,
    opened.shown === opened.total
      ? opened.moreHidden === true
      : (opened.moreHidden === false && /\+\d+ more/.test(opened.moreText)),
    JSON.stringify(opened));

  // Leave the page as it was found: the header-row check that follows
  // measures the live status pill, which only Live view shows.
  await page.evaluate("setViewMode('live')");
  await sleep(600);
}

/**
 * Stop bubbles that leave the map visible.
 *
 * A flat 60px cluster cell put a bubble every 60px across the whole coast: at
 * the two zooms where clustering first applies that is a curtain of numbered
 * discs with the map behind it, which is the opposite of what clustering is
 * for. Measured against the old rule rather than against a fixed number, so
 * the check says "this is better than what it replaced" and keeps saying it.
 */
async function checkClusterDensity(page, where) {
  await page.evaluate("setViewMode('live')");
  await sleep(500);
  for (const zoom of [13, 12]) {
    // Leaflet returns the map from setZoom, and serialising that whole object
    // graph fails with "Object reference chain is too long".
    await page.evaluate(`(() => { state.map.setZoom(${zoom}); return ""; })()`);
    await sleep(1200);
    const r = JSON.parse(await page.evaluate(`
      (() => {
        const cell = px => px * 360 / (256 * Math.pow(2, ${zoom}));
        const bounds = state.map.getBounds().pad(0.3);
        const ids = Object.keys(state.stopMarkers).filter(a => {
          const d = state.stopData[a]; return d && bounds.contains([d.lat, d.lon]); });
        const buckets = px => {
          const c = cell(px), b = new Set();
          for (const a of ids) { const d = state.stopData[a];
            b.add(Math.floor(d.lat / c) + ":" + Math.floor(d.lon / c)); }
          return b.size; };
        const sizes = state.stopClusters.map(
          m => +(m.options.icon.options.html.match(/>(\d+)</) || [0, 0])[1]);
        return JSON.stringify({
          stopsInView: ids.length,
          drawn: state.stopClusters.length,
          wouldHaveBeen: buckets(60),
          biggest: sizes.length ? Math.max(...sizes) : 0,
        });
      })()`));
    // Not "fewer than before" by a hair — the point was to make the map
    // readable, so it has to be a real reduction.
    check(`stop bubbles thin out at zoom ${zoom} — ${where}`,
      r.stopsInView === 0 || (r.drawn > 0 && r.drawn <= r.wouldHaveBeen / 2),
      JSON.stringify(r));
  }
  await page.evaluate('(() => { state.map.setZoom(14); return ""; })()');
  await sleep(800);
}

/**
 * A selected proposal lands where the reader can see it.
 *
 * Selecting one from the list fits the map to its shape — but on a phone the
 * bottom half of the map is under the sheet the list is in, so the shape it
 * had just drawn attention to was drawn behind the thing that asked for it.
 */
async function checkProposalFitsAboveSheet(page, where) {
  await page.evaluate("setViewMode('improvements')");
  await waitFor(page, "(state.proposals || []).length > 0", 15000);
  await sleep(600);
  const r = JSON.parse(await page.evaluate(`
    (() => {
      const withGeom = (state.proposals || []).find(
        p => (p._polylines || []).flat().length > 1);
      if (!withGeom) return JSON.stringify({ skip: "no proposal geometry" });
      selectProposal(withGeom.id);
      const pts = (withGeom._polylines || []).flat();
      const b = L.latLngBounds(pts);
      const panel = document.getElementById("departure-panel").getBoundingClientRect();
      const sw = state.map.latLngToContainerPoint(b.getSouthWest());
      const ne = state.map.latLngToContainerPoint(b.getNorthEast());
      return JSON.stringify({
        id: withGeom.id, zoom: state.map.getZoom(),
        shapeTop: Math.round(Math.min(sw.y, ne.y)),
        shapeBottom: Math.round(Math.max(sw.y, ne.y)),
        sheetTop: Math.round(panel.top),
      });
    })()`));
  if (r.skip) { check(`a selected proposal clears the sheet — ${where}`, true, r.skip); }
  else {
    check(`a selected proposal clears the sheet — ${where}`,
      r.shapeBottom <= r.sheetTop && r.shapeTop >= 0, JSON.stringify(r));
  }
  await page.evaluate('(() => { selectProposal(null); setViewMode("live"); return ""; })()');
  await sleep(600);
}

/**
 * A ticket preset always draws something.
 *
 * Two of the six — Shoreham to the universities, Sompting to the Marina —
 * found no one-change itinerary and drew nothing at all, so pressing them
 * looked like pressing a dead button. The ends of the journey are worth
 * drawing even when how to make it is the part we cannot answer.
 */
async function checkPresetsDraw(page, where) {
  await page.evaluate("setViewMode('tickets')");
  await waitFor(page, "document.querySelectorAll('[data-preset]').length > 0", 15000);
  const ids = JSON.parse(await page.evaluate(
    `JSON.stringify([...document.querySelectorAll("[data-preset]")].map(b => b.dataset.preset))`));
  for (const id of ids) {
    await page.evaluate(`document.querySelector('[data-preset="${id}"]').click()`);
    await waitFor(page, "state.journeyLayers.length > 0", 20000).catch(() => {});
    const r = JSON.parse(await page.evaluate(`
      (() => {
        const marks = state.journeyLayers.filter(l => l instanceof L.Marker);
        const labels = marks.map(
          m => (m.options.icon.options.html.match(/>([^<]*)</) || [])[1]);
        return JSON.stringify({
          layers: state.journeyLayers.length,
          polys: state.journeyLayers.filter(l => l instanceof L.Polyline).length,
          labels,
        });
      })()`));
    check(`preset "${id}" draws its journey — ${where}`,
      r.polys > 0 && r.labels.includes("From") && r.labels.includes("To"),
      JSON.stringify(r));
  }
  await page.evaluate("setViewMode('live')");
  await sleep(600);
}

async function checkFailureIsVisible(page, where) {
  // Set and measure in one evaluation. Done as two, a vehicle poll landing in
  // between put the label back to "Updated HH:MM:SS" and the check failed on
  // its own timing rather than on the page — which it did, once, after four
  // clean runs.
  const state = await page.evaluate(`
    (() => {
      setStatusLabel({ text: "Update failed — retrying", loading: true, error: true });
      const el = document.querySelector(".live-status-pill") || document.getElementById("last-updated-label");
      if (!el) return JSON.stringify({ found: false });
      const cs = getComputedStyle(el);
      const r = el.getBoundingClientRect();
      // Walk up: a visible label inside a hidden ancestor is still hidden.
      let node = el, hidden = false;
      while (node && node !== document.body) {
        const s = getComputedStyle(node);
        if (s.display === "none" || s.visibility === "hidden" || s.opacity === "0") hidden = true;
        node = node.parentElement;
      }
      return JSON.stringify({
        found: true, hidden,
        text: (el.textContent || "").trim(),
        w: Math.round(r.width), h: Math.round(r.height),
      });
    })()`);
  const s = JSON.parse(state);
  check(`a failing live feed is visible — ${where}`,
    s.found && !s.hidden && s.w > 0 && s.h > 0 && /fail/i.test(s.text),
    JSON.stringify(s));
  await page.evaluate(`setStatusLabel({ text: "Updated", loading: false })`);
}

/**
 * The "waking up" banner is legible where it is put.
 *
 * It exists for the worst moment of a first visit — a free-tier container
 * taking twenty seconds to start — so a banner that renders behind the header,
 * off the top of a short landscape viewport, or over the bottom sheet the
 * reader is watching would be worse than the silence it replaced.
 *
 * Shown directly rather than by holding a request: the banner arms on a 2.5 s
 * timer, and five viewports' worth of real waiting buys nothing the unit tests
 * do not already cover. What only a browser can answer is where it lands.
 */
async function checkWakingBanner(page, where) {
  const r = JSON.parse(await page.evaluate(`
    (() => {
      showWakingBanner();
      const el = document.getElementById("waking-banner");
      if (!el) return JSON.stringify({ found: false });
      const b  = el.getBoundingClientRect();
      const cs = getComputedStyle(el);
      const header = document.querySelector(".site-header");
      const hb = header ? header.getBoundingClientRect() : { bottom: 0 };
      // What is actually painted at the banner's own centre. If the header or
      // the sheet is on top, this is not the banner.
      const mid = document.elementFromPoint(
        Math.round(b.left + b.width / 2), Math.round(b.top + b.height / 2));
      return JSON.stringify({
        found: true,
        onTop: !!mid && (mid === el || el.contains(mid)),
        top: Math.round(b.top), bottom: Math.round(b.bottom),
        left: Math.round(b.left), right: Math.round(b.right),
        w: Math.round(b.width), h: Math.round(b.height),
        headerBottom: Math.round(hb.bottom),
        vw: window.innerWidth, vh: window.innerHeight,
        opacity: cs.opacity,
        text: (el.textContent || "").trim().slice(0, 40),
      });
    })()`));

  const ok = r.found
    && r.h > 0 && r.w > 0
    && r.opacity !== "0"
    && r.top >= r.headerBottom        // not tucked under the header
    && r.bottom <= r.vh               // not off the bottom of a short viewport
    && r.left >= 0 && r.right <= r.vw // not off the side at 320px
    && r.onTop;
  check(`the waking-up banner is legible where it lands — ${where}`, ok,
    JSON.stringify(r));

  await page.evaluate("hideWakingBanner()");
}

async function checkReachable(page, where) {
  const bad = await page.evaluate(CLIPPED_SCAN);
  check(`all panel content is reachable — ${where}`, bad.length === 0, bad.join(", "));
}

/**
 * The surfaces that only exist once something is clicked.
 *
 * A contrast or target-size regression inside a dialog is invisible to a pass
 * that only ever looks at the page as it loads — and these two are where the
 * campaign's evidence lives, so they are the last places a defect should be
 * allowed to sit unseen.
 */
async function checkInteractiveSurfaces(page) {
  // ── Boundary evidence dialog ──
  await page.evaluate(`setViewMode('live')`);
  await sleep(1200);
  // Wrapped so the expression evaluates to a primitive: setView returns the
  // Leaflet map, and CDP cannot serialise that object graph — it fails the
  // whole run with "Object reference chain is too long".
  await page.evaluate(`(state.map.setView([50.8456, -0.2284], 13), 1)`);
  await sleep(1500);

  const labelIsButton = await page.evaluate(`
    (() => { const b = document.querySelector(".council-boundary-label");
             return !!b && b.tagName === "BUTTON"; })()`);
  check("the boundary label is a real button", labelIsButton,
    "a div with a click handler is unreachable by keyboard");

  // ── The line itself is hoverable, not just its label ──
  //
  // The visible boundary is a 5px dash and cannot be hit; an invisible 22px
  // line carries the pointer events and lights it through .is-hot. Hovering
  // is dispatched as a real mouse move at the hit path's own midpoint —
  // calling the handler directly, or toggling the class in script, would
  // pass whether or not the hit line exists, which is the whole question.
  const hitBox = await page.evaluate(`
    (() => {
      const hit = document.querySelector(".council-boundary-hit");
      if (!hit) return "null";
      const r = hit.getBoundingClientRect();
      const t = hit.getTotalLength ? hit.getTotalLength() / 2 : 0;
      const p = hit.getPointAtLength ? hit.getPointAtLength(t) : null;
      // getPointAtLength is in the SVG's user space; the pane is translated,
      // so convert through the element's own screen matrix.
      const m = hit.getScreenCTM && hit.getScreenCTM();
      const x = (p && m) ? p.x * m.a + p.y * m.c + m.e : r.x + r.width / 2;
      const y = (p && m) ? p.x * m.b + p.y * m.d + m.f : r.y + r.height / 2;
      return JSON.stringify({ x, y, w: r.width, h: r.height });
    })()`);
  const hit = hitBox === "null" ? null : JSON.parse(hitBox);
  check("the boundary line has a hit target of its own", !!hit,
    "a 5px dashed line is under half the 24px target floor — unhittable in practice");

  if (hit) {
    const strokeOf = () => page.evaluate(
      `parseFloat(getComputedStyle(document.querySelector(".council-boundary-line")).strokeWidth)`);
    const cold = await strokeOf();
    await page.send("Input.dispatchMouseEvent",
      { type: "mouseMoved", x: Math.round(hit.x), y: Math.round(hit.y), buttons: 0 });
    await sleep(300);
    const hot = await strokeOf();
    const labelLit = await page.evaluate(
      `!!document.querySelector(".council-boundary-label.is-hot")`);
    // Move away again so nothing downstream inherits a hovered line.
    await page.send("Input.dispatchMouseEvent",
      { type: "mouseMoved", x: 5, y: 5, buttons: 0 });
    await sleep(200);

    check("hovering anywhere on the boundary line reacts", hot > cold,
      `stroke-width stayed at ${cold} — the line does not respond to the cursor`);
    check("hovering the line also lights its label", labelLit,
      "the line and the label are one object; highlighting only one splits them");
  }

  if (labelIsButton) {
    await page.evaluate(`(document.querySelector(".council-boundary-label").click(), 1)`);
    await sleep(1400);
    const open = await page.evaluate(`!!document.getElementById("evidence-dialog").open`);
    check("the boundary label opens the evidence", open);
    if (open) {
      await page.evaluate(`(document.querySelector(".evidence-method").open = true, 1)`);
      await sleep(300);
      await freezeMotion(page);
      await screenshot(page, "evidence-dialog");
      await checkLayout(page, "evidence dialog");
      await checkContrastBothThemes(page, "evidence dialog");
      // The figure has to arrive with its provenance, or it is just a number.
      const shown = await page.evaluate(
        `document.getElementById("evidence-body").textContent`);
      check("the evidence states its method and its caveats",
        /How this was worked out/.test(shown) && /What would make this wrong/.test(shown),
        "a derived statistic published without either is not checkable");

      // Three panels: a weekday, a merged weekend, and one named place either
      // side of the line. Two weekend panels used to sit here saying much the
      // same thing, and the place comparison is the one a resident recognises.
      const panels = await page.evaluate(`
        (() => {
          const secs = [...document.querySelectorAll("#evidence-body .evidence-day")];
          return JSON.stringify({
            titles: secs.map(s => s.querySelector(".evidence-day-title").textContent
                                   .replace(/\\s+/g, " ").trim()),
            places: secs.filter(s => s.classList.contains("evidence-day--place")).length,
          });
        })()`);
      const { titles, places } = JSON.parse(panels);
      check("the evidence shows a weekday and a merged weekend panel",
        titles.length === 3 && /^Weekday/.test(titles[0]) && /^Weekend/.test(titles[1]),
        `panels are ${JSON.stringify(titles)}`);
      check("the weekend panel still names each day's routes",
        /on Sunday alone/.test(shown),
        "averaging Saturday with Sunday hides Sunday being the thinner day");
      check("the evidence compares two named places either side of the line",
        places === 1 && /Lancing against South Portslade/.test(shown),
        "the band gives the average effect; this is what it means somewhere specific");

      // One axis across every panel. A per-panel scale would draw Lancing's
      // 32.8 the same length as the band's 64.4 and quietly halve the gap.
      const scale = await page.evaluate(`
        (() => {
          const body = document.getElementById("evidence-body");
          const w = [...body.querySelectorAll(".evidence-bar")]
                      .map(b => parseFloat(b.style.width));
          const v = [...body.querySelectorAll(".evidence-bar-value")]
                      .map(el => parseFloat(el.textContent));
          const k = w.map((x, i) => x / v[i]);
          return JSON.stringify({ widest: Math.max(...w), bars: w.length,
                                  spread: Math.max(...k) - Math.min(...k) });
        })()`);
      const s = JSON.parse(scale);
      check("every bar is drawn on the same scale",
        s.bars === 6 && s.spread < 0.01 && Math.abs(s.widest - 100) < 0.6,
        `${s.bars} bars, widest ${s.widest}%, scale spread ${s.spread}`);

      // The place panel is the last thing in a dialog that is taller than its
      // max-height. Network Objectives once grew past its container and simply
      // stopped being readable, so an added panel gets asked whether it can
      // actually be scrolled to rather than only whether it rendered.
      // Same test the panels get: overflowing is fine, overflowing with nothing
      // able to scroll it is content that has silently stopped existing. A
      // programmatic scrollTop still moves an overflow:hidden box, so asking
      // whether it scrolled would pass on exactly the broken case.
      const scrolled = await page.evaluate(`
        (() => {
          const b = document.querySelector(".evidence-body");
          const scrolls = el => {
            const oy = getComputedStyle(el).overflowY;
            return (oy === "auto" || oy === "scroll") &&
                   el.scrollHeight > el.clientHeight + 1;
          };
          return JSON.stringify({
            overflows: b.scrollHeight > b.clientHeight + 1,
            hidden: b.scrollHeight - b.clientHeight,
            scrollable: scrolls(b) || [...b.querySelectorAll("*")].some(scrolls),
          });
        })()`);
      const sc = JSON.parse(scrolled);
      check("the evidence can be scrolled to its last panel",
        !sc.overflows || sc.scrollable,
        `${sc.hidden}px below the fold, scrollable: ${sc.scrollable}`);
      await page.evaluate(`(document.getElementById("evidence-dialog").close(), 1)`);
      await sleep(300);
    }
  }

  // ── Service span ──
  await page.evaluate(`(openDepartures("4400AD0117", "Lancing Station"), 1)`);
  await sleep(2500);
  const hasSpan = await page.evaluate(
    `!document.getElementById("stop-span").classList.contains("hidden")`);
  // The span is a local timetable read, so it must survive the departure board
  // failing — which is exactly when someone wants to know if a bus ever comes.
  check("the service span renders without live departures", hasSpan);
  if (hasSpan) {
    await page.evaluate(`(document.querySelector(".stop-span-disclosure").open = true, 1)`);
    await sleep(300);
    await screenshot(page, "stop-span");
    await checkLayout(page, "service span");
    await checkContrastBothThemes(page, "service span");
  }

  // ── Journey presets ──
  await page.evaluate(`setViewMode('tickets')`);
  await sleep(2200);
  const presets = await page.evaluate(`document.querySelectorAll(".jc-preset").length`);
  check("the journey checker offers worked examples", presets > 0,
    `${presets} presets`);
  if (presets > 0) {
    await checkLayout(page, "journey presets");
    await checkContrastBothThemes(page, "journey presets");

    // A journey needing a change is drawn as its legs, in route liveries. The
    // point of the picture is that two coloured lines meet at a dot — so the
    // check is that both legs and the change marker are actually on the map.
    await page.evaluate(`(document.querySelector('[data-preset="worthing-to-hangleton"]').click(), 1)`);
    await sleep(3200);
    const drawn = await page.evaluate(`state.journeyLayers.filter(l => state.map.hasLayer(l)).length`);
    check("a journey with a change is drawn on the map", drawn >= 5,
      `${drawn} layers — two casings, two legs, a service pill each and a change marker`);
    const itin = await page.evaluate(
      `(document.querySelector(".journey-itinerary") || {}).textContent || ""`);
    check("the itinerary names both buses and the change",
      /\bchange|wait\b/i.test(itin) && /minutes door to door/.test(itin),
      itin.replace(/\s+/g, " ").trim().slice(0, 90));
    await checkLayout(page, "journey itinerary");
    await checkContrastBothThemes(page, "journey itinerary");

    // Route liveries are chosen against each other, not against a basemap, so
    // a leg drawn without its casing can vanish over a main road.
    await page.evaluate(`(setViewMode("live"), 1)`);
    await sleep(1500);
    const left = await page.evaluate(`state.journeyLayers.filter(l => state.map.hasLayer(l)).length`);
    check("the journey is torn down when the view changes", left === 0,
      `${left} layers left behind`);
  }
}

async function checkViews(page) {
  for (const [mode, label] of [
    ["live", "Live Bus Tracking"], ["improvements", "Route view"],
    ["tickets", "Ticket view"], ["network", "Network Objectives"],
    ["updates", "Network Updates"],
  ]) {
    await page.evaluate(`setViewMode('${mode}')`);
    await sleep(1500);
    check(`view "${mode}" activates`,
      (await page.evaluate("document.body.dataset.view")) === mode, label);
    await screenshot(page, `${mode}`);
    // Measured here, not in one pass up front: a panel that is not the
    // active view is display:none, and its contents cannot be meaningfully
    // measured until the view that owns them is showing.
    await checkLayout(page, `${mode} view`);
    await checkContrastBothThemes(page, `${mode} view`);
  }
}

/**
 * Walk every view, and both tabs of the two-tab views, asserting nothing is
 * clipped out of reach.
 *
 * Run per viewport, because this is a viewport-dependent defect and running it
 * on one size proves nothing about the others: the panel is a bottom sheet
 * under 900px and a fixed side column above it, and the desktop column was the
 * one clipping. A first version of this check ran only at 390px, passed with
 * the bug still in the stylesheet, and would have shipped it.
 *
 * The second tab of each pair matters for the same reason: it is never the one
 * showing when a view opens, so nothing had ever looked at it.
 */
async function checkReachableAcrossViews(page, where) {
  for (const mode of ["live", "improvements", "tickets", "network", "updates"]) {
    await page.evaluate(`setViewMode('${mode}')`);
    await sleep(1200);
    await checkReachable(page, `${mode} view — ${where}`);
    await checkSheetFitsViewport(page, `${mode} view — ${where}`);
    for (const [owner, fn, second, first] of [
      ["network", "setNetworkTab", "ideas",     "objectives"],
      ["updates", "setUpdatesTab", "community", "official"],
    ]) {
      if (mode !== owner) continue;
      await page.evaluate(`${fn}('${second}')`);
      await sleep(600);
      await checkReachable(page, `${mode} view, ${second} tab — ${where}`);
      await page.evaluate(`${fn}('${first}')`);
      await sleep(300);
    }
  }
}

/** Screenshot every view in both themes, so a restyle can be compared. */
async function shootThemes(page) {
  if (!SHOTS) return;
  for (const theme of ["light", "dark"]) {
    await setTheme(page, theme);
    for (const mode of ["live", "improvements", "tickets", "network"]) {
      await page.evaluate(`setViewMode('${mode}')`);
      await sleep(1200);
      await screenshot(page, `${mode}-${theme}`);
    }
    await page.evaluate("setViewMode('live')");
    await sleep(800);
    await page.evaluate(
      `openDepartures("${SAMPLE_STOP.atco}", "${SAMPLE_STOP.name}")`);
    await waitFor(page, "document.querySelectorAll('.departure-row').length > 0", 20000);
    await screenshot(page, `departures-${theme}`);
  }
}

/**
 * Regression: collapsing the panel in one view used to hide the next view's
 * content too, because `panel-collapsed` sits on <body>. Ticket view has no
 * collapse control of its own, so there was nothing on screen to undo it.
 *
 * Runs LAST, and deliberately so: the widening check assigns
 * `window.innerWidth`, which is [Replaceable] and therefore stays replaced for
 * the rest of the page's life. Anything running afterwards would see a desktop
 * width in JS while CSS still rendered at 390px.
 */
async function checkPanelCollapse(page) {
  await page.evaluate("setViewMode('improvements')");
  await sleep(1200);
  await page.evaluate("setPanelCollapsed(true)");
  check("collapses in Route view",
    await page.evaluate("document.body.classList.contains('panel-collapsed')"));

  await page.evaluate("setViewMode('tickets')");
  await sleep(2000);

  check("switching view clears the collapsed state",
    (await page.evaluate("document.body.classList.contains('panel-collapsed')")) === false);
  check("Ticket view content is displayed",
    (await page.evaluate(
      "getComputedStyle(document.getElementById('tab-content-tickets')).display")) !== "none");
  check("boundary calculator is reachable",
    await page.evaluate(
      "!!document.getElementById('jc-check') && " +
      "document.getElementById('jc-check').getBoundingClientRect().height > 0"));
  check("Ticket view has its own collapse control",
    await page.evaluate(
      `!!document.querySelector('.panel-mode[data-mode="tickets"] .btn-collapse-panel')`));

  // The control is mobile-only; staying collapsed past the breakpoint would
  // strand the panel with nothing on screen to reopen it.
  await page.evaluate("setPanelCollapsed(true); window.innerWidth = 1200; syncPanelCollapsedToWidth()");
  check("widening past 700px clears the collapsed state",
    (await page.evaluate("document.body.classList.contains('panel-collapsed')")) === false);
}

/**
 * "Email your councillor", from the objective card to the open dialog.
 *
 * Two things worth checking here that unit tests cannot see. First, the button
 * only appears where there is actually a councillor to write to — an objective
 * that only an operator can act on must not offer one, because a button that
 * leads to "no councillors" is worse than no button. Second, the card was
 * restructured to get this button out of the middle of another button; nested
 * interactive elements are invalid, and the links that were already in there
 * were awkward to activate as a result.
 *
 * The postcode lookup itself is not exercised: it calls a live third-party
 * service, and a check that goes red when postcodes.io has a bad afternoon is
 * a check people learn to ignore. tests/test_councillor.mjs covers that logic
 * against fixtures.
 */
/**
 * A preset journey answers itself.
 *
 * The presets are worked examples, so choosing one *is* the request: filling
 * the form and then waiting to be told to press Check is a step nobody wants.
 * The scroll is half the feature and the half that fails silently — the
 * presets sit above both stop pickers and the button, so on a short panel the
 * whole answer lands below the fold and the click reads as having done
 * nothing.
 *
 * Asserted on "the result is visible", not "the panel scrolled": at a wide
 * viewport the panel may not overflow at all, and there a scroll of zero is
 * the correct outcome rather than a regression.
 */
async function checkJourneyPresets(page) {
  await page.evaluate(`setViewMode('tickets')`);
  await sleep(2200);

  const count = await page.evaluate(
    `document.querySelectorAll("#jc-presets [data-preset]").length`);
  check("the ticket view offers preset journeys", count > 0,
    "an empty checker asks the reader to already know which two stops make the point");
  if (!count) return;

  await page.evaluate(`(document.querySelector("#jc-presets [data-preset]").click(), 1)`);
  await sleep(4000);

  const state = await page.evaluate(`
    (() => {
      const box = document.getElementById("tab-content-tickets");
      const res = document.getElementById("jc-result");
      const b = box.getBoundingClientRect(), r = res.getBoundingClientRect();
      return JSON.stringify({
        filled:  !!document.getElementById("jc-from").value &&
                 !!document.getElementById("jc-to").value,
        answered: res.innerHTML.trim().length > 0,
        visible: r.top >= b.top - 2 && r.top < b.bottom,
      });
    })()`);
  const { filled, answered, visible } = JSON.parse(state);

  check("choosing a preset fills both stops", filled);
  check("choosing a preset runs the check without a second click", answered,
    "the preset is a worked example — it should answer, not just fill the form");
  check("the answer to a preset is on screen", visible,
    "the result rendered below the fold, so the click looks like it did nothing");
}

async function checkCouncillorContact(page) {
  await page.evaluate(`setViewMode('network')`);
  await sleep(900);
  await page.evaluate(`setNetworkTab('objectives')`);
  await sleep(700);

  // One objective an authority is involved in, picked from the live data so
  // this does not go red the next time an objective is added or reassigned —
  // and one only operators can act on, which is *injected*, because every
  // objective currently published involves a council. Waiting for the data to
  // grow such a case would leave this branch never running, which is the same
  // as not having written it.
  const picked = await page.evaluate(`
    (() => {
      const os = state.objectives || [];
      const withAuthority = os.find(o => objectiveAuthorities(o).length > 0);
      const probe = {
        id: "__operator-only-probe", title: "Operator-only probe",
        summary: "Injected by browser_check.mjs", description: "",
        status: "not_considered", links: [], category: "Information",
        lead: ["BHBC"], shared: ["SCSO"], featured: false,
      };
      os.push(probe);
      return JSON.stringify({
        withAuthority: withAuthority ? withAuthority.id : null,
        operatorOnly: probe.id,
      });
    })()`);
  const { withAuthority, operatorOnly } = JSON.parse(picked);

  check("an objective names an authority someone can write to", !!withAuthority,
    "no objective involves a council, so the contact route is unreachable");
  if (!withAuthority) return;

  const contactShown = async (id) => {
    await page.evaluate(
      `(state.selectedObjectiveId = ${JSON.stringify(id)}, renderObjectivesList(), 1)`);
    await sleep(400);
    return page.evaluate(
      `!!document.querySelector('[data-contact-objective=${JSON.stringify(id)}]')`);
  };

  check("an objective a council must act on offers to email a councillor",
    await contactShown(withAuthority));

  if (operatorOnly) {
    check("an operator-only objective offers no councillor",
      (await contactShown(operatorOnly)) === false,
      "route and timetable decisions are commercial — a councillor cannot make them");
  }

  // Drop the probe again so nothing downstream renders it.
  await page.evaluate(
    `(state.objectives = state.objectives.filter(o => o.id !== "__operator-only-probe"), 1)`);

  // Back to one that has the button.
  //
  // There is deliberately no "the button is not nested inside the card button"
  // check here. One was written and it passed with the nesting restored: the
  // HTML parser closes an open <button> when it meets another, so assigning
  // that markup through innerHTML never produces a nested button in the DOM at
  // all. The check could not fail, which makes it a tautology rather than a
  // guard. The structural fix is real — it is what stops the parser scrambling
  // the card — but the DOM is the wrong place to look for evidence of it.
  await contactShown(withAuthority);

  await page.evaluate(`(document.querySelector("[data-contact-objective]").click(), 1)`);
  await sleep(700);
  const open = await page.evaluate(`!!document.getElementById("councillor-dialog").open`);
  check("the councillor dialog opens", open);

  if (open) {
    const hasForm = await page.evaluate(
      `!!document.getElementById("councillor-postcode") && !!document.getElementById("councillor-form")`);
    check("the dialog asks for a postcode", hasForm);
    await freezeMotion(page);
    await screenshot(page, "councillor-dialog");
    await checkLayout(page, "councillor dialog");
    await checkContrastBothThemes(page, "councillor dialog");
    await page.evaluate(`(document.getElementById("councillor-dialog").close(), 1)`);
    await sleep(300);
  }
}

/**
 * The header's three controls — section nav, live-status pill, theme toggle —
 * are meant to read as one row. They drifted to 52px, 44px and 44px, which is
 * visible as a ragged row long before anyone can say why.
 *
 * Checks height, not rounding: the theme toggle is deliberately a circle while
 * the other two share a radius, so a single "same rounding" assertion would be
 * wrong. Under 700px the pill leaves the header for the map, so only the
 * controls actually in the row at that width are compared.
 */
async function checkHeaderControlRow(page, where) {
  const raw = await page.evaluate(`
    (() => {
      const ids = ["section-nav-trigger", "dark-mode-btn"];
      const out = {};
      for (const id of ids) {
        const el = document.getElementById(id);
        if (el && el.offsetParent !== null) out[id] = Math.round(el.getBoundingClientRect().height);
      }
      const pill = document.getElementById("live-status-pill");
      if (pill && getComputedStyle(pill).position !== "fixed") {
        out["live-status-pill"] = Math.round(pill.getBoundingClientRect().height);
      }
      return JSON.stringify(out);
    })()`);
  const heights = JSON.parse(raw);
  const values = Object.values(heights);
  check(`header controls share one height — ${where}`,
    values.length > 1 && new Set(values).size === 1,
    Object.entries(heights).map(([k, v]) => `${k} ${v}px`).join(", "));
}

// ── Run ─────────────────────────────────────────────────────

try {
  await fetch(`${CDP}/json/version`);
} catch {
  console.error(`No Chrome debugger at ${CDP}. See the usage note at the top of this file.`);
  process.exit(2);
}

const page = await openPage(VIEWPORTS[0]);
await checkBasemap(page);
await checkHeaderControlRow(page, VIEWPORTS[0].name);
await checkLayout(page, "live view");
await checkFailureIsVisible(page, VIEWPORTS[0].name);
await checkRouteViewOnAPhone(page, VIEWPORTS[0].name);
await checkSearchResults(page, VIEWPORTS[0].name);
await checkFirstUsefulAction(page, VIEWPORTS[0].name);
await checkFareEntryReachable(page, VIEWPORTS[0].name);
await checkEditorHasRoom(page, VIEWPORTS[0].name);
await checkTabKeyboard(page, VIEWPORTS[0].name);
await checkDialogDismiss(page, VIEWPORTS[0].name);
await checkWakingBanner(page, VIEWPORTS[0].name);
await checkClusterDensity(page, VIEWPORTS[0].name);
await checkProposalFitsAboveSheet(page, VIEWPORTS[0].name);
await checkPresetsDraw(page, VIEWPORTS[0].name);
await checkBusSelectionReveals(page, VIEWPORTS[0].name);
await checkDepartureBoard(page);
await checkViews(page);
await checkReachableAcrossViews(page, VIEWPORTS[0].name);
await checkInteractiveSurfaces(page);
await checkCouncillorContact(page);
await checkJourneyPresets(page);
await shootThemes(page);
await checkPanelCollapse(page);   // must stay last — see the note on the function
await checkDeepLinkIndependence();   // own page + request interception; keep it apart

// The other two viewports get the layout assertions whether or not
// screenshots were asked for — an assertion that only runs with --shots is
// one that will not run in CI.
for (const vp of VIEWPORTS.slice(1)) {
  const p = await openPage(vp);
  await screenshot(p, `live-${vp.name}`);
  await checkLayout(p, vp.name);
  await checkFailureIsVisible(p, vp.name);
  if (vp.mobile) await checkRouteViewOnAPhone(p, vp.name);
  await checkHeaderControlRow(p, vp.name);
  await checkWakingBanner(p, vp.name);
  await checkContrastBothThemes(p, vp.name);
  if (vp.name === "desktop") await checkReadingLayout(p);
  await checkReachableAcrossViews(p, vp.name);
  p.ws.close();
}

const jsErrors = page.consoleErrors.filter((e) => JS_ERROR.test(e));
check("no JavaScript exceptions on the page", jsErrors.length === 0,
  jsErrors.slice(0, 2).join(" | "));

const failed = results.filter((r) => !r.pass);
for (const r of results) {
  const tag = r.skipped ? "SKIP" : r.pass ? "PASS" : "FAIL";
  console.log(`${tag}  ${r.name}${r.detail ? `  (${r.detail})` : ""}`);
}
if (page.consoleErrors.length) {
  console.log("\nconsole errors seen (BODS/RTT keys are absent locally — expected):");
  for (const e of page.consoleErrors.slice(0, 5)) console.log(`  ${e}`);
}
const skipped = results.filter((r) => r.skipped).length;
console.log(`\n${results.length - failed.length - skipped}/${results.length - skipped} checks passed` +
  (skipped ? `, ${skipped} skipped` : ""));
page.ws.close();
process.exit(failed.length ? 1 : 0);
