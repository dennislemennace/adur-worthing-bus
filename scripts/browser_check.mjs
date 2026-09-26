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

import { readFile, writeFile, mkdir } from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";

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
    // Screen-reader-only text (.visually-hidden) is clipped to nothing and
    // never drawn, so there is no visual contrast to measure. It sat in the
    // header as a 1px box and was being scored against the page background.
    const box = el.getBoundingClientRect();
    if ((box.width <= 1 && box.height <= 1) || cs.clipPath === "inset(50%)") continue;
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
  for (const mode of ["improvements", "network", "updates", "journeytimes"]) {
    await page.evaluate(`setViewMode('${mode}')`);
    await sleep(900);
    widths[mode] = await page.evaluate(
      `Math.round(document.getElementById("departure-panel").getBoundingClientRect().width)`);
  }
  check("prose views get a wider column than the map views",
    widths.network > widths.improvements + 100
    && widths.updates > widths.improvements + 100,
    JSON.stringify(widths));
  // The journey-time view for a different reason: its chart draws a 640-unit
  // viewBox, so at 360px the 11px axis labels render at about 6px. This ran
  // green for a release while reporting `journeytimes: 360` in its own detail
  // string, which is the difference between measuring something and checking
  // it.
  check("the journey-time chart gets a column its labels survive",
    widths.journeytimes > widths.improvements + 100,
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

/**
 * About, privacy and terms are separate pages with their own inline styles, so
 * none of the checks on the map page ever looked at them. They carry the
 * legal text and the switch that stops visit counting, and a switch that does
 * not save, or text too faint to read, is a promise the page is not keeping.
 */
async function checkStaticPages() {
  const base = new URL(SITE);
  const origin = `${base.origin}${base.pathname.replace(/[^/]*$/, "")}`;
  for (const name of ["about.html", "privacy.html", "terms.html"]) {
    for (const vp of [{ label: "mobile", width: 390, height: 844, mobile: true },
                      { label: "desktop", width: 1440, height: 900, mobile: false }]) {
      const page = await connect("about:blank");
      try {
        await page.send("Page.enable");
        await page.send("Runtime.enable");
        await page.send("Network.enable");
        await page.send("Network.setCacheDisabled", { cacheDisabled: true });
        await page.send("Emulation.setDeviceMetricsOverride",
          { width: vp.width, height: vp.height, deviceScaleFactor: 2, mobile: vp.mobile });
        for (const scheme of ["light", "dark"]) {
          await page.send("Emulation.setEmulatedMedia",
            { features: [{ name: "prefers-color-scheme", value: scheme }] });
          await page.send("Page.navigate", { url: origin + name });
          await sleep(1500);
          const where = `${name}, ${vp.label}, ${scheme}`;
          const over = await page.evaluate(OVERFLOW_SCAN);
          check(`nothing overflows the viewport — ${where}`, over.length === 0, over.join(", "));
          const small = await page.evaluate(TARGET_SCAN);
          check(`touch targets are at least 44px — ${where}`, small.length === 0, small.join(", "));
          const bad = await page.evaluate(CONTRAST_SCAN);
          check(`text meets WCAG AA contrast — ${where}`, bad.length === 0, bad.join(", "));
        }
        if (name === "privacy.html" && vp.label === "mobile") {
          // The switch has to save, survive a reload, and say what it did.
          const before = JSON.parse(await page.evaluate(`JSON.stringify({
            stored: localStorage.getItem("analytics-opt-out"),
            button: document.getElementById("counting-switch").textContent })`));
          await page.evaluate(`(() => { document.getElementById("counting-switch").click(); return ""; })()`);
          await page.send("Page.navigate", { url: origin + name });
          await sleep(1200);
          const after = JSON.parse(await page.evaluate(`JSON.stringify({
            stored: localStorage.getItem("analytics-opt-out"),
            state: document.getElementById("counting-state").textContent,
            button: document.getElementById("counting-switch").textContent })`));
          check("the privacy page's switch stops visit counting and remembers it",
            before.stored === null && after.stored === "1"
              && /not counted/.test(after.state) && /again/.test(after.button),
            JSON.stringify({ before, after }));
          // Put it back, so the rest of the run sees a default browser.
          await page.evaluate(`(() => { localStorage.removeItem("analytics-opt-out"); return ""; })()`);
        }
        const errors = page.consoleErrors.filter((e) => JS_ERROR.test(e));
        check(`no JavaScript exceptions — ${name}, ${vp.label}`, errors.length === 0,
          errors.slice(0, 2).join(" | "));
      } finally {
        page.ws.close();
      }
    }
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

/**
 * One lead objective, and the rest still reachable.
 *
 * The four featured objectives were four identical cards, which said all four
 * mattered the same amount and made the section read as a template. The lead
 * now carries a standfirst and larger type. The risk in that change is the
 * other three quietly losing their way in or their accessible name, so both
 * are asserted rather than the appearance alone.
 */
async function checkObjectiveLead(page, where) {
  await page.evaluate("setViewMode('network')");
  await waitFor(page, "document.querySelectorAll('[data-objective-id]').length > 0", 20000);
  await sleep(400);
  const r = JSON.parse(await page.evaluate(`
    (() => {
      const featured = document.querySelector(".objective-featured");
      if (!featured) return JSON.stringify({ skip: "no featured section" });
      const lead = featured.querySelector(".objective-lead");
      const also = [...featured.querySelectorAll(".objective-also .proposal-card-wrap")];
      const size = el => el ? parseFloat(getComputedStyle(el).fontSize) : 0;
      const shown = el => !!el && el.getClientRects().length > 0
                       && getComputedStyle(el).display !== "none"
                       && el.textContent.trim().length > 0;
      const leadTitle = lead && lead.querySelector(".proposal-card-name");
      const alsoTitle = also[0] && also[0].querySelector(".proposal-card-name");
      return JSON.stringify({
        hasLead: !!lead,
        leadSummary: shown(lead && lead.querySelector(".proposal-card-summary")),
        leadPx: size(leadTitle), alsoPx: size(alsoTitle),
        leadFont: leadTitle ? getComputedStyle(leadTitle).fontFamily : "",
        alsoFont: alsoTitle ? getComputedStyle(alsoTitle).fontFamily : "",
        alsoCount: also.length,
        alsoSummaries: also.map(w => shown(w.querySelector(".proposal-card-summary"))),
        alsoClickable: also.every(w => {
          const b = w.querySelector("[data-objective-id]");
          return b && b.tagName === "BUTTON" && !b.disabled;
        }),
      });
    })()`));
  if (r.skip) { check(`the objectives lead renders — ${where}`, true, r.skip); }
  else {
    // Larger, but in the same face as every other card: the lead is primary
    // by size, not by switching to the editorial serif, which read as a
    // different kind of thing rather than the most important of the same kind.
    check(`one objective leads the section — ${where}`,
      r.hasLead && r.leadPx > r.alsoPx && r.leadFont === r.alsoFont && r.leadSummary,
      JSON.stringify(r));
    check(`every featured objective keeps its short summary — ${where}`,
      r.alsoCount > 0 && r.alsoSummaries.every(Boolean) && r.alsoClickable,
      JSON.stringify(r));
  }
  await page.evaluate('(() => { setViewMode("live"); return ""; })()');
  await sleep(500);
}

/** Apply accessibility settings the way the dialog does, without the dialog. */
async function setA11y(page, settings) {
  await page.evaluate(`(() => {
    const next = normaliseA11y(${JSON.stringify(settings)});
    applyA11ySettings(next); saveA11ySettings(next); return "";
  })()`);
  await sleep(400);
}

/**
 * The accessibility menu: every way in, and what each setting does.
 *
 * The hold on the theme button is the risky one twice over. A hold that does
 * not open anything is a shortcut that silently isn't there, and a hold that
 * opens the menu *and* flips the theme is worse than no shortcut at all.
 */
async function checkA11yMenu(page, where, { desktop }) {
  const isOpen = () => page.evaluate(`document.getElementById("a11y-dialog").open`);
  const close = () => page.evaluate(`(() => { const d = document.getElementById("a11y-dialog"); if (d.open) d.close(); return ""; })()`);

  if (desktop) {
    const btn = JSON.parse(await page.evaluate(`(() => {
      const b = document.getElementById("a11y-btn"); const r = b.getBoundingClientRect();
      return JSON.stringify({ shown: r.width > 0 && getComputedStyle(b).display !== "none", w: Math.round(r.width), h: Math.round(r.height) });
    })()`));
    check(`the accessibility button is in the header — ${where}`, btn.shown && btn.h >= 40, JSON.stringify(btn));
    await page.evaluate(`document.getElementById("a11y-btn").click()`);
    await sleep(300);
    check(`the header button opens the accessibility settings — ${where}`, await isOpen());
    await close();
  }

  // The link a reader can see at this width. Phones get the compact footer, and
  // clicking the full footer's link by id would pass even while it is hidden.
  const visibleLink = JSON.parse(await page.evaluate(`(() => {
    const links = [...document.querySelectorAll("[data-open-a11y]")]
      .filter(a => a.getClientRects().length && getComputedStyle(a).visibility !== "hidden");
    if (links[0]) links[0].setAttribute("data-probe-visible", "");
    return JSON.stringify({ visible: links.length,
      where: links[0] ? links[0].closest("p").className : null });
  })()`));
  check(`an accessibility link is visible in the footer — ${where}`,
    visibleLink.visible >= 1, JSON.stringify(visibleLink));
  if (visibleLink.visible) {
    await page.evaluate(`(() => { document.querySelector("[data-probe-visible]").click(); return ""; })()`);
    await sleep(300);
    check(`the footer link opens the accessibility settings — ${where}`, await isOpen(), JSON.stringify(visibleLink));
    await page.evaluate(`(() => { document.querySelector("[data-probe-visible]").removeAttribute("data-probe-visible"); return ""; })()`);
  }
  await close();

  // A hold: pointerdown, wait past the threshold, release, and the click the
  // browser sends after it.
  const hold = async (ms) => {
    await page.evaluate(`(() => {
      const b = document.getElementById("dark-mode-btn"); const r = b.getBoundingClientRect();
      const o = { bubbles: true, pointerType: "touch", isPrimary: true, clientX: r.left + 5, clientY: r.top + 5, button: 0 };
      window.__holdBtn = b; window.__holdOpts = o;
      b.dispatchEvent(new PointerEvent("pointerdown", o)); return "";
    })()`);
    await sleep(ms);
    await page.evaluate(`(() => {
      const b = window.__holdBtn, o = window.__holdOpts;
      b.dispatchEvent(new PointerEvent("pointerup", o));
      b.dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 })); return "";
    })()`);
    await sleep(300);
  };

  const themeBefore = await page.evaluate("state.darkMode");
  await hold(650);
  const heldOpen = await isOpen();
  const themeAfterHold = await page.evaluate("state.darkMode");
  check(`holding the theme button opens the accessibility settings — ${where}`, heldOpen);
  check(`holding the theme button does not also switch the theme — ${where}`,
    themeAfterHold === themeBefore, JSON.stringify({ themeBefore, themeAfterHold }));
  await close();

  await hold(60);
  const tapOpen = await isOpen();
  const themeAfterTap = await page.evaluate("state.darkMode");
  check(`a quick tap on the theme button still just switches the theme — ${where}`,
    !tapOpen && themeAfterTap !== themeBefore, JSON.stringify({ tapOpen, themeBefore, themeAfterTap }));
  if (themeAfterTap !== themeBefore) await page.evaluate("toggleDarkMode()");
  await close();

  // Reduce motion. The harness freezes transitions on every page it opens, so
  // a computed-style check would pass by itself; assert on what the setting
  // controls instead: the rule is in the stylesheet, the app reads it, and
  // Leaflet stops animating zooms.
  await setA11y(page, { reduceMotion: true });
  const motion = JSON.parse(await page.evaluate(`(() => {
    let rule = false;
    for (const sheet of document.styleSheets) {
      try { for (const r of sheet.cssRules) if ((r.selectorText || "").includes("html.a11y-reduce-motion")) rule = true; }
      catch (e) {}
    }
    return JSON.stringify({ cls: document.documentElement.classList.contains("a11y-reduce-motion"),
      rule, reduced: motionReduced(), zoomAnimated: state.map._zoomAnimated });
  })()`));
  check(`reduce motion stops the map animating and is applied to the page — ${where}`,
    motion.cls && motion.rule && motion.reduced && motion.zoomAnimated === false, JSON.stringify(motion));
  await setA11y(page, {});
}

/** Settings survive a reload, applied before the app starts. */
async function checkA11yPersists(page, where) {
  await setA11y(page, { textScale: 1.4, cvd: true });
  await page.send("Page.reload", { ignoreCache: true });
  await sleep(3500);
  const r = JSON.parse(await page.evaluate(`JSON.stringify({
    size: document.documentElement.style.fontSize,
    cvd: document.documentElement.classList.contains("a11y-cvd"),
    stored: localStorage.getItem("a11y") })`));
  check(`accessibility settings are still applied after a reload — ${where}`,
    r.size === "140%" && r.cvd, JSON.stringify(r));
  await freezeMotion(page);
  await setA11y(page, {});
}

/** Everything still fits and can be reached at the largest text size. */
async function checkLargestText(page, where) {
  await setA11y(page, { textScale: 1.4 });
  await checkLayout(page, `${where} at 140% text`);
  await checkHeaderControlRow(page, `${where} at 140% text`);
  await setA11y(page, {});
}

/** The colour-blind-safe palette keeps every text colour readable. */
async function checkCvdContrast(page, where) {
  await setA11y(page, { cvd: true });
  await checkContrastBothThemes(page, `${where}, colour-blind-safe`);
  await setA11y(page, {});
}

/**
 * "Show the live buses here" from the boundary evidence.
 *
 * The invitation is only worth making if it lands somewhere the difference
 * can be seen: Live view, buses on, and both sides of the line in frame.
 */
async function checkBoundaryLiveButton(page, where) {
  await page.evaluate(`(() => { setViewMode("improvements"); return ""; })()`);
  await waitFor(page, "Object.keys(state.councilBoundaryLayers || {}).length > 0", 15000);
  await page.evaluate(`(() => { openBoundaryEvidence(); return ""; })()`);
  const ready = await waitFor(page, "!!document.querySelector('[data-show-live-boundary]')", 15000);
  if (!ready) { check(`the boundary evidence offers the live buses — ${where}`, false, "no button after 15s"); return; }
  await page.evaluate(`(() => { state.busesVisible = false; document.querySelector("[data-show-live-boundary]").click(); return ""; })()`);
  await sleep(900);
  const r = JSON.parse(await page.evaluate(`(() => {
    const line = L.latLngBounds([]);
    for (const l of Object.values(state.councilBoundaryLayers)) line.extend(l.getBounds());
    const view = state.map.getBounds();
    return JSON.stringify({ view: state.viewMode, buses: state.busesVisible,
      dialogOpen: document.getElementById("evidence-dialog").open,
      containsLine: view.contains(line), zoom: state.map.getZoom() });
  })()`));
  check(`the boundary evidence shows the live buses either side — ${where}`,
    r.view === "live" && r.buses && !r.dialogOpen && r.containsLine, JSON.stringify(r));
}

/** Limited services sit after frequent ones and start switched off. */
async function checkChipPriority(page, where) {
  // From a known state. An earlier check left the page in night mode, where
  // every service counts as frequent, and this passed on nine chips with no
  // limited ones in the list at all.
  await page.evaluate(`(() => {
    setViewMode("improvements"); setServiceMode("day");
    state.visibleCategories = new Set(["all"]);
    state.visibleOperators = new Set(Object.values(state.routeOperatorByService));
    renderRouteFilterChips(); return "";
  })()`);
  await waitFor(page, "document.querySelectorAll('.route-chip').length > 0", 45000);
  const r = JSON.parse(await page.evaluate(`(() => {
    const chips = [...document.querySelectorAll(".route-chip")];
    const firstLimited = chips.findIndex(c => c.classList.contains("route-chip--limited"));
    const frequentAfter = firstLimited < 0 ? 0
      : chips.slice(firstLimited).filter(c => !c.classList.contains("route-chip--limited")).length;
    const limitedOn = chips.filter(c => c.classList.contains("route-chip--limited")
                                     && c.getAttribute("aria-pressed") === "true").length;
    return JSON.stringify({ total: chips.length, limited: chips.length - (firstLimited < 0 ? chips.length : firstLimited) - frequentAfter,
      firstLimited, frequentAfter, limitedOn, showLimited: state.showLimitedServices });
  })()`));
  check(`limited services come after frequent ones and start off — ${where}`,
    r.limited > 0 && r.frequentAfter === 0 && r.limitedOn === 0, JSON.stringify(r));
  await page.evaluate(`(() => { setViewMode("live"); return ""; })()`);
  await sleep(400);
}

/**
 * The last bus home from central Brighton, in the stop panel.
 *
 * Needs an API new enough to send it. Against an older deployment the check
 * says so and skips, rather than failing on something the frontend cannot fix.
 */
async function checkLastBusHome(page, where) {
  await page.evaluate(`(() => { setViewMode("live"); openDepartures("4400AD0204", "High Street"); return ""; })()`);
  await waitFor(page, "!!document.querySelector('#stop-span .stop-span-disclosure, #stop-span .stop-lastbus, #stop-span .stop-span-none')", 45000);
  const r = JSON.parse(await page.evaluate(`(() => {
    const box = document.querySelector("#stop-span .stop-lastbus");
    return JSON.stringify({ rendered: !!box,
      headers: box ? [...box.querySelectorAll("thead th")].map(t => t.textContent.trim()) : [],
      text: box ? box.textContent.replace(/\\s+/g, " ").trim().slice(0, 140) : "" });
  })()`));
  if (!r.rendered) {
    check(`the stop panel shows the last bus home from Brighton — ${where}`, true,
      "skipped: this API does not send last_from_brighton yet");
    await page.evaluate(`(() => { closePanel(); return ""; })()`);
    await sleep(400);
    return;
  }
  check(`the stop panel shows the last bus home from Brighton — ${where}`,
    r.headers.includes("Day bus") && r.headers.includes("Night bus"), JSON.stringify(r));
  await page.evaluate(`(() => { closePanel(); return ""; })()`);
  await sleep(400);
}

/** A gap-monitor answer to draw with, so the check does not depend on the time
 *  of day or on a BODS key the local API does not have. */
const GAP_STOPS = [
  { atco: "4400AD0330", name: "Shoreham Port",
    next: [{ service: "700", due: "12:34", minutes: 4, source: "live" },
           { service: "700", due: "12:44", minutes: 14, source: "scheduled" }] },
  { atco: "4400AD0203", name: "Shoreham High Street",
    next: [{ service: "2", due: "12:30", minutes: 0, source: "live" }] },
  { atco: "4400AD0063", name: "Beach Green Hotel, Lancing",
    next: [{ service: "700", due: "13:03", minutes: 33, source: "live" }] },
];
const gapDirection = (id, over = {}) => ({
  id, label: `A259 Coast Rd towards ${id === "brighton" ? "Brighton" : "Worthing"}`,
  towards: `towards ${id === "brighton" ? "Brighton" : "Worthing"}`,
  status: "normal", reason: null, alert: null, stops: GAP_STOPS, ...over });
// One direction alerting and one not, so the combined row has to show both.
const GAP_ALERT = {
  active: true, as_of: "2026-09-16T12:30:00+01:00",
  directions: [
    gapDirection("worthing"),
    gapDirection("brighton", { status: "alert", alert: {
      atco: "4400AD0204", name: "Beach Green Hotel, Lancing", minutes: 32,
      from: "12:31", to: "13:03", from_now: false, to_horizon: false,
      timetable_minutes: 10, not_reporting: 2, alert: true } }),
  ],
};
const GAP_NORMAL = { ...GAP_ALERT, directions: [gapDirection("worthing"), gapDirection("brighton")] };

/**
 * The gap monitor was asked for on one condition: that it not take map space,
 * least of all on a phone. So measure the map and the sheet with and without
 * it, rather than trusting where the markup happens to sit.
 */
/**
 * The bus panel may name a journey only when the feed named it.
 *
 * GTFS-RT states which scheduled journey a bus is running; our own matching
 * guesses it, and is right about four times in five. "The 14:22" is a claim a
 * reader will act on — they will let one bus go to catch another — so it has
 * to come from the operator, not from us.
 */
async function checkBusJourney(page, where) {
  const r = JSON.parse(await page.evaluate(`(() => {
    setViewMode("live");
    const declared = {
      vehicle_ref: "CHECK-DECLARED", service_ref: "700", operator_ref: "SCSO",
      latitude: 50.832, longitude: -0.27, destination: "Worthing",
      trip_source: "feed", journey_start: "14:22",
      nearest_stop_name: "High Street", lateness_secs: 240,
    };
    const guessed = {
      vehicle_ref: "CHECK-GUESSED", service_ref: "700", operator_ref: "SCSO",
      latitude: 50.832, longitude: -0.27, destination: "Worthing",
      trip_id: "VJ_guessed", journey_start: "14:32",
    };
    // renderBusTab reads state.selectedVehicle, not the ref.
    state.busDetails = null;
    state.selectedVehicleRef = declared.vehicle_ref;
    state.selectedVehicle = declared;
    renderBusTab();
    const withFeed = document.getElementById("bus-info-container").textContent;
    state.busDetails = null;
    state.selectedVehicleRef = guessed.vehicle_ref;
    state.selectedVehicle = guessed;
    renderBusTab();
    const withGuess = document.getElementById("bus-info-container").textContent;
    state.selectedVehicle = null;
    state.selectedVehicleRef = null;
    closePanel();
    return JSON.stringify({ withFeed, withGuess });
  })()`));

  check(`a declared journey is named on the bus panel — ${where}`,
    /The 14:22/.test(r.withFeed) && /4 min late/.test(r.withFeed),
    r.withFeed.slice(0, 160));
  check(`a guessed journey is not named as though it were stated — ${where}`,
    !/The 14:32/.test(r.withGuess),
    r.withGuess.slice(0, 160));
}

/**
 * The stop board a visitor reads: live or timetable on every row, route
 * buttons that actually filter, and a key that survives the phone layout
 * hiding the column headers. Fixture rows, so the result does not depend on
 * which buses happen to be running.
 */
async function checkStopBoardPolish(page, where) {
  const r = JSON.parse(await page.evaluate(`(async () => {
    setViewMode("live");
    const atco = Object.keys(state.stopData || {})[0];
    if (!atco) return JSON.stringify({ skip: "no stops loaded" });
    await openDepartures(atco, (state.stopData[atco] || {}).name || atco);
    const soon = (m) => new Date(Date.now() + m * 60000).toISOString();
    renderDepartures({
      stop_name: "Fixture Stop", live: true,
      disruptions: [{ id: "D1", summary: "700 diverted via Church Road while Portland Road is closed",
        description: "Eastbound buses only.", advice: "Use the stop on Church Road.",
        publisher: "WestSussexCC", starts: null, ends: soon(60 * 24 * 5), link: "https://example.org/",
        lines: [{ operator: "SCSO", line: "700" }], stops: [], operators: [] }],
      departures: [
        { service: "700", operator: "SCSO", destination: "Brighton Pier", disruption_ids: ["D1"],
          aimed_departure: soon(3), expected_departure: soon(6), status: "Late", delay_seconds: 180 },
        { service: "9", operator: "SCSO", destination: "Worthing",
          aimed_departure: soon(7), expected_departure: null, status: "Scheduled", delay_seconds: null },
        { service: "10", operator: "SCSO", destination: "Lancing",
          aimed_departure: soon(11), expected_departure: soon(11), status: "On time", delay_seconds: 20 },
        { service: "700", operator: "SCSO", destination: "Littlehampton",
          aimed_departure: soon(15), expected_departure: null, status: "Scheduled", delay_seconds: null },
      ],
    });
    const vis = ${VISIBLE_FN};
    const rows = [...document.querySelectorAll("tr.departure-row")];
    const liveRows = rows.filter(tr => tr.querySelector(".live-dot"));
    const lateLabel = rows[0].textContent;
    const buttons = [...document.querySelectorAll("#board-filter button")];
    const key = document.querySelector(".board-key");
    const notice = document.querySelector("#board-disruptions details.disruption");
    if (notice) notice.open = true;
    const noticeShown = !!notice && vis(notice) && vis(notice.querySelector(".disruption-body"));
    const rowTag = !!rows[0].querySelector(".row-disruption");
    const help = document.getElementById("board-help");
    const btn700 = buttons.find(b => b.dataset.service === "700");
    btn700 && btn700.click();
    const shownAfter = rows.filter(tr => !tr.hidden).map(tr => tr.dataset.service);
    const count = document.getElementById("departures-count").textContent;
    return JSON.stringify({
      liveRows: liveRows.length, lateLabel,
      buttons: buttons.map(b => b.textContent.trim()),
      keyShown: !!key && vis(key), helpShown: !!help && vis(help),
      pressed: btn700 && btn700.getAttribute("aria-pressed"),
      shownAfter, count, noticeShown, rowTag,
    });
  })()`));
  if (r.skip) { skip(`stop board shows live and timetable apart — ${where}`, r.skip); return; }
  check(`stop board marks exactly the live rows — ${where}`, r.liveRows === 2, `${r.liveRows} dots`);
  check(`a late bus says by how much — ${where}`, /3 min late/i.test(r.lateLabel), r.lateLabel.replace(/\s+/g, " ").slice(0, 80));
  check(`a published disruption shows, and its row points at it — ${where}`,
    r.noticeShown && r.rowTag, `notice ${r.noticeShown}, row tag ${r.rowTag}`);
  check(`the board key and help are on screen — ${where}`, r.keyShown && r.helpShown,
    `key ${r.keyShown}, help ${r.helpShown}`);
  check(`route buttons filter the board — ${where}`,
    r.buttons.join(",") === "All,9,10,700" && r.pressed === "true"
      && r.shownAfter.length === 2 && r.shownAfter.every(s => s === "700")
      && /on the 700/.test(r.count),
    JSON.stringify({ buttons: r.buttons, shown: r.shownAfter, count: r.count }));
  await checkLayout(page, `stop board — ${where}`);
  await checkContrastBothThemes(page, `stop board — ${where}`);
  await page.evaluate(`(() => { const b = document.querySelector('#board-filter button[data-service=""]'); b && b.click(); })()`);
}

/**
 * The Bus tab's stops ahead, and the refresh that used to wipe the tab.
 *
 * Every 20 seconds the map's poll re-renders the selected bus. That rebuilt
 * the whole tab, so an expanded list folded itself up, focus fell back to the
 * page, and a half-typed report vanished. The fixture here expands the list,
 * focuses the toggle and types into the report, then triggers the refresh.
 */
async function checkUpcomingStops(page, where) {
  const r = JSON.parse(await page.evaluate(`(() => {
    setViewMode("live");
    const at = (m) => new Date(Date.now() + m * 60000).toISOString();
    const bus = {
      vehicle_ref: "CHECK-UPCOMING", service_ref: "700", operator_ref: "SCSO",
      latitude: 50.832, longitude: -0.27, destination: "Brighton",
      trip_source: "feed", journey_start: "14:22", lateness_secs: 240,
      report_age_secs: 40, recorded_at: at(-1),
    };
    const stops = Array.from({ length: 22 }, (_, i) => ({
      stop_id: "S" + i, stop_name: i === 5 ? "Shoreham-by-Sea Old Shoreham Road Holmbush Roundabout" : "Stop " + i,
      seq: i, scheduled: at(i * 2), expected: i === 0 ? null : at(i * 2 + 4),
      lateness_secs: i === 0 ? null : 240, timing_point: i % 5 === 0,
      passed: i === 0, is_next: i === 1, is_terminus: i === 21,
    }));
    state.selectedVehicleRef = bus.vehicle_ref;
    state.selectedVehicle = bus;
    state.busDetailsLoading = false;
    state.busDetails = { source: "trip", upcoming_stops: stops,
                         vehicle: { trip_source: "feed", recorded_at: bus.recorded_at, report_age_secs: 40 } };
    state.busTabShellRef = null;
    state.upcomingExpanded = {};
    setActiveTab("bus");
    renderBusTab();
    const folded = document.querySelectorAll("#bus-upcoming .upcoming-stop").length;
    const toggle = document.querySelector("#bus-upcoming .upcoming-toggle");
    toggle.click();
    const expanded = document.querySelectorAll("#bus-upcoming .upcoming-stop").length;
    document.querySelector("#bus-upcoming .upcoming-toggle").focus();
    const report = document.getElementById("rb-details");
    if (report) report.value = "half-typed";
    // What the 20-second poll does to the selected bus.
    state.selectedVehicle = { ...bus, recorded_at: at(0) };
    renderBusTab();
    const after = document.querySelectorAll("#bus-upcoming .upcoming-stop").length;
    const focusKept = document.activeElement && document.activeElement.dataset.focusKey === "toggle";
    const reportKept = (document.getElementById("rb-details") || {}).value === "half-typed";
    const text = document.getElementById("bus-upcoming").textContent.replace(/\\s+/g, " ");
    const said = {
      passed: /Passed/.test(text), next: /Next stop/.test(text), late: /4 min late/.test(text),
      advice: /be at your stop by the timetabled time/.test(text),
    };
    return JSON.stringify({ folded, expanded, after, focusKept, reportKept, said });
  })()`));
  check(`upcoming stops fold, then show every stop — ${where}`,
    r.folded === 8 && r.expanded === 22, `folded ${r.folded}, expanded ${r.expanded}`);
  check(`stops ahead say late or on time, with the method — ${where}`,
    Object.values(r.said).every(Boolean), JSON.stringify(r.said));
  check(`a refresh keeps the list open, focus, and a half-typed report — ${where}`,
    r.after === 22 && r.focusKept && r.reportKept,
    JSON.stringify({ after: r.after, focusKept: r.focusKept, reportKept: r.reportKept }));
  await checkLayout(page, `bus tab stops — ${where}`);
  await checkReachable(page, `bus tab stops — ${where}`);
  await checkContrastBothThemes(page, `bus tab stops — ${where}`);
  await page.evaluate(`(() => { state.selectedVehicle = null; state.selectedVehicleRef = null; closePanel(); })()`);
}

async function checkGapMonitor(page, where) {
  await page.evaluate(`(() => { setViewMode("live"); closePanel(); return ""; })()`);
  await sleep(400);

  // Quiet hours are a promise about the real page, so check the real page when
  // the run happens to fall inside them.
  const quiet = await page.evaluate(`String(isGapQuietHours())`);
  if (quiet === "true") {
    await page.evaluate(`(() => { fetchGapMonitor(); return ""; })()`);
    await sleep(200);
    const hidden = await page.evaluate(`String(document.getElementById("gap-monitor").hidden)`);
    check(`the gap monitor is hidden between 23:30 and 04:30 — ${where}`, hidden === "true", `hidden=${hidden}`);
  }

  // Entering Live view starts the real monitor, and its request can land in
  // the middle of this check and replace the answer being measured. Stop it,
  // and let anything already in the air come back first.
  await page.evaluate(`(() => { stopGapMonitor(); return ""; })()`);
  await waitFor(page, "!state.gapMonitorInFlight", 60000);

  const r = JSON.parse(await page.evaluate(`(() => {
    stopGapMonitor();
    if (isSheetLayout()) setSheetDetent(defaultDetentForViewport());
    const map = document.getElementById("map").getBoundingClientRect();
    const sheetBefore = sheetOverlapPx();
    renderGapMonitor(${JSON.stringify(GAP_ALERT)});
    const host = document.getElementById("gap-monitor");
    const heights = [...host.querySelectorAll("summary")].map(x => Math.round(x.getBoundingClientRect().height));
    const panel = document.getElementById("departure-panel").getBoundingClientRect();
    const mapAfter = document.getElementById("map").getBoundingClientRect();
    const b = host.getBoundingClientRect();
    return JSON.stringify({
      shown: !host.hidden && b.height > 0,
      inPanel: b.left >= panel.left - 1 && b.right <= panel.right + 1 && b.top >= panel.top - 1,
      mapSame: Math.round(map.width) === Math.round(mapAfter.width)
            && Math.round(map.height) === Math.round(mapAfter.height),
      sheetBefore, detentBefore: state.sheetDetent,
      rows: heights.length, heights,
      sheet: isSheetLayout(), vh: window.innerHeight,
    });
  })()`));
  // The sheet animates between heights, so a measurement taken in the same
  // tick as the render cannot see it grow. Look again once it has settled.
  await sleep(600);
  Object.assign(r, JSON.parse(await page.evaluate(
    `JSON.stringify({ sheetAfter: sheetOverlapPx(), detentAfter: state.sheetDetent })`)));
  check(`the gap monitor sits in the panel, not over the map — ${where}`,
    r.shown && r.inPanel && r.mapSame, JSON.stringify(r));
  check(`showing the gap monitor does not grow the sheet — ${where}`,
    r.sheetBefore === r.sheetAfter && r.detentBefore === r.detentAfter, JSON.stringify(r));
  // Both directions share one row: one line, two at most when it wraps on a phone.
  check(`the gap monitor is one short row for the corridor — ${where}`,
    r.rows === 1 && r.heights.every(h => h >= 44 && h <= 72), JSON.stringify(r));

  // At the resting sheet height the monitor is below the fold on a phone, so
  // an alert is only seen through the status-pill button. Press it and look.
  const btn = JSON.parse(await page.evaluate(`(() => {
    const b = document.getElementById("gap-alert-btn");
    const r = b.getBoundingClientRect();
    const out = { shown: !b.hidden && r.width >= 44 && r.height >= 44,
                  inView: r.left >= 0 && r.right <= innerWidth && r.top >= 0 && r.bottom <= innerHeight };
    b.click();
    return JSON.stringify(out);
  })()`));
  await sleep(800);
  const seen = JSON.parse(await page.evaluate(`(() => {
    const d = document.querySelector("#gap-monitor details");
    const s = d.querySelector("summary").getBoundingClientRect();
    const panel = document.getElementById("departure-panel").getBoundingClientRect();
    return JSON.stringify({ open: d.open, lines: d.querySelectorAll("li").length,
      focused: document.activeElement === d.querySelector("summary"),
      top: Math.round(s.top), bottom: Math.round(s.bottom), panelTop: Math.round(panel.top),
      vh: innerHeight, detent: state.sheetDetent });
  })()`));
  check(`the alert button brings the gap monitor into view — ${where}`,
    btn.shown && btn.inView && seen.open && seen.lines === 6 && seen.focused
      && seen.top >= seen.panelTop && seen.bottom <= seen.vh,
    JSON.stringify({ ...btn, ...seen }));

  await checkLayout(page, `gap monitor alert, open — ${where}`);
  await checkContrastBothThemes(page, `gap monitor alert, open — ${where}`);
  const normalBtn = await page.evaluate(`(() => {
    renderGapMonitor(${JSON.stringify(GAP_NORMAL)});
    return String(document.getElementById("gap-alert-btn").hidden); })()`);
  check(`in normal service nothing is added over the map — ${where}`, normalBtn === "true",
    `alert button hidden=${normalBtn}`);
  await checkContrastBothThemes(page, `gap monitor normal — ${where}`);

  await page.evaluate(`(() => {
    renderGapMonitor(null);
    if (isSheetLayout()) setSheetDetent(defaultDetentForViewport());
    startGapMonitor();
    return ""; })()`);
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
  // Nothing on top of the line. On a phone the sheet at its half detent covers
  // the line's midpoint, and in Live view a real bus is often parked on it
  // where the A259 crosses the boundary; either takes the mouse, correctly,
  // and the hover below then reported a line that "does not respond". Whether
  // it passed depended on where the buses were, which is why it came and went
  // against the live API and always passed against a local one with none.
  // Buses are put back after the hover.
  await page.evaluate(`(() => {
    setViewMode('live'); closePanel(); setSheetDetent('peek');
    window.__busesWereVisible = state.busesVisible; setBusesVisible(false);
    return "";
  })()`);
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
    await page.evaluate(`(() => { setBusesVisible(window.__busesWereVisible !== false); return ""; })()`);

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
    ["tickets", "Tickets & fares"], ["network", "Better buses"],
    ["updates", "News & notes"],
    ["journeytimes", "How long it really takes"],
  ]) {
    await page.evaluate(`setViewMode('${mode}')`);
    await sleep(1500);
    check(`view "${mode}" activates`,
      (await page.evaluate("document.body.dataset.view")) === mode, label);

    // Activating is not showing. A mode with no rule in the visibility
    // allow-list in style.css sets data-view, renders its content, and stays
    // display:none — the tab works and the panel is blank. Every other check
    // passes in that state, because nothing overflows a box of zero height.
    const shown = JSON.parse(await page.evaluate(`(() => {
      const el = document.querySelector('.panel-mode[data-mode="${mode}"]');
      if (!el) return JSON.stringify({ missing: true });
      const box = el.getBoundingClientRect();
      return JSON.stringify({
        height: Math.round(box.height),
        display: getComputedStyle(el).display,
        text: (el.textContent || "").trim().length,
      });
    })()`));
    check(`view "${mode}" is actually visible`,
      !shown.missing && shown.display !== "none" && shown.height > 20 && shown.text > 0,
      JSON.stringify(shown));
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
  for (const mode of ["live", "improvements", "tickets", "network", "updates",
                      "journeytimes"]) {
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

  // The same shape for every journey. The answer used to be six mutually
  // exclusive layouts — provenance on two of them, the reform row on four, no
  // money at all on two — so a reader comparing two journeys was comparing two
  // formats. This sweeps every preset rather than sampling one, because the
  // layouts that differed were the uncommon ones.
  const ids = JSON.parse(await page.evaluate(`
    JSON.stringify([...document.querySelectorAll("#jc-presets [data-preset]")]
      .map(b => b.dataset.preset))`));
  const seen = [];
  for (const id of ids) {
    await page.evaluate(
      `(document.querySelector('[data-preset="${id}"]').click(), 1)`);
    await sleep(4500);
    seen.push(JSON.parse(await page.evaluate(`
      (() => {
        const host = document.getElementById("jc-result");
        const rows = [...host.querySelectorAll(".journey-row")];
        const box = document.getElementById("tab-content-tickets");
        const list = host.querySelector(".journey-rows");
        const r = list && list.getBoundingClientRect();
        const b = box.getBoundingClientRect();
        return JSON.stringify({
          id: "${id}",
          rows: rows.length,
          labels: rows.map(x => ((x.querySelector(".journey-row-label") || {}).textContent || "").trim().toLowerCase()),
          priced: rows.every(x => ((x.querySelector(".journey-row-price") || {}).textContent || "").trim().length > 0),
          basis: /one return trip/i.test(host.textContent),
          sourced: /checked/i.test(host.textContent),
          tail: (host.querySelector(".journey-rows") || host).textContent.replace(/\\s+/g, " ").trim().slice(0, 200),
          clipped: !!r && (r.right > b.right + 2 || r.left < b.left - 2),
        });
      })()`)));
  }

  const withRows = seen.filter(s => s.rows > 0);
  check("every preset answers in the same row format",
    withRows.length === seen.length,
    seen.map(s => `${s.id}:${s.rows}`).join(" "));
  check("every row in every preset carries a price",
    seen.every(s => s.priced),
    seen.filter(s => !s.priced).map(s => s.id).join(", "));
  check("every priced answer states the basis it is priced on",
    withRows.every(s => s.basis),
    "a fare with no basis cannot be compared with another fare — "
    + withRows.filter(s => !s.basis).map(s => s.id).join(", "));
  check("every priced answer says where its fares came from",
    withRows.every(s => s.sourced),
    "provenance used to render on two of six layouts — "
    + withRows.filter(s => !s.sourced).map(s => `${s.id} [${s.tail}]`).join(" | "));
  check("the rows do not overflow the ticket panel",
    seen.every(s => !s.clipped),
    seen.filter(s => s.clipped).map(s => s.id).join(", "));
  // Quickest is shown only when it is a different journey from the cheapest.
  // A direct single-operator bus has no trade-off to show, and printing one
  // route twice under two headings reads as padding.
  check("no answer repeats one route as both cheapest and quickest",
    seen.every(s => s.labels.filter(l => l === "quickest").length <= 1),
    "a quickest row appeared more than once");

  // A pair typed into the boxes, not chosen from the worked examples. The
  // presets are all journeys the campaign has something to say about; the
  // custom path is where a reader brings their own, and it is where the old
  // code was loosest — a stop pair with no bus between them was priced anyway,
  // on an assumption of two legs.
  for (const [from, to, label] of [
    ["4400AD0204", "149000007830", "a custom pair with a bus"],
    // Kingston Bay Road on Shoreham Beach is on the map and in the zones, and
    // no service calls at it at all. It has to be a stop the picker can
    // actually resolve: the first attempt used one outside the map's bounding
    // box, which failed to resolve and so passed this check without ever
    // reaching the costing it was written to test.
    ["4400AD0157", "149000006480", "a custom pair with no bus between them"],
  ]) {
    await page.evaluate(`
      (() => {
        const f = document.getElementById("jc-from");
        const t = document.getElementById("jc-to");
        f.value = "${from}"; t.value = "${to}";
        document.getElementById("jc-check").click();
        return "1";
      })()`);
    await sleep(6000);
    const custom = JSON.parse(await page.evaluate(`
      (() => {
        const host = document.getElementById("jc-result");
        const rows = [...host.querySelectorAll(".journey-row")];
        return JSON.stringify({
          rows: rows.length,
          priced: rows.every(x => ((x.querySelector(".journey-row-price") || {}).textContent || "").trim().length > 0),
          basis: /one return trip/i.test(host.textContent),
          money: /£/.test(host.textContent),
          says: host.textContent.replace(/\\s+/g, " ").trim().slice(0, 80),
        });
      })()`));
    // Either it answers in the same format as every preset, or it says there
    // is no journey — and in that case it must not print a price at all.
    check(`${label} answers in the standard format or not at all`,
      custom.rows > 0 ? (custom.priced && custom.basis) : !custom.money,
      `rows=${custom.rows} priced=${custom.priced} basis=${custom.basis} `
      + `money=${custom.money} :: ${custom.says}`);
  }
}

/**
 * The journey-time view, which is the one that publishes a number about a
 * named operator, so a chart that silently fails is worse here than anywhere.
 *
 * This exists because of the incident that shaped the harness: every check
 * passed against a panel that was `display: none`, and certified an invisible
 * view as working. So the first thing asserted is that dots were actually
 * drawn, before anything about what they mean.
 */
/** Deterministic review fixture: exercises rendered controls without a live publication. */
async function checkJourneyReview(page, viewport) {
  const result = await page.evaluate(`(async () => {
    const build = "a".repeat(64), file = "builds/" + build + "/700-SCSO.json";
    const day = "2026-09-21", origin = Date.parse(day + "T00:00:00+01:00") / 1000;
    const stops = [{atco:"4400AD0064",name:"Lancing",lat:50.823,lon:-.321},
                  {atco:"149000007830",name:"Brighton",lat:50.820,lon:-.136}];
    const journeys = [0,1,2].map(i => ({day, start:"07:00", trip_id:"probe-"+i,
      direction:"eastbound", headsign:"Brighton", route_pattern:"pattern", data_version:"timetable", method_version:4,
      match:i===2?"inferred":"declared", source_files:["input-hash"], quality_flags:[],
      calls:[[0,28800+i*1800,28740+i*1800,i===1?1:0,0,origin+28800+i*1800,origin+28740+i*1800,[origin+28800,origin+28830],[],i===2?"inferred":"declared"],
             [1,30000+i*1800,29340+i*1800,0,1,origin+30000+i*1800,origin+29340+i*1800,[origin+30000,origin+30030],[],i===2?"inferred":"declared"]]}));
    const doc = {build_id:build, service:"700", operator:"SCSO", days:[day], stops, journeys,
      method:"Review fixture", as_of:day, caveats:["Synthetic browser fixture"], source_methods:[]};
    const index = {build_id:build, days:[day], services:[{file,service:"700",operator:"SCSO",journeys:3}]};
    journeyTimesCache.set("index.json", index); journeyTimesCache.set(file, doc);
    // Detailed is behind its own switch; this fixture exercises it as preview would.
    CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC = true; jtEntry.view = "detailed"; jtEntry.section = "journey";
    jtEntry.mode="browse"; jtEntry.only=null; jtEntry.wanted=null;
    for (const key of ["service", "direction", "from", "to", "days"]) {
      const el=document.getElementById("journey-times-"+key); for(const attr of Object.keys(el.dataset)) delete el.dataset[attr];
    }
    setViewMode("journeytimes"); await renderJourneyTimes();
    const host=document.getElementById("journey-times-result");
    const axis=host.querySelector(".jt-axis"), dot=host.querySelector(".jt-dot");
    const font=axis ? parseFloat(getComputedStyle(axis).fontSize)*axis.getScreenCTM().a : 0;
    const initialDots=host.querySelectorAll(".jt-dot").length;
    dot?.dispatchEvent(new MouseEvent("click", {bubbles:true}));
    const detail=host.querySelector(".jt-point-detail")?.textContent || "";
    const link=[...host.querySelectorAll("a")].find(a=>a.textContent.includes("Link to this view"));
    document.getElementById("journey-times-identity").value="declared";
    await renderJourneyTimes();
    const declaredDots=host.querySelectorAll(".jt-dot").length;
    host.querySelector(".jt-expand")?.click();
    const expanded=host.querySelector(".jt-chart-area--expanded");
    const expandedRect=expanded?.getBoundingClientRect();
    const expands=!!expandedRect && expandedRect.left>=0 && expandedRect.right<=innerWidth && expandedRect.height<=innerHeight;
    host.querySelector(".jt-expand")?.click();
    host.querySelector(".jt-chart")?.scrollIntoView({block:"center"});
    const bodyWidth=document.documentElement.scrollWidth;
    // A shared URL must restore values after the controls have lost their state.
    const oldUrl=location.href;
    const share=journeyTimesShareUrl(index);
    journeyTimesCache.set("builds/"+build+"/index.json", index);
    history.replaceState(null,"",share);
    document.getElementById("journey-times-identity").value="all";
    document.getElementById("journey-times-mode").value="duration";
    jtEntry.sharedApplied=false; await renderJourneyTimes();
    const restored=document.getElementById("journey-times-identity").value==="declared"
      && document.getElementById("journey-times-mode").value==="delay"
      && document.getElementById("journey-times-service").value===file;
    history.replaceState(null,"",oldUrl);
    host.querySelector(".jt-chart")?.scrollIntoView({block:"center"});
    const detailedFlag = true;
    CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC = false; jtEntry.view = undefined;
    return {restored, font, initialDots, declaredDots, detail, expands, bodyWidth, width:innerWidth, detailedFlag,
      shared:link?.href, csv:!!host.querySelector(".jt-csv"), evidence:!!host.querySelector(".jt-evidence"),
      departure:host.querySelector(".jt-table tbody tr td:nth-child(2)")?.textContent};
  })()`);
  check(`journey review ${viewport}: mobile chart text is readable`, result.font >= 12, `${result.font.toFixed(1)}px`);
  check(`journey review ${viewport}: measured and declared filters agree with chart`, result.initialDots === 2 && result.declaredDots === 1, JSON.stringify(result));
  check(`journey review ${viewport}: tapping a point opens its actual departure and evidence`, /08:00/.test(result.detail) && /arrived (on time|\d+ min (late|early))/i.test(result.detail), result.detail.slice(0,140));
  check(`journey review ${viewport}: selected-stop departure is shown in the table`, result.departure === "08:00", result.departure);
  check(`journey review ${viewport}: chart expands inside the viewport`, result.expands);
  check(`journey review ${viewport}: shared links restore the chosen cohort and metric`, result.restored);
  check(`journey review ${viewport}: view and build are shareable`, /jt-build=a{64}/.test(result.shared || "") && /jt-service=/.test(result.shared || ""));
  check(`journey review ${viewport}: exports and evidence are available without page overflow`, result.csv && result.evidence && result.bodyWidth <= result.width + 1, JSON.stringify(result));
}

/** Simple against a fixture with a recorded timetable: the line is the
 *  timetable's, and the headline agrees with Detailed's for the same trip. */
async function checkJourneySimple(page, viewport) {
  const result = JSON.parse(await page.evaluate(`(async () => {
    const build = "c".repeat(64), file = "builds/" + build + "/700-SCSO.json";
    const day = "2026-09-21", origin = Date.parse(day + "T00:00:00+01:00") / 1000;
    const stops = [{atco:"4400AD0064",name:"Lancing",lat:50.823,lon:-.321},
                  {atco:"149000007830",name:"Brighton",lat:50.820,lon:-.136}];
    const journeys = [], trips = [];
    for (let i = 0; i < 12; i++) {
      const dep = 25200 + i * 1800, took = 1500 + (i % 4) * 120, sched = 1320;
      journeys.push({day, start: "", trip_id: "s-" + i, direction: "eastbound", headsign: "Brighton",
        route_pattern: "p", data_version: "t", method_version: 4, match: "declared", source_files: ["h"], quality_flags: [],
        calls: [[0, dep, dep, 0, 0, origin + dep, origin + dep, [origin + dep, origin + dep + 30], [], "declared"],
                [1, dep + took, dep + sched, 0, 1, origin + dep + took, origin + dep + sched, [origin + dep + took, origin + dep + took + 30], [], "declared"]]});
      trips.push(["s-" + i, 0, dep, "Brighton"]);
    }
    trips.push(["unseen", 0, 25200 + 12 * 1800, "Brighton"]);
    const doc = {build_id: build, service: "700", operator: "SCSO", days: [day], window_days: [day], stops, journeys,
      method: "Simple fixture", as_of: day, caveats: [], source_methods: [],
      schedule: {profiles: [[[0, 0, 1], [1, 1320, 1]]], sets: [trips], days: {[day]: 0}, unrecorded_days: []}};
    const index = {build_id: build, days: [day], services: [{file, service: "700", operator: "SCSO", journeys: 12}]};
    journeyTimesCache.set("index.json", index); journeyTimesCache.set(file, doc);
    CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC = false; jtEntry.view = undefined; jtEntry.section = "journey";
    jtEntry.period = "any"; jtEntry.runs = null;
    jtEntry.mode = "browse"; jtEntry.only = null; jtEntry.wanted = null;
    for (const key of ["service", "direction", "from", "to", "days"]) {
      const el = document.getElementById("journey-times-" + key); for (const attr of Object.keys(el.dataset)) delete el.dataset[attr];
    }
    setViewMode("journeytimes"); await renderJourneyTimes();
    const host = document.getElementById("journey-times-result");
    const simple = host.querySelector(".jt-simple");
    const usually = simple?.querySelector(".jt-simple-usually strong")?.textContent.trim() || "";
    const legend = simple?.querySelector(".jt-legend")?.textContent || "";
    const line = simple ? simple.querySelectorAll("path.jt-timetable--weekday").length : 0;
    const trust = simple?.querySelector(".jt-simple-trust")?.textContent.replace(/\\s+/g, " ") || "";
    const small = [...(simple ? simple.querySelectorAll("button, a") : [])]
      .map(el => el.getBoundingClientRect()).filter(r => r.width && Math.min(r.width, r.height) < 44).length;
    // One control reads the chart: the hour slider, not a tab stop per bus.
    const dotStops = simple ? simple.querySelectorAll(".jt-chart [tabindex]").length : -1;
    const range = simple?.querySelector(".jt-hour-range");
    const before = range?.getAttribute("aria-valuetext") || "";
    if (range) { range.value = String(Math.min(Number(range.max), Number(range.min) + 3)); range.dispatchEvent(new Event("input", {bubbles: true})); }
    const tapped = host.querySelector(".jt-hour-detail")?.textContent || "";
    const moved = (range?.getAttribute("aria-valuetext") || "") !== before;
    const badge = simple?.querySelector(".jt-evidence-badge")?.textContent.trim() || "";
    const allow = simple?.querySelector(".jt-simple-allow")?.textContent.replace(/\\s+/g, " ").trim() || "";
    const chip = simple?.querySelector('[data-jt-period="10-16"]');
    chip?.focus();
    chip?.click();
    const pressed = host.querySelector('[data-jt-period="10-16"]')?.getAttribute("aria-pressed");
    // The chip was replaced by the redraw: focus has to land on its successor,
    // not fall back to the top of the page.
    const keptFocus = document.activeElement?.getAttribute?.("data-jt-period") || document.activeElement?.tagName || "";
    await new Promise(r => setTimeout(r, 150));
    const said = document.getElementById("jt-announce")?.textContent || "";
    const hoursTable = !!host.querySelector(".jt-hours-table");
    host.querySelector('[data-jt-period="any"]')?.click();
    const bodyWidth = document.documentElement.scrollWidth;
    // The same trip in Detailed, which must say the same number.
    CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC = true; jtEntry.view = "detailed";
    await renderJourneyTimes();
    const detailed = host.querySelector(".jt-headline strong")?.textContent.trim() || "";
    CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC = false; jtEntry.view = undefined;
    await renderJourneyTimes();
    return JSON.stringify({simple: !!simple, usually, detailed, legend, line, trust, small, tapped, pressed,
      dotStops, moved, badge, allow, keptFocus, said, hoursTable, bodyWidth, width: innerWidth});
  })()`));
  check(`journey simple ${viewport}: the passenger answer renders`, result.simple && !!result.usually, JSON.stringify(result));
  check(`journey simple ${viewport}: Simple and Detailed agree on the headline`, result.usually === result.detailed,
    `Simple "${result.usually}" against Detailed "${result.detailed}"`);
  check(`journey simple ${viewport}: the line is the recorded timetable`, result.line === 1
    && /Timetable/.test(result.legend) && !/from the buses we timed/.test(result.legend), result.legend.slice(0, 160));
  check(`journey simple ${viewport}: it says how many scheduled buses were tracked`,
    /timetable scheduled 13 between these stops/.test(result.trust) && /tracked 12 of them/.test(result.trust), result.trust);
  check(`journey simple ${viewport}: the hour slider says in words what an hour shows`,
    result.moved && /^Leaving between \d\d:00 and \d\d:00 on weekdays/.test(result.tapped), result.tapped);
  check(`journey simple ${viewport}: no bus on the chart is its own tab stop`, result.dotStops === 0, `${result.dotStops} tab stops`);
  // The fixture's timetable runs every 30 minutes, which is "some buses".
  check(`journey simple ${viewport}: how often buses run, and how long to allow, are in words`,
    result.badge === "Some buses · every 30 min" && /Allow \d+ min (None|Only \d+) of the 12 buses we timed took longer/.test(result.allow),
    `${result.badge} | ${result.allow}`);
  check(`journey simple ${viewport}: the time-of-day choice responds`, result.pressed === "true");
  check(`journey simple ${viewport}: a pressed chip keeps the focus after the redraw`, result.keptFocus === "10-16",
    `focus on ${result.keptFocus}`);
  check(`journey simple ${viewport}: the change is announced in one sentence`,
    /Lancing to Brighton, Weekdays, daytime .*: usually takes \d+ min\. (Allow \d+ min|Too few)/.test(result.said), result.said);
  check(`journey simple ${viewport}: every hour is also in a table`, result.hoursTable);
  check(`journey simple ${viewport}: targets and width`, result.small === 0 && result.bodyWidth <= result.width + 1,
    `${result.small} small targets; ${result.bodyWidth}px on ${result.width}px`);
}

/** The delay map against a fixture: two stretches, one with enough evidence. */
async function checkDelayMap(page, viewport) {
  const result = JSON.parse(await page.evaluate(`(async () => {
    const build = "d".repeat(64), day = "2026-09-21";
    const floor = {journeys: 30, distinct_days: 5};
    const days = ["2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22"];
    const cell = (stretch, over) => ({stretch, latest: true, day_type: "weekday", resolution: "period",
      period: "08-10", traversals: 40, at_least_600s: 8, journeys: 40, distinct_days: 6,
      median_gained_secs: 200, sample_sufficient: true, schedule_era: "e", data_versions: ["t"], ...over});
    const mapDoc = {build_id: build, schema_version: 1, service: "700", operator: "SCSO", floor, days_collected: days,
      method: "Delay fixture", pooling: "", caveats: ["Fixture"],
      stretches: [
        {id: "s1", direction: "westbound", from_atco: "4400AD0064", to_atco: "4400AD0204",
         from_name: "Lancing", to_name: "Shoreham", headsign: "Worthing",
         geometry: [[50.823, -0.321], [50.828, -0.30], [50.832, -0.275]], approximate: false},
        {id: "s2", direction: "westbound", from_atco: "4400AD0204", to_atco: "4400AD0259",
         from_name: "Shoreham", to_name: "Southwick", headsign: "Worthing",
         geometry: [[50.832, -0.275], [50.834, -0.24]], approximate: true}],
      cells: [cell("s1"), cell("s2", {median_gained_secs: 30, journeys: 9, traversals: 9, distinct_days: 2,
        at_least_600s: 0, sample_sufficient: false}),
        cell("s1", {resolution: "hour", hour: 8, period: undefined, journeys: 31, traversals: 31, distinct_days: 5})]};
    const mapIndex = {schema_version: 1, floor, days_collected: days,
      services: [{service: "700", operator: "SCSO", file: "hotspot-map-700-SCSO.json", stretches: 2, sufficient_cells: 2, days_collected: 6}]};
    const index = {build_id: build, days: [day], services: [], artifacts: {
      "hotspot-map-index.json": {file: "builds/" + build + "/hotspot-map-index.json"},
      "hotspot-map-700-SCSO.json": {file: "builds/" + build + "/hotspot-map-700-SCSO.json"}}};
    journeyTimesCache.set("index.json", index);
    journeyTimesCache.set("builds/" + build + "/hotspot-map-index.json", mapIndex);
    journeyTimesCache.set("builds/" + build + "/hotspot-map-700-SCSO.json", mapDoc);
    CONFIG.DELAY_MAP_PUBLIC = true; CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC = false; jtEntry.view = undefined;
    Object.assign(jtDelay, {service: null, direction: null, slot: "08-10", day: "weekday", time: "08-10", hour: 8,
      dayType: "weekday", exploratory: false, selected: null, fittedFor: "", pendingService: null});
    setViewMode("journeytimes");
    document.querySelector('#jt-sections [data-jt-section="delays"]').click();
    await new Promise(r => setTimeout(r, 400));
    const panel = document.getElementById("jt-delay-panel");
    const rect = panel.getBoundingClientRect();
    const red = document.querySelector(".delay-line--red"), none = document.querySelector(".delay-line--none");
    const stroke = el => el ? getComputedStyle(el).stroke : "";
    const lightRed = stroke(red);
    document.documentElement.classList.add("dark-mode");
    const darkRed = stroke(red);
    document.documentElement.classList.remove("dark-mode");
    const mapLegend = document.querySelector(".delay-legend-map");
    const mapLegendShown = !!mapLegend && getComputedStyle(mapLegend).display !== "none";
    const listButtons = [...panel.querySelectorAll("[data-delay-stretch]")];
    const small = listButtons.map(b => b.getBoundingClientRect()).filter(r => r.width && Math.min(r.width, r.height) < 44).length;
    const status = panel.querySelector(".delay-status")?.textContent.replace(/\\s+/g, " ") || "";
    panel.querySelector(".delay-list-wrap")?.setAttribute("open", "");
    listButtons[0]?.click();
    const detail = panel.querySelector(".delay-detail")?.textContent.replace(/\\s+/g, " ") || "";
    const bodyWidth = document.documentElement.scrollWidth;
    // Detailed: the hour slider, on the same stretches.
    CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC = true; jtEntry.view = "detailed"; jtDelay.time = "hour"; jtDelay.hour = 8;
    await renderJourneyTimes(); await new Promise(r => setTimeout(r, 300));
    const slider = !!panel.querySelector('input[type="range"][data-delay-hour]');
    const hourRed = document.querySelectorAll(".delay-line--red").length;
    // Leaving the view must take every line with it.
    setViewMode("live"); await new Promise(r => setTimeout(r, 300));
    const leftover = document.querySelectorAll(".delay-line, .delay-casing").length;
    const muted = state.map.getContainer().classList.contains("delay-map-on");
    CONFIG.DELAY_MAP_PUBLIC = false; CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC = false;
    jtEntry.view = undefined; jtEntry.section = "journey"; jtDelay.time = "08-10";
    return JSON.stringify({shown: !panel.hidden && rect.height > 0, red: !!red, none: !!none,
      dashed: none?.getAttribute("stroke-dasharray") || "", lightRed, darkRed, mapLegendShown,
      panelLegend: panel.querySelectorAll(".delay-legend-panel .delay-legend li").length,
      small, status, detail, bodyWidth, width: innerWidth, slider, hourRed, leftover, muted});
  })()`));
  const blue = "rgb(51, 136, 255)";   // Leaflet's default: the CSS failed to apply
  check(`delay map ${viewport}: the panel and both kinds of line are drawn`, result.shown && result.red && result.none,
    JSON.stringify(result));
  check(`delay map ${viewport}: thin evidence is grey and dashed, never green`, !!result.dashed, `dasharray "${result.dashed}"`);
  check(`delay map ${viewport}: colours come from the theme in both themes`,
    result.lightRed && result.darkRed && result.lightRed !== result.darkRed
    && result.lightRed !== blue && result.darkRed !== blue, `${result.lightRed} / ${result.darkRed}`);
  check(`delay map ${viewport}: the legend is in the panel, and on the map only where the sheet cannot hide it`,
    result.panelLegend === 5 && result.mapLegendShown === (result.width > 900),
    `panel ${result.panelLegend}, map legend shown ${result.mapLegendShown} at ${result.width}px`);
  check(`delay map ${viewport}: it says how much of the map has enough evidence`,
    /1 of 2 stretches have enough journeys/.test(result.status), result.status);
  check(`delay map ${viewport}: every stretch is reachable from the list and says what it measured`,
    result.small === 0 && /usually lose 3\.3 min/.test(result.detail), `${result.small} small; ${result.detail.slice(0, 120)}`);
  check(`delay map ${viewport}: Detailed adds an hourly slider over the same stretches`, result.slider && result.hourRed === 1,
    `slider ${result.slider}, red at 08:00 ${result.hourRed}`);
  check(`delay map ${viewport}: leaving the view takes every line and the muted map with it`,
    result.leftover === 0 && !result.muted, `${result.leftover} lines left, muted ${result.muted}`);
  check(`delay map ${viewport}: fits the screen`, result.bodyWidth <= result.width + 1, `${result.bodyWidth}px on ${result.width}px`);
}

/** Two services make one trip, and the busier is not the lowest-numbered.
 *
 *  The service list used to be set before it was rebuilt for the new pair, so
 *  the choice silently failed, the lowest number opened, and the pair's stop
 *  indices from the 7's document were applied to the 3X's: Brighton Station to
 *  Hove Station was answered as Brighton University to Hardwick Road. */
const JT_PAIR_FIXTURE = `
  const day = "2026-09-21";
  const A = {atco: "probe-bs", name: "Probe Station", lat: 50.829, lon: -0.141, services: ["3X", "7"], locality: "Probetown"};
  const B = {atco: "probe-hs", name: "Probe Hove", lat: 50.835, lon: -0.170, services: ["3X", "7"], locality: "Probe Hove"};
  const X = {atco: "probe-x", name: "Probe University", lat: 50.84, lon: -0.12, services: ["3X"], locality: "Probetown"};
  const Y = {atco: "probe-y", name: "Probe Hardwick", lat: 50.85, lon: -0.20, services: ["3X"], locality: "Probe Hills"};
  const Z = {atco: "probe-z", name: "Probe Elsewhere", lat: 50.81, lon: -0.35, services: ["99"], locality: "Faraway"};
  const fixtureStops = [A, B, X, Y, Z];
  for (const s of fixtureStops) state.stopData[s.atco] = s;
  state._stopIndex = null;
  const trip = (i, calls) => ({day, start: "", trip_id: "t" + i, direction: "westbound", headsign: "Probe Hove",
    route_pattern: "p", data_version: "t", method_version: 4, match: "declared", source_files: [], quality_flags: [], calls});
  const doc3x = {service: "3X", operator: "BHBC", days: [day], window_days: [day], method: "fixture", caveats: [], source_methods: [],
    stops: [X, A, B, Y].map(s => ({atco: s.atco, name: s.name})),
    journeys: [0, 1, 2].map(i => { const d = 28800 + i * 3600; return trip("3x" + i,
      [[0, d, d, 0], [1, d + 300, d + 300, 0], [2, d + 900, d + 840, 0], [3, d + 1500, d + 1400, 0]]); })};
  const doc7 = {service: "7", operator: "BHBC", days: [day], window_days: [day], method: "fixture", caveats: [], source_methods: [],
    stops: [{atco: "probe-p", name: "Probe P"}, {atco: "probe-q", name: "Probe Q"}, {atco: A.atco, name: A.name}, {atco: B.atco, name: B.name}],
    journeys: Array.from({length: 12}, (_, i) => { const d = 25200 + i * 1200; return trip("7-" + i,
      [[0, d, d, 0], [1, d + 200, d + 200, 0], [2, d + 400, d + 400, 0], [3, d + 1100, d + 1000, 0]]); })};
  const index = {days: [day], services: [
    {file: "3X-BHBC.json", service: "3X", operator: "BHBC", journeys: 3},
    {file: "7-BHBC.json", service: "7", operator: "BHBC", journeys: 12},
    {file: "99-BHBC.json", service: "99", operator: "BHBC", journeys: 1}]};
  journeyTimesCache.set("index.json", index);
  journeyTimesCache.set("3X-BHBC.json", doc3x);
  journeyTimesCache.set("7-BHBC.json", doc7);
  const cleanup = () => {
    for (const s of fixtureStops) delete state.stopData[s.atco];
    state._stopIndex = null; jtBar.list = null;
    for (const k of ["index.json", "3X-BHBC.json", "7-BHBC.json"]) journeyTimesCache.delete(k);
    clearJourneyTimesEntry(); jtEntry.runs = null; jtEntry.wanted = null; jtEntry.hour = null;
  };
  CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC = false; jtEntry.view = undefined; jtEntry.section = "journey";
  setViewMode("journeytimes");
  await new Promise(r => setTimeout(r, 300));
  const svc = document.getElementById("journey-times-service");
  svc.innerHTML = ""; for (const k of Object.keys(svc.dataset)) delete svc.dataset[k];
  for (const key of ["direction", "from", "to", "days"]) {
    const el = document.getElementById("journey-times-" + key); for (const k of Object.keys(el.dataset)) delete el.dataset[k];
  }
  clearJourneyTimesEntry(); jtEntry.mode = "start"; jtEntry.runs = null; jtEntry.wanted = null;
  const selName = id => { const sel = document.getElementById(id); return sel.options[sel.selectedIndex]?.textContent.replace(/\\s+—.*$/, "").trim() || ""; };
  const bar = () => [document.getElementById("jt-from-input").value, document.getElementById("jt-to-input").value];
`;

async function checkJourneyPairService(page, viewport) {
  const result = JSON.parse(await page.evaluate(`(async () => {
    ${JT_PAIR_FIXTURE}
    try {
      jtEntry.a = A.atco; jtEntry.b = B.atco;
      await journeyTimesResolvePair();
      await new Promise(r => setTimeout(r, 300));
      const first = {service: svc.value, from: selName("journey-times-from"), to: selName("journey-times-to"), bar: bar()};
      // The other bus, from the chips: the same two stops on the 3X's own list.
      document.querySelector('.jt-which [data-jt-file="3X-BHBC.json"]')?.click();
      await new Promise(r => setTimeout(r, 600));
      const second = {service: svc.value, from: selName("journey-times-from"), to: selName("journey-times-to"), bar: bar()};
      return JSON.stringify({first, second});
    } finally { cleanup(); }
  })()`));
  check(`journey pair ${viewport}: the busier of two services opens, on the stops that were picked`,
    result.first.service === "7-BHBC.json" && result.first.from === "Probe Station" && result.first.to === "Probe Hove",
    JSON.stringify(result.first));
  check(`journey pair ${viewport}: switching bus keeps the two stops`,
    result.second.service === "3X-BHBC.json" && result.second.from === "Probe Station" && result.second.to === "Probe Hove"
    && result.second.bar[0] === "Probe Station" && result.second.bar[1] === "Probe Hove",
    JSON.stringify(result.second));
}

/** A compact journey-times file, fetched the way the view fetches one, reads
 *  back as exactly the document it was written from. The nightly build writes
 *  every service compact (scripts/journey_times_codec.py). Node tests check
 *  the decoder; this checks it in the browser that actually runs it. */
async function checkJourneyCompactFile(page) {
  const fixture = JSON.parse(await readFile(
    new URL("../tests/fixtures/journey_times_codec.json", import.meta.url), "utf8"));
  const got = JSON.parse(await page.evaluate(`(async () => {
    const compact = ${JSON.stringify(fixture.compact)};
    const realFetch = window.fetch;
    window.fetch = async (url, ...rest) => String(url).endsWith("/probe-compact.json")
      ? new Response(JSON.stringify(compact), {headers: {"Content-Type": "application/json"}})
      : realFetch(url, ...rest);
    try {
      const doc = await loadJourneyTimes("probe-compact.json");
      return JSON.stringify({journeys: doc.journeys, encoding: doc.encoding ?? null, tables: "tables" in doc});
    } finally {
      window.fetch = realFetch;
      journeyTimesCache.delete("probe-compact.json");
    }
  })()`));
  check("journey times: a compact file read by the view is exactly the document written",
    isDeepStrictEqual(got.journeys, fixture.expanded.journeys) && got.encoding === null && !got.tables,
    got.encoding ? `still compact: ${got.encoding}` : "");
}

/** The journey bar: type a stop, choose it from the keyboard, and be offered
 *  only stops a direct bus reaches from it. */
async function checkJourneyBar(page, viewport) {
  const result = JSON.parse(await page.evaluate(`(async () => {
    ${JT_PAIR_FIXTURE}
    const wait = ms => new Promise(r => setTimeout(r, ms));
    const type = async (slot, text) => { const i = document.getElementById("jt-" + slot + "-input"); i.focus(); i.value = text;
      i.dispatchEvent(new Event("input", {bubbles: true})); await wait(250); };
    const key = (slot, k) => document.getElementById("jt-" + slot + "-input")
      .dispatchEvent(new KeyboardEvent("keydown", {key: k, bubbles: true, cancelable: true}));
    const options = slot => [...document.querySelectorAll("#jt-" + slot + "-list li")].map(li => li.textContent.replace(/\\s+/g, " ").trim());
    try {
      renderJourneyTimes(); await wait(300);
      await type("from", "probetown sta");
      const fromOptions = options("from");
      const fromInput = document.getElementById("jt-from-input");
      const expanded = fromInput.getAttribute("aria-expanded");
      const active = fromInput.getAttribute("aria-activedescendant");
      key("from", "Enter"); await wait(400);
      const chosen = {a: jtEntry.a, bar: bar(), focus: document.activeElement?.id, collapsed: fromInput.getAttribute("aria-expanded")};
      await type("to", "probe else");
      // Said beside the box, with a way on, not as a dead option in the list.
      const unreachable = [document.getElementById("jt-to-note")?.textContent.replace(/\\s+/g, " ").trim() || ""];
      const wayOn = !!document.querySelector('#jt-to-note [data-jt-note-go="tickets"]');
      const listShut = document.getElementById("jt-to-input").getAttribute("aria-expanded") === "false";
      const told = document.getElementById("jt-combo-status")?.textContent || "";
      await type("to", "probe");
      const reachable = options("to");
      key("to", "ArrowDown");
      const moved = document.getElementById("jt-to-input").getAttribute("aria-activedescendant");
      const targets = [...document.querySelectorAll("#jt-journey input, #jt-journey button")]
        .filter(el => el.offsetParent !== null).map(el => el.getBoundingClientRect())
        .filter(r => r.width && Math.min(r.width, r.height) < 44).length;
      await type("to", "probe hove");
      key("to", "Enter");
      await wait(900);
      const result = {service: svc.value, bar: bar(), usually: document.querySelector(".jt-simple-usually strong")?.textContent || ""};
      document.getElementById("jt-swap").click(); await wait(900);
      // The fixture's buses run one way only, so a swap is answered the way
      // they run, and says so, rather than with nothing.
      const swapped = {bar: bar(), note: document.querySelector(".jt-simple .jt-provenance")?.textContent || ""};
      document.querySelector('[data-jt-clear="to"]').click(); await wait(400);
      const cleared = {mode: jtEntry.mode, a: jtEntry.a, b: jtEntry.b, bar: bar()};
      return JSON.stringify({fromOptions, expanded, active, chosen, unreachable, wayOn, listShut, told, reachable, moved, targets, result, swapped, cleared,
        bodyWidth: document.documentElement.scrollWidth, width: innerWidth});
    } finally { cleanup(); }
  })()`));
  check(`journey bar ${viewport}: a stop is found by its place and words in any order`,
    result.fromOptions.some(o => /^Probe Station/.test(o)) && result.expanded === "true" && /opt-0$/.test(result.active || ""),
    JSON.stringify(result.fromOptions));
  check(`journey bar ${viewport}: Enter takes the highlighted stop and moves on to To`,
    result.chosen.a === "probe-bs" && result.chosen.bar[0] === "Probe Station" && result.chosen.focus === "jt-to-input"
    && result.chosen.collapsed === "false", JSON.stringify(result.chosen));
  check(`journey bar ${viewport}: To offers only stops a direct bus reaches`,
    /No stop with a direct bus from Probe Station/.test(result.unreachable.join(" ")) && result.wayOn && result.listShut
    && /No stop with a direct bus/.test(result.told)
    && !result.reachable.some(o => /Probe Elsewhere|Probe Station/.test(o))
    && result.reachable.some(o => /Probe Hove/.test(o)), `${result.unreachable.join(" | ")} / ${result.reachable.join(" | ")}`);
  check(`journey bar ${viewport}: the arrow keys move through the suggestions`, /jt-to-opt-1$/.test(result.moved || ""),
    String(result.moved));
  check(`journey bar ${viewport}: two typed stops give an answer on the busier bus`,
    result.result.service === "7-BHBC.json" && /min/.test(result.result.usually) && result.result.bar[1] === "Probe Hove",
    JSON.stringify(result.result));
  check(`journey bar ${viewport}: swap turns the trip round, or says the buses only run one way`,
    (result.swapped.bar[0] === "Probe Hove" && result.swapped.bar[1] === "Probe Station")
    || (/direction buses actually run/.test(result.swapped.note) && result.swapped.bar[0] === "Probe Station"),
    JSON.stringify(result.swapped));
  check(`journey bar ${viewport}: clearing To goes back a step and keeps From`,
    result.cleared.mode === "start" && result.cleared.b === null && result.cleared.bar[1] === ""
    && result.cleared.bar[0] !== "", JSON.stringify(result.cleared));
  check(`journey bar ${viewport}: targets and width`, result.targets === 0 && result.bodyWidth <= result.width + 1,
    `${result.targets} small; ${result.bodyWidth}px on ${result.width}px`);
}

/** A chosen journey survives a visit to the delay map, and a visit to Live.
 *
 *  Both used to clear it as a side effect of taking their layers off the map,
 *  so "Next buses from", then Back, came back to an empty form. */
async function checkJourneyKept(page, viewport) {
  const result = JSON.parse(await page.evaluate(`(async () => {
    ${JT_PAIR_FIXTURE}
    const wait = ms => new Promise(r => setTimeout(r, ms));
    try {
      CONFIG.DELAY_MAP_PUBLIC = true;
      jtEntry.a = A.atco; jtEntry.b = B.atco;
      await journeyTimesResolvePair(); await wait(300);
      document.querySelector('#jt-sections [data-jt-section="delays"]').click(); await wait(500);
      document.querySelector('#jt-sections [data-jt-section="journey"]').click(); await wait(800);
      const afterSection = {mode: jtEntry.mode, bar: bar(), answer: !!document.querySelector(".jt-simple .jt-simple-usually")};
      setViewMode("live"); await wait(400);
      setViewMode("journeytimes"); await wait(1200);
      const afterView = {mode: jtEntry.mode, bar: bar(), answer: !!document.querySelector(".jt-simple .jt-simple-usually")};
      return JSON.stringify({afterSection, afterView});
    } finally { CONFIG.DELAY_MAP_PUBLIC = false; jtEntry.section = "journey"; cleanup(); }
  })()`));
  const kept = r => r.mode === "picked" && r.answer && r.bar[0] === "Probe Station" && r.bar[1] === "Probe Hove";
  check(`journey kept ${viewport}: the delay map and back keeps the journey`, kept(result.afterSection),
    JSON.stringify(result.afterSection));
  check(`journey kept ${viewport}: another view and back keeps the journey`, kept(result.afterView),
    JSON.stringify(result.afterView));
}

async function checkSelectedFareRoute(page) {
  const result = await page.evaluate(`(async () => {
    await loadTicketZones(); setViewMode("tickets");
    const a={atco:"probe-a",name:"Start",lat:50.823,lon:-.16};
    const b={atco:"probe-b",name:"End",lat:50.823,lon:-.13};
    const mid={atco:"probe-mid",name:"Change",lat:50.823,lon:-.15};
    state.stopData[a.atco]=a; state.stopData[b.atco]=b;
    const legs=[{service:"2",operator:"BHBC",depart:"12:00",arrive:"12:08",stops:[a,mid]},
      {service:"25",operator:"BHBC",depart:"12:10",arrive:"12:20",stops:[mid,b]}];
    const interchange={legs,total_minutes:20,change_at:mid,board_at:mid,walk_metres:0,wait_minutes:2};
    const journey={from:a,to:b,options:[{service:"700",operator:"SCSO",depart:"12:00",arrive:"13:00",stops:[a,b],stop_count:2}],
      interchange,itineraries:[interchange]};
    renderJourneyResult(journey,a.atco,b.atco);
    const before=state.journeyLayers.filter(layer=>layer.options?.className?.startsWith("journey-leg ")).length;
    const button=dom.jcResult.querySelector('[data-journey-choice="quickest"]');
    const rect=button?.getBoundingClientRect();
    button?.click();
    const after=state.journeyLayers.filter(layer=>layer.options?.className?.startsWith("journey-leg ")).length;
    const chosen=dom.jcResult.querySelector('[data-journey-choice="quickest"]')?.getAttribute("aria-pressed");
    return {before,after,chosen,height:rect?.height,says:dom.jcResult.textContent};
  })()`);
  check("selecting a fare alternative redraws its own route and itinerary", result.before===1 && result.after===2 && result.chosen==="true" && /quickest option found/.test(result.says), JSON.stringify(result));
  check("fare route selection meets the touch target minimum", result.height>=44, String(result.height));
}

/** The public flow: the Simple view, against the live published data. */
async function checkJourneyTimesSimpleFlow(page) {
  await page.evaluate(`(() => { CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC = false; jtEntry.view = undefined;
    jtEntry.section = "journey"; clearJourneyTimesEntry(); jtEntry.mode = "start"; setViewMode('journeytimes'); return "1"; })()`);
  await sleep(4000);
  const opening = JSON.parse(await page.evaluate(`
    (() => {
      const host = document.getElementById("journey-times-result");
      const controls = document.querySelector(".journey-times-controls");
      const toggle = document.getElementById("jt-view-toggle");
      return JSON.stringify({
        says: host.textContent.replace(/\\s+/g, " ").trim().slice(0, 120),
        controlsHidden: !controls || controls.hidden,
        browse: !!host.querySelector('[data-act="browse"]'),
        toggleHidden: !toggle || toggle.hidden,
        pickable: document.querySelectorAll("path.jt-stop, circle.jt-stop").length,
      });
    })()`));
  check("the public journey-time view asks a passenger's question",
    /how long will my bus really take/i.test(opening.says), opening.says);
  check("the public view shows no expert controls or ways into them",
    opening.controlsHidden && !opening.browse && opening.toggleHidden, JSON.stringify(opening));
  check("the public view still draws stops to tap", opening.pickable > 50, `${opening.pickable} stops`);

  await page.evaluate(`(() => { journeyTimesEntryPick("4400AD0064"); journeyTimesEntryPick("149000007830"); return "1"; })()`);
  await sleep(6000);
  const answer = JSON.parse(await page.evaluate(`
    (() => {
      const simple = document.querySelector(".jt-simple");
      const controls = document.querySelector(".journey-times-controls");
      const small = [...(simple ? simple.querySelectorAll("button, a") : [])]
        .map(el => el.getBoundingClientRect()).filter(r => r.width && Math.min(r.width, r.height) < 44).length;
      return JSON.stringify({
        simple: !!simple,
        usually: simple ? /usually takes/i.test(simple.textContent) : false,
        line: document.querySelectorAll(".jt-simple path.jt-timetable").length,
        controlsHidden: !controls || controls.hidden,
        small, bodyWidth: document.documentElement.scrollWidth, width: innerWidth,
        text: simple ? simple.textContent.replace(/\\s+/g, " ").trim().slice(0, 160) : "",
      });
    })()`));
  if (!answer.simple && !answer.text) {
    check("the Simple view has live data to answer from", false,
      "no answer rendered: if R2 is unreachable this is expected");
    return;
  }
  check("picking two stops gives a passenger an answer in words", answer.simple && answer.usually, answer.text);
  check("the Simple answer draws the timetable across the day", answer.line > 0, `${answer.line} timetable lines`);
  check("the Simple answer keeps the expert controls out of sight", answer.controlsHidden);
  check("every Simple target meets the tap-target minimum", answer.small === 0, `${answer.small} under 44px`);
  check("the Simple answer fits the screen", answer.bodyWidth <= answer.width + 1,
    `${answer.bodyWidth}px wide on a ${answer.width}px screen`);
}

async function checkJourneyTimes(page) {
  await checkJourneyTimesSimpleFlow(page);
  // Everything below exercises the Detailed tool, as ?preview=1 would.
  // setViewMode is a no-op when the view is already open, so the reset has to
  // redraw for itself: otherwise the Simple answer stays on screen and every
  // Detailed check below reads it.
  await page.evaluate(`(() => { CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC = true; jtEntry.view = "detailed";
    jtEntry.section = "journey"; clearJourneyTimesEntry(); jtEntry.wanted = null; jtEntry.runs = null;
    renderJourneyTimes(); return "1"; })()`);
  try {
    await checkJourneyTimesDetailedFlow(page);
  } finally {
    await page.evaluate(`(() => { CONFIG.JOURNEY_TIMES_DETAILED_PUBLIC = false; jtEntry.view = undefined; return "1"; })()`);
  }
}

async function checkJourneyTimesDetailedFlow(page) {
  await page.evaluate(`setViewMode('journeytimes')`);
  await sleep(4000);

  // The opening state. The view used to arrive as six selects and a chart of
  // whichever service sorted first, which answers a question nobody asked.
  const intro = await page.evaluate(`
    (() => {
      const host = document.getElementById("journey-times-result");
      const controls = document.querySelector(".journey-times-controls");
      const browse = host && host.querySelector('[data-act="browse"]');
      const r = browse && browse.getBoundingClientRect();
      return JSON.stringify({
        verdict: !!host.querySelector(".jt-verdict"),
        says: host.textContent.replace(/\\s+/g, " ").trim().slice(0, 90),
        controlsHidden: !controls || controls.hidden,
        chart: !!document.querySelector(".jt-chart"),
        browse: !!browse,
        tap: r ? Math.round(Math.min(r.width, r.height)) : 0,
        pickable: document.querySelectorAll("path.jt-stop, circle.jt-stop").length,
      });
    })()`);
  const at = JSON.parse(intro);
  check("the journey-time view opens on an instruction, not on the tool",
    at.verdict && at.controlsHidden && !at.chart,
    `opened with controlsHidden=${at.controlsHidden} chart=${at.chart}`);
  check("the instruction says to pick two stops",
    /pick any two stops/i.test(at.says), at.says);
  check("the opening state offers a way to browse everything", at.browse,
    "a reader who does not want to hunt on the map has no way through");
  check("the browse button meets the tap-target minimum", at.tap >= 44,
    `${at.tap}px against the 44px house rule`);
  check("the opening state draws stops on the map to click", at.pickable > 50,
    `${at.pickable} clickable stops — the flow the view invites needs them`);

  // Two clicks on the map should reveal the tool for the services that really
  // run between them, and nothing else.
  await page.evaluate(`
    (() => {
      // Two stops a long way apart on one corridor: Lancing seafront and Old
      // Steine, which the Coastliner runs end to end.
      journeyTimesEntryPick("4400AD0064");
      journeyTimesEntryPick("149000007830");
      return "1";
    })()`);
  await sleep(6000);
  const afterPick = await page.evaluate(`
    (() => {
      const svc = document.getElementById("journey-times-service");
      const controls = document.querySelector(".journey-times-controls");
      return JSON.stringify({
        mode: jtEntry.mode,
        options: svc ? svc.options.length : -1,
        labels: svc ? [...svc.options].map(o => o.textContent.trim()) : [],
        controlsShown: !!controls && !controls.hidden,
        dots: document.querySelectorAll(".jt-dot").length,
      });
    })()`);
  const two = JSON.parse(afterPick);
  check("picking two stops reveals the tool", two.controlsShown && two.dots > 0,
    `mode=${two.mode} controlsShown=${two.controlsShown} dots=${two.dots}`);
  check("picking two stops narrows the service list to what runs between them",
    two.options > 0 && two.options < 10,
    `${two.options} services offered: ${two.labels.join(" | ")}`);

  // And back to the whole dataset, which must restore the full list — the
  // "browse everything" way in, which is the other half of the opening state.
  await page.evaluate(`
    (() => { clearJourneyTimesEntry(); renderJourneyTimes(); return "1"; })()`);
  await sleep(3500);
  // Defensively: if the opening state has gone, say which check to look at
  // rather than throwing a stack trace out of the harness. A regression here
  // took down the whole run instead of reporting one red line.
  const browsed = await page.evaluate(`
    (() => {
      const b = document.querySelector('[data-act="browse"]');
      if (b) b.click();
      return b ? "1" : "0";
    })()`);
  check("the way out of the opening state is still there", browsed === "1",
    "no [data-act=browse] button — the opening state did not render, so every "
    + "check below is about whatever the view did instead");
  await sleep(6000);

  // Browsing everything must land on a whole, marked pair. Reported in full
  // because when this goes wrong the next check reads "1 ends marked" and says
  // nothing whatever about which control disagreed with which.
  const restored = await page.evaluate(`
    (() => {
      const svc = document.getElementById("journey-times-service");
      const dir = document.getElementById("journey-times-direction");
      const from = document.getElementById("journey-times-from");
      const to = document.getElementById("journey-times-to");
      return JSON.stringify({
        mode: jtEntry.mode,
        services: svc.options.length,
        service: svc.value,
        dir: dir.value,
        from: from.value,
        to: to.value,
        ends: document.querySelectorAll(".jt-stop--from, .jt-stop--to").length,
        stops: document.querySelectorAll(".jt-stop").length,
      });
    })()`);
  const back = JSON.parse(restored);
  check("browsing every service restores the whole list",
    back.services > 10, `${back.services} services offered`);
  check("browsing every service opens on a pair marked at both ends",
    back.ends === 2,
    `service=${back.service} dir=${back.dir} from=${back.from} to=${back.to} `
    + `ends=${back.ends} of ${back.stops} drawn`);

  const drawn = await page.evaluate(`
    (() => {
      const svg = document.querySelector(".jt-chart");
      const host = document.getElementById("journey-times-result");
      const r = svg && svg.getBoundingClientRect();
      return JSON.stringify({
        chart: !!svg,
        dots: document.querySelectorAll(".jt-dot").length,
        painted: !!r && r.width > 100 && r.height > 60,
        legend: document.querySelectorAll(".jt-legend li").length,
        zero: document.querySelectorAll(".jt-grid--zero").length,
        empty: host ? host.textContent.trim().length === 0 : true,
        directions: document.getElementById("journey-times-direction")?.options.length || 0,
        headsign: document.getElementById("journey-times-direction")?.options[0]?.text || "",
      });
    })()`);
  const d = JSON.parse(drawn);

  // Without data the rest says nothing, and locally the documents come from
  // R2 — so an offline run should skip rather than invent failures.
  if (!d.chart && d.empty) {
    check("the journey-time view has data to draw", false,
      "no chart and no message: if R2 is unreachable this is expected, "
      + "but an empty panel is what a reader would also see");
    return;
  }

  check("the journey-time chart is actually drawn", d.chart && d.painted,
    JSON.stringify(d));
  check("the chart has points on it", d.dots > 0,
    "an axis with no data is a chart that failed quietly");
  check("the chart says what its marks mean", d.legend >= 2,
    "a dashed dot means 'part interpolated' to nobody who is not told");
  check("the delay chart marks the timetable", d.zero > 0,
    "'above the line is slower than promised' needs the line");

  // Directions name places, and there are two of them. Before this the control
  // listed every destination — three for the 700, seven for the 2 — and named
  // them after stops, so a reader wanting "towards Brighton" had to know that
  // Old Steine is Brighton.
  const dirs = await page.evaluate(`
    (() => {
      const sel = document.getElementById("journey-times-direction");
      const opts = [...sel.options].map(o => o.text);
      return JSON.stringify({ count: opts.length, labels: opts });
    })()`);
  const dd = JSON.parse(dirs);
  check("a service offers two directions, not one per destination",
    dd.count === 2, `${dd.count}: ${dd.labels.join(" | ")}`);
  check("a direction is named where the bus goes, without the feed's stand letters",
    dd.labels.every(t => /^(Towards |(East|West|North|South)bound)/.test(t)
      && !/\(stop [A-Z0-9]+\)| [A-Z]{1,2}\d{1,2}\b/.test(t)),
    dd.labels.join(" | "));

  // The To list offers only what a bus reaches from From. A direction pools
  // route variants, so a union stop list can otherwise offer a pair no single
  // bus runs — which answers "no bus we tracked made that trip".
  const narrowing = await page.evaluate(`
    (() => {
      const from = document.getElementById("journey-times-from");
      const to = document.getElementById("journey-times-to");
      const before = to.options.length;
      // Move From to the far end of the line: almost nothing follows it.
      from.value = from.options[from.options.length - 1].value;
      from.dispatchEvent(new Event("change", { bubbles: true }));
      return JSON.stringify({ before, fromCount: from.options.length });
    })()`);
  const nn = JSON.parse(narrowing);
  await sleep(1800);
  const onward = await page.evaluate(`
    (() => {
      const to = document.getElementById("journey-times-to");
      return JSON.stringify({
        after: to.options.length,
        counted: [...to.options].every(o => / \\d+ journeys?$/.test(o.text)),
      });
    })()`);
  const aa = JSON.parse(onward);
  check("the far end of the line offers fewer onward stops",
    aa.after < nn.before,
    `${nn.before} stops from the first, ${aa.after} from the last of ${nn.fromCount}`);
  check("each onward stop says how many journeys make the trip",
    aa.after === 0 || aa.counted,
    "a pair served twice a week looks like one served every ten minutes");

  // A service number is not a route. The 1, the 5 and the 7 are each run by
  // two operators over roads sharing not one stop, so the picker has to say
  // whose bus it is — otherwise two different services answer to one entry and
  // the stop list is the union of both.
  const services = await page.evaluate(`
    (() => {
      const sel = document.getElementById("journey-times-service");
      const opts = [...sel.options];
      const labels = opts.map(o => o.text);
      // Same number appearing twice is the case that matters; each must then
      // name a different operator.
      const numbers = labels.map(t => (t.match(/Service (\\S+)/) || [])[1]);
      const dupes = numbers.filter((n, i) => numbers.indexOf(n) !== i);
      return JSON.stringify({
        count: opts.length,
        labels: labels.slice(0, 3),
        sharedNumbers: [...new Set(dupes)],
        // Ignoring the journey count, which differs between two operators'
        // routes anyway and so made this pass with the operator stripped out
        // — distinct by accident is not distinct.
        allDistinct: new Set(labels.map(t => t.replace(/\\s*\\(\\d+ journeys\\)$/, "")))
          .size === labels.length,
        valuesAreFiles: opts.every(o => /\\.json$/.test(o.value)),
      });
    })()`);
  const sv = JSON.parse(services);
  check("every service entry is distinct", sv.allDistinct,
    `${sv.count} entries, first: ${sv.labels.join(" | ")}`);
  check("a service picker entry names the operator", sv.valuesAreFiles
    && sv.labels.every(t => /·/.test(t) || !sv.sharedNumbers.length),
    `shared numbers: ${sv.sharedNumbers.join(", ") || "none in this data"}`);

  // Direction named by headsign, not by compass. "towards Worthing" appeared
  // on services that have never been near Worthing.
  check("directions are named as the bus names them", d.directions > 0 && /^Towards /.test(d.headsign),
    `first direction offered: ${d.headsign}`);

  // The map as a way in. The selects stay — this is the second route, not the
  // only one — but a reader picking two stops on a coast road is doing a
  // spatial task with a list.
  const picked = await page.evaluate(`
    (() => {
      const before = document.getElementById("journey-times-from").value;
      // Deliberately not either end. Clicking an end is defined as "start
      // again from here", so it leaves the selection where it is — a check
      // that clicked the first marker in the DOM tested that the view does
      // nothing, and passed.
      const marker = document.querySelector(
        ".jt-stop:not(.jt-stop--from):not(.jt-stop--to)");
      const offered = document.querySelectorAll(".jt-stop").length;
      const wasEnds = document.querySelectorAll(
        ".jt-stop--from, .jt-stop--to").length;
      const mode = jtEntry.mode;
      const to = document.getElementById("journey-times-to").options.length;
      // How many of the chosen ends the map is able to show. Montreal Way is
      // the 700's own Durrington terminus and sits twenty metres outside the
      // bounding box this map draws, so a pair ending there can only ever mark
      // one end — and the panel has to say so.
      const offmap = (jtPick.unplaceable || []).length;
      if (!marker) return JSON.stringify({ offered, wasEnds, mode, to, offmap });
      // dispatchEvent, not .click(): these are SVG circles and .click() throws.
      marker.dispatchEvent(new MouseEvent("click",
        { bubbles: true, cancelable: true, view: window }));
      return JSON.stringify({
        offered, wasEnds, mode, to,
        offmap: (jtPick.unplaceable || []).length,
        placeable: jtPick.placeable,
        said: !!document.querySelector(".jt-offmap"),
        ends: document.querySelectorAll(".jt-stop--from, .jt-stop--to").length,
        before,
      });
    })()`);
  const m = JSON.parse(picked);
  check("the service's stops are offered on the map", m.offered > 0,
    "the markers already exist and join by ATCO; leaving them inert makes the "
    + "select the only way to name a stop");
  if (!m.offered) return;
  // Every chosen end the map *can* show must be marked. Asserting a flat two
  // was the wrong invariant and hid a real gap: Montreal Way, the 700's own
  // Durrington terminus, is in the published document and twenty metres outside
  // the bounding box the map draws, so a pair ending there can never mark both.
  // The right requirement is that the map marks what it can and the panel says
  // what it cannot.
  check("every chosen end the map can show is marked on it",
    m.ends === m.placeable,
    `${m.ends} marked, ${m.placeable} placeable, ${m.offmap} off-map `
    + `(before the click: ${m.wasEnds} `
    + `of ${m.offered} markers, mode=${m.mode}, To offered ${m.to})`);
  check("a chosen stop the map cannot show is admitted in the panel",
    !m.offmap || m.said,
    "one end silently vanished from the map with nothing to explain it");

  await sleep(1500);
  const after = await page.evaluate(
    `JSON.stringify({ from: document.getElementById("journey-times-from").value,
                      dots: document.querySelectorAll(".jt-dot").length })`);
  const a = JSON.parse(after);
  check("clicking a stop on the map changes the charted pair",
    a.from !== m.before, `from ${m.before} to ${a.from}`);
  check("the chart survives being picked on the map", a.dots > 0,
    "the pair changed and nothing was drawn for it");
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
if (process.argv.includes("--journey-review")) {
  await checkJourneyCompactFile(page);
  await checkJourneyPairService(page, VIEWPORTS[0].name);
  await checkJourneyBar(page, VIEWPORTS[0].name);
  await checkJourneyKept(page, VIEWPORTS[0].name);
  await checkJourneyReview(page, VIEWPORTS[0].name);
  await checkSelectedFareRoute(page);
  await checkJourneySimple(page, VIEWPORTS[0].name);
  await checkDelayMap(page, VIEWPORTS[0].name);
  for (const vp of VIEWPORTS.slice(1)) {
    const p = await openPage(vp);
    await checkJourneyPairService(p, vp.name);
    await checkJourneyBar(p, vp.name);
    await checkJourneyKept(p, vp.name);
    await checkJourneyReview(p, vp.name);
    await checkJourneySimple(p, vp.name);
    await checkDelayMap(p, vp.name);
    await screenshot(p, `journey-review-${vp.name}`);
    p.ws.close();
  }
  for (const r of results) console.log(`${r.pass ? "PASS" : "FAIL"} ${r.name} ${r.detail || ""}`);
  console.log(`${results.filter(r => r.pass).length}/${results.length} checks passed`);
  page.ws.close();
  process.exit(results.some(r => !r.pass) ? 1 : 0);
}
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
await checkObjectiveLead(page, VIEWPORTS[0].name);
await checkBoundaryLiveButton(page, VIEWPORTS[0].name);
await checkChipPriority(page, VIEWPORTS[0].name);
await checkLastBusHome(page, VIEWPORTS[0].name);
await checkGapMonitor(page, VIEWPORTS[0].name);
await checkBusJourney(page, VIEWPORTS[0].name);
await checkStopBoardPolish(page, VIEWPORTS[0].name);
await checkUpcomingStops(page, VIEWPORTS[0].name);
await checkA11yMenu(page, VIEWPORTS[0].name, { desktop: false });
await checkLargestText(page, VIEWPORTS[0].name);
await checkCvdContrast(page, VIEWPORTS[0].name);
await checkA11yPersists(page, VIEWPORTS[0].name);
await checkBusSelectionReveals(page, VIEWPORTS[0].name);
await checkDepartureBoard(page);
await checkViews(page);
await checkReachableAcrossViews(page, VIEWPORTS[0].name);
await checkInteractiveSurfaces(page);
await checkCouncillorContact(page);
await checkJourneyPresets(page);
await checkJourneyTimes(page);
await checkSelectedFareRoute(page);
await checkJourneyCompactFile(page);
await checkJourneyPairService(page, VIEWPORTS[0].name);
await checkJourneyBar(page, VIEWPORTS[0].name);
await checkJourneyKept(page, VIEWPORTS[0].name);
await checkJourneyReview(page, VIEWPORTS[0].name);
await checkJourneySimple(page, VIEWPORTS[0].name);
await checkDelayMap(page, VIEWPORTS[0].name);
await shootThemes(page);
await checkPanelCollapse(page);   // must stay last — see the note on the function
await checkDeepLinkIndependence();   // own page + request interception; keep it apart
await checkStaticPages();           // own pages: about, privacy, terms

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
  await checkGapMonitor(p, vp.name);
  await checkStopBoardPolish(p, vp.name);
  await checkUpcomingStops(p, vp.name);
  await checkContrastBothThemes(p, vp.name);
  await checkLargestText(p, vp.name);
  if (vp.name === "desktop") await checkA11yMenu(p, vp.name, { desktop: true });
  if (vp.name === "desktop") await checkReadingLayout(p);
  await checkReachableAcrossViews(p, vp.name);
  await checkJourneyPairService(p, vp.name);
  await checkJourneyBar(p, vp.name);
  await checkJourneyKept(p, vp.name);
  await checkJourneyReview(p, vp.name);
  await checkJourneySimple(p, vp.name);
  await checkDelayMap(p, vp.name);
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
