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
  const sel = ".header-title, .header-subtitle, .section-nav-label," +
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

// ── Run ─────────────────────────────────────────────────────

try {
  await fetch(`${CDP}/json/version`);
} catch {
  console.error(`No Chrome debugger at ${CDP}. See the usage note at the top of this file.`);
  process.exit(2);
}

const page = await openPage(VIEWPORTS[0]);
await checkBasemap(page);
await checkLayout(page, "live view");
await checkDepartureBoard(page);
await checkViews(page);
await shootThemes(page);
await checkPanelCollapse(page);   // must stay last — see the note on the function

// The other two viewports get the layout assertions whether or not
// screenshots were asked for — an assertion that only runs with --shots is
// one that will not run in CI.
for (const vp of VIEWPORTS.slice(1)) {
  const p = await openPage(vp);
  await screenshot(p, `live-${vp.name}`);
  await checkLayout(p, vp.name);
  await checkContrastBothThemes(p, vp.name);
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
