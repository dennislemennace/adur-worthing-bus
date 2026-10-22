/**
 * build_tour_pdf.mjs — print tour.html to the PDF the tour's opening offers.
 *
 * Run by hand after the tour's words change, not in a workflow: the output is a
 * committed file (media/tour/worthing-brighton-bus-tour.pdf), named in
 * data/tour.json under opening.modes. The slides are printed by the browser
 * from the same page a visitor sees, using tour.html's own print styles (one
 * 16:9 slide to a page), so the PDF cannot say something the site does not.
 *
 * Needs a local server for the site and a headless Chrome with remote
 * debugging, as for scripts/browser_check.mjs:
 *
 *   python -m http.server 8765 --bind 127.0.0.1 &
 *   flatpak run --share=network com.google.Chrome --headless=new --remote-debugging-port=9222 about:blank &
 *   node scripts/build_tour_pdf.mjs
 *
 * SITE_URL and CDP_URL override the defaults.
 */
import { writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SITE = process.env.SITE_URL || "http://127.0.0.1:8765/";
const CDP = process.env.CDP_URL || "http://127.0.0.1:9222";
const OUT = join(ROOT, "media", "tour", "worthing-brighton-bus-tour.pdf");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function connect(url) {
  const target = await (await fetch(`${CDP}/json/new?${encodeURIComponent(url)}`, { method: "PUT" })).json();
  const ws = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((r) => ws.addEventListener("open", r));
  let id = 0;
  const pending = new Map();
  ws.addEventListener("message", (e) => {
    const m = JSON.parse(e.data);
    if (m.id && pending.has(m.id)) { pending.get(m.id)(m); pending.delete(m.id); }
  });
  const send = (method, params = {}) => new Promise((res, rej) => {
    const myId = ++id;
    pending.set(myId, (m) => (m.error ? rej(new Error(`${method}: ${m.error.message}`)) : res(m.result)));
    ws.send(JSON.stringify({ id: myId, method, params }));
  });
  const evaluate = async (expression) => {
    const r = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    if (r.exceptionDetails) throw new Error(`${r.exceptionDetails.text} :: ${expression}`);
    return r.result.value;
  };
  return { ws, send, evaluate, close: () => ws.close() };
}

async function main() {
  const base = new URL(SITE);
  const url = `${base.origin}${base.pathname.replace(/[^/]*$/, "")}tour.html`;
  const page = await connect("about:blank");
  try {
    await page.send("Page.enable");
    await page.send("Runtime.enable");
    await page.send("Network.enable");
    await page.send("Network.setCacheDisabled", { cacheDisabled: true });
    await page.send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 900, deviceScaleFactor: 1, mobile: false });
    await page.send("Emulation.setEmulatedMedia", { features: [{ name: "prefers-color-scheme", value: "light" }] });
    await page.send("Page.navigate", { url });
    let slides = 0;
    for (let i = 0; i < 80 && slides < 2; i++) {
      await sleep(250);
      slides = await page.evaluate("document.querySelectorAll('.slide').length").catch(() => 0);
    }
    if (slides < 2) throw new Error(`tour.html did not draw its slides at ${url}`);
    // Every figure filled, the fonts in and the QR drawn before anything prints.
    await page.evaluate(`Promise.all([document.fonts.ready,
      ...[...document.images].map(i => i.complete ? null : new Promise(r => { i.onload = i.onerror = r; }))]).then(() => true)`);
    const unfilled = await page.evaluate("/\\{[a-z_]+\\}/.test(document.getElementById('slides').textContent)");
    if (unfilled) throw new Error("a placeholder is unfilled on a slide; not printing");
    const base64 = await (async () => {
      const params = { printBackground: true, preferCSSPageSize: true, displayHeaderFooter: false };
      try {
        return (await page.send("Page.printToPDF", { ...params, generateTaggedPDF: true, generateDocumentOutline: true })).data;
      } catch {
        return (await page.send("Page.printToPDF", params)).data;
      }
    })();
    mkdirSync(dirname(OUT), { recursive: true });
    const bytes = Buffer.from(base64, "base64");
    writeFileSync(OUT, bytes);
    console.log(`${OUT.slice(ROOT.length + 1)}: ${slides} slides, ${(bytes.length / 1024).toFixed(0)} KB, from ${url}`);
  } finally {
    page.close();
  }
}

main().catch((e) => { console.error(e.message || e); process.exit(1); });
