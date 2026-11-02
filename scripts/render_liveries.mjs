// Draw the livery patterns of scripts/livery_designs.py to flat PNGs, one per
// livery, for scripts/build_icons.py to repaint the masters in.
//
//   python scripts/livery_designs.py --jobs > /tmp/jobs.json
//   node scripts/render_liveries.mjs /tmp/jobs.json icons/source/liveries
//
// Needs headless Chrome on CDP_URL (default http://127.0.0.1:9222), as
// scripts/browser_check.mjs does. Only needed when a design changes: the
// PNGs are committed, and building the icons needs no browser.
import fs from "node:fs";

const CDP = process.env.CDP_URL || "http://127.0.0.1:9222";
const [jobsPath, outDir] = process.argv.slice(2);
if (!jobsPath || !outDir) {
  console.error("usage: node scripts/render_liveries.mjs jobs.json outdir");
  process.exit(2);
}
const jobs = JSON.parse(fs.readFileSync(jobsPath, "utf8"));
fs.mkdirSync(outDir, { recursive: true });

const target = await (await fetch(`${CDP}/json/new?about:blank`, { method: "PUT" })).json();
const ws = new WebSocket(target.webSocketDebuggerUrl);
await new Promise(r => ws.addEventListener("open", r));
let id = 0;
const pending = new Map();
ws.addEventListener("message", e => {
  const m = JSON.parse(e.data);
  if (m.id && pending.has(m.id)) { pending.get(m.id)(m); pending.delete(m.id); }
});
const send = (method, params = {}) => new Promise((res, rej) => {
  const i = ++id;
  pending.set(i, m => m.error ? rej(new Error(m.error.message)) : res(m.result));
  ws.send(JSON.stringify({ id: i, method, params }));
});
const run = async expression => {
  const r = await send("Runtime.evaluate", { expression, returnByValue: true });
  if (r.exceptionDetails) throw new Error(r.exceptionDetails.text);
  return r.result.value;
};

await send("Emulation.setDeviceMetricsOverride", { width: 1600, height: 1200, deviceScaleFactor: 1, mobile: false });
await run(`document.body.style.margin='0';document.body.innerHTML='<div id=b></div>';1`);
let failed = 0;
for (const j of jobs) {
  const ok = await run(`(() => {
    const b = document.getElementById('b');
    b.removeAttribute('style'); b.innerHTML = '';
    b.style.width = '${j.w}px'; b.style.height = '${j.h}px'; b.style.lineHeight = '0';
    ${j.svg ? `b.innerHTML = ${JSON.stringify(j.svg)}; return !!b.querySelector('svg');`
            : `b.style.background = ${JSON.stringify(j.css)}; return b.style.background !== '';`}
  })()`);
  if (!ok) { console.error(`${j.key}: the browser rejected its pattern`); failed++; continue; }
  const shot = await send("Page.captureScreenshot", {
    format: "png", clip: { x: 0, y: 0, width: j.w, height: j.h, scale: 1 }, omitBackground: true,
  });
  fs.writeFileSync(`${outDir}/${j.key}.png`, Buffer.from(shot.data, "base64"));
}
await send("Target.closeTarget", { targetId: target.id }).catch(() => {});
ws.close();
console.log(`${jobs.length - failed} of ${jobs.length} patterns drawn to ${outDir}`);
process.exit(failed ? 1 : 0);
