/* The tour as slides: tour.html.
 *
 * Presentation on a laptop or a big screen (one 16:9 slide at a time, arrow
 * keys, F for full screen), all the slides one under another on a phone, and
 * the same slides one to a page when printed or saved as a PDF. The words are
 * data/tour.json's, the numbers tour-figures.js's, so the slides say what the
 * tour in the app says.
 *
 * Self-contained, like figures.html: no app.js, no map. Reads the published
 * files directly.
 */
"use strict";

const TOUR_PUBLISHED = "https://adur-worthing-submissions.dennislemennace.workers.dev/journey-times";
const TOUR_QR = "media/tour/qr-tour.svg";

function tourEsc(value) {
  return String(value == null ? "" : value).replace(/[&<>"']/g, ch =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch]);
}

/** A link only for addresses that cannot run script: https, mailto, or a
 *  file on this site. Anything else is shown as plain text. */
function tourSafeHref(url) {
  const u = String(url || "");
  if (/^https:\/\//i.test(u) || /^mailto:/i.test(u)) return u;
  return /^[a-z0-9_./-]+(#[a-z0-9_-]*)?$/i.test(u) ? u : "";
}

function tourSlideText(text, values) {
  return tourEsc(tourFill(text, values));
}

function tourQrHtml(caption) {
  return `<figure class="slide-qr"><img src="${TOUR_QR}" alt="QR code for the tour on worthingbrightonbus.co.uk">
    <figcaption>${tourEsc(caption)}</figcaption></figure>`;
}

function tourSlideSources(sources) {
  const shown = (sources || []).map(s => {
    const href = tourSafeHref(s.url);
    const label = tourEsc(s.label);
    return href ? `<a href="${tourEsc(href)}">${label}</a>` : label;
  });
  return shown.length ? `<p class="slide-sources">Sources: ${shown.join("; ")}.</p>` : "";
}

function tourOpeningSlide(d, values) {
  return `<section class="slide slide--opening" data-slide="opening" aria-label="The tour">
    <div class="slide-body">
      <p class="slide-kicker">Worthing Brighton Bus</p>
      <h1 class="slide-title">${tourEsc(d.opening.headline)}</h1>
      <p class="slide-lead">${tourSlideText(d.opening.lead, values)}</p>
    </div>
    <aside class="slide-side">${tourQrHtml("Try it live: worthingbrightonbus.co.uk")}</aside>
  </section>`;
}

function tourStepSlide(step, i, total, values, contact) {
  const last = i === total - 1;
  const kind = /^[a-z]+$/.test(step.badge && step.badge.kind) ? step.badge.kind : "about";
  return `<section class="slide" data-slide="${tourEsc(step.id)}" aria-label="Step ${i + 1} of ${total}">
    <div class="slide-body">
      <p class="slide-kicker">Step ${i + 1} of ${total} · <span class="slide-badge slide-badge--${kind}">${
        tourSlideText(step.badge && step.badge.label, values)}</span></p>
      <h2 class="slide-title">${tourEsc(step.title)}</h2>
      <p class="slide-lead">${tourSlideText(step.lead, values)}</p>
      ${(step.points || []).length ? `<ul class="slide-points">${step.points.map(p =>
        `<li>${tourSlideText(p, values)}</li>`).join("")}</ul>` : ""}
      ${tourSlideSources(step.sources)}
    </div>
    ${last && contact ? `<aside class="slide-side">
      <p class="slide-contact"><strong>${tourEsc(contact.name)}</strong><br>
        <a href="mailto:${tourEsc(contact.email)}">${tourEsc(contact.email)}</a><br>
        <a href="${tourEsc(tourSafeHref(contact.site))}">worthingbrightonbus.co.uk</a><br>
        <a href="${tourEsc(tourSafeHref(contact.code))}">The code on GitHub</a></p>
      ${tourQrHtml("The tour on your phone")}</aside>` : ""}
  </section>`;
}

/** Every slide, in order: the opening, then one per step. */
function tourSlidesHtml(d, values) {
  const steps = d.steps || [];
  return [tourOpeningSlide(d, values),
          ...steps.map((s, i) => tourStepSlide(s, i, steps.length, values, d.contact))].join("\n");
}

// ── The deck ────────────────────────────────────────────────────

async function tourFetchJson(url) {
  try {
    const res = await fetch(url);
    return res.ok ? await res.json() : null;
  } catch {
    return null;
  }
}

async function tourPageData() {
  const [d, evidence, index] = await Promise.all([
    tourFetchJson("data/tour.json"),
    tourFetchJson("data/boundary_evidence.json"),
    tourFetchJson(`${TOUR_PUBLISHED}/index.json`),
  ]);
  const entry = index && index.artifacts && index.artifacts["identification.json"];
  const ident = entry ? await tourFetchJson(`${TOUR_PUBLISHED}/${entry.file}`) : null;
  return { d, values: tourFigureValues(evidence, index, ident) };
}

function tourDeck(root) {
  const slides = [...root.querySelectorAll(".slide")];
  const counter = document.getElementById("deck-counter");
  const progress = document.getElementById("deck-progress");
  let at = 0;
  const show = (i, focus = false) => {
    at = Math.max(0, Math.min(slides.length - 1, i));
    slides.forEach((s, j) => s.classList.toggle("is-current", j === at));
    if (counter) counter.textContent = `${at + 1} / ${slides.length}`;
    if (progress) progress.style.width = `${((at + 1) / slides.length) * 100}%`;
    history.replaceState(null, "", `#${at + 1}`);
    if (focus) slides[at].focus({ preventScroll: true });
  };
  document.getElementById("deck-prev")?.addEventListener("click", () => show(at - 1, true));
  document.getElementById("deck-next")?.addEventListener("click", () => show(at + 1, true));
  document.getElementById("deck-full")?.addEventListener("click", () => {
    if (document.fullscreenElement) document.exitFullscreen();
    else document.documentElement.requestFullscreen?.();
  });
  document.getElementById("deck-print")?.addEventListener("click", () => window.print());
  document.addEventListener("keydown", (e) => {
    if (e.altKey || e.ctrlKey || e.metaKey) return;
    if (e.target.closest && e.target.closest("button, a")) {
      if (e.key === " " || e.key === "Enter") return;
    }
    if (["ArrowRight", "PageDown", " "].includes(e.key)) { e.preventDefault(); show(at + 1); }
    else if (["ArrowLeft", "PageUp"].includes(e.key)) { e.preventDefault(); show(at - 1); }
    else if (e.key === "Home") { e.preventDefault(); show(0); }
    else if (e.key === "End") { e.preventDefault(); show(slides.length - 1); }
    else if (e.key === "f" || e.key === "F") document.getElementById("deck-full")?.click();
  });
  const start = Number((location.hash || "").slice(1));
  show(Number.isInteger(start) && start > 0 ? start - 1 : 0);
}

async function tourPageInit() {
  const root = document.getElementById("slides");
  const { d, values } = await tourPageData();
  if (!d || !Array.isArray(d.steps)) {
    root.innerHTML = `<p class="deck-error">The tour could not be loaded. Check your connection and reload.</p>`;
    return;
  }
  // Until its copy is approved the tour is for ?preview=1 only, here as in the app.
  let preview = false;
  try { preview = new URLSearchParams(location.search).get("preview") === "1"; } catch { /* no address */ }
  if (d.preview_only && !preview) {
    root.innerHTML = `<p class="deck-error">This tour is not public yet.</p>`;
    return;
  }
  root.innerHTML = tourSlidesHtml(d, values);
  for (const s of root.querySelectorAll(".slide")) s.tabIndex = -1;
  tourDeck(root);
}

if (typeof document !== "undefined" && document.getElementById("slides")) tourPageInit();
