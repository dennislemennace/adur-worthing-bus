/**
 * TransportAPI's predictions, sent by the API so they can be scored beside
 * our own estimates (api/prediction_log.py, scripts/score_served_estimates.py).
 *
 * Server to server, so it is not fenced by origin like /submit: it is fenced
 * by a shared secret instead (PREDICTION_LOG_TOKEN, set with `wrangler secret
 * put`), and answers 404 when that is unset, so the route does not exist until
 * someone turns it on. Rows are checked field by field and stored beside the
 * recorder's snapshots, under the same byte budget and the same pruning
 * (`predlog/<day>/` sorts and prunes by day like `raw/` and `rt/`).
 */

import { readUsage, withinBudget } from "./recorder.js";

const MAX_BODY_BYTES = 512 * 1024;
const MAX_ROWS = 2000;
const LONDON_DATE = new Intl.DateTimeFormat("en-CA", {
  timeZone: "Europe/London", year: "numeric", month: "2-digit", day: "2-digit",
});
const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)$/;

/** Constant time, so the token cannot be guessed a character at a time. */
function sameSecret(a, b) {
  if (typeof a !== "string" || typeof b !== "string" || a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i += 1) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

/** A row as the API sends it, or null: nothing else is stored. */
export function cleanRow(r) {
  if (!r || typeof r !== "object" || Array.isArray(r)) return null;
  const { fetched_at, stop, trip_id, aimed, expected, cancelled } = r;
  if (!Number.isInteger(fetched_at) || fetched_at < 1_700_000_000 || fetched_at > 4_000_000_000) return null;
  if (typeof stop !== "string" || !/^[A-Za-z0-9]{1,20}$/.test(stop)) return null;
  if (typeof trip_id !== "string" || !/^[A-Za-z0-9_:.-]{1,100}$/.test(trip_id)) return null;
  if (typeof aimed !== "string" || !ISO.test(aimed)) return null;
  if (expected !== null && (typeof expected !== "string" || !ISO.test(expected))) return null;
  if (typeof cancelled !== "boolean") return null;
  return { fetched_at, stop, trip_id, aimed, expected, cancelled };
}

export function logKey(when, rand = Math.random().toString(36).slice(2, 10)) {
  return `predlog/${LONDON_DATE.format(when)}/${when.getTime()}-${rand}.json`;
}

const reply = (body, status) => new Response(JSON.stringify(body), {
  status, headers: { "Content-Type": "application/json" },
});

export async function predictionLog(request, env, when = new Date()) {
  if (!env.PREDICTION_LOG_TOKEN) return reply({ ok: false, error: "Not found" }, 404);
  if (request.method !== "POST") return reply({ ok: false, error: "Method not allowed" }, 405);
  const auth = request.headers.get("Authorization") || "";
  if (!sameSecret(auth, `Bearer ${env.PREDICTION_LOG_TOKEN}`)) return reply({ ok: false, error: "Forbidden" }, 403);
  const length = Number(request.headers.get("Content-Length") || 0);
  if (length > MAX_BODY_BYTES) return reply({ ok: false, error: "Too large" }, 413);
  const text = await request.text();
  if (text.length > MAX_BODY_BYTES) return reply({ ok: false, error: "Too large" }, 413);
  let payload;
  try { payload = JSON.parse(text); } catch { return reply({ ok: false, error: "Malformed" }, 400); }
  const rows = payload && Array.isArray(payload.rows) ? payload.rows : null;
  if (!rows || rows.length > MAX_ROWS) return reply({ ok: false, error: "Malformed" }, 400);
  const clean = rows.map(cleanRow).filter(Boolean);
  if (!clean.length) return reply({ ok: true, stored: 0 }, 200);
  if (!env.SNAPSHOTS) return reply({ ok: false, error: "No storage" }, 503);
  const budget = withinBudget(await readUsage(env), when);
  if (!budget.ok) return reply({ ok: false, error: budget.reason }, 507);
  await env.SNAPSHOTS.put(logKey(when), clean.map(r => JSON.stringify(r)).join("\n") + "\n",
    { httpMetadata: { contentType: "application/x-ndjson" } });
  return reply({ ok: true, stored: clean.length, rejected: rows.length - clean.length }, 200);
}
