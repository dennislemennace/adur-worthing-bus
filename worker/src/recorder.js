/**
 * Keep what the live feed says, so the site can measure what actually ran.
 *
 * Everything this project shows is live and then gone: positions are drawn on
 * the map and discarded, so the published evidence can only ever describe the
 * timetable. That leaves the obvious rebuttal ("the timetable is not the
 * problem") unanswerable. A minute-by-minute record of where the buses were
 * makes punctuality, missing journeys and bunching measurable.
 *
 * **Why the Worker and not GitHub Actions.** Sampling has to be regular or the
 * gaps bias every figure. GitHub treats schedules as best effort: 13 runs in 38
 * hours, measured. Cloudflare's cron fires on time.
 *
 * **Why the raw XML, unparsed.** The feed carries fields the API throws away,
 * including each operator's own journey number, which is what lets a vehicle's
 * day be cut into separate journeys later. Parsing here would bake today's
 * questions into the record: the bytes go to R2 as they arrived.
 *
 * Snapshots are working material, not the evidence. The nightly processor turns
 * them into arrival observations and deletes what it has used.
 */

const BODS_FEED = "https://data.bus-data.dft.gov.uk/api/v1/datafeed/";

// The same box the API watches: Adur, Worthing and the Brighton coast.
const BBOX = "-0.42,50.78,-0.10,50.87";

// London time. Buses run from about 05:00, and the last night bus is off the
// road by 00:30; recording the quiet hours would be storage spent on nothing.
const RECORD_FROM = "05:00";
const RECORD_TO = "00:30";

// ── Staying inside R2's free tier ────────────────────────────
// R2 bills automatically past its free allowances and offers no "stop when the
// free tier runs out" switch, so the ceiling has to be enforced here.
//
// Operations cannot be what breaks it: one write a minute is at most 44,640 a
// month against a million free, and every delete and list is bounded by those
// writes. Storage is the real risk. A day of snapshots is roughly 350 MB, which
// is harmless while the nightly processor deletes what it has used, and becomes
// a bill if the processor ever stops: a month of failure reaches the free 10 GB.
//
// So the recorder does not trust the processor. It prunes on its own schedule,
// and refuses to write at all once the bucket passes a budget set well below
// the free allowance.
const RETENTION_DAYS = 7;                        // ~2.5 GB at the observed size
const MAX_STORED_BYTES = 4 * 1024 * 1024 * 1024; // 40% of the 10 GB free tier
const MAX_STORED_OBJECTS = 15_000;               // ~13 days of minutes; catches a runaway
const USAGE_KEY = "r2-usage";                    // cached in the KV the Worker already binds
// The budget is read from KV every minute (~1,400 reads a day, inside the free
// 100,000) but written only when the bucket is measured, once an hour, because
// KV allows 1,000 writes a day and the submission counters draw on the same.
const USAGE_REFRESH_MINUTE = 7;

const LONDON_CLOCK = new Intl.DateTimeFormat("en-GB", {
  timeZone: "Europe/London", hour: "2-digit", minute: "2-digit", hourCycle: "h23",
});

const LONDON_DATE = new Intl.DateTimeFormat("en-CA", {
  timeZone: "Europe/London", year: "numeric", month: "2-digit", day: "2-digit",
});

/** Whether a snapshot is worth taking at this moment. The window crosses
 *  midnight, so it is written as "not inside the quiet hours": a
 *  from < now < to test across midnight is never true. */
export function isRecordingTime(when) {
  const now = LONDON_CLOCK.format(when);
  return !(now >= RECORD_TO && now < RECORD_FROM);
}

/** Where a snapshot lives: one folder a day, one object a minute, so a day can
 *  be listed and deleted by prefix once it has been processed. */
export function snapshotKey(when) {
  return `raw/${LONDON_DATE.format(when)}/${LONDON_CLOCK.format(when).replace(":", "")}.xml`;
}

/** What the bucket held when it was last measured, or null if it never has
 *  been. Never throws: a KV outage must not stop recording. */
export async function readUsage(env) {
  if (!env.RATE_LIMIT) return null;
  try {
    const raw = await env.RATE_LIMIT.get(USAGE_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

/** Whether the bucket is inside its budget. Only a *measured* overrun stops
 *  recording: an unmeasured bucket is not assumed to be full, since the hourly
 *  measurement will correct it within the hour and long before the free tier. */
export function withinBudget(usage) {
  if (!usage) return { ok: true, reason: "unmeasured" };
  if (usage.bytes >= MAX_STORED_BYTES) return { ok: false, reason: "byte_budget" };
  if (usage.objects >= MAX_STORED_OBJECTS) return { ok: false, reason: "object_budget" };
  return { ok: true };
}

/**
 * Measure what is stored, and delete anything past the retention window.
 *
 * This is what makes an unattended failure safe: if the nightly processor stops,
 * storage still cannot climb past about a week of snapshots. Days are dropped
 * whole, which is why the key starts with the date.
 *
 * Listing and deleting are Class A operations like the writes they follow, and
 * both are bounded by them: a day's 1,170 objects list in two pages.
 */
export async function pruneAndMeasure(env, when) {
  if (!env.SNAPSHOTS) return { measured: false, reason: "no_bucket" };
  const oldestKept = LONDON_DATE.format(new Date(when.getTime() - RETENTION_DAYS * 86_400_000));
  let bytes = 0, objects = 0, deleted = 0, cursor;
  do {
    const page = await env.SNAPSHOTS.list({ prefix: "raw/", cursor, limit: 1000 });
    const stale = [];
    for (const obj of page.objects) {
      // "raw/2026-09-16/1245.xml" → "2026-09-16", which compares as a date.
      const day = obj.key.slice(4, 14);
      if (day < oldestKept) stale.push(obj.key);
      else { bytes += obj.size || 0; objects += 1; }
    }
    if (stale.length) {
      await env.SNAPSHOTS.delete(stale);
      deleted += stale.length;
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);

  const usage = { bytes, objects, deleted, measured_at: when.toISOString() };
  if (env.RATE_LIMIT) {
    try {
      await env.RATE_LIMIT.put(USAGE_KEY, JSON.stringify(usage));
    } catch (err) {
      console.error(`snapshot usage not cached: ${err && err.message}`);
    }
  }
  console.log(`snapshots: ${objects} kept, ${(bytes / 1e6).toFixed(0)} MB, ${deleted} pruned`);
  return { measured: true, ...usage };
}

/**
 * Take one snapshot. Never throws: a scheduled handler that rejects shows as a
 * failed invocation and nothing else, and a missed minute is not worth waking
 * anyone for. The result is returned so the log says what happened.
 */
export async function recordSnapshot(env, when, fetchImpl = fetch) {
  if (!isRecordingTime(when)) return { recorded: false, reason: "quiet_hours" };
  if (!env.SNAPSHOTS) return { recorded: false, reason: "no_bucket" };
  if (!env.BODS_API_KEY) return { recorded: false, reason: "no_key" };

  // Once an hour, measure the bucket and drop expired days before deciding.
  if (when.getUTCMinutes() === USAGE_REFRESH_MINUTE) {
    try {
      await pruneAndMeasure(env, when);
    } catch (err) {
      console.error(`snapshot prune failed: ${err && err.message}`);
    }
  }
  const budget = withinBudget(await readUsage(env));
  if (!budget.ok) {
    console.error(`snapshots paused: ${budget.reason}; the processor is not clearing the bucket`);
    return { recorded: false, reason: budget.reason };
  }

  const key = snapshotKey(when);
  const url = `${BODS_FEED}?api_key=${encodeURIComponent(env.BODS_API_KEY)}&boundingBox=${BBOX}`;
  const started = Date.now();
  try {
    const res = await fetchImpl(url, { signal: AbortSignal.timeout(45_000) });
    if (!res.ok) {
      console.error(`snapshot ${key}: feed returned HTTP ${res.status}`);
      return { recorded: false, reason: "upstream", status: res.status, key };
    }
    // The bytes are read before they are stored, which looks wasteful and is
    // not optional: R2 rejects a stream whose length it cannot know, and the
    // feed answers chunked, so piping res.body straight in fails every time
    // with "Provided readable stream must have a known length". Decoding to
    // text would cost more; an ArrayBuffer is a copy, not a parse. The feed is
    // ~300 KB against the Worker's 128 MB, but this is the one place that grows
    // with the feed, so the size is logged to make growth visible.
    const body = await res.arrayBuffer();
    await env.SNAPSHOTS.put(key, body, {
      httpMetadata: { contentType: "application/xml" },
      customMetadata: { recordedAt: when.toISOString() },
    });
    console.log(`snapshot ${key}: ${Math.round(body.byteLength / 1024)} KB in ${Date.now() - started} ms`);
    return { recorded: true, key, bytes: body.byteLength };
  } catch (err) {
    console.error(`snapshot ${key}: ${err && err.message}`);
    return { recorded: false, reason: "error", error: String(err && err.message), key };
  }
}

export const _internals = {
  isRecordingTime, snapshotKey, pruneAndMeasure, withinBudget, readUsage,
  RECORD_FROM, RECORD_TO, BBOX,
  RETENTION_DAYS, MAX_STORED_BYTES, MAX_STORED_OBJECTS, USAGE_REFRESH_MINUTE, USAGE_KEY,
};
