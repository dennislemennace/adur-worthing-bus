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

// The same service's other feed. SIRI-VM gives positions and the operator's own
// journey number, which matches no timetable trip here — 0 of 256 in a live
// sample — so every journey has to be inferred from position and time. GTFS-RT
// states `trip_id` and `current_stop_sequence` outright. If ours carries them,
// the inference and the caveats it drags with it can go.
const BODS_GTFS_RT = "https://data.bus-data.dft.gov.uk/api/v1/gtfsrtdatafeed/";

// The same box the API watches: Adur, Worthing and the Brighton coast.
const BBOX = "-0.42,50.78,-0.10,50.87";

// London time, for the SIRI feed only. Its 285 KB a minute is worth spending
// only while buses are running.
const RECORD_FROM = "05:00";
const RECORD_TO = "00:30";

// GTFS-RT runs all night. At 34 KB a minute — an eighth of SIRI — the whole
// 24 hours costs 49 MB a day, and it is the feed that names the journey. The
// night services this project argues about (N7, N12, N25, N29, N700) run past
// 00:30 and were simply not recorded before.

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
// Seven days of both feeds. Measured rather than guessed: 285 KB of SIRI XML
// and 34 KB of GTFS-RT a minute is ~330 MB a day, so a week is ~2.3 GB against
// the 4 GB budget below. The estimate this replaces (540 MB a day, four days)
// assumed protobuf would be the larger of the two; it is an eighth of the size.
const RETENTION_DAYS = 7;

// What a week of recording actually comes to, so the ceiling below is derived
// rather than remembered: GTFS-RT every minute of the day, SIRI every minute
// of its shorter window.
const RT_OBJECTS_PER_DAY = 24 * 60;
const SIRI_OBJECTS_PER_DAY = 19.5 * 60;
const MAX_STORED_BYTES = 4 * 1024 * 1024 * 1024; // 40% of the 10 GB free tier
// The object ceiling exists to catch a runaway loop, not to control cost —
// bytes do that. Sized at 15,000 for one feed, it became a scheduled outage
// the moment a second arrived: a week of both is 16,380 objects, so recording
// would have refused itself mid-week. Now it is a generous multiple of what a
// full retention window holds, and a test derives that figure rather than
// trusting this comment.
const MAX_STORED_OBJECTS = 30_000;
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

/** GTFS-RT is recorded around the clock — 34 KB a minute buys the night
 *  services, which are exactly the ones a reader asks about and the ones the
 *  SIRI window threw away. Takes `when` so that narrowing it later is a change
 *  to one function rather than to the caller. */
export function isJourneyFeedTime(_when) {
  return true;
}

/** Where a snapshot lives: one folder a day, one object a minute, so a day can
 *  be listed and deleted by prefix once it has been processed. */
export function snapshotKey(when) {
  return `raw/${LONDON_DATE.format(when)}/${LONDON_CLOCK.format(when).replace(":", "")}-${Math.floor(when.getTime() / 60000) * 60}.xml`;
}

/** The same minute's GTFS-RT, beside it. */
export function gtfsRtKey(when) {
  return `rt/${LONDON_DATE.format(when)}/${LONDON_CLOCK.format(when).replace(":", "")}-${Math.floor(when.getTime() / 60000) * 60}.pb`;
}

/** The service day a key belongs to: "raw/2026-09-18/0740.xml" → "2026-09-18".
 *  Both feeds are laid out the same way, so pruning reads either. */
export function keyDay(key) {
  const parts = key.split("/");
  return parts.length > 2 ? parts[1] : "";
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
    // Every prefix, not just raw/. The second feed lives under rt/, and a
    // prune that only knew about the first would leave it growing unmeasured
    // and unbilled-for until it was billed for.
    const page = await env.SNAPSHOTS.list({ cursor, limit: 1000 });
    const stale = [];
    for (const obj of page.objects) {
      const day = keyDay(obj.key);
      if (day && day < oldestKept) stale.push(obj.key);
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

  // The journey feed runs all night, and never blocks the other: a GTFS-RT
  // outage must not cost us the positions we already know how to use. The
  // window is asked for rather than assumed, so that the two feeds' hours stay
  // two separate decisions — while they were one decision, the night services
  // this project argues about were the ones missing from the evidence.
  const rt = isJourneyFeedTime(when)
    ? await storeFeed(env, fetchImpl, gtfsRtKey(when), when,
                      `${BODS_GTFS_RT}?api_key=${encodeURIComponent(env.BODS_API_KEY)}`
                      + `&boundingBox=${BBOX}`, "application/x-protobuf")
    : { stored: false, reason: "out_of_window" };

  // SIRI is the expensive one — 285 KB against 34 KB — so it keeps to the
  // hours buses run.
  if (!isRecordingTime(when)) {
    return { recorded: false, reason: "quiet_hours", rt };
  }

  const key = snapshotKey(when);
  const url = `${BODS_FEED}?api_key=${encodeURIComponent(env.BODS_API_KEY)}&boundingBox=${BBOX}`;
  const started = Date.now();
  try {
    const res = await fetchImpl(url, { signal: AbortSignal.timeout(45_000) });
    if (!res.ok) {
      console.error(`snapshot ${key}: feed returned HTTP ${res.status}`);
      return { recorded: false, reason: "upstream", status: res.status, key, rt };
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
    console.log(`snapshot ${key}: ${Math.round(body.byteLength / 1024)} KB in ${Date.now() - started} ms`
                + (rt.stored ? `, rt ${Math.round(rt.bytes / 1024)} KB` : `, rt ${rt.reason}`));
    return { recorded: true, key, bytes: body.byteLength, rt };
  } catch (err) {
    console.error(`snapshot ${key}: ${err && err.message}`);
    return { recorded: false, reason: "error", error: String(err && err.message), key, rt };
  }
}

/**
 * Fetch one feed and store it. Never throws, and never reports a failure as a
 * stored object: a feed that answers with an error page is not a snapshot.
 */
async function storeFeed(env, fetchImpl, key, when, url, contentType) {
  try {
    const res = await fetchImpl(url, { signal: AbortSignal.timeout(45_000) });
    if (!res.ok) {
      console.error(`snapshot ${key}: feed returned HTTP ${res.status}`);
      return { stored: false, reason: `http_${res.status}` };
    }
    const body = await res.arrayBuffer();
    await env.SNAPSHOTS.put(key, body, {
      httpMetadata: { contentType },
      customMetadata: { recordedAt: when.toISOString() },
    });
    return { stored: true, bytes: body.byteLength, key };
  } catch (err) {
    console.error(`snapshot ${key}: ${err && err.message}`);
    return { stored: false, reason: "error", error: String(err && err.message) };
  }
}

export const _internals = {
  isRecordingTime, isJourneyFeedTime, snapshotKey, gtfsRtKey, keyDay,
  pruneAndMeasure, withinBudget, readUsage,
  RECORD_FROM, RECORD_TO, BBOX,
  RETENTION_DAYS, MAX_STORED_BYTES, MAX_STORED_OBJECTS, USAGE_REFRESH_MINUTE, USAGE_KEY,
  RT_OBJECTS_PER_DAY, SIRI_OBJECTS_PER_DAY,
};
