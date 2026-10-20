/**
 * Where the recorder's requests leave from.
 *
 * Cloudflare runs a cron wherever it has room, and from the evening of
 * 17 October 2026 that was Singapore, which BODS (behind Amazon CloudFront)
 * refused with HTTP 403 and CloudFront's "request could not be satisfied"
 * page, every minute, for both feeds. The same key fetched the same URL from
 * London with HTTP 200, and GitHub's runners still downloaded timetables from
 * the same host: the refusal was about where the request came from.
 *
 * A cron cannot be placed; a Durable Object can. So the recorder's two
 * requests go through this one, created in Western Europe. It does nothing
 * but fetch the feed: it is reachable only through the Worker's own binding,
 * and refuses any other host, so it cannot become an open proxy.
 *
 * A plain class rather than `extends DurableObject`: it only answers fetch,
 * and the tests run under Node, where `cloudflare:workers` does not exist.
 */

export const RELAY_NAME = "bods-feed";
export const RELAY_LOCATION = "weur";
const FEED_HOST = "data.bus-data.dft.gov.uk";
const FEED_TIMEOUT_MS = 45_000;

export class FeedRelay {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.protocol !== "https:" || url.hostname !== FEED_HOST) {
      return new Response("not the feed", { status: 400 });
    }
    return fetch(url.toString(), { signal: AbortSignal.timeout(FEED_TIMEOUT_MS) });
  }
}

/** The fetch the recorder uses: through the relay where one is bound,
 *  otherwise straight from wherever this runs, as before. The relay's own
 *  timeout stands in for the caller's signal, which cannot cross to it. */
export function feedFetch(env) {
  if (!env.FEED_RELAY) return (url, init) => fetch(url, init);
  const stub = env.FEED_RELAY.get(env.FEED_RELAY.idFromName(RELAY_NAME),
                                  { locationHint: RELAY_LOCATION });
  return (url) => stub.fetch(String(url));
}
