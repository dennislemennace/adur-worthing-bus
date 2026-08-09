# Adur & Worthing Live Bus Tracker

A free, open-source live bus departure board and vehicle tracking website for the **Adur & Worthing** area of West Sussex, UK.

- 🗺 **Interactive Leaflet map** showing live bus positions and stop markers  
- 🕐 **Real-time departure boards** for any stop, one click away  
- 📱 **Mobile-friendly** responsive layout  
- 🎯 **Network Objectives** tab — the objectives the network is working towards, plus a
  community ideas box anyone can use **without a GitHub account**

Data from the [Bus Open Data Service (BODS)](https://data.bus-data.dft.gov.uk/) — UK Department for Transport.

## Community ideas (Network Objectives → Ideas)

The Ideas form, the proposal editor's **Submit** button, and the departure
board's **Report an issue** button all post to a small
[Cloudflare Worker](./worker/README.md), which files each submission as a
GitHub issue. **No account is needed to submit** — and unlike an email relay,
every submission gets a public home the sender can follow, and the sender is
linked straight to it.

To switch it on, deploy the Worker (see [`worker/README.md`](./worker/README.md)
for the full runbook: KV namespace, fine-grained PAT, Turnstile keys) and paste
its URL into `CONFIG.SUBMIT_ENDPOINT` in `app.js`, along with your Turnstile
**site key** in `CONFIG.TURNSTILE_SITE_KEY`. Until the endpoint is set, the
forms show a friendly "not switched on yet" message.

Spam is held back by a honeypot, Cloudflare Turnstile, per-client rate limits
and a global daily cap. Submissions are also sanitised server-side before they
reach an issue — notably `@mentions` are defused, because an issue body is a
broadcast.

### Approving an idea

Nothing is published automatically. Submissions arrive as issues labelled
`community-submission` / `unverified`, and stay there until you publish them —
so junk never reaches the site. Each idea issue carries a ready-to-publish JSON
blob. To approve one:

```sh
# straight from the issue number:
python scripts/add_suggestion.py --from-issue 42
# …or paste the JSON blob:
python scripts/add_suggestion.py '{"title":"…","body":"…","area":"…"}'
# …or pipe it in:  wl-paste | python scripts/add_suggestion.py
# …or run with no args to type the fields in by hand
```

`--from-issue` needs the [`gh`](https://cli.github.com) CLI authenticated, and
closes the issue for you once the idea is published.

The script gives the entry a unique `id`, fills today's date, forces
`status: "published"`, appends it to `data/suggestions.json`, and re-validates
the file. Then review and publish:

```sh
git add data/suggestions.json && git commit -m "Publish community idea" && git push
```

(Pass `--commit` to stage + commit for you; it never pushes.) GitHub Pages
redeploys and the idea appears in the **Ideas** tab after a page reload.

### Other curated content

Network goals (`data/objectives.json`) are maintainer-authored — edit them by
hand and change a `status` (`not_considered` → `discussed` → `in_progress` →
`delivered`) as delivery progresses. Route proposals submitted via the editor's
**Submit** button arrive as issues labelled `proposal`, carrying a JSON block you
paste into `data/proposals.json`. Run `pytest` (see `requirements-dev.txt`) to
validate any of these files before committing.

Stop faults reported from a departure board arrive labelled `stop-issue`, with
the stop's ATCO code in the title so repeat reports about the same stop fold
into one thread. Most are council responsibilities rather than operator ones —
the issue records the report, it doesn't raise a works order.

## Ticket view: what does your journey cost?

The Ticket view can check a specific A-to-B journey against the operators' zone
maps. It asks the backend (`GET /api/journey`) for the **actual ordered stops**
between the two you pick — the endpoints alone aren't enough, because a bus can
dip through a third zone on the way — then tests every stop against every zone.

Fares live in `data/ticket_zones.json` under each zone's `fares` block, in
whole pence, each with a `source_url` and the `checked_on` date. **Prices only
appear when they can be sourced**: if any zone on a journey has no fare data,
the boundary warning still shows but the £ figures don't. `pytest` enforces the
schema, so an undated or unsourced price fails CI rather than reaching the site.

Four rules keep the numbers defensible:

- **Validity is not just geography.** Each zone lists `valid_on_operators`.
  A Stagecoach ticket isn't accepted on Metrobus or Brighton & Hove; Metrovoyager
  *is* accepted on Brighton & Hove, and networkSAVER on Metrobus.
- **Zone tickets and operator-wide tickets are judged separately.** An
  operator-wide ticket (Stagecoach Gold, networkSAVER) covers every journey on
  that operator by definition, so folding it into the same search would report
  "one ticket, no problem" for almost everything and hide the boundary.
- **The headline price is the cheapest thing you could actually buy.** On an
  all-Stagecoach Worthing–Brighton run the two zone tickets come to £12, but a
  Gold DayRider covers it for £9 — so £9 is what's quoted. Claiming £12 would be
  wrong and would discredit the point.
- **Night services are priced properly.** `service_supplements` adds the N700's
  £2 add-on, and the Discovery ticket is marked `not_valid_on_services` for the
  N700/N1, so it's never offered as a fix on a journey where it can't be used.
- **Time-restricted tickets are only offered when they apply.** The Gold
  Nightrider is £4 against the DayRider's £9, but only from 19:30
  (`valid_from_time` / `valid_to_time`, wrapping past midnight so the small-hours
  N700 still counts as that evening). A midday journey is quoted £9, not £4. A
  ticket with a time window is excluded entirely when the departure time is
  unknown, rather than assumed usable.

A saving is only claimed when there genuinely is one. The all-operator
**South Downs Discovery Ticket** (£10) is a real ticket, not a hypothetical —
but it doesn't beat a £9 Gold DayRider on a single-operator journey, and the UI
says so instead of printing a negative "saving". Where it does pay is journeys
that need a change between operators.

## Previewing locally

Serve the static files and, if you're working on backend features, run the API
alongside them:

```sh
python -m http.server 8765                       # the site
uvicorn api.main:app --port 8000                 # the API (needs BODS_API_KEY)
```

Then open <http://127.0.0.1:8765/?api=http://localhost:8000>. Without the `?api=`
parameter the page talks to the deployed Render API, so anything not yet
deployed (a new endpoint, say) will 404. The override is ignored unless the page
itself is served from localhost and the target is a local address, so a link
can't be used to redirect someone else's traffic.

Pull requests are welcome — please keep the code plain HTML/CSS/JS on the frontend and pure FastAPI on the backend (no heavy frameworks) so it stays easy to maintain.

The site runs entirely on free tiers. Before adding new external calls, scheduled jobs, or polling changes, check [`LIMITS.md`](./LIMITS.md) for the caps on Render, GitHub, BODS, TransportAPI, and the tile providers.
