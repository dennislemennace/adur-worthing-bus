# Adur & Worthing Live Bus Tracker

A free, open-source live bus departure board and vehicle tracking website for the **Adur & Worthing** area of West Sussex, UK.

- 🗺 **Interactive Leaflet map** showing live bus positions and stop markers  
- 🕐 **Real-time departure boards** for any stop, one click away  
- 📱 **Mobile-friendly** responsive layout  
- 🎯 **Network Objectives** tab — the objectives the network is working towards, plus a
  community ideas box anyone can use **without a GitHub account**

Data from the [Bus Open Data Service (BODS)](https://data.bus-data.dft.gov.uk/) — UK Department for Transport.

## Community ideas (Network Objectives → Ideas)

The Ideas form and the proposal editor's **Submit** button relay suggestions to
the maintainer's inbox via [Web3Forms](https://web3forms.com) — no backend, no
database, and no account needed for the person submitting.

To switch it on, create a free Web3Forms account and paste your **access key**
into `CONFIG.WEB3FORMS_ACCESS_KEY` in `app.js`. The key is public by design (it
only authorises sending mail to your own account); spam is held back by a
honeypot plus Web3Forms' built-in filtering. Until a key is set, the form shows a
friendly "not switched on yet" message.

Curated content is plain JSON you edit by hand: network goals live in
`data/objectives.json`, and approved community ideas in `data/suggestions.json`
(copy the good ones from your inbox). Run `pytest` (see `requirements-dev.txt`)
to validate these files before committing.

Pull requests are welcome — please keep the code plain HTML/CSS/JS on the frontend and pure FastAPI on the backend (no heavy frameworks) so it stays easy to maintain.

The site runs entirely on free tiers. Before adding new external calls, scheduled jobs, or polling changes, check [`LIMITS.md`](./LIMITS.md) for the caps on Render, GitHub, BODS, TransportAPI, and the tile providers.
