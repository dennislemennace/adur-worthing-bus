"""One-shot migration: rewrite data/proposals.json so every proposed stop
references a canonical GTFS stop (atco_code + canonical lat/lon), and
rebuild each proposal's polyline as the ordered list of its stops.

Rationale: the editor is now stops-only. Existing proposal entries had
hand-typed coordinates that drifted from real stop positions, which made
side-by-side comparison against existing services confusing. Snapping
each entry to the nearest real stop keeps the data model consistent
across hand-written and editor-authored proposals.

Run from the repo root:

    python scripts/migrate_proposals_to_atco.py

The script is idempotent — it's safe to re-run after editing names or
adding new proposals. Stops already carrying an `atco_code` are kept
in place (only coords are re-synced).

Exits non-zero (without overwriting the file) if any proposal stop is
further than `MAX_SNAP_M` from its best canonical match, so the operator
can review.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH       = REPO_ROOT / "data" / "timetable.sqlite"
PROPOSALS     = REPO_ROOT / "data" / "proposals.json"

# Adur & Worthing-ish bbox (matches api/main.py BBOX_* and the frontend).
# We bias matches toward stops inside this box, but allow fallback outside
# for proposals reaching Brighton.
BBOX_MIN_LAT, BBOX_MAX_LAT = 50.78, 50.92
BBOX_MIN_LON, BBOX_MAX_LON = -0.50,  0.00

# Search radii by match quality.
NAME_RADIUS_M    = 2500   # accept a name-token match anywhere within ~2.5 km
GEO_RADIUS_M     = 400    # geometric-nearest fallback only within ~400 m
MAX_SNAP_M       = 2500   # beyond this, fail loud (no plausible match)

# Synonyms expanded both ways before token comparison.
ABBREV = {
    "stn":  "station",
    "sta":  "station",
    "rd":   "road",
    "sq":   "square",
    "pde":  "parade",
    "ave":  "avenue",
    "ln":   "lane",
    "rly":  "railway",
    "rail": "railway",
    "ctr":  "centre",
    "ctre": "centre",
}

# Tokens stripped before name comparison (high-frequency, low-info).
# Kept short — words that genuinely distinguish stops (pier, hospital,
# station, hall, square, parade) are intentionally NOT stopwords.
STOPWORDS = frozenset({
    "the", "of", "and", "a", "an", "to", "at",
    "st",   # too ambiguous between "Street" and "Saint"
    "by", "near",
})

# Hand-curated overrides for the canonical "this proposal stop means this
# GTFS stop" mapping, used when the hand-typed name maps to a different
# vernacular than GTFS uses (e.g. "Hove Town Hall" → "Town Hall" near Hove).
# Each value is an `atco_code` directly. The matcher pins these without
# distance checking; only added when the auto-resolver picks something
# clearly wrong and we want to lock in the correct stop.
EXPLICIT_ATCO = {
    # (proposal_id, stop_name) -> atco_code
    ("coastal-sprinter",  "Worthing Pier"):     "4400WO0270",   # The Lido (closest)
    ("coastal-sprinter",  "Worthing Stn"):      "4400WO0229",   # Railway Station
    ("coastal-sprinter",  "Lancing Manor"):     "4400AD0053",   # nearest neighbourhood stop
    ("coastal-sprinter",  "Lancing Stn"):       "4400AD0117",   # Lancing Station
    ("coastal-sprinter",  "Shoreham-by-Sea"):   "4400AD0314",   # Shoreham-by-Sea Railway Station
    ("coastal-sprinter",  "Southwick Sq"):      "4400AD0259",   # Southwick Square
    ("coastal-sprinter",  "Hove Town Hall"):    "149000007954", # Town Hall (Hove area)
    ("coastal-sprinter",  "Brighton Stn"):      "149000007927", # Brighton Station
    ("downs-loop",        "Worthing Stn"):      "4400WO0229",
    ("downs-loop",        "Offington"):         "4400WO0477",   # Offington Corner
    ("downs-loop",        "Findon Village"):    "4400LH0876",   # The Black Horse (Findon centre)
    ("downs-loop",        "Washington"):        "4400HR0465",   # Washington Bostal
    ("downs-loop",        "Steyning High St"):  "4400HR0223",   # Clock Tower (Steyning centre)
    ("downs-loop",        "Bramber"):           "4400HR0231",   # St Mary's House (Bramber)
    ("downs-loop",        "Upper Beeding"):     "4400HR0343",   # Dacre Gardens (Upper Beeding)
    ("downs-loop",        "Coombes"):           "4400AD0005",   # Coombes Road
    ("downs-loop",        "Lancing Stn"):       "4400AD0117",
    ("hospital-shuttle",  "Worthing Hospital"): "4400WO0397",   # Hospital
    ("hospital-shuttle",  "Goring Rd"):         "4400WO0470",   # Cranleigh Road (Goring area)
    ("hospital-shuttle",  "Marine Pde W"):      "4400WO0512",   # Normandy Road (W seafront)
    ("hospital-shuttle",  "Worthing Pier"):     "4400WO0270",
    ("hospital-shuttle",  "Brooklands"):        "4400AD0057",   # Brooklands rec ground
    ("hospital-shuttle",  "Sompting Crossing"): "4400AD0077",   # East Street, Sompting
    ("hospital-shuttle",  "Lancing Beach"):     "4400AD0175",   # South Bank Court
    ("coastliner-700x",   "Worthing Hospital"): "4400WO0397",
    ("coastliner-700x",   "Durrington-on-Sea"): "4400WO0470",   # Cranleigh Road
    ("coastliner-700x",   "West Worthing"):     "4400WO0388",   # Harrow Road (nearest to W Worthing Stn)
    ("coastliner-700x",   "Worthing Stn"):      "4400WO0229",
    ("coastliner-700x",   "Worthing Pier"):     "4400WO0270",
    ("coastliner-700x",   "Lancing Stn"):       "4400AD0117",
    ("coastliner-700x",   "Shoreham-by-Sea"):   "4400AD0314",
    ("coastliner-700x",   "Hove Town Hall"):    "149000007954",
    ("coastliner-700x",   "Churchill Sq"):      "149000006989", # Churchill Square
    ("coastliner-700x",   "Brighton Stn"):      "149000007927",
}


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres."""
    R = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def name_tokens(name: str) -> set:
    """Lowercased word set with stopwords removed, punctuation dropped,
    and abbreviations expanded (so "Worthing Stn" and "Worthing Railway
    Station" share tokens)."""
    parts = re.findall(r"[A-Za-z0-9]+", str(name or "").lower())
    out = set()
    for p in parts:
        if not p or p in STOPWORDS:
            continue
        out.add(ABBREV.get(p, p))
    return out


def load_gtfs_stops(con: sqlite3.Connection) -> tuple:
    """Return (stops_list, by_atco) for every GTFS stop that has any
    timetabled departure. Includes stops outside the Adur & Worthing bbox
    so proposals reaching Brighton / Portsmouth still snap correctly."""
    stops_with_times = {
        row[0] for row in con.execute(
            "SELECT DISTINCT s.stop_id "
            "FROM stop_times st JOIN stops s ON s.sid = st.sid"
        )
    }
    stops_list = []
    by_atco = {}
    for stop_id, name, lat, lon in con.execute(
        "SELECT stop_id, name, lat, lon FROM stops"
    ):
        if lat is None or lon is None:
            continue
        if stop_id not in stops_with_times:
            continue
        s = {"atco_code": stop_id, "name": name or "Bus Stop",
             "lat": float(lat), "lon": float(lon),
             "_tokens": name_tokens(name)}
        stops_list.append(s)
        by_atco[stop_id] = s
    return stops_list, by_atco


def snap_one(target_name: str, target_lat: float, target_lon: float,
             gtfs_stops: list) -> tuple:
    """Pick the best canonical stop for a hand-typed proposal landmark.

    Scoring: stops with shared name tokens get a strong discount on
    effective distance (so "Worthing Stn" matches "Railway Station" up to
    2.5 km away if the token "station" overlaps and there's no closer
    name match). Falls back to pure geometric-nearest within GEO_RADIUS_M.

    Returns (stop, distance_m, reason).
    """
    target_tokens = name_tokens(target_name)

    best = None
    best_score = float("inf")
    best_d = float("inf")
    best_reason = ""

    # Pass 1: name-aware scoring within NAME_RADIUS_M.
    if target_tokens:
        for s in gtfs_stops:
            d = haversine_m(target_lat, target_lon, s["lat"], s["lon"])
            if d > NAME_RADIUS_M:
                continue
            shared = target_tokens & s["_tokens"]
            if not shared:
                continue
            # Each shared token cancels ~600 m of geographic distance.
            # Tunes so a strong 2-token match wins over a 200 m geometric
            # near-miss with no name overlap.
            score = d - len(shared) * 600
            if score < best_score:
                best       = s
                best_score = score
                best_d     = d
                best_reason = f"name ({', '.join(sorted(shared))})"

    if best is not None:
        return best, best_d, best_reason

    # Pass 2: pure geometric-nearest within GEO_RADIUS_M.
    nearest = None
    nearest_d = float("inf")
    for s in gtfs_stops:
        d = haversine_m(target_lat, target_lon, s["lat"], s["lon"])
        if d < nearest_d:
            nearest = s
            nearest_d = d

    if nearest is not None and nearest_d <= GEO_RADIUS_M:
        return nearest, nearest_d, "nearest"
    return nearest, nearest_d, "nearest (out of radius)"


def main() -> int:
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found. Build the timetable first.",
              file=sys.stderr)
        return 1
    if not PROPOSALS.exists():
        print(f"ERROR: {PROPOSALS} not found.", file=sys.stderr)
        return 1

    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        gtfs_stops, by_atco = load_gtfs_stops(con)
    finally:
        con.close()
    print(f"Loaded {len(gtfs_stops)} canonical GTFS stops with timetable data.")

    data = json.loads(PROPOSALS.read_text(encoding="utf-8"))
    proposals = data.get("proposals", [])
    failures = []

    for p in proposals:
        proposal_id = p.get("id", "?")
        new_stops = []
        for raw in p.get("stops", []):
            name = raw.get("name", "")
            lat = float(raw.get("lat", 0.0))
            lon = float(raw.get("lon", 0.0))

            # 1. Honor an existing atco_code on the entry (idempotent re-runs).
            atco = raw.get("atco_code")
            if atco and atco in by_atco:
                hit = by_atco[atco]
                new_stops.append({
                    "atco_code": hit["atco_code"], "name": hit["name"],
                    "lat": round(hit["lat"], 5), "lon": round(hit["lon"], 5),
                })
                continue

            # 2. Honor a hand-curated alias for vernacular landmark names.
            override = EXPLICIT_ATCO.get((proposal_id, name))
            if override and override in by_atco:
                hit = by_atco[override]
                new_stops.append({
                    "atco_code": hit["atco_code"], "name": hit["name"],
                    "lat": round(hit["lat"], 5), "lon": round(hit["lon"], 5),
                })
                print(f"  {proposal_id:<22}  {name!r:>30}  ->  "
                      f"{hit['atco_code']}  {hit['name']!r}  [alias]")
                continue

            # 3. Auto-resolve by name + geometry.
            match, dist, reason = snap_one(name, lat, lon, gtfs_stops)
            if match is None:
                failures.append((proposal_id, name, lat, lon, None, None,
                                 "no match"))
                continue
            if dist > MAX_SNAP_M:
                failures.append((proposal_id, name, lat, lon,
                                 match["name"], dist, reason))
                continue

            new_stops.append({
                "atco_code": match["atco_code"], "name": match["name"],
                "lat": round(match["lat"], 5), "lon": round(match["lon"], 5),
            })
            print(f"  {proposal_id:<22}  {name!r:>30}  ->  "
                  f"{match['atco_code']}  {match['name']!r}"
                  f"  [{reason}, {dist:.0f} m]")

        p["stops"]    = new_stops
        # Preserve a pre-baked road-following polyline (e.g. the one-time
        # OSRM-aligned 60X / 700X geometry). A straight stop-to-stop line has
        # exactly one point per stop; anything denser was hand-baked and must
        # not be clobbered. Only (re)generate the cheap straight line when no
        # such geometry exists, keeping it in sync with the snapped stops.
        # CAVEAT: if you change a proposal's STOPS, this guard keeps the old
        # baked line (now stale — it routes through where stops used to be).
        # Re-bake that proposal from OSRM after editing its stops.
        existing = p.get("polyline") or []
        if len(existing) <= len(new_stops):
            p["polyline"] = [[s["lat"], s["lon"]] for s in new_stops]
        else:
            print(f"  {proposal_id:<22}  preserving baked polyline "
                  f"({len(existing)} pts > {len(new_stops)} stops)")
        # Every existing proposal is an all-day daytime concept; mark them
        # explicitly so the new "Show limited services" toggle gates them
        # the same way it gates real services.
        p.setdefault("frequency_class", "frequent_all_day")

    if failures:
        print("\nFAILED to snap the following stops within MAX_SNAP_M:")
        for proposal_id, name, lat, lon, m_name, dist, reason in failures:
            d = f"{dist:.0f} m" if dist is not None else "?"
            print(f"  {proposal_id}: {name!r} @ ({lat}, {lon}) "
                  f"-> {m_name!r} ({d}, {reason})")
        print("\nproposals.json was NOT modified. Edit the offending entries"
              " and re-run.")
        return 2

    PROPOSALS.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"\nRewrote {PROPOSALS} with {sum(len(p.get('stops', [])) for p in proposals)} "
          f"snapped stops across {len(proposals)} proposals.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
