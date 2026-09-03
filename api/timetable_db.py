"""SQLite-backed timetable store.

Replaces the old dict-of-dicts loaded from data/timetable.json.
Small reference tables (stops/routes/trips/calendar/calendar_dates) are
kept in RAM so existing dict-style consumer code continues to work.
The hot bulk tables (stop_times, per-trip stop sequences, service
endpoint summaries) live on disk and are queried on demand.

This keeps Render Free-tier RSS well under 512 MB.
"""
from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import threading
import time
import urllib.request
from pathlib import Path
from typing import Iterator, Optional

log = logging.getLogger("bus_api.timetable")


def _haversine_km(a: tuple, b: tuple) -> float:
    """Distance between two (lat, lon) points in kilometres."""
    import math
    la1, lo1 = math.radians(a[0]), math.radians(a[1])
    la2, lo2 = math.radians(b[0]), math.radians(b[1])
    d = (math.sin((la2 - la1) / 2) ** 2
         + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2)
    return 6371.0 * 2 * math.asin(math.sqrt(d))


def _split_long_chords(pts: list, threshold_km: float = 2.0) -> list:
    """Split a polyline at any consecutive-pair distance > threshold_km.
    Returns a list of contiguous sub-polylines. A polyline with no long
    chord returns as `[pts]`. Used as a safety net for stop-to-stop
    fallback polylines whose source GTFS skipped intermediate stops.
    """
    if len(pts) < 2:
        return [pts]
    cuts: list = [0]
    for i in range(len(pts) - 1):
        if _haversine_km(pts[i], pts[i + 1]) > threshold_km:
            cuts.append(i + 1)
    cuts.append(len(pts))
    out = []
    for a, b in zip(cuts, cuts[1:]):
        seg = pts[a:b]
        if len(seg) >= 2:
            out.append(seg)
    return out if out else [pts]

# ~1.2% of trips (460 of 39,528 as of the 2026-06 build) have their stop times
# wrapped around midnight. GTFS represents a stop at 00:06 on a trip that began
# the previous evening as 24:06:00 (86760s); the builder instead wraps it to
# 360s and the trip's stops then sort with the post-midnight tail at the front.
# The result reads as a bus teleporting across the county with a 23-hour gap
# between consecutive stops.
#
# Left alone this poisons any A-to-B path — you get a plausible-looking
# two-stop "journey" from Worthing to Brighton that skips every stop between,
# and the ticket-zone classifier then sees none of the zones actually crossed.
# So spans are sanity-checked before being offered as journey options.
#
# The real fix belongs in scripts/build_timetable.py (keep times past 24:00:00
# rather than wrapping them). Until then these night trips are excluded here.
MAX_SECS_PER_STOP  = 1_800    # 30 min average between stops — generous for rural
MAX_LEG_GAP_SECS   = 7_200    # 2 h between two consecutive stops is never real
MAX_JOURNEY_SECS   = 21_600   # 6 h end to end


# Where the ordinary service day starts, for reporting a stop's span. Matches
# the 04:00 close of the Gold Nightrider window already modelled in
# data/ticket_zones.json, so "night" means the same thing across the site.
NIGHT_ENDS_SECS = 4 * 3600
DAY_SECS = 24 * 3600


def _span_hhmm(secs) -> str:
    """Seconds past midnight to HH:MM, keeping GTFS's past-midnight hours.

    Deliberately not wrapped at 24:00. A last bus at 24:20 left at twenty past
    midnight *at the end of that service day*, and rendering it as "00:20"
    would put it thirteen hours before the first bus.
    """
    if secs is None:
        return ""
    return f"{secs // 3600:02d}:{(secs % 3600) // 60:02d}"


def _route_sort_key(name: str):
    """Sort route labels the way a timetable does: 2, 9, 19, 46, N1, 700."""
    text = str(name)
    night = text[:1].upper() == "N" and text[1:2].isdigit()
    body = text[1:] if night else text
    digits = ""
    for ch in body:
        if ch.isdigit():
            digits += ch
        else:
            break
    return (night, int(digits) if digits else 9999, text)


def _plausible_span(trip: dict) -> bool:
    """Reject A-to-B spans whose timings can't describe one real journey."""
    span_secs = trip["arrive_secs"] - trip["depart_secs"]
    stops_apart = max(1, trip["to_seq"] - trip["from_seq"])
    if span_secs <= 0:
        return False
    if span_secs > MAX_JOURNEY_SECS:
        return False
    return (span_secs / stops_apart) <= MAX_SECS_PER_STOP


def path_has_time_gap(stops: list) -> bool:
    """True if any consecutive pair of stops is implausibly far apart in time.

    Catches stitched trips whose break lands inside the requested span, which
    the span-level check can't see.
    """
    for prev, cur in zip(stops, stops[1:]):
        a, b = prev.get("dep_secs"), cur.get("dep_secs")
        if a is None or b is None:
            continue
        if b - a > MAX_LEG_GAP_SECS:
            return True
    return False


TIMETABLE_URL = os.environ.get(
    "TIMETABLE_URL",
    "https://github.com/dennislemennace/adur-worthing-bus/releases/download/timetable-latest/timetable.sqlite",
)

# Suffix appended to db_path for the locally-computed hash cache. Trusted
# when its mtime ≥ the DB's mtime; otherwise we rehash. Saves ~200 ms of
# SHA-256 over 63 MB on every warm cold start.
_LOCAL_HASH_SUFFIX = ".sha256.local"
_SIDECAR_TIMEOUT = 5  # seconds — sidecar is ~64 bytes, this is generous.


class Timetable:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._con: Optional[sqlite3.Connection] = None
        self.stops: dict = {}
        self.routes: dict = {}
        self.trips: dict = {}
        self.calendar: dict = {}
        self.calendar_dates: dict = {}
        # stop_ids that have at least one stop_times entry. Preloaded so
        # the geo-proximity fallback can filter candidates without issuing
        # one SELECT per stop.
        self.stops_with_times: frozenset = frozenset()
        # Reverse: surrogate sid/tid/rid -> text id. Used to decode query rows.
        self._sid_to_stop: dict = {}
        self._tid_to_trip: dict = {}
        # Lazily built {short_name: noc}; see noc_for_short_name().
        self._noc_map: Optional[dict] = None
        # Lazily built {stop name: [stop_id, ...]}; see sibling_stops().
        self._stops_by_name: Optional[dict] = None
        self.loaded_at: float = 0.0
        self._open_and_preload()

    def _fetch_db(self) -> bool:
        """Download timetable.sqlite to db_path via a .tmp+replace.
        Returns True on success."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.db_path.with_suffix(self.db_path.suffix + ".tmp")
        try:
            urllib.request.urlretrieve(TIMETABLE_URL, tmp)
            tmp.replace(self.db_path)
            log.info("Timetable DB downloaded: %d bytes",
                     self.db_path.stat().st_size)
            return True
        except Exception as exc:
            log.error("Timetable download failed: %s", exc)
            if tmp.exists():
                try:
                    tmp.unlink()
                except Exception:
                    pass
            return False

    def _cached_local_hash_path(self) -> Path:
        return self.db_path.with_suffix(self.db_path.suffix + _LOCAL_HASH_SUFFIX)

    def _local_hash(self) -> Optional[str]:
        """Return the SHA-256 of the on-disk DB. Uses an mtime-keyed cache
        file alongside the DB to avoid rehashing 63 MB on every cold start."""
        if not self.db_path.exists():
            return None
        cache = self._cached_local_hash_path()
        try:
            if (cache.exists()
                    and cache.stat().st_mtime >= self.db_path.stat().st_mtime):
                digest = cache.read_text(encoding="utf-8").strip().lower()
                if len(digest) == 64:
                    return digest
        except Exception:
            pass
        try:
            h = hashlib.sha256()
            with self.db_path.open("rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
            digest = h.hexdigest()
        except Exception as exc:
            log.warning("Failed to hash %s: %s", self.db_path, exc)
            return None
        try:
            tmp = cache.with_suffix(cache.suffix + ".tmp")
            tmp.write_text(digest + "\n", encoding="utf-8")
            tmp.replace(cache)
        except Exception as exc:
            log.warning("Failed to cache local hash: %s", exc)
        return digest

    def _remote_hash(self) -> Optional[str]:
        """Fetch the .sha256 sidecar from TIMETABLE_URL. Returns None on
        any network or parse failure — callers should treat that as
        'no change needed' to avoid bringing the API down on a blip."""
        sidecar_url = TIMETABLE_URL + ".sha256"
        try:
            with urllib.request.urlopen(sidecar_url, timeout=_SIDECAR_TIMEOUT) as resp:
                text = resp.read(256).decode("utf-8", errors="replace")
        except Exception as exc:
            log.warning("Timetable sidecar fetch failed (%s): %s",
                        sidecar_url, exc)
            return None
        digest = (text.strip().split() or [""])[0].lower()
        if len(digest) == 64 and all(c in "0123456789abcdef" for c in digest):
            return digest
        log.warning("Timetable sidecar returned unexpected content: %r",
                    text[:64])
        return None

    def _ensure_fresh(self) -> None:
        """Make sure data/timetable.sqlite matches the remote sidecar hash.
        On first start (db missing) just downloads. On subsequent starts,
        compares local hash to the remote sidecar; refreshes on mismatch.
        Falls back to serving the existing file when the sidecar is
        unreachable (a brittle release shouldn't take the API down)."""
        if not self.db_path.exists():
            log.info("Timetable DB missing; downloading from %s", TIMETABLE_URL)
            self._fetch_db()
            self._local_hash()  # warm the cache for next start
            return
        remote = self._remote_hash()
        if remote is None:
            return
        local = self._local_hash()
        if local == remote:
            log.info("Timetable DB up-to-date (sha256 %s)", remote[:12])
            return
        log.info("Timetable DB stale (local %s != remote %s); refreshing",
                 (local or "?")[:12], remote[:12])
        if self._fetch_db():
            self._local_hash()  # rewrite cache to match the new file

    def _open_and_preload(self) -> None:
        self._ensure_fresh()
        # Drop memoized derivations — reload() lands here with a new DB file.
        self._noc_map = None
        self._stops_by_name = None
        if not self.db_path.exists():
            log.error("Timetable DB missing: %s", self.db_path)
            self._con = None
            return

        con = sqlite3.connect(
            f"file:{self.db_path}?mode=ro",
            uri=True,
            check_same_thread=False,
        )
        con.execute("PRAGMA query_only = 1")
        con.execute("PRAGMA temp_store = MEMORY")
        con.execute("PRAGMA cache_size = -8000")  # ~8 MB page cache

        stops: dict = {}
        sid_to_stop: dict = {}
        for sid, stop_id, name, lat, lon in con.execute(
            "SELECT sid, stop_id, name, lat, lon FROM stops"
        ):
            stops[stop_id] = {"name": name, "lat": lat, "lon": lon, "_sid": sid}
            sid_to_stop[sid] = stop_id

        routes: dict = {}
        rid_to_route: dict = {}
        for rid, route_id, short_name, long_name in con.execute(
            "SELECT rid, route_id, short_name, long_name FROM routes"
        ):
            routes[route_id] = {
                "short_name": short_name,
                "long_name":  long_name,
                "_rid": rid,
            }
            rid_to_route[rid] = route_id

        trips: dict = {}
        tid_to_trip: dict = {}
        for tid, trip_id, rid, service_id, headsign in con.execute(
            "SELECT tid, trip_id, rid, service_id, headsign FROM trips"
        ):
            trips[trip_id] = {
                "route_id":   rid_to_route.get(rid, ""),
                "service_id": service_id,
                "headsign":   headsign,
                "_tid": tid,
            }
            tid_to_trip[tid] = trip_id

        calendar: dict = {}
        for row in con.execute(
            "SELECT service_id, monday, tuesday, wednesday, thursday, "
            "friday, saturday, sunday, start_date, end_date FROM calendar"
        ):
            (service_id, mon, tue, wed, thu, fri, sat, sun, start, end) = row
            calendar[service_id] = {
                "monday":    str(mon),
                "tuesday":   str(tue),
                "wednesday": str(wed),
                "thursday":  str(thu),
                "friday":    str(fri),
                "saturday":  str(sat),
                "sunday":    str(sun),
                "start_date": start,
                "end_date":   end,
            }

        calendar_dates: dict = {}
        for service_id, date_str, exc in con.execute(
            "SELECT service_id, date, exception FROM calendar_dates"
        ):
            calendar_dates.setdefault(service_id, {})[date_str] = str(exc)

        stops_with_times = frozenset(
            sid_to_stop[row[0]]
            for row in con.execute("SELECT DISTINCT sid FROM stop_times")
        )

        # Atomic swap.
        with self._lock:
            old_con = self._con
            self._con = con
            self.stops = stops
            self.routes = routes
            self.trips = trips
            self.calendar = calendar
            self.calendar_dates = calendar_dates
            self.stops_with_times = stops_with_times
            self._sid_to_stop = sid_to_stop
            self._tid_to_trip = tid_to_trip
            self.loaded_at = time.time()
        if old_con is not None:
            try:
                old_con.close()
            except Exception:
                pass

        log.info(
            "Timetable loaded: %d stops, %d routes, %d trips, "
            "%d calendar, %d calendar_dates",
            len(stops), len(routes), len(trips),
            len(calendar), len(calendar_dates),
        )

    def reload(self) -> None:
        self._open_and_preload()

    def ok(self) -> bool:
        return self._con is not None

    # ── Hot-path queries ─────────────────────────────────────

    def stop_times_for(self, stop_id: str) -> list:
        """Return [(dep_secs, trip_id), ...] ordered by dep_secs for a stop.

        Empty list if the stop is unknown.
        """
        if self._con is None:
            return []
        stop = self.stops.get(stop_id)
        if not stop:
            return []
        sid = stop["_sid"]
        tid_to_trip = self._tid_to_trip
        return [
            (dep_secs, tid_to_trip[tid])
            for dep_secs, tid in self._con.execute(
                "SELECT dep_secs, tid FROM stop_times "
                "WHERE sid=? ORDER BY dep_secs",
                (sid,),
            )
        ]

    def trip_stops_for(self, trip_id: str) -> list:
        """Return [(dep_secs, stop_id), ...] in trip sequence order."""
        if self._con is None:
            return []
        trip = self.trips.get(trip_id)
        if not trip:
            return []
        tid = trip["_tid"]
        sid_to_stop = self._sid_to_stop
        return [
            (dep_secs, sid_to_stop[sid])
            for dep_secs, sid in self._con.execute(
                "SELECT dep_secs, sid FROM stop_times "
                "WHERE tid=? ORDER BY seq",
                (tid,),
            )
        ]

    def trips_connecting(self, from_stop: str, to_stop: str,
                         limit: int = 40, from_secs: int = None) -> list:
        """Trips that serve `from_stop` and later `to_stop`, in that order.

        Returns [{trip_id, route_id, short_name, headsign, service_id,
                  depart_secs, arrive_secs, from_seq, to_seq}, ...]
        earliest departure first.

        This backs the ticket-boundary calculator, which needs the stops a
        passenger actually travels through — the endpoints alone can't tell you
        whether a journey dips through a third zone on the way.

        Only direct trips are considered: no interchange, no walking legs. A
        journey needing a change comes back empty, and the caller says so.

        The `idx_stop_times_stop` index on (sid, dep_secs) makes both lookups
        covering-index scans. The intersection is cheap even for the busiest
        pair of stops in the network; the per-candidate ordering check below is
        what `limit` is guarding.
        """
        if self._con is None:
            return []
        a = self.stops.get(from_stop)
        b = self.stops.get(to_stop)
        if not a or not b or from_stop == to_stop:
            return []

        con = self._con
        # (tid -> seq/dep) for each end, then intersect on tid.
        from_rows = {
            tid: (seq, dep) for tid, seq, dep in con.execute(
                "SELECT tid, seq, dep_secs FROM stop_times WHERE sid=?", (a["_sid"],))
        }
        if not from_rows:
            return []
        to_rows = {
            tid: (seq, dep) for tid, seq, dep in con.execute(
                "SELECT tid, seq, dep_secs FROM stop_times WHERE sid=?", (b["_sid"],))
        }

        tid_to_trip = self._tid_to_trip
        out = []
        for tid, (from_seq, from_dep) in from_rows.items():
            hit = to_rows.get(tid)
            if hit is None:
                continue
            to_seq, to_dep = hit
            if to_seq <= from_seq:
                continue          # wrong direction on this trip
            trip_id = tid_to_trip.get(tid)
            if trip_id is None:
                continue
            trip = self.trips.get(trip_id) or {}
            route = self.routes.get(trip.get("route_id", "")) or {}
            out.append({
                "trip_id":     trip_id,
                "route_id":    trip.get("route_id", ""),
                "short_name":  route.get("short_name", ""),
                "headsign":    trip.get("headsign", ""),
                "service_id":  trip.get("service_id", ""),
                "depart_secs": from_dep,
                "arrive_secs": to_dep,
                "from_seq":    from_seq,
                "to_seq":      to_seq,
            })

        out = [t for t in out if _plausible_span(t)]
        if from_secs is None:
            out.sort(key=lambda t: t["depart_secs"])
        else:
            # Order outward from a time of day, wrapping at midnight, so that
            # `limit` keeps the trips around that time rather than the first
            # forty of the service day. Without this a caller asking about
            # midday is handed the small hours and never sees anything else,
            # because the cap has already thrown the rest away. GTFS writes a
            # trip running past midnight as 24:xx, hence the modulo.
            out.sort(key=lambda t: (((t["depart_secs"] % 86400) - from_secs) % 86400,
                                    t["depart_secs"]))
        return out[:limit] if limit else out

    def sibling_stops(self, stop_id: str, max_km: float = 0.4) -> list:
        """Stops that are the same physical place as `stop_id`.

        Most stops are one of a pair of poles on opposite sides of a road, with
        the same name a few metres apart. Only one of them is served by a bus
        going the passenger's way, so a journey search that fixes on the pole
        the user happened to pick finds nothing half the time.

        Returns `stop_id` first, then its siblings.
        """
        stop = self.stops.get(stop_id)
        if not stop:
            return [stop_id]
        name = stop.get("name")
        if not name:
            return [stop_id]

        if self._stops_by_name is None:
            index: dict = {}
            for sid, s in self.stops.items():
                index.setdefault(s.get("name", ""), []).append(sid)
            self._stops_by_name = index

        out = [stop_id]
        for other in self._stops_by_name.get(name, ()):
            if other == stop_id:
                continue
            o = self.stops.get(other) or {}
            if o.get("lat") is None or o.get("lon") is None:
                continue
            if _haversine_km((stop["lat"], stop["lon"]), (o["lat"], o["lon"])) <= max_km:
                out.append(other)
        return out

    def noc_for_short_name(self, short_name: str) -> str:
        """Operator NOC for a route short_name, or "" if unknown.

        Memoized — `_noc_by_short_name` runs a query and builds the whole map
        each call, which is fine once but not once per journey option.
        """
        if self._noc_map is None:
            self._noc_map = self._noc_by_short_name()
        return self._noc_map.get(short_name, "")

    def stops_between(self, trip_id: str, from_seq: int, to_seq: int) -> list:
        """Ordered stops on `trip_id` from `from_seq` to `to_seq` inclusive.

        Returns [{atco, name, lat, lon, seq, dep_secs}, ...] — everything the
        zone classifier needs, with no follow-up lookups.
        """
        if self._con is None:
            return []
        trip = self.trips.get(trip_id)
        if not trip:
            return []
        sid_to_stop = self._sid_to_stop
        out = []
        for seq, sid, dep_secs in self._con.execute(
            "SELECT seq, sid, dep_secs FROM stop_times "
            "WHERE tid=? AND seq BETWEEN ? AND ? ORDER BY seq",
            (trip["_tid"], from_seq, to_seq),
        ):
            atco = sid_to_stop.get(sid)
            if atco is None:
                continue
            s = self.stops.get(atco) or {}
            out.append({
                "atco":     atco,
                "name":     s.get("name", ""),
                "lat":      s.get("lat"),
                "lon":      s.get("lon"),
                "seq":      seq,
                "dep_secs": dep_secs,
            })
        return out

    def operators_at_stop(self, stop_id: str) -> list:
        """NOCs of every operator whose services call at `stop_id`.

        This is what makes a ticket answer honest. A zone polygon says where a
        ticket is *geographically* valid; it says nothing about whether a bus
        you can use that ticket on actually stops there. Boundary Road in
        Portslade sits inside the Brighton DayRider zone and is served only by
        Brighton & Hove — so a DayRider buys you nothing at that stop, and a
        calculator that reasons from the polygon alone will cheerfully tell you
        otherwise.

        Widened across sibling poles, for the same reason journey search is:
        a passenger picks a place, not a side of the road.
        """
        if self._con is None:
            return []
        sids = [
            self.stops[s]["_sid"]
            for s in self.sibling_stops(stop_id)
            if s in self.stops and "_sid" in self.stops[s]
        ]
        if not sids:
            return []
        placeholders = ",".join("?" * len(sids))
        rows = self._con.execute(
            f"""SELECT DISTINCT r.short_name, r.noc
                  FROM stop_times st
                  JOIN trips  t ON t.tid = st.tid
                  JOIN routes r ON r.rid = t.rid
                 WHERE st.sid IN ({placeholders})""",
            sids,
        )
        # Read routes.noc per route, NOT noc_for_short_name. That helper
        # collapses the whole routes table into one short_name -> noc map and
        # is last-row-wins (see the TODO on _OPERATOR_OVERRIDES), so a number
        # used by two operators anywhere in the region resolves to whichever
        # row happened to come last. At a Portslade stop served only by
        # Brighton & Hove routes 1/1X/6 that produced "SCSO", which is exactly
        # the false positive this method exists to prevent.
        nocs = set()
        for short_name, noc in rows:
            nocs.add(self._OPERATOR_OVERRIDES.get(short_name, noc))
        return sorted(n for n in nocs if n)

    def service_endpoints(self, short_name: str) -> Iterator[tuple]:
        """Yield (trip_id, first_stop_id, last_stop_id, first_secs) for every
        trip whose route short_name matches."""
        if self._con is None:
            return
        tid_to_trip = self._tid_to_trip
        sid_to_stop = self._sid_to_stop
        for tid, first_sid, last_sid, first_secs in self._con.execute(
            "SELECT tid, first_sid, last_sid, first_secs "
            "FROM trip_endpoints WHERE short_name=?",
            (short_name,),
        ):
            yield (
                tid_to_trip[tid],
                sid_to_stop.get(first_sid, ""),
                sid_to_stop.get(last_sid, ""),
                first_secs,
            )

    def service_frequency(self, short_name: str,
                          allowed_tids: Optional[set] = None) -> dict:
        """Coarse operating-pattern stats for one route short_name.

        Powers the Improvements tab's "frequent all-day services only"
        default filter. A route is `is_frequent_all_day` iff it runs every
        day of the week, has at least one journey starting at or after
        18:00, and its median weekday daytime headway is <= 30 min.

        allowed_tids: when given, only trips whose surrogate `tid` is in
        this set are counted. Callers pass the bbox-relevant trip ids so a
        same-numbered route elsewhere in the feed (e.g. a frequent "60" in
        another town) doesn't pollute the local classification. When None,
        the whole feed is considered.

        runs_days union: any service_id touched by a counted trip with this
        short_name contributes its day-of-week flags. last_start_sec is
        the max first_secs across counted trips. weekday_headway_min uses
        Tuesday as the "typical weekday" sample, restricted to 08:00–18:00.
        """
        empty = {
            "runs_days": [],
            "last_start_sec": 0,
            "weekday_headway_min": None,
            "is_frequent_all_day": False,
        }
        if self._con is None:
            return empty

        days_seen = [False] * 7
        last_start_sec = 0
        tuesday_starts: list = []

        tid_to_trip = self._tid_to_trip
        for tid, first_secs in self._con.execute(
            "SELECT tid, first_secs FROM trip_endpoints WHERE short_name=?",
            (short_name,),
        ):
            if allowed_tids is not None and tid not in allowed_tids:
                continue
            if first_secs is None:
                continue
            if first_secs > last_start_sec:
                last_start_sec = first_secs

            trip_id = tid_to_trip.get(tid)
            if trip_id is None:
                continue
            trip = self.trips.get(trip_id)
            if trip is None:
                continue
            cal = self.calendar.get(trip.get("service_id", ""))
            if not cal:
                continue
            for i, col in enumerate(self._DAY_COLS):
                if cal.get(col, "0") == "1":
                    days_seen[i] = True
            if cal.get("tuesday", "0") == "1":
                tuesday_starts.append(first_secs)

        # Dedupe identical start minutes: the same clock-time journey often
        # appears under several service_ids (schools / holidays / base
        # calendar variants), and two buses leaving at the same minute is
        # one departure slot, not a zero-minute headway. Without this an
        # hourly-but-duplicated route would collapse to a 0-min median and
        # be misclassified as frequent.
        DAY_LO = 8 * 3600
        DAY_HI = 18 * 3600
        daytime = sorted({s // 60 for s in tuesday_starts if DAY_LO <= s <= DAY_HI})
        weekday_headway_min: Optional[int] = None
        if len(daytime) >= 2:
            gaps = sorted(
                daytime[i + 1] - daytime[i]
                for i in range(len(daytime) - 1)
            )
            mid = len(gaps) // 2
            median = gaps[mid] if len(gaps) % 2 else (gaps[mid - 1] + gaps[mid]) / 2.0
            weekday_headway_min = int(round(median))

        runs_days = [self._DAY_SHORT[i] for i, on in enumerate(days_seen) if on]
        is_frequent = (
            len(runs_days) == 7
            and last_start_sec >= 18 * 3600
            and weekday_headway_min is not None
            and weekday_headway_min <= 30
        )
        return {
            "runs_days": runs_days,
            "last_start_sec": last_start_sec,
            "weekday_headway_min": weekday_headway_min,
            "is_frequent_all_day": is_frequent,
        }

    def runs_on(self, service_id: str, day) -> bool:
        """Whether a GTFS service actually runs on a given date.

        Not "does its calendar row mention this weekday" — that question has a
        much larger answer. This feed carries 112 calendars over 14 different
        date ranges, so a single real bus is described by several service_ids
        covering term time, holidays and seasonal variations. Counting every
        calendar whose `monday` column is 1 counts that bus once per calendar:
        one Portslade stop came out at 782 Monday departures, a bus every ninety
        seconds, and the error is not uniform between operators, so it does not
        even cancel in a ratio.

        `calendar_dates` wins over `calendar`, which is what makes a bank
        holiday behave like a Sunday.
        """
        cal = self.calendar.get(service_id)
        exceptions = self.calendar_dates.get(service_id, {})
        stamp = day.strftime("%Y%m%d")
        if stamp in exceptions:
            return exceptions[stamp] == "1"
        if not cal:
            return False
        start, end = cal.get("start_date", ""), cal.get("end_date", "")
        if start and stamp < start:
            return False
        if end and stamp > end:
            return False
        return cal.get(self._DAY_COLS[day.weekday()]) == "1"

    def sample_week(self, from_day=None) -> dict:
        """A concrete week to measure, as {day name: date}.

        Measuring "a Monday" requires picking one. The first full week that
        starts inside the timetable's own validity window is used, so the answer
        is reproducible and can be quoted with the date it refers to.
        """
        import datetime as _dt
        base = from_day or _dt.date.today()
        # Move to the next Monday (or today, if today is one).
        monday = base + _dt.timedelta(days=(0 - base.weekday()) % 7)
        return {name: monday + _dt.timedelta(days=i)
                for i, name in enumerate(self._DAY_COLS)}

    def service_span(self, stop_id: str) -> dict:
        """When buses actually run from a stop, by day of week.

        The departure board answers "what is coming next". It cannot answer the
        question this network's problems actually turn on: *is there a bus here
        at seven in the evening, or on a Sunday at all?* A peak-only stop with
        no weekend service looks identical on a Tuesday morning to one served
        every ten minutes until midnight.

        Returns, per day type, the first and last departure and how many there
        are, plus the set of routes calling that day:

            {"monday": {"first": "06:12", "last": "18:40", "count": 31,
                        "routes": ["9", "19"]}, ..., "sunday": None}

        `None` for a day means no service at all on it, which is the finding
        worth showing rather than an empty result to be styled away.

        Widened across sibling poles, like every other stop-level answer here:
        a passenger picks a place, not a side of the road.

        Computed on demand rather than precomputed into the database. A stop is
        one indexed scan on `idx_stop_times_stop`, only stops somebody opens are
        ever touched, and it costs no upstream quota — where a new column would
        have meant every deployment serving a stale schedule until the next
        weekly rebuild caught up.
        """
        if self._con is None:
            return {}
        cache = getattr(self, "_span_cache", None)
        if cache is None:
            cache = self._span_cache = {}
        if stop_id in cache:
            return cache[stop_id]

        sids = [
            self.stops[s]["_sid"]
            for s in self.sibling_stops(stop_id)
            if s in self.stops
        ]
        if not sids:
            cache[stop_id] = {}
            return {}

        placeholders = ",".join("?" * len(sids))
        rows = self._con.execute(
            f"""SELECT st.dep_secs, t.service_id, r.short_name
                  FROM stop_times st
                  JOIN trips  t ON t.tid = st.tid
                  JOIN routes r ON r.rid = t.rid
                 WHERE st.sid IN ({placeholders})""",
            sids,
        ).fetchall()

        # Measured against a concrete week, not against calendar weekday flags:
        # overlapping calendars describe one bus several times over, and summing
        # them reports a stop as busier than any stop in Britain. See runs_on.
        week = self.sample_week()
        runs = {}   # (service_id, day) -> bool, memoised across the row scan

        out = {}
        for day in self._DAY_COLS:
            first = last = None
            count = night = 0
            routes = set()
            for dep, service_id, short_name in rows:
                key = (service_id, day)
                if key not in runs:
                    runs[key] = self.runs_on(service_id, week[day])
                if not runs[key]:
                    continue
                count += 1
                if short_name:
                    routes.add(short_name)
                if dep is None:
                    continue
                # Small-hours departures are counted separately, not folded into
                # the span. Feeds are inconsistent about them: the same 00:13
                # trip is written as 24:13 by one operator and 00:13 by another,
                # and taking the raw minimum reports a stop whose first morning
                # bus is 05:40 as starting at ten past midnight. Both forms land
                # in `night` here, so `first` and `last` describe the ordinary
                # service day and mean the same thing at every stop.
                if dep < NIGHT_ENDS_SECS or dep >= DAY_SECS:
                    night += 1
                    continue
                if first is None or dep < first:
                    first = dep
                if last is None or dep > last:
                    last = dep
            out[day] = None if count == 0 else {
                "date":   week[day].isoformat(),
                "first":  _span_hhmm(first),
                "last":   _span_hhmm(last),
                "count":  count,
                "night":  night,
                "routes": sorted(routes, key=_route_sort_key),
            }
        cache[stop_id] = out
        return out

    def has_stop_times(self, stop_id: str) -> bool:
        return stop_id in self.stops_with_times

    def sample_stop_ids_with_times(self, n: int = 10) -> list:
        it = iter(self.stops_with_times)
        return [next(it) for _ in range(min(n, len(self.stops_with_times)))]

    def night_serving_stop_ids(self) -> frozenset:
        """Stop ids served by at least one route whose short_name matches
        N + digit (Brighton & Hove night services and similar). Computed
        once per Timetable lifetime (data is read-only)."""
        cached = getattr(self, "_night_stops_cache", None)
        if cached is not None:
            return cached
        if self._con is None:
            return frozenset()
        ids = frozenset(row[0] for row in self._con.execute("""
            SELECT DISTINCT s.stop_id
            FROM stop_times st
            JOIN trips  t ON t.tid = st.tid
            JOIN routes r ON r.rid = t.rid
            JOIN stops  s ON s.sid = st.sid
            WHERE r.short_name GLOB 'N[0-9]*'
        """))
        self._night_stops_cache = ids
        return ids

    # Coach / school / unwanted services that pollute the route filter
    # in the Improvements view. Matched against GTFS `route_short_name`
    # exactly (case-sensitive).
    _EXCLUDED_SERVICES = frozenset({
        "025", "B25", "VC3",  # National Express + odd one-offs
        "59", "59A", "100",   # not relevant to Adur & Worthing
    })

    # Padding (in degrees, ~2 km at this latitude) added to the bbox when
    # clipping polylines, so lines don't terminate abruptly at the edge of
    # the visible area.
    _CLIP_PADDING_DEG = 0.02

    # Hand-curated list of routes that primarily serve the Adur & Worthing
    # area, regardless of how far they extend in either direction. The
    # frontend uses this for the "Focused" service-type filter chip.
    _FOCUSED_SHORT_NAMES = frozenset({
        "2", "2B", "9", "16", "19", "19A", "46", "69", "106",
        "700", "701", "740", "743",
    })

    # National Operator Codes (NOCs) collapse into a small set of UI
    # buckets so the operator filter strip stays readable. Any NOC not
    # listed here falls into "OTHER".
    _OPERATOR_BUCKETS = {
        "BHBC": "BHBC",
        "SCSO": "SCSO", "SCSC": "SCSO",
        "COMT": "COMT", "CMPA": "COMT",
    }

    # Hand-curated operator fixes, keyed by route short_name, for routes whose
    # GTFS agency NOC doesn't match the operator that actually runs them.
    # Route 2 is Brighton & Hove (live SIRI reports operator_ref=BHBC) but its
    # timetable agency NOC buckets to OTHER.
    # Routes 47 and 60 each have two route rows: the operator serving our bbox
    # (47=Compass/COMT around Brighton-Hove, 60=Brighton & Hove/BHBC around
    # Shoreham-Fishersgate) plus an unrelated Stagecoach/SCSO service of the
    # same number over in Chichester (lon ~-0.79, entirely outside our area).
    # _noc_by_short_name is last-row-wins, so the Chichester SCSO row wins and
    # mislabels them as Stagecoach — pin them to the correct local operator.
    # TODO: confirm the underlying NOC (the routes table has two rows for "2",
    # and _noc_by_short_name is last-row-wins) via a debug dump, then fix it at
    # the NOC level (_OPERATOR_BUCKETS / NOC selection) instead of here.
    _OPERATOR_OVERRIDES = {
        "2": "BHBC",
        "47": "COMT",
        "60": "BHBC",
    }

    _DAY_COLS = ("monday", "tuesday", "wednesday", "thursday",
                 "friday", "saturday", "sunday")
    _DAY_SHORT = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")

    def representative_polylines(
        self,
        bbox: Optional[tuple] = None,
    ) -> list:
        """Return up to two indicative polylines per route short_name.

        Strategy: for each route, pick the trip with the most stops as the
        primary polyline; then pick the longest trip whose first stop is
        geographically close to the primary's last stop as the reverse-
        direction polyline. This collapses dozens of short-turn / partial
        variants into one or two clean lines per route.

        Polylines are stop-to-stop straight lines (no GTFS shapes yet).

        bbox: optional (min_lat, max_lat, min_lon, max_lon) — each polyline
        is clipped to the longest contiguous run of points falling inside
        a slightly padded bbox, so through-routes (700, etc.) only show
        their Adur & Worthing arc instead of stretching to Brighton or
        Portsmouth.
        """
        if self._con is None:
            return []

        stop_counts = dict(self._con.execute(
            "SELECT tid, COUNT(*) FROM stop_times GROUP BY tid"
        ))

        stop_info = {
            sid: (lat, lon, name)
            for sid, lat, lon, name in self._con.execute(
                "SELECT sid, lat, lon, name FROM stops"
            )
        }
        stop_coords = {sid: (lat, lon) for sid, (lat, lon, _name) in stop_info.items()}

        # Pre-compute the set of trips that actually touch the bbox, so
        # route 46 (which has both a Southwick variant and a Bognor-area
        # variant under the same short_name) only considers the trip
        # variants that belong in our map.
        bbox_trip_ids: Optional[set] = None
        if bbox is not None:
            min_lat, max_lat, min_lon, max_lon = bbox
            bbox_trip_ids = {
                row[0] for row in self._con.execute(
                    "SELECT DISTINCT st.tid FROM stop_times st "
                    "JOIN stops s ON s.sid = st.sid "
                    "WHERE s.lat BETWEEN ? AND ? AND s.lon BETWEEN ? AND ?",
                    (min_lat, max_lat, min_lon, max_lon),
                )
            }

        trips_by_route: dict = {}
        for short_name, tid, first_sid, last_sid in self._con.execute(
            "SELECT short_name, tid, first_sid, last_sid FROM trip_endpoints"
        ):
            if short_name in self._EXCLUDED_SERVICES:
                continue
            if bbox_trip_ids is not None and tid not in bbox_trip_ids:
                continue
            trips_by_route.setdefault(short_name, []).append(
                (tid, first_sid, last_sid)
            )

        # short_name -> NOC. Built once; defensive about the `noc` column
        # not existing in older SQLite builds (returns "" in that case).
        noc_by_short = self._noc_by_short_name()

        # ~1 km cutoff in squared degrees (rough at this latitude — fine
        # for grouping terminus stops that share a stand)
        TERMINUS_NEAR_SQ = 0.0001

        # Set of trip ids that have a GTFS or OSRM shape — used as a
        # tie-breaker so we prefer a road-following trip over an equally
        # long stop-to-stop trip. Tolerates older blobs without the
        # shape_id column by leaving the set empty (sort key still works).
        try:
            shaped_tids = {row[0] for row in self._con.execute(
                "SELECT tid FROM trips WHERE shape_id != ''")}
        except sqlite3.OperationalError:
            shaped_tids = set()

        out = []
        for short_name, trips in trips_by_route.items():
            trips.sort(
                key=lambda t: (t[0] in shaped_tids, stop_counts.get(t[0], 0)),
                reverse=True,
            )
            primary_tid, primary_first, primary_last = trips[0]
            primary_last_coord = stop_coords.get(primary_last)

            chosen_trips = [(primary_tid, primary_first, primary_last)]
            if primary_last_coord is not None:
                for tid, first_sid, last_sid in trips[1:]:
                    if first_sid == primary_first:
                        continue
                    fc = stop_coords.get(first_sid)
                    if fc is None:
                        continue
                    dlat = fc[0] - primary_last_coord[0]
                    dlon = fc[1] - primary_last_coord[1]
                    if dlat * dlat + dlon * dlon <= TERMINUS_NEAR_SQ:
                        chosen_trips.append((tid, first_sid, last_sid))
                        break  # already sorted by stop count desc

            # Pre-fetch headsigns for the chosen trips so we can surface
            # the front-of-bus destination (the only reliable name source
            # for off-map routes like N29 → Lewes — the last in-bbox stop
            # is just an edge stop).
            chosen_tids = [t[0] for t in chosen_trips]
            placeholders = ",".join("?" * len(chosen_tids))
            chosen_headsigns = dict(self._con.execute(
                f"SELECT tid, headsign FROM trips WHERE tid IN ({placeholders})",
                chosen_tids,
            )) if chosen_tids else {}
            primary_tid = chosen_trips[0][0]
            reverse_tid = chosen_trips[1][0] if len(chosen_trips) > 1 else None
            primary_headsign = chosen_headsigns.get(primary_tid) or None
            reverse_headsign = chosen_headsigns.get(reverse_tid) or None

            polylines = []
            endpoints = []
            for poly_idx, (tid, first_sid, last_sid) in enumerate(chosen_trips):
                # Prefer the trip's GTFS shape (road-following) over the
                # stop-to-stop chord when available. shape_id is populated
                # for ~50% of trips in the BODS South-East feed; OSRM
                # fills the rest at build time as "osrm:{tid}" shapes.
                # Older SQLite blobs lack the shapes table / shape_id
                # column — OperationalError there silently degrades.
                pts = self._shape_points_for_trip(tid)
                used_fallback = pts is None or len(pts) < 2
                if used_fallback:
                    pts = self._con.execute(
                        "SELECT s.lat, s.lon FROM stop_times st "
                        "JOIN stops s ON s.sid = st.sid "
                        "WHERE st.tid=? ORDER BY st.seq", (tid,)
                    ).fetchall()
                if len(pts) < 2:
                    continue
                lost_before = lost_after = False
                if bbox is not None:
                    pts, lost_before, lost_after = self._clip_to_bbox(pts, bbox)
                    if len(pts) < 2:
                        continue
                # Day/express routes only carry an end-name when clipped at
                # the bbox edge ("continues off-map to X"). Night routes get
                # the terminus name unconditionally so their destination is
                # visible even when the whole loop sits inside the bbox.
                is_night = (len(short_name) > 1
                            and short_name[0].upper() == "N"
                            and short_name[1].isdigit())
                from_name = (stop_info[first_sid][2] if first_sid in stop_info
                             and (lost_before or is_night) else None)
                to_name   = (stop_info[last_sid][2]  if last_sid in stop_info
                             and (lost_after  or is_night) else None)
                # poly 0 = primary trip's geometry; to_headsign is the
                # primary's destination, from_headsign is the reverse
                # trip's destination (= where the primary direction
                # starts). Mirror for poly 1.
                if poly_idx == 0:
                    to_hs, from_hs = primary_headsign, reverse_headsign
                else:
                    to_hs, from_hs = reverse_headsign, primary_headsign
                outer_endpoints = {
                    "from_name":     from_name,
                    "to_name":       to_name,
                    "from_headsign": from_hs if (lost_before or is_night) else None,
                    "to_headsign":   to_hs   if (lost_after  or is_night) else None,
                }
                # Chord-hide safety net: if a fallback (stop-to-stop)
                # polyline contains an implausibly long gap between
                # consecutive points (>2 km), split it. Catches GTFS
                # data quirks where a trip's stop_times skips a chunk
                # of road (e.g. route 46's old 8.21 km chord). Do NOT
                # apply to shaped/OSRM polylines — express routes
                # legitimately have long road segments without points.
                if used_fallback:
                    segments = _split_long_chords([(la, lo) for la, lo in pts])
                else:
                    segments = [[(la, lo) for la, lo in pts]]
                for seg_idx, seg in enumerate(segments):
                    polylines.append([[la, lo] for la, lo in seg])
                    is_first = seg_idx == 0
                    is_last  = seg_idx == len(segments) - 1
                    endpoints.append({
                        "from_name":     outer_endpoints["from_name"]     if is_first else None,
                        "to_name":       outer_endpoints["to_name"]       if is_last  else None,
                        "from_headsign": outer_endpoints["from_headsign"] if is_first else None,
                        "to_headsign":   outer_endpoints["to_headsign"]   if is_last  else None,
                    })

            if polylines:
                noc = noc_by_short.get(short_name, "")
                out.append({
                    "service":   short_name,
                    "polylines": polylines,
                    "endpoints": endpoints,
                    "category":  self._categorise(short_name),
                    "operator":  (self._OPERATOR_OVERRIDES.get(short_name)
                                  or self._operator_bucket(noc)),
                    "frequency": self.service_frequency(short_name, bbox_trip_ids),
                })

        return sorted(out, key=lambda x: x["service"])

    @classmethod
    def _categorise(cls, short_name: str) -> str:
        """Bucket a route into 'focused', 'express', or 'other'.

        Night variants (N + digits) inherit the day route's category, so
        N700 lands in 'focused' alongside 700.
        """
        s = str(short_name or "")
        # Strip leading "N" if followed by digits — N700 → 700, N1 → 1.
        stripped = s[1:] if (len(s) > 1 and s[0].upper() == "N" and s[1].isdigit()) else s
        if stripped in cls._FOCUSED_SHORT_NAMES:
            return "focused"
        if s and s[-1].upper() == "X":
            return "express"
        return "other"

    @classmethod
    def _operator_bucket(cls, noc: str) -> str:
        if not noc:
            return ""
        return cls._OPERATOR_BUCKETS.get(noc, "OTHER")

    def _noc_by_short_name(self) -> dict:
        """Build a {short_name: noc} map. Tolerates older SQLite files
        without the `noc` column (returns an empty dict in that case).
        """
        if self._con is None:
            return {}
        try:
            cols = {row[1] for row in self._con.execute("PRAGMA table_info(routes)")}
        except sqlite3.Error:
            return {}
        if "noc" not in cols:
            return {}
        out: dict = {}
        for short, noc in self._con.execute(
            "SELECT short_name, noc FROM routes WHERE COALESCE(noc, '') <> ''"
        ):
            # If multiple route_ids share a short_name with different
            # operators, the last one wins — typically they all match.
            out[short] = noc
        return out

    def _shape_points_for_trip(self, tid: int) -> Optional[list]:
        """Return road-following polyline points for a trip, or None if
        the trip has no shape_id, the shape has no points, or the SQLite
        blob predates the shapes table / shape_id column.
        """
        try:
            row = self._con.execute(
                "SELECT shape_id FROM trips WHERE tid=?", (tid,)
            ).fetchone()
            if not row or not row[0]:
                return None
            pts = self._con.execute(
                "SELECT lat, lon FROM shapes WHERE shape_id=? ORDER BY seq",
                (row[0],),
            ).fetchall()
            return pts if pts else None
        except sqlite3.OperationalError:
            # Older blob without trips.shape_id or shapes table — degrade
            # silently. Next weekly workflow run replaces the artifact.
            return None

    @classmethod
    def _clip_to_bbox(cls, pts: list, bbox: tuple) -> tuple:
        """Return ``(best_run, lost_before, lost_after)``.

        ``best_run`` is the longest contiguous run of points falling inside
        the padded bbox; the bools indicate whether the original polyline
        had points dropped before / after that kept run, so the caller can
        annotate the truncated end with where the route was heading.
        """
        min_lat, max_lat, min_lon, max_lon = bbox
        pad = cls._CLIP_PADDING_DEG
        min_lat -= pad; max_lat += pad
        min_lon -= pad; max_lon += pad

        runs: list = []   # list of (start_idx, end_idx) inclusive
        cur_start: Optional[int] = None
        for i, (lat, lon) in enumerate(pts):
            inside = min_lat <= lat <= max_lat and min_lon <= lon <= max_lon
            if inside and cur_start is None:
                cur_start = i
            elif not inside and cur_start is not None:
                runs.append((cur_start, i - 1))
                cur_start = None
        if cur_start is not None:
            runs.append((cur_start, len(pts) - 1))

        if not runs:
            return [], False, False

        start_idx, end_idx = max(runs, key=lambda r: r[1] - r[0])
        kept = [(pts[i][0], pts[i][1]) for i in range(start_idx, end_idx + 1)]
        return kept, start_idx > 0, end_idx < len(pts) - 1
