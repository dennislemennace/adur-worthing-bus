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

    def has_stop_times(self, stop_id: str) -> bool:
        return stop_id in self.stops_with_times

    def sample_stop_ids_with_times(self, n: int = 10) -> list:
        it = iter(self.stops_with_times)
        return [next(it) for _ in range(min(n, len(self.stops_with_times)))]

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
    # TODO: confirm the underlying NOC (the routes table has two rows for "2",
    # and _noc_by_short_name is last-row-wins) via a debug dump, then fix it at
    # the NOC level (_OPERATOR_BUCKETS / NOC selection) instead of here.
    _OPERATOR_OVERRIDES = {
        "2": "BHBC",
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
                # for ~50% of trips in the BODS South-East feed; the rest
                # fall back to straight stops. Older SQLite blobs lack the
                # shapes table / shape_id column — OperationalError there
                # silently degrades to the fallback path.
                pts = self._shape_points_for_trip(tid)
                if pts is None or len(pts) < 2:
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
                polylines.append([[lat, lon] for lat, lon in pts])
                # Day/express routes only carry an end-name when clipped at
                # the bbox edge ("continues off-map to X"). Night routes get
                # the terminus name unconditionally so their destination is
                # visible even when the whole loop sits inside the bbox —
                # the disparity between Brighton's east-bound night network
                # and Worthing's bare west-bound picture only reads on the
                # map if each pill names its destination.
                is_night = (len(short_name) > 1
                            and short_name[0].upper() == "N"
                            and short_name[1].isdigit())
                from_name = (stop_info[first_sid][2] if first_sid in stop_info
                             and (lost_before or is_night) else None)
                to_name   = (stop_info[last_sid][2]  if last_sid in stop_info
                             and (lost_after  or is_night) else None)
                # For poly 0 (primary trip): to_headsign = the primary's
                # destination text, from_headsign = the reverse trip's
                # destination (= where the primary direction starts).
                # Mirror for poly 1.
                if poly_idx == 0:
                    to_hs, from_hs = primary_headsign, reverse_headsign
                else:
                    to_hs, from_hs = reverse_headsign, primary_headsign
                endpoints.append({
                    "from_name":     from_name,
                    "to_name":       to_name,
                    "from_headsign": from_hs if (lost_before or is_night) else None,
                    "to_headsign":   to_hs   if (lost_after  or is_night) else None,
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
