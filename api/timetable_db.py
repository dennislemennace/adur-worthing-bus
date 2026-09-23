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
import re
import sqlite3
import threading
import time
import shutil
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
# The database is ~63 MB. Generous, but bounded: `urlretrieve` had no timeout
# at all, so a stalled transfer blocked the hourly refresh — and with it every
# request sharing the event loop — for as long as the socket stayed open.
_DOWNLOAD_TIMEOUT = 120  # seconds


_DAY_COLUMNS = ("monday", "tuesday", "wednesday", "thursday",
                "friday", "saturday", "sunday")


def service_runs_on(calendar: dict, calendar_dates: dict,
                    service_id: str, day) -> bool:
    """Whether a GTFS service runs on `day`. The single answer to that question.

    There were two implementations of this, and they disagreed. This one is
    the strict reading, and it is the correct one:

      * `calendar_dates` wins over `calendar`, which is what makes a bank
        holiday behave like a Sunday;
      * a service with no `calendar` row does **not** therefore run every day.
        GTFS permits a service defined only by `calendar_dates`, and a date
        that is not listed is the absence of permission to run, not a gap to
        fill in optimistically. The API's departure boards used to return True
        here, inventing service on every unlisted date for every such trip.
    """
    stamp = day.strftime("%Y%m%d")
    exceptions = calendar_dates.get(service_id) or {}
    if stamp in exceptions:
        return str(exceptions[stamp]) == "1"
    cal = calendar.get(service_id)
    if not cal:
        return False
    start, end = cal.get("start_date", ""), cal.get("end_date", "")
    if start and stamp < start:
        return False
    if end and stamp > end:
        return False
    return str(cal.get(_DAY_COLUMNS[day.weekday()])) == "1"


# The tables and content a file must have before it is allowed to become the
# live timetable. Checked in full: a truncated download, an HTML error page
# saved under a .sqlite name, and a half-built release asset all open without
# complaint under SQLite's lazy schema reading, and all of them used to be
# swapped straight in on top of a working database.
_REQUIRED_TABLES = ("stops", "routes", "trips", "stop_times")


def db_is_usable(path) -> bool:
    """Whether `path` is a timetable this service can serve from.

    Deliberately strict, and deliberately cheap: an integrity check on a 63 MB
    file at every startup would cost more than it saves, so this asks the
    questions that separate a real timetable from the things that actually go
    wrong — is it a database, does it have the tables, does it have any
    departures in it.
    """
    path = Path(path)
    if not path.exists() or path.stat().st_size < 1024:
        return False
    con = None
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        names = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if not set(_REQUIRED_TABLES).issubset(names):
            log.error("Candidate timetable is missing tables: %s",
                      sorted(set(_REQUIRED_TABLES) - names))
            return False
        for table in ("stops", "routes", "trips", "stop_times"):
            if con.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is None:
                log.error("Candidate timetable has an empty %s table", table)
                return False
        return True
    except sqlite3.Error as exc:
        log.error("Candidate timetable is not a usable database: %s", exc)
        return False
    finally:
        if con is not None:
            try:
                con.close()
            except sqlite3.Error:
                pass


class Timetable:
    def __init__(self, db_path: Path, allow_fetch: bool = True):
        self.db_path = db_path
        # The API downloads the published database when its copy is missing or
        # stale. A build script must not: it runs immediately after
        # json_to_sqlite.py has written a *newer* file, whose hash necessarily
        # differs from the released sidecar, and the freshness check would read
        # that as "stale" and overwrite the thing being published.
        self._allow_fetch = allow_fetch
        self._timepoint_column: Optional[bool] = None
        self._lock = threading.Lock()
        # One read-only connection per thread, not one shared across all of
        # them. The queries here run in a worker thread now (see the API's
        # route handlers), and a single connection handed round a threadpool
        # is a lifetime nobody had examined: it happens to work for read-only
        # traffic under CPython, which is not the same as being designed.
        #
        # `_generation` invalidates them. A thread that opened its connection
        # before a reload is holding one to the previous file, so it reopens
        # on its next use rather than serving from a database that has been
        # replaced underneath it.
        self._local = threading.local()
        self._generation = 0
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
        # `_noc_by_rid` is the per-route map that noc_for_route() prefers.
        self._noc_map: Optional[dict] = None
        self._noc_by_rid: Optional[dict] = None
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
            # A timeout, because a stalled download on a shared event loop
            # holds up every other request behind it indefinitely.
            with urllib.request.urlopen(TIMETABLE_URL, timeout=_DOWNLOAD_TIMEOUT) as resp:
                with tmp.open("wb") as out:
                    shutil.copyfileobj(resp, out, length=1 << 20)

            # Verified *before* the swap. This used to be `urlretrieve` then
            # `tmp.replace(db_path)` with nothing in between, so a truncated
            # transfer or an error page saved under a .sqlite name replaced a
            # working timetable with something unusable — and the last known
            # good copy was already gone by the time anyone noticed.
            if not db_is_usable(tmp):
                log.error("Downloaded timetable failed verification — keeping "
                          "the existing file")
                tmp.unlink(missing_ok=True)
                return False

            tmp.replace(self.db_path)
            log.info("Timetable DB downloaded and verified: %d bytes",
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

    def _open_connection(self) -> Optional[sqlite3.Connection]:
        if not self.db_path.exists():
            return None
        con = sqlite3.connect(
            f"file:{self.db_path}?mode=ro",
            uri=True,
            check_same_thread=False,
        )
        con.execute("PRAGMA query_only = 1")
        con.execute("PRAGMA temp_store = MEMORY")
        return con

    def _conn(self) -> Optional[sqlite3.Connection]:
        """This thread's read-only connection, or None if there is no database.

        Opened on first use and reopened after a reload. Cheap: SQLite's
        connect is a file open, and the page cache it matters for is the OS's.
        """
        if self._con is None:
            return None
        gen = self._generation
        cached = getattr(self._local, "con", None)
        if cached is not None and getattr(self._local, "gen", None) == gen:
            return cached
        if cached is not None:
            try:
                cached.close()
            except sqlite3.Error:
                pass
        con = self._open_connection()
        self._local.con = con
        self._local.gen = gen
        return con

    def _clear_derived(self) -> None:
        """Drop everything computed from the database file.

        One method rather than a list at each call site, because the list was
        incomplete: `_span_cache` was not on it, so service spans — sampled
        from a week of the timetable — survived `reload()` and went on
        describing a timetable that had been replaced.
        """
        self._noc_map = None
        self._noc_by_rid = None
        self._stops_by_name = None
        self._span_cache = {}
        self._hub_sids_cache = None
        self._last_bus_cache = {}

    def _open_and_preload(self) -> None:
        if self._allow_fetch:
            self._ensure_fresh()
        # Drop memoized derivations — reload() lands here with a new DB file.
        self._clear_derived()
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
        # `locality` arrived after the first databases were built, so it is
        # read only where the column exists — an older release asset must not
        # crash a newer API on a field it has never heard of.
        present = {r[1] for r in con.execute("PRAGMA table_info(stops)")}
        extra = [c for c in ("locality", "locality_parent") if c in present]
        columns = ", ".join(["sid", "stop_id", "name", "lat", "lon"] + extra)
        for row in con.execute(f"SELECT {columns} FROM stops"):
            sid, stop_id, name, lat, lon = row[:5]
            rest = dict(zip(extra, row[5:]))
            stops[stop_id] = {"name": name, "lat": lat, "lon": lon, "_sid": sid,
                              "locality": rest.get("locality", ""),
                              "locality_parent": rest.get("locality_parent", "")}
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
            self._generation += 1     # every thread's connection is now stale
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
            for dep_secs, tid in self._conn().execute(
                "SELECT dep_secs, tid FROM stop_times "
                "WHERE sid=? ORDER BY dep_secs",
                (sid,),
            )
        ]

    def timepoints_for(self, trip_id: str) -> dict:
        """`{stop_id: 1 | 0 | None}` — which of a trip's times the operator commits to.

        1 is the operator's own timing point, 0 a time GTFS interpolated
        between two of them, None the feed not saying. Punctuality at an
        interpolated stop partly measures the interpolation, so anything
        published has to keep them apart.

        Returns an empty dict against a database built before the column
        existed — the live one is rebuilt weekly, and a caller must treat
        "no answer" as "unstated" rather than assume a timing point.
        """
        if self._con is None or not self._has_timepoint():
            return {}
        trip = self.trips.get(trip_id)
        if not trip:
            return {}
        sid_to_stop = self._sid_to_stop
        return {
            sid_to_stop[sid]: tp
            for sid, tp in self._conn().execute(
                "SELECT sid, timepoint FROM stop_times WHERE tid=? ORDER BY seq",
                (trip["_tid"],),
            )
            if sid in sid_to_stop
        }

    def timepoints_by_call(self, trip_id: str) -> list:
        """Timing-point flags in call order, preserving repeated stop visits."""
        trip = self.trips.get(trip_id)
        if not trip or self._con is None:
            return []
        column = "timepoint" if self._has_timepoint() else "NULL"
        return [row[0] for row in self._conn().execute(
            f"SELECT {column} FROM stop_times WHERE tid=? ORDER BY seq", (trip["_tid"],))]

    def _has_timepoint(self) -> bool:
        """Whether this database has the timing-point column. Asked once."""
        if self._timepoint_column is None:
            try:
                cols = {r[1] for r in self._conn().execute("PRAGMA table_info(stop_times)")}
            except sqlite3.Error:
                cols = set()
            self._timepoint_column = "timepoint" in cols
        return self._timepoint_column

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
            for dep_secs, sid in self._conn().execute(
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

    def stop_list(self, bbox) -> list:
        """Every stop inside `bbox` that has a scheduled departure.

        The single source of the stop list. `/api/stops` serves it and
        `scripts/build_stops_json.py` writes it to a static file for GitHub
        Pages, so a visitor gets the map without waiting for a free-tier
        container to wake. Two implementations would have drifted the first
        time either was touched, and the static one would have drifted
        silently.

        `bbox` is (min_lat, max_lat, min_lon, max_lon).
        """
        min_lat, max_lat, min_lon, max_lon = bbox
        night = self.night_serving_stop_ids()
        stops = []
        for stop_id, s in self.stops.items():
            lat, lon = s.get("lat"), s.get("lon")
            if lat is None or lon is None:
                continue
            if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
                continue
            if not self.has_stop_times(stop_id):
                continue
            stops.append({
                "atco_code":     stop_id,
                "name":          s.get("name") or "Bus Stop",
                "latitude":      lat,
                "longitude":     lon,
                "night_serving": stop_id in night,
            })

        # Direction and services, but only where the name alone is ambiguous.
        # Colebrook Road and Shoreham Port each have two poles a hundred
        # metres apart under one name, so a search showing a single entry
        # sends half its users to the opposite kerb. Where a name is unique
        # there is nothing to disambiguate, and carrying the fields for all
        # 5,000 stops would pad a file the browser downloads on every visit.
        by_name: dict = {}
        for entry in stops:
            by_name.setdefault(entry["name"], []).append(entry)
        ambiguous = [e for g in by_name.values() if len(g) > 1 for e in g]
        if ambiguous:
            directions = self.stop_directions([e["atco_code"] for e in ambiguous])
            for entry in ambiguous:
                d = directions.get(entry["atco_code"])
                if not d:
                    continue
                if d["towards"]:
                    entry["towards"] = d["towards"]
                if d["services"]:
                    entry["services"] = d["services"]
        return stops

    def stop_directions(self, stop_ids) -> dict:
        """For each stop: the services calling there and where they head.

        Answers the question a passenger asks of a shelter — "which way does
        this one go?" — for the stops where it matters. Two poles of the same
        road carry the same name and sit 100 m apart, so a search that shows
        one entry sends half its users across the road.

        Direction comes from the trip headsigns rather than a compass bearing
        off the coordinates: "towards Old Steine" is what the reader is
        deciding between, and it is what the flag on the bus says. The most
        frequent headsign wins, because a pole's occasional short-workings
        should not rename it.

        Returns {stop_id: {"services": [...], "towards": str}}.
        """
        con = self._conn()
        if con is None or not stop_ids:
            return {}

        wanted = {sid for sid in stop_ids if sid in self.stops}
        if not wanted:
            return {}
        by_sid = {self.stops[sid]["_sid"]: sid for sid in wanted}

        out: dict = {}
        # One pass over the stop_times for these stops. `idx_stop_times_stop`
        # makes each lookup a covering-index scan.
        placeholders = ",".join("?" * len(by_sid))
        rows = con.execute(
            f"""SELECT st.sid, r.short_name, t.headsign
                  FROM stop_times st
                  JOIN trips  t ON t.tid = st.tid
                  JOIN routes r ON r.rid = t.rid
                 WHERE st.sid IN ({placeholders})""",
            list(by_sid.keys()),
        )
        tally: dict = {}
        for sid, short_name, headsign in rows:
            stop_id = by_sid.get(sid)
            if stop_id is None:
                continue
            entry = tally.setdefault(stop_id, {"services": set(), "heads": {}})
            if short_name:
                entry["services"].add(short_name)
            if headsign:
                entry["heads"][headsign] = entry["heads"].get(headsign, 0) + 1

        for stop_id, entry in tally.items():
            heads = entry["heads"]
            towards = max(heads, key=heads.get) if heads else ""
            out[stop_id] = {
                "services": sorted(entry["services"], key=_route_sort_key),
                "towards": towards,
            }
        return out

    def noc_for_route(self, route_id: str) -> str:
        """Operator NOC for a route, or "" if unknown. The right lookup.

        A route number is not a company. This feed carries 23 short names used
        by more than one operator — Brighton & Hove and Compass both run a
        "5", Compass and Stagecoach both run a "47" — so asking "who runs the
        5?" has no single answer, while asking "who runs *this route*?" always
        does. `noc_for_short_name` below cannot tell them apart and its answer
        fed ticket eligibility, not just a badge colour.

        The routes table is small (a few hundred rows), so the whole map is
        built once and kept.
        """
        if self._noc_by_rid is None:
            self._noc_by_rid = {}
            if self._con is not None:
                for route_id_text, noc in self._conn().execute(
                        "SELECT route_id, noc FROM routes"):
                    self._noc_by_rid[route_id_text] = noc or ""
        noc = self._noc_by_rid.get(route_id, "")
        route = self.routes.get(route_id) or {}
        return self._OPERATOR_OVERRIDES.get(route.get("short_name", ""), noc)

    def noc_for_short_name(self, short_name: str) -> str:
        """Operator NOC for a route short_name, or "" if unknown.

        Last-row-wins across every operator using that number, so prefer
        `noc_for_route` wherever a route or trip is in hand. Kept for the
        callers that genuinely have only a number — a GTFS-RT vehicle
        reporting a line with no trip reference.

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
        for seq, sid, dep_secs in self._conn().execute(
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
        rows = self._conn().execute(
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
        for tid, first_sid, last_sid, first_secs in self._conn().execute(
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
        for tid, first_secs in self._conn().execute(
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

        Thin wrapper over `service_runs_on` so that this class and the API's
        departure boards cannot drift apart — they had, and the API's copy
        answered `True` for a service with no calendar row at all.

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
        return service_runs_on(self.calendar, self.calendar_dates,
                               service_id, day)

    def service_window(self) -> tuple:
        """The first and last dates this build describes any service on.

        A BODS bundle looks forward: the one fetched on 20 September 2026 ran
        from `20260920` to `20270621` and said nothing whatever about the 18th.
        Measuring a day outside this window is not a thin measurement, it is no
        measurement — every service is inactive, nothing is scheduled, nothing
        can be matched, and the day publishes zeroes that every downstream check
        then finds internally consistent. That is exactly how 18 and 19
        September 2026 came to be published empty.

        Returned as GTFS-style compact `YYYYMMDD` strings, or `(None, None)`
        where a build carries no dated calendar at all, so a caller can tell
        "outside the window" from "no window to be outside of" and decline to
        guess. `calendar_dates` is included because GTFS permits a service
        defined only by exception dates, with no `calendar` row to bound it.
        """
        dates = set()
        for cal in self.calendar.values():
            for key in ("start_date", "end_date"):
                value = (cal.get(key) or "").strip()
                if value:
                    dates.add(value)
        for exceptions in self.calendar_dates.values():
            dates.update(d for d, kind in exceptions.items() if d and str(kind) == "1")
        if not dates:
            return (None, None)
        return (min(dates), max(dates))

    def covers_day(self, day, service_ids=None) -> bool:
        """A relevant calendar actually describes this date, not its envelope.

        A bounded calendar describes non-running weekdays too. Exception-only
        service describes its addition dates, not all intervening dates. A
        removal outside a calendar cannot extend the build's validity.
        Callers can restrict the check to a route/operator's service IDs.
        """
        stamp = day.strftime("%Y%m%d") if hasattr(day, "strftime") else str(day).replace("-", "")
        ids = (set(self.calendar) | set(self.calendar_dates)) if service_ids is None else set(service_ids)
        for sid in ids:
            cal = self.calendar.get(sid, {})
            first, last = cal.get("start_date"), cal.get("end_date")
            if first and last and first <= stamp <= last:
                return True
            if str(self.calendar_dates.get(sid, {}).get(stamp)) == "1":
                return True
        return False

    def uncovered_cohorts(self, day, atcos) -> list:
        """Local operator/service groups whose timetable cannot describe the date.

        A date somewhere in this database must not validate an unrelated route,
        so coverage is asked per operator and service rather than of the build
        as a whole.

        "Uncovered" is not the same as "not running", and the difference is the
        whole point. A bounded calendar that has not started yet is a statement
        about the network: the 25X runs one week from 27 September, so on the
        22nd it is legitimately absent, and blocking publication over it would
        stop the pipeline every night until term begins. What cannot be
        published is a date the timetable is unable to speak about at all,
        because that is how two days came to be published with no observations
        and read as "no service".

        So a cohort blocks only when it is uncovered and one of:

          * no local cohort at all describes the date — the build is the wrong
            one for this day, whatever an unrelated calendar may span; or
          * this cohort's own dates have run out before it — its operator's
            data is stale inside an otherwise current build, which would
            publish a false zero for a service that really ran.
        """
        from api.trip_match import COACH_NOCS
        trip_ids = {trip for atco in atcos for _secs, trip in self.stop_times_for(atco)}
        cohorts = {}
        for tid in trip_ids:
            trip = self.trips.get(tid, {})
            route_id = trip.get("route_id", "")
            noc = self.noc_for_route(route_id)
            if noc in COACH_NOCS:
                continue
            key = (noc, self.routes.get(route_id, {}).get("short_name", ""))
            cohorts.setdefault(key, set()).add(trip.get("service_id", ""))

        uncovered = [(key, ids) for key, ids in sorted(cohorts.items())
                     if not self.covers_day(day, ids)]
        if not uncovered:
            return []
        stamp = (day.strftime("%Y%m%d") if hasattr(day, "strftime")
                 else str(day).replace("-", ""))
        nothing_local_covers = len(uncovered) == len(cohorts)
        return [list(key) for key, ids in uncovered
                if nothing_local_covers or self._cohort_ends_before(ids, stamp)]

    def _cohort_ends_before(self, service_ids, stamp: str) -> bool:
        """Whether every date these services describe falls before `stamp`.

        The last date a cohort can speak about is the latest of its calendar
        end dates and its added exception dates. A cohort with no dated
        calendar at all describes nothing, which is the original failure this
        guard exists for, so it counts as ended.
        """
        last = ""
        for sid in service_ids:
            end = (self.calendar.get(sid, {}) or {}).get("end_date") or ""
            last = max(last, end)
            for date_stamp, kind in (self.calendar_dates.get(sid, {}) or {}).items():
                if str(kind) == "1" and date_stamp:
                    last = max(last, date_stamp)
        return last < stamp

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

    # A change on foot. Two poles of one road share a name and are caught by
    # sibling_stops, but a real interchange often is not, and Portslade Station
    # is the case that set this number: the 1X stops around the corner from the
    # 46, while the 1 is five minutes down the road. Both are changes people
    # make, so the radius has to reach the further one — with the walk penalised
    # in scoring so the corner beats the road when both connect.
    INTERCHANGE_WALK_KM = 0.4
    WALK_METRES_PER_SEC = 1.35

    # What a second operator costs, in the only currency this search counts.
    #
    # Changing between two companies means buying a second ticket, because no
    # single operator's day ticket is valid on the other's bus. This site exists
    # partly to say so, and it was saying the opposite by accident: Shoreham
    # High Street to Park Road is a journey both ends of which sit inside
    # citySAVER, answerable with a 2 and a 25 on one Brighton & Hove ticket, and
    # the search offered a 700 then a 5B because that pairing happened to be a
    # few minutes quicker. A reader sees a Stagecoach bus beside a Brighton &
    # Hove bus and concludes, reasonably, that they need two tickets.
    #
    # Fifteen minutes is the trade this encodes: a one-operator itinerary wins
    # unless it is more than a quarter of an hour slower, at which point the
    # second fare is arguably the better bargain and the reader can see both
    # facts anyway. It is a preference, not a filter — where no single operator
    # runs the journey, as from Worthing to Hangleton, nothing changes.
    TWO_OPERATOR_PENALTY_SECS = 15 * 60

    def interchange_legs(self, from_stop: str, to_stop: str, day,
                         anchor_secs: int = 43200) -> Optional[dict]:
        """The best one-change itinerary, when no bus runs the whole way."""
        found = self._interchange_search(from_stop, to_stop, day, anchor_secs, 1)
        return found[0] if found else None

    def interchange_options(self, from_stop: str, to_stop: str, day,
                            anchor_secs: int = 43200, limit: int = 4) -> list:
        """Several genuinely different one-change itineraries, best first.

        The ticket view cannot say "cheapest" and "quickest" while it is handed
        one route: the cheapest way across a boundary is often a slower bus on
        one operator's ticket, and the quickest is two operators and two fares.
        Showing one of those and calling it the answer hides the trade that this
        whole site is about.

        This costs almost nothing. The search below already enumerates every
        pairing of first and second leg and then throws all but one away; this
        keeps the best few instead. What it must not do is return the same
        journey several times over — the raw enumeration yields dozens of
        near-identical pairings differing by a few minutes — so results are
        deduplicated by which services are ridden and where the change happens,
        keeping the best-scoring of each.

        Memoised on the instance, which is the timetable build: a new build
        makes a new Timetable and the answers go with the old one, so there is
        no version to invalidate against. The six presets are the reason —
        every reader who opens the ticket view presses one, and on Render's
        shared tenth of a CPU the second press should not pay again. Bounded
        and cleared wholesale rather than evicted one at a time; the process is
        killed nightly anyway, so a cold start stays honest about its cost.
        """
        memo = getattr(self, "_itinerary_memo", None)
        if memo is None:
            memo = self._itinerary_memo = {}
        stamp = day.strftime("%Y%m%d") if hasattr(day, "strftime") else str(day)
        key = (from_stop, to_stop, stamp, anchor_secs, limit)
        if key not in memo:
            if len(memo) >= 256:
                memo.clear()
            memo[key] = self._interchange_search(from_stop, to_stop, day,
                                                 anchor_secs, max(1, limit))
        return memo[key]

    def _interchange_search(self, from_stop: str, to_stop: str, day,
                            anchor_secs: int = 43200, limit: int = 1) -> list:
        """The shared body. Returns a list, best score first.

        The fare side of this site can say a journey needs two tickets. It could
        never say what the journey actually *is* — which bus, changing where,
        taking how long — because /api/journey returns direct trips or nothing.
        That is the half a passenger cares about, and the half that makes an
        hour-long two-bus trip legible as the problem it is.

        Deliberately one change, not two. A second change is a different kind of
        journey and searching for it would invite answers nobody would make.

        Two things this gets right that a first version did not, both of which
        made it report no way at all between places people plainly do travel
        between:

        * **Changing costs a walk.** Requiring the identical stop, or even the
          same-named pair of poles, misses the ordinary case of stepping a
          couple of hundred metres to another stop. Southwick to Mile Oak is
          exactly that — the 46 and the 1 meet near Portslade Station without
          sharing a stop.
        * **The first bus to arrive is not always the one to catch.** Keeping
          only the earliest arrival at each stop throws away the later one that
          actually connects. Several are kept and every pairing is tried.

        Trip paths are a primary-key lookup — stop_times is WITHOUT ROWID keyed
        on (tid, seq) — so walking every candidate trip end to end is cheap.
        """
        if self._con is None:
            return []
        MIN_CHANGE_SECS = 4 * 60
        MAX_WAIT_SECS = 60 * 60          # a longer wait is not a connection
        KEEP_ARRIVALS = 8                # per stop, earliest after the anchor

        a_sids = {self.stops[s]["_sid"] for s in self.sibling_stops(from_stop)
                  if s in self.stops}
        b_sids = {self.stops[s]["_sid"] for s in self.sibling_stops(to_stop)
                  if s in self.stops}
        if not a_sids or not b_sids or (a_sids & b_sids):
            return []

        runs: dict = {}

        def trip_runs(tid):
            if tid not in runs:
                trip_id = self._tid_to_trip.get(tid)
                trip = self.trips.get(trip_id) if trip_id else None
                runs[tid] = bool(trip) and self.runs_on(trip.get("service_id", ""), day)
            return runs[tid]

        def paths_from(sids):
            out, seen = [], set()
            for sid in sids:
                for (tid,) in self._conn().execute(
                        "SELECT DISTINCT tid FROM stop_times WHERE sid=?", (sid,)):
                    if tid in seen or not trip_runs(tid):
                        continue
                    seen.add(tid)
                    out.append((tid, self._conn().execute(
                        "SELECT seq, sid, dep_secs FROM stop_times "
                        "WHERE tid=? ORDER BY seq", (tid,)).fetchall()))
            return out

        # Leg one: every stop the first bus can put us at, with several arrival
        # times each, not just the earliest.
        first_leg: dict = {}
        for tid, calls in paths_from(a_sids):
            board = next((c for c in calls
                          if c[1] in a_sids and c[2] is not None
                          and c[2] >= anchor_secs), None)
            if board is None:
                continue
            for c in calls:
                if c[0] <= board[0] or c[2] is None:
                    continue
                first_leg.setdefault(c[1], []).append(
                    {"tid": tid, "board_seq": board[0], "alight_seq": c[0],
                     "depart": board[2], "arrive": c[2]})
        if not first_leg:
            return []
        noc_cache: dict = {}

        def noc_of(tid):
            """Which company runs this trip. Memoised: the loop below asks the
            same question thousands of times, and it is three dict hops."""
            if tid not in noc_cache:
                trip = self.trips.get(self._tid_to_trip.get(tid)) or {}
                # noc_for_route, not routes[...]["noc"]. The in-memory route
                # table carries no noc at all — it is None for every route —
                # so reading it directly returned "" for every trip, every
                # itinerary looked like one operator changing to itself, and
                # the fare penalty became a constant added to every candidate.
                # A constant added to everything ranks nothing: the preference
                # was inert at fifteen minutes and still inert at three hours,
                # which is what gave it away. _describe_leg uses this accessor
                # and has always reported the operator correctly, which is why
                # the output looked right while the scoring was blind.
                #
                # By route, never by short name: noc_for_short_name is
                # last-row-wins, and two operators share numbers here.
                noc_cache[tid] = self.noc_for_route(trip.get("route_id", "")) or ""
            return noc_cache[tid]

        # Keep the earliest arrivals *per operator*, not simply the earliest.
        #
        # Pruning to the eight earliest outright threw the fare-aware choice
        # away before it could be made. Shoreham to the universities is the
        # case: the 700 is the express along the coast, so at every stop the 2
        # and the 700 share, all eight survivors were 700s and the Brighton &
        # Hove option had gone before scoring. The two-operator penalty then
        # looked inert at any size — it was, because nothing was left to prefer.
        #
        # Capped overall so a stop served by a dozen companies cannot turn this
        # back into the cross product it was written to avoid.
        KEEP_TOTAL = 4 * KEEP_ARRIVALS
        for sid in first_leg:
            first_leg[sid].sort(key=lambda x: x["arrive"])
            per_operator: dict = {}
            kept: list = []
            for leg in first_leg[sid]:
                noc = noc_of(leg["tid"])
                seen = per_operator.get(noc, 0)
                if seen >= KEEP_ARRIVALS or len(kept) >= KEEP_TOTAL:
                    continue
                per_operator[noc] = seen + 1
                kept.append(leg)
            first_leg[sid] = kept

        # Where each of those stops is, bucketed into a coarse grid so a walking
        # transfer costs a handful of neighbour lookups rather than a full cross
        # product against every boarding stop on the second leg. Without this the
        # search ran to two and a half seconds on a long coastal pair.
        CELL = 0.005                      # ~0.5 km, comfortably over the walk cap
        coords, grid = {}, {}
        for sid in first_leg:
            stop = self.stops.get(self._sid_to_stop.get(sid)) or {}
            if stop.get("lat") is None:
                continue
            coords[sid] = (stop["lat"], stop["lon"])
            key = (int(stop["lat"] // CELL), int(stop["lon"] // CELL))
            grid.setdefault(key, []).append(sid)

        def nearby(lat, lon):
            cy, cx = int(lat // CELL), int(lon // CELL)
            for dy in (-1, 0, 1):
                for dx in (-1, 0, 1):
                    for sid in grid.get((cy + dy, cx + dx), ()):
                        yield sid

        route_cache: dict = {}

        def route_of(tid):
            """Which published route a trip belongs to — the dedup key.

            Twenty pairings of the same two services four minutes apart are one
            journey to a passenger, so results are collapsed on the route ridden
            rather than the trip, and the best-scoring of each survives.
            """
            if tid not in route_cache:
                trip = self.trips.get(self._tid_to_trip.get(tid)) or {}
                route_cache[tid] = trip.get("route_id", "")
            return route_cache[tid]

        # Best candidate per distinct journey, rather than one best overall.
        candidates: dict = {}
        for tid, calls in paths_from(b_sids):
            arrive = next((c for c in reversed(calls)
                           if c[1] in b_sids and c[2] is not None), None)
            if arrive is None:
                continue
            for c in calls:
                if c[0] >= arrive[0] or c[2] is None:
                    continue
                stop = self.stops.get(self._sid_to_stop.get(c[1])) or {}
                if stop.get("lat") is None:
                    continue
                for sid in set(nearby(stop["lat"], stop["lon"])) | ({c[1]} & first_leg.keys()):
                    xy = coords.get(sid)
                    if xy is None:
                        continue
                    if sid == c[1]:
                        walk_km, walk_secs = 0.0, 0
                    else:
                        walk_km = _haversine_km(xy, (stop["lat"], stop["lon"]))
                        if walk_km > self.INTERCHANGE_WALK_KM:
                            continue
                        walk_secs = int(walk_km * 1000 / self.WALK_METRES_PER_SEC)
                    for one in first_leg[sid]:
                        wait = c[2] - one["arrive"]
                        if wait < MIN_CHANGE_SECS + walk_secs or wait > MAX_WAIT_SECS:
                            continue
                        total = arrive[2] - one["depart"]
                        # Walking to another stop is worth more than the clock
                        # says: it is the part of a change people get wrong, in
                        # rain, with a pushchair. A stop-for-stop change wins
                        # unless the walk genuinely saves time. A second
                        # operator costs more still — it costs a second ticket.
                        one_noc, two_noc = noc_of(one["tid"]), noc_of(tid)
                        fare_penalty = (0 if one_noc and one_noc == two_noc
                                        else self.TWO_OPERATOR_PENALTY_SECS)
                        score = total + walk_secs * 3 + fare_penalty
                        # The services ridden, and nothing else. Including the
                        # change stop looked more precise and was worse: it
                        # returned the 700 then the 49 four times over, once
                        # per stop they happen to meet at, which is one journey
                        # to anybody making it — and it crowded out the routes
                        # that differ in the way that matters, which is which
                        # company you are paying.
                        key = (route_of(one["tid"]), route_of(tid))
                        held = candidates.get(key)
                        if held is None or (total, walk_km) < (held["total_secs"], held["walk_m"] / 1000):
                            candidates[key] = {
                                "score": score, "total_secs": total, "one": one,
                                "from_sid": sid, "to_sid": c[1],
                                "walk_m": round(walk_km * 1000),
                                "two": {"tid": tid, "board_seq": c[0],
                                        "alight_seq": arrive[0],
                                        "depart": c[2], "arrive": arrive[2]},
                                "wait_secs": wait}
        if not candidates:
            return []

        def describe(best):
            change = {
                "change_at": self._stop_place(best["from_sid"]),
                # Where the second bus is actually caught, when it is not the
                # same stop. Named separately because "walk 120 m to Southern
                # Cross" is part of the journey, not a detail.
                "board_at": self._stop_place(best["to_sid"]),
                "walk_metres": best["walk_m"],
                "wait_minutes": best["wait_secs"] // 60,
            }
            return {
                "legs": [self._describe_leg(best["one"]),
                         self._describe_leg(best["two"])],
                **change,
                # The same change again as a list, so a caller can draw any
                # number of changes from one field; see interchange_legs_two.
                "changes": [change],
                "total_minutes": best["total_secs"] // 60,
            }

        # Described lazily: _describe_leg walks the whole stop path of a trip,
        # so describing every candidate when the caller wanted one would make
        # the common case pay for the uncommon one.
        ranked = sorted(candidates.values(), key=lambda x: (x["total_secs"], x["walk_m"], x["score"]))
        # Preserve the fastest and different operator combinations before the
        # display limit. Fares are calculated later against the actual paths.
        diverse, rest, seen_operators = [], [], set()
        for entry in ranked:
            operators = tuple(self.noc_for_route(route_of(entry[k]["tid"])) for k in ("one", "two"))
            if operators not in seen_operators:
                diverse.append(entry)
                seen_operators.add(operators)
            else:
                rest.append(entry)
        return [describe(entry) for entry in (diverse + rest)[:limit]]

    def _describe_leg(self, leg) -> dict:
        trip_id = self._tid_to_trip.get(leg["tid"])
        trip = self.trips.get(trip_id) or {}
        route = self.routes.get(trip.get("route_id", "")) or {}
        # By route, never by number: see noc_for_route.
        noc = self.noc_for_route(trip.get("route_id", ""))
        return {
            "service": route.get("short_name", ""),
            "operator": noc,
            "headsign": trip.get("headsign", ""),
            "depart": _span_hhmm(leg["depart"]),
            "arrive": _span_hhmm(leg["arrive"]),
            "minutes": max(0, (leg["arrive"] - leg["depart"]) // 60),
            "stops": self.stops_between(trip_id, leg["board_seq"], leg["alight_seq"]),
        }

    def _stop_place(self, sid) -> dict:
        atco = self._sid_to_stop.get(sid)
        stop = self.stops.get(atco) or {}
        return {"atco": atco, "name": stop.get("name", ""),
                # Which town, so "change at Waitrose" says which Waitrose.
                # There are two within this dataset — one on Western Road in
                # Brighton with twelve services, one in Hove Park with three,
                # two kilometres apart and identically named. A change stop
                # given by name alone is an instruction a passenger can follow
                # to the wrong place.
                "locality": stop.get("locality", ""),
                "lat": stop.get("lat"), "lon": stop.get("lon")}

    def interchange_legs_two(self, from_stop: str, to_stop: str, day,
                             anchor_secs: int = 43200) -> Optional[dict]:
        """The best three-bus itinerary, for pairs no single change connects.

        interchange_legs stops at one change on purpose, and for most journeys on
        this coast that is right. Some are not: Sompting to Brighton Marina has
        no one-change itinerary on any day, and without the three buses the map
        could draw nothing and the fare panel could not say which buses its
        tickets were being judged against. Call this only when one change fails.

        The same rules as one change, applied twice: at least MIN_CHANGE_SECS
        plus the walk at each change, no wait over an hour, walks up to
        INTERCHANGE_WALK_KM, and walking weighed at three times its clock time.

        One pass per middle trip, not a nested one: walking its calls in order
        and carrying the best boarding seen so far, an alighting call only has
        to be compared with that. Measured at about 230 ms for Sompting to the
        Marina on a desktop, and the endpoint caches it for the day.
        """
        if self._con is None:
            return None
        MIN_CHANGE_SECS = 4 * 60
        MAX_WAIT_SECS = 60 * 60
        KEEP = 6
        CELL = 0.005
        con = self._conn()

        a_sids = {self.stops[s]["_sid"] for s in self.sibling_stops(from_stop) if s in self.stops}
        b_sids = {self.stops[s]["_sid"] for s in self.sibling_stops(to_stop) if s in self.stops}
        if not a_sids or not b_sids or (a_sids & b_sids):
            return None

        runs, paths = {}, {}

        def trip_runs(tid):
            if tid not in runs:
                trip = self.trips.get(self._tid_to_trip.get(tid))
                runs[tid] = bool(trip) and self.runs_on(trip.get("service_id", ""), day)
            return runs[tid]

        def calls(tid):
            if tid not in paths:
                paths[tid] = con.execute(
                    "SELECT seq, sid, dep_secs FROM stop_times WHERE tid=? ORDER BY seq",
                    (tid,)).fetchall()
            return paths[tid]

        def trips_at(sids):
            seen = set()
            for sid in sids:
                for (tid,) in con.execute("SELECT DISTINCT tid FROM stop_times WHERE sid=?", (sid,)):
                    if tid not in seen and trip_runs(tid):
                        seen.add(tid)
                        yield tid

        def xy(sid):
            stop = self.stops.get(self._sid_to_stop.get(sid)) or {}
            return (stop["lat"], stop["lon"]) if stop.get("lat") is not None else None

        def grid_of(sids):
            grid = {}
            for sid in sids:
                pt = xy(sid)
                if pt:
                    grid.setdefault((int(pt[0] // CELL), int(pt[1] // CELL)), []).append(sid)
            return grid

        def nearby(grid, pt):
            cy, cx = int(pt[0] // CELL), int(pt[1] // CELL)
            found = set()
            for dy in (-1, 0, 1):
                for dx in (-1, 0, 1):
                    found.update(grid.get((cy + dy, cx + dx), ()))
            return found

        def walk_between(pt, sid, other):
            if sid == other:
                return 0.0, 0
            km = _haversine_km(pt, xy(other))
            if km > self.INTERCHANGE_WALK_KM:
                return None, None
            return km, int(km * 1000 / self.WALK_METRES_PER_SEC)

        # Leg one: every stop the first bus can put us at, after the anchor.
        first: dict = {}
        for tid in trips_at(a_sids):
            cs = calls(tid)
            board = next((c for c in cs if c[1] in a_sids and c[2] is not None
                          and c[2] >= anchor_secs), None)
            if board is None:
                continue
            for c in cs:
                if c[0] > board[0] and c[2] is not None:
                    first.setdefault(c[1], []).append(
                        {"tid": tid, "board_seq": board[0], "alight_seq": c[0],
                         "depart": board[2], "arrive": c[2]})
        # Leg three: every stop from which a bus still reaches the destination.
        last: dict = {}
        for tid in trips_at(b_sids):
            cs = calls(tid)
            arrive = next((c for c in reversed(cs) if c[1] in b_sids and c[2] is not None), None)
            if arrive is None:
                continue
            for c in cs:
                if c[0] < arrive[0] and c[2] is not None:
                    last.setdefault(c[1], []).append(
                        {"tid": tid, "board_seq": c[0], "alight_seq": arrive[0],
                         "depart": c[2], "arrive": arrive[2]})
        if not first or not last:
            return None
        for sid in first:
            first[sid].sort(key=lambda x: x["arrive"])
            del first[sid][KEEP * 2:]
        for sid in last:
            last[sid].sort(key=lambda x: x["depart"])
        first_grid, last_grid = grid_of(first), grid_of(last)

        best = None
        for tid in trips_at(set(first)):
            carry = None          # best boarding so far on this trip
            for seq, sid, t in calls(tid):
                if t is None:
                    continue
                pt = xy(sid)
                if pt is None:
                    continue
                # Off here, onto a bus that reaches the destination?
                if carry is not None:
                    for other in nearby(last_grid, pt) | ({sid} & last.keys()):
                        km, walk_secs = walk_between(pt, sid, other)
                        if km is None:
                            continue
                        for three in last[other]:
                            wait = three["depart"] - t
                            if wait < MIN_CHANGE_SECS + walk_secs:
                                continue
                            if wait > MAX_WAIT_SECS:
                                break          # sorted by departure
                            score = (three["arrive"] + walk_secs * 3) - carry["value"]
                            if best is None or score < best["score"]:
                                best = {
                                    "score": score, "one": carry["one"],
                                    "two": {"tid": tid, "board_seq": carry["seq"],
                                            "alight_seq": seq, "depart": carry["depart"],
                                            "arrive": t},
                                    "three": three,
                                    "changes": [
                                        (carry["from_sid"], carry["to_sid"], carry["walk_km"], carry["wait"]),
                                        (sid, other, km, wait),
                                    ],
                                }
                # On here, from a first bus?
                for other in nearby(first_grid, pt) | ({sid} & first.keys()):
                    km, walk_secs = walk_between(pt, sid, other)
                    if km is None:
                        continue
                    for one in first[other]:
                        wait = t - one["arrive"]
                        if wait < MIN_CHANGE_SECS + walk_secs or wait > MAX_WAIT_SECS:
                            continue
                        # The later you can leave the origin, the shorter the
                        # journey; walking is weighed as it is everywhere else.
                        value = one["depart"] - walk_secs * 3
                        if carry is None or value > carry["value"]:
                            carry = {"value": value, "one": one, "seq": seq, "depart": t,
                                     "from_sid": other, "to_sid": sid, "walk_km": km, "wait": wait}
        if best is None:
            return None

        changes = [{
            "change_at": self._stop_place(from_sid),
            "board_at": self._stop_place(to_sid),
            "walk_metres": round(km * 1000),
            "wait_minutes": wait // 60,
        } for from_sid, to_sid, km, wait in best["changes"]]
        return {
            "legs": [self._describe_leg(best["one"]), self._describe_leg(best["two"]),
                     self._describe_leg(best["three"])],
            # The first change in the older single fields, so a caller that only
            # knows about one change still draws something true.
            **changes[0],
            "changes": changes,
            "total_minutes": (best["three"]["arrive"] - best["one"]["depart"]) // 60,
        }

    # Central Brighton: where an evening out ends and the journey home starts.
    # The same box app.js calls CENTRAL_BRIGHTON, covering Old Steine, Churchill
    # Square, North Street and the Royal Pavilion. (min_lat, max_lat, min_lon, max_lon)
    CENTRAL_BRIGHTON = (50.815, 50.830, -0.158, -0.130)

    def _hub_sids(self) -> frozenset:
        cached = getattr(self, "_hub_sids_cache", None)
        if cached is None:
            lo_lat, hi_lat, lo_lon, hi_lon = self.CENTRAL_BRIGHTON
            cached = self._hub_sids_cache = frozenset(
                s["_sid"] for s in self.stops.values()
                if s.get("lat") is not None and s.get("lon") is not None
                and lo_lat <= s["lat"] <= hi_lat and lo_lon <= s["lon"] <= hi_lon)
        return cached

    def last_bus_from_hub(self, stop_id: str, from_day=None) -> dict:
        """The last scheduled direct bus home from central Brighton, by day.

        For each day of a concrete week: the latest trip that leaves a stop in
        CENTRAL_BRIGHTON and *later* calls at `stop_id` or one of its sibling
        poles, running on that date.

            {"inside_hub": False, "week_of": "2026-09-14",
             "days": {"monday": {"day_bus":   {"depart": "23:50", "arrive": "00:25",
                                               "after_midnight": False, "service": "700",
                                               "operator": "SCSO", "from": "Old Steine"},
                                 "night_bus": {... "service": "N700" ...}},
                      ..., "sunday": None}}

        Split into the last ordinary bus and the last night bus (a short name of
        N and a digit). One figure would be the night bus at gone three in the
        morning at every stop on the coast, which is true and says the opposite
        of what matters: ordinary buses stop far earlier, and the night service
        is a separate route that day tickets do not cover. A day with neither is
        None.

        "Later" is by stop sequence, not by clock: a trip that calls here at
        23:59 and reaches Brighton at 00:40 is on its way in, and taking the
        latest call at the stop regardless of direction offers it as the way
        home. A departure before NIGHT_ENDS_SECS is read as after midnight at
        the end of that service day, the same rule service_span uses, because
        feeds disagree about writing twenty past midnight as 24:20 or 00:20.

        Direct buses only. A journey home that needs a change is not counted,
        and the caller says so rather than implying there is no way back.
        """
        if self._con is None:
            return {}
        cache = getattr(self, "_last_bus_cache", None)
        if cache is None:
            cache = self._last_bus_cache = {}
        key = (stop_id, from_day)
        if key in cache:
            return cache[key]

        week = self.sample_week(from_day)
        result = {"inside_hub": False, "week_of": week["monday"].isoformat(), "days": {}}
        hub = self._hub_sids()
        stop = self.stops.get(stop_id)
        if not stop:
            cache[key] = result
            return result
        if stop.get("_sid") in hub:
            result["inside_hub"] = True
            cache[key] = result
            return result

        sids = [self.stops[s]["_sid"] for s in self.sibling_stops(stop_id) if s in self.stops]
        rows = []
        if sids and hub:
            ph_stop = ",".join("?" * len(sids))
            hub_list = list(hub)
            ph_hub = ",".join("?" * len(hub_list))
            rows = self._conn().execute(
                f"""SELECT st.tid, st.dep_secs, h.dep_secs, h.sid, t.service_id, r.short_name
                      FROM stop_times st
                      JOIN stop_times h ON h.tid = st.tid AND h.seq < st.seq
                      JOIN trips  t ON t.tid = st.tid
                      JOIN routes r ON r.rid = t.rid
                     WHERE st.sid IN ({ph_stop}) AND h.sid IN ({ph_hub})""",
                [*sids, *hub_list],
            ).fetchall()

        def late(secs):
            if secs is None:
                return None
            return secs + DAY_SECS if secs < NIGHT_ENDS_SECS else secs

        def clock(secs):
            secs %= DAY_SECS
            return f"{secs // 3600:02d}:{(secs % 3600) // 60:02d}"

        def describe(best):
            leave, reach, tid, hub_sid, short_name = best
            trip = self.trips.get(self._tid_to_trip.get(tid)) or {}
            return {
                "depart": clock(leave),
                "arrive": clock(reach),
                "after_midnight": leave >= DAY_SECS,
                "service": short_name or "",
                "operator": self.noc_for_route(trip.get("route_id", "")),
                "from": (self.stops.get(self._sid_to_stop.get(hub_sid)) or {}).get("name", ""),
            }

        runs = {}
        for day in self._DAY_COLS:
            best = {"day_bus": None, "night_bus": None}
            for tid, arr, dep, hub_sid, service_id, short_name in rows:
                k = (service_id, day)
                if k not in runs:
                    runs[k] = self.runs_on(service_id, week[day])
                if not runs[k]:
                    continue
                leave, reach = late(dep), late(arr)
                # The night ends at NIGHT_ENDS_SECS. GTFS lets a service day run
                # on past 24:00, and the first bus of the next morning can be
                # written as 28:37 on the day before: numerically the latest,
                # and nobody's way home from an evening out. In the real feed it
                # was offered as the last 700 at every stop along the coast.
                if leave is not None and leave >= DAY_SECS + NIGHT_ENDS_SECS:
                    continue
                # Belt and braces with `h.seq < st.seq` above: a trip whose
                # clock says it reaches the stop before leaving Brighton is
                # heading the other way, or its times are broken.
                if leave is None or reach is None or reach < leave:
                    continue
                kind = "night_bus" if re.match(r"N\d", short_name or "") else "day_bus"
                if best[kind] is None or leave > best[kind][0]:
                    best[kind] = (leave, reach, tid, hub_sid, short_name)
            if best["day_bus"] is None and best["night_bus"] is None:
                result["days"][day] = None
                continue
            result["days"][day] = {k: (describe(v) if v else None) for k, v in best.items()}
        cache[key] = result
        return result

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
        rows = self._conn().execute(
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
        ids = frozenset(row[0] for row in self._conn().execute("""
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
        # FlixBus's Brighton services. "UK066" and "UK998" are internal line
        # codes rather than anything a passenger sees on the front of the
        # coach, and neither calls in Adur or Worthing, so they have no place
        # in a filter for local service improvement.
        "UK066", "UK998",
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

        stop_counts = dict(self._conn().execute(
            "SELECT tid, COUNT(*) FROM stop_times GROUP BY tid"
        ))

        stop_info = {
            sid: (lat, lon, name)
            for sid, lat, lon, name in self._conn().execute(
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
                row[0] for row in self._conn().execute(
                    "SELECT DISTINCT st.tid FROM stop_times st "
                    "JOIN stops s ON s.sid = st.sid "
                    "WHERE s.lat BETWEEN ? AND ? AND s.lon BETWEEN ? AND ?",
                    (min_lat, max_lat, min_lon, max_lon),
                )
            }

        trips_by_route: dict = {}
        for short_name, tid, first_sid, last_sid in self._conn().execute(
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
            shaped_tids = {row[0] for row in self._conn().execute(
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
            chosen_headsigns = dict(self._conn().execute(
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
                    pts = self._conn().execute(
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
            cols = {row[1] for row in self._conn().execute("PRAGMA table_info(routes)")}
        except sqlite3.Error:
            return {}
        if "noc" not in cols:
            return {}
        out: dict = {}
        for short, noc in self._conn().execute(
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
            row = self._conn().execute(
                "SELECT shape_id FROM trips WHERE tid=?", (tid,)
            ).fetchone()
            if not row or not row[0]:
                return None
            pts = self._conn().execute(
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
