"""SQLite-backed timetable store.

Replaces the old dict-of-dicts loaded from data/timetable.json.
Small reference tables (stops/routes/trips/calendar/calendar_dates) are
kept in RAM so existing dict-style consumer code continues to work.
The hot bulk tables (stop_times, per-trip stop sequences, service
endpoint summaries) live on disk and are queried on demand.

This keeps Render Free-tier RSS well under 512 MB.
"""
from __future__ import annotations

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

    def _download_if_missing(self) -> None:
        if self.db_path.exists():
            return
        log.info("Timetable DB missing; downloading from %s", TIMETABLE_URL)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.db_path.with_suffix(self.db_path.suffix + ".tmp")
        try:
            urllib.request.urlretrieve(TIMETABLE_URL, tmp)
            tmp.replace(self.db_path)
            log.info("Timetable DB downloaded: %d bytes",
                     self.db_path.stat().st_size)
        except Exception as exc:
            log.error("Timetable download failed: %s", exc)
            if tmp.exists():
                tmp.unlink()

    def _open_and_preload(self) -> None:
        self._download_if_missing()
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

    def has_stop_times(self, stop_id: str) -> bool:
        return stop_id in self.stops_with_times

    def sample_stop_ids_with_times(self, n: int = 10) -> list:
        it = iter(self.stops_with_times)
        return [next(it) for _ in range(min(n, len(self.stops_with_times)))]

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

        bbox: optional (min_lat, max_lat, min_lon, max_lon) — routes with
        no polyline point inside this bbox are dropped.
        """
        if self._con is None:
            return []

        stop_counts = dict(self._con.execute(
            "SELECT tid, COUNT(*) FROM stop_times GROUP BY tid"
        ))

        stop_coords = {
            sid: (lat, lon)
            for sid, lat, lon in self._con.execute(
                "SELECT sid, lat, lon FROM stops"
            )
        }

        trips_by_route: dict = {}
        for short_name, tid, first_sid, last_sid in self._con.execute(
            "SELECT short_name, tid, first_sid, last_sid FROM trip_endpoints"
        ):
            trips_by_route.setdefault(short_name, []).append(
                (tid, first_sid, last_sid)
            )

        # ~1 km cutoff in squared degrees (rough at this latitude — fine
        # for grouping terminus stops that share a stand)
        TERMINUS_NEAR_SQ = 0.0001

        out = []
        for short_name, trips in trips_by_route.items():
            trips.sort(key=lambda t: stop_counts.get(t[0], 0), reverse=True)
            primary_tid, primary_first, primary_last = trips[0]
            primary_last_coord = stop_coords.get(primary_last)

            chosen_tids = [primary_tid]
            if primary_last_coord is not None:
                for tid, first_sid, _last_sid in trips[1:]:
                    if first_sid == primary_first:
                        continue
                    fc = stop_coords.get(first_sid)
                    if fc is None:
                        continue
                    dlat = fc[0] - primary_last_coord[0]
                    dlon = fc[1] - primary_last_coord[1]
                    if dlat * dlat + dlon * dlon <= TERMINUS_NEAR_SQ:
                        chosen_tids.append(tid)
                        break  # already sorted by stop count desc

            polylines = []
            for tid in chosen_tids:
                pts = self._con.execute(
                    "SELECT s.lat, s.lon FROM stop_times st "
                    "JOIN stops s ON s.sid = st.sid "
                    "WHERE st.tid=? ORDER BY st.seq", (tid,)
                ).fetchall()
                if len(pts) < 2:
                    continue
                if bbox is not None:
                    min_lat, max_lat, min_lon, max_lon = bbox
                    if not any(
                        min_lat <= lat <= max_lat and min_lon <= lon <= max_lon
                        for lat, lon in pts
                    ):
                        continue
                polylines.append([[lat, lon] for lat, lon in pts])

            if polylines:
                out.append({"service": short_name, "polylines": polylines})

        return sorted(out, key=lambda x: x["service"])
