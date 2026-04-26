#!/usr/bin/env python3
"""One-shot bootstrap: convert data/timetable.json -> data/timetable.sqlite.

Integer surrogate keys (tid, sid) keep the hot tables small enough to commit
to git. The external GTFS text ids remain queryable via UNIQUE columns.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JSON_PATH = ROOT / "data" / "timetable.json"
DB_PATH = ROOT / "data" / "timetable.sqlite"

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA page_size = 4096;

CREATE TABLE stops (
    sid     INTEGER PRIMARY KEY,
    stop_id TEXT NOT NULL UNIQUE,
    name    TEXT NOT NULL,
    lat     REAL NOT NULL,
    lon     REAL NOT NULL
);

CREATE TABLE routes (
    rid        INTEGER PRIMARY KEY,
    route_id   TEXT NOT NULL UNIQUE,
    short_name TEXT NOT NULL,
    long_name  TEXT NOT NULL,
    noc        TEXT NOT NULL DEFAULT ''
);
CREATE INDEX idx_routes_short ON routes(short_name);

CREATE TABLE trips (
    tid        INTEGER PRIMARY KEY,
    trip_id    TEXT NOT NULL UNIQUE,
    rid        INTEGER NOT NULL,
    service_id TEXT NOT NULL,
    headsign   TEXT NOT NULL
);
CREATE INDEX idx_trips_rid     ON trips(rid);
CREATE INDEX idx_trips_service ON trips(service_id);

CREATE TABLE stop_times (
    tid      INTEGER NOT NULL,
    seq      INTEGER NOT NULL,
    sid      INTEGER NOT NULL,
    dep_secs INTEGER NOT NULL,
    PRIMARY KEY (tid, seq)
) WITHOUT ROWID;
CREATE INDEX idx_stop_times_stop ON stop_times(sid, dep_secs);

CREATE TABLE calendar (
    service_id TEXT PRIMARY KEY,
    monday     INTEGER NOT NULL,
    tuesday    INTEGER NOT NULL,
    wednesday  INTEGER NOT NULL,
    thursday   INTEGER NOT NULL,
    friday     INTEGER NOT NULL,
    saturday   INTEGER NOT NULL,
    sunday     INTEGER NOT NULL,
    start_date TEXT NOT NULL,
    end_date   TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE calendar_dates (
    service_id TEXT NOT NULL,
    date       TEXT NOT NULL,
    exception  INTEGER NOT NULL,  -- 1 = added, 2 = removed
    PRIMARY KEY (service_id, date)
) WITHOUT ROWID;

-- Denormalised per-trip summary; short_name lookup drives GTFS-RT matching.
CREATE TABLE trip_endpoints (
    tid        INTEGER PRIMARY KEY,
    short_name TEXT NOT NULL,
    first_sid  INTEGER NOT NULL,
    last_sid   INTEGER NOT NULL,
    first_secs INTEGER NOT NULL
);
CREATE INDEX idx_trip_endpoints_short ON trip_endpoints(short_name);
"""


def main() -> None:
    if not JSON_PATH.exists():
        print(f"missing {JSON_PATH}", file=sys.stderr)
        sys.exit(1)

    t0 = time.monotonic()
    print(f"reading {JSON_PATH} ({JSON_PATH.stat().st_size / 1_000_000:.1f} MB) …")
    with JSON_PATH.open() as fh:
        tt = json.load(fh)

    if DB_PATH.exists():
        DB_PATH.unlink()
    con = sqlite3.connect(DB_PATH)
    con.executescript(SCHEMA)

    # --- surrogate id maps ---
    stop_sid: dict[str, int] = {}
    for i, sid_text in enumerate(tt["stops"].keys(), start=1):
        stop_sid[sid_text] = i
    route_rid: dict[str, int] = {}
    for i, rid_text in enumerate(tt["routes"].keys(), start=1):
        route_rid[rid_text] = i
    trip_tid: dict[str, int] = {}
    for i, tid_text in enumerate(tt["trips"].keys(), start=1):
        trip_tid[tid_text] = i

    # --- stops ---
    con.executemany(
        "INSERT INTO stops VALUES (?,?,?,?,?)",
        (
            (stop_sid[sid], sid, s["name"], s["lat"], s["lon"])
            for sid, s in tt["stops"].items()
        ),
    )
    print(f"  stops: {len(tt['stops'])}")

    # --- routes ---
    con.executemany(
        "INSERT INTO routes VALUES (?,?,?,?,?)",
        (
            (
                route_rid[rid],
                rid,
                r.get("short_name", ""),
                r.get("long_name", ""),
                r.get("noc", ""),
            )
            for rid, r in tt["routes"].items()
        ),
    )
    print(f"  routes: {len(tt['routes'])}")

    # --- trips ---
    trip_rid: dict[int, int] = {}
    trip_rows = []
    for tid_text, t in tt["trips"].items():
        tid = trip_tid[tid_text]
        rid = route_rid[t["route_id"]]
        trip_rid[tid] = rid
        trip_rows.append((tid, tid_text, rid, t["service_id"], t.get("headsign", "")))
    con.executemany("INSERT INTO trips VALUES (?,?,?,?,?)", trip_rows)
    print(f"  trips: {len(trip_rows)}")

    # --- stop_times: invert stop-indexed map into per-trip list ---
    per_trip: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for stop_id_text, entries in tt["stop_times"].items():
        sid = stop_sid.get(stop_id_text)
        if sid is None:
            continue
        for dep_secs, trip_id_text in entries:
            tid = trip_tid.get(trip_id_text)
            if tid is None:
                continue
            per_trip[tid].append((dep_secs, sid))

    route_short = {
        route_rid[rid]: r.get("short_name", "")
        for rid, r in tt["routes"].items()
    }

    st_rows = []
    endpoint_rows = []
    for tid, pairs in per_trip.items():
        pairs.sort(key=lambda p: p[0])
        for seq, (dep_secs, sid) in enumerate(pairs):
            st_rows.append((tid, seq, sid, dep_secs))
        first_dep, first_sid = pairs[0]
        _, last_sid = pairs[-1]
        short = route_short.get(trip_rid.get(tid, 0), "")
        endpoint_rows.append((tid, short, first_sid, last_sid, first_dep))

    con.executemany("INSERT INTO stop_times VALUES (?,?,?,?)", st_rows)
    print(f"  stop_times: {len(st_rows)} (across {len(per_trip)} trips)")

    con.executemany("INSERT INTO trip_endpoints VALUES (?,?,?,?,?)", endpoint_rows)
    print(f"  trip_endpoints: {len(endpoint_rows)}")

    # --- calendar ---
    cal_rows = []
    for sid_text, c in tt["calendar"].items():
        cal_rows.append(
            (
                sid_text,
                int(c["monday"]),
                int(c["tuesday"]),
                int(c["wednesday"]),
                int(c["thursday"]),
                int(c["friday"]),
                int(c["saturday"]),
                int(c["sunday"]),
                c["start_date"],
                c["end_date"],
            )
        )
    con.executemany(
        "INSERT INTO calendar VALUES (?,?,?,?,?,?,?,?,?,?)", cal_rows
    )
    print(f"  calendar: {len(cal_rows)}")

    # --- calendar_dates ---
    cd_rows = []
    for sid_text, dates in tt["calendar_dates"].items():
        for date, exc in dates.items():
            cd_rows.append((sid_text, date, int(exc)))
    con.executemany("INSERT INTO calendar_dates VALUES (?,?,?)", cd_rows)
    print(f"  calendar_dates: {len(cd_rows)}")

    con.commit()
    con.execute("ANALYZE")
    con.close()

    # VACUUM outside the WAL transaction to compact final file.
    con = sqlite3.connect(DB_PATH)
    con.execute("VACUUM")
    con.close()

    size = DB_PATH.stat().st_size / 1_000_000
    print(f"wrote {DB_PATH} ({size:.1f} MB) in {time.monotonic() - t0:.1f}s")


if __name__ == "__main__":
    main()
