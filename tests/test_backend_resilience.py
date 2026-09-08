"""Tests for what the API does under load, on a bad download, and at restart.

None of these are about correct answers to correct questions. They are about
the shapes of failure the service has never been asked to survive: a cache
that only forgets what it is asked for, a database replaced before it has been
checked, a derived cache outliving the data it was derived from, and a health
endpoint that says "ok" whatever is underneath it.

Run with:  pytest
"""

import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import api.main as main            # noqa: E402
from api.timetable_db import Timetable   # noqa: E402


@pytest.fixture(autouse=True)
def clean_cache():
    main._cache.clear()
    yield
    main._cache.clear()


# ── The cache forgets on its own ────────────────────────────

def test_expired_entries_do_not_wait_to_be_asked_for():
    """Expiry used to happen only in `cache_get`, for the key being read.

    One-off journey combinations and per-service rail keys are never read a
    second time, so nothing ever removed them. On a long-lived process that is
    a slow leak with no bound on it.
    """
    main.cache_set("never-read-again", {"big": "value"}, ttl=-1)
    main.cache_set("something-else", {"x": 1}, ttl=60)
    main.cache_get("something-else")     # ordinary traffic, other keys
    assert "never-read-again" not in main._cache, \
        "an expired entry survived because nobody asked for it again"


def test_the_cache_has_a_ceiling():
    for i in range(main.CACHE_MAX_ENTRIES + 50):
        main.cache_set(f"k{i}", {"i": i}, ttl=600)
    assert len(main._cache) <= main.CACHE_MAX_ENTRIES, \
        f"cache grew to {len(main._cache)} with no bound"


def test_the_ceiling_evicts_the_least_recently_used():
    for i in range(main.CACHE_MAX_ENTRIES):
        main.cache_set(f"k{i}", i, ttl=600)
    main.cache_get("k0")                     # k0 is now the freshest
    main.cache_set("overflow", 1, ttl=600)
    assert main.cache_get("k0") == 0, "the entry just used was the one dropped"


# ── Concurrent misses do the work once ──────────────────────

def test_twelve_simultaneous_misses_do_one_piece_of_work():
    """The audit sent 12 concurrent vehicle requests and watched 12 upstream
    fetches go out. On a free tier with a daily quota that is the difference
    between a working service and an exhausted one."""
    calls = []
    lock = threading.Lock()

    def expensive():
        with lock:
            calls.append(1)
        # Long enough that every other thread is certainly inside
        # cache_single_flight while this one is still working. A barrier here
        # would deadlock, which is the point: the followers are not running
        # `expensive`, they are waiting for this one.
        time.sleep(0.3)
        return {"value": "computed once"}

    results = []
    start = threading.Event()

    def worker():
        start.wait(timeout=5)
        results.append(main.cache_single_flight("shared-key", expensive, ttl=60))

    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    start.set()
    for t in threads:
        t.join(timeout=15)

    assert len(calls) == 1, f"the work was done {len(calls)} times, not once"
    assert all(r == results[0] for r in results), "callers got different answers"


def test_a_failure_is_not_cached_and_does_not_wedge_the_key():
    def boom():
        raise RuntimeError("upstream down")

    with pytest.raises(RuntimeError):
        main.cache_single_flight("flaky", boom, ttl=60)

    # The next caller must be free to try again.
    assert main.cache_single_flight("flaky", lambda: "recovered", ttl=60) == "recovered"


# ── A downloaded database is checked before it is trusted ───

def _make_db(path: Path, *, valid: bool = True) -> None:
    con = sqlite3.connect(path)
    if valid:
        con.executescript("""
            CREATE TABLE stops (sid INTEGER PRIMARY KEY, stop_id TEXT, name TEXT,
                                lat REAL, lon REAL);
            CREATE TABLE routes (rid INTEGER PRIMARY KEY, route_id TEXT,
                                 short_name TEXT, long_name TEXT, noc TEXT);
            CREATE TABLE trips (tid INTEGER PRIMARY KEY, trip_id TEXT, rid INTEGER,
                                service_id TEXT, headsign TEXT, shape_id TEXT);
            CREATE TABLE stop_times (tid INTEGER, seq INTEGER, sid INTEGER,
                                     dep_secs INTEGER);
            INSERT INTO stops VALUES (1, '4400A', 'A', 50.8, -0.3);
            INSERT INTO routes VALUES (1, 'R1', '700', 'Coast', 'TSTO');
            INSERT INTO trips VALUES (1, 'T1', 1, 'S1', 'X', '');
            INSERT INTO stop_times VALUES (1, 0, 1, 3600);
        """)
    else:
        con.executescript("CREATE TABLE something_else (x INTEGER);")
    con.commit()
    con.close()


def test_a_candidate_download_is_verified_before_it_replaces_anything(tmp_path):
    """The replacement was `urlretrieve` then `tmp.replace(db_path)`.

    Nothing between them asked whether the file was a database, had the right
    tables, or contained any rows — so a truncated download, an HTML error
    page saved with a .sqlite name, or a half-built release asset would take
    the live timetable with it.
    """
    live = tmp_path / "timetable.sqlite"
    _make_db(live, valid=True)
    before = live.read_bytes()

    candidate = tmp_path / "candidate.sqlite"
    candidate.write_bytes(b"<html>404 Not Found</html>")

    assert main_db_is_usable(candidate) is False
    assert live.read_bytes() == before, "the live database was replaced anyway"


def test_a_database_missing_its_tables_is_rejected(tmp_path):
    bad = tmp_path / "bad.sqlite"
    _make_db(bad, valid=False)
    assert main_db_is_usable(bad) is False


def test_an_empty_but_well_formed_database_is_rejected(tmp_path):
    empty = tmp_path / "empty.sqlite"
    _make_db(empty, valid=True)
    con = sqlite3.connect(empty)
    con.execute("DELETE FROM stop_times")
    con.commit()
    con.close()
    assert main_db_is_usable(empty) is False, \
        "a database with no departures in it was accepted as a timetable"


def test_a_good_database_is_accepted(tmp_path):
    good = tmp_path / "good.sqlite"
    _make_db(good, valid=True)
    assert main_db_is_usable(good) is True


def main_db_is_usable(path):
    from api.timetable_db import db_is_usable
    return db_is_usable(path)


# ── Derived caches do not outlive their data ────────────────

def test_reopening_the_database_clears_the_service_span_cache(tmp_path):
    """`_span_cache` survived `reload()`.

    Service spans are derived by sampling a week of the timetable. After a
    rebuild they describe a timetable that is no longer there, and nothing
    invalidates them, so the answer can stay wrong until the process restarts.
    """
    db = tmp_path / "timetable.sqlite"
    _make_db(db, valid=True)

    tt = Timetable.__new__(Timetable)
    tt.db_path = db
    tt._span_cache = {"4400A": {"runs_days": ["mon"]}}
    tt._noc_map = {"700": "TSTO"}
    tt._noc_by_rid = {"R1": "TSTO"}
    tt._stops_by_name = {"A": ["4400A"]}
    tt._clear_derived()

    assert not tt._span_cache, "service spans survived a reload of the database"
    assert tt._noc_map is None
    assert tt._stops_by_name is None


# ── Readiness is not the same as being alive ────────────────

def test_the_health_endpoint_reports_the_timetable_it_actually_has():
    from fastapi.testclient import TestClient
    orig = main._timetable
    main._timetable = None
    try:
        with TestClient(main.app) as client:
            body = client.get("/").json()
        assert body["timetable_loaded"] is False
        assert body["status"] != "ok", \
            "the service reported ok with no timetable behind it"
    finally:
        main._timetable = orig


# ── The coroutine coalescer ─────────────────────────────────

def test_concurrent_awaits_for_one_key_make_one_upstream_call():
    """`/api/vehicles` awaits an upstream feed rather than threading it, so it
    needs a coalescer on the event loop. Every duplicate call spends quota."""
    import asyncio

    calls = []

    async def produce():
        calls.append(1)
        await asyncio.sleep(0.05)
        return {"vehicles": [], "count": 0}

    async def scenario():
        return await asyncio.gather(*[
            main.cache_single_flight_async("veh", produce, 15) for _ in range(12)
        ])

    results = asyncio.run(scenario())
    assert len(calls) == 1, f"{len(calls)} upstream calls for one cache miss"
    assert all(r == results[0] for r in results)


def test_one_client_going_away_does_not_cancel_the_others():
    """A shielded task, because a browser closing a tab must not take the
    response out from under every other waiter on the same key."""
    import asyncio

    async def produce():
        await asyncio.sleep(0.1)
        return {"ok": True}

    async def scenario():
        leader = asyncio.ensure_future(
            main.cache_single_flight_async("shared", produce, 15))
        await asyncio.sleep(0)
        follower = asyncio.ensure_future(
            main.cache_single_flight_async("shared", produce, 15))
        await asyncio.sleep(0)
        leader.cancel()                      # the first client disconnects
        return await follower

    assert asyncio.run(scenario()) == {"ok": True}
