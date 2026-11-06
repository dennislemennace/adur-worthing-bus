"""What made Route view and the departure boards slow, kept from coming back.

Measured on the live API on 6 November 2026: route lines took 14 seconds to
build and 3 to 4 seconds to send even from the cache, and the hourly timetable
check ran on whichever request came next, with every other request waiting
behind it. See scripts/build_route_lines.py for the file the site now reads.

Run with:  pytest
"""

import json
import sys
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import api.main as main  # noqa: E402
import build_route_lines as brl  # noqa: E402


class FakeTimetable:
    """Only what the loader and the route lines ask of a timetable."""

    def __init__(self, current=True, block=None):
        self.loaded_at = 0.0          # an hour and more ago
        self._current = current
        self._block = block
        self.lines_built = 0

    def ok(self):
        return True

    def is_current(self):
        if self._block:
            self._block.wait(5)
        return self._current

    def reload(self):
        # What the request did before: the whole check, in line.
        self.is_current()

    def representative_polylines(self, bbox=None):
        self.lines_built += 1
        return [{"service": "700", "operator": "SCSO", "polylines": [[[50.8, -0.3], [50.81, -0.31]]]}]


def _wait_for_check():
    for t in threading.enumerate():
        if t.name == "timetable-check":
            t.join(5)


@pytest.fixture
def loader(monkeypatch):
    monkeypatch.setattr(main, "_cache", {})
    monkeypatch.setattr(main, "_timetable_checking", False)
    yield
    # A check left running would touch the next test's timetable.
    _wait_for_check()


def test_the_hourly_check_does_not_hold_up_the_request(loader, monkeypatch):
    held = threading.Event()
    tt = FakeTimetable(block=held)
    monkeypatch.setattr(main, "_timetable", tt)
    started = time.perf_counter()
    got = main._get_timetable_sync()
    took = time.perf_counter() - started
    assert got is tt
    assert took < 0.5, f"the request waited {took:.2f}s on the hourly check"
    # A second request meanwhile starts no second check.
    main._get_timetable_sync()
    assert sum(t.name == "timetable-check" for t in threading.enumerate()) == 1
    held.set()
    _wait_for_check()
    assert time.time() - tt.loaded_at < 5, "an unchanged timetable is not checked again for an hour"
    assert main._timetable is tt


def test_a_newer_timetable_is_swapped_in_and_old_route_lines_dropped(loader, monkeypatch):
    old, new = FakeTimetable(current=False), FakeTimetable()
    monkeypatch.setattr(main, "_timetable", old)
    monkeypatch.setattr(main, "Timetable", lambda path: new)
    main.cache_set(main.ROUTE_LINES_KEY, b"{}", 3600)
    main._get_timetable_sync()
    _wait_for_check()
    assert main._timetable is new
    assert main.cache_get(main.ROUTE_LINES_KEY) is None


def test_route_lines_are_built_once_and_sent_as_they_were_encoded(loader, monkeypatch):
    tt = FakeTimetable()

    async def timetable():
        return tt
    monkeypatch.setattr(main, "_get_timetable", timetable)
    client = TestClient(main.app)
    first = client.get("/api/route-lines")
    second = client.get("/api/route-lines")
    assert first.status_code == 200 and first.headers["content-type"].startswith("application/json")
    assert first.json()["routes"][0]["service"] == "700"
    assert second.content == first.content
    assert tt.lines_built == 1, "route lines were built again from the cache"
    # Kept encoded: encoding 2.7 MB again took 3 to 4 seconds a request.
    assert isinstance(main.cache_get(main.ROUTE_LINES_KEY), bytes)


def test_the_route_lines_file_thins_lines_without_moving_them():
    # A straight road drawn with a point every few metres, then one bend.
    road = ([[round(50.80 + i * 0.00005, 5), -0.30] for i in range(41)]
            + [[50.802, round(-0.30 + i * 0.00005, 5)] for i in range(1, 41)])
    thin = brl.simplify(road)
    assert thin[0] == road[0] and thin[-1] == road[-1]
    assert len(thin) == 3, f"{len(thin)} points kept for a road with one bend"
    assert thin[1] == [50.802, -0.3], "the bend was cut off"
    doc = brl.build([{"service": "7", "polylines": [road]}], "2026-11-06")
    assert doc["count"] == 1 and doc["generated_on"] == "2026-11-06"
    json.dumps(doc)


def test_the_site_reads_the_file_before_the_api():
    js = (ROOT / "app.js").read_text(encoding="utf-8")
    impl = js[js.index("async function fetchRouteLinesData"):js.index("async function loadRouteLinesImpl")]
    assert impl.index('fetch("data/route_lines.json")') < impl.index('apiFetch("/api/route-lines")')
    load = js[js.index("async function loadRouteLinesImpl"):][:400]
    assert "fetchRouteLinesData()" in load and 'apiFetch("/api/route-lines")' not in load


def test_the_weekly_build_writes_and_commits_the_route_lines():
    wf = (ROOT / ".github" / "workflows" / "update-timetable.yml").read_text(encoding="utf-8")
    assert "scripts/build_route_lines.py" in wf
    commit = wf[wf.index("Commit refreshed statistics"):]
    assert "data/route_lines.json" in commit.split("git commit")[0]
