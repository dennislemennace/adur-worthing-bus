"""Road lines traced for journeys with none, in the weekly build.

Before 2 November 2026 the build traced one or two journeys per route, and
only on a route with no line at all, which left 43% of journeys drawn stop to
stop. Now every pattern of stops without a line is traced once, unless the API
can already cut it from a line its route has, and the line goes to every
journey with those stops. OSRM is faked here: no test calls the public server.

Run with:  pytest
"""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import build_timetable as bt   # noqa: E402

# Five stops along a street in Worthing, west to east, and one off it.
STOPS = {f"S{i}": {"lat": 50.8150, "lon": -0.3800 + i * 0.004, "name": f"Stop {i}"} for i in range(5)}
STOPS["X"] = {"lat": 50.8300, "lon": -0.3700, "name": "Off the street"}


def timetable(trips):
    """trips: {tid: (route_id, [stop ids], shape_id)}."""
    tt = {"stops": STOPS, "stop_times": {}, "shapes": {}, "trips": {},
          "routes": {"R1": {"short_name": "1", "noc": "COMT"}, "R2": {"short_name": "2", "noc": "COMT"}}}
    for tid, (rid, calls, shape) in trips.items():
        tt["trips"][tid] = {"route_id": rid, "shape_id": shape or ""}
        for seq, sid in enumerate(calls):
            tt["stop_times"].setdefault(sid, []).append([36000 + seq * 60, tid, seq, None])
    return tt


@pytest.fixture
def osrm(monkeypatch):
    asked = []

    class Reply:
        def __init__(self, url):
            self.coords = [list(map(float, c.split(","))) for c in url.split("/driving/")[1].split("?")[0].split(";")]

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return json.dumps({"code": "Ok", "routes": [{"geometry": {"coordinates": self.coords}}]}).encode()

    def urlopen(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else req
        asked.append(url)
        return Reply(url)

    monkeypatch.setattr(bt.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(bt, "OSRM_SLEEP_SEC", 0)
    return asked


def test_each_pattern_without_a_line_is_traced_once_and_shared(osrm):
    tt = timetable({
        "A1": ("R1", ["S0", "S1", "S2", "S3", "S4"], ""),
        "A2": ("R1", ["S0", "S1", "S2", "S3", "S4"], ""),   # same stops: shares A1's line
        "B1": ("R1", ["S1", "S2", "S3"], ""),               # a stretch of A's: cut at runtime
        "C1": ("R2", ["S0", "X", "S4"], ""),                # its own road
    })
    bt._osrm_fill(tt, set(STOPS), set(STOPS))
    assert len(osrm) == 2, osrm
    assert tt["trips"]["A1"]["shape_id"] and tt["trips"]["A1"]["shape_id"] == tt["trips"]["A2"]["shape_id"]
    assert tt["trips"]["B1"]["shape_id"] == ""
    assert tt["trips"]["C1"]["shape_id"]


def test_a_pattern_already_on_its_routes_line_is_not_traced(osrm):
    tt = timetable({
        "SHAPED": ("R1", ["S0", "S1", "S2", "S3", "S4"], "G1"),
        "EXPRESS": ("R1", ["S0", "S2", "S4"], ""),
    })
    tt["shapes"]["G1"] = [[s["lat"], s["lon"]] for k, s in STOPS.items() if k != "X"]
    bt._osrm_fill(tt, set(STOPS), set(STOPS))
    assert osrm == []
