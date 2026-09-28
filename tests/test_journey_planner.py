"""The journey planner preview (/api/plan, api/journey_planner.py).

It spends a metered, free-tier allowance on someone else's service, so these
pin the guards: no key means no call, a repeated question is answered from
the cache, the day's cap is kept, a failure does not spend the allowance, and
the key travels in a header and never in the URL. The upstream is faked: no
test reaches busesandtrains.co.uk.
"""

import asyncio
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import api.main as main                                          # noqa: E402
from api import journey_planner as planner                       # noqa: E402

PLAN = {"origin": "Lancing", "destination": "Brighton", "options": [{
    "departure_time": "2026-09-28T14:00:00Z", "arrival_time": "2026-09-28T14:45:00Z",
    "duration_seconds": 2700, "num_transfers": 0, "extra": "dropped",
    "legs": [{"mode": "BUS", "route": "700", "agency": "Stagecoach South",
              "from_name": "Lancing Park", "to_name": "Old Steine",
              "from_stop_id": "4400AD0062", "to_stop_id": "149000007828",
              "departure_time": "2026-09-28T14:00:00Z", "arrival_time": "2026-09-28T14:45:00Z",
              "duration_seconds": 2700, "num_stops": 30, "geometry": "abc", "secret": "x"}]}]}


class FakeClient:
    calls: list = []
    status = 200
    fail = False

    def __init__(self, *a, **k): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False

    async def get(self, url, params=None, headers=None):
        FakeClient.calls.append((url, dict(params or {}), dict(headers or {})))
        if FakeClient.fail:
            raise RuntimeError("down")
        status = FakeClient.status

        class R:
            status_code = status
            def raise_for_status(self):
                if status >= 400:
                    raise RuntimeError(status)
            def json(self): return PLAN
        return R()


@pytest.fixture
def setup(monkeypatch, tmp_path):
    FakeClient.calls, FakeClient.status, FakeClient.fail = [], 200, False
    monkeypatch.setattr(main.httpx, "AsyncClient", FakeClient)
    monkeypatch.setattr(planner, "BAT_API_KEY", "test-key-not-real")
    monkeypatch.setattr(main, "_plan_quota", planner.DailyQuota(tmp_path / "q.json", 2))
    main._cache.clear()
    return monkeypatch


def plan(lat=50.83, **kw):
    args = dict(from_lat=lat, from_lon=-0.32, to_lat=50.822, to_lon=-0.137, when="14:00", day="2026-09-28")
    args.update(kw)
    return asyncio.run(main.get_plan(**args))


def test_without_a_key_nothing_is_asked(setup):
    setup.setattr(planner, "BAT_API_KEY", "")
    assert plan()["reason"] == "not_configured"
    assert FakeClient.calls == []


def test_a_plan_comes_back_trimmed_and_the_key_stays_out_of_the_url(setup):
    out = plan()
    assert out["available"] is True
    leg = out["options"][0]["legs"][0]
    assert leg["from_stop_id"] == "4400AD0062" and "secret" not in leg
    assert "extra" not in out["options"][0]
    url, params, headers = FakeClient.calls[0]
    assert url.endswith("/v1/journey/plan")
    assert "test-key-not-real" not in url and "app_key" not in params, "the key went into the URL"
    assert headers["Authorization"] == "Bearer test-key-not-real"


def test_the_same_question_is_answered_from_the_cache(setup):
    plan(); plan()
    assert len(FakeClient.calls) == 1, "a repeated question spent the allowance twice"


def test_the_days_cap_is_kept(setup):
    plan(lat=50.80); plan(lat=50.81)
    assert plan(lat=50.82)["reason"] == "quota"
    assert len(FakeClient.calls) == 2


def test_a_failure_does_not_spend_the_allowance(setup):
    FakeClient.fail = True
    assert plan()["reason"] == "upstream"
    assert main._plan_quota.remaining() == 2


def test_a_journey_far_outside_the_area_is_refused(setup):
    with pytest.raises(HTTPException):
        plan(lat=53.48)
    assert FakeClient.calls == []


# ── /api/fares-coverage (BODS fares metadata) ───────────────

def test_fares_coverage_without_a_key_asks_nothing(monkeypatch):
    monkeypatch.setattr(main, "BODS_API_KEY", "")
    monkeypatch.setattr(main.httpx, "AsyncClient", FakeClient)
    FakeClient.calls = []
    assert asyncio.run(main.get_fares_coverage())["reason"] == "not_configured"
    assert FakeClient.calls == []


def test_fares_coverage_keeps_metadata_and_never_the_key(monkeypatch):
    class FaresClient(FakeClient):
        async def get(self, url, params=None, headers=None):
            FakeClient.calls.append((url, dict(params or {}), {}))

            class R:
                status_code = 200
                def raise_for_status(self): pass
                def json(self): return {"count": 1, "results": [{
                    "id": 7, "name": "B&H fares", "noc": ["BHBC"], "numOfFareProducts": 12,
                    "api_key": "leak?", "internal": "x"}]}
            return R()
    FakeClient.calls = []
    monkeypatch.setattr(main, "BODS_API_KEY", "bods-key-not-real")
    monkeypatch.setattr(main.httpx, "AsyncClient", FaresClient)
    main._cache.clear()
    out = asyncio.run(main.get_fares_coverage())
    assert out["available"] and out["count"] == 1
    assert out["datasets"] == [{"id": 7, "name": "B&H fares", "noc": ["BHBC"], "numOfFareProducts": 12}]
    assert "bods-key-not-real" not in str(out), "the key came back in the response"
    url, params, _ = FakeClient.calls[0]
    assert url.endswith("/fares/dataset/") and params["noc"].startswith("BHBC")
