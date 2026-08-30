"""Tests for the /api/debug/* gate, the OpenAPI gate and the CORS allowlist.

Background: nine diagnostic endpoints were reachable in production, seven of
them ungated. One (`/api/debug/live-raw`) called TransportAPI directly with no
cache and no quota accounting, so it bypassed NEXTBUSES_DAILY_LIMIT and spent
the real upstream allowance. `/openapi.json` published the whole list.

The test that matters most here is `test_every_debug_route_is_gated`: it walks
the registered routes rather than naming them, so a diagnostic added later
without the gate fails CI instead of shipping.

Run with:  pytest
"""

import importlib
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import api.main as main  # noqa: E402

SITE_ORIGIN = "https://dennislemennace.github.io"


def debug_paths(app):
    return sorted(
        r.path for r in app.routes if getattr(r, "path", "").startswith("/api/debug")
    )


@pytest.fixture
def debug_on(monkeypatch):
    """Reload the module with diagnostics switched on, then put it back."""
    monkeypatch.setenv("DEBUG_ENABLED", "1")
    reloaded = importlib.reload(main)
    yield reloaded
    monkeypatch.delenv("DEBUG_ENABLED", raising=False)
    importlib.reload(main)


# ── The gate ────────────────────────────────────────────────

def test_every_debug_route_is_gated():
    """Walk the routes; do not name them. A new ungated diagnostic fails here."""
    paths = debug_paths(main.app)
    assert paths, "expected /api/debug routes to exist"

    ungated = []
    for route in main.app.routes:
        if not getattr(route, "path", "").startswith("/api/debug"):
            continue
        calls = [d.call for d in route.dependant.dependencies]
        if main.require_debug not in calls:
            ungated.append(route.path)

    assert ungated == [], f"debug routes missing require_debug: {ungated}"


def test_debug_routes_404_by_default():
    client = TestClient(main.app)
    for path in debug_paths(main.app):
        resp = client.get(path, params={"stopId": "149000007413", "crs": "WRH"})
        assert resp.status_code == 404, f"{path} returned {resp.status_code}"


def test_gate_returns_404_not_403():
    """403 would confirm the route exists — that is the leak, not the payload."""
    resp = TestClient(main.app).get("/api/debug/nb-quota")
    assert resp.status_code == 404
    assert resp.status_code != 403


def test_debug_routes_reachable_when_enabled(debug_on, monkeypatch):
    """The flag must actually open the gate, or it is security theatre.

    Asserting "not 404" is the real check: a 503 here means the request got
    past require_debug and was turned away by the missing-BODS-key guard
    instead, which is the endpoint's own business.
    """
    client = TestClient(debug_on.app)
    assert client.get("/api/debug/nb-quota").status_code != 404

    monkeypatch.setattr(debug_on, "BODS_API_KEY", "test-key")
    resp = client.get("/api/debug/nb-quota")
    assert resp.status_code == 200
    assert "count" in resp.json()


# ── OpenAPI ─────────────────────────────────────────────────

def test_openapi_is_off_by_default():
    assert main.app.openapi_url is None
    assert main.app.docs_url is None
    assert main.app.redoc_url is None
    client = TestClient(main.app)
    for path in ("/openapi.json", "/docs", "/redoc"):
        assert client.get(path).status_code == 404, f"{path} is exposed"


def test_openapi_available_when_debug_enabled(debug_on):
    assert TestClient(debug_on.app).get("/openapi.json").status_code == 200


def test_debug_routes_stay_out_of_the_schema(debug_on):
    """Even switched on, diagnostics should not be advertised in the schema."""
    schema = TestClient(debug_on.app).get("/openapi.json").json()
    advertised = [p for p in schema["paths"] if p.startswith("/api/debug")]
    assert advertised == []


# ── Real routes keep working ────────────────────────────────

def test_health_endpoint_is_unaffected():
    resp = TestClient(main.app).get("/")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ── CORS ────────────────────────────────────────────────────

def test_cors_allows_the_site_origin():
    resp = TestClient(main.app).get("/", headers={"Origin": SITE_ORIGIN})
    assert resp.headers.get("access-control-allow-origin") == SITE_ORIGIN


def test_cors_rejects_an_unknown_origin():
    """The wildcard this replaces let any site build on the Render quota."""
    resp = TestClient(main.app).get("/", headers={"Origin": "https://evil.example"})
    assert resp.headers.get("access-control-allow-origin") is None


def test_cors_allows_localhost_for_the_preview_flow():
    """README documents `python -m http.server 8765` + ?api=localhost:8000."""
    for origin in ("http://localhost:8765", "http://127.0.0.1:8000"):
        resp = TestClient(main.app).get("/", headers={"Origin": origin})
        assert resp.headers.get("access-control-allow-origin") == origin, origin


def test_allowed_origin_env_var_is_honoured(monkeypatch):
    """render.yaml has documented this var all along; it must actually work."""
    monkeypatch.setenv("ALLOWED_ORIGIN", "https://example.test,https://other.test")
    reloaded = importlib.reload(main)
    try:
        assert reloaded._allowed_origins == ["https://example.test", "https://other.test"]
        resp = TestClient(reloaded.app).get("/", headers={"Origin": "https://other.test"})
        assert resp.headers.get("access-control-allow-origin") == "https://other.test"
    finally:
        monkeypatch.delenv("ALLOWED_ORIGIN", raising=False)
        importlib.reload(main)


# ── Quota ───────────────────────────────────────────────────

def test_live_raw_respects_the_daily_quota(debug_on, monkeypatch):
    """It must refuse at the cap rather than calling upstream anyway.

    The old version had no quota check at all, so this asserts the fix
    directly: at the limit it returns the refusal without a network call.
    """
    monkeypatch.setattr(debug_on, "NEXTBUSES_APP_ID", "x")
    monkeypatch.setattr(debug_on, "NEXTBUSES_APP_KEY", "y")
    monkeypatch.setattr(debug_on, "BODS_API_KEY", "z")
    monkeypatch.setattr(debug_on, "NEXTBUSES_DAILY_LIMIT", 5)
    monkeypatch.setitem(debug_on._nb_quota, "count", 5)

    def explode(*a, **kw):
        raise AssertionError("live-raw called upstream despite the quota being spent")

    monkeypatch.setattr(debug_on.httpx, "AsyncClient", explode)

    resp = TestClient(debug_on.app).get("/api/debug/live-raw", params={"stopId": "1490001"})
    assert resp.status_code == 200
    assert resp.json()["error"] == "quota exhausted"
