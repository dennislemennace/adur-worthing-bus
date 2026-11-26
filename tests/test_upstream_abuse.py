"""What a stranger's request can make the API do to the services behind it.

Found in the review of 10 November 2026. The API is public and unauthenticated,
and three things a request could do had nothing in the way:

* **Spend the metered allowance.** `/api/departures` took any `stopId` of 30
  characters or fewer. A stop the timetable does not hold went to TransportAPI
  as the only source of times, so every made-up id spent one call of the day's
  allowance, and a short loop spent all of it.
* **Choose the upstream address.** The id went into the URL's path as it came:
  `../../train/station/BTN` asked a different TransportAPI product, with this
  site's credentials.
* **Read what should not leave the server.** A failed upstream call was logged
  with the exception's text, which for an HTTP error is the whole URL, API key
  included; and the rail routes sent that text back to the caller.

No call is made: the timetable is three lines and the HTTP client is a fake that
answers with real httpx responses, so the errors raised are the real ones.
"""

import asyncio
import logging
import sys
from datetime import date
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import api.main as main                                          # noqa: E402

KNOWN = "4400KNOWN1"
SECRET = "SECRETKEY"


class Board:
    """Enough timetable for a board with nothing on it: one stop, no times."""

    def __init__(self):
        self.stops = {KNOWN: {"lat": 50.83, "lon": -0.27, "name": "Known stop"}}
        self.stops_with_times = frozenset()
        self.trips, self.routes = {}, {}

    def has_stop_times(self, _stop_id):
        return False

    def trip_stops_for(self, _trip_id):
        return []


def _async(value):
    async def wrapper(*_args, **_kwargs):
        return value
    return wrapper


@pytest.fixture
def asked(monkeypatch):
    """A board whose every upstream is off except TransportAPI, which is
    counted: the stop ids it was asked for."""
    main._cache.clear()
    monkeypatch.setattr(main, "_get_timetable", _async(Board()))
    monkeypatch.setattr(main, "_departures_for_stop",
                        lambda _tt, _stop: {"stop_name": "", "departures": [], "services_here": []})
    for name in ("_disruptions_state", "_diversions_doc", "_recovery_doc", "_turnarounds_doc"):
        monkeypatch.setattr(main, name, _async(None))
    monkeypatch.setattr(main, "NEXTBUSES_APP_ID", "id")
    monkeypatch.setattr(main, "NEXTBUSES_APP_KEY", SECRET)
    monkeypatch.setattr(main, "_nb_quota", {"date": date.today().isoformat(), "count": 0})
    monkeypatch.setattr(main, "_nb_quota_save", lambda: None)
    stops = []

    async def fetch(stop_id):
        stops.append(stop_id)
        return []
    monkeypatch.setattr(main, "_fetch_nextbuses", fetch)
    yield stops
    main._cache.clear()


def _get(path, **params):
    return TestClient(main.app, raise_server_exceptions=False).get(path, params=params)


# ── The metered allowance ─────────────────────────────────────

def test_a_stop_the_timetable_does_not_hold_is_never_asked_of_transportapi(asked):
    r = _get("/api/departures", stopId="NOTASTOP1")
    assert r.status_code == 200 and r.json()["departures"] == []
    assert asked == [], "a made-up stop id reached the metered service"
    assert main._nb_quota["count"] == 0, "a made-up stop id spent the day's allowance"


def test_a_stop_the_timetable_holds_but_has_no_times_for_is_still_asked(asked):
    # Not a regression test: it holds the line the fix above must not cross.
    # TransportAPI is the only source of times for such a stop.
    assert _get("/api/departures", stopId=KNOWN).status_code == 200
    assert asked == [KNOWN]
    assert main._nb_quota["count"] == 1


# ── The upstream address ──────────────────────────────────────

ODD_IDS = ["../../train/station/BTN", "a?b", "a/b", "4400 SH01", "x" * 21]


@pytest.mark.parametrize("stop_id", ODD_IDS)
def test_a_stop_id_that_is_not_one_is_refused_before_anything_is_asked(asked, stop_id):
    r = _get("/api/departures", stopId=stop_id)
    assert r.status_code == 422, r.text
    assert asked == []


@pytest.mark.parametrize("path, params", [
    ("/api/stop-span", {"stopId": "a/b"}),
    ("/api/next-departures", {"stopId": "a/b"}),
    ("/api/journey", {"from": "a/b", "to": KNOWN}),
    ("/api/journey", {"from": KNOWN, "to": "a/b"}),
    ("/api/trip", {"tripId": "VJ_1", "from": "a/b"}),
])
def test_every_route_that_takes_a_stop_id_refuses_one_that_is_not(asked, path, params):
    assert _get(path, **params).status_code == 422


def test_a_journey_opened_with_no_stop_chosen_is_not_refused(asked):
    # The page sends an empty `from` when a journey is opened from a saved
    # stop's card with no board open (app.js, the departure plan). The answer
    # here is 404 only because this timetable holds no journeys.
    assert _get("/api/trip", **{"tripId": "VJ_1", "from": ""}).status_code == 404


class Upstream:
    """httpx.AsyncClient, answering every request with `status` and keeping
    what was asked. The response is a real one, so `raise_for_status` raises
    what httpx raises: an error whose text is the URL, query string and all."""

    status = 200
    requests: list = []
    error = None

    def __init__(self, **_kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        pass

    async def _answer(self, method, url, params=None, **_kwargs):
        if Upstream.error is not None:
            raise Upstream.error
        request = httpx.Request(method, url, params=params)
        Upstream.requests.append(request)
        return httpx.Response(Upstream.status, request=request, content=b"{}")

    async def get(self, url, **kwargs):
        return await self._answer("GET", url, **kwargs)

    async def post(self, url, **kwargs):
        return await self._answer("POST", url, params=kwargs.get("params"))


@pytest.fixture
def upstream(monkeypatch):
    main._cache.clear()
    Upstream.status, Upstream.requests, Upstream.error = 200, [], None
    monkeypatch.setattr(main.httpx, "AsyncClient", Upstream)
    monkeypatch.setattr(main, "NEXTBUSES_APP_ID", "id")
    monkeypatch.setattr(main, "NEXTBUSES_APP_KEY", SECRET)
    monkeypatch.setattr(main, "BODS_API_KEY", SECRET)
    yield Upstream
    main._cache.clear()


def test_a_stop_id_cannot_change_which_transportapi_address_is_asked(upstream, monkeypatch):
    # Validation is the first line; this is the second, at the request itself.
    monkeypatch.setattr(main, "NEXTBUSES_MODE", "rest")
    asyncio.run(main._fetch_nextbuses("../../train/station/BTN"))
    [request] = upstream.requests
    assert request.url.path.startswith("/v3/uk/bus/stop/"), request.url.path
    assert request.url.path.endswith("/live.json")


# ── What leaves the server ────────────────────────────────────

def _rest(monkeypatch):
    monkeypatch.setattr(main, "NEXTBUSES_MODE", "rest")
    return main._fetch_nextbuses(KNOWN)


def _siri(monkeypatch):
    monkeypatch.setattr(main, "NEXTBUSES_MODE", "siri")
    return main._fetch_nextbuses(KNOWN)


@pytest.mark.parametrize("call", [
    _rest, _siri,
    lambda _mp: main._fetch_declared_journeys(),
    lambda _mp: main._fetch_disruptions(),
], ids=["transportapi-rest", "transportapi-siri", "gtfs-rt", "siri-sx"])
def test_a_failed_upstream_call_is_logged_without_its_key(upstream, monkeypatch, caplog, call):
    upstream.status = 503
    with caplog.at_level(logging.INFO, logger="bus_api"):
        asyncio.run(call(monkeypatch))
    assert upstream.requests, "the fake upstream was never asked: the test proves nothing"
    assert SECRET in str(upstream.requests[0].url), "the key was not in the URL to begin with"
    assert caplog.text, "the failure was not logged at all"
    assert SECRET not in caplog.text, "an upstream failure wrote the API key to the log"
    assert "503" in caplog.text, "the log no longer says what went wrong"


@pytest.fixture
def rail(upstream, monkeypatch):
    monkeypatch.setattr(main, "_rtt_headers", _async({}))
    monkeypatch.setattr(main, "_log_rtt_quota", lambda _resp: None)
    return upstream


@pytest.mark.parametrize("uid", ["../x", "a b", "W1234?x=1", "x" * 17])
def test_a_rail_service_id_that_is_not_one_is_refused(rail, uid):
    assert _get("/api/rail-service", uid=uid, date="2026-11-10").status_code == 422
    assert rail.requests == []


def test_a_rail_failure_does_not_tell_the_caller_what_the_server_saw(rail):
    rail.error = RuntimeError("could not reach https://internal.example/?token=abc123")
    crs = sorted(main.RAIL_CRS_SET)[0]
    for r in (_get("/api/rail-service", uid="W12345", date="2026-11-10"),
              _get("/api/rail-departures", crs=crs)):
        assert r.status_code == 502, r.text
        assert "abc123" not in r.text and "internal.example" not in r.text
