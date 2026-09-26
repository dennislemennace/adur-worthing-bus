"""NextBuses over SIRI Stop Monitoring, TransportAPI's NextBuses-only route.

The pay-per-hit plan covers `POST /nextbuses` and nothing else, so switching
plan means switching `NEXTBUSES_MODE` to "siri". These tests pin that the SIRI
response reads into the same predictions the board already uses, and that the
extra it carries (operator, vehicle, cancellation) is used rather than dropped.
No call is made: the response is TransportAPI's documented example and
variations on it.
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import api.main as main                                          # noqa: E402

# TransportAPI's documented example response, verbatim but for whitespace.
DOC_EXAMPLE = b"""<Siri xmlns="http://www.siri.org.uk/" version="1.0">
  <ServiceDelivery>
    <ResponseTimestamp>2026-01-12T10:22:38.705Z</ResponseTimestamp>
    <StopMonitoringDelivery version="1.0">
      <ResponseTimestamp>2026-01-12T10:22:38.705Z</ResponseTimestamp>
      <MonitoredStopVisit>
        <RecordedAtTime>2026-01-12T10:22:38.705Z</RecordedAtTime>
        <MonitoringRef>4900801620</MonitoringRef>
        <MonitoredVehicleJourney>
          <FramedVehicleJourneyRef>
            <DataFrameRef>-</DataFrameRef>
            <DatedVehicleJourneyRef>-</DatedVehicleJourneyRef>
          </FramedVehicleJourneyRef>
          <VehicleMode>bus</VehicleMode>
          <PublishedLineName>A4</PublishedLineName>
          <DirectionName>Cippenham, Moreton</DirectionName>
          <OperatorRef>FTVA</OperatorRef>
          <MonitoredCall>
            <AimedDepartureTime>2026-01-12T10:25:00.000Z</AimedDepartureTime>
            <ExpectedDepartureTime>2026-01-12T10:25:00.000Z</ExpectedDepartureTime>
          </MonitoredCall>
        </MonitoredVehicleJourney>
      </MonitoredStopVisit>
    </StopMonitoringDelivery>
  </ServiceDelivery>
</Siri>"""


def visit(line="700", op="SCSO", aimed="2026-09-27T13:10:00Z", expected=None,
          status=None, vehicle=None, monitored=None, ns="http://www.siri.org.uk/siri"):
    parts = [f"<PublishedLineName>{line}</PublishedLineName>",
             "<DirectionName>Brighton</DirectionName>", f"<OperatorRef>{op}</OperatorRef>"]
    if vehicle:
        parts.append(f"<VehicleRef>{vehicle}</VehicleRef>")
    if monitored is not None:
        parts.append(f"<Monitored>{monitored}</Monitored>")
    call = f"<AimedDepartureTime>{aimed}</AimedDepartureTime>"
    if expected:
        call += f"<ExpectedDepartureTime>{expected}</ExpectedDepartureTime>"
    if status:
        call += f"<DepartureStatus>{status}</DepartureStatus>"
    parts.append(f"<MonitoredCall>{call}</MonitoredCall>")
    body = "".join(parts)
    return (f'<Siri xmlns="{ns}"><ServiceDelivery><StopMonitoringDelivery><MonitoredStopVisit>'
            f"<MonitoredVehicleJourney>{body}</MonitoredVehicleJourney>"
            "</MonitoredStopVisit></StopMonitoringDelivery></ServiceDelivery></Siri>").encode()


def test_the_documented_example_reads_as_a_prediction():
    [p] = main._parse_siri_sm(DOC_EXAMPLE)
    assert p["service"] == "A4" and p["operator"] == "FTVA"
    assert p["direction"] == "Cippenham, Moreton"
    # 10:25Z in January is 10:25 in London; stored as a London time, like the REST parser.
    assert p["aimed"].startswith("2026-01-12T10:25:00") and p["aimed"].endswith("+00:00")
    assert p["expected"] == p["aimed"]
    assert p["cancelled"] is False and p["monitored"] is True


def test_summer_times_come_back_on_the_london_clock():
    [p] = main._parse_siri_sm(visit(aimed="2026-09-27T13:10:00Z", expected="2026-09-27T13:14:00Z"))
    assert p["aimed"] == "2026-09-27T14:10:00+01:00"
    assert p["expected"] == "2026-09-27T14:14:00+01:00"


def test_either_siri_namespace_is_read():
    assert main._parse_siri_sm(visit(ns="http://www.siri.org.uk/"))
    assert main._parse_siri_sm(visit(ns="http://www.siri.org.uk/siri"))


def test_a_cancelled_departure_says_so_and_has_no_time():
    [p] = main._parse_siri_sm(visit(expected="2026-09-27T13:12:00Z", status="cancelled"))
    assert p["cancelled"] is True and p["expected"] is None


def test_an_unmonitored_departure_has_no_expected_time_to_offer():
    # Unmonitored producers often copy the aimed time into "expected".
    [p] = main._parse_siri_sm(visit(monitored="false", expected="2026-09-27T13:10:00Z"))
    assert p["monitored"] is False and p["expected"] is None


def test_the_vehicle_working_it_is_kept():
    [p] = main._parse_siri_sm(visit(vehicle="SCSO-15621"))
    assert p["vehicle_ref"] == "SCSO-15621"


def test_the_request_asks_for_this_stop_over_the_next_two_hours():
    body = main._siri_sm_request("4400AD0204", datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc))
    assert "<MonitoringRef>4400AD0204</MonitoringRef>" in body
    assert "<PreviewInterval>PT2H</PreviewInterval>" in body
    assert "<RequestTimestamp>2026-09-27T12:00:00.000Z</RequestTimestamp>" in body
    assert "&lt;" in main._siri_sm_request("<x>"), "a stop id went into the XML unescaped"


def test_siri_mode_calls_the_siri_service(monkeypatch):
    called = []

    async def fake(stop_id):
        called.append(stop_id)
        return []

    monkeypatch.setattr(main, "NEXTBUSES_MODE", "siri")
    monkeypatch.setattr(main, "_fetch_nextbuses_siri", fake)
    assert asyncio.run(main._fetch_nextbuses("4400AD0204")) == []
    assert called == ["4400AD0204"]


# ── On the board ────────────────────────────────────────────

def _overlay(monkeypatch, predictions, departures):
    monkeypatch.setattr(main, "NEXTBUSES_APP_ID", "x")
    monkeypatch.setattr(main, "NEXTBUSES_APP_KEY", "y")
    monkeypatch.setattr(main, "cache_get",
                        lambda key: predictions if key == "nb:STOP" else None)
    return asyncio.run(main._apply_live_overlay({"departures": departures}, "STOP"))


def _soon(minutes):
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).astimezone(main.UK_TZ)


def test_a_prediction_for_another_operators_bus_is_not_applied(monkeypatch):
    aimed = _soon(10)
    pred = {"service": "2", "operator": "BHBC", "aimed": aimed.isoformat(),
            "expected": (aimed + timedelta(minutes=4)).isoformat()}
    row = {"service": "2", "operator": "SCSO", "aimed_departure": aimed.isoformat(),
           "expected_departure": None, "status": "Scheduled"}
    out = _overlay(monkeypatch, [pred], [row])
    assert out["departures"][0]["expected_departure"] is None, \
        "Brighton & Hove's 2 made Stagecoach's 2 late"


def test_a_cancellation_reaches_the_board(monkeypatch):
    aimed = _soon(10)
    pred = {"service": "700", "operator": "SCSO", "aimed": aimed.isoformat(),
            "expected": None, "cancelled": True, "vehicle_ref": "15621"}
    row = {"service": "700", "operator": "SCSO", "aimed_departure": aimed.isoformat(),
           "expected_departure": None, "status": "Scheduled"}
    dep = _overlay(monkeypatch, [pred], [row])["departures"][0]
    assert dep["status"] == "Cancelled" and dep["expected_departure"] is None
    assert dep["vehicle_ref"] == "15621"


def test_our_own_estimate_does_not_overrule_a_cancellation(monkeypatch):
    board = {"departures": [{"service": "700", "operator": "SCSO", "status": "Cancelled",
                             "aimed_departure": _soon(10).isoformat(),
                             "expected_departure": None, "_trip_id": "VJ"}]}
    monkeypatch.setattr(main, "cache_get", lambda key: {"vehicles": [
        {"trip_id": "VJ", "trip_source": "feed", "lateness_secs": 120}]}
        if key == main.RECENT_VEHICLES_KEY else None)
    out = main._apply_own_feed_estimates(board, None, "STOP", datetime.now(timezone.utc))
    assert out["departures"][0]["status"] == "Cancelled"
    assert out["departures"][0]["expected_departure"] is None
