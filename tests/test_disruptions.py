"""Published disruptions from BODS SIRI-SX (api/disruptions.py), and where they show.

A notice is the author's claim, shown as theirs; these tests pin that only the
ones touching this area reach a board, that dead ones do not, and that each
lands beside the rows it is about.
"""

import io
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import api.main as main                                          # noqa: E402
from api import disruptions as sx                                # noqa: E402

FEED = (ROOT / "tests" / "fixtures" / "siri_sx_sample.xml").read_bytes()
NOW = datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc)
OUR_STOPS = {"1490WESTBV", "4400AD0204", "4400AD0063"}
OUR_ROUTES = {("SCSO", "700"), ("SCSO", "N700"), ("SCSO", "9"), ("SCSO", "6"),
              ("BHBC", "2"), ("NATX", "025")}


OUR_TOWNS = {"Worthing", "Lancing", "Shoreham-by-Sea", "Brighton", "Hove"}


def area():
    return sx.parse_feed(FEED, sx.area_filter(OUR_STOPS, OUR_ROUTES, OUR_TOWNS))


def test_every_situation_is_read():
    ids = [s["id"] for s in sx.parse_feed(FEED)]
    assert len(ids) == 12 and "DIVERSION-700" in ids


def test_only_situations_touching_this_area_are_kept():
    ids = {s["id"] for s in area()}
    assert "ELSEWHERE" not in ids, "a Leeds closure reached a Shoreham board"
    assert "CLOSED-700" not in ids, "a situation marked closed was kept"
    assert "COACH-ALL" not in ids, "an operator-wide coach notice would sit on every local board"
    assert "HAMPSHIRE-9" not in ids, \
        "a Hampshire stop suspension on Stagecoach's 9 reached our Stagecoach 9"
    assert {"DIVERSION-700", "STOP-CLOSED", "BHBC-ALL", "EXPIRED-9", "TOMORROW-N700"} <= ids


def test_a_regional_operator_line_number_alone_does_not_place_a_notice_here():
    # Stagecoach South is one operator code from Hampshire to Sussex and
    # reuses route numbers. Live, 27 Sep 2026: Hampshire's road closure in
    # Aldershot on Stagecoach 3, 7 and 15 named no stops and matched
    # Worthing's Stagecoach 7. A notice that reaches us only through a
    # regional operator needs a local publisher or one of our towns.
    ids = {s["id"] for s in area()}
    assert "ALDERSHOT-9" not in ids, "an Aldershot road closure reached a Worthing board"
    assert "WORTHING-6" in ids, "a notice that names Worthing was dropped"
    assert "EXPIRED-9" in ids, "a West Sussex council notice needs no town named"
    assert "SOUTHWICK-9" not in ids, (
        "Southwick is also in Hampshire; only towns our stops are in count, "
        "and Southwick is not one of them")
    assert "TOMORROW-N700" in ids, "'Shoreham' did not count as Shoreham-by-Sea"


def test_a_local_only_operator_needs_no_town():
    # Brighton & Hove Buses runs only here, so its operator code places the
    # notice by itself.
    assert "BHBC-ALL" in {s["id"] for s in area()}


def test_only_those_in_force_or_starting_soon_are_published():
    now = {d["id"]: d for d in sx.current(area(), NOW)}
    assert "EXPIRED-9" not in now, "last week's diversion is still on the board"
    assert now["DIVERSION-700"]["starts"] is None
    assert now["DIVERSION-700"]["ends"].startswith("2026-10-03")
    assert now["TOMORROW-N700"]["starts"].startswith("2026-09-28T00:30"), \
        "tonight's change was hidden until it began"


def test_a_notice_keeps_its_own_words_and_author():
    d = next(d for d in sx.current(area(), NOW) if d["id"] == "DIVERSION-700")
    assert d["summary"] == "700 diverted via Church Road"
    assert d["advice"] == "Use the stop on Church Road."
    assert d["publisher"] == "WestSussexCC"
    assert d["reason"] == "roadworks"
    assert d["link"] == "https://www.stagecoachbus.com/service-updates"
    assert d["lines"] == [{"operator": "SCSO", "line": "700"}]


def test_a_zipped_delivery_is_read_the_same():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("sirisx.xml", FEED)
    assert len(sx.parse_feed(buf.getvalue())) == 12


def test_a_night_n_matches_the_day_number_either_way():
    assert sx.service_key("N700") == sx.service_key("700") == "700"
    assert sx.service_key("N") == "N"


def test_a_board_carries_the_notices_for_its_stop_and_rows():
    now = sx.current(area(), NOW)
    board = {"departures": [
        {"service": "700", "operator": "SCSO", "aimed_departure": "x"},
        {"service": "2", "operator": "BHBC", "aimed_departure": "y"},
        {"service": "9", "operator": "SCSO", "aimed_departure": "z"},
    ]}
    out = main._attach_disruptions(board, "4400AD0204", now)
    assert {d["id"] for d in out["disruptions"]} == {
        "DIVERSION-700", "STOP-CLOSED", "BHBC-ALL", "TOMORROW-N700"}
    rows = out["departures"]
    assert set(rows[0]["disruption_ids"]) == {"DIVERSION-700", "TOMORROW-N700"}
    assert rows[1]["disruption_ids"] == ["BHBC-ALL"], "an operator-wide notice missed its row"
    assert "disruption_ids" not in rows[2], "the 9 was tagged with someone else's diversion"


def test_a_board_with_nothing_relevant_is_untouched():
    board = {"departures": [{"service": "9", "operator": "SCSO"}]}
    assert main._attach_disruptions(board, "4400AD0063", sx.current(area(), NOW)) is board


def test_no_key_means_no_fetch_and_says_so(monkeypatch):
    import asyncio
    monkeypatch.setattr(main, "BODS_API_KEY", "")
    state = asyncio.run(main._fetch_disruptions())
    assert state["available"] is False and state["reason"] == "not_configured"


# ── Cancelled journeys ──────────────────────────────────────

CANCELLATION = b"""<?xml version="1.0" encoding="utf-8"?>
<Siri version="2.0" xmlns="http://www.siri.org.uk/siri"><ServiceDelivery><SituationExchangeDelivery><Situations>
  <PtSituationElement>
    <ParticipantRef>SCSO</ParticipantRef><SituationNumber>CANCEL-700-1405</SituationNumber>
    <Progress>open</Progress>
    <ValidityPeriod><StartTime>2026-09-27T06:00:00Z</StartTime><EndTime>2026-09-27T23:00:00Z</EndTime></ValidityPeriod>
    <MiscellaneousReason>staffShortage</MiscellaneousReason>
    <Summary>Journey cancelled</Summary>
    <Affects><VehicleJourneys><AffectedVehicleJourney>
      <Operator><OperatorRef>SCSO</OperatorRef></Operator>
      <PublishedLineName>700</PublishedLineName>
      <DatedVehicleJourneyRef>4012</DatedVehicleJourneyRef>
      <OriginAimedDepartureTime>2026-09-27T13:05:00+01:00</OriginAimedDepartureTime>
      <Calls><Call><StopPointRef>4400AD0063</StopPointRef></Call></Calls>
    </AffectedVehicleJourney></VehicleJourneys></Affects>
    <Consequences><Consequence><Condition>cancelled</Condition></Consequence></Consequences>
  </PtSituationElement>
  <PtSituationElement>
    <ParticipantRef>SCSO</ParticipantRef><SituationNumber>CANCEL-HAMPSHIRE</SituationNumber>
    <Progress>open</Progress>
    <Summary>Journey cancelled</Summary>
    <Affects><VehicleJourneys><AffectedVehicleJourney>
      <Operator><OperatorRef>SCSO</OperatorRef></Operator>
      <PublishedLineName>700</PublishedLineName>
      <OriginAimedDepartureTime>2026-09-27T13:20:00+01:00</OriginAimedDepartureTime>
      <Calls><Call><StopPointRef>1900HA000001</StopPointRef></Call></Calls>
    </AffectedVehicleJourney></VehicleJourneys></Affects>
    <Consequences><Consequence><Condition>cancelled</Condition></Consequence></Consequences>
  </PtSituationElement>
</Situations></SituationExchangeDelivery></ServiceDelivery></Siri>"""


class JourneyTimetable:
    """Two 700 journeys, starting 13:05 and 13:20, at the same board."""
    def trip_stops_for(self, trip_id):
        start = {"T1305": 13 * 3600 + 5 * 60, "T1320": 13 * 3600 + 20 * 60}[trip_id]
        return [(start, "ORIGIN"), (start + 1200, "4400AD0063")]


def test_a_cancelled_journey_is_marked_on_its_own_row_only():
    kept = sx.parse_feed(CANCELLATION, sx.area_filter(OUR_STOPS, OUR_ROUTES, OUR_TOWNS))
    assert [s["id"] for s in kept] == ["CANCEL-700-1405"], \
        "a Hampshire 700's cancellation was placed here by its line number"
    notices = sx.current(kept, NOW)
    board = {"departures": [
        {"service": "700", "operator": "SCSO", "_trip_id": "T1305", "status": "Scheduled",
         "aimed_departure": "2026-09-27T13:25:00+01:00"},
        {"service": "700", "operator": "SCSO", "_trip_id": "T1320", "status": "Scheduled",
         "aimed_departure": "2026-09-27T13:40:00+01:00"},
    ]}
    out = main._attach_disruptions(board, "4400AD0063", notices, JourneyTimetable())
    first, second = out["departures"]
    assert first["status"] == "Cancelled" and first["disruption_ids"] == ["CANCEL-700-1405"]
    assert second["status"] == "Scheduled" and "disruption_ids" not in second, \
        "every 700 was marked, not the one journey called off"
