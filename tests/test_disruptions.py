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
OUR_ROUTES = {("SCSO", "700"), ("SCSO", "N700"), ("SCSO", "9"), ("BHBC", "2"),
              ("NATX", "025")}


def area():
    return sx.parse_feed(FEED, sx.area_filter(OUR_STOPS, OUR_ROUTES))


def test_every_situation_is_read():
    ids = [s["id"] for s in sx.parse_feed(FEED)]
    assert len(ids) == 8 and "DIVERSION-700" in ids


def test_only_situations_touching_this_area_are_kept():
    ids = {s["id"] for s in area()}
    assert "ELSEWHERE" not in ids, "a Leeds closure reached a Shoreham board"
    assert "CLOSED-700" not in ids, "a situation marked closed was kept"
    assert "COACH-ALL" not in ids, "an operator-wide coach notice would sit on every local board"
    assert {"DIVERSION-700", "STOP-CLOSED", "BHBC-ALL", "EXPIRED-9", "TOMORROW-N700"} <= ids


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
    assert len(sx.parse_feed(buf.getvalue())) == 8


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
