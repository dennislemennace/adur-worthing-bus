"""Rail replacement buses on a station's board.

Realtime Trains lists a train operator's replacement buses on the station's
board beside its trains: on 1 November 2026 Brighton's had Southern's 18:31,
18:45 and 19:15 to Lewes, each with headcode 0B00, the code a bus runs under,
and no platform. The site drew them as trains. The board now says which are
buses, so the page can say where they leave from instead of a platform.

Run with:  pytest
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import api.main as main     # noqa: E402


def _entry(headcode, platform=None):
    return {
        "scheduleMetadata": {"identity": "U1", "trainReportingIdentity": headcode,
                             "departureDate": "2026-11-01", "operator": {"code": "SN", "name": "Southern"}},
        "locationMetadata": {"platform": {"planned": platform} if platform else None},
        "temporalData": {"displayAs": "CALL", "departure": {"scheduledAdvertised": "2026-11-01T18:31:00"}},
        "origin": [{"location": {"description": "Brighton", "shortCodes": ["BTN"]}}],
        "destination": [{"location": {"description": "Lewes", "shortCodes": ["LWS"]}}],
    }


def test_a_bus_headcode_is_a_replacement_bus():
    assert main._project_board_service(_entry("0B00"), board_crs="BTN")["mode"] == "bus"
    assert main._project_board_service(_entry("0B12"), board_crs="BTN")["mode"] == "bus"


def test_a_train_is_a_train():
    assert main._project_board_service(_entry("1A23", "4"), board_crs="BTN")["mode"] == "train"
    assert main._project_board_service(_entry(None), board_crs="BTN")["mode"] == "train"
