"""One bus, reported twice, is one bus.

On 2 November 2026 every Stagecoach bus came through BODS twice, a few seconds
apart, once as "SCSO-10952" and once as "10952": the map drew two buses on the
spot, one on top of the other, and the night's figures would have timed each
journey twice. The reports are merged on the operator and the fleet number,
keeping the latest, under the "SCSO-" name the site has always used.

Run with:  pytest
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import api.main as main                  # noqa: E402
import process_snapshots as ps           # noqa: E402

NOW = datetime(2026, 11, 2, 10, 47, 30, tzinfo=timezone.utc)


def activity(ref, op, line, at, lat=50.8270, lon=-0.1510, bearing=104.0):
    return f"""<VehicleActivity><RecordedAtTime>{at}</RecordedAtTime><MonitoredVehicleJourney>
     <LineRef>{line}</LineRef><PublishedLineName>{line}</PublishedLineName><OperatorRef>{op}</OperatorRef>
     <DestinationName>Old Steine</DestinationName>
     <VehicleLocation><Longitude>{lon}</Longitude><Latitude>{lat}</Latitude></VehicleLocation>
     <Bearing>{bearing}</Bearing><VehicleRef>{ref}</VehicleRef></MonitoredVehicleJourney></VehicleActivity>"""


def siri(*activities):
    return ('<?xml version="1.0"?><Siri xmlns="http://www.siri.org.uk/siri"><ServiceDelivery>'
            f'<VehicleMonitoringDelivery>{"".join(activities)}</VehicleMonitoringDelivery>'
            '</ServiceDelivery></Siri>')


FEED = siri(
    activity("10952", "SCSO", "700", "2026-11-02T10:47:07+00:00", bearing=104.0),
    activity("SCSO-10952", "SCSO", "700", "2026-11-02T10:47:20+00:00", lat=50.82701, bearing=0.0),
    # Another operator's bus with the same number is another bus.
    activity("10952", "BHBC", "7", "2026-11-02T10:47:10+00:00", lat=50.8300),
    activity("SCSO-27660", "SCSO", "9", "2026-11-02T10:47:13+00:00", lat=50.8400),
)


def test_the_map_draws_a_bus_reported_twice_once():
    vehicles = main._parse_siri_vm(FEED, now=NOW)
    refs = sorted((v["operator_ref"], v["vehicle_ref"]) for v in vehicles)
    assert refs == [("BHBC", "10952"), ("SCSO", "SCSO-10952"), ("SCSO", "SCSO-27660")], refs
    kept = next(v for v in vehicles if v["vehicle_ref"] == "SCSO-10952")
    assert kept["recorded_at"].startswith("2026-11-02T10:47:20"), "the older report was kept"


def test_the_nights_figures_count_it_once():
    vehicles = ps.parse_snapshot(FEED, at_utc=NOW)
    scso = sorted(v["vehicle_ref"] for v in vehicles if v["operator_ref"] == "SCSO")
    assert scso == ["SCSO-10952", "SCSO-27660"], scso


def test_a_bare_number_alone_is_named_as_before():
    # Only one report, without the prefix: still the "SCSO-" name, so a bus
    # keeps one identity whichever of the two feeds happens to report it.
    vehicles = main._parse_siri_vm(siri(activity("10952", "SCSO", "700", "2026-11-02T10:47:07+00:00")), now=NOW)
    assert [v["vehicle_ref"] for v in vehicles] == ["SCSO-10952"]
