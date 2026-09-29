"""scripts/build_fares.py: both NeTEx dialects read into the same fare triangle.

The files here are cut down by hand from the shapes the operators publish:
Brighton & Hove's price bands and Stagecoach's per-pair prices. Nothing is
downloaded.
"""

import io
import json
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import build_fares  # noqa: E402

NS = 'xmlns="http://www.netex.org.uk/netex"'

BANDS = f"""<PublicationDelivery {NS}>
<Line id="BHBC:PK:7:12X"><Name>12X Outbound</Name><Description>Brighton Station - Eastbourne</Description>
  <PublicCode>12X</PublicCode><OperatorRef ref="noc:BHBC"/></Line>
<ScheduledStopPoint id="atco:149000006934"><Name>Brighton Station B5</Name></ScheduledStopPoint>
<ScheduledStopPoint id="atco:149000006355"><Name>Imperial Arcade C8</Name></ScheduledStopPoint>
<ScheduledStopPoint id="atco:1400EAST"><Name>Eastbourne</Name></ScheduledStopPoint>
<FareZone id="fs@38@boarding"><Name>Brighton Station</Name><members><ScheduledStopPointRef ref="atco:149000006934"/></members></FareZone>
<FareZone id="fs@38@alighting"><Name>Brighton Station</Name><members><ScheduledStopPointRef ref="atco:149000006934"/></members></FareZone>
<FareZone id="fs@65@boarding"><Name>Churchill Square</Name><members><ScheduledStopPointRef ref="atco:149000006355"/></members></FareZone>
<FareZone id="fs@65@alighting"><Name>Churchill Square</Name><members><ScheduledStopPointRef ref="atco:149000006355"/></members></FareZone>
<FareZone id="fs@90@alighting"><Name>Eastbourne</Name><members><ScheduledStopPointRef ref="atco:1400EAST"/></members></FareZone>
<ValidBetween><FromDate>2001-01-01T00:00:00Z</FromDate></ValidBetween>
<ValidBetween><FromDate>2026-09-23T00:00:00</FromDate></ValidBetween>
<ValidBetween><FromDate>2999-01-01T00:00:00</FromDate></ValidBetween>
<DistanceMatrixElement id="38+65"><priceGroups><PriceGroupRef ref="price_band_1.8"/></priceGroups>
  <StartTariffZoneRef ref="fs@38@boarding"/><EndTariffZoneRef ref="fs@65@alighting"/></DistanceMatrixElement>
<DistanceMatrixElement id="38+90"><priceGroups><PriceGroupRef ref="price_band_3.0"/></priceGroups>
  <StartTariffZoneRef ref="fs@38@boarding"/><EndTariffZoneRef ref="fs@90@alighting"/></DistanceMatrixElement>
<DistanceMatrixElement id="38+38"><StartTariffZoneRef ref="fs@38@boarding"/><EndTariffZoneRef ref="fs@38@alighting"/></DistanceMatrixElement>
<PriceGroup id="price_band_1.8"><members><GeographicalIntervalPrice><Amount>1.8</Amount></GeographicalIntervalPrice></members></PriceGroup>
<PriceGroup id="price_band_3.0"><members><GeographicalIntervalPrice><Amount>3.0</Amount></GeographicalIntervalPrice></members></PriceGroup>
</PublicationDelivery>"""

PER_PAIR = f"""<PublicationDelivery {NS}>
<Line id="SCSO:PH:137:17"><Name>Brighton - Horsham</Name><PublicCode>17</PublicCode>
  <OperatorRef ref="noc:SCSO">noc:1</OperatorRef></Line>
<ScheduledStopPoint id="atco:149000007830"><Name>Old Steine</Name><NameSuffix>Stop E</NameSuffix></ScheduledStopPoint>
<ScheduledStopPoint id="atco:4400HO0001"><Name>Carfax</Name></ScheduledStopPoint>
<FareZone id="zone@1@0@boarding"><Name>BrightonCentre</Name><members><ScheduledStopPointRef ref="atco:149000007830"/></members></FareZone>
<FareZone id="zone@2@1@alighting"><Name>HorshamCentre</Name><members><ScheduledStopPointRef ref="atco:4400HO0001"/></members></FareZone>
<FromDate>2026-09-04T00:00:00</FromDate>
<DistanceMatrixElement id="d1"><StartTariffZoneRef ref="zone@1@0@boarding"/><EndTariffZoneRef ref="zone@2@1@alighting"/></DistanceMatrixElement>
<DistanceMatrixElementPrice id="p1"><Amount>3.00</Amount><DistanceMatrixElementRef ref="d1"/></DistanceMatrixElementPrice>
</PublicationDelivery>"""


def test_price_bands_are_read_into_a_triangle():
    [t] = build_fares.parse_table(BANDS.encode())
    assert (t["line"], t["operator"]) == ("12X", "BHBC")
    names = [st["name"] for st in t["stages"]]
    assert names == ["Brighton Station", "Churchill Square", "Eastbourne"]
    assert t["prices"] == [[0, 1, 180], [0, 2, 300]], "a stage to itself has no fare"
    assert t["stages"][2]["board"] == [] and t["stages"][2]["alight"] == ["1400EAST"]


def test_the_fares_start_on_the_latest_date_that_has_come():
    [t] = build_fares.parse_table(BANDS.encode())
    assert t["valid_from"] == "2026-09-23", "an old frame date or a future one was taken"


def test_per_pair_prices_are_read_into_the_same_shape():
    [t] = build_fares.parse_table(PER_PAIR.encode())
    assert (t["line"], t["operator"]) == ("17", "SCSO")
    assert t["prices"] == [[0, 1, 300]]
    assert t["stop_names"]["149000007830"] == "Old Steine Stop E"


def _zip(files):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, body in files.items():
            z.writestr(name, body)
    return buf.getvalue()


def test_only_adult_singles_calling_in_the_area_are_kept():
    bh = _zip({"BHBC_12X_Outbound_AdultSingle_x.xml": BANDS,
               "BHBC_12X_Outbound_ChildSingle_x.xml": BANDS,
               "BHBC_12X_Outbound_AdultWeekly_x.xml": BANDS})
    sc = _zip({"FX_SCSO_LINE_FARE_17_O_ADULT‐SINGLE.xml": PER_PAIR,
               "FX_SCSO_LINE_FARE_17_O_16-20‐Sgl.xml": PER_PAIR})
    out = build_fares.build({"BHBC": (1, bh), "SCSO": (2, sc)}, area={"149000006934"})
    assert [(t["operator"], t["line"]) for t in out["tables"]] == [("BHBC", "12X")], \
        "a child fare, a weekly or a route outside the area was kept"
    assert out["tables"][0]["direction"] == "Brighton Station to Eastbourne"
    assert out["counts"]["SCSO"] == {"adult_single_files": 1, "tables_kept": 0}


def test_the_published_file_carries_its_provenance():
    data = json.loads((ROOT / "data" / "fare_tables.json").read_text())
    for field in ("generated_at", "as_of", "method", "caveats", "sources", "tables"):
        assert data.get(field), f"fare_tables.json has no {field}"
    for src in data["sources"]:
        assert src["source_url"].startswith("https://data.bus-data.dft.gov.uk/fares/dataset/")
    for t in data["tables"]:
        n = len(t["stages"])
        assert t["line"] and t["operator"] and t["prices"], t.get("line")
        for i, j, pence in t["prices"]:
            assert 0 <= i < n and 0 <= j < n and 0 < pence <= 1500, (t["line"], i, j, pence)
