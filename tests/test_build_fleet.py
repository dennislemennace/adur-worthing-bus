"""Each bus in its own livery (asked for 2 November 2026).

scripts/build_fleet.py turns bustimes.org's vehicle lists into data/fleet.json,
naming the icon drawn for each bus's livery; scripts/livery_designs.py draws
them and scripts/build_icons.py builds them. These check the rules that pick a
bus's icon, and that every icon named or designed exists at the site's size.

Run with:  pytest
"""
import json
import sys
from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import build_fleet as bf       # noqa: E402
import build_icons as bi       # noqa: E402
import livery_designs as ld    # noqa: E402


def vehicle(fleet, reg, livery_id=None, livery="", branding="", dd=True, notes=""):
    return {"fleet_code": fleet, "fleet_number": int(fleet) if fleet.isdigit() else None, "reg": reg,
            "livery": {"id": livery_id, "name": livery} if livery_id else None, "branding": branding,
            "notes": notes, "withdrawn": False,
            "vehicle_type": {"name": "ADL Enviro400 MMC" if dd else "ADL Enviro200 MMC",
                             "double_decker": dd, "coach": False}}


def test_the_1x_and_3x_wear_their_route_pills():
    assert bf.icon_for("BHBC", vehicle("740", "YX24OXL", 3057, "Brighton & Hove Buses", "1X")) == "liveries/bh-teal-1x-dd.png"
    assert bf.icon_for("BHBC", vehicle("751", "YY25HZK", 3057, "Brighton & Hove Buses", "3X")) == "liveries/bh-teal-3x-dd.png"
    assert bf.icon_for("BHBC", vehicle("840", "SK67FKV", 3057, "Brighton & Hove Buses", "I Am Electric")) == "liveries/bh-teal-dd.png"


def test_red_and_cream_keeps_the_route_icon_except_on_a_metrobus():
    assert bf.icon_for("BHBC", vehicle("212", "LJ61NXC", 36, "Brighton & Hove")) == ""
    assert bf.icon_for("METR", vehicle("413", "BF12KXS", 36, "Brighton & Hove")) == "BHBC.png"


def test_an_advert_bus_is_found_by_its_branding():
    assert bf.icon_for("BHBC", vehicle("418", "BJ11XHU", branding="Amex")) == "liveries/bh-amex-dd.png"


def test_a_deck_with_no_drawing_gets_no_icon():
    # Coaster is drawn only as a double-decker.
    assert bf.icon_for("BHBC", vehicle("9", "X", 3243, "Coaster", dd=False)) == ""


def test_buses_are_keyed_as_the_feed_names_them_and_ticket_machines_left_out():
    out = bf.build({
        "BHBC": [vehicle("212", "LJ61NXC", 3057, "Brighton & Hove Buses"),
                 vehicle("1000", "", notes="Spare ticket machine")],
        "COMT": [vehicle("4201", "YW25 DFC", 2959, "Compass Travel", dd=False)],
    })
    assert set(out["BHBC"]["vehicles"]) == {"212"}
    assert out["BHBC"]["vehicles"]["212"]["r"] == "LJ61NXC"
    assert out["COMT"]["by"] == "reg"
    assert out["COMT"]["vehicles"]["YW25DFC"] == {"l": "Compass Travel", "t": "ADL Enviro200 MMC", "d": "sd",
                                                 "i": "liveries/compass-sd.png", "f": "4201"}


def test_every_icon_a_livery_can_name_exists():
    named = {p for t in (bf.LIVERY_ICONS, bf.OPERATOR_LIVERY_ICONS, bf.BRANDING_ICONS)
             for decks in t.values() for p in decks.values()}
    missing = sorted(p for p in named if not (ROOT / "icons" / p).exists())
    assert not missing, f"named but not built: {missing}"


def test_every_design_is_drawn_and_built_at_its_kinds_length():
    for key, design in ld.DESIGNS.items():
        assert (ROOT / "icons" / "source" / "liveries" / f"{key}.png").exists(), f"{key}: pattern not drawn"
        served = ROOT / "icons" / "liveries" / f"{key}.png"
        assert served.exists(), f"{key}: icon not built"
        im = Image.open(served).convert("RGBA")
        assert im.size == bi.CANVAS
        body = im.getchannel("A").point(lambda a: 255 if a > 160 else 0).getbbox()
        want = bi.LENGTH_PX[bi.LIVERY_KIND[design["kind"]]]
        assert abs((body[2] - body[0]) - want) <= 3, f"{key}: drawn {body[2] - body[0]}px long, not {want}"


def test_nothing_is_built_that_no_design_draws():
    built = {p.stem for p in (ROOT / "icons" / "liveries").glob("*.png")}
    assert built <= set(ld.DESIGNS), f"stale icons: {sorted(built - set(ld.DESIGNS))}"


def test_the_published_fleet_list_says_where_it_came_from():
    doc = json.loads((ROOT / "data" / "fleet.json").read_text())
    assert doc["source_url"].startswith("https://bustimes.org")
    assert len(doc["checked_on"]) == 10 and doc["credit"]
    assert set(doc["operators"]) == set(bf.OPERATORS)
    for op in doc["operators"].values():
        for k, row in op["vehicles"].items():
            assert k == bf.key(k) and row["d"] in ("dd", "sd", "coach")
