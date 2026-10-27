#!/usr/bin/env python3
"""Work out the figures quoted from West Sussex's reply to FOI/13765.

    python scripts/build_foi_figures.py           # rewrite `figures` in the data file
    python scripts/build_foi_figures.py --check   # exit 1 if the stored figures are stale

The inputs in data/foi_wscc_13765.json are typed in from a one-off reply, so
nothing here runs on a schedule. What it guards is that every number the
Network Updates article quotes is worked out the same way from the same
inputs, and that tests/test_curated_data.py can check the article against it
(.claude/skills/evidence-provenance).
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "foi_wscc_13765.json"


def pct(part, whole):
    return round(100 * part / whole, 1)


def compute(inputs):
    pop = inputs["population"]["values"]
    rtpi = inputs["rtpi_by_district"]["values"]
    routes = inputs["route_support"]["routes"]
    residents = sum(pop.values())

    rtpi_total = round(sum(rtpi.values()), 2)
    per_resident = {d: round(rtpi[d] / pop[d], 2) for d in sorted(pop)}

    route_total = round(sum(r["total"] for r in routes), 2)
    crossing = [r for r in routes if r["cross_boundary"]]
    cross_total = round(sum(r["total"] for r in crossing), 2)
    n700 = next(r for r in routes if r["service"] == "N700")

    capital_total = round(sum(inputs["capital"]["values"].values()), 2)
    reply_total = round(route_total + rtpi_total + capital_total, 2)
    awards_total = round(sum(inputs["awards"]["values"].values()), 2)

    return {
        "rtpi_total": rtpi_total,
        "rtpi_per_resident": per_resident,
        "rtpi_per_resident_county": round(rtpi_total / residents, 2),
        "adur_share_of_residents_pct": pct(pop["Adur"], residents),
        "adur_share_of_rtpi_pct": pct(rtpi["Adur"], rtpi_total),
        "adur_rtpi_per_resident_as_pct_of_worthing": round(
            100 * (rtpi["Adur"] / pop["Adur"]) / (rtpi["Worthing"] / pop["Worthing"])),
        "route_support_entries": len(routes),
        "route_support_total": route_total,
        "cross_boundary_routes": [r["service"] for r in crossing],
        "cross_boundary_total": cross_total,
        "cross_boundary_pct": pct(cross_total, route_total),
        "cross_boundary_serving_adur": [r["service"] for r in crossing if "Adur" in r["districts"]],
        "n700_total": n700["total"],
        "capital_total": capital_total,
        "reply_total": reply_total,
        "awards_total": awards_total,
        "unaccounted": round(awards_total - reply_total, 2),
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="fail if the stored figures are stale")
    args = ap.parse_args(argv)
    doc = json.loads(DATA.read_text(encoding="utf-8"))
    figures = compute(doc["inputs"])
    if args.check:
        if doc.get("figures") != figures:
            print(f"{DATA.name}: stored figures are stale; run without --check")
            return 1
        print(f"{DATA.name}: figures match their inputs")
        return 0
    doc["figures"] = figures
    DATA.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote figures to {DATA.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
