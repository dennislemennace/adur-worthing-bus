#!/usr/bin/env python3
"""Write ward-delays.json: how much time buses lose on the road in each ward.

Route view's Areas card says how many buses a ward gets (data/localities.json);
this says what happens to them on its roads. Built nightly from the delay map
(scripts/build_delay_hotspots.py writes hotspot-map/*.json, one per route) and
published beside it, so the card reads it through the journey-times index.
Asked for on 6 November 2026 (Areas, phase B).

Each stretch of road between two timing stops is placed in the ward its
midpoint falls in. A stretch counts only where the delay map itself colours it:
a cell for the time of day on the latest timetable, past the map's own floor
(journeys and distinct days). Below that the ward says "too few", never a
figure. For each ward and time of day: how many stretches count, the median of
their median time lost, and the stretch that loses most.

Weekday daytime (10:00 to 16:00) and the evening peak (16:00 to 18:00).

    python scripts/build_ward_delays.py --map-dir hotspot-map
        [--localities data/localities.json] [--out ward-delays.json]
"""

import argparse
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from build_evidence import in_rings  # noqa: E402

SLOTS = (("weekday", "10-16"), ("weekday", "16-18"))
DEFAULT_FLOOR = {"journeys": 30, "distinct_days": 5}
METHOD = ("Each stretch between two timing stops is placed in the ward its midpoint falls in, and counts where "
          "the delay map colours it: a cell on the latest timetable with at least the map's floor of journeys "
          "and distinct days. Per ward and time of day, the median of those stretches' median time lost, and "
          "the stretch that loses most.")
CAVEATS = [
    "Time lost is against the timetable between two timing stops, from the buses' own positions, not a "
    "stopwatch.",
    "A stretch that crosses a ward boundary is counted in the ward its midpoint falls in.",
    "A ward with no stretch past the floor says too few, not no delay.",
]


def midpoint(geometry):
    """The middle point of a stretch's [lat, lon] line, by vertex count."""
    if not geometry:
        return None
    return geometry[len(geometry) // 2]


def ward_for(point, wards):
    if not point:
        return None
    lat, lon = point
    for w in wards:
        if in_rings(w["_xy"], lon, lat):
            return w["code"]
    return None


def counted_cell(doc, stretch_id, day_type, period, floor):
    """The cell the delay map would colour this stretch with, or None."""
    good = [c for c in doc.get("cells") or []
            if c.get("stretch") == stretch_id and c.get("latest") is not False
            and c.get("day_type") == day_type and c.get("resolution") == "period" and c.get("period") == period
            and (c.get("journeys") or 0) >= floor["journeys"]
            and (c.get("distinct_days") or 0) >= floor["distinct_days"]
            and c.get("median_gained_secs") is not None]
    return max(good, key=lambda c: c.get("journeys") or 0) if good else None


def build(docs, localities, as_of=None):
    # The wards' rings as [lon, lat], once, for the even-odd test.
    wards = [{**w, "_xy": [[[x, y] for y, x in ring] for ring in w["rings"]]}
             for w in localities.get("wards") or []]
    found = {w["code"]: {f"{d}:{p}": [] for d, p in SLOTS} for w in wards}
    days = set()
    for doc in docs:
        floor = doc.get("floor") or DEFAULT_FLOOR
        days.update(doc.get("days_collected") or [])
        for st in doc.get("stretches") or []:
            code = ward_for(midpoint(st.get("geometry")), wards)
            if code is None:
                continue
            for day_type, period in SLOTS:
                cell = counted_cell(doc, st["id"], day_type, period, floor)
                if cell:
                    found[code][f"{day_type}:{period}"].append({
                        "service": doc.get("service", ""), "operator": doc.get("operator", ""),
                        "from": st.get("from_name", ""), "to": st.get("to_name", ""),
                        "lost_secs": round(cell["median_gained_secs"]), "journeys": cell.get("journeys")})
    out = {}
    for code, slots in found.items():
        out[code] = {}
        for key, rows in slots.items():
            if not rows:
                out[code][key] = {"stretches": 0}
                continue
            out[code][key] = {"stretches": len(rows),
                              "median_lost_secs": round(statistics.median(r["lost_secs"] for r in rows)),
                              "worst": max(rows, key=lambda r: r["lost_secs"])}
    ordered = sorted(days)
    return {
        "as_of": as_of or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window": {"from": ordered[0] if ordered else None, "to": ordered[-1] if ordered else None,
                   "days": len(ordered)},
        "slots": [{"day_type": d, "period": p} for d, p in SLOTS],
        "method": METHOD,
        "caveats": CAVEATS,
        "wards": out,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--map-dir", required=True)
    ap.add_argument("--localities", default=str(ROOT / "data" / "localities.json"))
    ap.add_argument("--out", default="ward-delays.json")
    args = ap.parse_args()
    docs = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(Path(args.map_dir).glob("*.json"))]
    localities = json.loads(Path(args.localities).read_text(encoding="utf-8"))
    result = build(docs, localities)
    Path(args.out).write_text(json.dumps(result, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    counted = sum(1 for w in result["wards"].values() for s in w.values() if s["stretches"])
    print(f"Wrote {args.out}: {len(docs)} route maps, {counted} ward and time slots with a figure")
    return 0


if __name__ == "__main__":
    sys.exit(main())
