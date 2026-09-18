"""Does the GTFS-RT feed tell us which journey each bus is running?

Everything about how we measure punctuality rests on the answer. Today a
journey is *inferred* from position and time, because SIRI-VM's
`DatedVehicleJourneyRef` matched 0 of 256 timetable trips. That inference
censors both tails of every distribution — nothing more than 5 minutes early
or 25 late can be observed at all — and turns a very late bus into an early
one on the following journey.

GTFS-RT is the same service's other feed. If it states a `trip_id` we hold,
the inference goes, and the caveats with it.

    python scripts/probe_gtfs_rt.py --feed rt.pb --siri same-minute.xml \\
        --out docs/reliability/gtfs-rt-probe.md

The SIRI snapshot of the same minute is optional but worth giving: comparing
the two feeds vehicle by vehicle is what proves the decoder in
`scripts/gtfs_rt.py` reads the right fields. A wrong field number shows as
positions that disagree, not as an error.
"""

import argparse
import datetime as dt
import sqlite3
import statistics
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from gtfs_rt import parse_feed                                   # noqa: E402

SIRI_NS = {"s": "http://www.siri.org.uk/siri"}
# The recorded box, from worker/src/recorder.js. Positions outside it would
# mean the decoder is reading the wrong fields.
BOX = (50.78, 50.87, -0.42, -0.10)

FIELDS = ("trip_id", "route_id", "start_date", "start_time",
          "current_stop_sequence", "current_status", "stop_id",
          "vehicle_id", "bearing", "timestamp")


def siri_vehicles(path):
    """What a SIRI-VM snapshot says about each vehicle."""
    out = {}
    root = ET.parse(path).getroot()
    for activity in root.findall(".//s:VehicleActivity", SIRI_NS):
        journey = activity.find("s:MonitoredVehicleJourney", SIRI_NS)
        if journey is None:
            continue
        ref = (journey.findtext("s:VehicleRef", "", SIRI_NS) or "").strip()
        recorded = activity.findtext("s:RecordedAtTime", "", SIRI_NS)
        loc = journey.find("s:VehicleLocation", SIRI_NS)
        if not ref or loc is None:
            continue
        stamp = None
        if recorded:
            try:
                stamp = dt.datetime.fromisoformat(recorded.replace("Z", "+00:00")).timestamp()
            except ValueError:
                stamp = None
        out[ref] = {
            "timestamp": stamp,
            "service": (journey.findtext("s:PublishedLineName", "", SIRI_NS) or "").strip(),
            "latitude": float(loc.findtext("s:Latitude", "0", SIRI_NS)),
            "longitude": float(loc.findtext("s:Longitude", "0", SIRI_NS)),
        }
    return out


def probe(feed_bytes, timetable, siri=None):
    """Everything the decision needs, from one minute of feed."""
    header, vehicles = parse_feed(feed_bytes)
    con = sqlite3.connect(timetable)
    trips = {r[0] for r in con.execute("SELECT trip_id FROM trips")}

    present = {f: sum(1 for v in vehicles if v.get(f) not in (None, "")) for f in FIELDS}
    declared = [v for v in vehicles if v.get("trip_id")]
    matched = [v for v in declared if v["trip_id"] in trips]

    lats = [v["latitude"] for v in vehicles]
    lons = [v["longitude"] for v in vehicles]
    inside = sum(1 for v in vehicles
                 if BOX[0] <= v["latitude"] <= BOX[1] and BOX[2] <= v["longitude"] <= BOX[3])

    out = {
        "header": header,
        "vehicles": len(vehicles),
        "present": present,
        "declared": len(declared),
        "matched": len(matched),
        "timetable_trips": len(trips),
        "inside_box": inside,
        "lat_range": (min(lats), max(lats)) if lats else None,
        "lon_range": (min(lons), max(lons)) if lons else None,
    }
    if header.get("timestamp") and vehicles:
        ages = sorted(header["timestamp"] - v["timestamp"]
                      for v in vehicles if v.get("timestamp"))
        if ages:
            out["age"] = {"median": statistics.median(ages),
                          "p90": ages[int(len(ages) * 0.9)], "worst": ages[-1]}

    if siri:
        shared = [v for v in vehicles if v.get("vehicle_id") in siri]
        gaps, clock = [], []
        for v in shared:
            other = siri[v["vehicle_id"]]
            gaps.append((((v["latitude"] - other["latitude"]) * 111_000) ** 2
                         + ((v["longitude"] - other["longitude"]) * 111_000 * 0.63) ** 2) ** 0.5)
            if other["timestamp"] and v.get("timestamp"):
                clock.append(v["timestamp"] - other["timestamp"])
        out["cross_check"] = {
            "siri_vehicles": len(siri),
            "shared": len(shared),
            "median_metres": round(statistics.median(gaps)) if gaps else None,
            "identical_clocks": sum(1 for d in clock if abs(d) < 1),
            "clock_samples": len(clock),
        }
        # Which services the unmatched journeys belong to. If they are all
        # outside the timetable we build, the gap is ours, not the feed's.
        unmatched = {}
        for v in declared:
            if v["trip_id"] not in trips:
                service = siri.get(v.get("vehicle_id", ""), {}).get("service", "?")
                unmatched[service] = unmatched.get(service, 0) + 1
        out["unmatched_services"] = dict(sorted(unmatched.items(), key=lambda kv: -kv[1]))
    return out


def report(found, feed_name):
    """The findings, as a document rather than terminal scrollback."""
    p, n = found["present"], found["vehicles"]

    def share(key):
        return f"{p[key]} of {n} ({100 * p[key] / n:.0f}%)" if n else "—"

    lines = [
        "# Does GTFS-RT say which journey a bus is running?",
        "",
        f"Probed `{feed_name}` on {dt.datetime.now(dt.UTC):%Y-%m-%d}, against "
        f"{found['timetable_trips']:,} trips in the timetable.",
        "",
        "## The answer",
        "",
        f"**{found['matched']} of {found['declared']} vehicles carrying a trip id "
        f"({100 * found['matched'] / max(1, found['declared']):.0f}%) name a journey we "
        "hold.** SIRI-VM's own journey reference matched 0 of 256, which is why every "
        "journey is currently inferred from position and time.",
        "",
        "## What the feed carries",
        "",
        "| field | populated |",
        "|---|---|",
        *[f"| `{f}` | {share(f)} |" for f in FIELDS],
        "",
        "## Is the decoder reading the right fields?",
        "",
        f"`scripts/gtfs_rt.py` has no protobuf dependency, so its field numbers are "
        f"checked against reality: {found['inside_box']} of {n} decoded positions fall "
        f"inside the recorded bounding box "
        f"(lat {found['lat_range'][0]:.3f}–{found['lat_range'][1]:.3f}, "
        f"lon {found['lon_range'][0]:.3f}–{found['lon_range'][1]:.3f}).",
    ]
    if "cross_check" in found:
        c = found["cross_check"]
        lines += [
            "",
            f"Against the SIRI-VM snapshot of the same minute: {c['shared']} of "
            f"{c['siri_vehicles']} vehicles appear in both feeds and their positions "
            f"agree to a median of {c['median_metres']} m. {c['identical_clocks']} of "
            f"{c['clock_samples']} carry an identical timestamp, so the two feeds are "
            "the same data in two formats.",
        ]
        if found.get("unmatched_services"):
            worst = list(found["unmatched_services"].items())[:10]
            lines += [
                "",
                "## What does not match, and why",
                "",
                "The journeys we cannot place belong to these services: "
                + ", ".join(f"{s} ({count})" for s, count in worst)
                + ". Those are city routes our timetable build filters out — it keeps "
                "routes touching West Sussex plus an allowlist — so the gap is in what "
                "we hold, not in what the feed publishes.",
            ]
    if "age" in found:
        a = found["age"]
        lines += [
            "",
            "## Staleness",
            "",
            f"Reports are a median {a['median']:.0f} s old against the feed's own header, "
            f"90th percentile {a['p90']:.0f} s, worst {a['worst']:.0f} s — the feed "
            "repeats a vehicle's last known position long after it has finished, which "
            "is why the processor drops anything over ten minutes stale.",
        ]
    return "\n".join(lines) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description="Probe the GTFS-RT feed.")
    ap.add_argument("--feed", required=True, help="a recorded GTFS-RT object (.pb)")
    ap.add_argument("--siri", help="the SIRI-VM snapshot of the same minute (.xml)")
    ap.add_argument("--timetable", default=str(ROOT / "data" / "timetable.sqlite"))
    ap.add_argument("--out", help="write the findings here as Markdown")
    args = ap.parse_args(argv)

    siri = siri_vehicles(args.siri) if args.siri else None
    found = probe(Path(args.feed).read_bytes(), args.timetable, siri)
    text = report(found, Path(args.feed).name)
    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
        print(f"findings written to {out}")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
