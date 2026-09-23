"""Read independent recorded feeds and preserve report-level provenance."""
import hashlib
import struct
import xml.etree.ElementTree as ET
from datetime import datetime, time, timezone, timedelta

from api import gtfs_rt, trip_match
from observation_contract import LONDON, service_origin


def recorded_stream(tt, day, xml_files, rt_files, health, manifest, siri_parser, stale_secs=600):
    def with_epoch(files):
        return {secs if secs >= 1_000_000_000 else int(datetime.combine(
            day, time(secs // 3600, secs % 3600 // 60), LONDON).timestamp()): path for secs, path in files}
    xml, rt = with_epoch(xml_files), with_epoch(rt_files)
    for name in ("siri", "gtfs_rt"):
        health[name] = {"expected_objects": 0, "objects_present": 0, "parsed_snapshots": 0, "parse_errors": 0,
                        "reported_vehicles": 0, "fresh_vehicles": 0,
                        "hours": {f"{h:02d}": {"expected": 0, "fetched": 0, "parsed": 0, "fresh_reports": 0,
                                                "matched_reports": 0, "measured_calls": 0} for h in range(24)}}
    # Count actual UTC minutes in a London date: 23/25-hour DST days are not 24-hour days.
    begin = int(datetime.combine(day, time(), LONDON).timestamp())
    end = int(datetime.combine(day + timedelta(days=1), time(), LONDON).timestamp())
    for stamp in range(begin, end, 60):
        local = datetime.fromtimestamp(stamp, LONDON)
        for feed in ("siri", "gtfs_rt"):
            minute = local.hour * 60 + local.minute
            if feed == "gtfs_rt" or minute < 30 or minute >= 300:
                health[feed]["expected_objects"] += 1
                health[feed]["hours"][local.strftime("%H")]["expected"] += 1
    for secs in sorted(set(xml) | set(rt)):
        at = datetime.fromtimestamp(secs, LONDON)
        at_epoch = int(at.timestamp())
        vehicles, parsed, rt_vehicles = [], False, []
        for feed, path in (("gtfs_rt", rt.get(secs)), ("siri", xml.get(secs))):
            if path is None:
                continue
            raw = path.read_bytes()
            digest = hashlib.sha256(raw).hexdigest()
            object_name = f'{"rt" if feed == "gtfs_rt" else "raw"}/{day.isoformat()}/{path.name}'
            manifest.append({"object": object_name, "sha256": digest, "bytes": len(raw)})
            h = health[feed]
            h["objects_present"] += 1
            h["hours"][at.strftime("%H")]["fetched"] += 1
            try:
                if feed == "gtfs_rt":
                    header, reports = gtfs_rt.parse_feed(raw)
                    if not header.get("version"):
                        raise ValueError("GTFS-RT has no header")
                    fresh = []
                    for report in reports:
                        stamp = report.get("timestamp")
                        if stamp is None or not -60 <= at_epoch - stamp <= stale_secs:
                            continue
                        trip = tt.trips.get(report.get("trip_id", ""), {})
                        route_id = trip.get("route_id", report.get("route_id", ""))
                        route = tt.routes.get(route_id, {})
                        fresh.append({**report, "vehicle_ref": report.get("vehicle_id", ""),
                                      "service_ref": route.get("short_name", ""),
                                      "operator_ref": tt.noc_for_route(route_id),
                                      "destination": trip.get("headsign", ""),
                                      "recorded_epoch": stamp,
                                      "recorded_secs": stamp - service_origin(day)})
                    rt_vehicles = fresh
                else:
                    tree = ET.fromstring(raw)
                    reports = tree.findall(".//{http://www.siri.org.uk/siri}VehicleActivity")
                    fresh = siri_parser(raw, at.astimezone(timezone.utc))
                h["reported_vehicles"] += len(reports)
                h["parsed_snapshots"] += 1
                h["fresh_vehicles"] += len(fresh)
                hour = at.strftime("%H")
                h["hours"][hour]["parsed"] += 1
                h["hours"][hour]["fresh_reports"] += len(fresh)
                parsed = True
                for n, v in enumerate(fresh):
                    if v.get("recorded_epoch") is not None:
                        v["recorded_secs"] = v["recorded_epoch"] - service_origin(day)
                    v["source_report"] = {"object": object_name, "sha256": digest, "feed": feed, "fetched_epoch": at_epoch,
                                          "entity": v.get("id") or v.get("vehicle_ref") or str(n),
                                          "reported_epoch": v.get("recorded_epoch")}
                    if feed == "siri" and v.get("recorded_epoch") is not None:
                        compatible = [r for r in rt_vehicles
                                      if r.get("vehicle_ref") == v.get("vehicle_ref") and r.get("trip_id")
                                      and abs(r["recorded_epoch"] - v["recorded_epoch"]) <= 30
                                      and trip_match.km(r["latitude"], r["longitude"],
                                                        v["latitude"], v["longitude"]) <= .05]
                        if len({(r["trip_id"], r.get("start_date")) for r in compatible}) == 1:
                            r = compatible[0]
                            v.update(trip_id=r["trip_id"], start_date=r.get("start_date", ""), start_time=r.get("start_time", ""),
                                     schedule_relationship=r.get("schedule_relationship", 0), identity_source=r["source_report"])
                    vehicles.append(v)
            except (ET.ParseError, ValueError, IndexError, struct.error, OverflowError):
                h["parse_errors"] += 1
        if parsed:
            yield at_epoch - service_origin(day), vehicles
