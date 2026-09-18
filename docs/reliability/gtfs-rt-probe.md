# Does GTFS-RT say which journey a bus is running?

Probed `sample.pb` on 2026-09-18, against 30,339 trips in the timetable.

## The answer

**184 of 256 vehicles carrying a trip id (72%) name a journey we hold.** SIRI-VM's own journey reference matched 0 of 256, which is why every journey is currently inferred from position and time.

## What the feed carries

| field | populated |
|---|---|
| `trip_id` | 256 of 259 (99%) |
| `route_id` | 256 of 259 (99%) |
| `start_date` | 62 of 259 (24%) |
| `start_time` | 62 of 259 (24%) |
| `current_stop_sequence` | 0 of 259 (0%) |
| `current_status` | 0 of 259 (0%) |
| `stop_id` | 0 of 259 (0%) |
| `vehicle_id` | 259 of 259 (100%) |
| `bearing` | 259 of 259 (100%) |
| `timestamp` | 259 of 259 (100%) |

## Is the decoder reading the right fields?

`scripts/gtfs_rt.py` has no protobuf dependency, so its field numbers are checked against reality: 259 of 259 decoded positions fall inside the recorded bounding box (lat 50.807–50.870, lon -0.420–-0.100).

Against the SIRI-VM snapshot of the same minute: 259 of 259 vehicles appear in both feeds and their positions agree to a median of 0 m. 259 of 259 carry an identical timestamp, so the two feeds are the same data in two formats.

## What does not match, and why

The journeys we cannot place belong to these services: 5A (11), 20 (7), 27 (7), 24 (6), 25 (5), 22 (4), 26 (4), 48 (4), 28 (4), 14 (3). Those are city routes our timetable build filters out — it keeps routes touching West Sussex plus an allowlist — so the gap is in what we hold, not in what the feed publishes.

## Staleness

Reports are a median 19 s old against the feed's own header, 90th percentile 4932 s, worst 85579 s — the feed repeats a vehicle's last known position long after it has finished, which is why the processor drops anything over ten minutes stale.
