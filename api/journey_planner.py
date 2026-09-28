"""Journey plans from the Buses & Trains API, which runs OpenTripPlanner.

A preview (the page shows it only with ?preview=1). Door-to-door plans need a
routing engine, walking legs and a street network, which this project does not
host: OpenTripPlanner wants gigabytes the free Render instance does not have.
Buses & Trains (busesandtrains.co.uk) runs one, with a free tier of 300
requests a day, so the planner is tried against theirs before anyone decides
to host one.

The key is `BAT_API_KEY`, set in Render and sent as a header, never in a URL
a log might keep. Every call counts against a daily cap kept below the free
tier and persisted across restarts, the same way the TransportAPI quota is,
and the same question within five minutes is answered from the cache.
"""

import json
import os
from datetime import date
from pathlib import Path

BAT_API_KEY = os.getenv("BAT_API_KEY", "")
BAT_BASE_URL = os.getenv("BAT_BASE_URL", "https://api.busesandtrains.co.uk").rstrip("/")
# Below the free tier's 300, so a burst of retries at the end of a busy day
# still leaves room, and the upstream never has to refuse us.
BAT_DAILY_LIMIT = int(os.getenv("BAT_DAILY_LIMIT", "250"))
PLAN_CACHE_TTL = 300

LEG_FIELDS = ("mode", "from_name", "to_name", "from_stop_id", "to_stop_id",
              "departure_time", "arrival_time", "duration_seconds", "route",
              "agency", "num_stops", "geometry")


class DailyQuota:
    """A per-day call counter written to disk, so a restart does not reset it."""

    def __init__(self, path: Path, limit: int):
        self.path, self.limit = path, limit
        self.state = self._load()

    def _load(self) -> dict:
        today = date.today().isoformat()
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if data.get("date") == today:
                return {"date": today, "count": int(data.get("count", 0))}
        except Exception:
            pass
        return {"date": today, "count": 0}

    def _rollover(self) -> None:
        today = date.today().isoformat()
        if self.state["date"] != today:
            self.state = {"date": today, "count": 0}

    def remaining(self) -> int:
        self._rollover()
        return max(0, self.limit - self.state["count"])

    def bump(self, delta: int = 1) -> None:
        self._rollover()
        self.state["count"] = max(0, self.state["count"] + delta)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(self.state), encoding="utf-8")
            os.replace(tmp, self.path)
        except Exception:
            pass


def normalise(payload: dict) -> dict:
    """The upstream answer, trimmed to what the page draws."""
    options = []
    for opt in (payload or {}).get("options") or []:
        legs = [{k: leg.get(k) for k in LEG_FIELDS} for leg in opt.get("legs") or []]
        options.append({
            "departure_time": opt.get("departure_time"),
            "arrival_time": opt.get("arrival_time"),
            "duration_seconds": opt.get("duration_seconds"),
            "num_transfers": opt.get("num_transfers"),
            "legs": legs,
        })
    return {"origin": (payload or {}).get("origin"),
            "destination": (payload or {}).get("destination"),
            "options": options}
