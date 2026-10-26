"""TransportAPI's predictions, kept so they can be scored beside ours.

The board shows TransportAPI's prediction for a departure where it has one
(main._apply_live_overlay) and our own estimate otherwise. Ours are scored
every night from the recorded feeds (scripts/score_served_estimates.py);
theirs leave no record, so nobody can say whether the board would be better
with more of them, fewer, or none. This keeps each fresh prediction we already
fetched (no extra TransportAPI calls: a cached answer is not logged twice) and
sends them in batches to the Worker, which stores them beside the recorder's
snapshots for the night's scoring.

It never holds a request up and never fails one: rows are buffered in memory,
sent in the background, and dropped, with a count, if the Worker cannot take
them. Off unless both PREDICTION_LOG_URL and PREDICTION_LOG_TOKEN are set.
TransportAPI's licence allows storing and processing their data (TransportAPI
Licence, Sch. 4, 2.1.3 and 2.1.4).
"""
import logging
import os
import time
from typing import Optional

log = logging.getLogger("bus_api")

PREDICTION_LOG_URL = os.environ.get("PREDICTION_LOG_URL", "")
PREDICTION_LOG_TOKEN = os.environ.get("PREDICTION_LOG_TOKEN", "")
FLUSH_ROWS = 200
FLUSH_EVERY_SECS = 300
# A Worker down for hours must not grow the free instance's memory without end.
MAX_BUFFERED = 5000

_buffer: list = []
_last_flush = time.monotonic()
stats = {"recorded": 0, "sent": 0, "dropped": 0}


def enabled() -> bool:
    return bool(PREDICTION_LOG_URL and PREDICTION_LOG_TOKEN)


def row(stop_id: str, trip_id: Optional[str], aimed: Optional[str], expected: Optional[str],
        cancelled: bool, fetched_at: int) -> Optional[dict]:
    """One matched prediction, in the shape the Worker accepts."""
    if not (trip_id and aimed and (expected or cancelled)):
        return None
    return {"fetched_at": int(fetched_at), "stop": stop_id, "trip_id": trip_id,
            "aimed": aimed, "expected": expected, "cancelled": bool(cancelled)}


def record(rows: list) -> None:
    if not enabled():
        return
    rows = [r for r in rows if r]
    stats["recorded"] += len(rows)
    _buffer.extend(rows)
    if len(_buffer) > MAX_BUFFERED:
        stats["dropped"] += len(_buffer) - MAX_BUFFERED
        del _buffer[: len(_buffer) - MAX_BUFFERED]


def due(now: Optional[float] = None) -> bool:
    now = time.monotonic() if now is None else now
    return bool(_buffer) and (len(_buffer) >= FLUSH_ROWS or now - _last_flush >= FLUSH_EVERY_SECS)


async def flush(post=None) -> int:
    """Send what is buffered. Returns the rows sent; never raises."""
    global _last_flush
    if not enabled() or not _buffer:
        return 0
    batch = _buffer[:FLUSH_ROWS * 5]
    del _buffer[:len(batch)]
    _last_flush = time.monotonic()
    try:
        if post is None:
            import httpx
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(PREDICTION_LOG_URL, json={"rows": batch},
                                         headers={"Authorization": f"Bearer {PREDICTION_LOG_TOKEN}"})
                ok = resp.status_code == 200
        else:
            ok = await post(batch)
    except Exception as exc:                      # noqa: BLE001 — see the module docstring
        log.warning("Prediction log not sent: %s", exc)
        ok = False
    if ok:
        stats["sent"] += len(batch)
        return len(batch)
    stats["dropped"] += len(batch)
    return 0
