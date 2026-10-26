"""api/prediction_log.py: TransportAPI's predictions, kept to be scored.

It must stay off without its settings, keep only what can be scored, never
fail a request, and never grow without end when the Worker is away.
"""

import asyncio
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api import prediction_log as pl                              # noqa: E402


@pytest.fixture
def on(monkeypatch):
    monkeypatch.setattr(pl, "PREDICTION_LOG_URL", "https://example.org/prediction-log")
    monkeypatch.setattr(pl, "PREDICTION_LOG_TOKEN", "t")
    monkeypatch.setattr(pl, "_buffer", [])
    monkeypatch.setattr(pl, "stats", {"recorded": 0, "sent": 0, "dropped": 0})


def a_row(i=0):
    return pl.row("4400A", f"T{i}", "2026-10-26T12:00:00+00:00", "2026-10-26T12:02:00+00:00", False, 1792800000)


def test_off_without_its_settings(monkeypatch):
    monkeypatch.setattr(pl, "PREDICTION_LOG_URL", "")
    monkeypatch.setattr(pl, "_buffer", [])
    pl.record([a_row()])
    assert pl._buffer == [], "rows were kept with nowhere to send them"


def test_only_what_can_be_scored_is_kept():
    assert pl.row("A", None, "x", "y", False, 1) is None, "a row with no journey cannot be scored"
    assert pl.row("A", "T", "x", None, False, 1) is None
    assert pl.row("A", "T", "x", None, True, 1)["cancelled"] is True


def test_rows_go_in_batches_and_a_failure_is_counted_not_raised(on):
    pl.record([a_row(i) for i in range(3)])
    assert not pl.due(now=pl._last_flush + 1), "three rows were sent at once"
    assert pl.due(now=pl._last_flush + pl.FLUSH_EVERY_SECS)
    sent = []

    async def ok(batch):
        sent.extend(batch)
        return True
    assert asyncio.run(pl.flush(post=ok)) == 3 and len(sent) == 3 and pl._buffer == []

    pl.record([a_row()])

    async def boom(_batch):
        raise RuntimeError("Worker away")
    assert asyncio.run(pl.flush(post=boom)) == 0
    assert pl.stats["dropped"] == 1


def test_the_buffer_has_a_ceiling(on, monkeypatch):
    monkeypatch.setattr(pl, "MAX_BUFFERED", 5)
    pl.record([a_row(i) for i in range(8)])
    assert len(pl._buffer) == 5 and pl.stats["dropped"] == 3
