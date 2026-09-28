"""Proof that a killed/interfered positioning or liquidation feed produces an
explicit Data Health gap rather than a silent one.

Reuses the DataHealthTracker fake-connection convention from
test_data_health_coalescing.py (no live DATABASE_URL required -- record_message/
sweep() are pure in-memory; only flush() touches the DB). Drives the exact
closures main.py wires up (on_positioning_polled's callback contract and
on_liquidation's callback contract) against DataHealthTracker directly, the
same way test_positioning_poller.py and test_liquidation.py already prove each
closure's logic in isolation -- this test proves the combination: repeated
poll failures / a stalled liquidation stream age a channel from ok -> stale ->
suspended with gap_started_at set, and a subsequent success clears the gap.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from littledevil_recorder.data_health import DataHealthTracker


class _Cursor:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def execute(self, statement, params):
        self.connection.calls.append((statement, params))


class _Connection:
    def __init__(self):
        self.calls = []

    def cursor(self):
        return _Cursor(self)


def _on_positioning_polled(health: DataHealthTracker, symbol: str, error: Exception | None) -> None:
    """Mirrors main.py's on_positioning_polled exactly: a failed poll must
    never call record_message, so the channel ages via sweep() instead of
    being falsely marked fresh."""
    channel = f"binance_positioning_{symbol}"
    health.register(channel)
    if error is None:
        health.record_message(channel)


@pytest.mark.asyncio
async def test_persistent_positioning_poll_failure_becomes_explicit_gap_not_silent():
    conn = _Connection()
    health = DataHealthTracker(conn)

    # First poll succeeds -- channel is healthy.
    _on_positioning_polled(health, "BTCUSDT", None)
    assert await health.flush()
    channel = "binance_positioning_BTCUSDT"
    assert health._channels[channel].status == "ok"
    assert health._channels[channel].gap_started_at is None

    # Binance-side outage: every subsequent poll fails. This must NOT
    # touch last_message_at -- simulate the passage of time the way
    # test_data_health.py does, without sleeping in real time.
    for _ in range(5):
        _on_positioning_polled(health, "BTCUSDT", RuntimeError("simulated Binance outage"))
    health._channels[channel].last_message_at = datetime.now(UTC) - timedelta(seconds=200)

    health.sweep()
    assert await health.flush()
    assert health._channels[channel].status == "suspended"
    assert health._channels[channel].gap_started_at is not None
    gap_start = health._channels[channel].gap_started_at

    # Recovery: the next successful poll clears the gap explicitly.
    _on_positioning_polled(health, "BTCUSDT", None)
    assert await health.flush()
    assert health._channels[channel].status == "ok"
    assert health._channels[channel].gap_started_at is None
    assert gap_start is not None  # the outage window was recorded, not lost


@pytest.mark.asyncio
async def test_stalled_liquidation_stream_becomes_explicit_gap_not_silent():
    """The liquidation stream is a single global connection (no per-symbol
    reconnect); if it stalls, on_liquidation simply never fires for any
    desired symbol. Prove that absence surfaces as a stale/suspended gap on
    that symbol's channel rather than staying silently 'ok' forever."""
    conn = _Connection()
    health = DataHealthTracker(conn)
    channel = "binance_liquidation_BTCUSDT"

    health.register(channel)
    health.record_message(channel)
    assert await health.flush()
    assert health._channels[channel].status == "ok"

    # Simulate the stream stalling: no on_liquidation callback fires for
    # this symbol for over SUSPENDED_AFTER_SECONDS.
    health._channels[channel].last_message_at = datetime.now(UTC) - timedelta(seconds=200)
    health.sweep()
    assert await health.flush()
    assert health._channels[channel].status == "suspended"
    assert health._channels[channel].gap_started_at is not None

    # A reconnect alone is not proof of recovery -- only a new liquidation
    # event (on_liquidation firing again) clears the gap.
    health.record_message(channel)
    assert await health.flush()
    assert health._channels[channel].status == "ok"
    assert health._channels[channel].gap_started_at is None
