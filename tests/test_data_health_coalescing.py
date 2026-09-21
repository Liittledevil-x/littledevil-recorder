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
        if self.connection.fail:
            raise RuntimeError("database unavailable")


class _Connection:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def cursor(self):
        return _Cursor(self)


@pytest.mark.asyncio
async def test_high_rate_events_coalesce_to_one_bounded_batch_write():
    conn = _Connection()
    tracker = DataHealthTracker(conn)
    for _ in range(20_000):
        tracker.record_message("binance_trades_BTCUSDT")
        tracker.record_message("binance_depth_BTCUSDT")

    assert tracker.pending_channels == 2
    assert await tracker.flush()
    assert len(conn.calls) == 1
    # One SQL statement contains exactly one snapshot per channel, not a
    # statement per market event (20,000 * 2 above).
    assert len(conn.calls[0][1]) == 8
    assert tracker.pending_channels == 0
    assert await tracker.flush()
    assert len(conn.calls) == 1


@pytest.mark.asyncio
async def test_database_failure_does_not_escape_or_discard_health_state():
    conn = _Connection(fail=True)
    tracker = DataHealthTracker(conn)
    tracker.record_message("binance_trades_ETHUSDT")
    assert not await tracker.flush()
    assert tracker.pending_channels == 1
    assert tracker.flush_failures == 1

    conn.fail = False
    assert await tracker.flush()
    assert tracker.pending_channels == 0
    assert len(conn.calls) == 2


@pytest.mark.asyncio
async def test_sweep_preserves_stale_and_recovery_semantics_in_batched_snapshot():
    conn = _Connection()
    tracker = DataHealthTracker(conn)
    tracker.record_message("binance_depth_ETHUSDT")
    tracker._channels["binance_depth_ETHUSDT"].last_message_at = datetime.now(UTC) - timedelta(seconds=121)
    tracker.sweep()
    assert tracker._channels["binance_depth_ETHUSDT"].status == "suspended"
    assert await tracker.flush()
    assert conn.calls[-1][1][-1] == "suspended"

    tracker.record_message("binance_depth_ETHUSDT")
    assert await tracker.flush()
    assert conn.calls[-1][1][-1] == "ok"
