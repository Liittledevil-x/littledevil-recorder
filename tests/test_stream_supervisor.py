"""Exercises _StreamSupervisor's runtime add/remove reconciliation directly,
without any real websocket: run_aggtrade_stream/run_depth_stream are
replaced with fakes that record the symbol list they were started with and
block until their own stop_event fires, exactly like the real streams do.
"""

from __future__ import annotations

import asyncio

import pytest

from littledevil_recorder import main as main_module
from littledevil_recorder.data_health import DataHealthTracker
from littledevil_recorder.subscription_manager import SubscriptionManager


class _FakeHealthConn:
    async def cursor(self):
        raise AssertionError("not used in these tests")

    async def close(self):
        pass


class _FakeHealth:
    """Minimal stand-in exposing only the methods _StreamSupervisor calls."""

    def __init__(self) -> None:
        self.registered: list[str] = []

    def register(self, channel: str) -> None:
        self.registered.append(channel)


@pytest.fixture
def fake_streams(monkeypatch):
    """Patches both stream entry points main.py imports at module scope.
    Each fake call is recorded (channel implied by which fake ran, symbols,
    and a handle to the asyncio.Event so the test can also confirm it was
    actually cancelled)."""
    calls: list[dict] = []

    async def fake_aggtrade(symbols, on_trade, *, stop_event=None):
        call = {"channel": "trades", "symbols": list(symbols), "stop_event": stop_event, "cancelled": False}
        calls.append(call)
        try:
            await stop_event.wait()
        except asyncio.CancelledError:
            call["cancelled"] = True
            raise

    async def fake_depth(symbols, on_event, *, stop_event=None):
        call = {"channel": "depth", "symbols": list(symbols), "stop_event": stop_event, "cancelled": False}
        calls.append(call)
        try:
            await stop_event.wait()
        except asyncio.CancelledError:
            call["cancelled"] = True
            raise

    monkeypatch.setattr(main_module, "run_aggtrade_stream", fake_aggtrade)
    monkeypatch.setattr(main_module, "run_depth_stream", fake_depth)
    return calls


async def _noop_on_trade(trade: dict) -> None:
    pass


async def _noop_on_depth(symbol: str, book, event: dict) -> None:
    pass


def _supervisor(tmp_path, stop_event):
    subscriptions = SubscriptionManager(tmp_path)
    health = _FakeHealth()
    supervisor = main_module._StreamSupervisor(
        subscriptions=subscriptions,
        health=health,
        on_trade=_noop_on_trade,
        on_depth_event=_noop_on_depth,
        stop_event=stop_event,
    )
    return subscriptions, health, supervisor


@pytest.mark.asyncio
async def test_reconcile_starts_stream_for_newly_added_symbols(tmp_path, fake_streams):
    stop_event = asyncio.Event()
    subscriptions, health, supervisor = _supervisor(tmp_path, stop_event)
    subscriptions.add_symbols("trades", ["BTCUSDT", "ETHUSDT"])

    await supervisor.reconcile()
    await asyncio.sleep(0)  # let the task actually start and hit stop_event.wait()

    assert len(fake_streams) == 1
    assert fake_streams[0]["channel"] == "trades"
    assert fake_streams[0]["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert supervisor._running["trades"] == ["BTCUSDT", "ETHUSDT"]
    assert "binance_trades_BTCUSDT" in health.registered
    assert "binance_trades_ETHUSDT" in health.registered

    await supervisor.stop()


@pytest.mark.asyncio
async def test_no_reconcile_when_nothing_changed(tmp_path, fake_streams):
    stop_event = asyncio.Event()
    subscriptions, health, supervisor = _supervisor(tmp_path, stop_event)
    subscriptions.add_symbols("trades", ["BTCUSDT"])

    await supervisor.reconcile()
    await asyncio.sleep(0)
    await supervisor.reconcile()  # nothing changed: must not restart the stream
    await asyncio.sleep(0)

    assert len(fake_streams) == 1  # only one stream start, not two

    await supervisor.stop()


@pytest.mark.asyncio
async def test_add_symbol_restarts_only_the_changed_channel(tmp_path, fake_streams):
    stop_event = asyncio.Event()
    subscriptions, health, supervisor = _supervisor(tmp_path, stop_event)
    subscriptions.add_symbols("trades", ["BTCUSDT"])
    subscriptions.add_symbols("depth", ["BTCUSDT"])

    await supervisor.reconcile()
    await asyncio.sleep(0)
    assert len(fake_streams) == 2  # initial trades + depth starts

    subscriptions.add_symbols("trades", ["ETHUSDT"])
    await supervisor.reconcile()
    await asyncio.sleep(0)

    # Only a new trades-stream call should appear; the depth stream is untouched.
    trades_calls = [c for c in fake_streams if c["channel"] == "trades"]
    depth_calls = [c for c in fake_streams if c["channel"] == "depth"]
    assert len(trades_calls) == 2
    assert trades_calls[-1]["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert len(depth_calls) == 1  # never restarted
    assert trades_calls[0]["cancelled"] is True  # old trades task was cancelled

    await supervisor.stop()


@pytest.mark.asyncio
async def test_remove_symbol_restarts_with_smaller_list(tmp_path, fake_streams):
    stop_event = asyncio.Event()
    subscriptions, health, supervisor = _supervisor(tmp_path, stop_event)
    subscriptions.add_symbols("trades", ["BTCUSDT", "ETHUSDT"])

    await supervisor.reconcile()
    await asyncio.sleep(0)

    subscriptions.remove_symbols("trades", ["ETHUSDT"])
    await supervisor.reconcile()
    await asyncio.sleep(0)

    trades_calls = [c for c in fake_streams if c["channel"] == "trades"]
    assert trades_calls[-1]["symbols"] == ["BTCUSDT"]

    await supervisor.stop()


@pytest.mark.asyncio
async def test_removing_all_symbols_stops_the_channel_entirely(tmp_path, fake_streams):
    stop_event = asyncio.Event()
    subscriptions, health, supervisor = _supervisor(tmp_path, stop_event)
    subscriptions.add_symbols("depth", ["BTCUSDT"])

    await supervisor.reconcile()
    await asyncio.sleep(0)
    assert supervisor._task["depth"] is not None

    subscriptions.remove_symbols("depth", ["BTCUSDT"])
    await supervisor.reconcile()
    await asyncio.sleep(0)

    assert supervisor._task["depth"] is None
    assert supervisor._running["depth"] == []

    await supervisor.stop()


@pytest.mark.asyncio
async def test_no_duplicate_streams_reconcile_called_repeatedly_without_change(tmp_path, fake_streams):
    stop_event = asyncio.Event()
    subscriptions, health, supervisor = _supervisor(tmp_path, stop_event)
    subscriptions.add_symbols("trades", ["BTCUSDT"])

    for _ in range(5):
        await supervisor.reconcile()
        await asyncio.sleep(0)

    assert len(fake_streams) == 1

    await supervisor.stop()


@pytest.mark.asyncio
async def test_stop_cancels_running_stream_tasks(tmp_path, fake_streams):
    stop_event = asyncio.Event()
    subscriptions, health, supervisor = _supervisor(tmp_path, stop_event)
    subscriptions.add_symbols("trades", ["BTCUSDT"])
    subscriptions.add_symbols("depth", ["ETHUSDT"])

    await supervisor.reconcile()
    await asyncio.sleep(0)

    await supervisor.stop()

    assert all(c["cancelled"] for c in fake_streams)
