"""Positioning poller tests: funding_oi polling from Binance USDⓈ-M REST endpoint."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from littledevil_recorder.positioning_poller import run_positioning_poller
from littledevil_recorder.subscription_manager import SubscriptionManager


class _FakeWriter:
    """Minimal stand-in for ParquetWriter, capturing write_funding_oi calls."""

    def __init__(self):
        self.writes: list[dict] = []

    def write_funding_oi(self, symbol: str, *, ts_exchange, ts_received, open_interest, funding_rate, mark_price, index_price, source="binance_usdm"):
        self.writes.append({
            "symbol": symbol,
            "ts_exchange": ts_exchange,
            "ts_received": ts_received,
            "open_interest": open_interest,
            "funding_rate": funding_rate,
            "mark_price": mark_price,
            "index_price": index_price,
            "source": source,
        })


@pytest.mark.asyncio
async def test_run_poller_reads_desired_symbols_and_polls_when_available(tmp_path):
    """Poller detects funding_oi desired symbols and polls them on iteration."""
    subscriptions = SubscriptionManager(tmp_path)
    writer = _FakeWriter()
    stop_event = asyncio.Event()

    subscriptions.add_symbols("funding_oi", ["BTCUSDT"])

    poll_count = 0

    async def mock_poll_batch(session, w, symbols):
        nonlocal poll_count
        poll_count += 1
        if symbols == ["BTCUSDT"]:
            w.write_funding_oi(
                "BTCUSDT",
                ts_exchange=datetime.now(UTC),
                ts_received=datetime.now(UTC),
                open_interest=100.0,
                funding_rate=0.0001,
                mark_price=50000.0,
                index_price=50001.0,
            )
        if poll_count >= 1:
            stop_event.set()

    with patch("littledevil_recorder.positioning_poller._poll_batch", side_effect=mock_poll_batch):
        await run_positioning_poller(subscriptions, writer, poll_interval=0.01, stop_event=stop_event)

    assert poll_count == 1
    assert len(writer.writes) == 1
    assert writer.writes[0]["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_poller_skips_poll_when_no_desired_symbols(tmp_path):
    """Poller skips _poll_batch entirely if desired_symbols list is empty."""
    subscriptions = SubscriptionManager(tmp_path)
    writer = _FakeWriter()
    stop_event = asyncio.Event()

    # No symbols added
    poll_count = 0

    async def mock_poll_batch(session, w, symbols):
        nonlocal poll_count
        poll_count += 1

    with patch("littledevil_recorder.positioning_poller._poll_batch", side_effect=mock_poll_batch):
        # Set stop_event immediately so we only iterate once
        stop_event.set()
        await run_positioning_poller(subscriptions, writer, poll_interval=0.01, stop_event=stop_event)

    # _poll_batch should not have been called because no symbols desired
    assert poll_count == 0


@pytest.mark.asyncio
async def test_poller_runtime_symbol_additions(tmp_path):
    """Poller respects runtime add_symbols calls between iterations."""
    subscriptions = SubscriptionManager(tmp_path)
    writer = _FakeWriter()
    stop_event = asyncio.Event()

    subscriptions.add_symbols("funding_oi", ["BTCUSDT"])

    poll_symbols_seen = []

    async def mock_poll_batch(session, w, symbols):
        poll_symbols_seen.append(list(symbols))
        if len(poll_symbols_seen) == 1:
            subscriptions.add_symbols("funding_oi", ["ETHUSDT"])
        elif len(poll_symbols_seen) == 2:
            stop_event.set()

    with patch("littledevil_recorder.positioning_poller._poll_batch", side_effect=mock_poll_batch):
        await run_positioning_poller(subscriptions, writer, poll_interval=0.01, stop_event=stop_event)

    assert len(poll_symbols_seen) == 2
    assert poll_symbols_seen[0] == ["BTCUSDT"]
    assert set(poll_symbols_seen[1]) == {"BTCUSDT", "ETHUSDT"}
