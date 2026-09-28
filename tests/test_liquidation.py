"""Public, market-wide `!forceOrder@arr` liquidation stream: parsing,
reconnect-on-drop, and the caller-side symbol filter (this stream has no
server-side per-symbol subscribe/unsubscribe, unlike @aggTrade/@depth).
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest
import websockets

from littledevil_recorder.liquidation import parse_liquidation, run_liquidation_stream


def test_parse_liquidation_matches_official_forceorder_payload_shape():
    # Real shape of an all-market forceOrder event per official docs.
    raw = {
        "e": "forceOrder",
        "E": 1568014460893,
        "o": {
            "s": "BTCUSDT",
            "S": "SELL",
            "o": "LIMIT",
            "f": "IOC",
            "q": "0.014",
            "p": "9910.79",
            "ap": "9910.79",
            "X": "FILLED",
            "l": "0.014",
            "z": "0.014",
            "T": 1568014460893,
        },
    }
    parsed = parse_liquidation(raw)
    assert parsed["symbol"] == "BTCUSDT"
    assert parsed["side"] == "SELL"
    assert parsed["order_type"] == "LIMIT"
    assert parsed["time_in_force"] == "IOC"
    assert parsed["orig_qty"] == 0.014
    assert parsed["price"] == 9910.79
    assert parsed["avg_price"] == 9910.79
    assert parsed["order_status"] == "FILLED"
    assert parsed["last_filled_qty"] == 0.014
    assert parsed["accumulated_qty"] == 0.014
    assert parsed["ts_exchange"] == datetime.fromtimestamp(1568014460893 / 1000, tz=UTC)
    assert parsed["order_trade_time"] == datetime.fromtimestamp(1568014460893 / 1000, tz=UTC)


def test_parse_liquidation_never_touches_private_forceorders_fields():
    """Sanity fence: nothing in parse_liquidation reads a field shape unique
    to the private GET /fapi/v1/forceOrders (user's own order history) --
    it only ever reads the public stream's own documented 'o' object."""
    raw = {
        "e": "forceOrder", "E": 1, "o": {
            "s": "ETHUSDT", "S": "BUY", "o": "LIMIT", "f": "IOC", "q": "1",
            "p": "1", "ap": "1", "X": "FILLED", "l": "1", "z": "1", "T": 1,
        },
    }
    parsed = parse_liquidation(raw)
    assert set(parsed) == {
        "symbol", "ts_exchange", "side", "order_type", "time_in_force",
        "orig_qty", "price", "avg_price", "order_status", "last_filled_qty",
        "accumulated_qty", "order_trade_time",
    }


class _FakeConnectionClosed(Exception):
    pass


@pytest.mark.asyncio
async def test_run_liquidation_stream_reconnects_after_drop(monkeypatch):
    """First connection yields one message then drops; second connection
    yields one more message then the test stops it -- confirms reconnect-
    on-drop without a real socket."""
    attempt = 0

    class _FakeWS:
        def __init__(self, messages):
            self._messages = list(messages)

        async def send(self, message):
            # Acknowledge subscription request
            pass

        async def recv(self):
            if not self._messages:
                raise websockets.ConnectionClosed(None, None)
            return self._messages.pop(0)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    def fake_connect(url, **kwargs):
        nonlocal attempt
        attempt += 1
        if attempt == 1:
            return _FakeWS([json.dumps({
                "e": "forceOrder", "E": 1, "o": {
                    "s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "f": "IOC",
                    "q": "1", "p": "1", "ap": "1", "X": "FILLED", "l": "1",
                    "z": "1", "T": 1,
                },
            })])
        return _FakeWS([json.dumps({
            "e": "forceOrder", "E": 2, "o": {
                "s": "ETHUSDT", "S": "BUY", "o": "LIMIT", "f": "IOC",
                "q": "1", "p": "1", "ap": "1", "X": "FILLED", "l": "1",
                "z": "1", "T": 2,
            },
        })])

    monkeypatch.setattr("littledevil_recorder.liquidation.websockets.connect", fake_connect)
    monkeypatch.setattr("littledevil_recorder.liquidation.RECONNECT_DELAY_SECONDS", 0.0)

    received: list[dict] = []
    stop_event = asyncio.Event()

    async def on_liquidation(event: dict) -> None:
        received.append(event)
        if len(received) >= 2:
            stop_event.set()

    await run_liquidation_stream(on_liquidation, stop_event=stop_event)

    assert attempt == 2
    assert [e["symbol"] for e in received] == ["BTCUSDT", "ETHUSDT"]
    assert all("ts_received" in e for e in received)


@pytest.mark.asyncio
async def test_main_liquidation_callback_filters_undesired_symbols(tmp_path):
    """The all-market stream carries every symbol; main.py's on_liquidation
    callback must drop anything not in the recorder's own desired liquidation
    symbol set rather than persisting it."""
    from littledevil_recorder.storage import ParquetWriter
    from littledevil_recorder.subscription_manager import SubscriptionManager

    subscriptions = SubscriptionManager(tmp_path)
    subscriptions.add_symbols("liquidation", ["BTCUSDT"])
    writer = ParquetWriter(tmp_path)

    written = []

    async def on_liquidation(event: dict) -> None:
        symbol = event["symbol"]
        if symbol not in subscriptions.desired_symbols("liquidation"):
            return
        written.append(symbol)

    await on_liquidation({"symbol": "BTCUSDT"})
    await on_liquidation({"symbol": "DOGEUSDT"})

    assert written == ["BTCUSDT"]
    writer.close()
