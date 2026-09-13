"""@aggTrade ingestion for the whole eligible universe (docs/architecture-review.md
§2, §6.1; docs/implementation-plan.md phase 0.2). Every trade is stamped with
both ts_exchange (Binance's own event time) and ts_received (our own wall
clock at receipt) -- this is the two-clock model architecture-review.md §7
requires for honest replay.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import websockets

logger = logging.getLogger(__name__)

STREAM_HOST = "wss://data-stream.binance.vision"
RECONNECT_DELAY_SECONDS = 2.0


def combined_stream_url(symbols: list[str]) -> str:
    streams = "/".join(f"{s.lower()}@aggTrade" for s in symbols)
    return f"{STREAM_HOST}/stream?streams={streams}"


def parse_agg_trade(raw: dict) -> dict:
    """Parse one @aggTrade payload (the 'data' field of a combined-stream
    message) into the fields data-and-events.md §2's trades/ schema wants."""
    return {
        "symbol": raw["s"],
        "trade_id": raw["a"],
        "ts_exchange": datetime.fromtimestamp(raw["T"] / 1000, tz=UTC),
        "price": float(raw["p"]),
        "qty": float(raw["q"]),
        "is_buyer_maker": raw["m"],
    }


OnTrade = Callable[[dict], Awaitable[None]]


async def run_aggtrade_stream(
    symbols: list[str],
    on_trade: OnTrade,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Connects to the combined @aggTrade stream for `symbols` and calls
    `on_trade(parsed)` for every message, reconnecting on any drop. Runs
    until `stop_event` is set (or forever if none given) -- callers that
    need graceful shutdown should pass one."""
    stop_event = stop_event or asyncio.Event()
    url = combined_stream_url(symbols)

    while not stop_event.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logger.info("aggTrade stream connected: %d symbols", len(symbols))
                while not stop_event.is_set():
                    message = await ws.recv()
                    ts_received = datetime.now(UTC)
                    envelope = json.loads(message)
                    parsed = parse_agg_trade(envelope["data"])
                    parsed["ts_received"] = ts_received
                    await on_trade(parsed)
        except (websockets.ConnectionClosed, OSError) as exc:
            logger.warning("aggTrade stream dropped (%s); reconnecting in %.1fs", exc, RECONNECT_DELAY_SECONDS)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
