"""@depth@100ms ingestion + local order-book reconstruction, recording-set
only (docs/scanner-attention-routing.md §2; docs/architecture-review.md
§6.1). Implements Binance's own documented procedure for maintaining a
local order book from the diff-depth stream:

  1. Open the @depth@100ms stream and buffer events.
  2. Fetch a REST snapshot (has its own lastUpdateId).
  3. Drop any buffered event where event.u <= snapshot.lastUpdateId.
  4. The first event applied must have event.U <= lastUpdateId+1 <= event.u.
  5. Apply each event in order: for each [price, qty] pair, qty == 0 means
     remove the level, otherwise set/replace it.

This is what Stage 0's ≥99% book-reconstruction gate criterion actually
checks: reconstructed book vs a fresh REST snapshot at aligned checkpoints.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import websockets

logger = logging.getLogger(__name__)

STREAM_HOST = "wss://data-stream.binance.vision"
REST_HOST = "https://data-api.binance.vision"
RECONNECT_DELAY_SECONDS = 2.0


class LocalOrderBook:
    """One symbol's reconstructed book. `bids`/`asks` map price -> qty,
    both as Decimal to avoid float drift across thousands of updates."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.last_update_id: int | None = None
        self.synced = False

    def load_snapshot(self, snapshot: dict) -> None:
        self.bids = {Decimal(p): Decimal(q) for p, q in snapshot["bids"]}
        self.asks = {Decimal(p): Decimal(q) for p, q in snapshot["asks"]}
        self.last_update_id = snapshot["lastUpdateId"]
        self.synced = False

    def can_apply_first(self, event: dict) -> bool:
        """First applicable event must straddle the snapshot's lastUpdateId."""
        return event["U"] <= self.last_update_id + 1 <= event["u"]

    def is_stale(self, event: dict) -> bool:
        """Events entirely before the snapshot are dropped, not applied."""
        return event["u"] <= self.last_update_id

    def apply(self, event: dict) -> None:
        for price_str, qty_str in event["b"]:
            self._apply_side(self.bids, price_str, qty_str)
        for price_str, qty_str in event["a"]:
            self._apply_side(self.asks, price_str, qty_str)
        self.last_update_id = event["u"]
        self.synced = True

    @staticmethod
    def _apply_side(side: dict[Decimal, Decimal], price_str: str, qty_str: str) -> None:
        price = Decimal(price_str)
        qty = Decimal(qty_str)
        if qty == 0:
            side.pop(price, None)
        else:
            side[price] = qty

    def top_n(self, n: int = 20) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
        bids = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:n]
        asks = sorted(self.asks.items(), key=lambda kv: kv[0])[:n]
        return (
            [(str(p), str(q)) for p, q in bids],
            [(str(p), str(q)) for p, q in asks],
        )


async def fetch_depth_snapshot(client: httpx.AsyncClient, symbol: str, limit: int = 1000) -> dict:
    resp = await client.get(
        f"{REST_HOST}/api/v3/depth", params={"symbol": symbol.upper(), "limit": limit}
    )
    resp.raise_for_status()
    return resp.json()


def combined_depth_stream_url(symbols: list[str]) -> str:
    streams = "/".join(f"{s.lower()}@depth@100ms" for s in symbols)
    return f"{STREAM_HOST}/stream?streams={streams}"


OnDepthEvent = Callable[[str, LocalOrderBook, dict], Awaitable[None]]


async def run_depth_stream(
    symbols: list[str],
    on_event: OnDepthEvent,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Maintains one LocalOrderBook per symbol, resyncing from a fresh REST
    snapshot on every (re)connect, and calls `on_event(symbol, book, raw)`
    after every successfully applied diff."""
    stop_event = stop_event or asyncio.Event()
    url = combined_depth_stream_url(symbols)

    while not stop_event.is_set():
        books = {s.upper(): LocalOrderBook(s.upper()) for s in symbols}
        buffers: dict[str, list[dict]] = {s.upper(): [] for s in symbols}

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    logger.info("depth stream connected: %d symbols", len(symbols))

                    # Buffer live events while snapshots are fetched, per
                    # Binance's documented procedure.
                    buffering = True

                    async def buffer_until_snapshots_ready():
                        nonlocal buffering
                        while buffering and not stop_event.is_set():
                            message = await ws.recv()
                            envelope = json.loads(message)
                            event = envelope["data"]
                            buffers[event["s"]].append(event)

                    buffer_task = asyncio.create_task(buffer_until_snapshots_ready())
                    await asyncio.sleep(1.0)  # let a bit of buffer accumulate first

                    for symbol in books:
                        snapshot = await fetch_depth_snapshot(client, symbol)
                        books[symbol].load_snapshot(snapshot)

                    buffering = False
                    await buffer_task

                    for symbol, book in books.items():
                        pending = [e for e in buffers[symbol] if not book.is_stale(e)]
                        for i, event in enumerate(pending):
                            if not book.synced:
                                if i == 0 and not book.can_apply_first(event):
                                    continue
                            book.apply(event)
                            await on_event(symbol, book, event)

                    while not stop_event.is_set():
                        message = await ws.recv()
                        ts_received = datetime.now(UTC)
                        envelope = json.loads(message)
                        event = envelope["data"]
                        symbol = event["s"]
                        book = books[symbol]
                        if book.is_stale(event):
                            continue
                        book.apply(event)
                        event["_ts_received"] = ts_received
                        await on_event(symbol, book, event)

        except (websockets.ConnectionClosed, OSError, httpx.HTTPError) as exc:
            logger.warning("depth stream dropped (%s); resyncing in %.1fs", exc, RECONNECT_DELAY_SECONDS)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
