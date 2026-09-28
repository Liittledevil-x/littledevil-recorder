"""Binance USDⓈ-M funding rate and open interest polling (30-60s cadence).

Polls the Binance USDⓈ-M REST endpoint for funding_rate and open_interest metrics
on a configurable symbol universe. Results are written to the funding_oi Parquet channel.
Per architecture-review.md §2, this poller runs independently from the WebSocket
streams and is restart-safe: desired symbol state is persisted by SubscriptionManager.

Note: Binance funding_rate is reset to zero at epoch each interval; open_interest is
a point-in-time cumulative snapshot. Both are marked with ts_exchange=server_time and
ts_received=local_now for provenance tracking per data-and-events.md §4.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

import aiohttp

from littledevil_recorder.storage import ParquetWriter
from littledevil_recorder.subscription_manager import SubscriptionManager

logger = logging.getLogger(__name__)

BINANCE_USDM_URL = "https://fapi.binance.com/fapi/v1/openInterest"
DEFAULT_POLL_INTERVAL_SECONDS = 60.0


async def run_positioning_poller(
    subscriptions: SubscriptionManager,
    writer: ParquetWriter,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Poll Binance USDⓈ-M funding rate and open interest at regular intervals.

    Desired symbol list is read from subscriptions on each iteration, allowing
    runtime add/remove without restarting the poller task. Polling is resilient
    to transient network errors; persistent failures are logged but do not halt
    the loop. Stopped via stop_event.
    """
    async with aiohttp.ClientSession() as session:
        while not stop_event.is_set():
            symbols = subscriptions.desired_symbols("funding_oi")
            if symbols:
                await _poll_batch(session, writer, symbols)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
            except asyncio.TimeoutError:
                pass


async def _poll_batch(session: aiohttp.ClientSession, writer: ParquetWriter, symbols: list[str]) -> None:
    """Poll a batch of symbols concurrently; failures are logged and skipped."""
    tasks = [_poll_symbol(session, writer, symbol) for symbol in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for symbol, result in zip(symbols, results, strict=False):
        if isinstance(result, Exception):
            logger.warning(f"Failed to poll {symbol}: {result!r}")


async def _poll_symbol(session: aiohttp.ClientSession, writer: ParquetWriter, symbol: str) -> None:
    """Poll a single symbol's funding_rate and open_interest from Binance USDⓈ-M."""
    ts_received = datetime.now(UTC)
    try:
        async with session.get(BINANCE_USDM_URL, params={"symbol": symbol}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}")
            data = await resp.json()
            ts_exchange = datetime.fromtimestamp(int(data.get("time", 0)) / 1000, tz=UTC)
            writer.write_funding_oi(
                symbol,
                ts_exchange=ts_exchange,
                ts_received=ts_received,
                open_interest=float(data.get("openInterest", 0.0)),
                funding_rate=float(data.get("fundingRate", 0.0)),
                mark_price=float(data.get("markPrice", 0.0)),
                index_price=float(data.get("indexPrice", 0.0)),
                source="binance_usdm",
            )
    except Exception as exc:
        raise RuntimeError(f"Failed to fetch {symbol}: {exc!r}") from exc
