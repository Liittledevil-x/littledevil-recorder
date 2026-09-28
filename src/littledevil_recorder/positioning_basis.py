"""Binance USDⓈ-M basis data polling.

Basis = futures price - index price (expressed as rate or bps).

Official Binance endpoint: `GET /futures/data/basis`
- Query parameters: pair (required), contractType (PERPETUAL/CURRENT_QUARTER/
  NEXT_QUARTER, required), period (required: 5m, 15m, 30m, 1h, 2h, 4h, 6h,
  8h, 12h, 1d), limit (max 500, default 30), startTime/endTime (ms).
- Response: array of {indexPrice, contractType, basisRate, futuresPrice,
  annualizedBasisRate, basis, pair, timestamp}.
- Rate limit: IP weight 0 (negligible).
- Data retention: latest 30 days only.
- timestamp: "Start time of the period, in milliseconds."

Basis is polled at the same cadence as positioning (OI/funding/mark-index).
Since basis is derived from two independent market observations (index price
and futures price), it is stored as a separate, independent schema per the
"one record per observed fact" convention used throughout the Recorder.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime

import aiohttp

from littledevil_recorder.storage import ParquetWriter
from littledevil_recorder.subscription_manager import SubscriptionManager

logger = logging.getLogger(__name__)

BASIS_URL = "https://fapi.binance.com/futures/data/basis"
DEFAULT_POLL_INTERVAL_SECONDS = 60.0
DEFAULT_PERIOD = "1h"  # 1-hour basis snapshots

OnSymbolPolled = Callable[[str, Exception | None], None]


async def run_basis_poller(
    subscriptions: SubscriptionManager,
    writer: ParquetWriter,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    *,
    stop_event: asyncio.Event,
    on_symbol_polled: OnSymbolPolled | None = None,
) -> None:
    """Poll Binance USDⓈ-M basis data at intervals.

    Desired symbol list is read from subscriptions on each iteration, allowing
    runtime add/remove without restarting. Polling is resilient to transient
    network errors; persistent failures are logged but do not halt the loop.
    Stopped via stop_event. `on_symbol_polled(symbol, error)` is called after
    every symbol's poll attempt (error is None on success) so a caller can
    feed Data Health.
    """
    async with aiohttp.ClientSession() as session:
        while not stop_event.is_set():
            symbols = subscriptions.desired_symbols("positioning")
            if symbols:
                await _poll_batch(session, writer, symbols, on_symbol_polled)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
            except asyncio.TimeoutError:
                pass


async def _poll_batch(
    session: aiohttp.ClientSession,
    writer: ParquetWriter,
    symbols: list[str],
    on_symbol_polled: OnSymbolPolled | None = None,
) -> None:
    """Poll a batch of symbols concurrently; failures are logged and skipped."""
    tasks = [_poll_symbol(session, writer, symbol) for symbol in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for symbol, result in zip(symbols, results, strict=False):
        error = result if isinstance(result, Exception) else None
        if error is not None:
            logger.warning(f"Failed to poll basis for {symbol}: {error!r}")
        if on_symbol_polled is not None:
            on_symbol_polled(symbol, error)


async def _poll_symbol(session: aiohttp.ClientSession, writer: ParquetWriter, symbol: str) -> None:
    """Poll a single symbol's latest basis (1-hour snapshot via the official endpoint)."""
    ts_received = datetime.now(UTC)
    params = {
        "pair": symbol,
        "contractType": "PERPETUAL",
        "period": DEFAULT_PERIOD,
        "limit": 1,
    }
    async with session.get(BASIS_URL, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} from {BASIS_URL}")
        rows = await resp.json()
        if not rows:
            raise RuntimeError(f"empty basis data for {symbol}")

        data = rows[0]
        ts_exchange = datetime.fromtimestamp(int(data["timestamp"]) / 1000, tz=UTC)

        writer.write_basis(
            symbol,
            ts_exchange=ts_exchange,
            ts_received=ts_received,
            index_price=float(data["indexPrice"]),
            futures_price=float(data["futuresPrice"]),
            basis_rate=float(data["basisRate"]),
            basis=float(data["basis"]),
            annualized_basis_rate=float(data["annualizedBasisRate"]),
            contract_type=data["contractType"],
            source="binance_usdm",
        )
