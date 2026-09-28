"""Binance USDⓈ-M positioning polling: open interest, funding, mark/index price.

Three genuinely distinct facts live behind three distinct, separately-verified
official endpoints -- they are polled and written independently so a field
absent from one response can never be silently zero-filled from another:

- Open interest: `GET /fapi/v1/openInterest` -> {symbol, openInterest, time}.
  No funding or price field exists in this response at all.
- Mark/index price: `GET /fapi/v1/premiumIndex` -> `markPrice`, `indexPrice`,
  `estimatedSettlePrice`, `lastFundingRate`, `interestRate`, `nextFundingTime`.
  `bookTicker` is a different market-data surface (best bid/ask only) and is
  never used as a mark/index source. `lastFundingRate` is carried on this
  record as one of `premiumIndex`'s own honestly-sourced fields, but it does
  NOT get its own `funding` schema record from here: `premiumIndex` gives no
  timestamp for when that rate actually settled (only `nextFundingTime`, the
  *upcoming* settlement), so fabricating a `funding_time` for it from the
  poll time would mislabel a poll-time observation as a settlement event.
- Funding (realized, with a true settlement time): `GET /fapi/v1/fundingRate`
  with `limit=1`, giving the single most recent realized settlement with its
  own authoritative `fundingTime` and `rateType`. Polled at the same cadence
  as the others; writing the same realized row again before the next 8h
  settlement is an idempotent re-observation, not a new fact -- callers
  wanting only settlement-boundary transitions can dedupe on `funding_time`.

Per architecture-review.md §2, this poller runs independently from the
WebSocket streams and is restart-safe: desired symbol state is persisted by
SubscriptionManager under the "positioning" channel.

Gap/recovery semantics: a missed poll (Binance-side error, timeout, process
downtime) is never actively backfilled -- there is no historical re-fetch of
a specific missed poll window anywhere in this module. architecture-
review.md §3 (D5 row) names "poll gaps" as positioning's known failure mode
and specifies no recovery mechanism beyond detecting them; §2's stage table
only requires OI/funding/mark-index to be "polled and stored," not backfilled
on gap. This is intentional, not an oversight: each poll is a fresh,
independent, point-in-time observation (unlike trades, which have a venue-
assigned ID sequence letting a specific missed range be identified and
re-fetched from Binance's own REST history). A missed positioning poll has no
equivalent recoverable identity to backfill -- the next successful poll is
simply the next observation, not a replacement for the missed one. The
existing DataHealthTracker stale (30s) / suspended (120s) / gap_started_at
mechanism (data_health.py; unmodified, reused as-is) is therefore the entire
gap-semantics contract for this channel: a Binance-side failure is made
visible as an explicit, timestamped gap in `data_health`, never silently
hidden, but it is a detection contract, not a backfill contract. See
main.py's `on_positioning_polled` for the callback that drives this, and
tests/test_positioning_liquidation_data_health.py for the interference proof.
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

OPEN_INTEREST_URL = "https://fapi.binance.com/fapi/v1/openInterest"
PREMIUM_INDEX_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
FUNDING_RATE_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
DEFAULT_POLL_INTERVAL_SECONDS = 60.0


OnSymbolPolled = Callable[[str, Exception | None], None]


async def run_positioning_poller(
    subscriptions: SubscriptionManager,
    writer: ParquetWriter,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    *,
    stop_event: asyncio.Event,
    on_symbol_polled: OnSymbolPolled | None = None,
) -> None:
    """Poll Binance USDⓈ-M open interest, funding, and mark/index at intervals.

    Desired symbol list is read from subscriptions on each iteration, allowing
    runtime add/remove without restarting the poller task. Polling is resilient
    to transient network errors; persistent failures are logged but do not halt
    the loop. Stopped via stop_event. `on_symbol_polled(symbol, error)` is
    called after every symbol's poll attempt (error is None on success) so a
    caller can feed Data Health without this module depending on it directly.
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
            logger.warning(f"Failed to poll {symbol}: {error!r}")
        if on_symbol_polled is not None:
            on_symbol_polled(symbol, error)


async def _poll_symbol(session: aiohttp.ClientSession, writer: ParquetWriter, symbol: str) -> None:
    """Poll a single symbol's open interest and mark/index/funding snapshot.

    Each source is fetched and written independently; a failure in one raises
    (surfacing in _poll_batch's per-symbol exception log) without silently
    fabricating or zero-filling the other's fields.
    """
    errors: dict[str, Exception] = {}
    for name, coro in (
        ("open_interest", _poll_open_interest(session, writer, symbol)),
        ("premium_index", _poll_premium_index(session, writer, symbol)),
        ("funding_rate", _poll_funding_rate(session, writer, symbol)),
    ):
        try:
            await coro
        except Exception as exc:
            errors[name] = exc
    if errors:
        detail = " ".join(f"{name}={exc!r}" for name, exc in errors.items())
        raise RuntimeError(f"Failed to fetch {symbol}: {detail}")


async def _poll_open_interest(session: aiohttp.ClientSession, writer: ParquetWriter, symbol: str) -> None:
    ts_received = datetime.now(UTC)
    async with session.get(OPEN_INTEREST_URL, params={"symbol": symbol}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} from {OPEN_INTEREST_URL}")
        data = await resp.json()
        ts_exchange = datetime.fromtimestamp(int(data["time"]) / 1000, tz=UTC)
        writer.write_open_interest(
            symbol,
            ts_exchange=ts_exchange,
            ts_received=ts_received,
            open_interest=float(data["openInterest"]),
            source="binance_usdm",
        )


async def _poll_premium_index(session: aiohttp.ClientSession, writer: ParquetWriter, symbol: str) -> None:
    ts_received = datetime.now(UTC)
    async with session.get(PREMIUM_INDEX_URL, params={"symbol": symbol}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} from {PREMIUM_INDEX_URL}")
        data = await resp.json()
        ts_exchange = datetime.fromtimestamp(int(data["time"]) / 1000, tz=UTC)
        next_funding_time = datetime.fromtimestamp(int(data["nextFundingTime"]) / 1000, tz=UTC)
        writer.write_mark_index(
            symbol,
            ts_exchange=ts_exchange,
            ts_received=ts_received,
            mark_price=float(data["markPrice"]),
            index_price=float(data["indexPrice"]),
            estimated_settle_price=float(data["estimatedSettlePrice"]),
            last_funding_rate=float(data["lastFundingRate"]),
            interest_rate=float(data["interestRate"]),
            next_funding_time=next_funding_time,
            source="binance_usdm",
        )


async def _poll_funding_rate(session: aiohttp.ClientSession, writer: ParquetWriter, symbol: str) -> None:
    """Fetch the single most recent realized funding settlement.

    `limit=1` with no startTime/endTime returns the latest record per the
    official contract ("If startTime and endTime are not sent, the most
    recent records are returned"). `fundingTime` here is the endpoint's own
    authoritative settlement timestamp -- never derived from poll time.
    """
    ts_received = datetime.now(UTC)
    params = {"symbol": symbol, "limit": 1}
    async with session.get(FUNDING_RATE_URL, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} from {FUNDING_RATE_URL}")
        rows = await resp.json()
        if not rows:
            raise RuntimeError(f"empty funding rate history for {symbol}")
        data = rows[0]
        funding_time = datetime.fromtimestamp(int(data["fundingTime"]) / 1000, tz=UTC)
        writer.write_funding(
            symbol,
            ts_exchange=funding_time,
            ts_received=ts_received,
            funding_rate=float(data["fundingRate"]),
            funding_time=funding_time,
            mark_price=float(data["markPrice"]),
            rate_type=data.get("rateType"),
            source="binance_usdm",
        )
