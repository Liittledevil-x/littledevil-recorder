"""Gap recovery for positioning channels (OI/funding/mark-index/basis).

When Data Health detects a gap, this module attempts to fill it using
official Binance historical endpoints. Each channel has different recovery
characteristics:

- OI: coarse (5-minute aggregated) recovery via /futures/data/openInterestHist
- Funding: full (event-level) recovery via /fapi/v1/fundingRate with time range
- Mark/index: coarse (kline bars) recovery via mark/indexPriceKlines
- Basis: coarse (time-period aggregated) recovery via /futures/data/basis
- Liquidation: unrecoverable (sampled stream, no authoritative history)

Recovery is orchestrated from main.py's periodic task, not inline during polling.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from littledevil_recorder.storage import ParquetWriter

logger = logging.getLogger(__name__)

OI_HISTORY_URL = "https://fapi.binance.com/futures/data/openInterestHist"
FUNDING_RATE_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
MARK_KLINES_URL = "https://fapi.binance.com/fapi/v1/markPriceKlines"
INDEX_KLINES_URL = "https://fapi.binance.com/fapi/v1/indexPriceKlines"
BASIS_HISTORY_URL = "https://fapi.binance.com/futures/data/basis"


@dataclass
class RecoveryResult:
    channel: str
    symbol: str
    gap_start: datetime
    gap_end: datetime
    recovered_count: int
    resolution: str  # "exact" | "1m" | "5m" | "1h" etc
    source: str
    errors: list[str]


def recovery_capability(channel: str) -> str | None:
    """Classify channel recoverability.

    Returns: "full", "coarse", "unrecoverable", or None if unknown channel.
    """
    if channel == "positioning":
        # Positioning is a composite channel; classify by sub-kind
        return "mixed"
    elif channel in ("funding", "mark_index", "open_interest", "basis"):
        return "coarse"  # All positioning sub-channels have coarse recovery
    elif channel == "liquidation":
        return "unrecoverable"
    return None


async def recover_oi_gap(
    client: httpx.AsyncClient, writer: ParquetWriter, symbol: str,
    gap_start: datetime, gap_end: datetime
) -> RecoveryResult:
    """Recover OI using historical 5-minute aggregated data.

    Returns coarse recovery result; persists with resolution metadata.
    """
    errors = []
    recovered = 0

    try:
        # OI history: 5-minute interval, last 30 days only
        params = {
            "symbol": symbol,
            "period": "5m",
            "startTime": int(gap_start.timestamp() * 1000),
            "endTime": int(gap_end.timestamp() * 1000),
            "limit": 500,
        }

        resp = await client.get(OI_HISTORY_URL, params=params, timeout=httpx.Timeout(10))
        if resp.status_code != 200:
            errors.append(f"HTTP {resp.status_code} from OI history")
        else:
            rows = resp.json()
            for row in rows:
                ts_exchange = datetime.fromtimestamp(int(row["timestamp"]) / 1000, tz=UTC)
                ts_received = datetime.now(UTC)

                writer.write_open_interest(
                    symbol,
                    ts_exchange=ts_exchange,
                    ts_received=ts_received,
                    open_interest=float(row["sumOpenInterest"]),
                    source="binance_usdm_recovered_5m",
                )
                recovered += 1
    except Exception as exc:
        errors.append(f"OI recovery error: {exc!r}")

    return RecoveryResult(
        channel="positioning",
        symbol=symbol,
        gap_start=gap_start,
        gap_end=gap_end,
        recovered_count=recovered,
        resolution="5m_coarse",
        source="openInterestHist",
        errors=errors,
    )


async def recover_funding_gap(
    client: httpx.AsyncClient, writer: ParquetWriter, symbol: str,
    gap_start: datetime, gap_end: datetime
) -> RecoveryResult:
    """Recover funding using full event-level history.

    Funding history is queryable by time range; we recover all events
    within the gap and deduplicate by fundingTime.
    """
    errors = []
    recovered = 0
    seen_times = set()

    try:
        params = {
            "symbol": symbol,
            "startTime": int(gap_start.timestamp() * 1000),
            "endTime": int(gap_end.timestamp() * 1000),
            "limit": 1000,
        }

        resp = await client.get(FUNDING_RATE_URL, params=params, timeout=httpx.Timeout(10))
        if resp.status_code != 200:
            errors.append(f"HTTP {resp.status_code} from funding history")
        else:
            rows = resp.json()
            for row in rows:
                funding_time = datetime.fromtimestamp(int(row["fundingTime"]) / 1000, tz=UTC)

                # Deduplicate by fundingTime
                if funding_time in seen_times:
                    continue
                seen_times.add(funding_time)

                ts_received = datetime.now(UTC)

                writer.write_funding(
                    symbol,
                    ts_exchange=funding_time,
                    ts_received=ts_received,
                    funding_rate=float(row["fundingRate"]),
                    funding_time=funding_time,
                    mark_price=float(row["markPrice"]),
                    rate_type=row.get("rateType", "unknown"),
                    source="binance_usdm_recovered_history",
                )
                recovered += 1
    except Exception as exc:
        errors.append(f"Funding recovery error: {exc!r}")

    return RecoveryResult(
        channel="positioning",
        symbol=symbol,
        gap_start=gap_start,
        gap_end=gap_end,
        recovered_count=recovered,
        resolution="exact_events",
        source="fundingRate_history",
        errors=errors,
    )


async def recover_mark_index_gap(
    client: httpx.AsyncClient, writer: ParquetWriter, symbol: str,
    gap_start: datetime, gap_end: datetime, interval: str = "1m"
) -> RecoveryResult:
    """Recover mark/index using historical klines at specified interval.

    Returns coarse (bar-level) recovery; persists with resolution metadata.
    """
    errors = []
    recovered_mark = 0
    recovered_index = 0

    try:
        common_params = {
            "interval": interval,
            "startTime": int(gap_start.timestamp() * 1000),
            "endTime": int(gap_end.timestamp() * 1000),
            "limit": 1500,
        }
        mark_params = {"symbol": symbol, **common_params}
        index_params = {"pair": symbol, **common_params}

        # Recover mark price klines
        resp = await client.get(MARK_KLINES_URL, params=mark_params, timeout=httpx.Timeout(10))
        if resp.status_code == 200:
            rows = resp.json()
            for row in rows:
                ts_exchange = datetime.fromtimestamp(int(row[0]) / 1000, tz=UTC)
                ts_received = datetime.now(UTC)

                writer.write_mark_index(
                    symbol,
                    ts_exchange=ts_exchange,
                    ts_received=ts_received,
                    mark_price=float(row[4]),  # close price
                    index_price=0.0,  # placeholder; will be overwritten by index query
                    estimated_settle_price=0.0,
                    last_funding_rate=0.0,
                    interest_rate=0.0,
                    next_funding_time=datetime.now(UTC),
                    source=f"binance_usdm_recovered_markKlines_{interval}",
                )
                recovered_mark += 1
        else:
            errors.append(f"HTTP {resp.status_code} from mark klines")

        # Recover index price klines
        resp = await client.get(INDEX_KLINES_URL, params=index_params, timeout=httpx.Timeout(10))
        if resp.status_code == 200:
            rows = resp.json()
            for row in rows:
                ts_exchange = datetime.fromtimestamp(int(row[0]) / 1000, tz=UTC)
                ts_received = datetime.now(UTC)

                writer.write_mark_index(
                    symbol,
                    ts_exchange=ts_exchange,
                    ts_received=ts_received,
                    mark_price=0.0,  # placeholder
                    index_price=float(row[4]),  # close price
                    estimated_settle_price=0.0,
                    last_funding_rate=0.0,
                    interest_rate=0.0,
                    next_funding_time=datetime.now(UTC),
                    source=f"binance_usdm_recovered_indexKlines_{interval}",
                )
                recovered_index += 1
        else:
            errors.append(f"HTTP {resp.status_code} from index klines")
    except Exception as exc:
        errors.append(f"Mark/index recovery error: {exc!r}")

    return RecoveryResult(
        channel="positioning",
        symbol=symbol,
        gap_start=gap_start,
        gap_end=gap_end,
        recovered_count=recovered_mark + recovered_index,
        resolution=f"{interval}_coarse",
        source="markPriceKlines_indexPriceKlines",
        errors=errors,
    )


async def recover_basis_gap(
    client: httpx.AsyncClient, writer: ParquetWriter, symbol: str,
    gap_start: datetime, gap_end: datetime, period: str = "1h"
) -> RecoveryResult:
    """Recover basis using historical aggregated data.

    Basis is available at period intervals (5m, 1h, etc); returns coarse recovery.
    """
    errors = []
    recovered = 0

    try:
        params = {
            "pair": symbol,
            "contractType": "PERPETUAL",
            "period": period,
            "startTime": int(gap_start.timestamp() * 1000),
            "endTime": int(gap_end.timestamp() * 1000),
            "limit": 500,
        }

        resp = await client.get(BASIS_HISTORY_URL, params=params, timeout=httpx.Timeout(10))
        if resp.status_code != 200:
            errors.append(f"HTTP {resp.status_code} from basis history")
        else:
            rows = resp.json()
            for row in rows:
                ts_exchange = datetime.fromtimestamp(int(row["timestamp"]) / 1000, tz=UTC)
                ts_received = datetime.now(UTC)

                writer.write_basis(
                    symbol,
                    ts_exchange=ts_exchange,
                    ts_received=ts_received,
                    index_price=float(row["indexPrice"]),
                    futures_price=float(row["futuresPrice"]),
                    basis_rate=float(row["basisRate"]),
                    basis=float(row["basis"]),
                    annualized_basis_rate=float(row["annualizedBasisRate"]),
                    contract_type=row["contractType"],
                    source=f"binance_usdm_recovered_basis_{period}",
                )
                recovered += 1
    except Exception as exc:
        errors.append(f"Basis recovery error: {exc!r}")

    return RecoveryResult(
        channel="positioning",
        symbol=symbol,
        gap_start=gap_start,
        gap_end=gap_end,
        recovered_count=recovered,
        resolution=f"{period}_coarse",
        source="basis_history",
        errors=errors,
    )
