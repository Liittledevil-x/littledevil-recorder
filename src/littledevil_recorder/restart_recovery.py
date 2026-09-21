"""Restart recovery: on startup, detect what a prior run's Parquet files
already cover and backfill the missed window via REST klines/aggTrades
before resuming the live stream -- so a process that dies mid-stream comes
back without a silent gap in the trade record (docs/architecture-review.md
§7's replay-clock discipline, applied to the recorder's own restart case;
Stage 0 gate criterion #3).

The gap itself is never hidden: data_health's gap_started_at/status already
records it (data_health.py), and this module's job is only to shrink the
*data* gap by pulling what Binance's public REST can still supply for the
missed window, not to pretend the gap didn't happen.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pyarrow.parquet as pq

from littledevil_recorder.storage import ParquetWriter, iter_parquet_paths

REST_HOST = "https://data-api.binance.vision"
MAX_AGGTRADES_PER_REQUEST = 1000


@dataclass
class RecoveryPlan:
    symbol: str
    last_known_trade_id: int | None
    last_known_ts: datetime | None


def last_recorded_trade(data_root: Path, symbol: str, day) -> RecoveryPlan:
    """Reads the most recent trade this process (or a prior one) already
    wrote for `symbol` on `day`, so recovery knows where to resume from.

    The day's persisted trade file can be large, so find the maximum in
    bounded Parquet batches rather than materializing its columns at once.
    """
    max_trade_id: int | None = None
    max_trade_ts: datetime | None = None
    for path in iter_parquet_paths(data_root, "trades", symbol, day):
        parquet = pq.ParquetFile(path)
        try:
            for batch in parquet.iter_batches(columns=["trade_id", "ts_exchange"]):
                for trade_id, timestamp in zip(
                    batch.column("trade_id").to_pylist(),
                    batch.column("ts_exchange").to_pylist(),
                    strict=True,
                ):
                    if max_trade_id is None or trade_id > max_trade_id:
                        max_trade_id = trade_id
                        max_trade_ts = timestamp
        finally:
            parquet.close()

    if max_trade_id is None:
        return RecoveryPlan(symbol=symbol, last_known_trade_id=None, last_known_ts=None)
    return RecoveryPlan(
        symbol=symbol,
        last_known_trade_id=max_trade_id,
        last_known_ts=max_trade_ts,
    )


async def backfill_missed_trades(
    client: httpx.AsyncClient, writer: ParquetWriter, symbol: str, from_trade_id: int
) -> int:
    """Pulls aggTrades strictly after `from_trade_id` via REST (the public,
    unauthenticated /api/v3/aggTrades endpoint supports fromId paging) and
    writes them into the same Parquet files the live stream would have.
    Returns the number of trades recovered."""
    recovered = 0
    next_from_id = from_trade_id + 1

    while True:
        resp = await client.get(
            f"{REST_HOST}/api/v3/aggTrades",
            params={"symbol": symbol.upper(), "fromId": next_from_id, "limit": MAX_AGGTRADES_PER_REQUEST},
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break

        for raw in batch:
            ts_exchange = datetime.fromtimestamp(raw["T"] / 1000, tz=UTC)
            writer.write_trade(
                symbol,
                trade_id=raw["a"],
                ts_exchange=ts_exchange,
                ts_received=ts_exchange,  # recovered, not observed live -- see module docstring
                price=float(raw["p"]),
                qty=float(raw["q"]),
                is_buyer_maker=raw["m"],
            )
            recovered += 1

        if len(batch) < MAX_AGGTRADES_PER_REQUEST:
            break
        next_from_id = batch[-1]["a"] + 1

    return recovered
