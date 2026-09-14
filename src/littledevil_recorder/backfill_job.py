"""Orchestrates the Stage 0 historical backfill (phase 0.3): pulls ~12
months of daily klines + aggTrades from the archive for the CoinGecko
top-200, writes them into the same Parquet layout the live recorder uses,
and logs the dev/calibration/holdout split boundary durably in Postgres
(the `experiments` table -- data-and-events.md §1 -- since this is exactly
the kind of "what configuration/window was this run against" fact that
table exists to hold).

Not meant to be imported and run inline in a test -- a real run against
200 symbols x ~365 days is many hours and a meaningful amount of bandwidth.
Run it as a script; it is idempotent per (symbol, day) since storage.py's
flush is append-and-write, and top_200_symbols/fetch_daily_* are cheap to
retry on a transient failure.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx

from littledevil_recorder.backfill import (
    archive_microseconds_to_datetime,
    compute_split_boundary,
    fetch_daily_klines,
    stream_daily_aggtrades,
    top_200_symbols,
    which_block,
)
from littledevil_recorder.db import connect
from littledevil_recorder.storage import ParquetWriter

logger = logging.getLogger(__name__)

BACKFILL_MONTHS = 12
# Streaming (see backfill_symbol below) removed the worst of the per-symbol
# memory cost, but each concurrent symbol still holds one day's compressed
# zip body plus the writer's buffered rows for that day at once -- 5
# concurrent liquid symbols was enough to OOM-kill a 2GB instance on the
# first full-scale run. 3 is a safer default; override via
# LITTLEDEVIL_BACKFILL_CONCURRENCY for a larger box.
CONCURRENT_SYMBOL_LIMIT = int(os.getenv("LITTLEDEVIL_BACKFILL_CONCURRENCY", "3"))


async def log_split_boundary(conn, *, dataset_id: str, boundary) -> None:
    """Records the split boundary in `experiments` so 'don't tune on the
    same window you validate on' has a durable, queryable answer to
    'which dates were dev/calibration/holdout for this backfill'
    (architecture-review.md §7, §8, §8b) -- this is Stage 0 gate criterion
    #5: "the backfill's dev/holdout split boundary logged"."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO experiments (id, dataset_id, feature_version, prompt_version, model_id, config_hash, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(uuid.uuid4()),
                dataset_id,
                "n/a",  # no feature engine exists yet at Stage 0
                "n/a",
                "n/a",
                "n/a",
                json.dumps(
                    {
                        "kind": "historical_backfill_split_boundary",
                        "dev_start": boundary.dev_start.isoformat(),
                        "dev_end": boundary.dev_end.isoformat(),
                        "calibration_start": boundary.calibration_start.isoformat(),
                        "calibration_end": boundary.calibration_end.isoformat(),
                        "holdout_start": boundary.holdout_start.isoformat(),
                        "holdout_end": boundary.holdout_end.isoformat(),
                    }
                ),
            ),
        )


async def backfill_symbol(
    client: httpx.AsyncClient,
    writer: ParquetWriter,
    symbol: str,
    days: list[date],
) -> tuple[int, int]:
    """Returns (days_with_data, days_missing) for this symbol."""
    days_with_data = 0
    days_missing = 0

    for day in days:
        had_any_rows = False
        # Streamed, not materialized as a list -- a liquid symbol's day is
        # 1M+ rows, and holding every row as a dict (on top of the writer's
        # own buffered copy) is what OOM-killed the first full-scale run
        # attempt on a 2GB instance. Each row is written and released
        # immediately; only one buffered copy (the writer's own) exists at
        # any moment, and that is flushed to disk at the end of this day
        # before the next day's rows start arriving.
        async for row in stream_daily_aggtrades(client, symbol, day):
            if row is None:  # sentinel: this symbol/day isn't archived
                break
            had_any_rows = True
            ts = archive_microseconds_to_datetime(row["transact_time"])
            writer.write_trade(
                symbol,
                trade_id=int(row["agg_trade_id"]),
                ts_exchange=ts,
                ts_received=ts,  # backfilled, not observed live
                price=float(row["price"]),
                qty=float(row["quantity"]),
                is_buyer_maker=row["is_buyer_maker"] == "True",
            )

        if not had_any_rows:
            days_missing += 1
            continue

        days_with_data += 1
        writer.flush_trades(symbol, day)

        klines = await fetch_daily_klines(client, symbol, day)
        # klines/ isn't in this repo's Stage 0 scope (candle rebuild from
        # aggTrades is Trunk work per architecture-review.md §7) -- fetched
        # here only to confirm archive coverage matches for both series;
        # not written to disk.
        if klines is None:
            logger.warning("%s %s: aggTrades present but klines missing", symbol, day)

    return days_with_data, days_missing


async def run_backfill(
    *, data_root: Path, months: int = BACKFILL_MONTHS, symbol_limit: int | None = None
) -> None:
    end = datetime.now(UTC).date() - timedelta(days=1)  # yesterday: today's file may not exist yet
    start = end - timedelta(days=30 * months - 1)
    boundary = compute_split_boundary(start, end)
    dataset_id = f"backfill_{start.isoformat()}_{end.isoformat()}"

    logger.info(
        "backfill window %s to %s -- dev %s..%s, calibration %s..%s, holdout %s..%s",
        start, end, boundary.dev_start, boundary.dev_end,
        boundary.calibration_start, boundary.calibration_end,
        boundary.holdout_start, boundary.holdout_end,
    )

    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    writer = ParquetWriter(data_root)
    conn = await connect()

    try:
        await log_split_boundary(conn, dataset_id=dataset_id, boundary=boundary)

        async with httpx.AsyncClient(timeout=30.0) as client:
            symbols = await top_200_symbols(client, api_key=os.getenv("COINGECKO_API_KEY"))
            if symbol_limit:
                symbols = symbols[:symbol_limit]

            semaphore = asyncio.Semaphore(CONCURRENT_SYMBOL_LIMIT)

            async def bounded(symbol: str) -> tuple[str, int, int] | None:
                async with semaphore:
                    try:
                        found, missing = await backfill_symbol(client, writer, symbol, days)
                    except Exception:
                        # Logged immediately, not batched until every one of
                        # ~200 symbols finishes (asyncio.gather's
                        # return_exceptions=True previously swallowed this
                        # silently for the run's entire multi-hour duration
                        # -- a PermissionError on the data directory was
                        # invisible this way across three separate OOM
                        # debugging attempts, since the real failure was
                        # never logged until the very end that never came).
                        logger.exception("symbol backfill failed: %s", symbol)
                        return None
                    logger.info("%s: %d days with data, %d days missing", symbol, found, missing)
                    return symbol, found, missing

            await asyncio.gather(*(bounded(s) for s in symbols))
    finally:
        await conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    data_root = Path(os.getenv("LITTLEDEVIL_DATA_ROOT", "./data"))
    symbol_limit = os.getenv("LITTLEDEVIL_BACKFILL_SYMBOL_LIMIT")
    asyncio.run(
        run_backfill(
            data_root=data_root,
            symbol_limit=int(symbol_limit) if symbol_limit else None,
        )
    )


if __name__ == "__main__":
    main()
