"""Entry point: wires aggTrade ingestion, depth ingestion (recording set),
Data Health, and Parquet storage into one always-on process. On startup,
runs restart recovery for every symbol before resuming the live stream, so
a prior crash never leaves a silent gap (docs/CLAUDE.md: recording never
pauses; Stage 0 gate criterion #3).

This is deliberately a thin composition root -- the modules it wires
(aggtrade.py, depth.py, universe.py, event_gates.py, data_health.py,
storage.py, restart_recovery.py) are each independently tested; this file
is what makes them one running service.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from datetime import UTC, datetime
from pathlib import Path

import httpx

from littledevil_recorder.aggtrade import run_aggtrade_stream
from littledevil_recorder.data_health import DataHealthTracker
from littledevil_recorder.db import connect
from littledevil_recorder.depth import run_depth_stream
from littledevil_recorder.local_manifest import (
    record_compaction_failure,
    record_flush_failure,
    record_process_start,
)
from littledevil_recorder.restart_recovery import backfill_missed_trades, last_recorded_trade
from littledevil_recorder.storage import (
    CompactionError,
    FlushAllError,
    ParquetWriter,
    closed_part_days,
    compact_closed_day,
)

logger = logging.getLogger(__name__)

DATA_HEALTH_SWEEP_INTERVAL_SECONDS = 5.0
FLUSH_INTERVAL_SECONDS = 60.0
COMPACTION_INTERVAL_SECONDS = 300.0


def data_root() -> Path:
    return Path(os.getenv("LITTLEDEVIL_DATA_ROOT", "./data"))


def _flush_all_logging_failures(writer: ParquetWriter) -> list:
    """writer.flush_all(), but a failing key must never take down the caller
    or silently stop future flushing for every OTHER key.

    This is the fix for the 2026-09-18/09-19 production incident: a
    pre-existing corrupt depth/ETHUSDT file made storage.flush_all() raise
    on its very first call inside periodic_flush's while loop. That
    exception was unguarded, so it silently killed the entire periodic_flush
    asyncio task -- with no log line, since nothing ever retrieved the
    task's exception -- for the rest of the process's life. Every trade/
    depth event after that kept appending to in-memory buffers with nothing
    ever flushing them again, until unbounded buffer growth hit the
    MemoryMax cgroup limit and got OOM-killed, roughly 7 hours later, twice
    in a row (2026-09-18 21:15:11 UTC and 2026-09-19 04:34:22 UTC).

    Every failing key is now logged loudly and recorded durably via
    local_manifest.record_flush_failure (see that module's docstring for
    why a log line alone isn't enough), but the call always returns
    normally so the caller's loop (periodic_flush, or the one-off startup
    flush) keeps running past a bad key instead of dying on it.
    """
    try:
        return writer.flush_all()
    except FlushAllError as exc:
        for err in exc.errors:
            logger.error(
                "flush failed for %s/%s/%s: %s",
                err.kind,
                err.symbol,
                err.day.isoformat(),
                err.cause,
            )
            record_flush_failure(
                data_root(),
                kind=err.kind,
                symbol=err.symbol,
                day=err.day.isoformat(),
                error=repr(err.cause),
            )
        return exc.written


async def recover_and_run(
    trade_symbols: list[str],
    depth_symbols: list[str],
    *,
    stop_event: asyncio.Event,
) -> None:
    heartbeat = record_process_start(data_root())
    logger.info(
        "process start #%d (first started %s)",
        heartbeat.restart_count,
        heartbeat.first_started_at,
    )

    writer = ParquetWriter(data_root())
    conn = await connect()
    health = DataHealthTracker(conn)
    for symbol in trade_symbols:
        health.register(f"binance_trades_{symbol}")
    for symbol in depth_symbols:
        health.register(f"binance_depth_{symbol}")

    async with httpx.AsyncClient(timeout=10.0) as client:
        today = datetime.now(UTC).date()
        for symbol in trade_symbols:
            plan = last_recorded_trade(data_root(), symbol, today)
            if plan.last_known_trade_id is not None:
                recovered = await backfill_missed_trades(
                    client, writer, symbol, plan.last_known_trade_id
                )
                if recovered:
                    logger.info("restart recovery: %s recovered %d trades", symbol, recovered)
        _flush_all_logging_failures(writer)

    async def on_trade(trade: dict) -> None:
        writer.write_trade(
            trade["symbol"],
            trade_id=trade["trade_id"],
            ts_exchange=trade["ts_exchange"],
            ts_received=trade["ts_received"],
            price=trade["price"],
            qty=trade["qty"],
            is_buyer_maker=trade["is_buyer_maker"],
        )
        health.record_message(f"binance_trades_{trade['symbol']}")

    async def on_depth_event(symbol: str, book, event: dict) -> None:
        import json

        bids, asks = book.top_n(50)
        writer.write_depth(
            symbol,
            ts_exchange=datetime.fromtimestamp(event["E"] / 1000, tz=UTC) if "E" in event else datetime.now(UTC),
            ts_received=event.get("_ts_received", datetime.now(UTC)),
            is_snapshot=False,
            bids_json=json.dumps(bids),
            asks_json=json.dumps(asks),
            seq=event["u"],
        )
        health.record_message(f"binance_depth_{symbol}")

    async def periodic_flush() -> None:
        while not stop_event.is_set():
            await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
            written = _flush_all_logging_failures(writer)
            if written:
                logger.info("flushed %d parquet file(s)", len(written))

    async def periodic_health_sweep() -> None:
        while not stop_event.is_set():
            await asyncio.sleep(DATA_HEALTH_SWEEP_INTERVAL_SECONDS)
            health.sweep()
            if not await health.flush():
                logger.error(
                    "data_health flush failed (%d total); pending=%d error=%s",
                    health.flush_failures, health.pending_channels, health.last_flush_error,
                )

    async def periodic_compaction() -> None:
        """Closed-day maintenance is intentionally off the ingest loop.
        A failed part read or disk write is durably logged and retried on a
        later pass; it cannot interrupt callbacks or normal flushing."""
        while not stop_event.is_set():
            await asyncio.sleep(COMPACTION_INTERVAL_SECONDS)
            for kind, symbols in (("trades", trade_symbols), ("depth", depth_symbols)):
                for symbol in symbols:
                    for day in closed_part_days(data_root(), kind, symbol):
                        try:
                            path = await asyncio.to_thread(compact_closed_day, data_root(), kind, symbol, day)
                            if path:
                                logger.info("compacted %s/%s/%s", kind, symbol, day.isoformat())
                        except CompactionError as exc:
                            logger.exception("background compaction failed for %s/%s/%s", kind, symbol, day)
                            record_compaction_failure(data_root(), kind=exc.kind, symbol=exc.symbol,
                                                      day=exc.day, error=repr(exc.cause))

    tasks = [
        asyncio.create_task(run_aggtrade_stream(trade_symbols, on_trade, stop_event=stop_event)),
        asyncio.create_task(periodic_flush()),
        asyncio.create_task(periodic_health_sweep()),
        asyncio.create_task(periodic_compaction()),
    ]
    if depth_symbols:
        tasks.append(
            asyncio.create_task(run_depth_stream(depth_symbols, on_depth_event, stop_event=stop_event))
        )

    try:
        await stop_event.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        _flush_all_logging_failures(writer)
        if not await health.flush():
            logger.error("final data_health flush failed; pending=%d", health.pending_channels)
        writer.close()
        await conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    trade_symbols_env = os.getenv("LITTLEDEVIL_TRADE_SYMBOLS", "BTCUSDT,ETHUSDT")
    depth_symbols_env = os.getenv("LITTLEDEVIL_DEPTH_SYMBOLS", "BTCUSDT,ETHUSDT")
    trade_symbols = [s.strip() for s in trade_symbols_env.split(",") if s.strip()]
    depth_symbols = [s.strip() for s in depth_symbols_env.split(",") if s.strip()]

    stop_event = asyncio.Event()

    async def run() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)
        await recover_and_run(trade_symbols, depth_symbols, stop_event=stop_event)

    asyncio.run(run())


if __name__ == "__main__":
    main()
