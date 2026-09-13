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
from littledevil_recorder.restart_recovery import backfill_missed_trades, last_recorded_trade
from littledevil_recorder.storage import ParquetWriter

logger = logging.getLogger(__name__)

DATA_HEALTH_SWEEP_INTERVAL_SECONDS = 5.0
FLUSH_INTERVAL_SECONDS = 60.0


def data_root() -> Path:
    return Path(os.getenv("LITTLEDEVIL_DATA_ROOT", "./data"))


async def recover_and_run(
    trade_symbols: list[str],
    depth_symbols: list[str],
    *,
    stop_event: asyncio.Event,
) -> None:
    writer = ParquetWriter(data_root())
    conn = await connect()
    health = DataHealthTracker(conn)

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
        writer.flush_all()

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
        await health.record_message(f"binance_trades_{trade['symbol']}")

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
        await health.record_message(f"binance_depth_{symbol}")

    async def periodic_flush() -> None:
        while not stop_event.is_set():
            await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
            written = writer.flush_all()
            if written:
                logger.info("flushed %d parquet file(s)", len(written))

    async def periodic_health_sweep() -> None:
        while not stop_event.is_set():
            await asyncio.sleep(DATA_HEALTH_SWEEP_INTERVAL_SECONDS)
            await health.sweep()

    tasks = [
        asyncio.create_task(run_aggtrade_stream(trade_symbols, on_trade, stop_event=stop_event)),
        asyncio.create_task(periodic_flush()),
        asyncio.create_task(periodic_health_sweep()),
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
        writer.flush_all()
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
