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
from littledevil_recorder.liquidation import run_liquidation_stream
from littledevil_recorder.local_manifest import (
    record_compaction_failure,
    record_flush_failure,
    record_process_start,
)
from littledevil_recorder.positioning_poller import run_positioning_poller
from littledevil_recorder.restart_recovery import backfill_missed_trades, last_recorded_trade
from littledevil_recorder.storage import (
    CompactionError,
    FlushAllError,
    ParquetWriter,
    closed_part_days,
    compact_closed_day,
)
from littledevil_recorder.subscription_manager import SubscriptionManager

logger = logging.getLogger(__name__)

DATA_HEALTH_SWEEP_INTERVAL_SECONDS = 5.0
FLUSH_INTERVAL_SECONDS = 60.0
COMPACTION_INTERVAL_SECONDS = 300.0
SUBSCRIPTION_RECONCILE_INTERVAL_SECONDS = 10.0


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
    positioning_symbols: list[str] | None = None,
    liquidation_symbols: list[str] | None = None,
    *,
    stop_event: asyncio.Event,
) -> None:
    heartbeat = record_process_start(data_root())
    logger.info(
        "process start #%d (first started %s)",
        heartbeat.restart_count,
        heartbeat.first_started_at,
    )

    subscriptions = SubscriptionManager(data_root())
    # Env-var symbols seed desired state on first-ever start; once the
    # manager has persisted state, that state is authoritative and env vars
    # are ignored -- otherwise a restart with a stale env var would silently
    # undo a runtime add/remove (violates restart-safe desired-state
    # restoration).
    if not subscriptions.desired_symbols("trades") and not subscriptions.desired_symbols("depth"):
        subscriptions.add_symbols("trades", trade_symbols)
        subscriptions.add_symbols("depth", depth_symbols)
    if not subscriptions.desired_symbols("positioning"):
        subscriptions.add_symbols("positioning", positioning_symbols or [])
    if not subscriptions.desired_symbols("liquidation"):
        subscriptions.add_symbols("liquidation", liquidation_symbols or [])
    trade_symbols = subscriptions.desired_symbols("trades")
    depth_symbols = subscriptions.desired_symbols("depth")

    writer = ParquetWriter(data_root())
    conn = await connect()
    health = DataHealthTracker(conn)
    for symbol in trade_symbols:
        health.register(f"binance_trades_{symbol}")
    for symbol in depth_symbols:
        health.register(f"binance_depth_{symbol}")
    for symbol in subscriptions.desired_symbols("positioning"):
        health.register(f"binance_positioning_{symbol}")
    for symbol in subscriptions.desired_symbols("liquidation"):
        health.register(f"binance_liquidation_{symbol}")

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

    def on_positioning_polled(symbol: str, error: Exception | None) -> None:
        channel = f"binance_positioning_{symbol}"
        health.register(channel)
        if error is None:
            health.record_message(channel)
        # A failed poll intentionally does not record_message: the channel
        # ages toward stale/suspended via sweep() instead of being marked
        # falsely fresh, so a persistent Binance-side failure is visible in
        # Data Health rather than hidden by a same-tick health touch.

    async def on_liquidation(event: dict) -> None:
        symbol = event["symbol"]
        if symbol not in subscriptions.desired_symbols("liquidation"):
            return
        writer.write_liquidation(
            symbol,
            ts_exchange=event["ts_exchange"],
            ts_received=event["ts_received"],
            side=event["side"],
            order_type=event["order_type"],
            time_in_force=event["time_in_force"],
            orig_qty=event["orig_qty"],
            price=event["price"],
            avg_price=event["avg_price"],
            order_status=event["order_status"],
            last_filled_qty=event["last_filled_qty"],
            accumulated_qty=event["accumulated_qty"],
            order_trade_time=event["order_trade_time"],
        )
        health.record_message(f"binance_liquidation_{symbol}")

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
        kinds_by_channel = {
            "trades": ["trades"],
            "depth": ["depth"],
            "positioning": ["open_interest", "funding", "mark_index"],
            "liquidation": ["liquidation"],
        }
        while not stop_event.is_set():
            await asyncio.sleep(COMPACTION_INTERVAL_SECONDS)
            for channel, kinds in kinds_by_channel.items():
                for symbol in subscriptions.desired_symbols(channel):
                    for kind in kinds:
                        for day in closed_part_days(data_root(), kind, symbol):
                            try:
                                path = await asyncio.to_thread(compact_closed_day, data_root(), kind, symbol, day)
                                if path:
                                    logger.info("compacted %s/%s/%s", kind, symbol, day.isoformat())
                            except CompactionError as exc:
                                logger.exception("background compaction failed for %s/%s/%s", kind, symbol, day)
                                record_compaction_failure(data_root(), kind=exc.kind, symbol=exc.symbol,
                                                          day=exc.day, error=repr(exc.cause))

    stream_supervisor = _StreamSupervisor(
        subscriptions=subscriptions,
        health=health,
        on_trade=on_trade,
        on_depth_event=on_depth_event,
        stop_event=stop_event,
    )
    await stream_supervisor.reconcile()

    async def periodic_reconcile() -> None:
        """Runtime add/remove: polls desired state against each stream's
        currently running symbol set and restarts only the affected
        stream(s) -- never both at once for a one-channel change, so a
        depth-only rotation never drops trade coverage for any symbol."""
        while not stop_event.is_set():
            await asyncio.sleep(SUBSCRIPTION_RECONCILE_INTERVAL_SECONDS)
            await stream_supervisor.reconcile()

    tasks = [
        asyncio.create_task(periodic_flush()),
        asyncio.create_task(periodic_health_sweep()),
        asyncio.create_task(periodic_compaction()),
        asyncio.create_task(periodic_reconcile()),
        asyncio.create_task(run_positioning_poller(
            subscriptions, writer, stop_event=stop_event, on_symbol_polled=on_positioning_polled,
        )),
        asyncio.create_task(run_liquidation_stream(on_liquidation, stop_event=stop_event)),
    ]

    try:
        await stop_event.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await stream_supervisor.stop()
        _flush_all_logging_failures(writer)
        if not await health.flush():
            logger.error("final data_health flush failed; pending=%d", health.pending_channels)
        writer.close()
        await conn.close()


class _StreamSupervisor:
    """Owns the live aggTrade/depth stream tasks and restarts exactly the
    channel whose desired symbol set changed. `run_aggtrade_stream`/
    `run_depth_stream` each take a fixed symbol list for their whole
    connection lifetime (no in-protocol SUBSCRIBE/UNSUBSCRIBE), and the
    recording set only rotates daily with hysteresis (scanner-attention-
    routing.md §2) -- so a full reconnect of just the changed channel on
    change, rather than per-symbol subscribe messages, is the right
    granularity: simple, and cheap at this churn rate.
    """

    def __init__(
        self,
        *,
        subscriptions: SubscriptionManager,
        health: DataHealthTracker,
        on_trade,
        on_depth_event,
        stop_event: asyncio.Event,
    ) -> None:
        self._subscriptions = subscriptions
        self._health = health
        self._on_trade = on_trade
        self._on_depth_event = on_depth_event
        self._outer_stop = stop_event
        self._running: dict[str, list[str]] = {"trades": [], "depth": []}
        self._task: dict[str, asyncio.Task | None] = {"trades": None, "depth": None}
        self._task_stop: dict[str, asyncio.Event | None] = {"trades": None, "depth": None}

    async def reconcile(self) -> None:
        if self._outer_stop.is_set():
            return
        for channel in ("trades", "depth"):
            diff = self._subscriptions.diff_from(channel, self._running[channel])
            if not diff.changed:
                continue
            desired = self._subscriptions.desired_symbols(channel)
            logger.info(
                "subscription change on %s: +%s -%s -> %d symbol(s)",
                channel, diff.to_add, diff.to_remove, len(desired),
            )
            for symbol in diff.to_add:
                self._health.register(f"binance_{channel}_{symbol}")
            await self._restart_channel(channel, desired)

    async def _restart_channel(self, channel: str, desired: list[str]) -> None:
        old_task = self._task[channel]
        old_stop = self._task_stop[channel]
        if old_task is not None:
            assert old_stop is not None
            old_stop.set()
            old_task.cancel()
            try:
                await old_task
            except (asyncio.CancelledError, Exception):  # a dropped connection cancels cleanly either way
                pass

        if not desired:
            self._running[channel] = []
            self._task[channel] = None
            self._task_stop[channel] = None
            return

        task_stop = asyncio.Event()

        async def _run_with_failure_tracking() -> None:
            try:
                if channel == "trades":
                    await run_aggtrade_stream(desired, self._on_trade, stop_event=task_stop)
                else:
                    await run_depth_stream(desired, self._on_depth_event, stop_event=task_stop)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("%s stream task failed for %s", channel, desired)
                self._subscriptions.mark_failed(channel, desired, repr(exc))
                raise

        self._task[channel] = asyncio.create_task(_run_with_failure_tracking())
        self._task_stop[channel] = task_stop
        self._running[channel] = list(desired)
        self._subscriptions.mark_subscribed(channel, desired)

    async def stop(self) -> None:
        for channel in ("trades", "depth"):
            task = self._task[channel]
            task_stop = self._task_stop[channel]
            if task is None:
                continue
            if task_stop is not None:
                task_stop.set()
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    trade_symbols_env = os.getenv("LITTLEDEVIL_TRADE_SYMBOLS", "BTCUSDT,ETHUSDT")
    depth_symbols_env = os.getenv("LITTLEDEVIL_DEPTH_SYMBOLS", "BTCUSDT,ETHUSDT")
    positioning_symbols_env = os.getenv("LITTLEDEVIL_POSITIONING_SYMBOLS", "BTCUSDT,ETHUSDT")
    liquidation_symbols_env = os.getenv("LITTLEDEVIL_LIQUIDATION_SYMBOLS", "BTCUSDT,ETHUSDT")
    trade_symbols = [s.strip() for s in trade_symbols_env.split(",") if s.strip()]
    depth_symbols = [s.strip() for s in depth_symbols_env.split(",") if s.strip()]
    positioning_symbols = [s.strip() for s in positioning_symbols_env.split(",") if s.strip()]
    liquidation_symbols = [s.strip() for s in liquidation_symbols_env.split(",") if s.strip()]

    stop_event = asyncio.Event()

    async def run() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)
        await recover_and_run(
            trade_symbols, depth_symbols, positioning_symbols, liquidation_symbols, stop_event=stop_event,
        )

    asyncio.run(run())


if __name__ == "__main__":
    main()
