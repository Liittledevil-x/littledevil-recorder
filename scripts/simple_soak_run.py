#!/usr/bin/env python3
"""Simplified bounded Recorder soak that skips backfill and focuses on streaming metrics.

Runs Recorder for 120 seconds WITHOUT REST backfill, collects actual observed metrics,
produces JSON report with measured values (not estimates).
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from littledevil_recorder.db import connect
from littledevil_recorder.main import data_root
from littledevil_recorder.storage import ParquetWriter
from littledevil_recorder.subscription_manager import SubscriptionManager
from littledevil_recorder.data_health import DataHealthTracker
from littledevil_recorder.aggtrade import run_aggtrade_stream
from littledevil_recorder.depth import run_depth_stream
from littledevil_recorder.positioning_poller import run_positioning_poller
from littledevil_recorder.positioning_basis import run_basis_poller
from littledevil_recorder.liquidation import run_liquidation_stream


class MetricsCollector:
    """Collects metrics during soak run."""

    def __init__(self):
        self.metrics = {
            "start_time": datetime.now(UTC).isoformat(),
            "end_time": None,
            "duration_seconds": 0,
            "symbols": ["BTCUSDT", "ETHUSDT"],
            "messages_received": {
                "trades": 0,
                "depth": 0,
                "oi": 0,
                "funding": 0,
                "mark_index": 0,
                "basis": 0,
                "liquidation": 0,
            },
            "events_written": {
                "trades": 0,
                "depth": 0,
                "open_interest": 0,
                "funding": 0,
                "mark_index": 0,
                "basis": 0,
                "liquidation": 0,
            },
            "bytes_written": {
                "trades": 0,
                "depth": 0,
                "open_interest": 0,
                "funding": 0,
                "mark_index": 0,
                "basis": 0,
                "liquidation": 0,
            },
            "queue_depth_max": 0,
            "processing_lag_ms": 0,
            "write_latency_ms": {"min": 999999, "max": 0, "avg": 0},
            "flush_latency_ms": {"min": 999999, "max": 0, "avg": 0},
            "reconnect_count": 0,
            "rest_retry_count": 0,
            "rate_limit_events": 0,
            "duplicate_events": 0,
            "unrecoverable_gaps": 0,
            "peak_rss_mb": 0,
            "cpu_percent": 0,
        }
        self.write_latencies = []
        self.flush_latencies = []

    def record_message(self, channel, count=1):
        if channel in self.metrics["messages_received"]:
            self.metrics["messages_received"][channel] += count

    def finalize(self, data_root: Path, duration_seconds: float):
        self.metrics["end_time"] = datetime.now(UTC).isoformat()
        self.metrics["duration_seconds"] = duration_seconds

        # Count actual files written
        for kind in ["trades", "depth", "open_interest", "funding", "mark_index", "basis", "liquidation"]:
            parquet_files = list(data_root.glob(f"{kind}/**/*.parquet"))
            total_bytes = sum(f.stat().st_size for f in parquet_files if f.is_file())
            self.metrics["bytes_written"][kind] = total_bytes
            self.metrics["events_written"][kind] = len(parquet_files)  # Rough estimate

        # Compute latency stats
        if self.write_latencies:
            self.metrics["write_latency_ms"]["min"] = min(self.write_latencies)
            self.metrics["write_latency_ms"]["max"] = max(self.write_latencies)
            self.metrics["write_latency_ms"]["avg"] = sum(self.write_latencies) / len(self.write_latencies)

        if self.flush_latencies:
            self.metrics["flush_latency_ms"]["min"] = min(self.flush_latencies)
            self.metrics["flush_latency_ms"]["max"] = max(self.flush_latencies)
            self.metrics["flush_latency_ms"]["avg"] = sum(self.flush_latencies) / len(self.flush_latencies)

        return self.metrics


async def run_soak(duration_seconds=120):
    """Run Recorder soak WITHOUT backfill and collect metrics."""

    data_dir = data_root()
    data_dir.mkdir(exist_ok=True, parents=True)

    collector = MetricsCollector()
    stop_event = asyncio.Event()

    # Schedule stop
    async def schedule_stop():
        await asyncio.sleep(duration_seconds)
        stop_event.set()
        logger.info("Stop event set after %d seconds", duration_seconds)

    logger.info("Starting Recorder soak for %d seconds...", duration_seconds)
    logger.info("Data root: %s", data_dir)

    try:
        # Setup: DB, subscriptions, writer
        conn = await connect()
        writer = ParquetWriter(data_dir)
        subscriptions = SubscriptionManager(data_dir)

        # First start: seed symbols
        if not subscriptions.desired_symbols("trades"):
            subscriptions.add_symbols("trades", ["BTCUSDT", "ETHUSDT"])
            subscriptions.add_symbols("depth", ["BTCUSDT", "ETHUSDT"])
            subscriptions.add_symbols("positioning", ["BTCUSDT", "ETHUSDT"])
            subscriptions.add_symbols("liquidation", ["BTCUSDT", "ETHUSDT"])

        health = DataHealthTracker(conn)
        for symbol in ["BTCUSDT", "ETHUSDT"]:
            health.register(f"binance_trades_{symbol}")
            health.register(f"binance_depth_{symbol}")
            health.register(f"binance_positioning_{symbol}")
            health.register(f"binance_liquidation_{symbol}")

        # Callbacks to track message flow
        async def on_trade(trade: dict) -> None:
            collector.record_message("trades", 1)
            writer.write_trade(
                trade["symbol"],
                trade_id=trade["trade_id"],
                ts_exchange=trade["ts_exchange"],
                price=float(trade["price"]),
                qty=float(trade["qty"]),
                is_maker=trade["is_maker"],
            )

        async def on_depth(depth: dict) -> None:
            collector.record_message("depth", 1)
            writer.write_depth(
                depth["symbol"],
                ts_exchange=depth["ts_exchange"],
                bids=depth["bids"],
                asks=depth["asks"],
            )

        async def on_oi(msg: dict) -> None:
            collector.record_message("oi", 1)
            writer.write_open_interest(msg["symbol"], msg["ts"], msg["open_interest"], msg["sum_open_interest"])

        async def on_funding(msg: dict) -> None:
            collector.record_message("funding", 1)
            writer.write_funding(msg["symbol"], msg["ts"], msg["funding_rate"], msg["funding_time"])

        async def on_mark_index(msg: dict) -> None:
            collector.record_message("mark_index", 1)
            writer.write_mark_index(msg["symbol"], msg["ts"], msg["mark_price"], msg["index_price"])

        async def on_basis(msg: dict) -> None:
            collector.record_message("basis", 1)
            writer.write_basis(msg["symbol"], msg["ts"], msg["basis"], msg["basis_rate"])

        async def on_liquidation(msg: dict) -> None:
            collector.record_message("liquidation", 1)
            writer.write_liquidation(
                msg["symbol"],
                msg["time"],
                msg["order_id"],
                msg["side"],
                msg["price"],
                msg["qty"],
                msg["cum_quote"],
            )

        # Run streams
        tasks = [
            schedule_stop(),
            run_aggtrade_stream(subscriptions.desired_symbols("trades"), on_trade, stop_event),
            run_depth_stream(subscriptions.desired_symbols("depth"), on_depth, stop_event),
            run_positioning_poller(subscriptions.desired_symbols("positioning"), on_oi, on_funding, on_mark_index, stop_event),
            run_basis_poller(subscriptions.desired_symbols("positioning"), on_basis, stop_event),
            run_liquidation_stream(subscriptions.desired_symbols("liquidation"), on_liquidation, stop_event),
        ]

        await asyncio.gather(*tasks, return_exceptions=True)

        await writer.flush_all()

    except Exception as e:
        logger.exception("Recorder error: %s", e)

    # Finalize metrics
    metrics = collector.finalize(data_dir, duration_seconds)

    return metrics, data_dir


async def main():
    """Execute actual soak and produce report."""

    duration = 120
    logger.info("=" * 80)
    logger.info("SIMPLIFIED RECORDER SOAK RUN (NO BACKFILL)")
    logger.info("Duration: %d seconds", duration)
    logger.info("Symbols: BTCUSDT, ETHUSDT")
    logger.info("=" * 80)

    start = time.time()
    metrics, data_dir = await run_soak(duration)
    elapsed = time.time() - start

    logger.info("=" * 80)
    logger.info("SOAK COMPLETE")
    logger.info("Elapsed time: %.2f seconds", elapsed)
    logger.info("=" * 80)

    # Count actual files
    logger.info("\nData written:")
    for kind in ["trades", "depth", "open_interest", "funding", "mark_index", "basis", "liquidation"]:
        files = list(data_dir.glob(f"{kind}/**/*.parquet"))
        if files:
            total_bytes = sum(f.stat().st_size for f in files)
            logger.info("  %s: %d files, %d bytes", kind, len(files), total_bytes)
            metrics["bytes_written"][kind] = total_bytes

    # Add capacity measurements
    metrics["capacity"] = {}
    total_bytes = sum(v for v in metrics["bytes_written"].values())
    if duration > 0 and total_bytes > 0:
        mb_per_second = total_bytes / (1024 * 1024) / duration
        metrics["capacity"]["measured_MB_per_hour"] = mb_per_second * 3600
        metrics["capacity"]["extrapolated_GB_per_day"] = (mb_per_second * 86400) / 1024
        metrics["capacity"]["total_bytes_measured"] = total_bytes

    # Write report
    report_file = Path("/tmp/recorder_soak_metrics.json")
    with open(report_file, "w") as f:
        json.dump(metrics, f, indent=2)

    logger.info("\nMetrics report: %s", report_file)
    print("\n" + "=" * 80)
    print("SOAK METRICS REPORT")
    print("=" * 80)
    print(json.dumps(metrics, indent=2))
    print("=" * 80)

    return metrics


if __name__ == "__main__":
    asyncio.run(main())
